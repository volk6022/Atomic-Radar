"""Настройки отбора целиком — одним JSON: выгрузить, поправить снаружи, залить назад.

Почему это заменяет версионирование. Прежний порядок был такой: правку предлагают
(`CONFIG_PROPOSE`), потом владелец её включает (`CONFIG_ACTIVATE`), у каждой сущности
своя лесенка версий — `v1`, `v2`, `l3-verdict-v4`. Со стороны человека, который просто
хочет поменять формулировки, это не работает: непонятно, что включено сейчас, непонятно,
как поменять всё разом, и нельзя отредактировать настройки в привычном редакторе.

Единица обмена здесь — весь набор настроек: описание бизнеса, боли с якорями и
эталонами, шум, дисквалификаторы, промпты L3. Выгрузка даёт файл, загрузка применяет
его целиком и сразу, без отдельного шага «включить».

⚠️ Загрузка обязана быть либо целой, либо никакой. Половина применённых настроек — это
не «частичный успех», а отбор по правилам, которых никто не задавал: якоря уже новые,
эталоны ещё старые. Поэтому самое рискованное (эталоны L2, которым нужен эмбеддер)
делается первым, и его отказ не должен оставлять следов.

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
"""
from __future__ import annotations

import asyncio
import os

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_DEBUG", "true")

from app.db.models import (Base, CascadeVersion, L2Prototype,  # noqa: E402
                           L3Prompt, ProfileVersion)
from app.services import config_bundle, embeddings  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")


BUNDLE = {
    "format": "atomic-radar-config",
    "version": 1,
    "name": "kurs-тест",
    "business": {"description": "КУРС — оплата счетов зарубежных поставщиков."},
    "pains": {
        "банк не пропускает платеж": {
            "anchors": ["валютный контроль", "банк отказал"],
            "prototypes": ["банк завернул платёж, требует контракт на учёт",
                           "комплаенс не пропускает перевод"],
        },
        "нет валютного счета": {
            "anchors": ["валютный счет", "swift"],
            "prototypes": ["у нас нет валютного счёта, а платить надо"],
        },
    },
    "noise": {
        "сам оказывает такие услуги": ["проводим платежи в любую страну, комиссия от 4%"],
        "офтоп": ["всем привет, как дела"],
    },
    "disqualifiers": {
        "вакансия": ["вакансия", "резюме"],
        "реклама": ["промокод", "вебинар"],
    },
    "l3_prompts": {"dm_v1": "Ты — фильтр сообщений. Ответь JSON."},
}


class FakeEmbedder:
    """Эмбеддер — внешний HTTP-сервис, и только он здесь подменяется.

    Считает вызовы: пересчёт эталонов стоит времени GPU, и «загрузили тот же набор
    ещё раз» не должно означать «посчитали всё заново».
    """

    def __init__(self):
        self.calls: list[list[str]] = []

    def install(self, monkeypatch, *, available=True):
        monkeypatch.setattr(embeddings, "enabled", lambda: available)

        async def fake(phrases):
            self.calls.append(list(phrases))
            return [[0.1, 0.2, 0.3] for _ in phrases]

        monkeypatch.setattr(embeddings, "embed", fake)
        return self


def fresh(fn):
    """Тест целиком в ОДНОМ цикле событий.

    Соединение asyncpg привязано к циклу, в котором создано, поэтому движок нельзя
    завести одним `asyncio.run`, а пользоваться другим: закрытие уедет в чужой, уже
    закрытый цикл, и падение выглядит как «Event loop is closed» где-то в недрах
    пула, а не как ошибка теста. В соседних файлах об эту же грабку уже спотыкались.
    """
    async def main():
        engine = create_async_engine(DB_URL, poolclass=None)
        try:
            async with engine.begin() as conn:
                await conn.execute(text("DROP SCHEMA public CASCADE"))
                await conn.execute(text("CREATE SCHEMA public"))
                await conn.run_sync(Base.metadata.create_all)
            return await fn(async_sessionmaker(engine, expire_on_commit=False))
        finally:
            await engine.dispose()
    return asyncio.run(main())


# ── загрузка ──────────────────────────────────────────────────────────────────

def test_import_applies_everything_and_activates_it_at_once(monkeypatch):
    """Никакого отдельного «включить»: загрузили — работает."""
    FakeEmbedder().install(monkeypatch)

    async def scenario(maker):
        async with maker() as db:
            await config_bundle.import_bundle(db, BUNDLE, actor="owner@local")
        async with maker() as db:
            profile = (await db.execute(select(ProfileVersion).where(
                ProfileVersion.is_active.is_(True)))).scalar_one()
            cascade = (await db.execute(select(CascadeVersion).where(
                CascadeVersion.is_active.is_(True)))).scalar_one()
            prompt = (await db.execute(select(L3Prompt).where(
                L3Prompt.is_active.is_(True)))).scalar_one()
            return (profile.business_description, dict(cascade.pain_anchors),
                    dict(cascade.disqualifiers), prompt.prompt_key,
                    prompt.system_prompt)

    description, anchors, disq, key, system = fresh(scenario)
    assert description == BUNDLE["business"]["description"]
    assert set(anchors) == set(BUNDLE["pains"])
    assert set(disq) == set(BUNDLE["disqualifiers"])
    assert key == "dm_v1"
    assert system == BUNDLE["l3_prompts"]["dm_v1"]


def test_import_stores_prototypes_for_both_pains_and_noise(monkeypatch):
    FakeEmbedder().install(monkeypatch)

    async def scenario(maker):
        async with maker() as db:
            await config_bundle.import_bundle(db, BUNDLE, actor="owner@local")
        async with maker() as db:
            rows = (await db.execute(select(L2Prototype))).scalars().all()
            return [(r.kind, r.phrase, bool(r.vector)) for r in rows]

    rows = fresh(scenario)
    pos = {phrase for kind, phrase, _ in rows if kind == "pos"}
    neg = {phrase for kind, phrase, _ in rows if kind == "neg"}
    assert pos == {p for body in BUNDLE["pains"].values() for p in body["prototypes"]}
    assert neg == {p for items in BUNDLE["noise"].values() for p in items}
    assert all(has_vector for _, _, has_vector in rows), \
        "эталон без вектора в L2 не участвует в сравнении"


def test_import_reports_what_it_applied(monkeypatch):
    """Отчёт — единственное, по чему видно, что загрузилось именно то."""
    FakeEmbedder().install(monkeypatch)

    async def scenario(maker):
        async with maker() as db:
            return await config_bundle.import_bundle(db, BUNDLE, actor="owner@local")

    out = fresh(scenario)
    assert out["pains"] == 2
    assert out["noise"] == 2
    assert out["disqualifiers"] == 2
    assert out["prompts"] == ["dm_v1"]
    assert out["prototypes"] == 5


def test_import_drops_labels_that_are_not_in_the_new_bundle(monkeypatch):
    """Файл — полная правда. Прежние боли, которых в нём нет, обязаны исчезнуть.

    Иначе отбор идёт по смеси двух наборов: старые ярлыки продолжают ловить
    сообщения, и система выглядит работающей, оставаясь настроенной не на то.
    """
    FakeEmbedder().install(monkeypatch)
    first = {**BUNDLE, "pains": {**BUNDLE["pains"],
                                 "старая боль": {"anchors": ["впн"],
                                                 "prototypes": ["не работает впн"]}}}

    async def scenario(maker):
        async with maker() as db:
            await config_bundle.import_bundle(db, first, actor="owner@local")
        async with maker() as db:
            await config_bundle.import_bundle(db, BUNDLE, actor="owner@local")
        async with maker() as db:
            cascade = (await db.execute(select(CascadeVersion).where(
                CascadeVersion.is_active.is_(True)))).scalar_one()
            rows = (await db.execute(select(L2Prototype).where(
                L2Prototype.cascade_version_id == cascade.id))).scalars().all()
            return dict(cascade.pain_anchors), [r.label for r in rows]

    anchors, labels = fresh(scenario)
    assert "старая боль" not in anchors
    assert "старая боль" not in labels


# ── целость ───────────────────────────────────────────────────────────────────

def test_a_dead_embedder_leaves_nothing_applied(monkeypatch):
    """Полбандла хуже, чем ноль: якоря новые, эталоны старые — отбор по правилам,
    которых никто не задавал."""
    FakeEmbedder().install(monkeypatch, available=False)

    async def scenario(maker):
        async with maker() as db:
            with pytest.raises(config_bundle.BundleError):
                await config_bundle.import_bundle(db, BUNDLE, actor="owner@local")
        async with maker() as db:
            return (len((await db.execute(select(ProfileVersion))).scalars().all()),
                    len((await db.execute(select(CascadeVersion))).scalars().all()),
                    len((await db.execute(select(L3Prompt))).scalars().all()))

    assert fresh(scenario) == (0, 0, 0), \
        "после отказа не должно остаться ни одной применённой части"


# ── проверка входа ────────────────────────────────────────────────────────────

@pytest.mark.parametrize("broken, why", [
    ({**BUNDLE, "format": "что-то другое"}, "чужой формат"),
    ({**BUNDLE, "version": 999}, "версия формата из будущего"),
    ({**BUNDLE, "pains": {}}, "без единой боли"),
    ({**BUNDLE, "business": {"description": "  "}}, "пустое описание бизнеса"),
    ({**BUNDLE, "l3_prompts": {"нет-такого": "текст"}}, "неизвестный ключ промпта"),
])
def test_a_broken_bundle_is_refused_before_anything_is_written(monkeypatch, broken, why):
    FakeEmbedder().install(monkeypatch)

    async def scenario(maker):
        async with maker() as db:
            with pytest.raises(config_bundle.BundleError):
                await config_bundle.import_bundle(db, broken, actor="o@local")
        async with maker() as db:
            return len((await db.execute(select(CascadeVersion))).scalars().all())

    assert fresh(scenario) == 0, f"{why}: запись не должна была начаться"


# ── выгрузка ──────────────────────────────────────────────────────────────────

def test_export_returns_what_was_imported(monkeypatch):
    """Круг: выгруженное обязано грузиться обратно без правок руками. Иначе
    «поправить снаружи» превращается в «собрать заново»."""
    FakeEmbedder().install(monkeypatch)

    async def scenario(maker):
        async with maker() as db:
            await config_bundle.import_bundle(db, BUNDLE, actor="owner@local")
        async with maker() as db:
            return await config_bundle.export_bundle(db)

    out = fresh(scenario)
    assert out["format"] == BUNDLE["format"]
    assert out["version"] == BUNDLE["version"]
    assert out["business"] == BUNDLE["business"]
    assert out["disqualifiers"] == BUNDLE["disqualifiers"]
    assert out["l3_prompts"]["dm_v1"] == BUNDLE["l3_prompts"]["dm_v1"]
    for label, body in BUNDLE["pains"].items():
        assert out["pains"][label]["anchors"] == body["anchors"]
        assert out["pains"][label]["prototypes"] == body["prototypes"]
    assert out["noise"] == BUNDLE["noise"]


def test_export_of_an_empty_system_says_so_instead_of_pretending():
    """Пустая выгрузка не должна выглядеть как настроенная система."""
    async def scenario(maker):
        async with maker() as db:
            return await config_bundle.export_bundle(db)

    out = fresh(scenario)
    assert out["pains"] == {}
    assert out["business"]["description"] is None


def test_a_round_trip_does_not_re_embed_unchanged_phrases(monkeypatch):
    """Пересчёт эталонов занимает GPU. Повторная загрузка того же набора не должна
    гонять эмбеддер заново."""
    fake = FakeEmbedder().install(monkeypatch)

    async def scenario(maker):
        async with maker() as db:
            await config_bundle.import_bundle(db, BUNDLE, actor="owner@local")
        first = sum(len(c) for c in fake.calls)
        async with maker() as db:
            out = await config_bundle.export_bundle(db)
        fake.calls.clear()
        async with maker() as db:
            await config_bundle.import_bundle(db, out, actor="owner@local")
        return first, sum(len(c) for c in fake.calls)

    first, second = fresh(scenario)
    assert first == 5, "в первый раз считаются все пять фраз"
    assert second == 0, "фразы не менялись — считать их заново незачем"
