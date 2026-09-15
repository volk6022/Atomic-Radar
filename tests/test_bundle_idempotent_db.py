"""Импорт набора настроек идемпотентен: тот же файл — те же версии (14.8.12a).

Дефект: импорт только что выгруженного `GET /profile/config` файла бампал
`business v5→v6` и промпт `v7→v8` без единого изменения текста, а
`stale_l3_verdict_count` разом объявлял устаревшими все трейсы L3 (+793 ложных
на стенде 14.09). Идемпотентность живёт в реестре (`cascade_registry`), а не в
импорте: тогда и ручная правка с экрана тем же текстом не плодит версий.
`import_bundle` в дополнение отчитывается списком `unchanged` — что не изменилось.

Рядом — 14.8.12b: действующий `l1_bypass_pos_min` в ответе `GET /profile`
(строка `limits` побеждает значение кода) — на экране его не было нигде.

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
"""
from __future__ import annotations

import asyncio
import logging
import os

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_DEBUG", "true")
os.environ.setdefault("RADAR_INGEST_TOKEN", "test-ingest-token")

from app.core.config import get_settings  # noqa: E402
from app.core.security import SessionSigner  # noqa: E402
from app.db.models import (Base, CascadeVersion, L2Prototype,  # noqa: E402
                           L3Prompt, Limit, LlmTrace, ProfileVersion, User)
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.main import create_app  # noqa: E402
from app.services import cascade_registry, config_bundle, embeddings  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")


BUNDLE = {
    "format": "atomic-radar-config",
    "version": 1,
    "name": "идемпотентность",
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
    "noise": {"офтоп": ["всем привет, как дела"]},
    "disqualifiers": {"вакансия": ["вакансия", "резюме"]},
    "l3_prompts": {"dm_v1": "Ты — фильтр сообщений. Ответь JSON.",
                   "public_v1": "Публичный фильтр. Ответь JSON."},
}

ALL_UNCHANGED = ["business", "taxonomy",
                 "l3_prompts:dm_v1", "l3_prompts:public_v1"]


class FakeEmbedder:
    """Шпион на эмбеддере: считает вызовы — пересчёт эталонов стоит GPU-времени,
    и «загрузили тот же набор ещё раз» не должно означать «посчитали заново»."""

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
    """Тест целиком в ОДНОМ цикле событий (см. тот же помощник в
    `test_config_bundle_db.py`: соединение asyncpg привязано к циклу)."""
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


@pytest.fixture(autouse=True)
def _restore_applied_thresholds():
    """Правка действующих порогов в памяти процесса не должна переживать тест:
    conftest эти значения не снимает (см. тот же фикс в `test_thresholds.py`)."""
    yield
    cascade_registry.apply_thresholds(cascade_registry.threshold_defaults())


async def _snapshot(maker):
    """Активные версии трёх групп и число строк в их таблицах."""
    async with maker() as db:
        profile = await cascade_registry.active_profile_version(db)
        cascade = await cascade_registry.active_cascade_version(db)
        dm = await cascade_registry.active_l3_prompt(db, "dm_v1")
        public = await cascade_registry.active_l3_prompt(db, "public_v1")
        counts = (
            len((await db.execute(select(ProfileVersion))).scalars().all()),
            len((await db.execute(select(CascadeVersion))).scalars().all()),
            len((await db.execute(select(L3Prompt))).scalars().all()),
        )
        from app.api.v1 import profile as profile_api
        stale = await profile_api.stale_l3_verdict_count(db)
        return {"profile_id": profile.id if profile else None,
                "cascade_id": cascade.id if cascade else None,
                "dm_id": dm.id if dm else None,
                "public_id": public.id if public else None,
                "dm_version": dm.version if dm else None,
                "counts": counts, "stale": stale}


# ── (а) тот же файл: ничего не меняется ───────────────────────────────────────

def test_reimporting_the_same_bundle_changes_nothing(monkeypatch, caplog):
    """Круг export → import обязан быть NOP-ом: те же активные версии, те же
    таблицы, тот же счётчик устаревших трейсов — и «unchanged» со всеми тремя
    группами в ответе и в хвосте журнальной строки."""
    fake = FakeEmbedder().install(monkeypatch)

    async def scenario(maker):
        async with maker() as db:
            await config_bundle.import_bundle(db, BUNDLE, actor="owner@local")
            # Трейсы «до» и «после»: старый промпт и активный. Импорт того же
            # файла не имеет права объявить устаревшими ни один из них.
            dm = await cascade_registry.active_l3_prompt(db, "dm_v1")
            db.add_all([LlmTrace(stage="l3", model="stub",
                                 prompt_version=dm.version),
                        LlmTrace(stage="l3", model="stub",
                                 prompt_version="dm_v1-v1"),
                        LlmTrace(stage="l3", model="stub",
                                 prompt_version="dm_v1-v1")])
            await db.commit()
        before = await _snapshot(maker)
        async with maker() as db:
            exported = await config_bundle.export_bundle(db)
        fake.calls.clear()
        with caplog.at_level(logging.INFO):
            async with maker() as db:
                out = await config_bundle.import_bundle(db, exported,
                                                        actor="owner@local")
        after = await _snapshot(maker)
        return before, after, out

    with caplog.at_level(logging.INFO):
        before, after, out = fresh(scenario)

    assert before == after, "импорт того же набора обязан ничего не менять"
    assert out["unchanged"] == ALL_UNCHANGED
    assert fake.calls == [], "неизменившиеся эталоны не пересчитываются"

    bundle_logs = [r for r in caplog.records
                   if r.getMessage().startswith("config_bundle_imported")]
    assert bundle_logs, "импорт обязан писать в журнал config_bundle_imported"
    msg = bundle_logs[-1].getMessage()
    # Порядок старых полей не сломан (проверки журнала ищут «thresholds= by=»),
    # а `unchanged` — в самом конце строки.
    assert "thresholds=l1_bypass_pos_min,l2_min_margin by=owner@local" in msg
    assert msg.endswith("unchanged=" + ",".join(ALL_UNCHANGED))

    registry_msgs = [r.getMessage() for r in caplog.records
                     if r.name == "radar.cascade_registry"]
    assert any(m.startswith("profile_version_unchanged version=")
               for m in registry_msgs)
    assert any(m.startswith("cascade_version_unchanged version=")
               for m in registry_msgs)
    assert any(m == "l3_prompt_unchanged key=dm_v1 version=" + after["dm_version"]
               for m in registry_msgs)


# ── (б) изменён один промпт — поднимается только он ──────────────────────────

def test_a_changed_prompt_bumps_only_that_prompt(monkeypatch):
    fake = FakeEmbedder().install(monkeypatch)

    async def scenario(maker):
        async with maker() as db:
            await config_bundle.import_bundle(db, BUNDLE, actor="owner@local")
        before = await _snapshot(maker)
        async with maker() as db:
            exported = await config_bundle.export_bundle(db)
        exported["l3_prompts"]["dm_v1"] += "\nДополнили вопрос."
        fake.calls.clear()
        async with maker() as db:
            out = await config_bundle.import_bundle(db, exported,
                                                    actor="owner@local")
        after = await _snapshot(maker)
        return before, after, out

    before, after, out = fresh(scenario)

    assert after["dm_id"] != before["dm_id"], "изменённый промпт обязан подняться"
    assert after["dm_version"] != before["dm_version"]
    assert after["public_id"] == before["public_id"]
    assert after["profile_id"] == before["profile_id"]
    assert after["cascade_id"] == before["cascade_id"]
    assert after["counts"] == (before["counts"][0], before["counts"][1],
                               before["counts"][2] + 1)
    assert out["unchanged"] == ["business", "taxonomy", "l3_prompts:public_v1"]
    assert fake.calls == [], "правка промпта не трогает эталоны"


# ── (в) новый якорь — новая версия, неизменившиеся боли не пересчитываются ────

def test_a_new_pain_recomputes_only_its_own_phrases(monkeypatch):
    fake = FakeEmbedder().install(monkeypatch)

    async def scenario(maker):
        async with maker() as db:
            await config_bundle.import_bundle(db, BUNDLE, actor="owner@local")
        async with maker() as db:
            exported = await config_bundle.export_bundle(db)
        exported["pains"]["свои платежи за рубеж"] = {
            "anchors": ["заплатить за границу"],
            "prototypes": ["совершенно новая эталонная фраза"],
        }
        fake.calls.clear()
        async with maker() as db:
            out = await config_bundle.import_bundle(db, exported,
                                                    actor="owner@local")
        async with maker() as db:
            cascade = await cascade_registry.active_cascade_version(db)
            rows = (await db.execute(select(L2Prototype).where(
                L2Prototype.cascade_version_id == cascade.id))).scalars().all()
            vectors = {r.phrase: r.vector for r in rows}
        return out, vectors

    out, vectors = fresh(scenario)

    assert out["unchanged"] == ["business", "l3_prompts:dm_v1",
                                "l3_prompts:public_v1"], "таксономия изменилась"
    assert fake.calls == [["совершенно новая эталонная фраза"]], \
        "пересчитывается только новая боль, а не весь набор"
    assert vectors["банк завернул платёж, требует контракт на учёт"] == [0.1, 0.2, 0.3]
    assert vectors["совершенно новая эталонная фраза"] == [0.1, 0.2, 0.3]


def test_a_new_anchor_without_phrase_changes_calls_no_embedder(monkeypatch):
    """Новый якорь L1 у существующей боли — новая версия таксономии, но эталонные
    фразы той же боли не изменились: эмбеддеру здесь делать нечего."""
    fake = FakeEmbedder().install(monkeypatch)

    async def scenario(maker):
        async with maker() as db:
            await config_bundle.import_bundle(db, BUNDLE, actor="owner@local")
        before = await _snapshot(maker)
        async with maker() as db:
            exported = await config_bundle.export_bundle(db)
        exported["pains"]["банк не пропускает платеж"]["anchors"].append(
            "платёж завис")
        fake.calls.clear()
        async with maker() as db:
            out = await config_bundle.import_bundle(db, exported,
                                                    actor="owner@local")
        after = await _snapshot(maker)
        return before, after, out

    before, after, out = fresh(scenario)

    assert after["cascade_id"] != before["cascade_id"], "якоря изменились"
    assert out["unchanged"] == ["business", "l3_prompts:dm_v1",
                                "l3_prompts:public_v1"]
    assert fake.calls == [], "фразы не менялись — эмбеддер не нужен"


# ── (г) прямой вызов с экрана: тот же текст — та же версия ────────────────────

def test_save_l3_prompt_with_the_same_text_returns_the_active_version(monkeypatch):
    FakeEmbedder().install(monkeypatch)

    async def scenario(maker):
        async with maker() as db:
            await config_bundle.import_bundle(db, BUNDLE, actor="owner@local")
        async with maker() as db:
            active = await cascade_registry.active_l3_prompt(db, "dm_v1")
            # Пробелы по краям изменением не считаются: нормализация та же, что
            # у любой правки.
            same = await cascade_registry.save_l3_prompt(
                db, prompt_key="dm_v1",
                system_prompt="  " + active.system_prompt + "\n",
                actor="owner@local", activate=True)
        # Правка «после» — отдельной сессией: её UPDATE деактивирует активную
        # строку, и в памяти того же объекта это видно (synchronize_session).
        async with maker() as db:
            counts = len((await db.execute(select(L3Prompt))).scalars().all())
            changed = await cascade_registry.save_l3_prompt(
                db, prompt_key="dm_v1",
                system_prompt=active.system_prompt + " Изменили вопрос.",
                actor="owner@local", activate=True)
            counts_after_change = len(
                (await db.execute(select(L3Prompt))).scalars().all())
        return active, same, counts, changed, counts_after_change

    active, same, counts, changed, counts_after = fresh(scenario)

    assert same.id == active.id, "тот же текст — та же строка"
    assert same.version == active.version
    assert same.is_active is True
    assert counts == 2, "новой строки тот же текст не заводит"
    assert changed.id != active.id, "изменение текста обязано поднять версию"
    assert changed.version != active.version
    assert counts_after == 3


def test_save_business_description_with_the_same_text_returns_the_active_version(
        monkeypatch):
    FakeEmbedder().install(monkeypatch)

    async def scenario(maker):
        async with maker() as db:
            await config_bundle.import_bundle(db, BUNDLE, actor="owner@local")
        async with maker() as db:
            active = await cascade_registry.active_profile_version(db)
            same = await cascade_registry.save_business_description(
                db, business_description="  " + active.business_description + " ",
                actor="owner@local", activate=True)
            counts = len((await db.execute(select(ProfileVersion))).scalars().all())
            changed = await cascade_registry.save_business_description(
                db, business_description=active.business_description + " Правка.",
                actor="owner@local", activate=True)
        return active, same, counts, changed

    active, same, counts, changed = fresh(scenario)

    assert same.id == active.id
    assert same.version == active.version
    assert counts == 1
    assert changed.id != active.id


def test_save_taxonomy_with_identical_content_returns_the_active_version(
        monkeypatch):
    """Включение набора, чьё итоговое состояние совпадает с активным, не создаёт
    версию. Нормализация входит в сравнение: «Банк Отказал» — тот же якорь, что
    и «банк отказал»."""
    FakeEmbedder().install(monkeypatch)
    pains = {label: (body["anchors"], body["prototypes"])
             for label, body in BUNDLE["pains"].items()}

    async def scenario(maker):
        async with maker() as db:
            await config_bundle.import_bundle(db, BUNDLE, actor="owner@local")
        async with maker() as db:
            active = await cascade_registry.active_cascade_version(db)
            same = await cascade_registry.save_taxonomy(
                db,
                pains={"банк не пропускает платеж":
                       (["Валютный контроль", "Банк Отказал"],
                        list(BUNDLE["pains"]["банк не пропускает платеж"]["prototypes"]))},
                disqualifiers=None, noise_prototypes=None,
                actor="owner@local", activate=True)
            counts = len((await db.execute(select(CascadeVersion))).scalars().all())
        return active, same, counts

    active, same, counts = fresh(scenario)

    assert same.id == active.id, \
        "частичное обновление без изменений — та же активная версия"
    assert same.version == active.version
    assert counts == 1


def test_a_draft_with_identical_content_is_still_a_new_row(monkeypatch):
    """`activate=False` — предложение, и оно заведомо новая строка, даже текстом
    совпадая с активной версией: у черновика есть право «включить позже»."""
    FakeEmbedder().install(monkeypatch)
    pains = {label: (body["anchors"], body["prototypes"])
             for label, body in BUNDLE["pains"].items()}
    noise = {label: list(items) for label, items in BUNDLE["noise"].items()}
    disq = {label: list(items) for label, items in BUNDLE["disqualifiers"].items()}

    async def scenario(maker):
        async with maker() as db:
            await config_bundle.import_bundle(db, BUNDLE, actor="owner@local")
        async with maker() as db:
            active_cascade = await cascade_registry.active_cascade_version(db)
            draft_cascade = await cascade_registry.save_taxonomy(
                db, pains=pains, disqualifiers=disq, noise_prototypes=noise,
                actor="customer@local", activate=False, replace=True)
            active_profile = await cascade_registry.active_profile_version(db)
            draft_profile = await cascade_registry.save_business_description(
                db, business_description=BUNDLE["business"]["description"],
                actor="customer@local", activate=False)
            active_prompt = await cascade_registry.active_l3_prompt(db, "dm_v1")
            draft_prompt = await cascade_registry.save_l3_prompt(
                db, prompt_key="dm_v1",
                system_prompt=BUNDLE["l3_prompts"]["dm_v1"],
                actor="customer@local", activate=False)
            still_active = await cascade_registry.active_cascade_version(db)
        return (active_cascade, draft_cascade, active_profile, draft_profile,
                active_prompt, draft_prompt, still_active)

    (active_cascade, draft_cascade, active_profile, draft_profile,
     active_prompt, draft_prompt, still_active) = fresh(scenario)

    assert draft_cascade.id != active_cascade.id
    assert draft_cascade.is_active is False
    assert draft_profile.id != active_profile.id
    assert draft_profile.is_active is False
    assert draft_prompt.id != active_prompt.id
    assert draft_prompt.is_active is False
    assert still_active.id == active_cascade.id, "черновик не включается сам"


# ── (д) экран: действующие пороги, включая l1_bypass_pos_min ──────────────────

BASE = "/api/v1"


async def _seed():
    engine = create_async_engine(DB_URL, poolclass=None)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        db.add(User(email="owner@local", name="owner", initials="OW",
                    role="owner", password_hash="!нельзя-войти",
                    totp_secret="X" * 32, totp_confirmed=True, is_active=True))
        # Строка `limits` на проде: 0.57 у bypass, у отрыва — не значение кода.
        db.add_all([Limit(key="l1_bypass_pos_min", value=0.57, unit="косинус"),
                    Limit(key="l2_min_margin", value=0.05, unit="косинус")])
        await db.commit()
    await engine.dispose()


async def _set_bypass_limit(value: float) -> None:
    engine = create_async_engine(DB_URL, poolclass=None)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        row = await db.get(Limit, "l1_bypass_pos_min")
        row.value = value
        await db.commit()
    await engine.dispose()


@pytest.fixture
def client():
    asyncio.run(_seed())
    previous = os.environ.get("RADAR_DATABASE_URL")
    os.environ["RADAR_DATABASE_URL"] = DB_URL
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_session_maker.cache_clear()

    app = create_app()
    with TestClient(app, raise_server_exceptions=False) as c:
        c.login = lambda email: c.cookies.set(  # type: ignore[attr-defined]
            get_settings().SESSION_COOKIE,
            SessionSigner(get_settings().SECRET_KEY).dumps(
                {"uid": 1, "totp_ok": True}))
        c.login("owner@local")  # type: ignore[attr-defined]
        yield c

    if previous is None:
        os.environ.pop("RADAR_DATABASE_URL", None)
    else:
        os.environ["RADAR_DATABASE_URL"] = previous
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_session_maker.cache_clear()


def test_profile_screen_shows_effective_thresholds_from_limits(client):
    """(14.8.12b) Действующий `l1_bypass_pos_min` виден на экране, оба порога —
    числа, и строка `limits` побеждает значение кода (0.57 у bypass — и значение
    кода тоже, поэтому правим строку и проверяем, что экран переезжает за ней)."""
    body = client.get(f"{BASE}/profile").json()
    cascade = body["cascade"]

    assert cascade["l1_bypass_pos_min"] == 0.57
    assert isinstance(cascade["l1_bypass_pos_min"], float), "число, не строка"
    assert cascade["l2_min_margin"] == 0.05, "действующее значение, не код"
    assert isinstance(cascade["l2_min_margin"], float)

    asyncio.run(_set_bypass_limit(0.6))
    body = client.get(f"{BASE}/profile").json()
    assert body["cascade"]["l1_bypass_pos_min"] == 0.6, \
        "строка `limits` побеждает значение кода — без рестарта"


def test_upload_reports_unchanged_parts_in_its_response(client, monkeypatch):
    """Ручка загрузки отдаёт тот же отчёт импорта, включая `unchanged`: повторная
    загрузка того же файла по HTTP отвечает «ничего не поменял», а не бампом
    версий."""
    FakeEmbedder().install(monkeypatch)

    def upload():
        return client.post(f"{BASE}/profile/config/files", json=BUNDLE)

    first = upload()
    assert first.status_code == 201, first.text
    assert isinstance(first.json()["applied"]["unchanged"], list)

    second = upload()
    assert second.status_code == 201, second.text
    assert second.json()["applied"]["unchanged"] == ALL_UNCHANGED
