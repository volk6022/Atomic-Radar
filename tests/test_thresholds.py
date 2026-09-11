"""Пороги каскада в `limits` и в наборе настроек: круг экспорт → импорт, перечитка.

Контракт каскада §3/§3.1: пороги `l2_min_margin` и `l1_bypass_pos_min` — строки
таблицы `limits` со значениями кода по умолчанию; в наборе настроек они едут
необязательным блоком `thresholds` («ключ → число»). Круг обязан замыкаться:
выгруженный с изменённым порогом набор, загруженный на чистую базу, даёт то же
значение порога — и перечитка его применила. Импорт **без** блока пороги не
трогает — и это видно (поле `thresholds` в ответе импорта и в логе), а не молча.

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os

import pytest
from sqlalchemy import delete, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_DEBUG", "true")

from app.core import cascade  # noqa: E402
from app.db.models import Base, CascadeVersion, Limit  # noqa: E402
from app.services import cascade_registry, config_bundle, embeddings  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")


BUNDLE = {
    "format": "atomic-radar-config",
    "version": 1,
    "name": "курс-тест",
    "business": {"description": "КУРС — оплата счетов зарубежных поставщиков."},
    "pains": {
        "банк не пропускает платеж": {
            "anchors": ["валютный контроль", "банк отказал"],
            "prototypes": ["банк завернул платёж, требует контракт на учёт",
                           "комплаенс не пропускает перевод"],
        },
    },
    "noise": {"офтоп": ["всем привет, как дела"]},
    "disqualifiers": {"вакансия": ["вакансия", "резюме"]},
    "l3_prompts": {"dm_v1": "Ты — фильтр сообщений. Ответь JSON."},
}

THRESHOLDS = {"l2_min_margin": 0.05, "l1_bypass_pos_min": 0.6}


class FakeEmbedder:
    """Эмбеддер — внешний HTTP-сервис, и только он здесь подменяется."""

    def install(self, monkeypatch, *, available=True):
        monkeypatch.setattr(embeddings, "enabled", lambda: available)

        async def fake(phrases):
            return [[0.1, 0.2, 0.3] for _ in phrases]

        monkeypatch.setattr(embeddings, "embed", fake)
        return self


def fresh(fn):
    """Тест целиком в ОДНОМ цикле событий: соединение asyncpg привязано к циклу
    (см. тот же помощник в `test_config_bundle_db.py`)."""
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
    """Тесты правят действующие пороги в памяти процесса; после теста — значения
    кода, чтобы соседний файл не начался на чужом пороге (conftest эти значения
    не снимает: в зелёном наборе их меняет только этот файл)."""
    yield
    cascade_registry.apply_thresholds(cascade_registry.threshold_defaults())


def _imported_log(caplog) -> str:
    records = [r for r in caplog.records
               if r.name == "app.services.config_bundle"
               and r.getMessage().startswith("config_bundle_imported")]
    assert records, "импорт обязан писать в журнал строку config_bundle_imported"
    return records[-1].getMessage()


# ── выгрузка ──────────────────────────────────────────────────────────────────

def test_export_always_carries_both_thresholds_even_on_an_empty_base():
    """Отсутствующая строка `limits` — действующее значение кода, а не дырка в
    файле: круг обязан замыкаться и на пустой базе."""

    async def scenario(maker):
        async with maker() as db:
            return await config_bundle.export_bundle(db)

    out = fresh(scenario)
    assert out["thresholds"] == {"l2_min_margin": 0.01, "l1_bypass_pos_min": 0.57}
    # В JSON значение порога — число, а не Decimal: файл правят снаружи.
    json.dumps(out["thresholds"])
    assert all(isinstance(v, float) for v in out["thresholds"].values())


# ── круг: экспорт → импорт → порог совпал (обязательный тест ревью) ──────────

def test_round_trip_carries_the_threshold_into_a_clean_base(monkeypatch, caplog):
    """Набор, выгруженный с изменённым порогом и загруженный на чистую базу,
    даёт то же значение порога — и перечитка его применила: профиль и переменная
    модуля каскада читают новое значение без рестарта."""
    FakeEmbedder().install(monkeypatch)
    with caplog.at_level(logging.INFO, logger="app.services.config_bundle"):

        async def source(maker):
            async with maker() as db:
                await config_bundle.import_bundle(
                    db, {**BUNDLE, "thresholds": THRESHOLDS}, actor="owner@local")
            async with maker() as db:
                return await config_bundle.export_bundle(db)

        exported = fresh(source)
    assert exported["thresholds"] == THRESHOLDS
    assert "thresholds=l1_bypass_pos_min,l2_min_margin" in _imported_log(caplog), \
        "записанные из файла пороги обязаны быть видны в журнале"

    with caplog.at_level(logging.INFO, logger="app.services.config_bundle"):

        async def target(maker):
            async with maker() as db:
                out = await config_bundle.import_bundle(db, exported,
                                                        actor="owner@local")
            async with maker() as db:
                rows = (await db.execute(
                    select(Limit).where(
                        Limit.key.in_(cascade_registry.THRESHOLD_LIMIT_KEYS))
                )).scalars().all()
                return out, {(r.key): float(r.value) for r in rows}

        out, rows = fresh(target)

    assert out["thresholds"] == ["l1_bypass_pos_min", "l2_min_margin"], \
        "в ответе импорта видно, какие пороги записаны из файла"
    assert rows == THRESHOLDS, "строки `limits` на чистой базе — как в файле"
    # Перечитка в конце импорта применила записанное — это и есть «без рестарта».
    assert cascade.PROFILES["dm_v1"].l2_min_margin == 0.05
    assert cascade.PROFILES["public_v1"].l2_min_margin == 0.05
    assert cascade.L1_BYPASS_POS_MIN == 0.6


# ── импорт без блока: пороги не тронуты, и это видно ─────────────────────────

def test_import_without_the_thresholds_block_leaves_them_untouched_and_says_so(
        monkeypatch, caplog):
    """Старый файл без блока `thresholds` грузится без ошибки; действующие пороги
    остаются как были, а в ответе и в журнале стоит пустой список — «не менялись»,
    а не молчание."""
    FakeEmbedder().install(monkeypatch)

    async def scenario(maker):
        async with maker() as db:
            await config_bundle.import_bundle(db, {**BUNDLE, "thresholds": THRESHOLDS},
                                              actor="owner@local")
        async with maker() as db:
            out = await config_bundle.import_bundle(db, BUNDLE, actor="owner@local")
        async with maker() as db:
            values = await cascade_registry.read_thresholds(db)
        return out, values

    with caplog.at_level(logging.INFO, logger="app.services.config_bundle"):
        out, values = fresh(scenario)

    assert out["thresholds"] == [], "пустой список читается однозначно: не менялись"
    assert values == THRESHOLDS, "строки `limits` остались как были"
    # Перечитка после импорта без блока не откатила порог к значению кода:
    # строка на месте — она и действует.
    assert cascade.PROFILES["dm_v1"].l2_min_margin == 0.05
    assert cascade.L1_BYPASS_POS_MIN == 0.6
    assert "thresholds= by=" in _imported_log(caplog)


# ── кривой блок: ошибка до первой записи ──────────────────────────────────────

@pytest.mark.parametrize("broken", [
    {"l2_min_margin": 1.5},               # вне 0 < v < 1
    {"pos_min": 0.57},                    # чужой ключ
    {"l1_bypass_pos_min": "0.57"},        # не число
])
def test_a_broken_thresholds_block_is_refused_before_anything_is_written(
        monkeypatch, broken):
    """Кривой блок — ошибка валидации до первой записи: ни одна часть файла
    (ни таксономия, ни промпты, ни пороги) не применена."""
    FakeEmbedder().install(monkeypatch)

    async def scenario(maker):
        async with maker() as db:
            with pytest.raises(config_bundle.BundleError):
                await config_bundle.import_bundle(db, {**BUNDLE, "thresholds": broken},
                                                  actor="o@local")
        async with maker() as db:
            return (len((await db.execute(select(CascadeVersion))).scalars().all()),
                    len((await db.execute(select(Limit))).scalars().all()))

    assert fresh(scenario) == (0, 0), "запись не должна была начаться"


# ── перечитка: строка побеждает, отсутствие строки — значение кода ───────────

def test_registry_reads_thresholds_from_limits_and_code_values_return_without_rows():
    """Строка `limits` побеждает значение кода у обоих профилей и у переменной
    модуля; удаление строки возвращает значения кода — так же без выкатки, как
    правка. Кривое значение из базы обязано упасть громко."""

    async def scenario(maker):
        async with maker() as db:
            db.add_all([Limit(key="l2_min_margin", value=0.05, unit="косинус"),
                        Limit(key="l1_bypass_pos_min", value=0.60, unit="косинус")])
            await db.commit()
        async with maker() as db:
            await cascade_registry.reload(db)
        applied = (cascade.PROFILES["dm_v1"].l2_min_margin,
                   cascade.PROFILES["public_v1"].l2_min_margin,
                   cascade.L1_BYPASS_POS_MIN)

        async with maker() as db:
            await db.execute(delete(Limit).where(
                Limit.key.in_(cascade_registry.THRESHOLD_LIMIT_KEYS)))
            await db.commit()
            await cascade_registry.reload(db)
        reverted = (cascade.PROFILES["dm_v1"].l2_min_margin,
                    cascade.PROFILES["public_v1"].l2_min_margin,
                    cascade.L1_BYPASS_POS_MIN)
        return applied, reverted

    applied, reverted = fresh(scenario)
    assert applied == (0.05, 0.05, 0.60), "строка `limits` побеждает значение кода"
    assert reverted == (0.01, 0.01, 0.57), "нет строки — значение кода, не ошибка"


def test_apply_thresholds_refuses_a_broken_value_loudly():
    with pytest.raises(ValueError, match="l2_min_margin"):
        cascade_registry.apply_thresholds({"l2_min_margin": 1.5,
                                           "l1_bypass_pos_min": 0.57})
    with pytest.raises(ValueError, match="l1_bypass_pos_min"):
        cascade_registry.apply_thresholds({"l2_min_margin": 0.01,
                                           "l1_bypass_pos_min": 0})
    with pytest.raises(ValueError, match="нет значения"):
        # Частичное применение оставило бы половину порогов от старой правки.
        cascade_registry.apply_thresholds({"l2_min_margin": 0.05})


# ── сидирование старта ────────────────────────────────────────────────────────

def test_bootstrap_seeds_missing_threshold_rows_and_keeps_existing_ones():
    """На чистой схеме `ensure_bootstrap` заводит обе строки со значениями кода;
    существующие строки не трогает — там может стоять правка владельца; повторный
    вызов не плодит дубликаты."""

    async def scenario(maker):
        async with maker() as db:
            db.add(Limit(key="l2_min_margin", value=0.09, unit="косинус"))
            await db.commit()
        async with maker() as db:
            await cascade_registry.ensure_bootstrap(db)
        async with maker() as db:
            await cascade_registry.ensure_bootstrap(db)  # повторный — ничего не меняет
            rows = (await db.execute(
                select(Limit).where(
                    Limit.key.in_(cascade_registry.THRESHOLD_LIMIT_KEYS))
            )).scalars().all()
            values = await cascade_registry.read_thresholds(db)
            return [(r.key, float(r.value)) for r in rows], values

    rows, values = fresh(scenario)
    assert sorted(rows) == [("l1_bypass_pos_min", 0.57), ("l2_min_margin", 0.09)], \
        "по одной строке на ключ; существующая строка со значением 0.09 не тронута"
    assert values["l2_min_margin"] == 0.09
    assert values["l1_bypass_pos_min"] == 0.57
