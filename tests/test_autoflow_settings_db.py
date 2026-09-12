"""Настройки автоматики (волна А): умолчания, сидирование, круг набора настроек.

Выключатели автоматики по умолчанию 0 — выкатка не меняет поведение ни одного
сценария, включение — осознанное действие владельца строкой `limits`. Отсюда
три опоры, которые здесь проверяются:

* `DEFAULTS` — дословный словарь умолчаний; расхождение с ним означало бы,
  что «выкатка ничего не меняет» перестало быть правдой;
* `ensure_bootstrap` сеет отсутствующие строки (существующие не трогает — там
  может стоять правка владельца) — иначе включать без выкатки было бы некуда;
* набор настроек возит блок `automation` по той же доктрине круга, что и
  пороги: выгруженный файл грузится обратно без правок руками, а старая
  выгрузка без блока импортируется без ошибки и честно сообщает пустым
  списком в ответе, что строки не тронуты.

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
                           L3Prompt, Limit, ProfileVersion)
from app.services import (autoflow, cascade_registry,  # noqa: E402
                          config_bundle, embeddings)

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
    },
    "noise": {
        "офтоп": ["всем привет, как дела"],
    },
    "disqualifiers": {
        "вакансия": ["вакансия", "резюме"],
    },
    "l3_prompts": {"dm_v1": "Ты — фильтр сообщений. Ответь JSON."},
}


class FakeEmbedder:
    """Эмбеддер — внешний HTTP-сервис, и только он здесь подменяется
    (образец — tests/test_config_bundle_db.py)."""

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
    """Тест целиком в ОДНОМ цикле событий: соединение asyncpg привязано к циклу,
    в котором создано (образец — tests/test_config_bundle_db.py)."""
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


def test_automation_switches_default_off_and_seeded():
    """T-01: выключатели по умолчанию 0; строки сеются и не дублируются;
    строка в БД побеждает умолчание, удаление строки возвращает его."""
    assert autoflow.DEFAULTS == {
        "autoflow_join_backfill_enabled": 0,
        "autoflow_backfill_depth_days": 30,
        "autoflow_backfill_target": 2000,
        "autoflow_reclassify_enabled": 0,
        "autoflow_reclassify_interval_min": 60,
        "autoflow_reclassify_l3_limit": 200,
        "autoflow_reclassify_batch_channels": 10,
        "discovery_autoscan_enabled": 0,
    }

    async def scenario(maker):
        # Пустая таблица — рабочее состояние: все 9 умолчаний.
        async with maker() as db:
            empty = await autoflow.thresholds(db)
            # Строка в БД побеждает умолчание.
            db.add(Limit(key="autoflow_reclassify_interval_min", value=30))
            await db.commit()
            overridden = await autoflow.thresholds(db)
            # Удаление строки возвращает умолчание.
            await db.execute(text(
                "DELETE FROM limits WHERE key = 'autoflow_reclassify_interval_min'"))
            await db.commit()
            restored = await autoflow.thresholds(db)
        # Чистая схема: сидирование заводит по одной строке на каждый ключ.
        async with maker() as db:
            created = await cascade_registry.ensure_bootstrap(db)
            rows = {r.key: r for r in (await db.execute(select(Limit))).scalars().all()
                    if r.key in autoflow.AUTOMATION_LIMIT_KEYS}
            # Повторный вызов дублей не плодит и существующие строки не трогает.
            await cascade_registry.ensure_bootstrap(db)
            rows_after = [r.key for r in
                          (await db.execute(select(Limit))).scalars().all()
                          if r.key in autoflow.AUTOMATION_LIMIT_KEYS]
        return empty, overridden, restored, created, rows, rows_after

    empty, overridden, restored, created, rows, rows_after = fresh(scenario)
    for key in autoflow.AUTOMATION_LIMIT_KEYS:
        spec = autoflow.LIMIT_SPECS[key]
        assert empty[key] == spec["default"], key
        assert restored[key] == spec["default"], key
    assert overridden["autoflow_reclassify_interval_min"] == 30
    assert created is True
    assert len(rows) == len(autoflow.AUTOMATION_LIMIT_KEYS) == 9
    for key, row in rows.items():
        spec = autoflow.LIMIT_SPECS[key]
        assert float(row.value) == spec["default"], key
        assert row.unit == spec["unit"], key
        assert row.description == spec["description"], key
    assert sorted(rows_after) == sorted(rows)


def test_config_bundle_automation_block(monkeypatch):
    """T-08: блок `automation` в наборе настроек — экспорт, импорт, старый файл,
    отказ по чужому ключу до первой записи."""
    FakeEmbedder().install(monkeypatch)

    async def scenario(maker):
        out = {}
        async with maker() as db:
            await cascade_registry.ensure_bootstrap(db)
            exported = await config_bundle.export_bundle(db)
        out["exported"] = exported["automation"]

        # Импорт с блоком: строка записана, ответ называет записанный ключ.
        with_block = {**BUNDLE, "automation": {"autoflow_reclassify_enabled": 1}}
        async with maker() as db:
            applied = await config_bundle.import_bundle(db, with_block,
                                                        actor="owner@local")
            row = await db.get(Limit, "autoflow_reclassify_enabled")
        out["applied"] = applied["automation"]
        out["row_value"] = float(row.value) if row else None

        # Старая выгрузка (та же с вырезанным блоком): без ошибки, строки не
        # изменились, в ответе пустой список.
        without_block = {k: v for k, v in with_block.items() if k != "automation"}
        async with maker() as db:
            applied_old = await config_bundle.import_bundle(db, without_block,
                                                            actor="owner@local")
            row_old = await db.get(Limit, "autoflow_reclassify_enabled")
        out["applied_old"] = applied_old["automation"]
        out["row_value_old"] = float(row_old.value) if row_old else None

        # Чужой ключ в блоке — BundleError до первой записи: сравниваем снимок
        # таблиц до и после отказа.
        async with maker() as db:
            before = {
                "limits": sorted((await db.execute(select(Limit.key))).scalars().all()),
                "versions": len((await db.execute(select(CascadeVersion))).scalars().all()),
                "profiles": len((await db.execute(select(ProfileVersion))).scalars().all()),
                "prototypes": len((await db.execute(select(L2Prototype))).scalars().all()),
                "prompts": len((await db.execute(select(L3Prompt))).scalars().all()),
            }
        bogus = {**BUNDLE, "automation": {"bogus": 1}}
        async with maker() as db:
            refused = False
            try:
                await config_bundle.import_bundle(db, bogus, actor="owner@local")
            except config_bundle.BundleError:
                refused = True
            after = {
                "limits": sorted((await db.execute(select(Limit.key))).scalars().all()),
                "versions": len((await db.execute(select(CascadeVersion))).scalars().all()),
                "profiles": len((await db.execute(select(ProfileVersion))).scalars().all()),
                "prototypes": len((await db.execute(select(L2Prototype))).scalars().all()),
                "prompts": len((await db.execute(select(L3Prompt))).scalars().all()),
            }
        out["refused"] = refused
        out["before_refusal"] = before
        out["after_refusal"] = after
        return out

    out = fresh(scenario)
    assert out["exported"] == {key: autoflow.LIMIT_SPECS[key]["default"]
                               for key in autoflow.AUTOMATION_LIMIT_KEYS}
    assert out["applied"] == ["autoflow_reclassify_enabled"]
    assert out["row_value"] == 1.0
    # Старый файл: без ошибки, строки не тронуты (включение осталось включением).
    assert out["applied_old"] == []
    assert out["row_value_old"] == 1.0
    # Отказ до первой записи: не изменилось ничего.
    assert out["refused"] is True
    assert out["after_refusal"] == out["before_refusal"]


def test_status_on_empty_database_has_all_scenarios():
    """Сводка на пустой базе (свежий инстанс: ни одного прогона, ни одной
    строки limits) — все четыре сценария, выключатели false, last_run None.
    Ловит `NoResultFound` на выборках «последний прогон» без строк."""
    async def scenario(maker):
        async with maker() as db:
            return await autoflow.status(db)

    out = fresh(scenario)
    assert set(out["settings"]) == set(autoflow.AUTOMATION_LIMIT_KEYS)
    assert set(out["scenarios"]) == {"join_backfill", "reclassify", "autoscan",
                                     "autoapprove"}
    for name, sc in out["scenarios"].items():
        assert sc["enabled"] is False, name
        assert sc["last_run"] is None, name
        assert sc["last_error"] is None, name
    assert out["scenarios"]["reclassify"]["next_at"] is None
    assert out["scenarios"]["reclassify"]["waiting_messages"] == 0
    assert out["scenarios"]["autoscan"]["next_at"] is not None
    assert out["scenarios"]["autoapprove"]["approved_waiting"] == 0
    assert out["scenarios"]["autoapprove"]["auto_joins_today"] == 0
