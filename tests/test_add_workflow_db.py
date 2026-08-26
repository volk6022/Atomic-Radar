"""`scripts/add_workflow.py` — добавление сценария `public_reply` на настоящем Postgres.

Почему не на моках. Скрипт трогает боевые данные, отката одной командой у Radar нет
(Alembic здесь не заведён) — половина риска в том, что валидация действительно
отвергнет то, что должна, и что запись действительно ничего не пишет в сухом прогоне.
Подделками это не проверить.

База берётся из `RADAR_TEST_DATABASE_URL`. Если переменной нет, тесты пропускаются:
падать на машине без Postgres было бы наказанием за чужие обстоятельства.

⚠️ Не путать с `radar_wf_test` — это отдельный живой стенд, эти тесты его не касаются.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db.models import Base, EngageInstance, Workflow
from app.services import workflows
from scripts import add_workflow as aw

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

NOW = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
async def db():
    """Чистая схема на каждый тест: скрипт идемпотентен, и проверять это надо с
    известного состояния, а не с того, что осталось от соседнего теста."""
    engine = create_async_engine(DB_URL, poolclass=None)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session:
        yield session
    await engine.dispose()


async def seed_cold_dm(db) -> EngageInstance:
    """Установка, на которой скрипт и должен работать: `cold_dm` уже существует,
    и `ensure_bootstrap` на ней больше ничего не сделает — реестр не пуст."""
    instance = EngageInstance(key="default", client_label="Основной",
                              base_url="http://engage:8103",
                              api_key_env="RADAR_ENGAGE_API_KEY")
    db.add(instance)
    await db.flush()
    db.add(Workflow(key="cold_dm", title="Личные сообщения", target_kind="user",
                    action="dm", visibility="private", engage_instance_id=instance.id,
                    engage_use_case="cold_dm", cascade_profile="dm_v1", sort_order=10,
                    is_active=True))
    await db.commit()
    return instance


async def _count_workflows(db) -> int:
    return (await db.execute(select(func.count()).select_from(Workflow))).scalar_one()


async def test_dry_run_writes_nothing(db):
    await seed_cold_dm(db)

    result = await aw.apply_workflow(db, is_active=False, apply=False)

    assert result.written is False
    assert result.already_existed is False
    assert result.problems == []
    assert result.workflow.key == aw.KEY
    assert await _count_workflows(db) == 1  # только cold_dm, засеянный выше


async def test_apply_creates_expected_row_inactive_by_default(db):
    await seed_cold_dm(db)

    result = await aw.apply_workflow(db, is_active=False, apply=True)

    assert result.written is True
    row = await workflows.by_key(db, aw.KEY)
    assert row is not None
    assert (row.target_kind, row.action, row.visibility) == ("message", "reply", "public")
    assert row.cascade_profile == "public_v1"
    assert row.sort_order == 20
    assert row.is_active is False  # по умолчанию — выключен, включение отдельным флагом
    assert await _count_workflows(db) == 2


async def test_enable_flag_creates_active_row(db):
    await seed_cold_dm(db)

    result = await aw.apply_workflow(db, is_active=True, apply=True)

    assert result.written is True
    assert result.workflow.is_active is True


async def test_running_twice_does_not_duplicate_or_fail(db):
    await seed_cold_dm(db)
    first = await aw.apply_workflow(db, is_active=False, apply=True)
    assert first.written is True

    second = await aw.apply_workflow(db, is_active=False, apply=True)

    assert second.already_existed is True
    assert second.written is False
    assert second.workflow.id == first.workflow.id
    assert await _count_workflows(db) == 2  # cold_dm + public_reply, не три


async def test_second_dry_run_after_apply_reports_existing_without_writing(db):
    await seed_cold_dm(db)
    await aw.apply_workflow(db, is_active=False, apply=True)

    result = await aw.apply_workflow(db, is_active=True, apply=False)

    # Существующая строка не переоткрывается сухим прогоном с другим is_active —
    # идемпотентность значит «менять нечего», а не «пересчитать заново».
    assert result.already_existed is True
    assert result.workflow.is_active is False
    assert await _count_workflows(db) == 2


async def test_incompatible_axes_are_rejected_even_with_apply(db, monkeypatch):
    """Сломанная тройка осей (`action='dm'` требует `target_kind='user'`, а не
    `'message'`) обязана быть отвергнута `workflows.validate()` — и не попасть в базу
    даже при `apply=True`."""
    await seed_cold_dm(db)
    broken = {**aw.SPEC, "action": "dm"}
    monkeypatch.setattr(aw, "SPEC", broken)

    result = await aw.apply_workflow(db, is_active=False, apply=True)

    assert result.written is False
    assert result.problems  # непустой список причин
    assert any("target_kind" in p for p in result.problems)
    assert await _count_workflows(db) == 1  # только cold_dm — мусор не попал


async def test_refuses_without_an_engage_instance(db):
    """Реестр инстансов пуст (даже `cold_dm` в такой базе быть не может — у него тот
    же NOT NULL FK) — скрипт обязан отказаться, а не писать с чем попало."""
    with pytest.raises(aw.NoEngageInstanceError):
        await aw.apply_workflow(db, is_active=False, apply=True)

    assert await _count_workflows(db) == 0
