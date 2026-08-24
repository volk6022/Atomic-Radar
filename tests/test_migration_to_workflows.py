"""Перенос накопленных данных в многопоточную схему — на настоящем Postgres.

Почему не на моках. Скрипт переноса трогает боевые данные, а отката одной командой
у Radar нет: Alembic здесь не заведён. Проверять такое подделками бессмысленно —
половина рисков живёт ровно в том, что база примет, а что отвергнет: CHECK адресации,
уникальность пары, NOT NULL на скоре.

База берётся из `RADAR_TEST_DATABASE_URL`. Если переменной нет, тесты пропускаются:
падать на машине без Postgres было бы наказанием за чужие обстоятельства.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db.models import (Base, Channel, Draft, EngageInstance, Lead, Message,
                           WfDraft, WfTarget, WfVerdict, Workflow)
from scripts import migrate_to_workflows as mig

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — тестам переноса нужен Postgres")

NOW = datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
async def db():
    """Чистая схема на каждый тест: перенос идемпотентен, и проверять это надо
    с известного состояния, а не с того, что осталось от соседа."""
    engine = create_async_engine(DB_URL, poolclass=None)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session:
        yield session
    await engine.dispose()


async def seed_old_schema(db, *, leads=3, drafts=2):
    """Данные в том виде, в каком они лежат сегодня: вердикт внутри `messages`,
    лид на сообщение, черновик на лид."""
    db.add(EngageInstance(key="default", client_label="Основной",
                          base_url="http://engage:8103", api_key_env="RADAR_ENGAGE_API_KEY"))
    channel = Channel(peer_id=-1001, username="ch", title="Канал")
    db.add(channel)
    await db.flush()

    messages = []
    for i in range(leads + 2):  # +2 сообщения, которые лидами не стали
        m = Message(
            channel_id=channel.id, tg_message_id=1000 + i,
            tg_date=NOW - timedelta(hours=i),
            author_peer_id=500 + i, author_username=f"user{i}", author_name=f"Имя {i}",
            author_is_bot=False, is_automatic_forward=False,
            text=f"нужен админ настроить vps, не работает {i}",
            cascade_level=1, cascade_passed=(i < leads),
            cascade_detail={"l0": "ок", "l1": "совпало"},
            processed_at=NOW,
        )
        messages.append(m)
        db.add(m)
    # Сообщение, которого каскад не касался вовсе: у него не должно появиться вердикта.
    db.add(Message(channel_id=channel.id, tg_message_id=9999, tg_date=NOW,
                   author_peer_id=999, author_is_bot=False,
                   is_automatic_forward=False, text="каскад сюда не доходил"))
    await db.flush()

    made = []
    for i in range(leads):
        lead = Lead(message_id=messages[i].id, channel_id=channel.id,
                    author_peer_id=messages[i].author_peer_id,
                    author_username=messages[i].author_username,
                    author_name=messages[i].author_name,
                    pain="нужен админ/подрядчик", quote="цитата",
                    score=40 + i, score_breakdown=[{"label": "боль", "value": 22}],
                    status="new")
        db.add(lead)
        made.append(lead)
    await db.flush()

    for i in range(drafts):
        db.add(Draft(lead_id=made[i].id, variants=[{"text": f"вариант {i}"}],
                     state="pending", prompt_version="template-v0"))
    await db.commit()
    return channel, messages, made


async def test_dry_run_writes_nothing(db):
    await seed_old_schema(db)
    wf = await mig._target_workflow(db, apply=True)

    moved, total = await mig.migrate_verdicts(db, wf, apply=False)
    assert moved > 0 and total > 0
    assert (await db.execute(select(func.count()).select_from(WfVerdict))).scalar_one() == 0

    moved, total = await mig.migrate_targets(db, wf, apply=False)
    assert moved == 3
    assert (await db.execute(select(func.count()).select_from(WfTarget))).scalar_one() == 0


async def test_verdicts_move_only_for_processed_messages(db):
    """«Нет строки вердикта» и «вердикт есть, но пустой» означают разное. Сообщение,
    которого каскад не касался, вердикта получить не должно — иначе переклассификация
    решит, что считать нечего."""
    await seed_old_schema(db)
    wf = await mig._target_workflow(db, apply=True)
    await mig.migrate_verdicts(db, wf, apply=True)

    total_messages = (await db.execute(select(func.count()).select_from(Message))).scalar_one()
    verdicts = (await db.execute(select(func.count()).select_from(WfVerdict))).scalar_one()
    assert total_messages == 6      # 5 обработанных + 1 нетронутое
    assert verdicts == 5

    untouched = (await db.execute(
        select(Message).where(Message.tg_message_id == 9999))).scalar_one()
    got = (await db.execute(
        select(WfVerdict).where(WfVerdict.message_id == untouched.id))).scalar_one_or_none()
    assert got is None


async def test_verdict_keeps_three_valued_passed(db):
    """`passed=None` — «ещё в пути», а не «не прошло». Схлопнуть его в False значило бы
    молча потерять лиды, которые просто не досчитали."""
    channel, messages, _ = await seed_old_schema(db)
    messages[0].cascade_passed = None
    await db.commit()

    wf = await mig._target_workflow(db, apply=True)
    await mig.migrate_verdicts(db, wf, apply=True)

    v = (await db.execute(
        select(WfVerdict).where(WfVerdict.message_id == messages[0].id))).scalar_one()
    assert v.passed is None


async def test_leads_become_addressable_targets(db):
    await seed_old_schema(db)
    wf = await mig._target_workflow(db, apply=True)
    await mig.migrate_verdicts(db, wf, apply=True)
    moved, total = await mig.migrate_targets(db, wf, apply=True)
    assert (moved, total) == (3, 3)

    targets = (await db.execute(select(WfTarget))).scalars().all()
    assert len(targets) == 3
    for t in targets:
        assert t.workflow_id == wf.id
        assert t.target_kind == "user"
        # Адресация ЛС: получатель — автор лида. Без неё CHECK бы не пропустил строку.
        assert t.recipient_peer_id == t.author_peer_id
        assert t.chat_peer_id is None and t.reply_to_message_id is None


async def test_drafts_relink_through_message_not_old_id(db):
    """Идентификаторы целей новые. Связь строится через сообщение, и если это
    сломается, черновики молча привяжутся не к тем целям."""
    _, messages, leads = await seed_old_schema(db, leads=3, drafts=2)
    wf = await mig._target_workflow(db, apply=True)
    await mig.migrate_verdicts(db, wf, apply=True)
    await mig.migrate_targets(db, wf, apply=True)
    moved, total, orphaned = await mig.migrate_drafts(db, wf, apply=True)
    assert (moved, total, orphaned) == (2, 2, 0)

    for old in (await db.execute(select(Draft))).scalars().all():
        lead = (await db.execute(select(Lead).where(Lead.id == old.lead_id))).scalar_one()
        target = (await db.execute(
            select(WfTarget).where(WfTarget.message_id == lead.message_id))).scalar_one()
        new = (await db.execute(
            select(WfDraft).where(WfDraft.target_id == target.id))).scalar_one()
        assert new.variants == old.variants
        assert new.state == old.state


async def test_lead_with_a_draft_cannot_be_deleted(db):
    """Внешний ключ `drafts.lead_id` объявлен без `ondelete`, то есть запрещает
    удаление лида, у которого есть черновик.

    Для переноса это хорошая новость: осиротевших черновиков в здоровой базе быть не
    может, и ветка их подсчёта в скрипте — страховка от ручной правки базы, а не
    рабочий путь.

    Но у того же факта есть вторая сторона, и она уже не безобидна:
    `reclassify._reconcile_leads` делает `db.delete(lead)` для лида в статусе `new`,
    переставшего проходить каскад (`app/services/reclassify.py:214-215`). Лид с
    черновиком — это норма, а не редкость: `ensure_draft` заводит черновик как раз
    новым лидам. Значит такой прогон падает с нарушением внешнего ключа и откатывает
    всю переклассификацию целиком.

    Тест фиксирует само ограничение. Починка поведения `reclassify` — отдельное
    решение: см. отчёт, там же варианты.
    """
    _, _, leads = await seed_old_schema(db, leads=3, drafts=2)

    with pytest.raises(IntegrityError):
        await db.delete(leads[0])
        await db.flush()
    await db.rollback()


async def test_running_twice_changes_nothing(db):
    """Прерванный на середине перенос дозапускается тем же вызовом. Если это неверно,
    повторный запуск удвоит данные — и заметят это нескоро."""
    await seed_old_schema(db)
    wf = await mig._target_workflow(db, apply=True)

    await mig.migrate_verdicts(db, wf, apply=True)
    await mig.migrate_targets(db, wf, apply=True)
    await mig.migrate_drafts(db, wf, apply=True)
    first = [
        (await db.execute(select(func.count()).select_from(WfVerdict))).scalar_one(),
        (await db.execute(select(func.count()).select_from(WfTarget))).scalar_one(),
        (await db.execute(select(func.count()).select_from(WfDraft))).scalar_one(),
    ]

    assert (await mig.migrate_verdicts(db, wf, apply=True))[0] == 0
    assert (await mig.migrate_targets(db, wf, apply=True))[0] == 0
    assert (await mig.migrate_drafts(db, wf, apply=True))[0] == 0

    second = [
        (await db.execute(select(func.count()).select_from(WfVerdict))).scalar_one(),
        (await db.execute(select(func.count()).select_from(WfTarget))).scalar_one(),
        (await db.execute(select(func.count()).select_from(WfDraft))).scalar_one(),
    ]
    assert first == second


async def test_old_tables_survive_the_migration(db):
    """Единственная страховка на случай неверного переноса — что старое цело.
    Удаление старых таблиц должно быть отдельным осознанным шагом."""
    await seed_old_schema(db)
    wf = await mig._target_workflow(db, apply=True)
    await mig.migrate_verdicts(db, wf, apply=True)
    await mig.migrate_targets(db, wf, apply=True)
    await mig.migrate_drafts(db, wf, apply=True)

    assert (await db.execute(select(func.count()).select_from(Lead))).scalar_one() == 3
    assert (await db.execute(select(func.count()).select_from(Draft))).scalar_one() == 2
    still = (await db.execute(
        select(Message).where(Message.cascade_level.isnot(None)))).scalars().all()
    assert len(still) == 5


async def test_bootstrap_creates_instance_and_workflow(db):
    """Выкатка не должна требовать ручного шага: сценарий ЛС существовал молча,
    и накопленным данным надо к чему-то привязаться."""
    wf = await mig._target_workflow(db, apply=True)
    assert wf.key == "cold_dm"
    assert (wf.target_kind, wf.action, wf.visibility) == ("user", "dm", "private")

    instances = (await db.execute(select(EngageInstance))).scalars().all()
    assert len(instances) == 1

    # Повторный вызов не плодит вторую строку.
    again = await mig._target_workflow(db, apply=True)
    assert again.id == wf.id
    assert (await db.execute(select(func.count()).select_from(Workflow))).scalar_one() == 1
