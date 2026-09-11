"""Согласование очереди лидов при переклассификации — на настоящем Postgres.

Правило целиком про внешние ключи: `drafts.lead_id` объявлен `NOT NULL` и без
`ondelete`, и именно база решает, что удалить можно, а что нет. Проверять это моками
бессмысленно — они пропустят ровно то, из-за чего прогон падал.

Прежнее поведение: лид в статусе `new`, переставший проходить каскад, удалялся
безусловно. Черновик у нового лида — норма (`ensure_draft` заводит его как раз новым),
поэтому `reclassify --scope all` падал нарушением ключа на первом же таком лиде и
откатывал весь прогон целиком.

Здесь же — частичный прогон по списку каналов (T12, T13 из `_TESTS-cascade.md`):
задан список — прогон только создаёт лиды, перезапись оценок и удаление
пропускаются, и пропущенное видно в сводке.

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db.models import Base, Channel, Draft, Lead, Message
from app.services import embeddings, llm, reclassify

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — правило живёт во внешних ключах")

NOW = datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
async def db():
    engine = create_async_engine(DB_URL, poolclass=None)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session:
        yield session
    await engine.dispose()


async def seed(db, *, lead_status="new", draft_state=None, bypass=False,
               age: timedelta | None = None, breakdown=None):
    """Сообщение, лид по нему и, если попросили, черновик в заданном состоянии.

    `bypass` — канал с включённым обходом L1 (для частичных прогонов по списку
    каналов); `age` — возраст сообщения (свежесть оценки считается от `tg_date`);
    `breakdown` — разбор оценки, посаженный в лид напрямую.
    """
    channel = Channel(peer_id=-1001, username="ch", title="Канал",
                      l1_bypass_enabled=bypass)
    db.add(channel)
    await db.flush()

    message = Message(channel_id=channel.id, tg_message_id=1000,
                      tg_date=(NOW - age) if age else NOW,
                      author_peer_id=500, author_username="user", author_name="Имя",
                      author_is_bot=False, is_automatic_forward=False,
                      text="не могу оплатить инвойс, помогите разобраться",
                      cascade_level=1, cascade_passed=True, processed_at=NOW)
    db.add(message)
    await db.flush()

    lead = Lead(message_id=message.id, channel_id=channel.id, author_peer_id=500,
                author_username="user", author_name="Имя", pain="не может оплатить за рубеж",
                quote="цитата", score=50, score_breakdown=breakdown,
                status=lead_status)
    db.add(lead)
    await db.flush()

    if draft_state is not None:
        db.add(Draft(lead_id=lead.id, variants=[{"text": "заготовка"}],
                     state=draft_state, prompt_version="template-v0"))
    await db.commit()
    return message, lead


REJECTED = {"level": 1, "passed": False, "detail": {}, "pain": None,
            "score": 0, "breakdown": [], "disqualifiers": []}
PENDING = {**REJECTED, "passed": None}


async def counts(db):
    return (
        (await db.execute(select(func.count(Lead.id)))).scalar_one(),
        (await db.execute(select(func.count(Draft.id)))).scalar_one(),
    )


async def test_a_new_lead_with_no_draft_is_removed(db):
    """Прежнее поведение, ради которого удаление и было написано: то, что система
    больше не считает лидом, не должно занимать время оператора."""
    message, _ = await seed(db)
    created, removed, kept = await reclassify._reconcile_leads(
        db, [message], {message.id: REJECTED})
    await db.commit()

    assert (created, removed, kept) == (0, 1, 0)
    assert await counts(db) == (0, 0)


async def test_a_new_lead_with_an_untouched_draft_is_removed_with_it(db):
    """Черновик в `pending` — это заготовка, которую никто не смотрел. Решения в ней
    нет, терять нечего, и оставлять лид ради неё значит не убрать мусор вовсе.

    Именно этот случай раньше ронял весь прогон нарушением внешнего ключа.
    """
    message, _ = await seed(db, draft_state="pending")
    created, removed, kept = await reclassify._reconcile_leads(
        db, [message], {message.id: REJECTED})
    await db.commit()

    assert (created, removed, kept) == (0, 1, 0)
    assert await counts(db) == (0, 0), "черновик обязан уйти вместе с лидом"


@pytest.mark.parametrize("state", ["approved", "rejected"])
async def test_a_lead_whose_draft_was_decided_survives(db, state):
    """Решение по черновику принял человек. Удалить лид значит стереть это решение —
    и заодно потерять единственный след того, что мы кому-то писали."""
    message, lead = await seed(db, draft_state=state)
    created, removed, kept = await reclassify._reconcile_leads(
        db, [message], {message.id: REJECTED})
    await db.commit()

    assert (created, removed, kept) == (0, 0, 1)
    assert await counts(db) == (1, 1)
    still = (await db.execute(select(Lead).where(Lead.id == lead.id))).scalar_one()
    assert still.status == "new", "статус лида менять мы не собирались"


@pytest.mark.parametrize("status", ["in_review", "approved", "rejected"])
async def test_a_lead_a_human_has_touched_survives(db, status):
    """Прежнее правило, оно не менялось: всё, кроме `new`, поставил человек."""
    message, _ = await seed(db, lead_status=status)
    created, removed, kept = await reclassify._reconcile_leads(
        db, [message], {message.id: REJECTED})
    await db.commit()

    assert (created, removed, kept) == (0, 0, 1)
    assert await counts(db) == (1, 0)


async def test_a_verdict_still_in_flight_removes_nothing(db):
    """`passed is None` — «ещё не досчитали», а не «признали мусором». Удалять по нему
    значит терять лиды из-за недоступности собственной модели."""
    message, _ = await seed(db, draft_state="pending")
    created, removed, kept = await reclassify._reconcile_leads(
        db, [message], {message.id: PENDING})
    await db.commit()

    assert (created, removed, kept) == (0, 0, 1)
    assert await counts(db) == (1, 1)


async def test_a_whole_run_no_longer_dies_on_the_first_such_lead(db):
    """Смысл починки не в одном лиде, а в том, что прогон доходит до конца.

    Раньше нарушение ключа откатывало транзакцию целиком: непройденный лид с
    черновиком обнулял и всю остальную работу переклассификации.
    """
    channel = Channel(peer_id=-1002, username="ch2", title="Канал 2")
    db.add(channel)
    await db.flush()

    messages, keep_me = [], None
    for i in range(6):
        m = Message(channel_id=channel.id, tg_message_id=2000 + i,
                    tg_date=NOW - timedelta(hours=i), author_peer_id=600 + i,
                    author_username=f"u{i}", author_is_bot=False,
                    is_automatic_forward=False, text=f"текст {i}",
                    cascade_level=1, cascade_passed=True, processed_at=NOW)
        messages.append(m)
        db.add(m)
    await db.flush()

    for i, m in enumerate(messages):
        lead = Lead(message_id=m.id, channel_id=channel.id, author_peer_id=m.author_peer_id,
                    author_username=m.author_username, pain="боль", quote="ц",
                    score=40, status="new")
        db.add(lead)
        await db.flush()
        # Каждый второй с черновиком, один из них — уже одобренный.
        if i % 2 == 0:
            db.add(Draft(lead_id=lead.id, variants=[{"text": "з"}],
                         state="approved" if i == 4 else "pending",
                         prompt_version="template-v0"))
        if i == 4:
            keep_me = lead
    await db.commit()

    verdicts = {m.id: REJECTED for m in messages}
    created, removed, kept = await reclassify._reconcile_leads(db, messages, verdicts)
    await db.commit()

    assert (created, removed, kept) == (0, 5, 1)
    leads_left, drafts_left = await counts(db)
    assert leads_left == 1 and drafts_left == 1
    survivor = (await db.execute(select(Lead))).scalar_one()
    assert survivor.id == keep_me.id


# ── частичный прогон по списку каналов: только создавать ──────────────────────

# Заглушки ступеней — те же, что в `tests/test_l1_bypass_db.py`: эмбеддер всегда
# отдаёт одну картину близости, модель всегда соглашается.
RANKED_POS = [("pos", "банк не пропускает платёж", 0.60), ("neg", "офтоп", 0.40)]
RANKED_NOISE = [("neg", "болтовня по теме, проблемы нет", 0.81),
                ("pos", "не может оплатить за рубеж", 0.60)]
LLM_YES = {"real_problem": True, "is_seller": False,
           "answering_someone_else": False, "why": "…"}


def stub_stages(monkeypatch, ranked=RANKED_POS):
    async def fake_prototype_vectors():
        return []

    async def fake_embed(texts):
        return [[0.0] for _ in texts]

    async def fake_verdict(*, text, context, prompt_key):
        return dict(LLM_YES), None

    monkeypatch.setattr(embeddings, "enabled", lambda: True)
    monkeypatch.setattr(embeddings, "prototype_vectors", fake_prototype_vectors)
    monkeypatch.setattr(embeddings, "embed", fake_embed)
    monkeypatch.setattr(embeddings, "rank", lambda vector, protos: list(ranked))
    monkeypatch.setattr(llm, "verdict", fake_verdict)


async def test_channel_scoped_run_creates_but_never_updates_leads(db, monkeypatch):
    """T12: прогон по списку каналов заводит новые лиды, но не дышит на существующие.

    Без create-only свежесть пересчиталась бы в 0 (сообщению 8 суток) и оценка
    стала бы 40 — ровно этот сдвиг оценок у заведённых лидов и запрещён.
    """
    stub_stages(monkeypatch)
    m1, lead = await seed(db, bypass=True, age=timedelta(days=8),
                          breakdown=[{"label": "свежесть", "value": 10}])
    channel_id, lead_id = m1.channel_id, lead.id
    m2 = Message(channel_id=channel_id, tg_message_id=1001, tg_date=NOW,
                 author_peer_id=501, author_username="user2", author_name="Имя 2",
                 author_is_bot=False, is_automatic_forward=False,
                 text="а" * 250, cascade_level=1, cascade_passed=False,
                 processed_at=NOW)
    db.add(m2)
    await db.commit()
    m2_id = m2.id

    summary = await reclassify.run(db, l2_enabled=True, l3_enabled=True,
                                   scope="all", channel_ids=[channel_id])

    assert summary["created"] == 1, "создан ровно один новый лид — по m2"
    assert summary["skipped_updates"] == 1, "перезапись лида m1 пропущена"
    still = (await db.execute(
        select(Lead).where(Lead.id == lead_id))).scalar_one()
    assert still.score == 50, "оценка существующего лида не сдвинулась"
    assert still.score_breakdown == [{"label": "свежесть", "value": 10}]
    assert still.status == "new"
    m2_after = (await db.execute(
        select(Message).where(Message.id == m2_id))).scalar_one()
    assert m2_after.cascade_level == 3 and m2_after.cascade_passed is True, \
        "мёртвое словарём сообщение ожило через обход и дошло до L3"
    fresh = (await db.execute(
        select(Lead).where(Lead.message_id == m2_id))).scalars().all()
    assert len(fresh) == 1 and fresh[0].status == "new"


async def test_channel_scoped_run_never_removes_leads(db, monkeypatch):
    """T13: решение об удалении в частичном прогоне не выносится вовсе — вердикт
    мог измениться из-за новых правил отмеченного канала, а не из-за сообщения."""
    stub_stages(monkeypatch, ranked=RANKED_NOISE)
    m3, lead = await seed(db, bypass=True)
    m3.cascade_level = 3
    await db.commit()
    lead_id, channel_id = lead.id, m3.channel_id

    summary = await reclassify.run(db, l2_enabled=True, l3_enabled=True,
                                   scope="all", channel_ids=[channel_id])

    leads = (await db.execute(select(Lead))).scalars().all()
    assert len(leads) == 1 and leads[0].id == lead_id, "лид обязан остаться"
    assert summary["skipped_removals"] == 1, "отказ от удаления виден в сводке"
