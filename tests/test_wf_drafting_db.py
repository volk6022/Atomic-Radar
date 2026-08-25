"""Черновики по целям сценариев — на настоящем Postgres.

Половина правил здесь живёт в схеме, и мок пропустит ровно их: «одна цель — один
черновик» держится `UNIQUE (target_id)`, а `wf_drafts.target_id` объявлен `NOT NULL`
без `ondelete`, то есть база физически запрещает осиротить заготовку.

Проверяется главное следствие среза: **одно сообщение даёт две разные заготовки**, и
разные они не тоном, а существом — в личку идёт рекомендация, в ветку короткий ответ
по делу без упоминания контакта.

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db.models import (Base, Channel, EngageInstance, Message, WfDraft, WfTarget,
                           Workflow)
from app.services import drafting, wf_drafting

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — половина правил живёт в схеме")

NOW = datetime(2026, 8, 25, 12, 0, tzinfo=timezone.utc)


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


async def seed(db) -> dict:
    """Два сценария разной формы над одним сообщением.

    Публичный заведён с профилем `public_v1` и действием `reply` — именно так он и
    выглядит в реестре; на копии сценария ЛС разница заготовок не проверяется никак.
    """
    instance = EngageInstance(key="default", client_label="Основной",
                              base_url="http://engage:8103",
                              api_key_env="RADAR_ENGAGE_API_KEY")
    db.add(instance)
    await db.flush()

    dm = Workflow(key="cold_dm", title="Личные сообщения", target_kind="user",
                  action="dm", visibility="private", engage_instance_id=instance.id,
                  engage_use_case="cold_dm", cascade_profile="dm_v1",
                  sort_order=10, is_active=True)
    public = Workflow(key="public_reply", title="Публичные ответы",
                      target_kind="message", action="reply", visibility="public",
                      engage_instance_id=instance.id, engage_use_case="public_reply",
                      cascade_profile="public_v1", sort_order=5, is_active=True)
    db.add_all([dm, public])

    channel = Channel(peer_id=-1001, username="chat", title="Обсуждение")
    db.add(channel)
    await db.flush()

    message = Message(channel_id=channel.id, tg_message_id=1000, tg_date=NOW,
                      author_peer_id=500, author_username="user", author_name="Имя",
                      author_is_bot=False, is_automatic_forward=False,
                      text="впн отваливается, ищу кто настроит", processed_at=NOW)
    db.add(message)
    await db.commit()
    return {"dm": dm, "public": public, "channel": channel, "message": message}


async def add_target(db, wf: Workflow, s: dict, *, pain="VPN не работает",
                     status="new") -> WfTarget:
    """Цель нужной формы. Адресация заполняется по `target_kind` — иначе `CHECK`
    не пропустит, и правильно сделает."""
    address = ({"recipient_peer_id": s["message"].author_peer_id}
               if wf.target_kind == "user"
               else {"chat_peer_id": s["channel"].peer_id,
                     "reply_to_message_id": s["message"].tg_message_id})
    target = WfTarget(workflow_id=wf.id, target_kind=wf.target_kind,
                      message_id=s["message"].id, channel_id=s["channel"].id,
                      author_peer_id=s["message"].author_peer_id,
                      author_username="user", author_name="Имя",
                      pain=pain, quote=s["message"].text, score=60,
                      score_breakdown=[], disqualifiers=[], status=status,
                      **address)
    db.add(target)
    await db.commit()
    return target


# ── одна цель — один черновик ─────────────────────────────────────────────────

async def test_draft_is_created_for_a_target(db):
    s = await seed(db)
    target = await add_target(db, s["dm"], s)

    draft = await wf_drafting.ensure_wf_draft(db, s["dm"], target)
    await db.commit()

    assert draft.id is not None
    assert draft.target_id == target.id
    assert draft.workflow_id == s["dm"].id
    assert draft.state == "pending"
    assert draft.variants


async def test_second_call_returns_the_same_draft(db):
    """Идемпотентность не роскошь: очередь дёргается при каждом открытии экрана, и
    второй черновик по той же цели нарушил бы `UNIQUE` посреди запроса."""
    s = await seed(db)
    target = await add_target(db, s["dm"], s)

    first = await wf_drafting.ensure_wf_draft(db, s["dm"], target)
    await db.commit()
    second = await wf_drafting.ensure_wf_draft(db, s["dm"], target)
    await db.commit()

    assert first.id == second.id
    assert (await db.execute(select(WfDraft))).scalars().all().__len__() == 1


# ── два контура — две разные заготовки ────────────────────────────────────────

async def test_one_message_gives_two_different_drafts(db):
    """Смысловой центр среза. Раньше публичный контур копил цели и не имел заготовок
    вовсе; теперь у каждого своя, и различаются они существом, а не тоном."""
    s = await seed(db)
    dm_target = await add_target(db, s["dm"], s)
    public_target = await add_target(db, s["public"], s)

    dm_draft = await wf_drafting.ensure_wf_draft(db, s["dm"], dm_target)
    public_draft = await wf_drafting.ensure_wf_draft(db, s["public"], public_target)
    await db.commit()

    dm_texts = [v["text"] for v in dm_draft.variants]
    public_texts = [v["text"] for v in public_draft.variants]

    assert dm_texts != public_texts
    assert not (set(dm_texts) & set(public_texts))
    assert dm_draft.prompt_version == drafting.PROMPT_VERSION
    assert public_draft.prompt_version == "template-public-v0"


async def test_public_draft_does_not_name_the_contact(db):
    """То, ради чего заготовки и разводились: под чужим вопросом «как починить»
    рекомендация подрядчика читается как реклама, и её удаляют."""
    s = await seed(db)
    target = await add_target(db, s["public"], s, pain="VPN не работает")

    draft = await wf_drafting.ensure_wf_draft(db, s["public"], target)
    await db.commit()

    for v in draft.variants:
        assert wf_drafting.CONTACT not in v["text"]
        assert v["lint_ok"] is True


async def test_dm_draft_does_name_the_contact(db):
    """Обратная сторона: в личке контакт и есть смысл сообщения."""
    s = await seed(db)
    target = await add_target(db, s["dm"], s, pain="VPN не работает")

    draft = await wf_drafting.ensure_wf_draft(db, s["dm"], target)
    await db.commit()

    assert any(wf_drafting.CONTACT in v["text"] for v in draft.variants)


async def test_public_draft_names_the_contact_when_asked_who_to_hire(db):
    """Правило про контакт зависит от боли, а не от контура целиком."""
    s = await seed(db)
    target = await add_target(db, s["public"], s,
                              pain=wf_drafting.ASKS_FOR_CONTRACTOR)

    draft = await wf_drafting.ensure_wf_draft(db, s["public"], target)
    await db.commit()

    assert any(wf_drafting.CONTACT in v["text"] for v in draft.variants)
    assert all(v["lint_ok"] for v in draft.variants)


# ── витрина для ревьюера ──────────────────────────────────────────────────────

async def test_draft_carries_thread_and_link(db):
    """Ревьюер должен открыть ветку, а не верить цитате."""
    s = await seed(db)
    target = await add_target(db, s["public"], s)

    draft = await wf_drafting.ensure_wf_draft(db, s["public"], target)
    await db.commit()

    assert draft.thread_context
    assert any(row["target"] for row in draft.thread_context)
    assert draft.source_message_link == "https://t.me/chat/1000"


async def test_draft_works_for_a_post_without_an_author(db):
    """Пост канала — законная публичная цель: писать в личку некому, а ответить под
    ним можно. Заготовка обязана собраться без автора."""
    s = await seed(db)
    post = Message(channel_id=s["channel"].id, tg_message_id=1001, tg_date=NOW,
                   author_peer_id=None, author_username=None, author_name=None,
                   author_is_bot=False, is_automatic_forward=True,
                   text="перенесли сайт, теперь всё лежит", processed_at=NOW)
    db.add(post)
    await db.commit()

    target = WfTarget(workflow_id=s["public"].id, target_kind="message",
                      message_id=post.id, channel_id=s["channel"].id,
                      chat_peer_id=s["channel"].peer_id, reply_to_message_id=1001,
                      pain="не может настроить сам", quote=post.text, score=40,
                      score_breakdown=[], disqualifiers=[], status="new")
    db.add(target)
    await db.commit()

    draft = await wf_drafting.ensure_wf_draft(db, s["public"], target)
    await db.commit()

    assert draft.variants
    assert draft.source_message_link == "https://t.me/chat/1001"


# ── очередь ───────────────────────────────────────────────────────────────────

async def test_queue_creates_drafts_and_marks_targets_in_review(db):
    s = await seed(db)
    await add_target(db, s["public"], s)

    created = await wf_drafting.ensure_queue(db, s["public"])
    await db.commit()

    assert created == 1
    target = (await db.execute(select(WfTarget))).scalars().one()
    assert target.status == "in_review"


async def test_queue_is_idempotent(db):
    """Второй проход не должен ни заводить дубликаты, ни считать цель новой."""
    s = await seed(db)
    await add_target(db, s["public"], s)

    assert await wf_drafting.ensure_queue(db, s["public"]) == 1
    await db.commit()
    assert await wf_drafting.ensure_queue(db, s["public"]) == 0
    await db.commit()

    assert len((await db.execute(select(WfDraft))).scalars().all()) == 1


async def test_queue_touches_only_its_own_workflow(db):
    """Цели чужого сценария — не его дело. Иначе публичная очередь молча растащила бы
    цели ЛС и пометила их как взятые в работу."""
    s = await seed(db)
    await add_target(db, s["dm"], s)
    await add_target(db, s["public"], s)

    created = await wf_drafting.ensure_queue(db, s["public"])
    await db.commit()

    assert created == 1
    drafts = (await db.execute(select(WfDraft))).scalars().all()
    assert len(drafts) == 1
    assert drafts[0].workflow_id == s["public"].id


async def test_queue_ignores_decided_targets(db):
    """Цель, по которой человек уже решил, в очередь не возвращается: заводить ей
    заготовку значило бы предлагать заново то, что закрыли."""
    s = await seed(db)
    await add_target(db, s["public"], s, status="rejected")

    created = await wf_drafting.ensure_queue(db, s["public"])
    await db.commit()

    assert created == 0
    assert (await db.execute(select(WfDraft))).scalars().all() == []


async def test_queue_fails_before_writing_when_the_action_is_unknown(db):
    """Комплект ищется до первой записи: сценарий с чужим действием должен уронить
    проход на старте, а не оставить половину целей помеченными."""
    s = await seed(db)
    await add_target(db, s["public"], s)
    s["public"].action = "телепатия"
    await db.commit()

    with pytest.raises(wf_drafting.UnknownActionError):
        await wf_drafting.ensure_queue(db, s["public"])

    assert (await db.execute(select(WfDraft))).scalars().all() == []
    target = (await db.execute(select(WfTarget))).scalars().one()
    assert target.status == "new"


async def test_one_draft_per_target_is_enforced_by_the_schema(db):
    """Правило держится базой, а не только кодом: `UNIQUE (target_id)`."""
    from sqlalchemy.exc import IntegrityError

    s = await seed(db)
    target = await add_target(db, s["public"], s)
    await wf_drafting.ensure_wf_draft(db, s["public"], target)
    await db.commit()

    db.add(WfDraft(workflow_id=s["public"].id, target_id=target.id,
                   variants=[], state="pending"))
    with pytest.raises(IntegrityError):
        await db.commit()
