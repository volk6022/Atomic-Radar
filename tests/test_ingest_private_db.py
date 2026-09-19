"""Личные сообщения из вотчера — события диалога, не сообщения канала (PLAN 16.3).

Каждый сценарий ловит одну ошибку: известный человек — чтобы ветка не переписала
`source` и не завела вторую нитку; неизвестный — что нитка `unsolicited` с
аккаунтом-читателем и именем без «@»; бот — что мимо; супергруппа — что обычный
путь не сломан и теперь пишет `chat_type` (раньше строки каналов стояли без типа).
База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_DEBUG", "true")

from app.db.models import (Base, Channel, Conversation,  # noqa: E402
                           ConversationEvent, EngageInstance, Message, Workflow)
from app.services import ingest  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

T0 = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)


def _envelope(**kw) -> dict:
    base = {
        "event": "incoming_message", "account_id": 3, "chat_type": "private",
        "chat_id": 700100, "chat_username": None, "chat_title": None,
        "message_id": 4242, "message": "Привет, а как платить в Китай?",
        "from_peer_id": 700100, "from_first_name": "Пётр", "from_last_name": "Смирнов",
        "sender_username": "@petr", "from_is_bot": False,
        "date": "2026-08-26T12:00:00Z",
    }
    base.update(kw)
    return base


@pytest.fixture
async def db():
    engine = create_async_engine(DB_URL, poolclass=None)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session:
        inst = EngageInstance(key="test", client_label="Тестовый клиент",
                              base_url="http://engage.invalid",
                              api_key_env="RADAR_ENGAGE_API_KEY", is_active=True)
        session.add(inst)
        await session.flush()
        session.add(Workflow(
            key="cold_dm", title="Личные сообщения", target_kind="user", action="dm",
            visibility="private", engage_instance_id=inst.id, engage_use_case="cold_dm",
            cascade_profile="dm_v1", sort_order=10, is_active=True))
        await session.commit()
        yield session
    await engine.dispose()


async def _count(db, model) -> int:
    return (await db.execute(select(func.count()).select_from(model))).scalar_one()


async def test_known_peer_gets_an_inbound_event_and_keeps_its_source(db):
    db.add(Conversation(peer_id=700100, engage_account_id=3, source="draft",
                        peer_username="petr", state="awaiting_reply", sent_count=1))
    await db.commit()

    out = await ingest.ingest_incoming_message(db, _envelope())

    assert out == {"accepted": 1, "conversation_id": 1, "event_id": 1,
                   "unsolicited": False}
    conv = await db.get(Conversation, 1)
    await db.refresh(conv)
    assert conv.source == "draft", "ветка не переписывает источник нитки"
    assert conv.state == "replied"
    assert conv.last_inbound_at == T0
    event = (await db.execute(select(ConversationEvent))).scalar_one()
    assert (event.kind, event.source, event.at, event.tg_message_id) == (
        "inbound", "engage:incoming", T0, 4242)
    assert event.payload["from_name"] == "Пётр Смирнов"
    assert await _count(db, Channel) == 0 and await _count(db, Message) == 0


async def test_unknown_peer_opens_an_unsolicited_thread(db):
    out = await ingest.ingest_incoming_message(db, _envelope(account_id=5))

    assert out["accepted"] == 1 and out["unsolicited"] is True
    conv = (await db.execute(select(Conversation))).scalar_one()
    assert (conv.source, conv.engage_account_id, conv.peer_username, conv.state) == (
        "unsolicited", 5, "petr", "replied")
    assert await _count(db, ConversationEvent) == 1
    assert await _count(db, Channel) == 0


async def test_bot_and_missing_sender_are_ignored(db):
    assert await ingest.ingest_incoming_message(db, _envelope(from_is_bot=True)) == {
        "accepted": 0, "reason": "бот"}
    assert await ingest.ingest_incoming_message(db, _envelope(from_peer_id=None)) == {
        "accepted": 0, "reason": "нет from_peer_id"}
    assert await _count(db, Conversation) == 0


async def test_group_message_still_becomes_a_channel_with_its_type(db):
    out = await ingest.ingest_incoming_message(db, _envelope(
        chat_type="supergroup", chat_id=-1001234, chat_username="stroy_chat",
        chat_title="Стройка"))

    assert out["accepted"] == 1 and out["created"] == 1
    channel = (await db.execute(select(Channel))).scalar_one()
    assert (channel.peer_id, channel.chat_type) == (-1001234, "supergroup")
    assert await _count(db, Message) == 1
    assert await _count(db, Conversation) == 0
