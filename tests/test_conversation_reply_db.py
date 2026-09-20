"""Ответ из Переписок (PLAN 16.5): гейт без предохранителей первого касания,
заказ без черновика, доставка в нитку, передача/закрытие.

Каждый тест ловит одну ошибку: гейт — что `first_touch=False` снимает ровно
паузу/потолок/«уже писали», а тихие часы остаются; заказ — что строка
`wf_outbound` без сценария и черновика вообще возможна (workflow_id nullable с
16.5) и несёт автора; доставка — что вебхук ведёт событие в ту же нитку, а не
заводит вторую; состояния — что повтор ничего не пишет.
База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_DEBUG", "true")

from app.api.v1 import ingest as ingest_api  # noqa: E402
from app.core import invariants  # noqa: E402
from app.core.outbound_gate import SendVerdict  # noqa: E402
from app.db.models import (AuditLog, Base, Conversation,  # noqa: E402
                           ConversationEvent, WfOutbound)
from app.services import conversation_reply, engage  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

# 12:00 UTC = 15:00 МСК — вне тихих часов; пауза 20 ч заведомо не выдержана.
NOW = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)
RECENT = NOW - timedelta(hours=1)


def test_reply_gate_keeps_quiet_hours_and_drops_first_touch_guards():
    common = dict(mode="DRY_RUN", draft_state="approved", text="см. https://x.y",
                  is_first=False, sent_count=4, last_sent_at=RECENT, now=NOW,
                  recipient_is_admin=False, previously_contacted=True, origin="manual")
    first = invariants.check_all(local_hour=15, **common)
    reply = invariants.check_all(local_hour=15, first_touch=False, **common)
    assert len(first) >= 3, first
    assert reply == [], "ответу в диалоге пауза, потолок и «уже писали» не мешают"
    night = invariants.check_all(local_hour=3, first_touch=False, **common)
    assert len(night) == 1 and "тихие часы" in night[0], night


@pytest.fixture
async def db():
    engine = create_async_engine(DB_URL, poolclass=None)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session:
        session.add(Conversation(peer_id=600, engage_account_id=3, source="unsolicited",
                                 peer_username="julia", state="replied",
                                 last_inbound_at=RECENT))
        await session.commit()
        yield session
    await engine.dispose()


@pytest.fixture
def fleet(monkeypatch):
    """Engage без сети: аккаунт 3 активен, остатков нет, заказ принимается."""
    async def list_accounts(*, instance=None):
        return [{"account_id": 3, "status": "active", "warmup_tier": "ready"}]

    async def limits(*, account_ids=None, instance=None):
        return {"accounts": [{"account_id": 3, "actions": [
            {"action": "messages_per_day", "remaining": 7}]}]}

    sent: list = []

    async def send(self, req, now):
        sent.append(req)
        return SendVerdict(allowed=True, task_id="task-1")

    monkeypatch.setattr(engage, "list_accounts", list_accounts)
    monkeypatch.setattr(engage, "limits", limits)
    monkeypatch.setattr(conversation_reply.OutboundGate, "send", send)
    return sent


async def _conv(db) -> Conversation:
    return (await db.execute(select(Conversation))).scalar_one()


async def test_preflight_reports_account_and_blocks_closed_thread(db, fleet):
    conv = await _conv(db)
    out = await conversation_reply.preflight(db, conv=conv, text="да, поможем", now=NOW)
    assert out["gate"] == {"allowed": True, "reasons": []}
    assert out["account"] == {"id": 3, "status": "active", "remaining_messages": 7}
    assert out["recipient"] == {"peer_id": 600, "username": "julia"}

    conv.state = "closed"
    out = await conversation_reply.preflight(db, conv=conv, text="", now=NOW)
    assert out["gate"]["allowed"] is False
    assert {"диалог закрыт", "нет текста ответа"} <= set(out["gate"]["reasons"])
    with pytest.raises(conversation_reply.ReplyBlocked):
        await conversation_reply.order(db, conv=conv, text="", actor="o@x", now=NOW)
    assert (await db.execute(select(func.count()).select_from(WfOutbound))).scalar_one() == 0


async def test_order_writes_outbound_without_draft_and_delivery_lands_in_the_thread(db, fleet):
    conv = await _conv(db)
    row = await conversation_reply.order(db, conv=conv, text="да, поможем",
                                         actor="owner@x", now=NOW)
    assert (row.draft_id, row.workflow_id, row.conversation_id, row.actor,
            row.state, row.engage_task_id) == (None, None, conv.id, "owner@x",
                                               "pending", "task-1")
    req = fleet[0]
    assert (req.first_touch, req.origin, req.outbound_id, req.recipient_peer_id) == (
        False, "manual", row.id, 600)
    audit = (await db.execute(select(AuditLog))).scalars().all()
    assert [a.action for a in audit] == ["wf_conversation_reply"]

    # Второй заказ поверх живого — конфликт, строка не плодится.
    with pytest.raises(conversation_reply.ReplyConflict):
        await conversation_reply.order(db, conv=conv, text="ещё раз", actor="owner@x", now=NOW)

    out = await ingest_api._handle_send_complete(
        db, {"telegram_message_id": 777}, {"outbound_id": str(row.id)})
    assert out["conversation_id"] == conv.id
    await db.refresh(row)
    assert (row.state, row.delivered_message_id) == ("delivered", 777)
    events = (await db.execute(select(ConversationEvent))).scalars().all()
    assert len(events) == 1
    assert (events[0].kind, events[0].source, events[0].actor, events[0].tg_message_id) == (
        "outbound", f"reply:{row.id}", "owner@x", 777)
    assert (await db.execute(select(func.count()).select_from(Conversation))).scalar_one() == 1
    await db.refresh(conv)
    assert (conv.state, conv.sent_count) == ("awaiting_reply", 1)


async def test_handoff_and_close_are_idempotent_and_leave_a_trace(db):
    conv = await _conv(db)
    conv = await conversation_reply.set_state(db, conv=conv, state="handed_off",
                                              actor="staff@x", now=NOW)
    assert (conv.state, conv.handed_off_at) == ("handed_off", NOW)
    conv = await conversation_reply.set_state(db, conv=conv, state="handed_off",
                                              actor="staff@x", now=NOW + timedelta(hours=1))
    assert conv.handed_off_at == NOW, "повтор ничего не переписывает"
    conv = await conversation_reply.set_state(db, conv=conv, state="closed",
                                              actor="staff@x", now=NOW)
    assert conv.state == "closed"
    events = (await db.execute(select(ConversationEvent).order_by(ConversationEvent.id))).scalars().all()
    assert [(e.kind, e.source, e.payload["to"]) for e in events] == [
        ("system", "state:handed_off", "handed_off"), ("system", "state:closed", "closed")]
    with pytest.raises(ValueError):
        await conversation_reply.set_state(db, conv=conv, state="new", actor="s", now=NOW)
