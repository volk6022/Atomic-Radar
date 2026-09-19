"""Вебхук доставки отправленного черновика (`kind="send"`) — правила приёма.

Здесь то, что не требует базы: ветвление по `kind` внутри `task_complete` /
`task_failed` / `task_deferred`, разбор причин отказа (конверт → допрос задачи →
перевод кода), идемпотентность повтора и поздние события после доставки. Нитка
диалога подменяется на двойников: состав события и свёртку под настоящими
внешними ключами держит `test_draft_send_db.py`, на Postgres.

Сессия — словарь строк по `(модель, pk)`: вебхук находит свои строки только по
`db.get`, порядок обращений не важен.
"""
from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_INGEST_TOKEN", "test-ingest-token")

from app.api.v1 import ingest  # noqa: E402
from app.db.models import WfDraft, WfOutbound, WfTarget  # noqa: E402
from app.services import conversations as conversations_service  # noqa: E402


# ── подделки ──────────────────────────────────────────────────────────────────

class FakeDB:
    def __init__(self, rows: dict):
        self.rows = dict(rows)
        self.commits = 0

    async def get(self, model, pk):
        return self.rows.get((model, pk))

    async def commit(self):
        self.commits += 1


def outbound(**over) -> WfOutbound:
    base = dict(id=5, workflow_id=1, target_id=7, draft_id=9, conversation_id=None,
                engage_account_id=3, recipient_peer_id=123456, allowed=True,
                reasons=[], mode="DRY_RUN", state="pending",
                engage_task_id="task-1", text_snapshot="текст сообщения")
    base.update(over)
    return WfOutbound(**base)


def seed_db(row=None) -> tuple[FakeDB, WfOutbound, SimpleNamespace, SimpleNamespace]:
    """Строка журнала + черновик (actor = одобривший) + цель."""
    row = row or outbound()
    draft = SimpleNamespace(id=9, state="approved", decided_by="andrey@local")
    target = SimpleNamespace(id=7, target_kind="user", author_username="ivan_p",
                             status="approved")
    db = FakeDB({(WfOutbound, 5): row, (WfDraft, 9): draft, (WfTarget, 7): target})
    return db, row, draft, target


CONV = SimpleNamespace(id=55)


def stub_thread(monkeypatch):
    """Двойники `ensure_thread`/`add_event`: события не пишутся — записываются."""
    threads, events = [], []

    async def ensure_thread(db_, *, peer_id, engage_account_id, source,
                            peer_username=None, target_id=None):
        threads.append(dict(peer_id=peer_id, engage_account_id=engage_account_id,
                            source=source, peer_username=peer_username,
                            target_id=target_id))
        return CONV

    async def add_event(db_, conv, *, kind, source, actor, at, text=None,
                        tg_message_id=None, workflow_id=None, payload=None):
        events.append(dict(kind=kind, source=source, actor=actor, text=text,
                           tg_message_id=tg_message_id,
                           workflow_id=workflow_id))
        return SimpleNamespace(id=1)

    monkeypatch.setattr(conversations_service, "ensure_thread", ensure_thread)
    monkeypatch.setattr(conversations_service, "add_event", add_event)
    return threads, events


Q = {"kind": "send", "account_id": "3", "outbound_id": "5"}


# ── доставка ──────────────────────────────────────────────────────────────────

async def test_complete_marks_delivered_and_opens_the_thread(monkeypatch):
    threads, events = stub_thread(monkeypatch)
    db, row, draft, target = seed_db()

    out = await ingest.process_event(
        db, {"event": "task_complete", "task_id": "task-1", "account_id": 3,
             "result": {"found": True, "telegram_message_id": 777}}, Q)

    assert out["accepted"] == 1 and out["delivered_message_id"] == 777
    assert row.state == "delivered" and row.delivered_message_id == 777
    assert row.conversation_id == 55 and row.error is None
    # Нитка (решение владельца №3): одна на человека, источник «draft».
    assert threads == [dict(peer_id=123456, engage_account_id=3, source="draft",
                            peer_username="ivan_p", target_id=7)]
    [event] = events
    assert event["kind"] == "outbound"
    assert event["source"] == "draft:9"
    assert event["actor"] == "andrey@local"
    assert event["text"] == "текст сообщения"
    assert event["tg_message_id"] == 777
    assert event["workflow_id"] == 1
    # Черновик и цель двигает доставка.
    assert draft.state == "sent" and target.status == "contacted"
    assert db.commits == 1


async def test_complete_replay_changes_nothing(monkeypatch):
    """At-least-once: повтор того же вебхука не заводит второе касание."""
    threads, events = stub_thread(monkeypatch)
    db, row, draft, target = seed_db()

    body = {"event": "task_complete", "task_id": "task-1", "account_id": 3,
            "result": {"found": True, "telegram_message_id": 777}}
    await ingest.process_event(db, body, Q)
    out = await ingest.process_event(db, body, Q)

    assert out == {"accepted": 0, "send": "already_delivered"}
    assert len(events) == 1 and len(threads) == 1
    assert db.commits == 1, "повтор не обязан трогать базу"


async def test_complete_without_outbound_in_the_url_is_400(monkeypatch):
    stub_thread(monkeypatch)
    db, *_ = seed_db()

    from fastapi import HTTPException
    with pytest.raises(HTTPException) as e:
        await ingest.process_event(
            db, {"event": "task_complete", "task_id": "t", "account_id": 3,
                 "result": {"found": True}}, {"kind": "send", "account_id": "3"})
    assert e.value.status_code == 400


async def test_complete_with_unknown_outbound_is_accepted_quietly(monkeypatch):
    """Строки нет — переигрывать нечего: тишина вместо 4xx, иначе Engage
    ретраил бы вебхук, который не станет успешным никогда."""
    stub_thread(monkeypatch)
    db, *_ = seed_db()
    db.rows.clear()

    out = await ingest.process_event(
        db, {"event": "task_complete", "task_id": "t", "account_id": 3,
             "result": {"found": True, "telegram_message_id": 1}}, Q)
    assert out == {"accepted": 0, "send": "outbound_not_found"}


# ── отказ ─────────────────────────────────────────────────────────────────────

async def test_failed_with_error_fields_needs_no_probe(monkeypatch):
    """Engage 16.8 кладёт `error_class`/`error` прямо в конверт — допрос задачи
    не нужен, и двойник задачи обязан упасть тест, если его позвали."""
    stub_thread(monkeypatch)
    db, row, draft, target = seed_db()

    async def no_probe(*a, **k):
        raise AssertionError("поля причины есть в конверте — опроса быть не должно")

    monkeypatch.setattr(ingest.engage, "task", no_probe)

    out = await ingest.process_event(
        db, {"event": "task_failed", "task_id": "task-1", "account_id": 3,
             "error_class": "UserIsBlocked", "error": "нельзя писать первому",
             "retry_count": 1}, Q)

    assert out["accepted"] == 1 and out["state"] == "failed"
    assert row.state == "failed"
    assert row.error == "UserIsBlocked: нельзя писать первому"
    # Отказ двигает только журнал: черновик остаётся approved, цель не тронута.
    assert draft.state == "approved" and target.status == "approved"


async def test_failed_without_fields_probes_the_task(monkeypatch):
    """Полей нет — допрос `GET /v1/tasks/{id}`: у задачи причин больше, чем у
    конверта."""
    stub_thread(monkeypatch)
    db, row, *_ = seed_db()
    probed = []

    async def task(task_id, *, instance=None):
        probed.append(task_id)
        return {"status": "failed", "result": {"error_class": "PeerFlood",
                                               "error": "флуд-контроль"}}

    monkeypatch.setattr(ingest.engage, "task", task)

    await ingest.process_event(
        db, {"event": "task_failed", "task_id": "task-1", "account_id": 3,
             "error_code": "peer_flood"}, Q)

    assert probed == ["task-1"]
    assert row.error == "PeerFlood: флуд-контроль"


async def test_failed_probe_unavailable_falls_back_to_the_code(monkeypatch):
    stub_thread(monkeypatch)
    db, row, *_ = seed_db()

    async def dead(task_id, *, instance=None):
        raise ingest.engage.EngageUnavailable("нет связи")

    monkeypatch.setattr(ingest.engage, "task", dead)

    await ingest.process_event(
        db, {"event": "task_failed", "task_id": "task-1", "account_id": 3,
             "error_code": "username_not_found"}, Q)

    assert row.state == "failed"
    assert "username" in row.error


async def test_complete_with_unresolved_recipient_is_a_failure(monkeypatch):
    """«Получателя не разыскали» (`found: false`) — провал заказа, а не тишина:
    иначе попытка остаётся `pending` навсегда."""
    stub_thread(monkeypatch)
    db, row, draft, _ = seed_db()

    out = await ingest.process_event(
        db, {"event": "task_complete", "task_id": "task-1", "account_id": 3,
             "result": {"found": False, "reason": "username_not_found"}}, Q)

    assert out["accepted"] == 1
    assert row.state == "failed"
    assert "username" in row.error
    assert draft.state == "approved"


async def test_failed_after_delivery_is_ignored(monkeypatch):
    """Поздний отказ после доставки — не верит задним числом: сообщение уже ушло."""
    threads, events = stub_thread(monkeypatch)
    db, row, draft, _ = seed_db(outbound(state="delivered",
                                         delivered_message_id=777))

    out = await ingest.process_event(
        db, {"event": "task_failed", "task_id": "task-1", "account_id": 3,
             "error_class": "TooLate", "error": "опоздал"}, Q)

    assert out == {"accepted": 0, "send": "already_delivered"}
    assert row.state == "delivered" and draft.state == "approved"
    assert events == [] and db.commits == 0


# ── откладывание ──────────────────────────────────────────────────────────────

async def test_deferred_keeps_the_order_alive(monkeypatch):
    stub_thread(monkeypatch)
    db, row, draft, _ = seed_db()

    out = await ingest.process_event(
        db, {"event": "task_deferred", "task_id": "task-1", "account_id": 3,
             "error_code": "BUDGET_PER_ACCOUNT",
             "deferred_until": "2026-09-20T00:00:00Z"}, Q)

    assert out == {"accepted": 1, "outbound_id": 5, "state": "deferred"}
    assert row.state == "deferred"
    assert "суточный бюджет Engage исчерпан" in row.error
    assert "возврат до 2026-09-20T00:00:00Z" in row.error
    assert draft.state == "approved", "заказ жив — повторять его нельзя"


async def test_deferred_then_delivered_ends_delivered(monkeypatch):
    """Полный цикл: лимит кончился → Engage вернулся → доставка."""
    threads, _ = stub_thread(monkeypatch)
    db, row, *_ = seed_db()

    await ingest.process_event(
        db, {"event": "task_deferred", "task_id": "task-1", "account_id": 3,
             "error_code": "BUDGET_PER_ACCOUNT",
             "deferred_until": "2026-09-20T00:00:00Z"}, Q)
    out = await ingest.process_event(
        db, {"event": "task_complete", "task_id": "task-1", "account_id": 3,
             "result": {"found": True, "telegram_message_id": 42}}, Q)

    assert out["accepted"] == 1
    assert row.state == "delivered" and row.delivered_message_id == 42
    assert row.error is None
    assert len(threads) == 1


# ── чужие события не задеты ───────────────────────────────────────────────────

async def test_other_kinds_are_not_captured_by_the_send_branch(monkeypatch):
    """Регресс: `kind="join"` со `task_failed` должен пойти старой веткой."""
    db = FakeDB({})

    out = await ingest.process_event(
        db, {"event": "task_failed", "task_id": "t-1", "account_id": 3,
             "error_code": "channel_private"}, {"kind": "join"})

    assert out == {"accepted": 0, "error": "channel_private"}
