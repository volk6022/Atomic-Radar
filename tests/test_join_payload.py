"""Вступление в канал должно называть цель тем словом, которое понимает Engage.

Воркер `join_group` в fleet_manager читает из payload `invite_link` или `target`
и про `username` не знает вовсе (`app/workers/join_group.py`). С ключом `username`
цель приезжала пустой, и вступление уходило в `join_chat(None)` — то есть
подключение канала (FIXES.md #7) не могло сработать ни разу, а на стенде это не
всплыло, потому что живого Engage там нет.
"""
import asyncio
import os

os.environ.setdefault("RADAR_SECRET_KEY", "x" * 32)
os.environ.setdefault("RADAR_DATABASE_URL", "postgresql+asyncpg://u:p@localhost/db")
os.environ.setdefault("RADAR_INGEST_TOKEN", "t" * 24)

from app.api.v1 import ingest  # noqa: E402


def test_join_chain_sends_target_not_username(monkeypatch):
    sent = {}

    async def fake_action(*, account_id, action, payload, webhook_url, **kw):
        sent.update(action=action, payload=payload, webhook_url=webhook_url)
        return {"task_id": "t-1"}

    monkeypatch.setattr(ingest.engage, "action", fake_action)
    asyncio.run(ingest._start_join_chain(
        account_id=3, username="kvt_zavtrak", run_id=7,
        subscribed_by="owner@example.com", stage="channel"))

    assert sent["action"] == "join_group"
    assert sent["payload"] == {"target": "kvt_zavtrak"}
    # `username` в адресе возврата остаётся: по нему `_handle_join` понимает, во что
    # именно вступили, — ответ Engage на `join_group` идентификатор чата не содержит.
    assert "username=kvt_zavtrak" in sent["webhook_url"]
