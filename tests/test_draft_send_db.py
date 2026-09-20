"""Ручная отправка одобренного черновика — сквозной путь на настоящем Postgres.

Ради чего файл существует при живом `test_draft_send.py`: там проверки на
подделках, здесь — то, что держат база и настоящий вебхук. Один сценарий
проходит путь задачи целиком: заказ → вебхук доставки → нитка диалога →
«этому человеку уже писали» во втором preflight. По дороге проверяются права
(403 у разборщика), 409 не-ЛС сценария, форма ответов для GUI и идемпотентность
повторной доставки.

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
Посев стирает схему public этой базы — она должна быть одноразовой.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_DEBUG", "true")
os.environ.setdefault("RADAR_INGEST_TOKEN", "test-ingest-token")

from app.core.config import get_settings  # noqa: E402
from app.core.security import SessionSigner  # noqa: E402
from app.db.models import (AuditLog, Base, Channel, Conversation,  # noqa: E402
                           ConversationEvent, EngageInstance, Message,
                           MessageReader, User, WfDraft, WfOutbound, WfTarget,
                           Workflow)
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.main import create_app  # noqa: E402
from app.core import clock  # noqa: E402
from app.services import conversations, engage  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

NOW = datetime(2026, 9, 19, 12, 0, tzinfo=timezone.utc)
TEXT = "Добрый день! Судя по описанию, дело в валютном контроле."


async def _seed() -> dict:
    engine = create_async_engine(DB_URL, poolclass=None)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)

    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        instance = EngageInstance(key="default", client_label="Основной",
                                  base_url="http://engage.invalid",
                                  api_key_env="RADAR_ENGAGE_API_KEY")
        db.add(instance)
        await db.flush()

        dm = Workflow(key="cold_dm", title="Личные сообщения", target_kind="user",
                      action="dm", visibility="private",
                      engage_instance_id=instance.id, engage_use_case="cold_dm",
                      cascade_profile="dm_v1", sort_order=10, is_active=True)
        public = Workflow(key="public_reply", title="Публичные ответы",
                          target_kind="message", action="reply",
                          visibility="public", engage_instance_id=instance.id,
                          engage_use_case="public_reply",
                          cascade_profile="public_v1", sort_order=20,
                          is_active=True)
        db.add_all([dm, public])

        channel = Channel(peer_id=-1001, username="chat", title="Обсуждение")
        db.add(channel)
        await db.flush()

        # Два сообщения ОДНОГО автора: второй черновик — тому же человеку, на
        # нём проверяется «этому человеку уже писали» после первой доставки.
        messages = []
        for n in range(2):
            m = Message(channel_id=channel.id, tg_message_id=1000 + n,
                        tg_date=NOW, author_peer_id=500,
                        author_username="ivan_p", author_name="Иван П.",
                        author_is_bot=False, is_automatic_forward=False,
                        text="платёж за рубеж не проходит, ищу через кого оплатить",
                        processed_at=NOW)
            messages.append(m)
        db.add_all(messages)
        await db.flush()

        def dm_target(m):
            return WfTarget(workflow_id=dm.id, target_kind="user",
                            message_id=m.id, channel_id=channel.id,
                            recipient_peer_id=m.author_peer_id,
                            author_peer_id=m.author_peer_id,
                            author_username=m.author_username,
                            author_name=m.author_name,
                            pain="не может оплатить за рубеж", quote=m.text,
                            score=70, score_breakdown=[], disqualifiers=[],
                            status="approved")

        pub_target = WfTarget(workflow_id=public.id, target_kind="message",
                              message_id=messages[0].id, channel_id=channel.id,
                              chat_peer_id=channel.peer_id,
                              reply_to_message_id=messages[0].tg_message_id,
                              author_peer_id=500, author_username="ivan_p",
                              author_name="Иван П.", pain="не может оплатить",
                              quote=messages[0].text, score=65,
                              score_breakdown=[], disqualifiers=[],
                              status="approved")
        t1, t2 = dm_target(messages[0]), dm_target(messages[1])
        db.add_all([t1, t2, pub_target])
        await db.flush()

        drafts = {}
        for name, t in (("d1", t1), ("d2", t2)):
            d = WfDraft(workflow_id=dm.id, target_id=t.id, variants=[{"text": TEXT}],
                        final_text=TEXT, state="approved",
                        decided_by="andrey@local", decided_at=NOW,
                        prompt_version="template-v0")
            db.add(d)
            drafts[name] = d
        pub_d = WfDraft(workflow_id=public.id, target_id=pub_target.id,
                        variants=[{"text": TEXT}], final_text=TEXT,
                        state="approved", decided_by="andrey@local",
                        decided_at=NOW, prompt_version="template-v0")
        db.add(pub_d)
        await db.flush()

        # Отправлять должен тот, кто прочитал сообщение цели (решение №2).
        db.add(MessageReader(message_id=messages[0].id, account_id=3,
                             first_seen_at=NOW))

        users = {}
        for role in ("owner", "customer", "reviewer"):
            u = User(email=f"{role}@local", name=role, initials=role[:2].upper(),
                     role=role, password_hash="!нельзя-войти", totp_secret="X" * 32,
                     totp_confirmed=True, is_active=True)
            db.add(u)
            users[role] = u
        await db.commit()

        out = {"uids": {r: u.id for r, u in users.items()},
               "drafts": {k: d.id for k, d in drafts.items()},
               "pub_draft": pub_d.id,
               "targets": {"t1": t1.id, "t2": t2.id}}
    await engine.dispose()
    return out


@pytest.fixture
def seeded():
    return asyncio.run(_seed())


@pytest.fixture
def client(seeded):
    previous = os.environ.get("RADAR_DATABASE_URL")
    os.environ["RADAR_DATABASE_URL"] = DB_URL
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_session_maker.cache_clear()

    app = create_app()
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c

    if previous is None:
        os.environ.pop("RADAR_DATABASE_URL", None)
    else:
        os.environ["RADAR_DATABASE_URL"] = previous
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_session_maker.cache_clear()


def _login(client, uid):
    token = SessionSigner(get_settings().SECRET_KEY).dumps(
        {"uid": uid, "totp_ok": True})
    client.cookies.set(get_settings().SESSION_COOKIE, token)
    return client


@pytest.fixture
def customer(client, seeded):
    return _login(client, seeded["uids"]["customer"])


@pytest.fixture
def reviewer(client, seeded):
    return _login(client, seeded["uids"]["reviewer"])


def _stub_engage(monkeypatch, *, send_error=None) -> list[dict]:
    """Engage без сети: флот с одним активным аккаунтом, остаток 17, задача t-1.

    Часы тоже подменяются: гейт считает тихие часы получателя от текущего
    времени, и без подмены набор краснел ночью по МСК (20.09 02:37 — «тихие
    часы: 0:00 попадает в 0:00-8:00»)."""
    sends: list[dict] = []
    monkeypatch.setattr(clock, "utcnow",
                        lambda: datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc))

    async def list_accounts(*, instance=None):
        return [{"account_id": 3, "status": "active", "warmup_tier": "ready"}]

    async def limits(*, account_ids=None, instance=None):
        return {"accounts": [{"account_id": a, "actions": [
            {"action": "messages_per_day", "remaining": 17}]}
            for a in (account_ids or [])]}

    async def send_message(**kw):
        if send_error is not None:
            raise send_error
        sends.append(kw)
        return {"task_id": "task-e2e", "status": "queued"}

    monkeypatch.setattr(engage, "list_accounts", list_accounts)
    monkeypatch.setattr(engage, "limits", limits)
    monkeypatch.setattr(engage, "send_message", send_message)
    return sends


def _rows(query):
    async def go():
        engine = create_async_engine(DB_URL, poolclass=None)
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with maker() as db:
            # scalars(): и сущности (select(WfOutbound)), и одиночные колонки
            # (select(WfDraft.state)) отдаются плоскими значениями, без 1-кортежей Row.
            out = (await db.execute(query)).scalars().all()
        await engine.dispose()
        return out
    return asyncio.run(go())


# ── права ─────────────────────────────────────────────────────────────────────

def test_reviewer_cannot_send_or_preflight(reviewer, seeded):
    """Разборщик одобряет тексты, но не отправляет людям от имени заказчика."""
    r = reviewer.get(
        f"/api/v1/workflows/cold_dm/drafts/{seeded['drafts']['d1']}/send-preflight")
    assert r.status_code == 403
    r = reviewer.post(
        f"/api/v1/workflows/cold_dm/drafts/{seeded['drafts']['d1']}/send", json={})
    assert r.status_code == 403


def test_anonymous_is_401(client, seeded):
    assert client.post(
        f"/api/v1/workflows/cold_dm/drafts/{seeded['drafts']['d1']}/send",
        json={}).status_code == 401


def test_non_dm_workflow_is_a_conflict(customer, seeded):
    """Публичный ответ — не ЛС: 409 с человеческой причиной и пустым списком гейта."""
    r = customer.post(
        f"/api/v1/workflows/public_reply/drafts/{seeded['pub_draft']}/send",
        json={})
    assert r.status_code == 409
    body = r.json()
    assert "личных сообщений" in body["detail"]
    assert body["reasons"] == []
    r = customer.get(
        f"/api/v1/workflows/public_reply/drafts/{seeded['pub_draft']}/send-preflight")
    assert r.status_code == 409


# ── заказ ─────────────────────────────────────────────────────────────────────

def test_preflight_shape_for_gui(customer, seeded, monkeypatch):
    """Форма ответа — контракт GUI 16.4; поле remaining_messages из Engage."""
    _stub_engage(monkeypatch)
    r = customer.get(
        f"/api/v1/workflows/cold_dm/drafts/{seeded['drafts']['d1']}/send-preflight")
    assert r.status_code == 200
    body = r.json()
    assert body["draft_id"] == seeded["drafts"]["d1"]
    assert body["state"] == "approved" and body["action"] == "dm"
    assert body["recipient"] == {"peer_id": 500, "username": "ivan_p",
                                 "name": "Иван П."}
    assert body["account"] == {"id": 3, "status": "active",
                               "remaining_messages": 17}
    assert body["gate"] == {"allowed": True, "reasons": []}
    assert body["already"] is None
    assert body["text"] == TEXT


def test_order_returns_202_and_writes_the_journal(customer, seeded, monkeypatch):
    sends = _stub_engage(monkeypatch)
    r = customer.post(
        f"/api/v1/workflows/cold_dm/drafts/{seeded['drafts']['d1']}/send", json={})

    assert r.status_code == 202
    body = r.json()
    assert body == {"outbound_id": body["outbound_id"], "task_id": "task-e2e",
                    "state": "pending", "account_id": 3}

    [sent] = sends
    assert sent["idempotency_key"] == f"radar-wf-outbound-{body['outbound_id']}"
    assert sent["recipient_peer_id"] == 500 and sent["recipient_username"] == "ivan_p"
    assert "kind=send" in sent["webhook_url"]
    assert f"outbound_id={body['outbound_id']}" in sent["webhook_url"]

    rows = _rows(select(WfOutbound))
    [row] = rows
    assert row.state == "pending" and row.engage_task_id == "task-e2e"
    assert row.mode == "DRY_RUN" and row.allowed is True
    assert row.delivered_message_id is None

    # Черновик остаётся approved: точка невозврата — доставка, не заказ.
    state = _rows(select(WfDraft.state).where(
        WfDraft.id == seeded["drafts"]["d1"]))[0]
    assert state == "approved"

    audit = _rows(select(AuditLog).where(AuditLog.action == "wf_draft_send"))
    assert audit and audit[0].detail["outbound_id"] == body["outbound_id"]
    assert audit[0].detail["origin"] == "manual"


def test_second_order_is_a_conflict(customer, seeded, monkeypatch):
    _stub_engage(monkeypatch)
    url = f"/api/v1/workflows/cold_dm/drafts/{seeded['drafts']['d1']}/send"
    assert customer.post(url, json={}).status_code == 202
    r = customer.post(url, json={})
    assert r.status_code == 409
    assert "уже заказана отправка" in r.json()["detail"]
    assert r.json()["reasons"] == []


def test_badge_outbound_in_the_card(customer, seeded, monkeypatch):
    """Бейдж в карточке — по последней строке `wf_outbound` черновика."""
    _stub_engage(monkeypatch)
    body = customer.post(
        f"/api/v1/workflows/cold_dm/drafts/{seeded['drafts']['d1']}/send",
        json={}).json()
    r = customer.get(
        f"/api/v1/workflows/cold_dm/drafts/{seeded['drafts']['d1']}")
    badge = r.json()["draft"]["outbound"]
    assert badge["id"] == body["outbound_id"]
    assert badge["state"] == "pending" and badge["task_id"] == "task-e2e"
    assert badge["delivered_message_id"] is None


# ── вебхук → нитка → второй заказ ─────────────────────────────────────────────

def _webhook_url(outbound_id: int) -> str:
    token = get_settings().INGEST_TOKEN
    return (f"/api/v1/ingest/{token}"
            f"?kind=send&account_id=3&outbound_id={outbound_id}")


def test_webhook_delivered_opens_the_thread(customer, seeded, monkeypatch):
    _stub_engage(monkeypatch)
    outbound_id = customer.post(
        f"/api/v1/workflows/cold_dm/drafts/{seeded['drafts']['d1']}/send",
        json={}).json()["outbound_id"]

    body = {"event": "task_complete", "task_id": "task-e2e", "account_id": 3,
            "result": {"found": True, "telegram_message_id": 555}}
    r = customer.post(_webhook_url(outbound_id), json=body)
    assert r.status_code == 200
    assert r.json()["accepted"] == 1

    # Повтор (at-least-once) — ничего не меняет.
    r = customer.post(_webhook_url(outbound_id), json=body)
    assert r.json() == {"accepted": 0, "send": "already_delivered"}

    events = _rows(select(ConversationEvent).order_by(ConversationEvent.id))
    assert len(events) == 1, "повтор доставки не заводит второе касание"
    [event] = events
    assert event.kind == "outbound" and event.tg_message_id == 555
    assert event.source == f"draft:{seeded['drafts']['d1']}"
    assert event.actor == "andrey@local" and event.workflow_id is not None

    conv = _rows(select(Conversation))[0]
    assert conv.peer_id == 500 and conv.source == "draft"
    assert conv.sent_count == 1 and conv.state == "awaiting_reply"
    assert conv.engage_account_id == 3

    row = _rows(select(WfOutbound))[0]
    assert row.state == "delivered" and row.delivered_message_id == 555
    assert row.conversation_id == conv.id

    state = _rows(select(WfDraft.state).where(
        WfDraft.id == seeded["drafts"]["d1"]))[0]
    assert state == "sent"
    status = _rows(select(WfTarget.status).where(
        WfTarget.id == seeded["targets"]["t1"]))[0]
    assert status == "contacted"

    # Факты касания — то, что увидит следующий заказ тому же человеку.
    async def facts():
        engine = create_async_engine(DB_URL, poolclass=None)
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with maker() as db:
            out = await conversations.contact_facts(db, peer_id=500)
        await engine.dispose()
        return out
    previously_contacted, sent_count, last_sent_at = asyncio.run(facts())
    assert previously_contacted is True
    assert sent_count == 1
    assert last_sent_at is not None


def test_second_draft_to_the_same_person_is_blocked(customer, seeded,
                                                    monkeypatch):
    """Сквозной хвост задачи: после доставки второму черновику тому же человеку
    гейт отвечает «этому человеку уже писали» — и в preflight, и в заказе."""
    _stub_engage(monkeypatch)
    outbound_id = customer.post(
        f"/api/v1/workflows/cold_dm/drafts/{seeded['drafts']['d1']}/send",
        json={}).json()["outbound_id"]
    customer.post(_webhook_url(outbound_id), json={
        "event": "task_complete", "task_id": "task-e2e", "account_id": 3,
        "result": {"found": True, "telegram_message_id": 555}})

    r = customer.get(
        f"/api/v1/workflows/cold_dm/drafts/{seeded['drafts']['d2']}/send-preflight")
    assert r.status_code == 200
    gate = r.json()["gate"]
    assert gate["allowed"] is False
    assert any("уже писали" in reason for reason in gate["reasons"])

    r = customer.post(
        f"/api/v1/workflows/cold_dm/drafts/{seeded['drafts']['d2']}/send", json={})
    assert r.status_code == 409
    body = r.json()
    assert "уже писали" in body["detail"]
    assert any("уже писали" in reason for reason in body["reasons"])


def test_failed_send_keeps_the_draft_sendable(customer, seeded, monkeypatch):
    """Отказ заказа: попытка `failed`, черновик остаётся approved и заказывается
    повторно (уже как новая попытка)."""
    from app.services.engage import EngageUnavailable
    _stub_engage(monkeypatch, send_error=EngageUnavailable("Engage ответил 502"))
    url = f"/api/v1/workflows/cold_dm/drafts/{seeded['drafts']['d1']}/send"
    r = customer.post(url, json={})
    assert r.status_code == 502

    rows = _rows(select(WfOutbound))
    [row] = rows
    assert row.state == "failed" and "502" in row.error
    state = _rows(select(WfDraft.state).where(
        WfDraft.id == seeded["drafts"]["d1"]))[0]
    assert state == "approved"

    # Вторая попытка по «failed» — уже не конфликт, а новый заказ.
    _stub_engage(monkeypatch)
    r = customer.post(url, json={})
    assert r.status_code == 202
    assert _rows(select(WfOutbound.state)) == ["failed", "pending"]
