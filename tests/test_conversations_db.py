"""Диалоги по HTTP — на настоящем Postgres.

Каждая строка посева существует ради одной ошибки, а не ради объёма:

* диалог без входящих вообще — ловит правило, забывшее про `last_inbound_at IS
  NULL`: без этой ветки такой диалог вечно числился бы непрочитанным;
* прочитанный после входящего — ловит сравнение «прочитано ли что-то вообще»
  вместо сравнения моментов;
* диалог, прочитанный и оживший новым входящим, — ловит отметку, которая
  ставится один раз и больше не двигается;
* события одной секунды — ловят сортировку нитки без второго ключа.

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
Посев стирает схему public этой базы — она должна быть одноразовой.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_DEBUG", "true")

from app.core import clock  # noqa: E402
from app.core.config import get_settings  # noqa: E402
from app.core.security import SessionSigner  # noqa: E402
from app.db.models import (Base, Account, Channel, Conversation,  # noqa: E402
                           ConversationEvent, Lead, Message, User)
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.main import create_app  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

LIST = "/api/v1/conversations"

NOW = datetime.now(timezone.utc)
HOUR = timedelta(hours=1)


async def _seed() -> dict:
    engine = create_async_engine(DB_URL, poolclass=None)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)

    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        db.add(Channel(peer_id=-1001, username="chat", title="Обсуждение"))
        db.add(Account(engage_account_id=12, engage_instance="default",
                       label="Основной", status="ok"))
        await db.flush()

        msg = Message(channel_id=1, tg_message_id=1000, tg_date=NOW,
                      author_peer_id=500, author_username="ivan",
                      author_name="Иван Горлов", author_is_bot=False,
                      is_automatic_forward=False, text="не проходит платёж за рубеж",
                      processed_at=NOW)
        db.add(msg)
        await db.flush()
        db.add(Lead(message_id=msg.id, channel_id=1, author_peer_id=500,
                    author_username="ivan", author_name="Иван Горлов",
                    pain="не может оплатить за рубеж", quote=msg.text, score=70))
        await db.flush()

        # Четыре диалога — четыре исхода правила из задачи. Входящее первого — на
        # секунду в прошлом от «сейчас» посева: отметка прочтения в тестах ниже
        # ставится настоящим временем, и совпадение микросекунд сделало бы исход
        # неопределённым.
        a = Conversation(lead_id=1, account_id=1, peer_id=501, state="new",
                         sent_count=1, last_inbound_at=NOW - timedelta(seconds=1),
                         read_at=None)
        b = Conversation(lead_id=1, account_id=1, peer_id=502, state="new",
                         sent_count=1, last_inbound_at=None, read_at=None)
        c = Conversation(lead_id=1, account_id=1, peer_id=503, state="replied",
                         sent_count=2, last_inbound_at=NOW - 2 * HOUR,
                         read_at=NOW - HOUR)
        d = Conversation(lead_id=1, account_id=1, peer_id=504,
                         state="awaiting_reply", sent_count=1,
                         last_inbound_at=NOW, read_at=NOW - 3 * HOUR)
        db.add_all([a, b, c, d])
        await db.flush()

        # Журнал первого диалога: по времени вставляются вперемешку, и ровно у двух
        # событий секунда одна и та же. Нитка обязана вернуться по возрастанию
        # времени, а пара с равной меткой — по `id`, то есть в порядке появления.
        # Имена «первое»/«второе» отражают именно порядок вставки: доразрыв по id —
        # единственное, что делает выдачу воспроизводимой, когда метки совпали.
        same = NOW - 2 * HOUR
        db.add_all([
            ConversationEvent(conversation_id=a.id, kind="inbound",
                              payload={"text": "новое"}, created_at=NOW - HOUR),
            ConversationEvent(conversation_id=a.id, kind="outbound",
                              payload={"text": "первое из той же секунды"},
                              created_at=same),
            ConversationEvent(conversation_id=a.id, kind="inbound",
                              payload={"text": "старое"}, created_at=NOW - 3 * HOUR),
            ConversationEvent(conversation_id=a.id, kind="outbound",
                              payload={"text": "второе из той же секунды"},
                              created_at=same),
        ])

        users = {}
        for role in ("owner", "viewer"):
            u = User(email=f"{role}@local", name=role, initials=role[:2].upper(),
                     role=role, password_hash="!нельзя-войти", totp_secret="X" * 32,
                     totp_confirmed=True, is_active=True)
            db.add(u)
            users[role] = u
        await db.commit()
        out = {"uids": {r: u.id for r, u in users.items()}, "unread": a.id}

    await engine.dispose()
    return out


@pytest.fixture
def seeded():
    """Посев в собственном цикле событий: соединение asyncpg привязано к тому
    циклу, где создано, и закрывается вместе с ним."""
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
    token = SessionSigner(get_settings().SECRET_KEY).dumps({"uid": uid, "totp_ok": True})
    client.cookies.set(get_settings().SESSION_COOKIE, token)
    return client


@pytest.fixture
def authed(client, seeded):
    return _login(client, seeded["uids"]["owner"])


async def _touch(conversation_id: int, **fields):
    """Прямая правка строки между запросами: имитация входящего, которое
    прилетело воркером, а не через API."""
    engine = create_async_engine(DB_URL, poolclass=None)
    async with engine.begin() as conn:
        sets = ", ".join(f"{k} = :{k}" for k in fields)
        await conn.execute(text(
            f"UPDATE conversations SET {sets} WHERE id = :cid"),
            {"cid": conversation_id, **fields})
    await engine.dispose()


# ── список ────────────────────────────────────────────────────────────────────

def test_default_list_shows_only_unread(authed, seeded):
    """Без параметров экран показывает непрочитанное: A (не читал) и D (читал, но
    пришло новое). B без входящих и C прочитанный сюда не попадают."""
    body = authed.get(LIST).json()

    assert body["unread_only"] is True
    assert body["total"] == 2 and body["unread_total"] == 2
    assert len(body["rows"]) == 2
    assert all(r["unread"] for r in body["rows"])


def test_unread_only_off_shows_all_and_marks_the_read_ones(authed):
    body = authed.get(LIST, params={"unread_only": "false"}).json()

    assert body["total"] == 4 and len(body["rows"]) == 4
    flags = {r["peer_id"]: r["unread"] for r in body["rows"]}
    assert flags[501] is True and flags[504] is True
    assert flags[502] is False and flags[503] is False
    # Значок не зависит от фильтра списка.
    assert body["unread_total"] == 2


def test_state_chips_follow_the_unread_filter(authed):
    """Чип «new» показывает непрочитанные новые (один), а не все новые (два):
    число на чипе обязано совпадать с длиной списка после клика по нему."""
    chips = {s["key"]: s["count"] for s in authed.get(LIST).json()["states"]}
    assert chips["new"] == 1 and chips["awaiting_reply"] == 1

    chips_all = {s["key"]: s["count"]
                 for s in authed.get(LIST, params={"unread_only": "false"}).json()["states"]}
    assert chips_all["new"] == 2


def test_state_filter_counts_in_total(authed):
    body = authed.get(LIST, params={"state": "replied"}).json()
    assert body["total"] == 0 and body["rows"] == []


# ── нитка ─────────────────────────────────────────────────────────────────────

def test_thread_comes_ascending(authed, seeded):
    body = authed.get(f"{LIST}/{seeded['unread']}").json()
    stamps = [e["created_at"] for e in body["events"]]
    assert stamps == sorted(stamps)
    assert len(body["events"]) == 4
    # Пара с одинаковой меткой возвращается по id — то есть в порядке появления,
    # а не как придётся. Без этого доразрыва порядок зависел бы от плана запроса.
    texts = [e["payload"]["text"] for e in body["events"]]
    assert texts[1] == "первое из той же секунды"
    assert texts[2] == "второе из той же секунды"


def test_thread_header(authed, seeded):
    header = authed.get(f"{LIST}/{seeded['unread']}").json()["conversation"]
    assert header["peer_name"] == "Иван Горлов"
    assert header["peer_username"] == "@ivan"
    assert header["account"] == 1 and header["state"] == "new"
    assert header["sent_count"] == 1 and header["unread"] is True
    assert header["read_at"] is None


def test_unknown_thread_is_404(authed):
    assert authed.get(f"{LIST}/999999").status_code == 404


# ── отметка о прочтении ───────────────────────────────────────────────────────

def test_read_removes_from_default_list_and_from_the_badge(authed, seeded):
    cid = seeded["unread"]
    r = authed.post(f"{LIST}/{cid}/read")
    assert r.status_code == 200 and r.json()["read_at"] is not None

    body = authed.get(LIST).json()
    assert cid not in [row["id"] for row in body["rows"]]
    assert body["total"] == 1 and body["unread_total"] == 1
    assert authed.get(f"{LIST}/{cid}").json()["conversation"]["unread"] is False


def test_read_moves_when_a_new_inbound_arrived(authed, seeded, monkeypatch):
    """Основной случай: прочитал, пришло новое — снова непрочитан, и повторная
    отметка двигает момент прочтения, а не молчит.

    Время подменяется через `clock` — точка существует ровно для этого: реальный
    `utcnow()` между двумя запросами не сдвинулся бы на секунду вперёд."""
    cid = seeded["unread"]
    t1 = datetime.now(timezone.utc)
    monkeypatch.setattr(clock, "utcnow", lambda: t1)
    first = authed.post(f"{LIST}/{cid}/read").json()["read_at"]

    asyncio.run(_touch(cid, last_inbound_at=t1 + timedelta(seconds=30)))
    assert cid in [row["id"] for row in authed.get(LIST).json()["rows"]]

    monkeypatch.setattr(clock, "utcnow", lambda: t1 + timedelta(minutes=1))
    second = authed.post(f"{LIST}/{cid}/read").json()["read_at"]
    assert second > first
    assert authed.get(LIST).json()["unread_total"] == 1  # остался только D


def test_read_unknown_is_404(authed):
    assert authed.post(f"{LIST}/999999/read").status_code == 404


# ── доступ ────────────────────────────────────────────────────────────────────

def test_guest_is_refused_everywhere(client, seeded):
    _login(client, seeded["uids"]["viewer"])
    cid = seeded["unread"]
    assert client.get(LIST).status_code == 403
    assert client.get(f"{LIST}/{cid}").status_code == 403
    assert client.post(f"{LIST}/{cid}/read").status_code == 403
