"""Диалоги по HTTP и на уровне сервиса — на настоящем Postgres.

Каждая строка посева существует ради одной ошибки, а не ради объёма:

* диалог без входящих вообще — ловит правило, забывшее про `last_inbound_at IS
  NULL`: без этой ветки такой диалог вечно числился бы непрочитанным;
* прочитанный после входящего — ловит сравнение «прочитано ли что-то вообще»
  вместо сравнения моментов;
* диалог, прочитанный и оживший новым входящим, — ловит отметку, которая
  ставится один раз и больше не двигается;
* события одной секунды — ловят сортировку нитки без второго ключа.

С 16.1 тут и сервисный слой (`app/services/conversations.py`): нитка одна на
`peer_id` на весь флот, свёртка состояний в `add_event`, привязка ручных
отправок и бэкфилл. Эти правила держат внешние ключи и уникальность — проверять
их подделками нечем.

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
Посев стирает схему public этой базы — она должна быть одноразовой.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_DEBUG", "true")

from app.core import clock  # noqa: E402
from app.core.config import get_settings  # noqa: E402
from app.core.security import SessionSigner  # noqa: E402
from app.db.models import (Base, Channel, Conversation,  # noqa: E402
                           ConversationEvent, EngageInstance, Lead, ManualSend,
                           Message, User, WfTarget, Workflow)
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.main import create_app  # noqa: E402
from app.services import conversations, manual_sends  # noqa: E402

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
        # неопределённым. `source="draft"` — нитки старого контура: они от лида.
        a = Conversation(lead_id=1, engage_account_id=1, source="draft", peer_id=501,
                         state="new",
                         sent_count=1, last_inbound_at=NOW - timedelta(seconds=1),
                         read_at=None)
        b = Conversation(lead_id=1, engage_account_id=1, source="draft", peer_id=502,
                         state="new",
                         sent_count=1, last_inbound_at=None, read_at=None)
        c = Conversation(lead_id=1, engage_account_id=1, source="draft", peer_id=503,
                         state="replied",
                         sent_count=2, last_inbound_at=NOW - 2 * HOUR,
                         read_at=NOW - HOUR)
        d = Conversation(lead_id=1, engage_account_id=1, source="draft", peer_id=504,
                         state="awaiting_reply", sent_count=1,
                         last_inbound_at=NOW, read_at=NOW - 3 * HOUR)
        db.add_all([a, b, c, d])
        await db.flush()

        # Журнал первого диалога: по времени вставляются вперемешку, и ровно у двух
        # событий секунда одна и та же. Нитка обязана вернуться по возрастанию
        # времени, а пара с равной меткой — по `id`, то есть в порядке появления.
        # Имена «первое»/«второе» отражают именно порядок вставки: доразрыв по id —
        # единственное, что делает выдачу воспроизводимой, когда метки совпали.
        # `at` у посева совпадает с `created_at`: это журнальные строки без своей
        # хронологии, момент события = момент записи.
        same = NOW - 2 * HOUR
        db.add_all([
            ConversationEvent(conversation_id=a.id, kind="inbound", at=NOW - HOUR,
                              payload={"text": "новое"}, created_at=NOW - HOUR),
            ConversationEvent(conversation_id=a.id, kind="outbound", at=same,
                              payload={"text": "первое из той же секунды"}, created_at=same),
            ConversationEvent(conversation_id=a.id, kind="inbound", at=NOW - 3 * HOUR,
                              payload={"text": "старое"}, created_at=NOW - 3 * HOUR),
            ConversationEvent(conversation_id=a.id, kind="outbound", at=same,
                              payload={"text": "второе из той же секунды"}, created_at=same),
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
    assert header["engage_account_id"] == 1 and header["state"] == "new"
    assert header["source"] == "draft" and header["target_id"] is None
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


# ── сервисный слой: нитки, события, привязка отправок (16.1) ──────────────────

T0 = datetime(2026, 9, 1, 10, 0, tzinfo=timezone.utc)


@pytest.fixture
async def db():
    """Сессия поверх свежей схемы — для сервисов, без HTTP. Схема стирается
    на каждом тесте: тот же контракт, что у `test_manual_sends_db`."""
    engine = create_async_engine(DB_URL, poolclass=None)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session:
        yield session
    await engine.dispose()


async def service_seed(db) -> dict:
    """Минимум для сервисных тестов: сценарий ЛС и сценарий публичных ответов,
    по одной цели каждого вида, лид — чтобы CHECK'у «обе привязки разом» было
    к чему приводить нарушение."""
    instance = EngageInstance(key="default", client_label="Основной",
                              base_url="http://engage:8103",
                              api_key_env="RADAR_ENGAGE_API_KEY")
    channel = Channel(peer_id=-1001, username="ch", title="Канал про ВЭД")
    db.add_all([instance, channel])
    await db.flush()

    msg = Message(channel_id=channel.id, tg_message_id=2000, tg_date=T0,
                  author_peer_id=600, author_username="julia",
                  author_name="Юлия Импортова", author_is_bot=False,
                  is_automatic_forward=False, text="не проходит платёж за рубеж",
                  processed_at=T0)
    db.add(msg)
    await db.flush()

    dm = Workflow(key="cold_dm", title="Личные сообщения", target_kind="user",
                  action="dm", visibility="private", engage_instance_id=instance.id,
                  engage_use_case="cold_dm", cascade_profile="dm_v1", sort_order=10)
    public = Workflow(key="public_reply", title="Публичные ответы",
                      target_kind="message", action="reply", visibility="public",
                      engage_instance_id=instance.id, engage_use_case="service_testing",
                      cascade_profile="dm_v1", sort_order=20)
    db.add_all([dm, public])
    await db.flush()

    user_target = WfTarget(workflow_id=dm.id, target_kind="user", message_id=msg.id,
                           channel_id=channel.id, recipient_peer_id=600,
                           author_peer_id=600, author_username="julia",
                           author_name="Юлия Импортова", pain="не может оплатить",
                           quote=msg.text, score=50, status="new")
    message_target = WfTarget(workflow_id=public.id, target_kind="message",
                              message_id=msg.id, channel_id=channel.id,
                              chat_peer_id=-1002, reply_to_message_id=2000,
                              author_username="julia", author_name="Юлия Импортова",
                              quote=msg.text, score=50, status="new")
    db.add_all([user_target, message_target])
    await db.flush()

    db.add(Lead(message_id=msg.id, channel_id=channel.id, author_peer_id=600,
                author_username="julia", author_name="Юлия Импортова",
                pain="не может оплатить", quote=msg.text, score=50))
    await db.commit()
    return {"dm": dm, "public": public, "user_target": user_target,
            "message_target": message_target}


async def test_one_thread_per_peer_across_accounts(db):
    """Решение владельца (PLAN 16.11): нитка одна на человека на весь флот.
    Второй писатель с другим аккаунтом получает ту же нитку и ничего не
    переписывает — аккаунт и цель фиксируются первым касанием."""
    s = await service_seed(db)
    first = await conversations.ensure_thread(
        db, peer_id=600, engage_account_id=3, source="draft",
        target_id=s["user_target"].id)
    second = await conversations.ensure_thread(
        db, peer_id=600, engage_account_id=9, source="manual")
    await db.commit()

    assert second.id == first.id
    assert second.engage_account_id == 3, "аккаунт — первое касание, не перезаписывается"
    assert second.target_id == s["user_target"].id
    assert (await db.execute(select(func.count(Conversation.id)))).scalar_one() == 1


async def test_add_event_moves_counters_and_states(db):
    """Свёртка состояний — по таблице из задачи 16.1: отправка/ручная ждут ответа,
    входящий снимает ожидание, ручная после входящего снова ставит нитку в
    `awaiting_reply`,     пометки счётчиков не трогают."""
    await service_seed(db)
    conv = await conversations.ensure_thread(db, peer_id=600, engage_account_id=3,
                                             source="draft")
    t1, t2, t3 = T0, T0 + HOUR, T0 + 2 * HOUR

    await conversations.add_event(db, conv, kind="outbound", source="draft:1",
                                  actor="wf:cold_dm", at=t1)
    assert conv.state == "awaiting_reply" and conv.waiting_since == t1
    assert conv.sent_count == 1 and conv.last_sent_at == t1

    await conversations.add_event(db, conv, kind="inbound", source="engage_history",
                                  actor=None, at=t2)
    assert conv.state == "replied" and conv.waiting_since is None
    assert conv.last_inbound_at == t2

    await conversations.add_event(db, conv, kind="manual", source="manual_send:7",
                                  actor="andrey@x", at=t3, text="и ещё раз привет")
    assert conv.state == "awaiting_reply" and conv.waiting_since == t3
    assert conv.sent_count == 2 and conv.last_sent_at == t3

    await conversations.add_event(db, conv, kind="note", source="web",
                                  actor="andrey@x", at=t3)
    await conversations.add_event(db, conv, kind="system", source="webhook",
                                  actor=None, at=t3)
    assert conv.sent_count == 2, "пометки — не переписка, счётчики не двигаются"
    await db.commit()


async def test_events_leave_human_states_alone(db):
    """`handed_off`/`closed` ставит человек (16.5), и события их не меняют —
    иначе автомат выводил бы нитку из решения оператора."""
    await service_seed(db)
    conv = await conversations.ensure_thread(db, peer_id=600, engage_account_id=3,
                                             source="draft")
    conv.state = "closed"
    await conversations.add_event(db, conv, kind="inbound", source="engage_history",
                                  actor=None, at=T0)
    assert conv.state == "closed" and conv.waiting_since is None
    await db.commit()


async def test_foreign_kind_and_source_are_refused(db):
    """Чужие значения справочников — ошибка программиста, а не данных."""
    with pytest.raises(ValueError, match="источник нитки"):
        await conversations.ensure_thread(db, peer_id=1, engage_account_id=1,
                                          source="twitter")
    conv = await conversations.ensure_thread(db, peer_id=2, engage_account_id=1,
                                             source="draft")
    with pytest.raises(ValueError, match="вид события"):
        await conversations.add_event(db, conv, kind="like", source="draft:1",
                                      actor=None, at=T0)


async def test_unsolicited_may_go_without_account(db):
    """Нитка «человек написал сам» заводится без аккаунта и без привязки —
    единственный источник, которому это разрешено."""
    conv = await conversations.ensure_thread(db, peer_id=800, engage_account_id=None,
                                             source="unsolicited",
                                             peer_username="stranger")
    await db.commit()
    assert conv.engage_account_id is None and conv.target_id is None
    assert conv.state == "new"


async def test_manual_send_to_a_user_target_opens_the_thread(db):
    """Ручная отправка по наводке-«user» заводит нитку и событие `manual_send:<id>`
    — с текстом, сценарием и временем отправки."""
    s = await service_seed(db)
    entry = await manual_sends.record(db, workflow=s["dm"], text="привет",
                                      recorded_by="andrey@x",
                                      target_id=s["user_target"].id,
                                      engage_account_id=3, sent_at=T0)
    await db.commit()

    assert entry.conversation_id is not None
    conv = await db.get(Conversation, entry.conversation_id)
    assert conv.peer_id == 600 and conv.source == "manual"
    assert conv.engage_account_id == 3 and conv.target_id == s["user_target"].id
    assert conv.sent_count == 1 and conv.state == "awaiting_reply"
    assert conv.last_sent_at == T0

    events = (await db.execute(select(ConversationEvent).where(
        ConversationEvent.conversation_id == conv.id))).scalars().all()
    assert len(events) == 1
    ev = events[0]
    assert ev.kind == "manual" and ev.source == f"manual_send:{entry.id}"
    assert ev.text == "привет" and ev.actor == "andrey@x"
    assert ev.at == T0 and ev.workflow_id == s["dm"].id


async def test_manual_send_without_account_still_opens_the_thread(db):
    """Человек мог оставить аккаунт пустым (список недоступен — «запись факта
    от него не зависит»): нитка заводится и без аккаунта. У автомата пустого
    аккаунта не бывает, но запрет здесь значил бы терять факт отправки."""
    s = await service_seed(db)
    entry = await manual_sends.record(db, workflow=s["dm"], text="привет",
                                      recorded_by="andrey@x",
                                      target_id=s["user_target"].id)
    await db.commit()
    assert entry.conversation_id is not None
    conv = await db.get(Conversation, entry.conversation_id)
    assert conv.engage_account_id is None and conv.source == "manual"


async def test_manual_send_to_a_public_target_does_not_open_a_thread(db):
    """Диалог — только про ЛС (решение владельца, 16.11): публичный ответ нитки
    не заводит, `conversation_id` остаётся пустым, и это не ошибка."""
    s = await service_seed(db)
    entry = await manual_sends.record(db, workflow=s["public"], text="в тред",
                                      recorded_by="andrey@x",
                                      target_id=s["message_target"].id)
    await db.commit()

    assert entry.conversation_id is None
    assert (await db.execute(select(func.count(Conversation.id)))).scalar_one() == 0


async def test_backfill_links_once_and_then_zero(db):
    """Бэкфилл привязывает отправки, записанные до 16.1, и на втором прогоне
    находит ноль. Запись без адресата не привязывается никогда — диалога для
    неё не придумать."""
    s = await service_seed(db)
    old = ManualSend(workflow_id=s["dm"].id, target_id=s["user_target"].id,
                     engage_account_id=3, text="старая запись", recorded_by="andrey@x")
    orphan = ManualSend(workflow_id=s["dm"].id, text="мимо радара",
                        recorded_by="andrey@x")
    db.add_all([old, orphan])
    await db.flush()

    assert await conversations.backfill_manual_sends(db) == 1
    await db.commit()
    assert old.conversation_id is not None
    assert orphan.conversation_id is None
    assert await conversations.backfill_manual_sends(db) == 0


async def test_binding_check_refuses_lead_and_target_together(db):
    """Нитка привязана к лиду ИЛИ к цели, или ни к чему — CHECK держит «не обе
    сразу» на уровне схемы, а не надежды на писателей."""
    s = await service_seed(db)
    db.add(Conversation(lead_id=1, target_id=s["user_target"].id, peer_id=610,
                        engage_account_id=3, source="draft"))
    with pytest.raises(IntegrityError):
        await db.flush()
    await db.rollback()


async def test_contact_facts_before_and_after_the_event(db):
    """Факты для гардрейла «этому человеку уже писали» (16.2): без нитки —
    (False, 0, None); нитка без отправок — всё ещё «не писали»; событие
    отправки делает человека «протронутым»."""
    await service_seed(db)
    assert await conversations.contact_facts(db, peer_id=600) == (False, 0, None)

    conv = await conversations.ensure_thread(db, peer_id=600, engage_account_id=3,
                                             source="draft")
    assert await conversations.contact_facts(db, peer_id=600) == (False, 0, None)

    await conversations.add_event(db, conv, kind="outbound", source="draft:1",
                                  actor="wf:cold_dm", at=T0)
    assert await conversations.contact_facts(db, peer_id=600) == (True, 1, T0)


async def test_correcting_sent_at_moves_the_event_and_the_thread(db):
    """Правка времени отправки доезжает до события `manual_send:<id>` и до
    `last_sent_at` нитки — все три места говорят одно."""
    s = await service_seed(db)
    entry = await manual_sends.record(db, workflow=s["dm"], text="привет",
                                      recorded_by="andrey@x",
                                      target_id=s["user_target"].id,
                                      engage_account_id=3, sent_at=T0)
    await db.commit()
    conv = await db.get(Conversation, entry.conversation_id)
    assert conv.last_sent_at == T0

    t_late = T0 + timedelta(hours=3)
    assert manual_sends.correct(entry, {"sent_at": t_late}) == ["sent_at"]
    await conversations.resync_manual_send_time(db, entry)
    await db.commit()

    ev = (await db.execute(select(ConversationEvent).where(
        ConversationEvent.source == f"manual_send:{entry.id}"))).scalar_one()
    assert ev.at == t_late
    assert conv.last_sent_at == t_late
