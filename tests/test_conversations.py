"""Диалоги: непрочитанное и нитка — правила, которые не требуют базы.

Здесь всё, что проверяется чистым кодом: правило непрочитанности (у гибрида две
половины — питоновская и SQL, и обязаны говорить одно), дефолт `unread_only`,
порядок нитки и поведение новых ручек. Проверки против настоящего Postgres живут
в `test_conversations_db.py`: без базы SQL-условие видно только текстом запроса,
а текст — ещё не выполнение.

Ручки гоняются с подменённой сессией: FastAPI разрешает зависимости по исходной
функции, поэтому `db_session` и `current_user` подменяются целиком, и тест не
трогает ни базы, ни-cookie входа. Подмена возвращает результаты по порядку
исполнения — он в ручке детерминирован, — а сами запросы запоминаются, и
утверждения проверяют в том числе текст того, что ушло бы в базу.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_DEBUG", "true")

from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import select  # noqa: E402
from sqlalchemy.dialects.postgresql import asyncpg as pg_asyncpg  # noqa: E402

from app.api.deps import current_user, db_session  # noqa: E402
from app.core import clock  # noqa: E402
from app.core.config import get_settings  # noqa: E402
from app.db.models import Conversation, ConversationEvent  # noqa: E402
from app.main import create_app  # noqa: E402

# Момент посева обязан быть в прошлом: отметка прочтения ставится настоящим
# `utcnow()`, и входящее «из будущего» честно осталось бы непрочитанным.
NOW = datetime.now(timezone.utc) - timedelta(hours=6)
HOUR = timedelta(hours=1)

STAFF = SimpleNamespace(id=1, email="owner@local", role="owner", is_active=True)
VIEWER = SimpleNamespace(id=2, email="guest@local", role="viewer", is_active=True)

LIST = "/api/v1/conversations"


# ── правило непрочитанности ───────────────────────────────────────────────────

def conv(read_at: datetime | None = None,
         last_inbound_at: datetime | None = None) -> Conversation:
    """Строка без базы: правилу нужны только два поля, и живут они в объекте."""
    return Conversation(read_at=read_at, last_inbound_at=last_inbound_at)


@pytest.mark.parametrize("read_at,inbound,expected", [
    # Не читал, и входящих не было — читать нечего.
    (None, None, False),
    # Не читал, а входящие есть — непрочитан.
    (None, NOW, True),
    # Прочитал после последнего входящего — прочитан.
    (NOW, NOW - HOUR, False),
    # Прочитал, но потом пришло входящее — снова непрочитан.
    (NOW - HOUR, NOW, True),
    # Входящее в тот же момент, что прочтение: правило строгое («больше»), и
    # равенство — это прочитано.
    (NOW, NOW, False),
])
def test_unread_follows_the_rule(read_at, inbound, expected):
    assert conv(read_at, inbound).unread is expected


def test_sql_half_of_the_rule_says_the_same():
    """SQL-половина гибрида — то, что уйдёт в WHERE фильтра и счётчиков. Разъедись
    она с питоновской — значок считал бы одно, список показывал другое.

    Компилируется под настоящий диалект базы (postgresql+asyncpg), а не только под
    дефолтный: у целевого свои типы, и ошибка компиляции ловится здесь, а не на
    первом запросе продакшена.
    """
    stmt = select(Conversation.id).where(Conversation.unread)
    for sql in (str(stmt), str(stmt.compile(dialect=pg_asyncpg.dialect()))):
        assert "read_at IS NULL" in sql
        assert "last_inbound_at IS NOT NULL" in sql
        assert "last_inbound_at > conversations.read_at" in sql


# ── приложение с подменённой сессией ──────────────────────────────────────────

@pytest.fixture(scope="module")
def app():
    get_settings.cache_clear()
    return create_app()


class FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def scalars(self):
        return self

    def all(self):
        return list(self._rows)

    def scalar_one(self):
        return self._rows[0]

    def scalar_one_or_none(self):
        return self._rows[0] if self._rows else None


class FakeDB:
    """Сессия без базы: раздаёт заготовленные результаты по порядку исполнения
    и помнит текст каждого запроса — утверждения проверяют и его тоже."""

    def __init__(self, script=()):
        self.script = list(script)
        self.queries: list[str] = []
        self.commits = 0

    async def execute(self, stmt):
        self.queries.append(str(stmt))
        return FakeResult(self.script.pop(0))

    async def commit(self):
        self.commits += 1


@pytest.fixture
def db():
    return FakeDB()


@pytest.fixture
def client(app, db):
    app.dependency_overrides[db_session] = lambda: db
    app.dependency_overrides[current_user] = lambda: STAFF
    yield TestClient(app, raise_server_exceptions=False)
    app.dependency_overrides.clear()


@pytest.fixture
def anon(app):
    return TestClient(app, raise_server_exceptions=False)


def unread_conv(i: int, state: str = "new") -> Conversation:
    return Conversation(id=i, lead_id=1, account_id=1, peer_id=1000 + i, state=state,
                        sent_count=0, last_inbound_at=NOW, read_at=None)


# ── список: unread_only ───────────────────────────────────────────────────────

def _list_script(rows, facets, unread_total):
    """Скрипт сессии для списка: total, страница, лиды по строкам, чипы, значок."""
    return [[len(rows)], list(rows)] + [[None]] * len(rows) + [facets, [unread_total]]


def test_unread_only_is_on_by_default(app):
    """Дефолт проверяется по OpenAPI: схема строится из настоящей сигнатуры ручки,
    и «true» здесь — то, что применит FastAPI, а не то, что тест решил увидеть."""
    params = app.openapi()["paths"][LIST]["get"]["parameters"]
    schema = next(p for p in params if p["name"] == "unread_only")["schema"]
    assert schema["default"] is True


def test_default_list_filters_in_the_database(db, client):
    db.script = _list_script([unread_conv(101), unread_conv(104)],
                             [("new", 1), ("awaiting_reply", 1)], 2)
    body = client.get(LIST).json()

    assert body["unread_only"] is True
    assert body["total"] == 2 and [r["id"] for r in body["rows"]] == [101, 104]
    assert body["unread_total"] == 2
    # Одно условие — в total, в выборку страницы и в значок по всей базе.
    assert "read_at IS NULL" in db.queries[0]
    assert "read_at IS NULL" in db.queries[1]
    assert "read_at IS NULL" in db.queries[-1]


def test_unread_only_off_disables_the_filter_but_not_the_badge(db, client):
    db.script = _list_script([unread_conv(101), unread_conv(102), unread_conv(103),
                              unread_conv(104)], [("new", 2)], 2)
    body = client.get(LIST, params={"unread_only": "false"}).json()

    assert body["unread_only"] is False
    assert body["total"] == 4 and len(body["rows"]) == 4
    assert body["unread_total"] == 2
    assert "read_at IS NULL" not in db.queries[1]
    assert "read_at IS NULL" in db.queries[-1]


def test_unknown_state_is_refused(client):
    assert client.get(LIST, params={"state": "испорчено"}).status_code == 422


# ── нитка ─────────────────────────────────────────────────────────────────────

def events_ascending() -> list[ConversationEvent]:
    """События с идентичной секундой у пары: без ключа `id` вторым Postgres волен
    вернуть эту пару в любом порядке, и переписка перевернётся."""
    same = NOW - HOUR
    return [ConversationEvent(id=1, kind="inbound", payload={"text": "старое"},
                              created_at=NOW - 2 * HOUR),
            ConversationEvent(id=2, kind="outbound", payload={"text": "первое из той же секунды"},
                              created_at=same),
            ConversationEvent(id=3, kind="outbound", payload={"text": "второе из той же секунды"},
                              created_at=same),
            ConversationEvent(id=4, kind="inbound", payload={"text": "новое"},
                              created_at=NOW)]


def test_thread_comes_ascending_by_time(db, client):
    c = unread_conv(101)
    db.script = [[c], events_ascending(), [None]]
    body = client.get(f"{LIST}/101").json()

    stamps = [e["created_at"] for e in body["events"]]
    assert stamps == sorted(stamps)
    # Пара одной секунды — по id, а не как попало.
    assert [e["id"] for e in body["events"]] == [1, 2, 3, 4]
    # Порядок запрошен у базы, а не отсортирован после: полный журнал нитки
    # сортировать на клиенте — значит однажды показать его перевёрнутым.
    assert ("ORDER BY conversation_events.created_at ASC, "
            "conversation_events.id ASC") in db.queries[1]


def test_thread_header_carries_the_peer_and_counters(db, client):
    lead = SimpleNamespace(author_name="Иван", author_username="ivan")
    c = unread_conv(101, state="awaiting_reply")
    c.sent_count = 3
    db.script = [[c], [], [lead]]
    header = client.get(f"{LIST}/101").json()["conversation"]

    assert header["peer_name"] == "Иван" and header["peer_username"] == "@ivan"
    assert header["account"] == 1 and header["state"] == "awaiting_reply"
    assert header["sent_count"] == 3 and header["unread"] is True
    assert header["read_at"] is None


def test_unknown_thread_is_404(db, client):
    db.script = [[]]
    assert client.get(f"{LIST}/999").status_code == 404


# ── отметка о прочтении ───────────────────────────────────────────────────────

def test_read_sets_read_at(db, client):
    c = unread_conv(101)
    db.script = [[c]]
    body = client.post(f"{LIST}/101/read").json()

    assert body["read_at"] is not None and body["unread"] is False
    assert c.read_at is not None
    assert db.commits == 1


def test_read_moves_forward_when_new_inbound_arrived(db, client, monkeypatch):
    """После нового входящего диалог снова непрочитан, и повторное прочтение —
    основной случай: отметка обязана двигаться, а не застревать первой.

    Время подменяется через `clock` — ровно то, для чего эта точка и существует:
    реальный `utcnow()` между двумя отправками не сдвинулся бы даже на секунду.
    """
    c = unread_conv(101)
    db.script = [[c], [c]]

    t1 = datetime.now(timezone.utc)
    monkeypatch.setattr(clock, "utcnow", lambda: t1)
    client.post(f"{LIST}/101/read")
    first = c.read_at

    c.last_inbound_at = t1 + timedelta(seconds=30)  # пришло новое входящее
    assert c.unread is True

    monkeypatch.setattr(clock, "utcnow", lambda: t1 + timedelta(minutes=1))
    body = client.post(f"{LIST}/101/read").json()
    assert c.read_at > first and body["unread"] is False


def test_read_unknown_is_404(db, client):
    db.script = [[]]
    assert client.post(f"{LIST}/999/read").status_code == 404


# ── доступ ────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("path,method", [
    (LIST, "get"),
    (f"{LIST}/101", "get"),
    (f"{LIST}/101/read", "post"),
])
def test_anonymous_gets_nothing(anon, path, method):
    assert getattr(anon, method)(path).status_code == 401


@pytest.mark.parametrize("path,method", [
    (f"{LIST}/101", "get"),
    (f"{LIST}/101/read", "post"),
])
def test_guest_is_refused_for_the_new_routes(app, db, path, method):
    """Права у нитки и отметки — те же, что у списка: раздел внутренний, гость
    его не видит. Ослабить здесь значило бы показать переписку тому, кому закрыт
    сам раздел."""
    app.dependency_overrides[db_session] = lambda: db
    app.dependency_overrides[current_user] = lambda: VIEWER
    c = TestClient(app, raise_server_exceptions=False)
    assert getattr(c, method)(path).status_code == 403
    app.dependency_overrides.clear()
