"""Кто из аккаунтов видел сообщение — атрибуция приёма, на живом Postgres.

Зачем это нужно. Андрей 29.08: «берем аккаунт **который прочитал сообщение** и я от
его имени пишу». Ответить на «кто прочитал» сегодня нечем, хотя данные приходят:
Engage кладёт `account_id` в каждое событие вотчера
(`fleet_manager/app/watchers/_message_payload.py`), а Радар это поле выбрасывает.

Почему таблица, а не колонка. Приём идемпотентен по ключу `(channel_id,
tg_message_id)`: одно и то же сообщение, увиденное двумя аккаунтами, — **одна**
строка `messages`. Колонка «кто видел» хранила бы только последнего, и выпадающий
список аккаунтов, который просил Андрей, взять было бы неоткуда.

Что здесь закрепляется:

* оба пути приёма — реалтайм вотчера и дочитывание истории — отмечают читателя;
* повторная доставка того же сообщения тем же аккаунтом не плодит строк;
* второй аккаунт добавляет **читателя**, а не второе сообщение;
* без `account_id` (старые строки, ручной засев) читателей просто нет — это не ошибка;
* экран сообщений отдаёт читателей списком, одним запросом на страницу, а не запросом
  на строку.

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
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

from app.core.config import get_settings  # noqa: E402
from app.core.security import SessionSigner  # noqa: E402
from app.db.models import (Base, EngageInstance, Message,  # noqa: E402
                           MessageReader, User, Workflow)
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.main import create_app  # noqa: E402
from app.services import ingest as ingest_service  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

ACCOUNT_A = 4001
ACCOUNT_B = 4002
CHAT_ID = 900700
NOW = datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc)


def _event(message_id: int, account_id: int | None, text_: str = "нужен подрядчик") -> dict:
    """Конверт вотчера. Поля — те, что читает `ingest_incoming_message`."""
    body = {
        "event": "incoming_message",
        "chat_id": CHAT_ID,
        "chat_username": "@stroy_chat",
        "chat_title": "Стройка и подряд",
        "message_id": message_id,
        "message": text_,
        "from_peer_id": 700700,
        "from_first_name": "Пётр",
        "sender_username": "@petr",
        "date": "2026-09-02T12:00:00Z",
    }
    if account_id is not None:
        body["account_id"] = account_id
    return body


async def _seed() -> None:
    engine = create_async_engine(DB_URL, poolclass=None)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)

    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        inst = EngageInstance(key="test", client_label="Тестовый клиент",
                              base_url="http://engage.invalid",
                              api_key_env="RADAR_ENGAGE_API_KEY", is_active=True)
        db.add(inst)
        # Учётка заводится с непригодным хешем и фиктивным секретом, но задаются
        # оба явно: колонки NOT NULL не по недосмотру — без второго фактора эта
        # админка одобряет отправку сообщений живым людям, имея только пароль.
        db.add(User(email="owner@local", name="owner", initials="OW", role="owner",
                    password_hash="!нельзя-войти", totp_secret="X" * 32,
                    totp_confirmed=True, is_active=True))
        await db.flush()
        db.add(Workflow(
            key="cold_dm", title="Личные сообщения", target_kind="user", action="dm",
            visibility="private", engage_instance_id=inst.id, engage_use_case="cold_dm",
            cascade_profile="dm_v1", sort_order=10, is_active=True))
        await db.commit()
    await engine.dispose()


def _maker():
    engine = create_async_engine(DB_URL, poolclass=None)
    return engine, async_sessionmaker(engine, expire_on_commit=False)


async def _ingest(body: dict) -> dict:
    engine, maker = _maker()
    async with maker() as db:
        out = await ingest_service.ingest_incoming_message(db, body)
        await db.commit()
    await engine.dispose()
    return out


async def _ingest_history(posts: list[dict], account_id: int | None) -> dict:
    engine, maker = _maker()
    async with maker() as db:
        out = await ingest_service.ingest_history(
            db, chat_id=CHAT_ID, chat_username="@stroy_chat",
            chat_title="Стройка и подряд", posts=posts, account_id=account_id)
        await db.commit()
    await engine.dispose()
    return out


async def _all(model) -> list:
    """Смотрим в базу мимо приложения: ответ ручки — не доказательство записи."""
    engine, maker = _maker()
    async with maker() as db:
        out = list((await db.execute(select(model))).scalars().all())
    await engine.dispose()
    return out


@pytest.fixture(scope="module")
def client():
    previous = os.environ.get("RADAR_DATABASE_URL")
    os.environ["RADAR_DATABASE_URL"] = DB_URL
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_session_maker.cache_clear()
    asyncio.run(_seed())

    app = create_app()
    with TestClient(app, raise_server_exceptions=False) as c:
        owner = asyncio.run(_owner_id())
        c.cookies.set(get_settings().SESSION_COOKIE,
                      SessionSigner(get_settings().SECRET_KEY).dumps(
                          {"uid": owner, "totp_ok": True}))
        yield c

    if previous is None:
        os.environ.pop("RADAR_DATABASE_URL", None)
    else:
        os.environ["RADAR_DATABASE_URL"] = previous
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_session_maker.cache_clear()


async def _owner_id() -> int:
    engine, maker = _maker()
    async with maker() as db:
        uid = (await db.execute(select(User.id).where(
            User.email == "owner@local"))).scalar_one()
    await engine.dispose()
    return uid


# ── реалтайм ──────────────────────────────────────────────────────────────────

def test_incoming_message_records_reader(client):
    """Аккаунт из события вотчера попадает в читатели сообщения."""
    asyncio.run(_ingest(_event(5001, ACCOUNT_A)))

    messages = asyncio.run(_all(Message))
    readers = asyncio.run(_all(MessageReader))
    mine = [m for m in messages if m.tg_message_id == 5001]
    assert len(mine) == 1, "сообщение должно лечь ровно одной строкой"
    assert [r.account_id for r in readers if r.message_id == mine[0].id] == [ACCOUNT_A]


def test_same_message_same_account_is_not_duplicated(client):
    """Повторная доставка того же события не плодит читателей.

    Вебхуки Engage переигрываются при неподтверждённой доставке, и приём обязан быть
    идемпотентным целиком, а не только в части `messages`.
    """
    asyncio.run(_ingest(_event(5002, ACCOUNT_A)))
    asyncio.run(_ingest(_event(5002, ACCOUNT_A)))

    messages = asyncio.run(_all(Message))
    mid = [m.id for m in messages if m.tg_message_id == 5002]
    assert len(mid) == 1
    readers = [r for r in asyncio.run(_all(MessageReader)) if r.message_id == mid[0]]
    assert len(readers) == 1, "второй раз тот же аккаунт не должен добавлять строку"


def test_second_account_adds_reader_not_message(client):
    """Второй аккаунт видит то же сообщение — читателей двое, сообщение одно.

    Ровно ради этого случая «кто видел» и стало таблицей: ключ
    `(channel_id, tg_message_id)` схлопывает дубликаты, и колонка сохранила бы
    только того, кто пришёл последним.
    """
    asyncio.run(_ingest(_event(5003, ACCOUNT_A)))
    asyncio.run(_ingest(_event(5003, ACCOUNT_B)))

    messages = asyncio.run(_all(Message))
    mid = [m.id for m in messages if m.tg_message_id == 5003]
    assert len(mid) == 1, "второй читатель не должен создавать второе сообщение"
    readers = sorted(r.account_id for r in asyncio.run(_all(MessageReader))
                     if r.message_id == mid[0])
    assert readers == [ACCOUNT_A, ACCOUNT_B]


def test_event_without_account_leaves_no_readers(client):
    """Без `account_id` читателей просто нет — это не ошибка приёма.

    Старые строки и ручной засев такого поля не несут, и падать на них нельзя:
    сообщение важнее атрибуции.
    """
    out = asyncio.run(_ingest(_event(5004, None)))
    assert out.get("accepted") == 1

    messages = asyncio.run(_all(Message))
    mid = [m.id for m in messages if m.tg_message_id == 5004]
    assert len(mid) == 1
    assert [r for r in asyncio.run(_all(MessageReader)) if r.message_id == mid[0]] == []


# ── дочитывание истории ───────────────────────────────────────────────────────

def test_history_backfill_records_reader(client):
    """Бэкфилл тоже отмечает читателя: историю читает конкретный аккаунт.

    `account_id` на этом пути известен — он приходит параметром вебхука
    (`app/api/v1/ingest.py`, `_continue_backfill`).
    """
    posts = [{"message_id": 6001, "date": "2026-09-02T11:00:00Z",
              "text": "ищу админа", "from_user_id": 700701,
              "from_username": "@ivan", "from_first_name": "Иван"},
             {"message_id": 6002, "date": "2026-09-02T11:05:00Z",
              "text": "и ещё вопрос", "from_user_id": 700702,
              "from_username": "@olga", "from_first_name": "Ольга"}]
    asyncio.run(_ingest_history(posts, ACCOUNT_B))

    messages = {m.tg_message_id: m.id for m in asyncio.run(_all(Message))}
    readers = asyncio.run(_all(MessageReader))
    for tg_id in (6001, 6002):
        got = [r.account_id for r in readers if r.message_id == messages[tg_id]]
        assert got == [ACCOUNT_B], f"у сообщения {tg_id} должен быть читатель {ACCOUNT_B}"


def test_history_backfill_without_account_is_allowed(client):
    """Без аккаунта дочитывание не падает — путь используется и в тестах, и в засеве."""
    posts = [{"message_id": 6003, "date": "2026-09-02T11:10:00Z",
              "text": "без атрибуции", "from_user_id": 700703,
              "from_username": "@anon", "from_first_name": "Аноним"}]
    asyncio.run(_ingest_history(posts, None))

    messages = {m.tg_message_id: m.id for m in asyncio.run(_all(Message))}
    assert 6003 in messages
    assert [r for r in asyncio.run(_all(MessageReader))
            if r.message_id == messages[6003]] == []


# ── экран ─────────────────────────────────────────────────────────────────────

def test_messages_screen_exposes_readers(client):
    """Строка сообщения несёт список читателей.

    Списком, а не одним значением: у сообщения их может быть несколько, и кнопка
    «ЛС» обязана предложить выбор, а не угадать за человека. Пустой список —
    честный ответ «неизвестно», по нему кнопка и решает, что предложить нечего.
    """
    asyncio.run(_ingest(_event(5005, ACCOUNT_A)))
    asyncio.run(_ingest(_event(5005, ACCOUNT_B)))

    r = client.get("/api/v1/messages", params={"limit": 100})
    assert r.status_code == 200, r.text
    rows = r.json()["rows"]

    # `id` в строке — идентификатор сообщения В TELEGRAM, а не строки в базе. Это не
    # опечатка ручки, а её сложившийся контракт: экран показывает человеку номер,
    # по которому сообщение можно найти в Telegram. Менять его здесь незачем.
    by_tg = {row["id"]: row for row in rows}

    row = by_tg[5005]
    assert "readers" in row, "строка сообщения обязана нести readers"
    assert sorted(row["readers"]) == [ACCOUNT_A, ACCOUNT_B]

    lonely = by_tg[5004]
    assert lonely["readers"] == [], "без атрибуции читателей нет, а не None"


def test_readers_cost_one_query_per_page(client):
    """Читатели считаются одним запросом на страницу, а не запросом на строку.

    В `messages` девять тысяч строк в сутки с одной активной группы. Запрос на
    строку здесь уже случался в `/channels` и стоил полутора сотен запросов на
    страницу в пятьдесят строк — повторять это некуда.
    """
    seen: list[str] = []

    from sqlalchemy import event as sa_event

    engine = get_engine()
    sync_engine = engine.sync_engine

    def before(conn, cursor, statement, parameters, context, executemany):
        if "message_readers" in statement.lower():
            seen.append(statement)

    sa_event.listen(sync_engine, "before_cursor_execute", before)
    try:
        r = client.get("/api/v1/messages", params={"limit": 50})
        assert r.status_code == 200, r.text
    finally:
        sa_event.remove(sync_engine, "before_cursor_execute", before)

    assert len(seen) <= 1, (
        f"читатели запрошены {len(seen)} раз(а) на одну страницу — нужен один запрос")
