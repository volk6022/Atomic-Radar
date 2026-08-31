"""Разбор групп обсуждения на живом Postgres (FIXES.md #3).

Проверяется то, ради чего прогон и написан: канал и его группа — две независимые
строки `channels`, и после разбора между ними обязана появиться связь в обе стороны,
а история группы — оказаться в базе. Плюс три случая, которые на списке из шестидесяти
штук встречаются каждый раз и не должны ронять прогон целиком: у канала нет
обсуждения, канал не отвечает, строка вообще без username.

Engage подменяется на уровне `engage.action` / `engage.wait_for_task`: проверять
здесь его HTTP незачем, а вот то, что мы правильно читаем ЕГО ответ, — как раз то,
что 31.08 оказалось сломанным в соседней ручке (`payload={"username"}` вместо
`target`).

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
"""
from __future__ import annotations

import asyncio
import os

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_INGEST_TOKEN", "test-ingest-token")

from app.core.config import get_settings  # noqa: E402
from app.db.models import Base, Channel, EngageInstance, Message, Workflow  # noqa: E402
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.services import discussions, engage  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

# Настоящая пара из прода: канал «Островок Командировки» и его чат. Именно на ней
# 29.08 Андрей увидел комментарии под постом, которых не было в Радаре.
CHANNEL = {"found": True, "type": "channel", "peer_id": -100_1, "username": "corpostrovokru",
           "title": "Островок Командировки", "members_count": 12000,
           "linked_chat_username": "corpostrovokru_chat"}
GROUP = {"found": True, "type": "supergroup", "peer_id": -100_2,
         "username": "corpostrovokru_chat", "title": "Островок Командировки Chat",
         "members_count": 3400, "linked_chat_username": "corpostrovokru"}
LONELY = {"found": True, "type": "channel", "peer_id": -100_3, "username": "zloytam",
          "title": "Злой таможенник", "members_count": 900,
          "linked_chat_username": None}


def _posts(start: int, count: int) -> list[dict]:
    return [{"message_id": start - i, "date": "2026-08-28T09:51:00Z",
             "text": f"комментарий {start - i}", "from_user_id": 500 + i,
             "from_username": f"user{i}", "from_first_name": "Имя"}
            for i in range(count)]


async def _reset() -> None:
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
        await db.flush()
        db.add(Workflow(key="cold_dm", title="Личные сообщения", target_kind="user",
                        action="dm", visibility="private", engage_use_case="cold_dm",
                        engage_instance_id=inst.id,
                        cascade_profile="dm_v1", sort_order=10, is_active=True))
        db.add(Channel(peer_id=CHANNEL["peer_id"], username="corpostrovokru",
                       title="Островок Командировки", ingest_enabled=True))
        db.add(Channel(peer_id=LONELY["peer_id"], username="zloytam",
                       title="Злой таможенник", ingest_enabled=True))
        db.add(Channel(peer_id=-100_4, username=None, title="Без имени",
                       ingest_enabled=True))
        await db.commit()
    await engine.dispose()


@pytest.fixture()
def db_ready():
    os.environ["RADAR_DATABASE_URL"] = DB_URL
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_session_maker.cache_clear()
    asyncio.run(_reset())
    yield


def _stub_engage(monkeypatch, *, info_by_name: dict, pages: dict, calls: list):
    """Подменить Engage: `action` только регистрирует задачу, результат отдаёт
    `wait_for_task` по её номеру. Ровно та же двухшаговая форма, что и у настоящего."""
    tasks: dict[str, dict] = {}

    async def action(*, account_id, action, payload, webhook_url, **kw):
        calls.append((account_id, action, dict(payload)))
        task_id = f"t{len(tasks) + 1}"
        if action == "get_chat_info":
            tasks[task_id] = info_by_name.get(payload["username"],
                                              {"found": False, "reason": "username_not_found"})
        elif action == "get_chat_history":
            key = payload.get("username") or payload.get("peer_id")
            queue = pages.setdefault(key, [])
            tasks[task_id] = {"found": True, "posts": queue.pop(0) if queue else []}
        else:
            raise AssertionError(f"разбор не должен звать {action}")
        return {"task_id": task_id}

    async def wait_for_task(task_id, **kw):
        return tasks[task_id]

    monkeypatch.setattr(engage, "action", action)
    monkeypatch.setattr(engage, "wait_for_task", wait_for_task)


async def _channels() -> dict[str, Channel]:
    async with get_session_maker()() as db:
        rows = (await db.execute(select(Channel))).scalars().all()
        return {(c.username or f"id{c.id}"): c for c in rows}


async def _message_count(username: str) -> int:
    async with get_session_maker()() as db:
        return (await db.execute(
            select(func.count(Message.id)).join(Channel, Message.channel_id == Channel.id)
            .where(Channel.username == username))).scalar_one()


async def test_scan_links_channel_to_its_group_and_reads_history(db_ready, monkeypatch):
    calls: list = []
    _stub_engage(monkeypatch,
                 info_by_name={"corpostrovokru": CHANNEL,
                               "corpostrovokru_chat": GROUP,
                               "zloytam": LONELY},
                 pages={"corpostrovokru_chat": [_posts(2000, 40)]},
                 calls=calls)

    async with get_session_maker()() as db:
        ids = await discussions.select_channels(db, scope="all", channel_ids=None)

    notes: list = []

    async def report(pct, note):
        notes.append(note)

    stats = await discussions.scan(channel_ids=ids, account_ids=[1, 2], target=40,
                                   report=report, cancelled=lambda: False)

    rows = await _channels()
    channel, group = rows["corpostrovokru"], rows["corpostrovokru_chat"]

    # Связь в обе стороны — до этого прогона её не было ни у одного из 108 каналов.
    assert channel.linked_chat_username == "corpostrovokru_chat"
    assert channel.linked_chat_peer_id == GROUP["peer_id"]
    assert group.linked_chat_peer_id == CHANNEL["peer_id"]
    assert channel.chat_type == "channel" and group.chat_type == "supergroup"
    assert channel.linked_checked_at is not None

    assert await _message_count("corpostrovokru_chat") == 40
    assert stats["messages"] == 40
    assert stats["groups_linked"] == 1

    # Канал без обсуждения — не отказ. Таких 149 из 220 опрошенных 28.08, и красный
    # крест над каждым сделал бы список нечитаемым.
    assert stats["no_group"] == 1
    assert rows["zloytam"].linked_checked_at is not None

    # Строка без username пропускается с причиной, а не падает: карточку по peer_id
    # у чужого канала не спросить — аккаунт этот пир ещё не знает.
    assert stats["skipped"] == 1
    assert stats["done"] == stats["total"] == 3
    assert any("пропущен" in n for n in notes)


async def test_history_is_not_reread_when_the_group_is_already_full(db_ready, monkeypatch):
    """Повторный разбор не должен перечитывать то, что уже лежит в базе.

    Идемпотентно оно и так — сообщения кладутся upsert'ом, — но каждая лишняя
    страница тратит дневной бюджет чтений аккаунта, а он общий на весь флот.
    """
    calls: list = []
    _stub_engage(monkeypatch,
                 info_by_name={"corpostrovokru": CHANNEL, "corpostrovokru_chat": GROUP,
                               "zloytam": LONELY},
                 pages={"corpostrovokru_chat": [_posts(2000, 30)]}, calls=calls)
    async with get_session_maker()() as db:
        ids = await discussions.select_channels(db, scope="all", channel_ids=None)

    async def report(pct, note):
        return None

    await discussions.scan(channel_ids=ids, account_ids=[1], target=30,
                           report=report, cancelled=lambda: False)
    first = [c for c in calls if c[1] == "get_chat_history"]
    assert len(first) == 1

    calls.clear()
    await discussions.scan(channel_ids=ids, account_ids=[1], target=30,
                           report=report, cancelled=lambda: False)
    assert [c for c in calls if c[1] == "get_chat_history"] == []


async def test_one_unreachable_channel_does_not_stop_the_rest(db_ready, monkeypatch):
    """Приватная группа или опечатка в имени — обычное событие на списке из
    шестидесяти. Прогон, падающий из-за одной строки, пришлось бы запускать заново,
    перечитывая уже прочитанное."""
    calls: list = []
    _stub_engage(monkeypatch,
                 info_by_name={"corpostrovokru": CHANNEL, "corpostrovokru_chat": GROUP},
                 pages={"corpostrovokru_chat": [_posts(2000, 10)]}, calls=calls)

    async with get_session_maker()() as db:
        ids = await discussions.select_channels(db, scope="all", channel_ids=None)

    async def report(pct, note):
        return None

    stats = await discussions.scan(channel_ids=ids, account_ids=[1], target=10,
                                   report=report, cancelled=lambda: False)
    assert stats["failed"] == 1          # @zloytam не нашёлся
    assert stats["groups_linked"] == 1   # но пара разобрана
    assert await _message_count("corpostrovokru_chat") == 10
