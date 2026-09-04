"""Вступление в группы обсуждения списком (план 1.6, шаг 7).

История публичной супергруппы Telegram отдаётся кому угодно, а живые апдейты — только
участнику. Поэтому 43 группы на проде читаются разовой выгрузкой и дальше молчат:
вступлений не делал никто. Подключение НОВОГО канала вступает само (FIXES.md #7), а
для уже заведённых такой дороги не было вовсе — её этот прогон и заводит.

Что проверяется по пунктам постановки:

* **только открытые группы.** Вступаем по `@username`; строка без имени — закрытая
  группа, куда пришлось бы проситься заявкой. Такие не трогаем вовсе.
* **только вступаем, историю не читаем.** Единственное действие прогона —
  `join_group`. Любой `get_chat_history` здесь ошибка, а не оптимизация.
* **лимиты Engage.** У `public_reply` `joins_per_day: 3`; прогон режет пачку по этому
  потолку сам, а не полагается на отказ Engage — Engage сверх лимита не отказывает,
  а откладывает.
* **кто вступил — записано.** Дальше именно этим аккаунтом пойдёт бэкфилл и с него же
  будет виден живой поток.

Engage подменяется на уровне `engage.action` / `engage.wait_for_task` — та же
двухшаговая форма, что и в `test_discussions_scan_db.py`.

⚠️ Один `asyncio.run` на тест. Движок и пул кешируются между вызовами
(`get_engine.cache_clear()` зовётся только в фикстуре), и второй `asyncio.run` в том
же тесте достаёт из пула соединение, привязанное к закрытому циклу.

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_INGEST_TOKEN", "test-ingest-token")

from app.core.config import get_settings  # noqa: E402
from app.db.models import (Base, Channel, EngageInstance, Message,  # noqa: E402
                           MessageReader, Workflow)
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.services import discussions, engage  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

NOW = datetime(2026, 9, 5, 12, 0, tzinfo=timezone.utc)

# Пять каналов с публичными группами обсуждения — ровно то состояние, в котором прод
# стоит сейчас: история прочитана, `linked_joined_at` пуст у всех.
PAIRS = [("corpostrovokru", "corpostrovokru_chat"),
         ("zloytam", "zloytam_chat"),
         ("buhpravo", "buhpravo_chat"),
         ("nalogi_msb", "nalogi_msb_chat"),
         ("tender_pro", "tender_pro_chat")]
GROUP_NAMES = [group for _, group in PAIRS]


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
        db.add(Workflow(key="public_reply", title="Публичные ответы",
                        target_kind="message", action="reply", visibility="public",
                        engage_use_case="public_reply", engage_instance_id=inst.id,
                        cascade_profile="public_v1", sort_order=20, is_active=True))

        peer = -100_000
        for i, (channel_name, group_name) in enumerate(PAIRS):
            peer += 1
            db.add(Channel(peer_id=peer, username=channel_name,
                           title=f"Канал {channel_name}", chat_type="channel",
                           linked_chat_username=group_name, linked_checked_at=NOW,
                           members=10_000 - i, ingest_enabled=True))
            peer += 1
            group = Channel(peer_id=peer, username=group_name,
                            title=f"Чат {group_name}", chat_type="supergroup",
                            linked_chat_username=channel_name, linked_checked_at=NOW,
                            members=1_000 - i, ingest_enabled=True)
            db.add(group)
            await db.flush()
            # История уже прочитана — иначе группа стоит в состоянии «не читаем», а
            # не «читаем, но не участвуем».
            db.add(Message(channel_id=group.id, tg_message_id=1 + i, tg_date=NOW,
                           text="комментарий", author_peer_id=500 + i))

        # Закрытая группа: имени нет, вступить по нему нельзя. Отслеживается — то
        # есть попадёт в выборку по любому признаку, кроме правильного.
        peer += 1
        db.add(Channel(peer_id=peer, username=None, title="Закрытый чат",
                       chat_type="supergroup", ingest_enabled=True))
        # Группа, в которую уже вступили: повторное вступление — потраченный лимит.
        peer += 1
        db.add(Channel(peer_id=peer, username="already_in_chat", title="Уже там",
                       chat_type="supergroup", ingest_enabled=True,
                       linked_joined_at=NOW))
        # Снятый с отслеживания чат: оператор его выключил, лезть туда незачем.
        peer += 1
        db.add(Channel(peer_id=peer, username="disabled_chat", title="Выключен",
                       chat_type="supergroup", ingest_enabled=False))
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


def _stub_engage(monkeypatch, calls: list, *, fail: dict | None = None):
    """Engage, который умеет ровно одно действие. Любое другое — падение теста, а не
    молчаливый пропуск: «вступили и заодно дочитали» здесь запрещено."""
    fail = fail or {}
    tasks: dict[str, str] = {}

    async def action(*, account_id, action, payload, webhook_url, **kw):
        assert action == "join_group", f"прогон вступлений позвал {action}"
        target = payload["target"]
        calls.append((account_id, target))
        task_id = f"t{len(tasks) + 1}"
        tasks[task_id] = target
        return {"task_id": task_id}

    async def wait_for_task(task_id, **kw):
        target = tasks[task_id]
        if target in fail:
            raise fail[target]
        return {"found": True, "chat_id": None, "target": target}

    monkeypatch.setattr(engage, "action", action)
    monkeypatch.setattr(engage, "wait_for_task", wait_for_task)


async def _report(pct, note):
    return None


async def _selected() -> list[int]:
    async with get_session_maker()() as db:
        return await discussions.select_groups_to_join(db, scope="pending",
                                                       channel_ids=None)


async def _run(*, account_ids: list[int], per_account: int = 3) -> dict:
    group_ids = await _selected()
    return await discussions.join_groups(
        group_ids=group_ids, account_ids=account_ids, per_account=per_account,
        subscribed_by="ivan@test", report=_report, cancelled=lambda: False)


async def _groups() -> dict[str, Channel]:
    async with get_session_maker()() as db:
        rows = (await db.execute(select(Channel))).scalars().all()
        return {(c.username or f"id{c.id}"): c for c in rows}


def test_only_open_groups_we_are_not_in_are_selected(db_ready):
    async def go():
        ids = await _selected()
        async with get_session_maker()() as db:
            return sorted((await db.execute(
                select(Channel.username).where(Channel.id.in_(ids)))).scalars().all())

    assert asyncio.run(go()) == sorted(GROUP_NAMES), (
        "в выборку должны попасть только публичные группы, куда мы ещё не вступали")


def test_join_marks_the_group_row_so_the_screen_says_live(db_ready, monkeypatch):
    """Главная проверка: тот, кто пишет отметку, и тот, кто её читает, должны иметь
    в виду одну и ту же строку.

    `discussion_state` смотрит `linked_joined_at` У СТРОКИ ГРУППЫ, и состояние канала
    становится `live` только по ней. Записать отметку в строку канала — значит
    вступить по-настоящему и оставить экран показывать «история».
    """
    calls: list = []
    _stub_engage(monkeypatch, calls)

    async def go():
        stats = await _run(account_ids=[1, 2], per_account=3)
        return stats, await _groups()

    stats, groups = asyncio.run(go())
    assert stats["joined"] == 5, stats
    for group_name in GROUP_NAMES:
        assert groups[group_name].linked_joined_at is not None, group_name
        assert groups[group_name].subscribed_account_id in (1, 2), group_name
        assert groups[group_name].subscribed_by == "ivan@test", group_name

    by_username = {g.username.lower(): g for g in groups.values() if g.username}
    counts = {g.id: 1 for g in groups.values()}
    for channel_name, _ in PAIRS:
        state = discussions.discussion_state(groups[channel_name], by_username, counts)
        assert state["state"] == "live", (channel_name, state)


def test_nothing_but_join_is_ordered(db_ready, monkeypatch):
    """Историю не читаем: заглушка Engage падает на любом другом действии, а
    заказанных вступлений ровно столько же, сколько групп."""
    calls: list = []
    _stub_engage(monkeypatch, calls)
    asyncio.run(_run(account_ids=[1, 2], per_account=3))
    assert sorted(target for _, target in calls) == sorted(GROUP_NAMES)


def test_daily_cap_bounds_the_batch(db_ready, monkeypatch):
    """Один аккаунт при `joins_per_day: 3` берёт три группы, а не пять. Остальные
    остаются в очереди на завтра — так пачка и растягивается на дни."""
    calls: list = []
    _stub_engage(monkeypatch, calls)

    async def go():
        stats = await _run(account_ids=[7], per_account=3)
        return stats, await _selected()

    stats, left = asyncio.run(go())
    assert stats["joined"] == 3, stats
    assert stats["left"] == 2, stats
    assert len(calls) == 3
    assert {account for account, _ in calls} == {7}
    assert len(left) == 2, (
        "вступившие группы обязаны уйти из очереди, иначе завтрашний прогон потратит "
        "лимит на них ещё раз")


def test_one_refusal_does_not_stop_the_rest(db_ready, monkeypatch):
    """Закрытая группа, флуд-контроль, опечатка — обычные события на списке из сорока
    штук. Прогон, падающий целиком из-за одной, пришлось бы запускать заново."""
    calls: list = []
    _stub_engage(monkeypatch, calls, fail={
        "buhpravo_chat": engage.EngageTaskFailed("нельзя", code="channels_too_much")})

    async def go():
        stats = await _run(account_ids=[1, 2, 3], per_account=3)
        return stats, await _groups()

    stats, groups = asyncio.run(go())
    assert stats["joined"] == 4, stats
    assert stats["failed"] == 1, stats
    assert groups["buhpravo_chat"].linked_joined_at is None, (
        "неудачное вступление не должно помечаться как состоявшееся")


def test_deferred_task_is_not_counted_as_joined(db_ready, monkeypatch):
    """Engage при исчерпанном бюджете не отказывает — он ОТКЛАДЫВАЕТ задачу. Дождаться
    такой нельзя, и считать её вступлением тем более нельзя: аккаунт в группе не
    появится, а экран показал бы «живая»."""
    calls: list = []
    _stub_engage(monkeypatch, calls, fail={
        "zloytam_chat": engage.EngageTaskDeferred("отложена", code="BUDGET_ACCOUNT")})

    async def go():
        stats = await _run(account_ids=[1], per_account=3)
        return stats, await _groups()

    stats, groups = asyncio.run(go())
    assert stats["deferred"] == 1, stats
    assert stats["joined"] == 2, stats
    assert groups["zloytam_chat"].linked_joined_at is None


def test_the_account_that_read_the_group_joins_it(db_ready, monkeypatch):
    """Вступает тот, кто эту группу и читал.

    Смысл не в аккуратности: живой поток пойдёт через вступивший аккаунт, а Андрей
    пишет из одного. Если группу читал третий, а вступил первый, наводка и ответ
    разъедутся по аккаунтам — та же причина, по которой в очереди черновиков
    появилась колонка аккаунта приёма.
    """
    calls: list = []
    _stub_engage(monkeypatch, calls)

    async def go():
        async with get_session_maker()() as db:
            groups = {c.username: c for c in (
                await db.execute(select(Channel))).scalars().all()}
            for group_name, account_id in (("corpostrovokru_chat", 4),
                                           ("zloytam_chat", 2)):
                message = (await db.execute(select(Message).where(
                    Message.channel_id == groups[group_name].id))).scalar_one()
                db.add(MessageReader(message_id=message.id, account_id=account_id))
            await db.commit()
        await _run(account_ids=[1, 2, 3, 4, 5], per_account=3)

    asyncio.run(go())
    by_target = {target: account for account, target in calls}
    assert by_target["corpostrovokru_chat"] == 4
    assert by_target["zloytam_chat"] == 2
