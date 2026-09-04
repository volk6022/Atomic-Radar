"""Правила автоматического дочитывания: глубина и тот самый аккаунт.

Постановка Ивана от 04.09.2026, дословно: «до 1 месяца истории + до 2000
исторических сообщений с соблюдением всех лимитов + с отслеживанием того, что
бэкфилл делается с того акка, который уже вступил в соответствующий канал/группу».

Очередь (`app/services/backfill_queue.py`) до этого умела только «поставить канал и
выдать его свободному воркеру»: глубины у элемента не было вовсе, а `account_id`
значил «пожелание, кем читать» — NULL отдавал канал любому. Для групп это неверно:
живой поток идёт через аккаунт, который в группе состоит, и читать её историю другим
— значит развести приём и ответ по разным аккаунтам (та же беда, из-за которой в
очереди черновиков появилась колонка аккаунта приёма).

Здесь зафиксированы четыре правила:

1. **глубина считается на постановке**, а не на выдаче. Элемент, простоявший неделю,
   обязан дочитать ровно то окно, которое человеку показали при нажатии;
2. **группа без вступления в очередь не ставится вовсе** — это отказ с объяснением,
   а не элемент, который потом молча упадёт у воркера;
3. **аккаунт группы не выбирается** — он тот, что вступил. Просьба о другом
   отвергается, а не исполняется молча;
4. **привязка перепроверяется при выдаче**: между постановкой и выдачей проходят
   сутки, и за это время в группу мог вступить другой аккаунт.

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
⚠️ Один `asyncio.run` на тест — движок кешируется, второй цикл достаёт из пула
соединение от закрытого (проверено на `test_discussions_join_db`).
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_INGEST_TOKEN", "test-ingest-token")

from app.core.config import get_settings  # noqa: E402
from app.db.models import BackfillItem, Base, Channel  # noqa: E402
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.services import backfill_queue as q  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

JOINED_AT = datetime(2026, 9, 4, 22, 6, tzinfo=timezone.utc)


async def _reset() -> None:
    engine = create_async_engine(DB_URL, poolclass=None)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        # Обычный канал: в него подписываются, а не вступают, — читать историю
        # может любой аккаунт.
        db.add(Channel(peer_id=-1001, username="corpostrovokru", title="Канал",
                       chat_type="channel", ingest_enabled=True))
        # Группа, в которую вступил третий аккаунт.
        db.add(Channel(peer_id=-1002, username="corpostrovokru_chat", title="Чат",
                       chat_type="supergroup", ingest_enabled=True,
                       linked_joined_at=JOINED_AT, subscribed_account_id=3,
                       subscribed_by="ivan@test"))
        # Группа, историю которой мы читали, но вступления не было.
        db.add(Channel(peer_id=-1003, username="zloytam_chat", title="Чат без нас",
                       chat_type="supergroup", ingest_enabled=True))
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


async def _ids() -> dict[str, int]:
    async with get_session_maker()() as db:
        rows = (await db.execute(select(Channel))).scalars().all()
        return {c.username: c.id for c in rows}


def test_depth_defaults_are_a_month_and_two_thousand(db_ready):
    """Умолчания названы в постановке, поэтому они и есть контракт, а не вкус.

    Граница считается ОТ МОМЕНТА ПОСТАНОВКИ: относительная глубина у элемента,
    простоявшего в очереди неделю, дала бы на неделю больше обещанного, и заметить
    это было бы нечем.
    """
    async def go():
        ids = await _ids()
        before = datetime.now(timezone.utc)
        async with get_session_maker()() as db:
            made = await q.enqueue(db, items=[{"channel_id": ids["corpostrovokru"]}],
                                   requested_by="ivan@test")
            await db.commit()
            return made[0].target, made[0].min_date, before

    target, min_date, before = asyncio.run(go())
    assert target == 2000, "потолок сообщений из постановки Ивана"
    assert min_date is not None, "глубина обязана проставляться, а не оставаться пустой"
    window = before - min_date
    assert timedelta(days=29, hours=23) < window < timedelta(days=30, hours=1), window


def test_depth_can_be_narrowed_per_item(db_ready):
    """Пачка «дочитать всем» и один канал руками — разные глубины; поэтому и
    параметр пачки, и переопределение на элементе."""
    async def go():
        ids = await _ids()
        deadline = datetime(2026, 9, 1, tzinfo=timezone.utc)
        async with get_session_maker()() as db:
            made = await q.enqueue(
                db,
                items=[{"channel_id": ids["corpostrovokru"], "target": 300,
                        "min_date": deadline}],
                requested_by="ivan@test")
            await db.commit()
            return made[0].target, made[0].min_date

    target, min_date = asyncio.run(go())
    assert target == 300
    assert min_date == datetime(2026, 9, 1, tzinfo=timezone.utc)


def test_group_without_a_join_is_refused_with_a_reason(db_ready):
    """Отказ на постановке, а не элемент, который упадёт у воркера через сутки.

    Разница видна человеку: «в группу @zloytam_chat ещё не вступали» — это
    следующий шаг, а `failed` в очереди — это разбирательство.
    """
    async def go():
        ids = await _ids()
        async with get_session_maker()() as db:
            try:
                await q.enqueue(db, items=[{"channel_id": ids["zloytam_chat"]}],
                                requested_by="ivan@test")
            except q.NotJoined as e:
                await db.rollback()
                return str(e)
            return None

    reason = asyncio.run(go())
    assert reason is not None, "группа без вступления не должна ставиться в очередь"
    assert "zloytam_chat" in reason or "вступ" in reason.lower(), reason


def test_group_is_bound_to_the_account_that_joined(db_ready):
    """Аккаунт группы не выбирают — он тот, что вступил."""
    async def go():
        ids = await _ids()
        async with get_session_maker()() as db:
            made = await q.enqueue(
                db, items=[{"channel_id": ids["corpostrovokru_chat"]}],
                requested_by="ivan@test")
            await db.commit()
            return made[0].account_id

    assert asyncio.run(go()) == 3


def test_asking_for_another_account_on_a_group_is_refused(db_ready):
    """Молча подменить аккаунт нельзя: тот, кто просил читать пятым, должен
    узнать, что группу держит третий, а не получить тихо переписанный элемент."""
    async def go():
        ids = await _ids()
        async with get_session_maker()() as db:
            try:
                await q.enqueue(
                    db,
                    items=[{"channel_id": ids["corpostrovokru_chat"], "account_id": 5}],
                    requested_by="ivan@test")
            except q.AccountMismatch as e:
                await db.rollback()
                return str(e)
            return None

    reason = asyncio.run(go())
    assert reason is not None, "просьба читать группу чужим аккаунтом — отказ"
    assert "3" in reason, "в отказе должен быть назван аккаунт, который вступил"


def test_plain_channel_keeps_the_requested_account(db_ready):
    """В канал подписываются, а не вступают: его историю Telegram отдаёт любому,
    и привязка тут была бы выдуманным ограничением."""
    async def go():
        ids = await _ids()
        async with get_session_maker()() as db:
            free = await q.enqueue(db, items=[{"channel_id": ids["corpostrovokru"]}],
                                   requested_by="ivan@test")
            await db.commit()
            return free[0].account_id

    assert asyncio.run(go()) is None


def test_take_next_rechecks_the_binding_and_does_not_hand_out_a_drifted_item(db_ready):
    """Между постановкой и выдачей проходят сутки.

    Если за это время в группу вступил другой аккаунт, элемент, привязанный к
    прежнему, выдавать нельзя: он прочитает историю аккаунтом, которого в группе
    больше нет, и живой поток с историей разъедутся. Такой элемент закрывается с
    объяснением, а очередь идёт дальше — один расхождённый канал не останавливает
    работу.
    """
    async def go():
        ids = await _ids()
        async with get_session_maker()() as db:
            await q.enqueue(db, items=[{"channel_id": ids["corpostrovokru_chat"]}],
                            requested_by="ivan@test")
            await q.enqueue(db, items=[{"channel_id": ids["corpostrovokru"],
                                        "account_id": 3}],
                            requested_by="ivan@test")
            await db.commit()

        # Группу перезаняли: вступил пятый.
        async with get_session_maker()() as db:
            group = await db.get(Channel, ids["corpostrovokru_chat"])
            group.subscribed_account_id = 5
            await db.commit()

        async with get_session_maker()() as db:
            item = await q.take_next(db, account_id=3)
            await db.commit()
            channel_id = item.channel_id if item else None

        async with get_session_maker()() as db:
            rows = {i.channel_id: i for i in (
                await db.execute(select(BackfillItem))).scalars().all()}
            return ids, channel_id, rows

    ids, taken, rows = asyncio.run(go())
    assert taken == ids["corpostrovokru"], (
        "третьему аккаунту должен достаться канал, а не группа, которую он больше "
        "не держит")
    drifted = rows[ids["corpostrovokru_chat"]]
    assert drifted.state == "failed", drifted.state
    assert drifted.error and "5" in drifted.error, drifted.error


def test_taking_an_item_counts_the_attempt(db_ready):
    """Канал, падающий каждый раз, обязан однажды закрыться, а не возвращаться в
    очередь вечно. Считать попытки больше негде: выдача — единственная точка, через
    которую элемент проходит всякий раз."""
    async def go():
        ids = await _ids()
        async with get_session_maker()() as db:
            await q.enqueue(db, items=[{"channel_id": ids["corpostrovokru"]}],
                            requested_by="ivan@test")
            await db.commit()
        async with get_session_maker()() as db:
            item = await q.take_next(db, account_id=1)
            await db.commit()
            return item.attempts

    assert asyncio.run(go()) == 1


def test_finish_records_how_much_was_actually_read(db_ready):
    """«Прочитано 0 из 2000» и «прочитано 2000» — разные итоги, и оба обязаны
    пережить ротацию логов."""
    async def go():
        ids = await _ids()
        async with get_session_maker()() as db:
            made = await q.enqueue(db, items=[{"channel_id": ids["corpostrovokru"]}],
                                   requested_by="ivan@test")
            await db.commit()
            item_id = made[0].id
        async with get_session_maker()() as db:
            await q.take_next(db, account_id=1)
            await db.commit()
        async with get_session_maker()() as db:
            done = await q.finish_item(db, item_id, state="done", read_total=1487)
            await db.commit()
            return done.read_total

    assert asyncio.run(go()) == 1487
