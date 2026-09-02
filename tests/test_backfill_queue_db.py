"""Очередь дочитывания: много каналов, разложенных по аккаунтам, с отложенным
запуском. На настоящем Postgres.

Замечания 4, 5, 6 и 7 по интерфейсу — это одна вещь, а не четыре. Сейчас дочитывание
устроено как «один канал, один прогон»: `jobs.create_external` отвергает второй запуск
через `JobBusy`, поэтому «дочитать всем» никогда не уходило дальше первого канала.

Очередь — таблица, а не список в параметрах прогона. Три причины, и все три
проверяются ниже: в неё можно добавлять, пока прогон идёт; она переживает перезапуск
процесса; и её видно снаружи — что стоит, что делается, что упало.

⚠️ Главное здесь — выдача элемента воркеру. Пул устроен как в `discussions.scan`:
параллелизм ровно по числу аккаунтов, потому что очередь у Engage поаккаунтная и два
одновременных чтения одним аккаунтом встанут друг за другом, потратив дневной бюджет
вдвое быстрее без выигрыша. Два воркера, взявшие один элемент, прочитают один канал
дважды и спишут двойной бюджет — молча. Поэтому выдача обязана быть атомарной, и на
это есть отдельный тест с настоящими параллельными сессиями.

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_DEBUG", "true")

from app.db.models import Base, BackfillItem, Channel  # noqa: E402
from app.services import backfill_queue as q  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

NOW = datetime(2026, 9, 3, 10, 0, tzinfo=timezone.utc)
ACC1, ACC2 = 101, 102


async def _fresh():
    """Чистая схема и три канала в реестре."""
    engine = create_async_engine(DB_URL, poolclass=None)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)

    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        chans = [Channel(peer_id=-1000 - i, username=f"c{i}", title=f"Канал {i}")
                 for i in range(1, 4)]
        db.add_all(chans)
        await db.commit()
        ids = [c.id for c in chans]
    return engine, maker, ids


def run(coro_fn):
    """Каждый тест — свой цикл событий и своя чистая схема."""
    async def main():
        engine, maker, ids = await _fresh()
        try:
            return await coro_fn(maker, ids)
        finally:
            await engine.dispose()
    return asyncio.run(main())


# ── постановка в очередь ──────────────────────────────────────────────────────

def test_bulk_enqueue_creates_one_row_per_channel_keeping_order():
    async def body(maker, ids):
        async with maker() as db:
            made = await q.enqueue(db, items=[{"channel_id": c} for c in ids],
                                   requested_by="owner@local")
            await db.commit()
            assert [i.channel_id for i in made] == ids
            assert [i.position for i in made] == sorted(i.position for i in made)
            assert {i.state for i in made} == {"queued"}
    run(body)


def test_the_same_channel_is_not_queued_twice_while_it_is_still_waiting():
    """Кнопка «дочитать всем», нажатая дважды, не должна удваивать работу."""
    async def body(maker, ids):
        async with maker() as db:
            await q.enqueue(db, items=[{"channel_id": ids[0]}],
                            requested_by="owner@local")
            await db.commit()
        async with maker() as db:
            again = await q.enqueue(db, items=[{"channel_id": ids[0]}],
                                    requested_by="owner@local")
            await db.commit()
            assert again == [], "канал уже стоит в очереди — второй строки быть не должно"
            total = len((await db.execute(select(BackfillItem))).scalars().all())
            assert total == 1
    run(body)


def test_a_finished_channel_can_be_queued_again():
    """Запрет на дубль — только про ожидающие. Дочитать канал ещё раз позже можно."""
    async def body(maker, ids):
        async with maker() as db:
            made = await q.enqueue(db, items=[{"channel_id": ids[0]}],
                                   requested_by="owner@local")
            await db.commit()
            await q.finish_item(db, made[0].id, state="done")
            await db.commit()
        async with maker() as db:
            again = await q.enqueue(db, items=[{"channel_id": ids[0]}],
                                    requested_by="owner@local")
            await db.commit()
            assert len(again) == 1
    run(body)


# ── отложенный запуск ─────────────────────────────────────────────────────────

def test_a_scheduled_item_is_not_due_before_its_time():
    async def body(maker, ids):
        async with maker() as db:
            await q.enqueue(db, items=[{"channel_id": ids[0]}],
                            requested_by="owner@local",
                            scheduled_for=NOW + timedelta(hours=2))
            await db.commit()
            assert await q.due(db, now=NOW) == []
    run(body)


def test_a_scheduled_item_becomes_due_when_its_time_comes():
    async def body(maker, ids):
        async with maker() as db:
            await q.enqueue(db, items=[{"channel_id": ids[0]}],
                            requested_by="owner@local",
                            scheduled_for=NOW + timedelta(hours=2))
            await db.commit()
            due = await q.due(db, now=NOW + timedelta(hours=3))
            assert [i.channel_id for i in due] == [ids[0]]
    run(body)


def test_an_item_without_a_schedule_is_due_at_once():
    async def body(maker, ids):
        async with maker() as db:
            await q.enqueue(db, items=[{"channel_id": ids[0]}],
                            requested_by="owner@local")
            await db.commit()
            assert len(await q.due(db, now=NOW)) == 1
    run(body)


# ── выдача воркерам ───────────────────────────────────────────────────────────

def test_two_workers_never_get_the_same_item():
    """Гонка, ради которой очередь и лежит в базе.

    Две ПАРАЛЛЕЛЬНЫЕ сессии просят работу одновременно. Если выдача не атомарна,
    обе получат первый элемент: канал прочитается дважды и спишет двойной дневной
    бюджет, причём совершенно молча.
    """
    async def body(maker, ids):
        async with maker() as db:
            await q.enqueue(db, items=[{"channel_id": c} for c in ids[:2]],
                            requested_by="owner@local")
            await db.commit()

        async def take(account_id):
            async with maker() as db:
                item = await q.take_next(db, account_id=account_id, now=NOW)
                await db.commit()
                return item.channel_id if item else None

        a, b = await asyncio.gather(take(ACC1), take(ACC2))
        assert a is not None and b is not None, "работы хватало на обоих"
        assert a != b, f"один и тот же канал выдан дважды: {a}"
    run(body)


def test_taking_an_item_marks_it_running_and_records_the_account():
    async def body(maker, ids):
        async with maker() as db:
            await q.enqueue(db, items=[{"channel_id": ids[0]}],
                            requested_by="owner@local")
            await db.commit()
        async with maker() as db:
            item = await q.take_next(db, account_id=ACC1, now=NOW)
            await db.commit()
            assert item.state == "running"
            assert item.account_id == ACC1
            assert item.started_at is not None
    run(body)


def test_an_item_pinned_to_one_account_is_not_given_to_another():
    """Канал, заведённый под конкретный аккаунт, обязан читаться именно им:
    аккаунт может состоять в группе, а другой — нет."""
    async def body(maker, ids):
        async with maker() as db:
            await q.enqueue(db, items=[{"channel_id": ids[0], "account_id": ACC1}],
                            requested_by="owner@local")
            await db.commit()
        async with maker() as db:
            assert await q.take_next(db, account_id=ACC2, now=NOW) is None
            await db.commit()
        async with maker() as db:
            got = await q.take_next(db, account_id=ACC1, now=NOW)
            await db.commit()
            assert got is not None and got.channel_id == ids[0]
    run(body)


def test_a_scheduled_item_is_not_handed_out_early():
    async def body(maker, ids):
        async with maker() as db:
            await q.enqueue(db, items=[{"channel_id": ids[0]}],
                            requested_by="owner@local",
                            scheduled_for=NOW + timedelta(hours=1))
            await db.commit()
        async with maker() as db:
            assert await q.take_next(db, account_id=ACC1, now=NOW) is None
    run(body)


def test_an_empty_queue_hands_out_nothing_rather_than_failing():
    async def body(maker, ids):
        async with maker() as db:
            assert await q.take_next(db, account_id=ACC1, now=NOW) is None
    run(body)


# ── отмена и отказы ───────────────────────────────────────────────────────────

def test_a_waiting_item_can_be_cancelled():
    async def body(maker, ids):
        async with maker() as db:
            made = await q.enqueue(db, items=[{"channel_id": ids[0]}],
                                   requested_by="owner@local")
            await db.commit()
            item = await q.cancel(db, made[0].id)
            await db.commit()
            assert item.state == "canceled"
            assert await q.take_next(db, account_id=ACC1, now=NOW) is None
    run(body)


def test_a_running_item_cannot_be_cancelled_by_the_queue():
    """Работа уже отдана Engage. Пометить её «отменённой» здесь — соврать экрану."""
    async def body(maker, ids):
        async with maker() as db:
            made = await q.enqueue(db, items=[{"channel_id": ids[0]}],
                                   requested_by="owner@local")
            await db.commit()
        async with maker() as db:
            taken = await q.take_next(db, account_id=ACC1, now=NOW)
            await db.commit()
            with pytest.raises(q.ItemNotWaiting):
                await q.cancel(db, taken.id)
    run(body)


def test_one_failed_channel_does_not_stop_the_rest():
    """Приватная группа, опечатка, флуд-контроль — обычные события на списке из
    шестидесяти штук. Очередь, встающая из-за одного, бесполезна."""
    async def body(maker, ids):
        async with maker() as db:
            await q.enqueue(db, items=[{"channel_id": c} for c in ids],
                            requested_by="owner@local")
            await db.commit()
        async with maker() as db:
            first = await q.take_next(db, account_id=ACC1, now=NOW)
            await q.finish_item(db, first.id, state="failed", error="приватная группа")
            await db.commit()
        async with maker() as db:
            nxt = await q.take_next(db, account_id=ACC1, now=NOW)
            await db.commit()
            assert nxt is not None and nxt.channel_id != first.channel_id
    run(body)


def test_a_failed_item_keeps_the_reason():
    async def body(maker, ids):
        async with maker() as db:
            made = await q.enqueue(db, items=[{"channel_id": ids[0]}],
                                   requested_by="owner@local")
            await db.commit()
            await q.finish_item(db, made[0].id, state="failed", error="нет доступа")
            await db.commit()
        async with maker() as db:
            row = (await db.execute(select(BackfillItem))).scalars().one()
            assert row.state == "failed"
            assert row.error == "нет доступа"
            assert row.finished_at is not None
    run(body)


# ── что показывать на экране ──────────────────────────────────────────────────

def test_summary_counts_states_and_the_split_across_accounts():
    """Экрану нужны две вещи разом: сколько чего в очереди и чем занят каждый аккаунт."""
    async def body(maker, ids):
        async with maker() as db:
            await q.enqueue(db, items=[{"channel_id": c} for c in ids],
                            requested_by="owner@local")
            await db.commit()
        async with maker() as db:
            await q.take_next(db, account_id=ACC1, now=NOW)
            await db.commit()
        async with maker() as db:
            s = await q.summary(db)
            assert s["states"]["queued"] == 2
            assert s["states"]["running"] == 1
            assert s["by_account"][ACC1]["running"] == 1
    run(body)
