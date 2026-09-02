"""Пакетная сборка ссылок на источник: та же правда, что поштучно, но без N+1.

Строка черновика показывает две ссылки — на комментарий и на пост, под которым он
написан. Считать их по одной значит сходить в базу дважды на строку: на странице в
пятьдесят строк это сотня лишних запросов, и растёт она молча, вместе с размером
страницы.

Поэтому у `source_links` есть пакетный брат. Опасность здесь ровно одна и она тихая:
пакетная версия расходится с поштучной на каком-нибудь краю — на комментарии без
известного поста, на приватной группе, на чужом канале, — и черновики начинают
показывать ссылку, которой в поштучной проверке никогда не было. Такое расхождение не
всплывает нигде: обе ветки «работают».

Поэтому главный тест здесь — не «пакет вернул что-то разумное», а «пакет вернул РОВНО
то же, что поштучная функция, на всех краях сразу».

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
os.environ.setdefault("RADAR_DEBUG", "true")

from app.db.models import Base, Channel, Message  # noqa: E402
from app.services.drafting import source_links, source_links_many  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

NOW = datetime(2026, 9, 3, 10, 0, tzinfo=timezone.utc)
CHANNEL_PEER = -1001111111111
GROUP_PEER = -1002222222222
PRIVATE_PEER = -1003333333333
POST_ID = 4242


async def _seed(maker):
    """Все краевые случаи разом, в двух разных чатах.

    Два чата намеренно: пакетная версия обязана держать привязку «корень ветки ищется
    в ТОМ ЖЕ канале». Ветка с номером 900 есть и в группе, и в закрытом чате — если
    пакет ищет корни по одному только `thread_id`, он перепутает их между чатами, а
    поштучная версия нет.
    """
    async with maker() as db:
        news = Channel(peer_id=CHANNEL_PEER, username="news", title="Новости",
                       chat_type="channel", linked_chat_peer_id=GROUP_PEER)
        group = Channel(peer_id=GROUP_PEER, username="newschat", title="Новости · чат",
                        chat_type="supergroup")
        closed = Channel(peer_id=PRIVATE_PEER, username=None, title="Закрытый",
                         chat_type="supergroup")
        db.add_all([news, group, closed])
        await db.flush()

        def msg(channel, tg_id, **kw):
            base = dict(channel_id=channel.id, tg_message_id=tg_id, tg_date=NOW,
                        author_peer_id=500, author_username="user", author_name="Имя",
                        author_is_bot=False, is_automatic_forward=False,
                        text="сообщение", processed_at=NOW)
            base.update(kw)
            return Message(**base)

        rows = [
            # 1. комментарий под известным постом публичного канала
            msg(group, 900, is_automatic_forward=True, author_peer_id=None,
                author_username=None, forward_from_chat_id=CHANNEL_PEER,
                forward_from_message_id=POST_ID),
            msg(group, 901, thread_id=900, reply_to_message_id=900),
            # 2. комментарий, у корня которого номера поста нет (старые данные)
            msg(group, 800, is_automatic_forward=True, author_peer_id=None,
                author_username=None),
            msg(group, 801, thread_id=800, reply_to_message_id=800),
            # 3. обычная реплика без ветки
            msg(group, 910),
            # 4. тот же номер ветки, но в ДРУГОМ чате и с другим постом
            msg(closed, 900, is_automatic_forward=True, author_peer_id=None,
                author_username=None, forward_from_chat_id=CHANNEL_PEER,
                forward_from_message_id=7777),
            msg(closed, 901, thread_id=900, reply_to_message_id=900),
            # 5. комментарий под постом канала, которого нет в реестре
            msg(group, 600, is_automatic_forward=True, author_peer_id=None,
                author_username=None, forward_from_chat_id=-1009999999999,
                forward_from_message_id=77),
            msg(group, 601, thread_id=600, reply_to_message_id=600),
        ]
        db.add_all(rows)
        await db.commit()
        return {"group": group.id, "closed": closed.id}


def fresh(fn):
    async def main():
        engine = create_async_engine(DB_URL, poolclass=None)
        try:
            async with engine.begin() as conn:
                await conn.execute(text("DROP SCHEMA public CASCADE"))
                await conn.execute(text("CREATE SCHEMA public"))
                await conn.run_sync(Base.metadata.create_all)
            maker = async_sessionmaker(engine, expire_on_commit=False)
            ids = await _seed(maker)
            return await fn(maker, ids)
        finally:
            await engine.dispose()
    return asyncio.run(main())


# Что именно проверяем — по (канал, номер сообщения).
CASES = [("group", 901), ("group", 801), ("group", 910),
         ("closed", 901), ("group", 601)]


def test_the_batch_agrees_with_the_one_by_one_version_on_every_edge():
    """Единственная защита от тихого расхождения двух веток одного расчёта."""
    async def scenario(maker, ids):
        async with maker() as db:
            targets = []
            for key, tg_id in CASES:
                channel = (await db.execute(select(Channel).where(
                    Channel.id == ids[key]))).scalar_one()
                message = (await db.execute(select(Message).where(
                    Message.channel_id == ids[key],
                    Message.tg_message_id == tg_id))).scalar_one()
                targets.append((channel, message))

            one_by_one = [await source_links(db, c, m) for c, m in targets]
            batched = await source_links_many(db, targets)
            return one_by_one, batched

    one_by_one, batched = fresh(scenario)
    assert len(batched) == len(one_by_one)
    for (key, tg_id), expected, got in zip(CASES, one_by_one, batched):
        assert got == expected, f"расхождение на {key}/{tg_id}: {got} против {expected}"


def test_the_batch_does_not_confuse_threads_from_different_chats():
    """Номер ветки уникален внутри чата, а не глобально. Поиск корня только по
    `thread_id` увёл бы комментарий закрытого чата на пост из группы."""
    async def scenario(maker, ids):
        async with maker() as db:
            out = {}
            for key, tg_id in (("group", 901), ("closed", 901)):
                channel = (await db.execute(select(Channel).where(
                    Channel.id == ids[key]))).scalar_one()
                message = (await db.execute(select(Message).where(
                    Message.channel_id == ids[key],
                    Message.tg_message_id == tg_id))).scalar_one()
                out[key] = (channel, message)
            return await source_links_many(db, [out["group"], out["closed"]])

    in_group, in_closed = fresh(scenario)
    assert in_group["post_link"] == f"https://t.me/news/{POST_ID}"
    assert in_closed["post_link"] == "https://t.me/news/7777"


def test_an_empty_batch_asks_nothing_and_returns_nothing():
    async def scenario(maker, ids):
        async with maker() as db:
            return await source_links_many(db, [])

    assert fresh(scenario) == []


def test_the_batch_keeps_the_order_it_was_given():
    """Строки страницы сопоставляются с ответом по позиции — перестановка тихо
    приклеила бы ссылки одного черновика к другому."""
    async def scenario(maker, ids):
        async with maker() as db:
            targets = []
            for key, tg_id in [("group", 910), ("group", 901)]:
                channel = (await db.execute(select(Channel).where(
                    Channel.id == ids[key]))).scalar_one()
                message = (await db.execute(select(Message).where(
                    Message.channel_id == ids[key],
                    Message.tg_message_id == tg_id))).scalar_one()
                targets.append((channel, message))
            return await source_links_many(db, targets)

    plain, comment = fresh(scenario)
    assert plain["is_comment"] is False
    assert comment["is_comment"] is True
