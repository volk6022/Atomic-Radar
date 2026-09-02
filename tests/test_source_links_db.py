"""Куда ведёт ссылка из черновика: на комментарий, на пост, или никуда.

Постановка Ивана дословно: «найти это сообщение в группе — это одно. Найти это
сообщение в телеграм-канале, когда в группу вход ещё не был, это совсем другое».

Разница практическая. Лид в публичном сценарии — это комментарий под постом канала.
Ссылка на сам комментарий открывает ГРУППУ ОБСУЖДЕНИЯ, а человек, который в ней не
состоит, попадает в тупик: он видит чужой чат без контекста. Ссылка на пост открывает
канал, где виден повод.

Поэтому ссылок две, и обе честные: «где лежит комментарий» и «под каким постом». Плюс
явная пометка, что это комментарий, — иначе по одной ссылке не отличить.

⚠️ Id поста внутри канала берётся из `forward_from_message_id` корня ветки. kurigram
отдаёт его через `forward_origin`, и Engage научился это передавать отдельной правкой.
**У сообщений, приехавших до неё, поля нет** — и тогда `post_link` обязан быть None, а
не собранным наугад: ссылка «куда-то» хуже отсутствия ссылки, потому что по ней пойдут.

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_DEBUG", "true")

from app.db.models import Base, Channel, Message  # noqa: E402
from app.services.drafting import source_links  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

NOW = datetime(2026, 9, 3, 10, 0, tzinfo=timezone.utc)

CHANNEL_PEER = -1001111111111   # сам канал, где выходит пост
GROUP_PEER = -1002222222222     # его группа обсуждения, где лежат комментарии
PRIVATE_GROUP_PEER = -1003333333333
POST_ID = 4242                  # номер поста ВНУТРИ канала
ROOT_IN_GROUP = 900             # тот же пост, отзеркаленный в группу
COMMENT_ID = 901


async def _seed():
    """Канал, его группа обсуждения и несколько сообщений разной природы."""
    engine = create_async_engine(DB_URL, poolclass=None)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)

    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        news = Channel(peer_id=CHANNEL_PEER, username="news", title="Новости",
                       chat_type="channel", linked_chat_peer_id=GROUP_PEER,
                       linked_chat_username="newschat")
        group = Channel(peer_id=GROUP_PEER, username="newschat", title="Новости · чат",
                        chat_type="supergroup")
        closed = Channel(peer_id=PRIVATE_GROUP_PEER, username=None,
                         title="Закрытый чат", chat_type="supergroup")
        db.add_all([news, group, closed])
        await db.flush()

        def msg(channel, tg_id, **kw):
            base = dict(channel_id=channel.id, tg_message_id=tg_id, tg_date=NOW,
                        author_peer_id=500, author_username="user", author_name="Имя",
                        author_is_bot=False, is_automatic_forward=False,
                        text="сообщение", processed_at=NOW)
            base.update(kw)
            return Message(**base)

        # Корень ветки: пост канала, автоматически отзеркаленный в группу. Именно он
        # знает, каким номером пост лежит в самом канале.
        root = msg(group, ROOT_IN_GROUP, is_automatic_forward=True,
                   author_peer_id=None, author_username=None,
                   forward_from_chat_id=CHANNEL_PEER, forward_from_message_id=POST_ID)
        comment = msg(group, COMMENT_ID, thread_id=ROOT_IN_GROUP,
                      reply_to_message_id=ROOT_IN_GROUP)
        # Обычная реплика в группе, не под постом.
        plain = msg(group, 910)
        # Комментарий, чей корень приехал до правки Engage: связь известна, номер
        # поста в канале — нет.
        old_root = msg(group, 800, is_automatic_forward=True,
                       author_peer_id=None, author_username=None)
        old_comment = msg(group, 801, thread_id=800, reply_to_message_id=800)
        # То же самое, но в закрытой группе без юзернейма.
        closed_root = msg(closed, 700, is_automatic_forward=True,
                          author_peer_id=None, author_username=None,
                          forward_from_chat_id=CHANNEL_PEER,
                          forward_from_message_id=POST_ID)
        closed_comment = msg(closed, 701, thread_id=700, reply_to_message_id=700)
        # Комментарий под постом канала, которого нет в нашем реестре каналов.
        alien_root = msg(group, 600, is_automatic_forward=True,
                         author_peer_id=None, author_username=None,
                         forward_from_chat_id=-1009999999999,
                         forward_from_message_id=77)
        alien_comment = msg(group, 601, thread_id=600, reply_to_message_id=600)

        db.add_all([root, comment, plain, old_root, old_comment,
                    closed_root, closed_comment, alien_root, alien_comment])
        await db.commit()

        return {"group": group.id, "closed": closed.id,
                "comment": COMMENT_ID, "plain": 910, "old": 801,
                "closed_comment": 701, "alien": 601}, engine, maker


def with_seed(fn):
    async def main():
        ids, engine, maker = await _seed()
        try:
            return await fn(ids, maker)
        finally:
            await engine.dispose()
    return asyncio.run(main())


async def _links(maker, channel_id, tg_message_id):
    from sqlalchemy import select
    async with maker() as db:
        channel = (await db.execute(
            select(Channel).where(Channel.id == channel_id))).scalar_one()
        message = (await db.execute(
            select(Message).where(Message.channel_id == channel_id,
                                  Message.tg_message_id == tg_message_id))).scalar_one()
        return await source_links(db, channel, message)


# ── комментарий под постом ────────────────────────────────────────────────────

def test_a_comment_under_a_post_is_marked_as_one():
    def body(ids, maker):
        return _links(maker, ids["group"], ids["comment"])
    out = with_seed(body)
    assert out["is_comment"] is True


def test_a_comment_links_both_to_itself_and_to_the_post():
    def body(ids, maker):
        return _links(maker, ids["group"], ids["comment"])
    out = with_seed(body)
    assert out["comment_link"] == f"https://t.me/newschat/{COMMENT_ID}"
    assert out["post_link"] == f"https://t.me/news/{POST_ID}"


def test_the_post_link_names_the_channel_so_the_reader_knows_where_it_leads():
    def body(ids, maker):
        return _links(maker, ids["group"], ids["comment"])
    out = with_seed(body)
    assert out["post_channel"] == "Новости"


# ── краевые случаи, где соврать легче всего ───────────────────────────────────

def test_a_comment_whose_post_id_we_never_received_has_no_post_link():
    """Сообщения до правки Engage. Пометка остаётся, ссылка — нет.

    Собрать её наугад (например, подставив номер корня в группе) значило бы отправить
    человека на чужой пост: нумерация в канале и в группе разная.
    """
    def body(ids, maker):
        return _links(maker, ids["group"], ids["old"])
    out = with_seed(body)
    assert out["is_comment"] is True
    assert out["post_link"] is None
    assert out["comment_link"] == "https://t.me/newschat/801"


def test_a_plain_message_in_a_group_is_not_a_comment():
    def body(ids, maker):
        return _links(maker, ids["group"], ids["plain"])
    out = with_seed(body)
    assert out["is_comment"] is False
    assert out["post_link"] is None
    assert out["comment_link"] == "https://t.me/newschat/910"


def test_a_closed_group_still_gets_an_internal_link():
    """Приватная супергруппа: ссылка вида t.me/c/<id>/<msg>, префикс -100 отброшен."""
    def body(ids, maker):
        return _links(maker, ids["closed"], ids["closed_comment"])
    out = with_seed(body)
    assert out["comment_link"] == "https://t.me/c/3333333333/701"
    assert out["is_comment"] is True
    # Пост лежит в публичном канале — на него ссылка обычная.
    assert out["post_link"] == f"https://t.me/news/{POST_ID}"


def test_a_post_from_a_channel_we_do_not_track_still_gets_a_link():
    """Канала нет в реестре — имени не знаем, но номер и id есть, ссылка строится."""
    def body(ids, maker):
        return _links(maker, ids["group"], ids["alien"])
    out = with_seed(body)
    assert out["is_comment"] is True
    assert out["post_link"] == "https://t.me/c/9999999999/77"
    assert out["post_channel"] is None
