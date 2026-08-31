"""Пять состояний обсуждения обязаны отличаться друг от друга (FIXES.md #3).

29.08 Андрей написал, что на части каналов «не подхватил чаты». Разбор показал, что
одинаково выглядели три разные вещи: у канала нет группы обсуждения, группа есть и
не прочитана, группа прочитана — но разово, потому что аккаунт в ней не состоит и
живых комментариев Telegram ему не шлёт. На экране все три были «ноль сообщений»,
и поэтому чинилось бы не то.

Тест держит именно это различение, а не форму словаря.
"""
import os
from datetime import datetime, timezone

os.environ.setdefault("RADAR_SECRET_KEY", "x" * 32)
os.environ.setdefault("RADAR_DATABASE_URL", "postgresql+asyncpg://u:p@localhost/db")
os.environ.setdefault("RADAR_INGEST_TOKEN", "t" * 24)

from app.services.discussions import discussion_state  # noqa: E402


class Row:
    """Строка `channels` ровно в тех полях, которые читает `discussion_state`."""

    def __init__(self, id, username=None, linked=None, checked=None, joined=None):
        self.id = id
        self.username = username
        self.linked_chat_username = linked
        self.linked_checked_at = checked
        self.linked_joined_at = joined


NOW = datetime(2026, 8, 31, 12, 0, tzinfo=timezone.utc)


def test_never_asked_is_not_the_same_as_no_group():
    """Пустой `linked_chat_username` без отметки об опросе — «не спрашивали».

    Это и есть та неоднозначность, из-за которой пункт 3 полгода читался как
    «не прошёл backfill»: у 149 каналов из 220 группы действительно нет, и это
    не поломка, а у остальных её просто никто не искал.
    """
    assert discussion_state(Row(1), {}, {})["state"] == "unknown"
    assert discussion_state(Row(1, checked=NOW), {}, {})["state"] == "none"


def test_group_known_but_never_read():
    channel = Row(1, username="corpostrovokru", linked="corpostrovokru_chat",
                  checked=NOW)
    group = Row(2, username="corpostrovokru_chat")
    state = discussion_state(channel, {"corpostrovokru_chat": group}, {})
    assert state["state"] == "unread"
    assert state["channel_id"] == 2


def test_group_row_missing_entirely_is_also_unread():
    """Строки группы в Радаре может не быть вовсе — так было у 61 из 71 пары.

    Кнопка «Запустить бэкфилл всем» на экране Channels перебирает существующие
    строки и до таких групп не добирается в принципе: их ещё предстоит завести.
    """
    channel = Row(1, username="klientvsprav", linked="klientyt", checked=NOW)
    assert discussion_state(channel, {}, {})["state"] == "unread"


def test_history_read_is_not_live_until_the_account_joins():
    """Прочитанная история и живой поток — разные вещи.

    `get_chat_history` публичную супергруппу отдаёт и постороннему, а апдейты
    Telegram шлёт только участнику. Ровно поэтому 28.08 восемь чатов получили по
    500 сообщений одной пачкой и с тех пор молчали.
    """
    channel = Row(1, username="corpostrovokru", linked="corpostrovokru_chat",
                  checked=NOW)
    group = Row(2, username="corpostrovokru_chat")
    counts = {2: 500}
    assert discussion_state(channel, {"corpostrovokru_chat": group},
                            counts)["state"] == "history"

    group.linked_joined_at = NOW
    assert discussion_state(channel, {"corpostrovokru_chat": group},
                            counts)["state"] == "live"


def test_group_is_matched_case_insensitively():
    """Имя канала в Telegram регистронезависимо, а в базе лежит как пришло:
    `CentrVED` и `CentrVED_chat` ссылаются друг на друга в любом написании."""
    channel = Row(1, username="CentrVED", linked="CentrVED_chat", checked=NOW)
    group = Row(2, username="centrved_chat")
    state = discussion_state(channel, {"centrved_chat": group}, {2: 2000})
    assert state["channel_id"] == 2
    assert state["messages"] == 2000


def test_last_message_travels_as_iso():
    """Дата последнего комментария — единственный способ увидеть на экране, что
    группа прочитана, но давно замолчала."""
    channel = Row(1, username="a", linked="a_chat", checked=NOW)
    group = Row(2, username="a_chat")
    state = discussion_state(channel, {"a_chat": group}, {2: 10}, {2: NOW})
    assert state["last_message_at"] == NOW.isoformat()
