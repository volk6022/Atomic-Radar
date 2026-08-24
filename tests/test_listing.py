"""Общие параметры листингов.

Главное здесь — две вещи, которые ломаются молча: сортировка по неизвестному полю
(экран показывает не тот порядок, о котором просил) и сортировка без устойчивого
тай-брейка (строки прыгают между страницами).
"""
from __future__ import annotations

import pytest
from fastapi import HTTPException
from sqlalchemy import select

from app.api.v1.listing import (MAX_LIMIT, ListParams, apply_search, apply_sort,
                                list_params)
from app.db.models import Message


def params(**kw) -> ListParams:
    base = {"limit": 50, "offset": 0, "sort": None, "order": "desc", "q": None}
    return ListParams(**{**base, **kw})


ALLOWED = {"date": Message.tg_date, "author": Message.author_name}


# ── сортировка ────────────────────────────────────────────────────────────────

def test_unknown_sort_field_is_rejected_not_ignored():
    """Молчаливый откат к сортировке по умолчанию — худший из вариантов: экран
    показывает не тот порядок, и никто этого не замечает."""
    with pytest.raises(HTTPException) as e:
        apply_sort(select(Message), params(sort="password"), ALLOWED,
                   default="date", tiebreak=Message.id)
    assert e.value.status_code == 422
    # В тексте перечислены доступные поля: иначе клиенту остаётся угадывать.
    assert "date" in e.value.detail and "author" in e.value.detail


def test_sort_field_never_reaches_sql_as_a_string():
    """Имя колонки приходит из браузера. В запрос должен уходить найденный по нему
    объект колонки, а не сама строка."""
    stmt = apply_sort(select(Message), params(sort="author"), ALLOWED,
                      default="date", tiebreak=Message.id)
    compiled = str(stmt)
    assert "author_name" in compiled
    assert "password" not in compiled


def test_tiebreak_is_always_appended():
    """Без него пагинация по неуникальной колонке неустойчива: строки с одинаковым
    значением Postgres волен вернуть в любом порядке, и часть покажется дважды."""
    stmt = apply_sort(select(Message), params(sort="author"), ALLOWED,
                      default="date", tiebreak=Message.id)
    order = str(stmt).split("ORDER BY")[1]
    assert "author_name" in order and "messages.id" in order


def test_order_direction_is_honoured():
    asc = str(apply_sort(select(Message), params(sort="date", order="asc"), ALLOWED,
                         default="date", tiebreak=Message.id)).split("ORDER BY")[1]
    desc = str(apply_sort(select(Message), params(sort="date", order="desc"), ALLOWED,
                          default="date", tiebreak=Message.id)).split("ORDER BY")[1]
    assert "tg_date DESC" not in asc
    assert "tg_date DESC" in desc


def test_default_sort_applies_when_none_requested():
    stmt = apply_sort(select(Message), params(), ALLOWED,
                      default="date", tiebreak=Message.id)
    assert "tg_date" in str(stmt).split("ORDER BY")[1]


# ── разбор параметров ─────────────────────────────────────────────────────────

# `list_params` — зависимость FastAPI: при прямом вызове значения по умолчанию не
# подставляются, вместо них приходят объекты `Query`. Поэтому в тестах параметры
# передаются явно — иначе проверка направления сработает на объекте `Query`, тест
# упадёт не там, где ожидалось, а «ожидали исключение» пройдёт по ложной причине.
def call(**kw):
    base = {"limit": 50, "offset": 0, "sort": None, "order": "desc", "q": None}
    return list_params(**{**base, **kw})


def test_unknown_order_is_rejected():
    with pytest.raises(HTTPException) as e:
        call(order="ASC; DROP TABLE messages")
    assert e.value.status_code == 422


def test_blank_query_becomes_none():
    """Пустая строка из поля поиска не должна превращаться в `ILIKE '%%'`."""
    assert call(q="   ").q is None
    assert call(q=" прокси ").q == "прокси"


def test_page_envelope_is_the_same_for_every_list():
    assert params(limit=25, offset=50).page(total=300) == {
        "total": 300, "limit": 25, "offset": 50}


# ── поиск ─────────────────────────────────────────────────────────────────────

def test_search_covers_every_given_column():
    """`ilike()` компилируется в `lower(col) LIKE lower(:param)` — важно, что
    условий ровно столько, сколько колонок, и они объединены через OR."""
    stmt = apply_search(select(Message), params(q="прокси"),
                        [Message.text, Message.author_name])
    compiled = str(stmt)
    assert compiled.count("LIKE lower(") == 2
    assert " OR " in compiled


def test_search_without_query_changes_nothing():
    plain = select(Message)
    assert str(apply_search(plain, params(), [Message.text])) == str(plain)


def test_limit_ceiling_is_declared():
    """Потолок защищает не сервер, а экран: 5000 строк таблица уже не рисует.

    Сам отказ выдаёт FastAPI по `le=MAX_LIMIT` в объявлении параметра, до входа в
    наш код, — поэтому здесь проверяется значение, а не поведение при прямом вызове.
    """
    assert MAX_LIMIT == 500
