"""Общие параметры листингов: страница, сортировка, поиск.

Каждый табличный экран задаёт одни и те же вопросы — покажи следующую страницу,
отсортируй по этой колонке, найди вот это. Раньше каждая ручка отвечала на них
по-своему или не отвечала вовсе, и экраны получались разными: где-то пятьдесят
строк без продолжения, где-то весь список целиком.

Здесь три вещи, которые в каждой ручке одинаковы и потому не должны писаться заново.
"""
from __future__ import annotations

from dataclasses import dataclass

from fastapi import HTTPException, Query, status
from sqlalchemy import or_

# Потолок страницы. Не защита от злоупотребления, а защита экрана: 500 строк
# таблица ещё рисует, 5000 — уже нет.
MAX_LIMIT = 500

ORDERS = ("asc", "desc")


@dataclass(frozen=True)
class ListParams:
    limit: int
    offset: int
    sort: str | None
    order: str
    q: str | None

    def page(self, total: int) -> dict:
        """Конверт ответа. Одинаковый у всех списков, чтобы фронтенд не разбирался,
        какой именно из них он сейчас читает."""
        return {"total": total, "limit": self.limit, "offset": self.offset}


def list_params(
    limit: int = Query(50, ge=1, le=MAX_LIMIT),
    offset: int = Query(0, ge=0),
    sort: str | None = Query(None),
    order: str = Query("desc"),
    q: str | None = Query(None),
) -> ListParams:
    """Зависимость FastAPI: разбирает общие параметры и проверяет направление."""
    if order not in ORDERS:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            f"порядок «{order}» неизвестен, ожидается asc или desc")
    return ListParams(limit=limit, offset=offset, sort=sort, order=order,
                      q=(q or "").strip() or None)


def apply_sort(stmt, p: ListParams, allowed: dict, *, default: str, tiebreak):
    """Сортировка по белому списку колонок.

    Имя колонки приходит из браузера, поэтому в SQL уходит не оно, а найденный по
    нему объект колонки: подставить в `order_by` произвольную строку — прямой путь
    к инъекции. Неизвестное имя — ошибка запроса, а не молчаливый откат к
    сортировке по умолчанию: иначе экран покажет не тот порядок, о котором просил,
    и никто этого не заметит.

    `tiebreak` обязателен и добавляется всегда. Без него сортировка по неуникальной
    колонке (скор, статус, дата без времени) даёт неустойчивую пагинацию: строки с
    одинаковым значением Postgres волен возвращать в любом порядке, и при переходе
    на следующую страницу часть из них показывается второй раз, а часть исчезает.
    """
    key = p.sort or default
    col = allowed.get(key)
    if col is None:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            f"сортировка по «{key}» недоступна, возможные поля: "
            f"{', '.join(sorted(allowed))}")
    primary = col.desc() if p.order == "desc" else col.asc()
    return stmt.order_by(primary, tiebreak.desc())


def apply_search(stmt, p: ListParams, columns: list):
    """Поиск подстроки без учёта регистра по нескольким колонкам сразу.

    `ILIKE '%…%'` не использует обычный btree-индекс и на сотнях тысяч строк
    потребует GIN по триграммам (`pg_trgm`). На нынешних 12 тысячах это не нужно,
    но когда понадобится — менять придётся здесь одно место, а не в каждой ручке.
    """
    if not p.q or not columns:
        return stmt
    needle = f"%{p.q}%"
    return stmt.where(or_(*[c.ilike(needle) for c in columns]))
