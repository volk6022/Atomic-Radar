"""Окно глубины обязано пережить всю цепочку страниц, а не первую.

05.09.2026, первый живой автоматический бэкфилл. Заказано «канал 166, не глубже
месяца, до 2000 сообщений». Прочитано 2032 сообщения до **30 ноября 2020 года** —
и все они прошли через каскад.

Причина ровно одна и видна в задачах Engage:

    19:05:00  {"limit": 500, "min_date": "2026-08-06T19:01:20+00:00", ...}
    19:05:04  {"limit": 500, "max_id": 2254, ...}      ← окна больше нет
    19:05:12  {"limit": 500, "max_id": 1750, ...}
    19:05:21  {"limit": 500, "max_id": 1194, ...}

`request_page` клал `min_date` в payload задачи, а цепочку двигает не наш процесс:
Engage возвращает страницу вебхуком, и продолжение собирается в `_continue_backfill`
**только из адреса возврата**. В адресе окна не было — значит со второй страницы
`min_date` был `None`, и чтение шло до `target`.

Форма ошибки знакомая: параметр объявлен, задокументирован и на одной странице
работает, поэтому и в тестах, и глазами он выглядит живым. Ловится он только
проверкой на ВТОРОЙ странице — она здесь и стоит.

Отдельно проверяется длина: адрес возврата лежит в `tasks.webhook_url VARCHAR(500)`
у Engage, и 29.08 цепочка уже вставала на переполнении этого поля.
"""
from __future__ import annotations

import asyncio
import os
from urllib.parse import parse_qs, urlparse

import pytest

os.environ.setdefault("RADAR_SECRET_KEY", "x" * 32)
os.environ.setdefault("RADAR_DATABASE_URL", "postgresql+asyncpg://u:p@localhost/db")
os.environ.setdefault("RADAR_INGEST_TOKEN", "t" * 24)
os.environ.setdefault("RADAR_SELF_BASE_URL", "http://radar.invalid")

from app.services import engage  # noqa: E402
from app.services.backfill_chain import request_page  # noqa: E402

# Ограничение чужой схемы: `tasks.webhook_url VARCHAR(500)` в fleet_manager.
ENGAGE_WEBHOOK_URL_LIMIT = 500

WINDOW = "2026-08-06T19:01:20.701049+00:00"


def _call(monkeypatch, **over) -> dict:
    seen: dict = {}

    async def action(*, account_id, action, payload, webhook_url, **kw):
        seen.update(payload=payload, webhook_url=webhook_url)
        return {"task_id": "t1"}

    monkeypatch.setattr(engage, "action", action)
    kwargs = dict(peer_id=-1001422570375, username="networkio_io", account_id=1,
                  limit=500, target=2000, max_id=0, cursor=0,
                  min_date=WINDOW, item_id=2, read0=2)
    kwargs.update(over)
    asyncio.run(request_page(**kwargs))
    return seen


def test_the_window_goes_to_engage_as_a_read_parameter(monkeypatch):
    """`min_date` — параметр самого `get_chat_history`.

    Фильтровать после чтения нельзя: страницы за пределами окна всё равно были бы
    прочитаны и списаны с дневного бюджета аккаунта.
    """
    assert _call(monkeypatch)["payload"]["min_date"] == WINDOW


def test_the_window_also_travels_in_the_callback_address(monkeypatch):
    """Тот же дефект, но с той стороны, где он и жил.

    Продолжение цепочки собирается только из адреса возврата. Нет окна в адресе —
    нет окна со второй страницы, и «не глубже месяца» превращается в «до target».
    """
    q = parse_qs(urlparse(_call(monkeypatch)["webhook_url"]).query)
    assert q.get("min_date") == [WINDOW], (
        "в адресе возврата нет границы окна: со второй страницы читать будут "
        "без ограничения по дате")


def test_a_chain_without_a_window_does_not_invent_one(monkeypatch):
    """Постановка без глубины — это «читай до target», и подставлять сюда
    умолчание значило бы молча урезать чужой заказ."""
    q = parse_qs(urlparse(_call(monkeypatch, min_date=None)["webhook_url"]).query)
    assert "min_date" not in q
    assert "min_date" not in _call(monkeypatch, min_date=None)["payload"]


def test_the_address_with_a_window_still_fits_engages_column(monkeypatch):
    """Окно добавляет к адресу полсотни символов, и это тот же предел, на котором
    29.08 встал бэкфилл @CentrVED."""
    url = _call(monkeypatch, username="CentrVED_chat", cursor=668759)["webhook_url"]
    assert len(url) < ENGAGE_WEBHOOK_URL_LIMIT, len(url)


@pytest.mark.parametrize("field", ["item_id", "read0", "prev_cursor", "target"])
def test_the_rest_of_the_chain_state_is_still_in_the_address(monkeypatch, field):
    """Правка адреса не должна выронить то, что там уже ехало: без `item_id`
    конец цепочки не знает, какой элемент очереди закрывать."""
    q = parse_qs(urlparse(_call(monkeypatch)["webhook_url"]).query)
    assert field in q
