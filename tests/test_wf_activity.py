"""Активность сценария — то, что можно проверить без базы.

Проверяемого здесь мало, и это не случайность: ручка почти целиком состоит из
агрегатов, а агрегат без данных ничего не значит. Зато две вещи ломаются тише всего
и базы для проверки не требуют.

Первая — **ряд дней**. Ряд на день короче или на день сдвинутый выглядит на графике
как совершенно нормальный график: у него та же форма, те же столбики, просто числа
приписаны не тем суткам. Отдельная функция `_window_dates` заведена ровно ради
возможности сказать про неё «ровно столько строк, последняя — сегодня».

Вторая — **объявление границ `days`**. Границы живут в `Query(...)`, то есть в
схеме, а не в теле; забыть их значит согласиться на `days=100000`, и обнаружится это
не отказом, а очень долгим запросом.

Отдельно закреплено, что ручка сообщает состояние ступени словами. Ноль отправленного
и «отправлять некому» — разные ответы, и подмена первого вторым уже была бы регрессией
контракта, а не оформлением.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_DEBUG", "true")

from app.api.v1.wf_queues import (  # noqa: E402
    AUTOMATIC_SENDING, RECENT_LIMIT, RECENT_TEXT_CHARS, SENDING_NOTE,
    _outbound_status, _window_dates, _window_start)
from app.core.access import Section  # noqa: E402
from app.core.config import get_settings  # noqa: E402
from app.db.models import WfOutbound  # noqa: E402
from app.main import create_app  # noqa: E402

PATH = "/api/v1/workflows/{key}/activity"
NOW = datetime(2026, 8, 26, 20, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def app():
    get_settings.cache_clear()
    return create_app()


@pytest.fixture(scope="module")
def client(app):
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


# ── ряд дней ──────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("days", [1, 2, 7, 30, 90])
def test_the_row_has_exactly_as_many_days_as_asked(days):
    """Дырок в ряду быть не должно: пустой день — это данные, а его отсутствие экран
    покажет как сдвинутый график."""
    dates = _window_dates(NOW, days)
    assert len(dates) == days
    assert len(set(dates)) == days


def test_the_row_ends_today_and_runs_forward():
    dates = _window_dates(NOW, 7)
    assert dates[-1] == NOW.date()
    assert dates == sorted(dates)
    assert (dates[-1] - dates[0]).days == 6


def test_the_row_matches_the_agreed_example():
    """Дословно пример из контракта: окно 19.08 20:00 → 26.08 20:00, первая строка
    ряда — 20 августа. Ряд короче окна на неполные первые сутки, и это решение, а не
    обсчёт: показать заведомо неполный день наравне с полными — соврать в графике."""
    assert _window_dates(NOW, 7)[0].isoformat() == "2026-08-20"


def test_the_row_crosses_a_month_boundary_without_arithmetic_of_its_own():
    dates = _window_dates(datetime(2026, 3, 2, 5, 0, tzinfo=timezone.utc), 4)
    assert [d.isoformat() for d in dates] == ["2026-02-27", "2026-02-28",
                                              "2026-03-01", "2026-03-02"]


# ── границы запроса ───────────────────────────────────────────────────────────

def test_days_bounds_are_declared_in_the_schema(app):
    """Границы живут в схеме, а не в теле: без них `days=100000` отвечает не отказом,
    а очень долгим запросом."""
    spec = app.openapi()["paths"][PATH]["get"]
    days = [p for p in spec["parameters"] if p["name"] == "days"][0]
    schema = days["schema"]
    assert schema["default"] == 7
    assert schema["minimum"] == 1
    assert schema["maximum"] == 90


def test_the_endpoint_is_wired_to_its_own_section(app):
    """Раздел у ручки свой, а не одолженный у соседей: иначе гость, которому закрыты
    переписки, увидел бы то же самое через блок публичного ответа."""
    assert Section.ACTIVITY.value == "activity"
    assert PATH in app.openapi()["paths"]


def test_anonymous_gets_nothing(client):
    assert client.get(PATH.format(key="cold_dm")).status_code == 401


# ── состояние ступени ─────────────────────────────────────────────────────────

def test_the_absence_of_a_sender_is_stated_rather_than_shown_as_a_zero():
    """Пустой журнал и отсутствующий отправитель — разные утверждения. Вычисляй ручка
    флаг по данным, первая же строка в `wf_outbound` объявила бы контур работающим."""
    assert AUTOMATIC_SENDING is False
    assert "отправитель не заведён" in SENDING_NOTE


def _attempt(allowed: bool = True, **kw) -> WfOutbound:
    return WfOutbound(workflow_id=1, allowed=allowed, mode="dry_run", **kw)


def test_an_attempt_that_nobody_blocked_is_not_called_blocked():
    """Три исхода, а не два. «Гейт пустил, но доставки нет» — это сухой прогон, и
    слить его с отказом гейта значило бы объявить заблокированным то, что никто не
    блокировал."""
    assert _outbound_status(_attempt(delivered_message_id=77)) == "доставлено"
    assert _outbound_status(_attempt()) == "отправка не подтверждена"
    assert _outbound_status(_attempt(allowed=False)) == "заблокировано гейтом"


def test_the_feed_stays_a_tail_rather_than_an_export():
    assert RECENT_LIMIT == 30
    assert RECENT_TEXT_CHARS == 200


@pytest.mark.parametrize("days", [1, 7, 30, 90])
def test_the_window_opens_at_the_midnight_of_the_first_daily_row(days):
    """Начало окна и начало ряда — один момент, иначе столбики не сложатся в плитку.

    Проверяется здесь, без базы, потому что сломать это можно молча: окно, сдвинутое
    на несколько часов внутрь суток, отличается от правильного только тем, что часть
    первого дня не попадает в сводки, а в ряду этот день стоит полным.
    """
    start = _window_start(NOW, days)
    assert start.date() == _window_dates(NOW, days)[0]
    assert (start.hour, start.minute, start.second, start.microsecond) == (0, 0, 0, 0)
    assert start.utcoffset() == timedelta(0)
    assert start <= NOW
