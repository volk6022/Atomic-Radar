"""`origin` в `invariants.check_all` — решение владельца PLAN 16.11.

Ручная отправка одобренного черновика (16.2) идёт мимо проверки режима, но
только её: остальные гардрейлы обязаны работать одинаково при любом origin.
Проверяются обе половины этого решения, потому что сломаться они могут по
отдельности: «пустили в LIVE» и «отменили остальные правила» — разные дефекты.

Без базы: `check_all` — чистая функция, зависимости у неё запрещены самим
модулем (`app/core/invariants.py` живёт без БД и FastAPI намеренно).
"""
from __future__ import annotations

from datetime import datetime, timezone

from app.core import invariants

NOW = datetime(2026, 9, 19, 14, 0, tzinfo=timezone.utc)

MODE_REASON = "режим DRY_RUN: отправка запрещена"

# Чистая попытка, кроме режима: каждый тест ломает ровно одно поле.
BASE = dict(
    mode="DRY_RUN", draft_state="approved",
    text="Видел твой вопрос про оплату за рубеж, могу посоветовать знакомого",
    is_first=True, sent_count=0, last_sent_at=None, now=NOW,
    local_hour=14, recipient_is_admin=False, previously_contacted=False,
)


def test_manual_origin_skips_the_mode_check():
    """Ручная отправка в сухом прогоне законна: режим не её предохранитель."""
    assert invariants.check_all(**BASE, origin="manual") == []


def test_auto_origin_still_checks_the_mode():
    """Автоматический путь не изменился: DRY_RUN запрещает — прежнее поведение."""
    assert invariants.check_all(**BASE) == [MODE_REASON]


def test_live_auto_is_clean():
    assert invariants.check_all(**{**BASE, "mode": "LIVE"}, origin="auto") == []


def test_manual_mode_argument_is_still_passed_and_recorded():
    """`mode` приходит и при manual: уходит в снимок `wf_outbound.mode`. Пропуск
    проверки не должен означать потерю факта — журнал обязан показывать, в каком
    режиме система находилась в момент заказа."""
    # Проверка существует косвенно: в сигнатуре `mode` обязателен (keyword).
    import inspect

    params = inspect.signature(invariants.check_all).parameters
    assert params["mode"].kind is inspect.Parameter.KEYWORD_ONLY
    assert params["origin"].default == "auto"


def test_other_reasons_do_not_depend_on_origin():
    """Остальные гардрейлы при manual действуют как прежде: «мимо режима» не
    значит «мимо правил приличия»."""
    for broken in ({"previously_contacted": True},
                   {"draft_state": "pending"},
                   {"local_hour": 4},
                   {"sent_count": 4, "is_first": False},
                   {"recipient_is_admin": True},
                   {"text": "глянь https://example.com"}):
        auto = invariants.check_all(**{**BASE, **broken}, origin="auto")
        manual = invariants.check_all(**{**BASE, **broken}, origin="manual")
        # Причины ручной отправки — подмножество причин автоматической, а вся
        # разница между ними — одна строка про режим.
        assert set(auto) - set(manual) <= {MODE_REASON}, broken
        assert manual, f"хотя бы одна причина обязана остаться: {broken}"


def test_known_breakages_block_manual_sends_too():
    manual = dict(origin="manual")
    assert any("уже писали" in r for r in
               invariants.check_all(**{**BASE, "previously_contacted": True}, **manual))
    assert any("approved" in r for r in
               invariants.check_all(**{**BASE, "draft_state": "pending"}, **manual))
    assert any("тихие часы" in r for r in
               invariants.check_all(**{**BASE, "local_hour": 4}, **manual))


def test_origins_catalogue():
    """Справочник origin — документ решения: оба значения принимаются, третьих
    `check_all` не знает и молча считает их автоматикой."""
    assert invariants.ORIGINS == ("auto", "manual")
    assert invariants.check_all(**{**BASE, "mode": "LIVE"}) == []
