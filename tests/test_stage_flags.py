"""Перевод пары (уровень, итог) в четыре галочки экрана потока.

В базе на весь каскад два поля, а экран рисует вердикт по каждой ступени. Правило
перевода одно на всё приложение и живёт в `screens._stage_flags`; здесь оно закрыто
тестом, потому что ошибиться в нём легко и незаметно: сообщение, отсеянное на L2,
при неверном правиле выглядит так, будто оно не прошло и L0, — а это ровно тот экран,
который отвечает на вопрос «почему это не стало лидом».
"""
from __future__ import annotations

from app.api.v1.screens import _stage_flags


def test_dropped_at_l0():
    assert _stage_flags(0, False) == {"l0": False, "l1": None, "l2": None, "l3": None}


def test_dropped_at_l1_keeps_l0_passed():
    assert _stage_flags(1, False) == {"l0": True, "l1": False, "l2": None, "l3": None}


def test_dropped_at_l2_keeps_l0_and_l1_passed():
    """Главный случай ради которого правило и написано: отсев на L2 не должен
    выглядеть как провал ранних ступеней."""
    assert _stage_flags(2, False) == {"l0": True, "l1": True, "l2": False, "l3": None}


def test_passed_everything():
    assert _stage_flags(3, True) == {"l0": True, "l1": True, "l2": True, "l3": True}


def test_stopped_because_next_stage_is_off():
    """L2 выключен: L0 и L1 пройдены, дальше не пошло. Ступени выше — «не дошло»,
    а не «не прошло»; текст причины объяснит, что именно выключено."""
    assert _stage_flags(1, True) == {"l0": True, "l1": True, "l2": None, "l3": None}


def test_in_flight_looks_the_same_as_stopped():
    """`passed is None` — сообщение ждёт следующую ступень. Пройденные ступени
    остаются пройденными: недосчитанный L2 не отменяет того, что L1 сработал."""
    assert _stage_flags(1, None) == {"l0": True, "l1": True, "l2": None, "l3": None}
    assert _stage_flags(2, None) == {"l0": True, "l1": True, "l2": True, "l3": None}


def test_never_processed():
    assert _stage_flags(None, None) == {"l0": None, "l1": None, "l2": None, "l3": None}
