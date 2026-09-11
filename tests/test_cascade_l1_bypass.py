"""Правило L2 для мимо-словарных (`pos_min`) и его проведение через `classify`.

По плану приёмки (TESTS-cascade, T2/T8) дом этих проверок —
`tests/test_cascade_l2_l3.py`, но задача classify разрешает только
`tests/test_cascade.py` и один новый файл, поэтому правило и его применение к
обходному пути живут здесь. Без базы и без сети: косинусы и ответ модели
приезжают готовыми — проверяется ровно то, что решает `core/cascade.py`.

Мутационное свойство набора: уберите параметр `pos_min` у `level2` или
`l1_bypass` у `classify` — тесты этого файла краснеют (`TypeError`), то есть
списание нового пути на «умолчания» невозможно.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.core import cascade

NOW = datetime(2026, 9, 11, 12, 0, tzinfo=timezone.utc)


def classify(text, **kw):
    kw.setdefault("is_automatic_forward", False)
    kw.setdefault("author_is_bot", False)
    kw.setdefault("author_peer_id", 777)
    kw.setdefault("author_username", "someone")
    kw.setdefault("tg_date", NOW - timedelta(hours=1))
    return cascade.classify(text=text, now=NOW, **kw)


# Длинный текст без единого якоря боли — единственный класс, который проходит
# мимо словаря: у обычных сообщений `pos_min` не появляется никогда.
LONG_NO_ANCHOR = "ведомость не сходится с декларацией, " + "а" * 220


# ── обычное правило отрыва не сдвинулось ни на волос ──────────────────────────

def test_level2_without_pos_min_unchanged_margin_rule():
    """TESTS-cascade T2: без `pos_min` — правило отрыва дословно как сейчас;
    на этом держится неизменность вердикта всех существующих вызовов."""
    ok, _, _, margin = cascade.level2([("pos", "не может оплатить за рубеж", 0.81),
                                       ("neg", "офтоп", 0.80)])
    # 0.01 не меньше 0.01 — проходит, как сейчас (0.81 − 0.80 в плавающей точке
    # даёт 0.010000000000000009, поэтому сравнение приближённое).
    assert ok is True and margin == pytest.approx(0.01)

    ok, why, _, _ = cascade.level2([("pos", "не может оплатить за рубеж", 0.809),
                                    ("neg", "офтоп", 0.80)])
    assert ok is False
    assert "отрыв 0.009 меньше 0.01" in why

    ok, why, _, _ = cascade.level2([("neg", "болтовня по теме, проблемы нет", 0.79),
                                    ("pos", "не может оплатить за рубеж", 0.77)])
    assert ok is False and "болтовня" in why


# ── правило близости для мимо-словарных ───────────────────────────────────────

def test_pos_min_threshold_rule():
    """TESTS-cascade T8: с заданным `pos_min` отрыв не считается вовсе — решает
    близость верхнего эталона, шум игнорируется (замер 5.3: вычитание негативных
    эталонов сегодня вредит)."""
    below = [("pos", "банк не пропускает платёж", 0.5699), ("neg", "офтоп", 0.40)]
    ok, why, pain, _ = cascade.level2(below, pos_min=0.57)
    assert ok is False and pain is None
    assert "0.5699" in why and "0.57" in why

    above = [("pos", "банк не пропускает платёж", 0.5701), ("neg", "офтоп", 0.75)]
    ok, _, pain, _ = cascade.level2(above, pos_min=0.57)
    assert ok is True and pain == "банк не пропускает платёж", \
        "отрыв до шума 0.75 не считается и не роняет — правило близости"

    noise = [("neg", "болтовня по теме, проблемы нет", 0.80),
             ("pos", "не может оплатить за рубеж", 0.79)]
    ok, why, _, _ = cascade.level2(noise, pos_min=0.57)
    assert ok is False and "шуму" in why


# ── classify ведёт мимо-словарные через pos_min, обычные — через отрыв ────────

def test_bypassed_message_is_judged_by_proximity_not_margin():
    """Сообщение, пришедшее обходом, `classify` судит близостью: верхний pos
    0.5701 при шуме 0.75 отрывом не прошёл бы никогда — близостью проходит."""
    v = classify(LONG_NO_ANCHOR, l2_enabled=True, l1_bypass=True,
                 ranked=[("pos", "банк не пропускает платёж", 0.5701),
                         ("neg", "офтоп", 0.75)])
    assert v["level"] == 2 and v["passed"] is True
    assert v["pain"] == "банк не пропускает платёж"
    assert "мимо словаря" in v["detail"]["l2"]


def test_bypassed_message_below_pos_min_dies_on_l2():
    """Чуть не дотянул до порога — отсев на L2 с видимой причиной, L3 не зовётся."""
    v = classify(LONG_NO_ANCHOR, l2_enabled=True, l1_bypass=True,
                 ranked=[("pos", "банк не пропускает платёж", 0.5699),
                         ("neg", "офтоп", 0.40)])
    assert v["level"] == 2 and v["passed"] is False
    assert "мимо словаря" in v["detail"]["l2"]
    assert v["detail"]["l3"] == "не запускался: отсеяно на L2"


def test_anchored_message_still_judged_by_margin_even_with_ranked():
    """Сообщению с якорем обход не положен — `pos_min` не передаётся, действует
    отрыв: тот же зазор 0.009, который близостью выглядел бы проходом, здесь
    честно роняет решение."""
    v = classify("не могу оплатить инвойс, помогите пожалуйста", l2_enabled=True,
                 ranked=[("pos", "не может оплатить за рубеж", 0.809),
                         ("neg", "офтоп", 0.80)])
    assert v["level"] == 2 and v["passed"] is False
    assert "отрыв" in v["detail"]["l2"]


# ── применители порогов: единственная точка записи, действует без рестарта ────

def test_apply_l2_min_margin_mutates_both_profiles_in_place_and_validates():
    """`apply_l2_min_margin` меняет поле у обоих синглтонов на месте — читается
    оно из профиля, и тот же вход `level2`, что проходил при 0.01, начинает
    отсекаться. Кривое значение из базы обязано упасть громко, а не молча
    отсеять всё."""
    ok, _, _, _ = cascade.level2([("pos", "не может оплатить за рубеж", 0.81),
                                  ("neg", "офтоп", 0.78)])
    assert ok is True, "при заводском 0.01 отрыв 0.03 проходит"
    try:
        cascade.apply_l2_min_margin(0.05)
        assert cascade.PROFILES["dm_v1"].l2_min_margin == 0.05
        assert cascade.PROFILES["public_v1"].l2_min_margin == 0.05
        ok, why, _, _ = cascade.level2([("pos", "не может оплатить за рубеж", 0.81),
                                        ("neg", "офтоп", 0.78)])
        assert ok is False and "неуверенное" in why, "тот же вход теперь отсечён"
    finally:
        cascade.apply_l2_min_margin(cascade.L2_MIN_MARGIN)
    assert cascade.PROFILES["dm_v1"].l2_min_margin == 0.01
    with pytest.raises(ValueError, match="l2_min_margin"):
        cascade.apply_l2_min_margin(1.5)
    with pytest.raises(ValueError, match="l2_min_margin"):
        cascade.apply_l2_min_margin(0)


def test_apply_l1_bypass_pos_min_reassigns_module_variable_and_validates():
    """`apply_l1_bypass_pos_min` переприсваивает переменную модуля, а `classify`
    читает её в момент вызова — перенастройка действует со следующего сообщения,
    без рестарта. Валидация симметрична `l2_min_margin`."""
    try:
        cascade.apply_l1_bypass_pos_min(0.60)
        assert cascade.L1_BYPASS_POS_MIN == 0.60
        v = classify(LONG_NO_ANCHOR, l2_enabled=True, l1_bypass=True,
                     ranked=[("pos", "банк не пропускает платёж", 0.5701),
                             ("neg", "офтоп", 0.75)])
        assert v["passed"] is False, "0.5701 ниже нового порога 0.60 — отсев"
    finally:
        cascade.apply_l1_bypass_pos_min(0.57)
    assert cascade.L1_BYPASS_POS_MIN == 0.57
    with pytest.raises(ValueError, match="l1_bypass_pos_min"):
        cascade.apply_l1_bypass_pos_min(1.0)
