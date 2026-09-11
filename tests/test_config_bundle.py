"""Блок `thresholds` в наборе настроек: проверка входа до всякой базы.

Пороги каскада (`l2_min_margin`, `l1_bypass_pos_min`) живут строками таблицы
`limits`, поэтому ехать обязаны в наборе настроек — иначе экспорт с одного
инстанса и импорт на другой перенёс бы боли и промпты, а порог молча оставил бы
прежним: отбор поехал бы, а все видимые настройки совпадали бы.

Здесь — только то, что проверяется без базы (`validate` зовётся до первой
записи, так что кривой файл применяется целиком или никак): круг с записью
строк `limits` и перечиткой — в `tests/test_thresholds.py`.
"""
from __future__ import annotations

import os

import pytest

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_DEBUG", "true")

from app.services import config_bundle  # noqa: E402

# Минимальный набор, проходящий все проверки кроме блока `thresholds`:
# валидация файла обязана быть независимой от базы.
BUNDLE = {
    "format": "atomic-radar-config",
    "version": 1,
    "name": "тест-порогов",
    "business": {"description": "КУРС — оплата счетов зарубежных поставщиков."},
    "pains": {
        "банк не пропускает платеж": {
            "anchors": ["валютный контроль"],
            "prototypes": ["банк завернул платёж, требует контракт на учёт"],
        },
    },
    "noise": {"офтоп": ["всем привет, как дела"]},
    "disqualifiers": {"вакансия": ["вакансия"]},
    "l3_prompts": {"dm_v1": "Ты — фильтр сообщений. Ответь JSON."},
}


def test_an_old_file_without_the_thresholds_block_is_valid():
    """Старая выгрузка блока `thresholds` не содержит — и обязана остаться
    валидной: формат не меняется, обратной совместимости не может не быть."""
    config_bundle.validate(dict(BUNDLE))


def test_a_thresholds_block_with_known_keys_and_numbers_is_valid():
    config_bundle.validate({**BUNDLE, "thresholds": {"l2_min_margin": 0.05,
                                                     "l1_bypass_pos_min": 0.6}})


def test_an_unknown_threshold_key_names_the_known_ones():
    with pytest.raises(config_bundle.BundleError) as e:
        config_bundle.validate({**BUNDLE, "thresholds": {"pos_min": 0.57}})
    message = str(e.value)
    assert "pos_min" in message, "в ошибке должно быть видно, что именно неизвестно"
    assert "l2_min_margin" in message and "l1_bypass_pos_min" in message, \
        "известные ключи должны быть перечислены — по образцу проверки промптов"


@pytest.mark.parametrize("value", ["0.5", None, True, [0.5]])
def test_a_non_number_threshold_value_is_refused(value):
    """Значение порога — число. Строка «0.5» и bool (частный случай int в Python)
    не числа: молча привести — значит принять файл, которого человек не имел
    в виду."""
    with pytest.raises(config_bundle.BundleError) as e:
        config_bundle.validate({**BUNDLE,
                                "thresholds": {"l2_min_margin": value}})
    assert "число" in str(e.value)


@pytest.mark.parametrize("value", [0, -0.1, 1, 1.5])
def test_an_out_of_range_threshold_value_is_refused(value):
    """То же условие, что у применителей порога в каскаде: 0 < v < 1. Порог 0
    пропустил бы всё, порог 1 не пропустил ничего — оба молча ломают отбор."""
    with pytest.raises(config_bundle.BundleError) as e:
        config_bundle.validate({**BUNDLE, "thresholds": {"l1_bypass_pos_min": value}})
    assert "0 < v < 1" in str(e.value)


def test_a_thresholds_block_that_is_not_an_object_is_refused():
    with pytest.raises(config_bundle.BundleError) as e:
        config_bundle.validate({**BUNDLE, "thresholds": [("l2_min_margin", 0.5)]})
    assert "thresholds" in str(e.value)
