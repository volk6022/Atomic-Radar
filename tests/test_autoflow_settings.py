"""Проверка границ настроек автоматики — чистая функция, без базы.

`validate_settings` — единственная дверь для ввода: и ручка настроек (волна Е),
и импорт набора зовут её до первой записи. Если граница живёт только в ручке,
импорт файла обходит её молча, и «включено то, чего не утверждали» обнаружится
по странному поведению сценария, а не по отказу. Поэтому правила §1.1 здесь и
нигде больше: неизвестный ключ, не-число, вне диапазона — ValueError.
"""
from __future__ import annotations

import os

import pytest

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_INGEST_TOKEN", "test-ingest-token")

from app.services import autoflow  # noqa: E402


def test_validate_settings_bounds():
    """T-02: границы §1.1 держатся на валидации, а не на надежде."""
    # граница включена: потолок глубины равен DEFAULT_DEPTH (30 суток)
    autoflow.validate_settings({"autoflow_backfill_depth_days": 30})
    # нижняя и верхняя граница интервала принимаются
    autoflow.validate_settings({"autoflow_reclassify_interval_min": 5})
    autoflow.validate_settings({"autoflow_reclassify_interval_min": 1440})
    # выключатели принимают только 0 и 1
    autoflow.validate_settings({"autoflow_reclassify_enabled": 0,
                                "discovery_autoscan_enabled": 1})

    with pytest.raises(ValueError, match="autoflow_backfill_depth_days"):
        autoflow.validate_settings({"autoflow_backfill_depth_days": 31})
    with pytest.raises(ValueError, match="autoflow_backfill_target"):
        autoflow.validate_settings({"autoflow_backfill_target": 2001})
    with pytest.raises(ValueError, match="autoflow_reclassify_interval_min"):
        autoflow.validate_settings({"autoflow_reclassify_interval_min": 4})
    with pytest.raises(ValueError, match="autoflow_reclassify_enabled"):
        autoflow.validate_settings({"autoflow_reclassify_enabled": 2})
    # неизвестный ключ — с перечнем известных, иначе человек не узнает, как надо
    with pytest.raises(ValueError, match="известны"):
        autoflow.validate_settings({"no_such_key": 1})
    # bool — не число: True молча превратился бы в 1
    with pytest.raises(ValueError, match="autoflow_reclassify_enabled"):
        autoflow.validate_settings({"autoflow_reclassify_enabled": True})
    # не число вовсе
    with pytest.raises(ValueError, match="autoflow_backfill_target"):
        autoflow.validate_settings({"autoflow_backfill_target": "2000"})
