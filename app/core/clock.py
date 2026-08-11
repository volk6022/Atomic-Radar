"""Единая точка получения текущего времени.

Вынесено в функцию, чтобы тесты сценариев могли подменить время, не трогая логику:
проверки интервалов и тихих часов иначе невозможно проверить набором сценариев.
"""
from __future__ import annotations

from datetime import datetime, timezone


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def local_hour(utc_dt: datetime, tz_offset_seconds: int) -> int:
    """Локальный час собеседника по смещению его часового пояса в секундах.

    В секундах, а не в часах, — так же, как это хранит Engage: смещение в часах
    однажды уже стоило нам аккаунта, уснувшего «вне рабочего окна».
    """
    return int((utc_dt.timestamp() + tz_offset_seconds) // 3600 % 24)
