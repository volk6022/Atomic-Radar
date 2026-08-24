"""Тревоги: события, о которых оператор обязан узнать, не разглядывая экраны.

До этого модуля таблица `alerts` существовала и не заполнялась никем, а лента на
дашборде собиралась из трёх условий прямо в обработчике. Разница принципиальная:
условие видно только пока оно длится, а событие — «задача упала», «включили LIVE»,
«аккаунт забанен» — происходит один раз, и если в этот момент никто не смотрел на
экран, оно исчезает бесследно.

Поэтому здесь два разных сорта, и лента показывает оба:

* **события** — строки в таблице, живут до отметки «прочитано»;
* **состояния** — сухой прогон, аварийная остановка. Их незачем хранить: они
  вычисляются из `system_state` в момент показа и не могут «протухнуть».

Ключ (`key`) нужен, чтобы одно и то же не копилось сотнями строк. Модель недоступна
десять минут — это одна тревога, а не двести: повторное событие с тем же ключом
поднимает время у существующей непрочитанной записи, а не заводит новую.
"""
from __future__ import annotations

import logging

from sqlalchemy import func, select

from app.core import clock
from app.db.models import Alert

logger = logging.getLogger("radar")

SEVERITIES = ("info", "warn", "error")


async def emit(db, *, key: str, text: str, severity: str = "warn") -> Alert:
    """Поднять тревогу. Повтор с тем же ключом обновляет непрочитанную, а не плодит.

    Коммит не делается: вызывающий обычно уже в транзакции, и лишний коммит посреди
    чужой операции — способ записать половину.
    """
    if severity not in SEVERITIES:
        severity = "warn"

    existing = (await db.execute(
        select(Alert).where(Alert.key == key, Alert.read_at.is_(None))
        .order_by(Alert.id.desc()).limit(1))).scalar_one_or_none()

    if existing is not None:
        existing.text = text
        existing.severity = severity
        existing.created_at = clock.utcnow()
        logger.info("alert_refreshed key=%s", key)
        return existing

    alert = Alert(key=key, text=text, severity=severity)
    db.add(alert)
    logger.warning("alert key=%s severity=%s: %s", key, severity, text)
    return alert


async def unread_count(db) -> int:
    return (await db.execute(
        select(func.count(Alert.id)).where(Alert.read_at.is_(None)))).scalar_one()
