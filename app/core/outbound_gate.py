"""OutboundGate — единственный путь, которым сообщение может уйти человеку.

Устройство намеренно неудобное: нигде в коде Radar больше нет вызова Engage на отправку.
Хочешь отправить — идёшь сюда и проходишь проверки. Это не паранойя, а условие сделки:
Андрей согласился на проект при требовании «сначала сухой прогон без единой отправки»,
и одна забытая ветка кода, дергающая Engage напрямую, это требование обнуляет.

Два свойства, ради которых всё написано именно так:

1. **В DRY_RUN до сети дело не доходит физически.** Не «мы не вызываем», а «вызов
   стоит после проверки и при отказе не выполняется». Флаг режима читается из БД
   на каждую попытку, а не из конфига при старте: переключение должно действовать
   немедленно, включая kill switch.
2. **Отказ — это запись, а не тишина.** Каждая заблокированная попытка ложится в журнал
   с полным списком причин. Иначе «почему оно не отправило» превращается в раскопки.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime

from app.core import invariants

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SendRequest:
    """Всё, что нужно знать о попытке отправки, чтобы принять решение."""
    draft_id: int
    conversation_id: int
    account_id: int
    recipient_peer_id: int
    text: str
    draft_state: str
    is_first_message: bool
    sent_count: int
    last_sent_at: datetime | None
    recipient_local_hour: int
    recipient_is_admin: bool
    previously_contacted: bool


@dataclass
class SendVerdict:
    allowed: bool
    reasons: list[str] = field(default_factory=list)
    delivered_message_id: int | None = None

    @property
    def blocked_by(self) -> str:
        return "; ".join(self.reasons) or "-"


class OutboundGate:
    """Проверяет и (только если всё чисто и режим LIVE) отправляет через Engage."""

    def __init__(self, engage_client, mode_provider, journal):
        # mode_provider — вызываемое, читающее режим из БД в момент попытки.
        # Передаём функцию, а не значение, чтобы нельзя было закешировать LIVE.
        self._engage = engage_client
        self._mode = mode_provider
        self._journal = journal

    async def evaluate(self, req: SendRequest, now: datetime) -> SendVerdict:
        """Прогнать проверки, ничего не отправляя. Тот же путь, что и `send`, но без сети —
        именно это делает сухой прогон честной репетицией, а не отдельной веткой кода."""
        mode = await self._mode()
        reasons = invariants.check_all(
            mode=mode,
            draft_state=req.draft_state,
            text=req.text,
            is_first=req.is_first_message,
            sent_count=req.sent_count,
            last_sent_at=req.last_sent_at,
            now=now,
            local_hour=req.recipient_local_hour,
            recipient_is_admin=req.recipient_is_admin,
            previously_contacted=req.previously_contacted,
        )
        return SendVerdict(allowed=not reasons, reasons=reasons)

    async def send(self, req: SendRequest, now: datetime) -> SendVerdict:
        verdict = await self.evaluate(req, now)

        if not verdict.allowed:
            logger.info(
                "outbound_blocked draft=%s conversation=%s reasons=%s",
                req.draft_id, req.conversation_id, verdict.blocked_by,
            )
            await self._journal.record_blocked(req, verdict.reasons, now)
            return verdict

        # Единственное место во всём Radar, где сообщение уходит наружу.
        message_id = await self._engage.send_message(
            account_id=req.account_id,
            peer_id=req.recipient_peer_id,
            text=req.text,
        )
        verdict.delivered_message_id = message_id
        logger.info(
            "outbound_sent draft=%s conversation=%s message=%s",
            req.draft_id, req.conversation_id, message_id,
        )
        await self._journal.record_sent(req, message_id, now)
        return verdict
