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
    """Всё, что нужно знать о попытке отправки, чтобы принять решение.

    С 16.2 отправка двухфазная: гейт заказывает её в Engage (`task_id`), а
    подтверждение доставки приходит позже вебхуком `kind="send"`. Поэтому у
    попытки появились `outbound_id` (номер строки журнала `wf_outbound` — из него
    строятся и адрес возврата, и ключ идемпотентности) и `instance` (инстанс
    Engage, в чьём флоте живёт аккаунт; у сценариев их может быть несколько).
    """
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
    # «auto» — прежнее поведение: режим проверяется. «manual» — ручная отправка
    # одобренного черновика (16.2): режим не проверяется, вместо него право
    # DRAFT_SEND, подтверждение в интерфейсе и аудит.
    origin: str = "auto"
    # Ответ во входящем диалоге (16.5): пауза и потолок — только на первое касание.
    first_touch: bool = True
    # Где аккаунт видел получателя (чат и сообщение цели) — воркер подгружает пир.
    context_chat_id: int | None = None
    context_message_id: int | None = None
    context_chat_username: str | None = None
    # Куда писать, если по peer_id адресата разыскали только по username.
    recipient_username: str | None = None
    workflow_id: int | None = None
    target_id: int | None = None
    # Номер строки `wf_outbound`, под которым заказ уже записан: из него строятся
    # `idempotency_key` и адрес вебхука. NULL у репетиций без отправки.
    outbound_id: int | None = None
    instance: str | None = None


@dataclass
class SendVerdict:
    allowed: bool
    reasons: list[str] = field(default_factory=list)
    delivered_message_id: int | None = None
    # Номер задачи Engage, под которым заказ принят. Заполняется с 16.2: доставка
    # подтверждается вебхуком позже и в вердикт попасть не может.
    task_id: str | None = None

    @property
    def blocked_by(self) -> str:
        return "; ".join(self.reasons) or "-"


class OutboundGate:
    """Проверяет и (только если всё чисто и режим LIVE) отправляет через Engage."""

    def __init__(self, engage_client, mode_provider, journal,
                 webhook_builder=None):
        # mode_provider — вызываемое, читающее режим из БД в момент попытки.
        # Передаём функцию, а не значение, чтобы нельзя было закешировать LIVE.
        self._engage = engage_client
        self._mode = mode_provider
        self._journal = journal
        # webhook_builder — то же, что mode_provider, только про обратный адрес:
        # адрес вебхука знает сервисный слой (секрет и SELF_BASE_URL живут в
        # настройках), и ядро не должно тянуть его само. Обязателен для боевой
        # отправки с `outbound_id` — без адреса возврата Engage не смог бы
        # сообщить о доставке, и заказ был бы выстрелом в пустоту.
        self._webhook = webhook_builder

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
            origin=req.origin,
            first_touch=req.first_touch,
        )
        return SendVerdict(allowed=not reasons, reasons=reasons)

    async def send(self, req: SendRequest, now: datetime) -> SendVerdict:
        verdict = await self.evaluate(req, now)

        if not verdict.allowed:
            logger.info(
                "outbound_blocked draft=%s conversation=%s reasons=%s",
                req.draft_id, req.conversation_id, verdict.blocked_by,
            )
            if self._journal is not None:
                await self._journal.record_blocked(req, verdict.reasons, now)
            return verdict

        if req.outbound_id is None:
            raise ValueError(
                "боевая отправка без outbound_id невозможна: строка журнала "
                "wf_outbound обязана существовать ДО заказа — из неё строятся "
                "ключ идемпотентности и адрес возврата вебхука")
        if self._webhook is None:
            raise ValueError(
                "гейт создан без webhook_builder: некому построить адрес, по "
                "которому Engage сообщит о доставке")

        # Единственное место во всём Radar, где сообщение уходит наружу.
        # С 16.2 заказ двухфазный: здесь создаётся задача Engage, а номер
        # доставленного сообщения приезжает позже вебхуком kind="send".
        response = await self._engage.send_message(
            account_id=req.account_id,
            recipient_peer_id=req.recipient_peer_id,
            recipient_username=req.recipient_username,
            text=req.text,
            context_chat_id=req.context_chat_id,
            context_message_id=req.context_message_id,
            context_chat_username=req.context_chat_username,
            webhook_url=self._webhook(kind="send", account_id=req.account_id,
                                      outbound_id=req.outbound_id),
            idempotency_key=f"radar-wf-outbound-{req.outbound_id}",
            instance=req.instance,
        )
        verdict.task_id = response.get("task_id")
        logger.info(
            "outbound_ordered draft=%s outbound=%s task=%s",
            req.draft_id, req.outbound_id, verdict.task_id,
        )
        # Журнал необязателен с 16.2: заказ уже записан вызвавшей стороной
        # (`draft_send.order` создаёт строку `wf_outbound` ДО гейта), и повторная
        # запись `record_sent` была бы вторым местом правды. Заглушки-репетиции
        # (`drafts.py`, `_gate_verdict`) зовут только `evaluate`, им это неважно;
        # строка журнала «кто и когда заказал» — дело заказчика, здесь не дублируется.
        if self._journal is not None:
            await self._journal.record_sent(req, verdict.task_id, now)
        return verdict
