"""Ответ из Переписок — ручная отправка без черновика (PLAN 16.5).

Тот же двухфазный путь, что у `draft_send`: строка `wf_outbound` → заказ в
Engage → вебхук `kind="send"` двигает состояние и пишет событие в нитку. Отличия
продиктованы решениями владельца (PLAN 16.11): ответ во входящем диалоге идёт
БЕЗ паузы 20 ч и потолка сообщений (`first_touch=False`), аккаунт отправки —
тот, что закреплён за ниткой первым касанием, адресат — `peer_id` нитки.
Тихие часы получателя действуют на всё.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from datetime import datetime

from sqlalchemy import select

from app.api.v1.system import current_mode
from app.core.outbound_gate import OutboundGate, SendRequest
from app.db.models import (AuditLog, Conversation, EngageInstance, WfOutbound,
                           WfTarget, Workflow)
from app.services import conversations as conversations_service
from app.services import engage
from app.services.draft_send import ACTIVE_OUTBOUND_STATES, MESSAGES_ACTION

logger = logging.getLogger(__name__)

HUMAN_STATES = ("handed_off", "closed")


class ReplyBlocked(Exception):
    """Ответ не пройдёт: структурные причины и вердикт гейта одним списком."""

    def __init__(self, reasons: list[str]) -> None:
        super().__init__("; ".join(reasons) or "ответ заблокирован")
        self.reasons = reasons


class ReplyConflict(Exception):
    """По этому диалогу уже есть живая отправка — заказывать вторую нельзя."""

    def __init__(self, outbound: WfOutbound) -> None:
        super().__init__(
            f"по диалогу уже заказан ответ (outbound {outbound.id}, "
            f"состояние «{outbound.state}»)")
        self.outbound = outbound


@dataclass
class _Plan:
    """Всё, что нужно и для показа, и для заказа — собрано одним проходом."""

    account_id: int | None
    account_status: str | None
    remaining_messages: int | None
    reasons: list[str]
    already: WfOutbound | None
    mode: str
    instance_key: str | None
    workflow_id: int | None
    request: SendRequest | None


async def _workflow_of(db, conv: Conversation) -> Workflow | None:
    """Сценарий нитки — через цель, если нитка заведена целью; иначе его нет."""
    if conv.target_id is None:
        return None
    target = await db.get(WfTarget, conv.target_id)
    if target is None:
        return None
    return await db.get(Workflow, target.workflow_id)


async def _plan(db, *, conv: Conversation, text: str | None, now: datetime) -> _Plan:
    """Собрать данные ответа, прогнать гейт и собрать список причин. Ничего не пишет."""
    reasons: list[str] = []
    text = (text or "").strip() or None
    if conv.engage_account_id is None:
        reasons.append("у диалога нет аккаунта отправки")
    if conv.state == "closed":
        reasons.append("диалог закрыт")
    if not text:
        reasons.append("нет текста ответа")

    workflow = await _workflow_of(db, conv)
    workflow_id = workflow.id if workflow is not None else None
    instance_key = None
    if workflow is not None:
        instance_key = await db.scalar(
            select(EngageInstance.key).where(
                EngageInstance.id == workflow.engage_instance_id))

    # ── аккаунт: закреплён за ниткой первым касанием ─────────────────────────
    account_id = conv.engage_account_id
    account_status = None
    remaining_messages = None
    if account_id is not None:
        try:
            fleet = await engage.list_accounts(instance=instance_key)
        except engage.EngageUnavailable as e:
            reasons.append(f"Engage недоступен: {e}")
        else:
            account_status = next(
                (a.get("status") for a in fleet
                 if a.get("account_id") == account_id), None)
            if account_status != "active":
                reasons.append(
                    f"аккаунт {account_id} не активен "
                    f"(статус «{account_status or 'нет во флоте'}»)")
        try:
            raw = await engage.limits(account_ids=[account_id], instance=instance_key)
        except engage.EngageUnavailable:
            raw = {}
        actions = next(
            (a.get("actions") or [] for a in raw.get("accounts", [])
             if a.get("account_id") == account_id), [])
        remaining_messages = next(
            (act.get("remaining") for act in actions
             if act.get("action") == MESSAGES_ACTION), None)

    # ── гейт: ответ — не первое касание ──────────────────────────────────────
    mode = await current_mode(db)
    request = None
    if text and account_id is not None:
        previously_contacted, sent_count, last_sent_at = (
            await conversations_service.contact_facts(db, peer_id=conv.peer_id))
        request = SendRequest(
            draft_id=0, conversation_id=conv.id, account_id=account_id,
            recipient_peer_id=conv.peer_id, text=text, draft_state="approved",
            is_first_message=False, sent_count=sent_count,
            last_sent_at=last_sent_at,
            recipient_local_hour=(now.hour + 3) % 24,
            recipient_is_admin=False, previously_contacted=previously_contacted,
            origin="manual", recipient_username=conv.peer_username,
            workflow_id=workflow_id, target_id=conv.target_id,
            first_touch=False,
        )

        async def _current_mode() -> str:
            return mode

        gate = OutboundGate(engage_client=None, mode_provider=_current_mode,
                            journal=None)
        verdict = await gate.evaluate(request, now)
        reasons.extend(verdict.reasons)

    # ── уже заказанный ответ по нитке ────────────────────────────────────────
    already = (await db.execute(
        select(WfOutbound)
        .where(WfOutbound.conversation_id == conv.id,
               WfOutbound.draft_id.is_(None))
        .order_by(WfOutbound.id.desc()).limit(1))).scalar_one_or_none()
    if already is not None and already.state in ("pending", "deferred"):
        reasons.append(
            f"по диалогу уже заказан ответ (outbound {already.id}, "
            f"состояние «{already.state}»)")

    return _Plan(account_id=account_id, account_status=account_status,
                 remaining_messages=remaining_messages, reasons=reasons,
                 already=already, mode=mode, instance_key=instance_key,
                 workflow_id=workflow_id, request=request)


def _account_view(plan: _Plan) -> dict | None:
    if plan.account_id is None:
        return None
    return {"id": plan.account_id, "status": plan.account_status,
            "remaining_messages": plan.remaining_messages}


def _already_view(outbound: WfOutbound | None) -> dict | None:
    if outbound is None:
        return None
    return {"outbound_id": outbound.id, "state": outbound.state,
            "task_id": outbound.engage_task_id,
            "delivered_message_id": outbound.delivered_message_id,
            "conversation_id": outbound.conversation_id, "error": outbound.error}


async def preflight(db, *, conv: Conversation, text: str | None,
                    now: datetime) -> dict:
    """Прогонка перед кнопкой «ответить»: та же форма, что у черновика. Ничего не пишет."""
    plan = await _plan(db, conv=conv, text=text, now=now)
    return {
        "conversation_id": conv.id, "state": conv.state,
        "recipient": {"peer_id": conv.peer_id, "username": conv.peer_username},
        "account": _account_view(plan),
        "gate": {"allowed": not plan.reasons, "reasons": plan.reasons},
        "already": _already_view(plan.already),
    }


async def order(db, *, conv: Conversation, text: str, actor: str,
                now: datetime) -> WfOutbound:
    """Заказать ответ: строка `wf_outbound` (без черновика) → заказ в Engage.

    Проверки те же, что у `preflight`, и заново. Строка журнала обязана появиться
    ДО заказа: из её id строятся ключ идемпотентности и адрес вебхука. Автор
    записывается в строку — вебхук доставки придёт без сессии.
    """
    plan = await _plan(db, conv=conv, text=text, now=now)
    if plan.already is not None and plan.already.state in ("pending", "deferred"):
        raise ReplyConflict(plan.already)
    if plan.reasons:
        raise ReplyBlocked(plan.reasons)

    row = WfOutbound(
        workflow_id=plan.workflow_id, target_id=conv.target_id, draft_id=None,
        conversation_id=conv.id, engage_account_id=plan.account_id,
        recipient_peer_id=conv.peer_id, allowed=True, reasons=[],
        mode=plan.mode, text_snapshot=plan.request.text, state="pending",
        actor=actor)
    db.add(row)
    await db.commit()

    async def _mode() -> str:
        return plan.mode

    gate = OutboundGate(engage_client=engage, mode_provider=_mode, journal=None,
                        webhook_builder=engage.webhook_url)
    req = replace(plan.request, outbound_id=row.id, instance=plan.instance_key)
    try:
        verdict = await gate.send(req, now)
    except engage.EngageUnavailable as e:
        row.state = "failed"
        row.error = str(e)
        await db.commit()
        raise

    row.engage_task_id = verdict.task_id
    db.add(AuditLog(
        user_email=actor, action="wf_conversation_reply",
        detail={"conversation_id": conv.id, "outbound_id": row.id,
                "account_id": plan.account_id, "origin": "manual",
                "task_id": verdict.task_id}))
    await db.commit()
    logger.info("conversation_reply_ordered conversation=%s outbound=%s account=%s "
                "task=%s by=%s", conv.id, row.id, plan.account_id,
                verdict.task_id, actor)
    return row


async def set_state(db, *, conv: Conversation, state: str, actor: str,
                    now: datetime) -> Conversation:
    """Передать (`handed_off`) или закрыть (`closed`) нитку — решение человека.

    Идемпотентно: повтор в том же состоянии ничего не пишет. `add_event` по
    `kind="system"` свёртку не двигает (она только для касаний), поэтому
    состояние ставится здесь, до события.
    """
    if state not in HUMAN_STATES:
        raise ValueError(f"неизвестное состояние «{state}»")
    if conv.state == state:
        return conv
    prev = conv.state
    conv.state = state
    if state == "handed_off":
        conv.handed_off_at = now
    await conversations_service.add_event(
        db, conv, kind="system", source=f"state:{state}", actor=actor, at=now,
        payload={"from": prev, "to": state})
    db.add(AuditLog(user_email=actor, action="wf_conversation_state",
                    detail={"conversation_id": conv.id, "from": prev, "to": state}))
    await db.commit()
    return conv
