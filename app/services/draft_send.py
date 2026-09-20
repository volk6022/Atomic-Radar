"""Ручная отправка одобренного черновика cold_dm через Engage (PLAN 16.2).

Путь сообщения: оператор одобрил текст → смотрит `preflight` (ничего не пишет) →
подтверждает в интерфейсе → `order` записывает строку в `wf_outbound` и заказывает
отправку → Engage подтверждает доставку вебхуком `kind="send"` (`ingest.py`),
который двигает журнал, черновик, цель и нитку диалога.

Три решения владельца (PLAN 16.11), на которых стоит весь модуль:

1. **LIVE не включается.** Ручная отправка идёт мимо проверки режима — гейт
   зовётся с `origin="manual"` (`invariants.check_all` пропускает
   `not_in_dry_run`). Вместо режима предохранители другие: право DRAFT_SEND
   (владелец и заказчик, не разборщик), явное подтверждение в интерфейсе и
   аудит `wf_draft_send` на каждый заказ. Гардрейлы приличия — потолок
   сообщений, пауза, тихие часы, «этому человеку уже писали» — действуют как
   прежде.
2. **Аккаунт отправки — тот, что прочитал сообщение цели.** `message_readers`
   по `wf_targets.message_id`, при нескольких — первый по `first_seen_at`:
   писать адресату с аккаунта, не читавшего группу, — прийти «ниоткуда». Не
   «active[0]» и не «у кого больше остаток».
3. **Одна нитка на человека.** Факты касаний берутся из
   `conversations.contact_facts` (а не констант `False/0`), поэтому пауза 20 ч и
   потолок 4 при первом касании срабатывают сами: нитки нет — нарушений нет,
   нитка есть — «уже писали» режет. Само заведение нитки делает вебхук доставки
   (`ingest.py`), теми же `ensure_thread`/`add_event`, что и ручные отправки.

`preflight` и `order` прогоняют одни и те же проверки — `order` не доверяет
результату `preflight`, потому что между «посмотрел» и «отправил» черновик могли
переодобрить, читатель пропасть, а другой оператор — заказать отправку первым.
Оба идут через `_plan`: один проход собирает факты и строит ОДИН `SendRequest`,
который и показывается, и уходит в гейт — у показа и заказа не может оказаться
разных представлений о том, кому, кем и что отправляется.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from datetime import datetime

from sqlalchemy import select

from app.api.v1.system import current_mode
from app.core import clock
from app.core.outbound_gate import OutboundGate, SendRequest
from app.db.models import (AuditLog, Channel, EngageInstance, Message, MessageReader,
                           WfDraft, WfOutbound, WfTarget, Workflow)
from app.services import conversations as conversations_service
from app.services import engage
from app.services.manual_sends import chosen_text

logger = logging.getLogger(__name__)

# Живые состояния попытки: заказ существует и его нельзя делать второй раз.
# `failed` — попытка кончилась, черновику можно заказать новую.
ACTIVE_OUTBOUND_STATES = ("pending", "deferred", "delivered")

# Действие Engage, чей остаток показывается оператору перед отправкой.
MESSAGES_ACTION = "messages_per_day"


class DraftSendBlocked(Exception):
    """Отправка не пройдёт: структурные причины и вердикт гейта одним списком.

    Наверх превращается в 409 с `reasons` — список нужен целиком, по той же
    причине, по какой `check_all` возвращает список: чинить по одной причине и
    каждый раз получать новый отказ — способ издеваться над оператором.
    """

    def __init__(self, reasons: list[str]) -> None:
        super().__init__("; ".join(reasons) or "отправка заблокирована")
        self.reasons = reasons


class DraftSendConflict(Exception):
    """По этому черновику уже есть живая отправка — заказывать вторую нельзя."""

    def __init__(self, outbound: WfOutbound) -> None:
        super().__init__(
            f"по черновику уже заказана отправка (outbound {outbound.id}, "
            f"состояние «{outbound.state}»)")
        self.outbound = outbound


@dataclass
class _Plan:
    """Всё, что нужно и для показа, и для заказа — собрано одним проходом."""

    target: WfTarget | None
    peer_id: int | None
    username: str | None
    name: str | None
    text: str | None
    account_id: int | None
    account_status: str | None
    remaining_messages: int | None
    reasons: list[str]
    already: WfOutbound | None
    mode: str
    instance_key: str | None
    request: SendRequest | None


async def _instance_key(db, workflow: Workflow) -> str | None:
    """Ключ инстанса Engage сценария — так же, как его считает `wf_queues`."""
    return await db.scalar(
        select(EngageInstance.key).where(
            EngageInstance.id == workflow.engage_instance_id))


async def _plan(db, *, workflow: Workflow, draft: WfDraft, now: datetime) -> _Plan:
    """Собрать данные попытки, прогнать гейт и собрать список причин.

    Ничего не пишет. Единственные сетевые ходы — флот и остатки Engage, и оба
    гасятся в причины: недоступный Engage — это «отправить нельзя», а не 500.
    """
    reasons: list[str] = []
    instance_key = await _instance_key(db, workflow)

    target = await db.get(WfTarget, draft.target_id) if draft.target_id else None

    # ── адресат ───────────────────────────────────────────────────────────────
    # Только цель-«user»: у публичного ответа адресата-человека нет, и отправлять
    # ему «в личку» нечего. Юзернейм — цели, а если у той пусто, то сообщения:
    # доехавший позже лучше пустоты, адресовать по нему умеет сам воркер Engage.
    peer_id = username = name = None
    context_chat_id = context_message_id = context_chat_username = None
    if target is not None and target.target_kind == "user":
        peer_id = target.recipient_peer_id
        username = target.author_username
        name = target.author_name
        message = await db.get(Message, target.message_id)
        if message is not None:
            username = username or message.author_username
            name = name or message.author_name
            # Контекст для воркера: в каком чате и каким сообщением видели человека.
            channel = await db.get(Channel, message.channel_id)
            if channel is not None:
                context_chat_id, context_message_id = channel.peer_id, message.tg_message_id
                context_chat_username = channel.username
    if workflow.action != "dm":
        reasons.append(f"ручная отправка заведена только для личных сообщений, "
                       f"у сценария действие «{workflow.action}»")
    if target is None or target.target_kind != "user":
        reasons.append("публичный ответ — не ЛС: у цели нет адресата-человека")
    elif peer_id is None:
        reasons.append("у цели нет peer_id адресата — отправлять некому")

    text = chosen_text(draft)
    if not text:
        reasons.append("у черновика нет текста сообщения")

    # ── аккаунт: тот, что прочитал сообщение цели (решение владельца №2) ─────
    account_id = account_status = None
    remaining_messages = None
    if peer_id is not None and target is not None:
        row = (await db.execute(
            select(MessageReader.account_id)
            .where(MessageReader.message_id == target.message_id)
            .order_by(MessageReader.first_seen_at, MessageReader.account_id)
            .limit(1))).first()
        if row is None:
            reasons.append("не найден аккаунт, читавший сообщение")
        else:
            account_id = row[0]
            try:
                fleet = await engage.list_accounts(instance=instance_key)
            except engage.EngageUnavailable as e:
                reasons.append(f"Engage недоступен: {e}")
            else:
                row = next((a for a in fleet if a.get("account_id") == account_id), {})
                account_status = row.get("status")
                if account_status != "active":
                    reasons.append(
                        f"аккаунт {account_id} не активен "
                        f"(статус «{account_status or 'нет во флоте'}»)")
                # Прогрев: `send_message` Engage разрешает только тиру `ready`
                # (safety_defaults). 20.09 все шесть заказов упали 409 «not warmed»
                # после зелёного preflight — тир обязан быть в причинах.
                elif row.get("warmup_tier") not in (None, "ready"):
                    reasons.append(
                        f"аккаунт {account_id} ещё в прогреве (тир "
                        f"«{row.get('warmup_tier')}», отправка — только с «ready»)")
            # Остаток сообщений — витрина для оператора, не предохранитель:
            # дневной потолок считает Engage, и его отказ не мешает показу.
            try:
                raw = await engage.limits(account_ids=[account_id],
                                          instance=instance_key)
            except engage.EngageUnavailable:
                raw = {}
            actions = next(
                (a.get("actions") or [] for a in raw.get("accounts", [])
                 if a.get("account_id") == account_id), [])
            remaining_messages = next(
                (act.get("remaining") for act in actions
                 if act.get("action") == MESSAGES_ACTION), None)

    # ── факты нитки и гейт (решение владельца №3) ────────────────────────────
    # Режим читается, хотя `origin="manual"` его не проверяет: значение уходит в
    # снимок `wf_outbound.mode` — журнал обязан показывать, в каком режиме
    # система находилась в момент заказа.
    mode = await current_mode(db)
    request = None
    if peer_id is not None and text and target is not None:
        previously_contacted, sent_count, last_sent_at = (
            await conversations_service.contact_facts(db, peer_id=peer_id))
        request = SendRequest(
            draft_id=draft.id, conversation_id=0, account_id=account_id or 0,
            recipient_peer_id=peer_id, text=text, draft_state=draft.state,
            is_first_message=sent_count == 0, sent_count=sent_count,
            last_sent_at=last_sent_at,
            recipient_local_hour=(now.hour + 3) % 24,
            recipient_is_admin=False, previously_contacted=previously_contacted,
            origin="manual", recipient_username=username,
            workflow_id=workflow.id, target_id=target.id,
            context_chat_id=context_chat_id, context_message_id=context_message_id,
            context_chat_username=context_chat_username,
        )
        # mode_provider обязан быть асинхронным: гейт читает его через `await`
        # (в бою он ходит в базу). Здесь режим уже прочитан для снимка — отдаём
        # его тем же контрактом, чтобы показ и заказ не разошлись в том, как
        # читается режим.
        async def _current_mode() -> str:
            return mode

        gate = OutboundGate(engage_client=None, mode_provider=_current_mode,
                            journal=None)
        verdict = await gate.evaluate(request, now)
        reasons.extend(verdict.reasons)

    # ── уже заказанная отправка ───────────────────────────────────────────────
    # Последняя попытка по черновику — любой судьбы: живая означает «уже заказано»,
    # упавшая остаётся видимой, чтобы оператор знал, что первая не доехала.
    already = (await db.execute(
        select(WfOutbound)
        .where(WfOutbound.workflow_id == workflow.id,
               WfOutbound.draft_id == draft.id)
        .order_by(WfOutbound.id.desc()).limit(1))).scalar_one_or_none()
    if already is not None and already.state in ACTIVE_OUTBOUND_STATES:
        reasons.append(
            f"по черновику уже заказана отправка (outbound {already.id}, "
            f"состояние «{already.state}»)")

    return _Plan(target=target, peer_id=peer_id, username=username, name=name,
                 text=text, account_id=account_id, account_status=account_status,
                 remaining_messages=remaining_messages, reasons=reasons,
                 already=already, mode=mode, instance_key=instance_key,
                 request=request)


def _account_view(plan: _Plan) -> dict | None:
    """Блок `account` формы preflight: аккаунта нет — `null`, причина в reasons."""
    if plan.account_id is None:
        return None
    return {"id": plan.account_id, "status": plan.account_status,
            "remaining_messages": plan.remaining_messages}


def _already_view(outbound: WfOutbound | None) -> dict | None:
    """Блок `already` формы preflight — последняя попытка по черновику."""
    if outbound is None:
        return None
    return {"outbound_id": outbound.id, "state": outbound.state,
            "task_id": outbound.engage_task_id,
            "delivered_message_id": outbound.delivered_message_id,
            "conversation_id": outbound.conversation_id, "error": outbound.error}


async def preflight(db, *, workflow: Workflow, draft: WfDraft,
                    now: datetime) -> dict:
    """Прогонка перед кнопкой: всё, что GUI покажет до подтверждения. Ничего не пишет."""
    plan = await _plan(db, workflow=workflow, draft=draft, now=now)
    recipient = None
    if plan.peer_id is not None:
        recipient = {"peer_id": plan.peer_id, "username": plan.username,
                     "name": plan.name}
    return {
        "draft_id": draft.id, "state": draft.state, "action": workflow.action,
        "recipient": recipient,
        "account": _account_view(plan),
        "gate": {"allowed": not plan.reasons, "reasons": plan.reasons},
        "already": _already_view(plan.already),
        "text": plan.text,
    }


async def order(db, *, workflow: Workflow, draft: WfDraft, actor: str,
                now: datetime) -> WfOutbound:
    """Заказать отправку: строка `wf_outbound` → заказ в Engage → задача.

    Проверки те же, что у `preflight`, и заново: между показом и нажатием всё
    могло измениться. Черновик остаётся `approved` — признак «заказано» живёт в
    `wf_outbound.state`, а состояние черновика двигает только вебхук доставки.
    Ошибка Engage при заказе помечает попытку `failed` и поднимается наверх:
    черновик готов к повторному заказу, причина смотрится в журнале.
    """
    plan = await _plan(db, workflow=workflow, draft=draft, now=now)
    if plan.already is not None and plan.already.state in ACTIVE_OUTBOUND_STATES:
        raise DraftSendConflict(plan.already)
    if plan.reasons:
        raise DraftSendBlocked(plan.reasons)

    # Строка журнала обязана появиться ДО заказа: из её id строятся и ключ
    # идемпотентности, и адрес вебхука, которым Engage сообщит о доставке.
    row = WfOutbound(
        workflow_id=workflow.id, target_id=plan.target.id, draft_id=draft.id,
        engage_account_id=plan.account_id, recipient_peer_id=plan.peer_id,
        allowed=True, reasons=[], mode=plan.mode, text_snapshot=plan.text,
        state="pending")
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
        user_email=actor, action="wf_draft_send",
        detail={"workflow": workflow.key, "draft_id": draft.id,
                "outbound_id": row.id, "account_id": plan.account_id,
                "origin": "manual", "task_id": verdict.task_id}))
    await db.commit()
    logger.info("draft_send_ordered workflow=%s draft=%s outbound=%s account=%s "
                "task=%s by=%s", workflow.key, draft.id, row.id, plan.account_id,
                verdict.task_id, actor)
    return row
