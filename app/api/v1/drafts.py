"""Очередь черновиков — ключевой экран продукта.

Здесь человек одобряет или отклоняет каждое сообщение до отправки. Именно этот экран
делает сухой прогон осмысленным: без него «прогон» — это просто выключенная отправка.

Три свойства, ради которых модуль устроен именно так:

1. **Одобрение проходит через `OutboundGate`.** Не «подключим потом»: одобрение зовёт
   `gate.evaluate()` и возвращает вердикт экрану. Оператор видит своими глазами, что
   после его «одобрить» отправка всё равно заблокирована режимом.
2. **Гейт собран без клиента Engage намеренно** (`engage_client=None`). `evaluate()`
   сети не касается, а случайный вызов `send()` здесь упадёт вместо отправки.
3. **Очередь курсорная.** Экран показывает один черновик и двигается по очереди,
   поэтому ручка отвечает «следующий после указанного», а не выдачей списком.

Черновики берутся из лидов и живут в БД: решение оператора обязано пережить рестарт,
иначе разобранная очередь вернётся целиком, а вместе с ней — риск написать человеку
повторно.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel, Field
from sqlalchemy import func, select

from app.api.deps import GetDB, requires
from app.api.v1.system import current_mode
from app.core import clock
from app.core.access import Section
from app.core.outbound_gate import OutboundGate, SendRequest
from app.db.models import AuditLog, Channel, Draft, Lead, Message
from app.services import drafting

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/drafts", tags=["drafts"])


REASONS = [
    {"n": 1, "label": "Не та боль"},
    {"n": 2, "label": "Не тот человек"},
    {"n": 3, "label": "Звучит как реклама"},
    {"n": 4, "label": "Слишком длинно"},
    {"n": 5, "label": "Фактическая ошибка"},
    {"n": 6, "label": "Неверный тон"},
    {"n": 7, "label": "Дублирует отправленное"},
    {"n": 8, "label": "Ссылка в первом сообщении"},
    {"n": 9, "label": "Другое"},
]
_REASON_BY_N = {r["n"]: r["label"] for r in REASONS}


async def _ensure_queue(db) -> int:
    """Завести черновики для лидов, у которых их ещё нет.

    Делается лениво, при обращении к очереди, а не фоновым воркером: генератор пока
    шаблонный и стоит микросекунды, а лишний фоновый процесс — это лишнее место, где
    что-то молча не запустится.
    """
    lead_ids = (await db.execute(
        select(Lead.id).where(Lead.status.in_(("new", "in_review")))
        .where(~Lead.id.in_(select(Draft.lead_id))))).scalars().all()

    for lead_id in lead_ids:
        lead = (await db.execute(select(Lead).where(Lead.id == lead_id))).scalar_one()
        await drafting.ensure_draft(db, lead)
        lead.status = "in_review"

    if lead_ids:
        await db.commit()
    return len(lead_ids)


async def _payload(db, draft: Draft) -> dict:
    lead = (await db.execute(select(Lead).where(Lead.id == draft.lead_id))).scalar_one()
    channel = (await db.execute(
        select(Channel).where(Channel.id == lead.channel_id))).scalar_one()
    return {
        "id": draft.id, "lead_id": lead.id,
        "author_name": lead.author_name or "—",
        "author_username": ("@" + lead.author_username) if lead.author_username else None,
        "channel": channel.title, "pain": lead.pain, "score": lead.score,
        "score_breakdown": lead.score_breakdown or [],
        "thread": draft.thread_context or [],
        "variants": draft.variants or [],
        "source_message_link": draft.source_message_link,
        "disqualifiers": lead.disqualifiers or [],
        "state": draft.state,
    }


# ── чтение ────────────────────────────────────────────────────────────────────

@router.get("/next")
async def next_draft(db: GetDB, after: int | None = None,
                     user=requires(Section.DRAFTS)):
    """Следующий неразобранный черновик.

    Когда очередь разобрана, отдаём `draft: null`, а не 404: пустая очередь —
    нормальное состояние экрана, а не ошибка запроса.
    """
    await _ensure_queue(db)

    remaining = (await db.execute(
        select(func.count(Draft.id)).where(Draft.state == "pending"))).scalar_one()

    q = select(Draft).where(Draft.state == "pending").order_by(Draft.id)
    draft = None
    if after is not None:
        draft = (await db.execute(q.where(Draft.id > after).limit(1))).scalar_one_or_none()
    if draft is None:
        # Дойдя до конца, заворачиваем на начало — так же ведёт себя клавиша J.
        draft = (await db.execute(q.limit(1))).scalar_one_or_none()

    return {"remaining": remaining,
            "draft": (await _payload(db, draft)) if draft else None}


@router.get("/reasons")
async def reject_reasons(user=requires(Section.DRAFTS)):
    """Справочник причин отклонения. Список закрытый: причина уходит в eval-датасет,
    на котором меряется качество генерации, и свободный текст его размывает."""
    return REASONS


# ── решения ───────────────────────────────────────────────────────────────────

class ApproveRequest(BaseModel):
    variant_index: int = Field(ge=0)
    text: str | None = None


class RejectRequest(BaseModel):
    reason_n: int = Field(ge=1, le=9)


async def _draft_for_decision(db, draft_id: int) -> Draft:
    draft = (await db.execute(
        select(Draft).where(Draft.id == draft_id))).scalar_one_or_none()
    if draft is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"черновик {draft_id} не найден")
    if draft.state != "pending":
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"по черновику {draft_id} уже принято решение «{draft.state}»")
    return draft


async def _evaluate(db, draft: Draft, lead: Lead, text: str) -> dict:
    """Прогнать одобренный текст через гейт, ничего не отправляя.

    Режим гейт спрашивает у БД в момент вызова, поэтому вердикт честный: переключение
    в LIVE станет видно здесь сразу же.
    """
    gate = OutboundGate(engage_client=None, mode_provider=lambda: current_mode(db),
                        journal=None)
    req = SendRequest(
        draft_id=draft.id, conversation_id=0, account_id=0,
        recipient_peer_id=lead.author_peer_id or 0, text=text, draft_state="approved",
        is_first_message=True, sent_count=0, last_sent_at=None,
        # Часовой пояс собеседника нам пока неоткуда взять; берём московский —
        # большинство читаемых чатов русскоязычные. Когда появится определение
        # по профилю, значение придёт оттуда, а проверка не изменится.
        recipient_local_hour=(clock.utcnow().hour + 3) % 24,
        recipient_is_admin=False, previously_contacted=False,
    )
    verdict = await gate.evaluate(req, clock.utcnow())
    return {"allowed": verdict.allowed, "reasons": verdict.reasons}


@router.post("/{draft_id}/approve")
async def approve(draft_id: int, body: ApproveRequest, request: Request, db: GetDB,
                  user=requires(Section.DRAFTS)):
    """Одобрить вариант — при необходимости с правкой текста.

    Правка и одобрение — одна ручка, потому что в интерфейсе это одно действие.
    Разделять их значило бы допустить состояние «текст поправлен, но не одобрен»,
    которого на экране не существует.
    """
    draft = await _draft_for_decision(db, draft_id)
    variants = draft.variants or []
    if body.variant_index >= len(variants):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            f"вариант {body.variant_index} не существует "
                            f"(их {len(variants)})")

    original = variants[body.variant_index]["text"]
    text = (body.text or original).strip()
    if not text:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "пустой текст сообщения")
    edited = text != original

    lead = (await db.execute(select(Lead).where(Lead.id == draft.lead_id))).scalar_one()
    send = await _evaluate(db, draft, lead, text)

    draft.state = "approved"
    draft.chosen_variant = body.variant_index
    draft.final_text = text
    draft.decided_by = user.email
    draft.decided_at = clock.utcnow()
    lead.status = "approved"

    db.add(AuditLog(
        user_id=user.id, user_email=user.email, action="draft_approve",
        detail={"draft_id": draft_id, "lead_id": lead.id,
                "variant_index": body.variant_index, "edited": edited,
                "send_allowed": send["allowed"], "send_reasons": send["reasons"]},
        ip=request.client.host if request.client else None))
    await db.commit()
    logger.info("draft_approved draft=%s by=%s edited=%s send_allowed=%s",
                draft_id, user.email, edited, send["allowed"])

    remaining = (await db.execute(
        select(func.count(Draft.id)).where(Draft.state == "pending"))).scalar_one()
    return {"draft_id": draft_id, "decision": "approved",
            "variant_index": body.variant_index, "edited": edited,
            "send": send, "remaining": remaining}


@router.post("/{draft_id}/reject")
async def reject(draft_id: int, body: RejectRequest, request: Request, db: GetDB,
                 user=requires(Section.DRAFTS)):
    """Отклонить с типизированной причиной из закрытого справочника."""
    draft = await _draft_for_decision(db, draft_id)
    label = _REASON_BY_N.get(body.reason_n)
    if label is None:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            f"причина {body.reason_n} отсутствует в справочнике")

    lead = (await db.execute(select(Lead).where(Lead.id == draft.lead_id))).scalar_one()
    draft.state = "rejected"
    draft.reject_reason = label
    draft.decided_by = user.email
    draft.decided_at = clock.utcnow()
    lead.status = "rejected"
    lead.reject_reason = label

    db.add(AuditLog(
        user_id=user.id, user_email=user.email, action="draft_reject",
        detail={"draft_id": draft_id, "lead_id": lead.id,
                "reason_n": body.reason_n, "reason": label},
        ip=request.client.host if request.client else None))
    await db.commit()
    logger.info("draft_rejected draft=%s by=%s reason=%s", draft_id, user.email, label)

    remaining = (await db.execute(
        select(func.count(Draft.id)).where(Draft.state == "pending"))).scalar_one()
    return {"draft_id": draft_id, "decision": "rejected", "reason_n": body.reason_n,
            "reason": label, "remaining": remaining}
