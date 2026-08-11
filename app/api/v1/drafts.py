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
from sqlalchemy import func, or_, select

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
        # Сохранённая правка человека. Экран обязан показать именно её, иначе
        # оператор увидит исходный вариант и решит, что правка потерялась.
        "final_text": draft.final_text,
        "chosen_variant": draft.chosen_variant,
        "decided_by": draft.decided_by,
        "decided_at": draft.decided_at.isoformat() if draft.decided_at else None,
        "reject_reason": draft.reject_reason,
    }


# ── чтение ────────────────────────────────────────────────────────────────────

STATES = ("pending", "approved", "rejected")


def _state_filter(state: str | None):
    """`state=all` или пусто — без фильтра. Иначе один из известных статусов."""
    if not state or state == "all":
        return None
    if state not in STATES:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            f"неизвестный статус «{state}», ожидается один из "
                            f"{', '.join(STATES)} или all")
    return Draft.state == state


@router.get("/next")
async def next_draft(db: GetDB, after: int | None = None, state: str = "pending",
                     user=requires(Section.DRAFTS)):
    """Следующий черновик в выбранном срезе очереди.

    По умолчанию срез — неразобранные: именно ради них экран и существует. Но
    одобренный черновик обязан оставаться доступным для просмотра, иначе решение
    оператора исчезает с глаз сразу после того, как принято, и проверить его
    можно только в базе.

    Когда в срезе пусто, отдаём `draft: null`, а не 404: пустая очередь —
    нормальное состояние экрана, а не ошибка запроса.
    """
    await _ensure_queue(db)
    cond = _state_filter(state)

    count_q = select(func.count(Draft.id))
    q = select(Draft).order_by(Draft.id)
    if cond is not None:
        count_q, q = count_q.where(cond), q.where(cond)

    remaining = (await db.execute(count_q)).scalar_one()

    draft = None
    if after is not None:
        draft = (await db.execute(q.where(Draft.id > after).limit(1))).scalar_one_or_none()
    if draft is None:
        # Дойдя до конца, заворачиваем на начало — так же ведёт себя клавиша J.
        draft = (await db.execute(q.limit(1))).scalar_one_or_none()

    return {"remaining": remaining, "state": state,
            "draft": (await _payload(db, draft)) if draft else None}


@router.get("/list")
async def list_drafts(db: GetDB, user=requires(Section.DRAFTS),
                      state: str | None = None, channel: str | None = None,
                      q: str | None = None, min_score: int = 0,
                      limit: int = 100, offset: int = 0):
    """Полный список черновиков — то, чего очереди принципиально не хватает.

    Очередь показывает по одному и только неразобранные: так и задумано, иначе
    ревью превращается в блуждание. Но после решения черновик из неё исчезает, и
    без этого списка одобренный текст нельзя ни перечитать, ни показать заказчику.
    """
    await _ensure_queue(db)

    stmt = (select(Draft, Lead, Channel)
            .join(Lead, Draft.lead_id == Lead.id)
            .join(Channel, Lead.channel_id == Channel.id))
    count_stmt = (select(func.count(Draft.id))
                  .join(Lead, Draft.lead_id == Lead.id)
                  .join(Channel, Lead.channel_id == Channel.id))

    cond = _state_filter(state)
    if cond is not None:
        stmt, count_stmt = stmt.where(cond), count_stmt.where(cond)
    if channel:
        stmt, count_stmt = stmt.where(Channel.title == channel), count_stmt.where(
            Channel.title == channel)
    if min_score:
        stmt, count_stmt = stmt.where(Lead.score >= min_score), count_stmt.where(
            Lead.score >= min_score)
    if q:
        # Поиск идёт и по автору, и по цитате: оператор помнит либо «кому писали»,
        # либо «про что было», и заранее неизвестно, что именно.
        like = f"%{q.lower()}%"
        search = or_(func.lower(Lead.author_name).like(like),
                     func.lower(Lead.author_username).like(like),
                     func.lower(Lead.quote).like(like))
        stmt, count_stmt = stmt.where(search), count_stmt.where(search)

    total = (await db.execute(count_stmt)).scalar_one()
    rows = (await db.execute(
        stmt.order_by(Draft.id.desc()).limit(limit).offset(offset))).all()

    out = []
    for draft, lead, channel_row in rows:
        variants = draft.variants or []
        chosen = draft.final_text or (
            variants[draft.chosen_variant]["text"]
            if draft.chosen_variant is not None and draft.chosen_variant < len(variants)
            else (variants[0]["text"] if variants else ""))
        out.append({
            "id": draft.id, "lead_id": lead.id, "state": draft.state,
            "author_name": lead.author_name or "—",
            "author_username": ("@" + lead.author_username) if lead.author_username else None,
            "channel": channel_row.title, "pain": lead.pain, "score": lead.score,
            "text": chosen, "edited": bool(draft.final_text),
            "reject_reason": draft.reject_reason,
            "decided_by": draft.decided_by,
            "decided_at": draft.decided_at.isoformat() if draft.decided_at else None,
        })

    return {"total": total, "limit": limit, "offset": offset, "rows": out,
            "states": {s: (await db.execute(
                select(func.count(Draft.id)).where(Draft.state == s))).scalar_one()
                for s in STATES}}


@router.get("/reasons")
async def reject_reasons(user=requires(Section.DRAFTS)):
    """Справочник причин отклонения. Список закрытый: причина уходит в eval-датасет,
    на котором меряется качество генерации, и свободный текст его размывает."""
    return REASONS


# ВНИМАНИЕ: всё, что объявлено ниже `/{draft_id}`, будет им перехвачено —
# FastAPI сопоставляет маршруты в порядке объявления. `/reasons` однажды уже
# уехал сюда и стал возвращать 422 «draft_id не число», из-за чего на экране
# молча переставала открываться правка.
@router.get("/{draft_id}")
async def one_draft(draft_id: int, db: GetDB, user=requires(Section.DRAFTS)):
    """Конкретный черновик — по нему из полного списка открывается очередь."""
    draft = (await db.execute(
        select(Draft).where(Draft.id == draft_id))).scalar_one_or_none()
    if draft is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"черновик {draft_id} не найден")
    remaining = (await db.execute(
        select(func.count(Draft.id)).where(Draft.state == "pending"))).scalar_one()
    return {"remaining": remaining, "state": draft.state,
            "draft": await _payload(db, draft)}


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
    # Приоритет: присланный текст → ранее сохранённая правка → исходный вариант.
    # Иначе одобрение после «сохранить с пометкой» молча отправило бы генерацию,
    # а не то, что человек написал руками.
    text = (body.text or draft.final_text or original).strip()
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


class EditRequest(BaseModel):
    variant_index: int = Field(ge=0)
    text: str


@router.post("/{draft_id}/edit")
async def edit(draft_id: int, body: EditRequest, request: Request, db: GetDB,
               user=requires(Section.DRAFTS)):
    """Сохранить правку, НЕ принимая решения.

    Отдельно от одобрения, потому что это разные действия: «текст поправлен, ещё
    думаю» — нормальное состояние работы, и заставлять человека одобрять только
    ради того, чтобы не потерять правку, значит подталкивать его к решению.

    Черновик остаётся в очереди, но помечен как отредактированный вручную — этот
    признак идёт в eval-датасет и говорит о генерации больше, чем отказ: человек
    не отверг текст, а дописал за него.
    """
    draft = await _draft_for_decision(db, draft_id)
    variants = draft.variants or []
    if body.variant_index >= len(variants):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            f"вариант {body.variant_index} не существует "
                            f"(их {len(variants)})")

    text = body.text.strip()
    if not text:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "пустой текст сообщения")

    draft.chosen_variant = body.variant_index
    draft.final_text = text
    # Состояние намеренно не трогаем: черновик остаётся неразобранным.
    db.add(AuditLog(
        user_id=user.id, user_email=user.email, action="draft_edit",
        detail={"draft_id": draft_id, "lead_id": draft.lead_id,
                "variant_index": body.variant_index,
                "changed": text != variants[body.variant_index]["text"]},
        ip=request.client.host if request.client else None))
    await db.commit()
    logger.info("draft_edited draft=%s by=%s", draft_id, user.email)

    return {"draft_id": draft_id, "saved": True, "state": draft.state,
            "edited": True, "text": text}


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
