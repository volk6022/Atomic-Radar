"""Лиды: список и решения по ним.

Отдельно от `screens.py`, потому что здесь появились побочные эффекты. Тот модуль
намеренно остаётся чтением: у него нет ни одной ручки, которая что-то меняет, и это
свойство удобно проверять глазами, а не тестом.

Главное решение модуля — массовые действия. Они нужны: разбирать сто девять лидов по
одному, когда половина из них очевидный шум, — ровно та работа, ради избавления от
которой админку и переделываем. Но они же и самые опасные во всём приложении: одна
ошибка в фильтре, и три сотни лидов отклонены одним нажатием. Предохранители описаны
у `bulk`.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field
from sqlalchemy import func, select

from app.api.deps import GetDB, permits, requires
from app.api.v1.listing import ListParams, apply_search, apply_sort, list_params
from app.core import clock
from app.core.access import BULK_LIMIT_REVIEWER, Capability, Role, Section
from app.db.models import AuditLog, Channel, Draft, Lead, OutboundAttempt

logger = logging.getLogger("radar")

router = APIRouter(prefix="/api/v1/leads", tags=["leads"])

STATUSES = ("new", "in_review", "approved", "rejected")

LEAD_SORTS = {"score": Lead.score, "created": Lead.created_at,
              "author": Lead.author_name, "channel": Channel.title,
              "status": Lead.status, "pain": Lead.pain}


def _check_status(value: str | None) -> None:
    if value and value not in STATUSES:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            f"неизвестный статус «{value}», ожидается один из {', '.join(STATUSES)}")


def _filtered(stmt, *, lead_status, channel_id, pain, min_score):
    if lead_status:
        stmt = stmt.where(Lead.status == lead_status)
    if channel_id is not None:
        stmt = stmt.where(Lead.channel_id == channel_id)
    if pain:
        stmt = stmt.where(Lead.pain == pain)
    if min_score:
        stmt = stmt.where(Lead.score >= min_score)
    return stmt


# ── чтение ────────────────────────────────────────────────────────────────────

@router.get("")
async def list_leads(db: GetDB, user=requires(Section.LEADS),
                     p: ListParams = Depends(list_params),
                     status_filter: str | None = Query(None, alias="status"),
                     channel_id: int | None = None,
                     pain: str | None = None,
                     min_score: int | None = None):
    """Список лидов.

    В запросе параметр называется `status` — как и был, ссылки и фронтенд не
    меняются. Внутри он `status_filter`, потому что имя `status` в этом модуле уже
    занято одноимённым модулем FastAPI, из которого берутся коды ответов.
    """
    _check_status(status_filter)

    q = select(Lead, Channel).join(Channel, Lead.channel_id == Channel.id)
    # Счётчик тоже присоединяет канал: без этого сортировка и фильтр по названию
    # канала считались бы по разным множествам, и «показано 50 из 12» стало бы
    # обычным делом.
    count_q = select(func.count(Lead.id)).join(Channel, Lead.channel_id == Channel.id)

    kw = dict(lead_status=status_filter, channel_id=channel_id, pain=pain,
              min_score=min_score)
    q, count_q = _filtered(q, **kw), _filtered(count_q, **kw)

    search = [Lead.author_name, Lead.author_username, Lead.quote, Lead.pain]
    q, count_q = apply_search(q, p, search), apply_search(count_q, p, search)

    total = (await db.execute(count_q)).scalar_one()
    q = apply_sort(q, p, LEAD_SORTS, default="score", tiebreak=Lead.id)
    rows = (await db.execute(q.limit(p.limit).offset(p.offset))).all()

    by_status = dict((await db.execute(
        select(Lead.status, func.count(Lead.id)).group_by(Lead.status))).all())

    out = [{
        "id": lead.id, "author_name": lead.author_name or "—",
        "author_username": ("@" + lead.author_username) if lead.author_username else None,
        "channel": c.title, "channel_id": lead.channel_id,
        "pain": lead.pain, "score": lead.score,
        "status": lead.status, "quote": lead.quote,
        "reject_reason": lead.reject_reason,
        "score_breakdown": lead.score_breakdown or [],
        "disqualifiers": lead.disqualifiers or [],
        "created_at": lead.created_at.isoformat() if lead.created_at else None,
    } for lead, c in rows]

    return {**p.page(total), "rows": out,
            "states": [{"key": k, "count": by_status.get(k, 0)} for k in STATUSES]}


@router.get("/pains")
async def pain_options(db: GetDB, user=requires(Section.LEADS)):
    """Боли, которые реально встретились в лидах, — для выпадающего фильтра.

    Берутся из данных, а не из конфигурации каскада: в базе лежат лиды, размеченные
    прошлыми версиями профиля, и фильтр обязан находить в том числе их.
    """
    rows = (await db.execute(
        select(Lead.pain, func.count(Lead.id)).where(Lead.pain.isnot(None))
        .group_by(Lead.pain).order_by(func.count(Lead.id).desc()))).all()
    return [{"value": pain, "label": pain, "count": n} for pain, n in rows]


# ── решения ───────────────────────────────────────────────────────────────────

async def _sent_lead_ids(db, lead_ids: list[int]) -> set[int]:
    """Лиды, по которым сообщение уже ушло человеку.

    Такие не трогаются никакими массовыми действиями: сменить статус лида, которому
    уже написали, значит соврать в отчётности, а отменить отправку всё равно нельзя.
    """
    if not lead_ids:
        return set()
    rows = (await db.execute(
        select(Draft.lead_id)
        .join(OutboundAttempt, OutboundAttempt.draft_id == Draft.id)
        .where(Draft.lead_id.in_(lead_ids),
               OutboundAttempt.delivered_message_id.isnot(None)))).all()
    return {r[0] for r in rows}


class BulkRequest(BaseModel):
    action: str = Field(description="reject | approve | reset")
    reason: str | None = None
    ids: list[int] | None = None
    # Срез вместо перечисления: «отклонить всё, что под фильтром».
    filter: dict | None = None
    # Сколько строк показал экран. Сервер сверяет и отказывается, если разошлось.
    expect: int | None = None


BULK_ACTIONS = {"reject": "rejected", "approve": "approved", "reset": "new"}


@router.post("/bulk")
async def bulk(body: BulkRequest, request: Request, db: GetDB,
               user=permits(Section.LEADS, Capability.BULK_DECIDE)):
    """Массовое решение по лидам.

    Три предохранителя, каждый закрывает свой способ навредить:

    1. **Отправленное не трогается.** Лиды, по которым сообщение уже ушло, молча
       исключаются из выборки и перечисляются в ответе.
    2. **Сверка количества.** Экран присылает `expect` — число, которое он показал
       человеку. Если к моменту нажатия под фильтр подходит другое количество
       (пришли новые сообщения, кто-то разобрал часть очереди), действие
       отклоняется. Иначе «отклонить 87 лидов» однажды отклонит двести.
    3. **Потолок для разборщика.** Наёмному разборщику доступно не больше
       `BULK_LIMIT_REVIEWER` строк за раз: ему нужен инструмент для «эти десять —
       очевидный шум», а не для «отклонить всё найденное».
    """
    target = BULK_ACTIONS.get(body.action)
    if target is None:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            f"действие «{body.action}» неизвестно, ожидается одно из "
            f"{', '.join(BULK_ACTIONS)}")
    if body.action == "reject" and not (body.reason or "").strip():
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            "массовое отклонение требует причины")

    if body.ids:
        stmt = select(Lead.id).where(Lead.id.in_(body.ids))
    elif body.filter is not None:
        f = body.filter
        _check_status(f.get("status"))
        stmt = _filtered(select(Lead.id), lead_status=f.get("status"),
                         channel_id=f.get("channel_id"), pain=f.get("pain"),
                         min_score=f.get("min_score"))
    else:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            "нужно передать либо ids, либо filter")

    matched = [r[0] for r in (await db.execute(stmt)).all()]

    if body.expect is not None and body.expect != len(matched):
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"выборка изменилась: экран показывал {body.expect}, сейчас под условие "
            f"подходит {len(matched)}. Обновите список и повторите")

    if user.role == Role.REVIEWER and len(matched) > BULK_LIMIT_REVIEWER:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            f"за раз можно решить не больше {BULK_LIMIT_REVIEWER} лидов, "
            f"в выборке {len(matched)}")

    sent = await _sent_lead_ids(db, matched)
    ids = [i for i in matched if i not in sent]
    if not ids:
        return {"changed": 0, "skipped_sent": sorted(sent), "matched": len(matched)}

    leads = (await db.execute(select(Lead).where(Lead.id.in_(ids)))).scalars().all()
    for lead in leads:
        lead.status = target
        lead.reject_reason = body.reason if body.action == "reject" else None

    # Черновики идут следом: отклонённый лид, оставшийся в очереди на ревью, —
    # это тот же лид, который человек уже разобрал, показанный ему второй раз.
    drafts = (await db.execute(
        select(Draft).where(Draft.lead_id.in_(ids),
                            Draft.state == "pending"))).scalars().all()
    for d in drafts:
        if body.action == "reset":
            continue
        d.state = target
        d.reject_reason = body.reason if body.action == "reject" else None
        d.decided_by = user.email
        d.decided_at = clock.utcnow()

    db.add(AuditLog(
        user_id=user.id, user_email=user.email, action="lead_bulk",
        detail={"action": body.action, "reason": body.reason, "count": len(ids),
                "drafts": len(drafts), "skipped_sent": sorted(sent),
                "by_filter": body.ids is None, "filter": body.filter},
        ip=request.client.host if request.client else None))
    await db.commit()
    logger.warning("lead_bulk %s count=%s by=%s reason=%s",
                   body.action, len(ids), user.email, body.reason)

    return {"changed": len(ids), "drafts_changed": len(drafts),
            "skipped_sent": sorted(sent), "matched": len(matched)}


# `/bulk` объявлен раньше `/{lead_id}` намеренно: FastAPI сопоставляет маршруты
# в порядке объявления, и литеральный путь, оказавшийся после параметризованного,
# однажды перехватывается им и начинает отвечать «422, это не число». В очереди
# черновиков так уже уезжал `/reasons`, и правка молча переставала открываться.

class LeadPatch(BaseModel):
    status: str | None = None
    pain: str | None = None
    reject_reason: str | None = None


@router.patch("/{lead_id}")
async def update_lead(lead_id: int, body: LeadPatch, request: Request, db: GetDB,
                      user=permits(Section.LEADS, Capability.LEAD_STATUS)):
    """Правка одного лида: статус и/или боль.

    Боль правится руками намеренно: это разметка, и она же датасет, по которому
    меряется качество классификации. Исправление «модель назвала это хостингом, а на
    деле человек искал админа» ценнее, чем ещё одна строка в логе.
    """
    _check_status(body.status)
    lead = (await db.execute(
        select(Lead).where(Lead.id == lead_id))).scalar_one_or_none()
    if lead is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"лид {lead_id} не найден")

    if await _sent_lead_ids(db, [lead_id]):
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"по лиду {lead_id} сообщение уже отправлено — статус менять нельзя")

    before = {"status": lead.status, "pain": lead.pain}
    if body.status:
        lead.status = body.status
        lead.reject_reason = body.reject_reason if body.status == "rejected" else None
    if body.pain is not None:
        lead.pain = body.pain

    db.add(AuditLog(
        user_id=user.id, user_email=user.email, action="lead_update",
        detail={"lead_id": lead_id, "from": before,
                "to": {"status": lead.status, "pain": lead.pain}},
        ip=request.client.host if request.client else None))
    await db.commit()
    logger.info("lead_updated lead=%s by=%s status=%s", lead_id, user.email, lead.status)
    return {"id": lead_id, "status": lead.status, "pain": lead.pain}
