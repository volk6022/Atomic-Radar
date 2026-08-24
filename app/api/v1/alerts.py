"""Лента тревог и отметка «прочитано».

Лента смешивает два сорта, и это осознанно. Состояния (сухой прогон, аварийная
остановка) вычисляются на месте и отметку «прочитано» не принимают: пометить
прочитанным то, что прямо сейчас так и есть, значит спрятать от себя факт. События
живут в таблице и отмечаются.

Отличить их на клиенте можно по `id`: у состояний он строковый (`state:dry_run`),
у событий — числовой.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request, status
from sqlalchemy import func, select

from app.api.deps import GetDB, permits, requires
from app.api.v1.system import get_state
from app.core import clock
from app.core.access import Capability, Section
from app.db.models import Alert, AuditLog, Message
from app.services import embeddings, llm

logger = logging.getLogger("radar")

router = APIRouter(prefix="/api/v1/alerts", tags=["alerts"])

# Сколько необработанных сообщений считать поводом для тревоги. Порог, а не факт:
# сотня-другая в очереди — нормальный ход ингеста, тысячи — значит пересчёт давно
# не запускали и лиды не находятся.
PENDING_ALERT_THRESHOLD = 1000


async def _states(db) -> list[dict]:
    """Состояния системы. Не хранятся: они истинны ровно сейчас и никогда не «протухают»."""
    state = await get_state(db)
    out: list[dict] = []

    if state.killed:
        out.append({"id": "state:killed", "severity": "error", "ack": False,
                    "text": "Аварийная остановка: " + (state.killed_reason or "без причины"),
                    "created_at": None})
    out.append({
        "id": "state:mode", "ack": False, "created_at": None,
        "severity": "info" if state.mode == "DRY_RUN" else "error",
        "text": ("Сухой прогон: ни одно сообщение не уходит наружу"
                 if state.mode == "DRY_RUN"
                 else "ВНИМАНИЕ: режим LIVE — сообщения уходят людям"),
    })

    # Недоступность моделей — тоже состояние, а не событие: она либо есть сейчас,
    # либо нет, и хранить её значило бы показывать вчерашнюю поломку как сегодняшнюю.
    for name, ok, what in (("embeddings", await embeddings.ping(), "эмбеддер (L2)"),
                           ("llm", await llm.ping(), "модель (L3)")):
        if ok not in ("ok", "off"):
            out.append({"id": f"state:{name}", "severity": "error", "ack": False,
                        "created_at": None,
                        "text": f"{what} недоступен: {ok}. Сообщения копятся "
                                f"необработанными"})

    pending = (await db.execute(select(func.count(Message.id))
                                .where(Message.cascade_passed.is_(None)))).scalar_one()
    if pending >= PENDING_ALERT_THRESHOLD:
        out.append({"id": "state:pending", "severity": "warn", "ack": False,
                    "created_at": None,
                    "text": f"Необработанных сообщений: {pending}. "
                            f"Запустите переклассификацию в разделе Runs"})
    return out


@router.get("")
async def list_alerts(db: GetDB, user=requires(Section.DASHBOARD),
                      include_read: bool = False, limit: int = 50):
    stmt = select(Alert).order_by(Alert.created_at.desc()).limit(min(limit, 200))
    if not include_read:
        stmt = stmt.where(Alert.read_at.is_(None))
    rows = (await db.execute(stmt)).scalars().all()

    events = [{"id": a.id, "key": a.key, "severity": a.severity, "text": a.text,
               "ack": a.read_at is not None, "created_at": a.created_at.isoformat()
               if a.created_at else None}
              for a in rows]
    unread = (await db.execute(
        select(func.count(Alert.id)).where(Alert.read_at.is_(None)))).scalar_one()

    return {"rows": await _states(db) + events, "unread": unread}


@router.post("/{alert_id}/ack")
async def ack(alert_id: int, request: Request, db: GetDB,
              user=permits(Section.DASHBOARD, Capability.ALERT_ACK)):
    alert = (await db.execute(
        select(Alert).where(Alert.id == alert_id))).scalar_one_or_none()
    if alert is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"тревога {alert_id} не найдена")
    if alert.read_at is None:
        alert.read_at = clock.utcnow()
        db.add(AuditLog(user_id=user.id, user_email=user.email, action="alert_ack",
                        detail={"alert_id": alert_id, "key": alert.key},
                        ip=request.client.host if request.client else None))
        await db.commit()
    return {"id": alert_id, "ack": True}


@router.post("/ack-all")
async def ack_all(request: Request, db: GetDB,
                  user=permits(Section.DASHBOARD, Capability.ALERT_ACK)):
    rows = (await db.execute(
        select(Alert).where(Alert.read_at.is_(None)))).scalars().all()
    now = clock.utcnow()
    for a in rows:
        a.read_at = now
    if rows:
        db.add(AuditLog(user_id=user.id, user_email=user.email, action="alert_ack_all",
                        detail={"count": len(rows)},
                        ip=request.client.host if request.client else None))
        await db.commit()
    return {"acked": len(rows)}
