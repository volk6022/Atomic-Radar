"""Режим системы и kill switch — самые опасные ручки сервиса.

`DRY_RUN` ⇄ `LIVE` хранится в БД и читается на каждую попытку отправки. Переключение
сюда, а не в конфиг, именно потому, что оно обязано действовать немедленно: если LIVE
включили по ошибке, между «выключить» и «перестало отправлять» не должно быть рестарта.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel, field_validator
from sqlalchemy import select

from app.api.deps import GetDB, requires
from app.core.access import Section
from app.db.models import AuditLog, SystemState

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/system", tags=["system"])


class ModeRequest(BaseModel):
    mode: str

    @field_validator("mode")
    @classmethod
    def known_mode(cls, v: str) -> str:
        if v not in ("DRY_RUN", "LIVE"):
            raise ValueError("mode must be DRY_RUN or LIVE")
        return v


class KillRequest(BaseModel):
    reason: str = ""


async def get_state(db) -> SystemState:
    state = (await db.execute(select(SystemState).limit(1))).scalar_one_or_none()
    if state is None:
        state = SystemState(id=1)
        db.add(state)
        await db.commit()
    return state


async def current_mode(db) -> str:
    """То, что читает OutboundGate. Kill switch сильнее режима: пока он взведён,
    система считается остановленной, каким бы ни был mode."""
    state = await get_state(db)
    return "DRY_RUN" if state.killed else state.mode


@router.get("/mode")
async def read_mode(db: GetDB, user=requires(Section.SAFETY)):
    state = await get_state(db)
    return {
        "mode": state.mode,
        "effective_mode": "DRY_RUN" if state.killed else state.mode,
        "killed": state.killed,
        "killed_reason": state.killed_reason,
        "changed_by": state.changed_by,
    }


@router.post("/mode")
async def set_mode(body: ModeRequest, request: Request, db: GetDB,
                   user=requires(Section.SAFETY)):
    state = await get_state(db)

    # Пока взведён kill switch, включить LIVE нельзя. Снять аварийную остановку —
    # отдельное осознанное действие, а не побочный эффект переключения режима.
    if body.mode == "LIVE" and state.killed:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            "kill switch активен: сначала снимите аварийную остановку",
        )

    previous, state.mode, state.changed_by = state.mode, body.mode, user.email
    db.add(AuditLog(
        user_id=user.id, user_email=user.email, action="system_mode",
        detail={"from": previous, "to": body.mode},
        ip=request.client.host if request.client else None,
    ))
    await db.commit()
    logger.warning("system_mode_changed %s -> %s by %s", previous, body.mode, user.email)
    return {"mode": state.mode, "previous": previous}


@router.post("/kill")
async def kill(body: KillRequest, request: Request, db: GetDB,
               user=requires(Section.SAFETY)):
    """Аварийная остановка. Немедленно переводит эффективный режим в DRY_RUN —
    воркерам не нужно ничего перечитывать, гейт спросит режим на следующей же попытке."""
    state = await get_state(db)
    state.killed = True
    state.killed_reason = body.reason or f"kill switch by {user.email}"
    state.changed_by = user.email
    db.add(AuditLog(
        user_id=user.id, user_email=user.email, action="system_kill",
        detail={"reason": state.killed_reason},
        ip=request.client.host if request.client else None,
    ))
    await db.commit()
    logger.error("KILL SWITCH by %s: %s", user.email, state.killed_reason)
    return {"killed": True, "effective_mode": "DRY_RUN"}


@router.post("/resume")
async def resume(request: Request, db: GetDB, user=requires(Section.SAFETY)):
    """Снять аварийную остановку. Режим при этом НЕ восстанавливается в LIVE —
    возвращаться к отправке нужно отдельным решением."""
    state = await get_state(db)
    state.killed = False
    state.killed_reason = None
    state.mode = "DRY_RUN"
    state.changed_by = user.email
    db.add(AuditLog(
        user_id=user.id, user_email=user.email, action="system_resume",
        ip=request.client.host if request.client else None,
    ))
    await db.commit()
    return {"killed": False, "mode": "DRY_RUN"}
