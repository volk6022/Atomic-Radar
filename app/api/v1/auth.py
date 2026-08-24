"""Вход: пароль, затем TOTP. Две ступени — две ручки.

Оболочка GUI уже устроена именно так (`step: 'login' → 'totp'`), поэтому серверная
часть повторяет её шаги один в один.

Важная деталь: после успешного пароля выдаётся cookie с `totp_ok: false`. Такая сессия
не проходит `current_user` и не открывает ни одной ручки с данными — она годится
только для второго шага. Иначе «половина входа» была бы полноценным доступом.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request, Response, status
from pydantic import BaseModel
from sqlalchemy import select

from app.api.deps import CurrentUser, GetDB
from app.core.access import capabilities_for, sections_for
from app.core.clock import utcnow
from app.core.config import get_settings
from app.core.security import SessionSigner, verify_password, verify_totp
from app.db.models import AuditLog, User

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/auth", tags=["auth"])


class LoginRequest(BaseModel):
    username: str
    password: str


class TotpRequest(BaseModel):
    code: str


def _set_cookie(response: Response, payload: dict) -> None:
    s = get_settings()
    response.set_cookie(
        s.SESSION_COOKIE,
        SessionSigner(s.SECRET_KEY).dumps(payload),
        max_age=s.SESSION_MAX_AGE,
        httponly=True,       # JS не должен видеть сессию
        secure=not s.DEBUG,  # только по TLS; в отладке иначе не войти по http
        samesite="lax",
    )


async def _audit(db, user: User | None, action: str, request: Request, detail=None) -> None:
    db.add(AuditLog(
        user_id=user.id if user else None,
        user_email=user.email if user else None,
        action=action,
        detail=detail,
        ip=request.client.host if request.client else None,
    ))
    await db.commit()


@router.post("/login")
async def login(body: LoginRequest, request: Request, response: Response, db: GetDB):
    user = (
        await db.execute(select(User).where(User.email == body.username.strip().lower()))
    ).scalar_one_or_none()

    # Один и тот же ответ на «нет такого пользователя» и «неверный пароль»: разные
    # ответы позволяют перебором узнать, кто здесь зарегистрирован.
    if user is None or not user.is_active or not verify_password(body.password, user.password_hash):
        await _audit(db, user, "login_failed", request)
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid credentials")

    _set_cookie(response, {"uid": user.id, "totp_ok": False})
    return {"step": "totp"}


@router.post("/totp")
async def totp(body: TotpRequest, request: Request, response: Response, db: GetDB):
    s = get_settings()
    token = request.cookies.get(s.SESSION_COOKIE)
    payload = SessionSigner(s.SECRET_KEY).loads(token, max_age=s.SESSION_MAX_AGE) if token else None
    if not payload:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "login first")

    user = (
        await db.execute(select(User).where(User.id == payload.get("uid")))
    ).scalar_one_or_none()
    if user is None or not user.is_active:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "login first")

    if not verify_totp(user.totp_secret, body.code):
        await _audit(db, user, "totp_failed", request)
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid code")

    user.last_login_at = utcnow()
    user.last_login_ip = request.client.host if request.client else None
    if not user.totp_confirmed:
        user.totp_confirmed = True
    await _audit(db, user, "login_ok", request)

    _set_cookie(response, {"uid": user.id, "totp_ok": True})
    return _me(user)


@router.post("/logout")
async def logout(response: Response):
    response.delete_cookie(get_settings().SESSION_COOKIE)
    return {"ok": True}


@router.get("/me")
async def me(user: CurrentUser):
    return _me(user)


def _me(user: User) -> dict:
    # `sections` отдаём с сервера, чтобы оболочке не приходилось держать свою копию
    # матрицы прав и расходиться с ней при каждой правке.
    return {
        "name": user.name,
        "initials": user.initials,
        "email": user.email,
        "role": user.role,
        "sections": sections_for(user.role),
        # Рядом с разделами — список разрешённых действий: по нему оболочка прячет
        # кнопки, которые роль всё равно не сможет нажать. Держать эту таблицу
        # второй копией во фронтенде значит однажды показать кнопку, отдающую 403.
        "capabilities": capabilities_for(user.role),
    }
