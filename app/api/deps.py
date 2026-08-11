"""Зависимости FastAPI: сессия БД, текущий пользователь, проверка раздела.

Здесь же живёт единственный способ узнать, кто делает запрос. Роль берётся из
подписанной cookie и перепроверяется по БД: пользователя могли деактивировать или
понизить в правах уже после того, как он вошёл.
"""
from __future__ import annotations

from typing import Annotated, Callable

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.access import Section, can
from app.core.config import get_settings
from app.core.security import SessionSigner
from app.db.models import User
from app.db.session import get_session_maker


async def db_session():
    async with get_session_maker()() as session:
        yield session


GetDB = Annotated[AsyncSession, Depends(db_session)]


def _signer() -> SessionSigner:
    return SessionSigner(get_settings().SECRET_KEY)


async def current_user(request: Request, db: GetDB) -> User:
    s = get_settings()
    token = request.cookies.get(s.SESSION_COOKIE)
    if not token:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "not authenticated")

    payload = _signer().loads(token, max_age=s.SESSION_MAX_AGE)
    # Незаконченный вход (пароль принят, TOTP ещё нет) — это НЕ авторизованная сессия.
    if not payload or not payload.get("totp_ok"):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "not authenticated")

    user = (
        await db.execute(select(User).where(User.id == payload.get("uid")))
    ).scalar_one_or_none()
    if user is None or not user.is_active:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "not authenticated")
    return user


CurrentUser = Annotated[User, Depends(current_user)]


def requires(section: Section | str) -> Callable:
    """Ограничить ручку разделом из матрицы прав.

    Каждая ручка обязана это объявить. В GUI матрица только прячет пункты меню —
    скрытый пункт не мешает отправить запрос руками.
    """
    async def _guard(user: CurrentUser) -> User:
        if not can(user.role, section):
            raise HTTPException(
                status.HTTP_403_FORBIDDEN,
                f"role '{user.role}' has no access to '{Section(section).value}'",
            )
        return user

    return Depends(_guard)
