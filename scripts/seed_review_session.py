#!/usr/bin/env python3
"""Тестовые пользователи и готовая сессия для ревью интерфейса — без пароля и TOTP.

Зачем это существует отдельно от `seed_users.py`. Тот заводит **боевых** людей:
генерирует пароль, печатает его один раз и выдаёт секрет TOTP, который человек
заносит в аутентификатор. Для осмотра интерфейса всё это лишнее и вредное: пароль
и одноразовый код — то, что агент вводить не должен, а человека может не оказаться
рядом.

Здесь вместо входа выписывается уже подписанная сессия. Это законно ровно в одном
случае и только в нём: **стенд локальный, база тестовая, а ключ подписи задан для
стенда**. На боевом Radar `SECRET_KEY` другой и неизвестен, поэтому выписанная тут
кука там не значит ничего — обойти этим настоящий вход нельзя.

Заводятся все четыре роли: у `seed_users.py` их две, и из-за этого `reviewer` и
`viewer` было нечем проверить, а это ровно те роли, где ошибка прав и живёт.

Пароли задаются заведомо непригодные: войти обычным путём этими учётками нельзя,
только предъявив куку.

    RADAR_DATABASE_URL=... RADAR_SECRET_KEY=... uv run python -m scripts.seed_review_session
"""
from __future__ import annotations

import asyncio
import secrets
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import select  # noqa: E402

from app.core.config import get_settings  # noqa: E402
from app.core.security import SessionSigner, new_totp_secret  # noqa: E402
from app.db.models import User  # noqa: E402
from app.db.session import get_engine, get_session_maker  # noqa: E402

REVIEW_USERS = [
    ("review-owner@local", "Ревью-владелец", "РВ", "owner"),
    ("review-customer@local", "Ревью-заказчик", "РЗ", "customer"),
    ("review-reviewer@local", "Ревью-разборщик", "РР", "reviewer"),
    ("review-viewer@local", "Ревью-гость", "РГ", "viewer"),
]


async def main() -> None:
    settings = get_settings()
    signer = SessionSigner(settings.SECRET_KEY)
    maker = get_session_maker()

    async with maker() as db:
        out = []
        for email, name, initials, role in REVIEW_USERS:
            user = (await db.execute(
                select(User).where(User.email == email))).scalar_one_or_none()
            if user is None:
                user = User(
                    email=email, name=name, initials=initials, role=role,
                    # Хеш заведомо не совпадёт ни с каким паролем: эти учётки
                    # существуют только ради куки.
                    password_hash="!" + secrets.token_hex(16),
                    totp_secret=new_totp_secret(), totp_confirmed=True,
                    is_active=True)
                db.add(user)
                await db.flush()
            out.append((role, user.id,
                        signer.dumps({"uid": user.id, "totp_ok": True})))
        await db.commit()

    print("cookie:", settings.SESSION_COOKIE)
    for role, uid, value in out:
        print(f"{role}\t{uid}\t{value}")

    await get_engine().dispose()


if __name__ == "__main__":
    asyncio.run(main())
