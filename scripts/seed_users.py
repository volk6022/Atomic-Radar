#!/usr/bin/env python3
"""Завести пользователей и стартовое состояние системы.

Пароли не задаются в коде и не берутся из аргументов командной строки (они попадают в
историю шелла и в список процессов) — скрипт генерирует их сам и печатает один раз.
Секрет TOTP печатается в виде ссылки otpauth://, её нужно открыть или превратить в
QR-код в приложении-аутентификаторе.

Запуск:
    python scripts/seed_users.py
    python scripts/seed_users.py --reset-password ivan@atomic-automation.net
"""
from __future__ import annotations

import argparse
import asyncio
import secrets
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import select  # noqa: E402

from app.core.config import get_settings  # noqa: E402
from app.core.security import hash_password, new_totp_secret, totp_uri  # noqa: E402
from app.db.models import Base, Limit, SystemState, User  # noqa: E402
from app.db.session import get_engine, get_session_maker  # noqa: E402

# Совпадает с USERS в оболочке GUI (contract/Atomic-Radar.md).
SEED_USERS = [
    {"email": "volk6932v2@gmail.com", "name": "Иван", "initials": "ИВ", "role": "owner"},
    {"email": "andrey@vertsanov.ru", "name": "Андрей", "initials": "АВ", "role": "customer"},
]

# Пороги гардрейлов. Значения обязаны совпадать с app/core/invariants.py — экран Safety
# их показывает и правит, но проверяет всё равно код.
SEED_LIMITS = [
    ("max_messages_per_conversation", 4, "шт", "больше — это преследование, а не диалог"),
    ("min_gap_hours", 20, "ч", "пауза между сообщениями одному человеку"),
    ("quiet_hours_start", 0, "ч", "начало тихих часов по времени собеседника"),
    ("quiet_hours_end", 8, "ч", "конец тихих часов"),
    ("max_links_first_message", 0, "шт", "ссылка в первом сообщении — прямой путь в спам"),
]


def new_password() -> str:
    return secrets.token_urlsafe(12)


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reset-password", metavar="EMAIL",
                    help="выдать новый пароль существующему пользователю")
    ap.add_argument("--reset-totp", metavar="EMAIL",
                    help="перевыпустить секрет TOTP (старый перестанет работать)")
    args = ap.parse_args()

    get_settings().validate_runtime()

    async with get_engine().begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with get_session_maker()() as db:
        if args.reset_password:
            user = (await db.execute(
                select(User).where(User.email == args.reset_password))).scalar_one_or_none()
            if not user:
                print(f"нет пользователя {args.reset_password}")
                return 1
            pwd = new_password()
            user.password_hash = hash_password(pwd)
            await db.commit()
            print(f"{user.email}: новый пароль {pwd}")
            return 0

        if args.reset_totp:
            user = (await db.execute(
                select(User).where(User.email == args.reset_totp))).scalar_one_or_none()
            if not user:
                print(f"нет пользователя {args.reset_totp}")
                return 1
            user.totp_secret = new_totp_secret()
            user.totp_confirmed = False
            await db.commit()
            print(f"{user.email}: {totp_uri(user.totp_secret, user.email)}")
            return 0

        created = []
        for spec in SEED_USERS:
            exists = (await db.execute(
                select(User).where(User.email == spec["email"]))).scalar_one_or_none()
            if exists:
                print(f"{spec['email']}: уже есть, пропускаю")
                continue
            pwd, secret = new_password(), new_totp_secret()
            db.add(User(**spec, password_hash=hash_password(pwd), totp_secret=secret))
            created.append((spec["email"], pwd, secret))

        state = (await db.execute(select(SystemState).limit(1))).scalar_one_or_none()
        if state is None:
            # Стартуем строго в DRY_RUN: включение отправки — отдельное осознанное действие.
            db.add(SystemState(id=1, mode="DRY_RUN"))
            print("system_state: создано, режим DRY_RUN")

        for key, value, unit, description in SEED_LIMITS:
            if (await db.execute(select(Limit).where(Limit.key == key))).scalar_one_or_none():
                continue
            db.add(Limit(key=key, value=value, unit=unit, description=description))

        await db.commit()

    if created:
        print("\n" + "=" * 70)
        print("СОХРАНИ ЭТО СЕЙЧАС — пароли больше нигде не появятся")
        print("=" * 70)
        for email, pwd, secret in created:
            print(f"\n{email}")
            print(f"  пароль: {pwd}")
            print(f"  TOTP:   {totp_uri(secret, email)}")
        print()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
