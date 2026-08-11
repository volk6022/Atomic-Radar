"""Пароли, TOTP и подпись сессии.

Пароли — argon2, а не bcrypt: у bcrypt молчаливое обрезание входа на 72 байтах, что с
парольными фразами даёт неприятный сюрприз. TOTP обязателен обоим пользователям —
через эту админку одобряются сообщения живым людям от лица настоящих аккаунтов.
"""
from __future__ import annotations

import hmac
import secrets

import pyotp
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError, VerificationError
from itsdangerous import BadSignature, URLSafeTimedSerializer

_hasher = PasswordHasher()


def hash_password(password: str) -> str:
    return _hasher.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return _hasher.verify(password_hash, password)
    except (VerifyMismatchError, VerificationError):
        return False


def needs_rehash(password_hash: str) -> bool:
    """argon2 со временем меняет параметры; хеш можно обновить при следующем входе."""
    return _hasher.check_needs_rehash(password_hash)


def new_totp_secret() -> str:
    return pyotp.random_base32()


def totp_uri(secret: str, email: str, issuer: str = "Atomic Radar") -> str:
    """Строка для QR-кода в приложении-аутентификаторе."""
    return pyotp.TOTP(secret).provisioning_uri(name=email, issuer_name=issuer)


def verify_totp(secret: str, code: str) -> bool:
    """`valid_window=1` — принимаем соседнее окно: расхождение часов на телефоне
    иначе делает вход невозможным, а лишние 30 секунд ничего не меняют."""
    if not code or not code.isdigit():
        return False
    return pyotp.TOTP(secret).verify(code, valid_window=1)


def constant_time_eq(a: str, b: str) -> bool:
    return hmac.compare_digest(a.encode(), b.encode())


def new_token(n: int = 32) -> str:
    return secrets.token_urlsafe(n)


class SessionSigner:
    """Подписанная cookie: состояние сессии живёт у клиента, сервер только проверяет подпись."""

    def __init__(self, secret_key: str, salt: str = "radar-session"):
        self._s = URLSafeTimedSerializer(secret_key, salt=salt)

    def dumps(self, payload: dict) -> str:
        return self._s.dumps(payload)

    def loads(self, token: str, max_age: int) -> dict | None:
        try:
            return self._s.loads(token, max_age=max_age)
        except BadSignature:
            return None
