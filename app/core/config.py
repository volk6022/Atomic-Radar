"""Настройки сервиса. Секреты — только из окружения, значений по умолчанию у них нет."""
from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="RADAR_", extra="ignore")

    DATABASE_URL: str = "postgresql+asyncpg://radar:radar@localhost:5432/radar"

    # Подпись cookie-сессии. Пустая по умолчанию, и это проверяется при старте:
    # рабочий ключ, случайно уехавший в репозиторий, хуже упавшего сервиса.
    SECRET_KEY: str = ""
    SESSION_COOKIE: str = "radar_session"
    SESSION_MAX_AGE: int = 60 * 60 * 12

    # Engage — «руки» в Telegram. Radar ходит в него как обычный клиент.
    ENGAGE_BASE_URL: str = "http://localhost:8103"
    ENGAGE_API_KEY: str = ""

    # Адрес, по которому Engage достучится до нас с вебхуком. Внутрисетевой:
    # наружу ручка приёма не нужна и на Caddy закрыта.
    SELF_BASE_URL: str = "http://api-radar:8000"
    # Секрет в URL вебхука — заголовок Engage поставить не может.
    INGEST_TOKEN: str = ""

    # Стартовое значение режима. После первой миграции источник истины — БД:
    # переключатель в интерфейсе обязан действовать без рестарта.
    DEFAULT_MODE: str = "DRY_RUN"

    CORS_ORIGINS: list[str] = Field(default_factory=list)
    DEBUG: bool = False

    def validate_runtime(self) -> None:
        if not self.SECRET_KEY:
            raise RuntimeError(
                "RADAR_SECRET_KEY не задан — сессии подписывать нечем. "
                "Сгенерируй: python -c \"import secrets;print(secrets.token_urlsafe(32))\""
            )
        if self.DEFAULT_MODE not in ("DRY_RUN", "LIVE"):
            raise RuntimeError(f"RADAR_DEFAULT_MODE={self.DEFAULT_MODE!r}, ожидается DRY_RUN или LIVE")


@lru_cache
def get_settings() -> Settings:
    return Settings()
