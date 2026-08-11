"""Точка входа Radar API."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1 import auth, drafts, ingest, screens, system
from app.core.config import get_settings
from app.db.models import Base
from app.db.session import get_engine
from app.services import engage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("radar")


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    # Падаем на старте, а не на первом запросе: сервис без ключа подписи не должен
    # выглядеть работающим.
    settings.validate_runtime()

    try:
        async with get_engine().begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        app.state.db_ready = True
    except Exception as e:  # noqa: BLE001 — без БД отдаём 503, но поднимаемся
        app.state.db_ready = False
        logger.warning("db_not_ready: %s", e)

    logger.info("radar_started mode_default=%s", settings.DEFAULT_MODE)
    yield
    await engage.close()
    logger.info("radar_stopping")


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title="Atomic Radar API", version="0.1.0", lifespan=lifespan)

    if settings.CORS_ORIGINS:
        # Список задаётся явно. Cookie-сессия с `allow_credentials` и `*` несовместима,
        # и это правильно: иначе любой сайт сможет дёргать API от имени вошедшего.
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.CORS_ORIGINS,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    app.include_router(auth.router)
    app.include_router(system.router)
    app.include_router(drafts.router)
    app.include_router(ingest.router)
    app.include_router(ingest.operator_router)
    app.include_router(screens.router)

    @app.get("/api/v1/health")
    async def health():
        return {"status": "ok"}

    return app


app = create_app()
