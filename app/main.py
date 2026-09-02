"""Точка входа Radar API."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1 import (alerts, auth, conversations, drafts, ingest, leads,
                        manual_sends, profile, runs, screens, system, wf_queues)
# Псевдоним по той же причине, что и у сценариев ниже: рядом живёт `app.services.events`.
from app.api.v1 import events as events_api
# Роутер и сервис называются одинаково; без псевдонима второй импорт молча затирает
# первый, и `include_router` уходит в модуль сервисов.
from app.api.v1 import workflows as workflows_api
from app.core.config import get_settings
from app.db.migrate import apply as apply_migrations
from app.db.models import Base
from app.db.session import get_engine, get_session_maker
from app.services import cascade_registry, engage, engage_registry, jobs, queue, workflows

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
            # Колонки, появившиеся после первой накатки: create_all их не добавит.
            await apply_migrations(conn)
        app.state.db_ready = True

        # Задачи, пережившие смерть процесса, честно помечаем прерванными: иначе
        # строка навсегда останется в «выполняется» с прогрессом, который уже
        # никогда не сдвинется.
        #
        # Но только когда прогоны идут здесь. С включённой очередью они живут в
        # воркере, которого наш рестарт не касается: пометка отсюда оборвала бы на
        # экране работу, идущую прямо сейчас, и заодно отпустила бы `active_run` —
        # оператор запустил бы вторую переклассификацию поверх первой, обе на одной
        # видеокарте. Метит их воркер на своём старте (`app/workers/jobs.py`).
        if queue.enabled():
            logger.info("jobs_interrupt_sweep_left_to_worker")
        else:
            stale = await jobs.mark_interrupted()
            if stale:
                logger.warning("jobs_interrupted_on_start count=%s", stale)

        # Реестр инстансов Engage. Заводим строку из настроек процесса, если реестр
        # пуст, — так уже работающая установка переживает выкатку без ручного шага.
        engage_registry.install()
        async with get_session_maker()() as db:
            await engage_registry.ensure_bootstrap(db)
            await engage_registry.reload(db)
            # Сценарий ЛС существовал молча — установка работает ровно по нему.
            # Заводим его строкой, чтобы накопленным данным было к чему привязаться.
            await workflows.ensure_bootstrap(db)

            # Таксономия каскада (FIXES.md #5): до первой правки она и так лежит в
            # константах кода — заводим версию `v1` из них же, чтобы редактор писал
            # поверх реального состояния, а не поверх пустоты, и перечитываем её в
            # module-level словари каскада на каждый старт процесса.
            await cascade_registry.ensure_bootstrap(db)
            await cascade_registry.reload(db)
        # Вне сессии выше: правку могут активировать из воркера или из другого
        # реплика API, и этот процесс обязан её увидеть без своего рестарта.
        cascade_registry.start_watch()
    except Exception as e:  # noqa: BLE001 — без БД отдаём 503, но поднимаемся
        app.state.db_ready = False
        logger.warning("db_not_ready: %s", e)

    logger.info("radar_started mode_default=%s", settings.DEFAULT_MODE)
    yield
    await cascade_registry.stop_watch()
    await engage.close()
    await queue.close()
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
    app.include_router(alerts.router)
    app.include_router(events_api.router)
    app.include_router(leads.router)
    app.include_router(manual_sends.router)
    app.include_router(runs.router)
    app.include_router(screens.router)
    # Экран «Переписки» отдельным роутером по той же причине, что и тревоги:
    # у него есть отметка «прочитано», то есть побочный эффект.
    app.include_router(conversations.router)
    app.include_router(profile.router)
    app.include_router(workflows_api.router)
    # Данные сценария отдельным роутером: у реестра выше права намеренно слабые
    # (меню рисуется до входа в раздел), а здесь — матрица на каждой ручке.
    app.include_router(wf_queues.router)

    @app.get("/api/v1/health")
    async def health():
        return {"status": "ok"}

    return app


app = create_app()
