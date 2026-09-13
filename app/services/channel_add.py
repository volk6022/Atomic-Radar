"""Одна дорога подключения канала (план 13.4, волна Д).

Подключить канал можно ровно одним способом: внешний прогон `channel_add`
плюс заказанная им цепочка `join` → `chat_info_join`. Служба вынесена из
ручки `POST /api/v1/channels`, чтобы автоподключение одобренных кандидатов
(`autoflow.approve_tick`, сценарий 4) шло той же дорогой, что и ручка:
вторая копия цепочки разъехалась бы с первой молча, и «автоматика подключает
не так, как человек» всплыло бы на проде, где её никто не смотрит.

`start` не ловит `jobs.JobBusy`: «занято» — не ошибка подключения, а состояние
вида; решать, что с ним делать (тику — стоп перебора, ручке — 409), живёт
наверху, у вызывающего.
"""
from __future__ import annotations

import logging

from sqlalchemy import select

from app.db.models import AuditLog, Channel, Run
from app.services import engage, jobs

logger = logging.getLogger(__name__)


class ChannelExists(RuntimeError):
    """Канал с этим username уже отслеживается — подключать нечего."""


class ChannelDisabled(RuntimeError):
    """Канал уже был подключён, но отслеживание сняли — включать PATCH'ем."""


async def start(db, *, username: str, account_id: int, actor: str,
                ip: str | None = None) -> Run:
    """Поставить внешний прогон channel_add и заказать цепочку
    подписки/вступления.

    Проверки существующего канала — прежние отказы ручки, теперь исключениями:
    человек получает 409 с тем же текстом, тик — понятный «кандидат уже
    закрыт». `EngageUnavailable`/`ValueError` отмечают прогон упавшим и
    пробрасываются наверх — ручка переводит их в 503/400, а тик знает, что
    подключение не состоялось. `JobBusy` проходит мимо — см. докстринг модуля.
    """
    existing = (await db.execute(
        select(Channel).where(Channel.username == username))).scalar_one_or_none()
    if existing is not None and existing.ingest_enabled:
        raise ChannelExists(f"канал @{username} уже отслеживается (id {existing.id})")
    if existing is not None and not existing.ingest_enabled:
        raise ChannelDisabled(
            f"канал @{username} уже был подключён и отслеживание снято — "
            f"включите его снова (PATCH /channels/{existing.id}), а не "
            f"подключайте заново: аккаунт уже подписан")

    run = await jobs.create_external(
        db, kind="channel_add",
        params={"username": username, "engage_account_id": account_id},
        name=f"Подключение канала · @{username}", user_email=actor)

    try:
        # Через модульную глобальную, а не захваченную ссылку: подмена
        # `channel_add.start_join_chain` в тестах обязана быть видна здесь.
        await start_join_chain(account_id=account_id, username=username,
                               run_id=run.id, subscribed_by=actor, stage="channel")
    except engage.EngageUnavailable as e:
        await jobs.finish(run.id, status="failed", error=str(e),
                          note=f"Engage недоступен: {e}")
        raise
    except ValueError as e:  # закрытый список действий у engage.action
        await jobs.finish(run.id, status="failed", error=str(e), note=str(e))
        raise

    db.add(AuditLog(
        user_id=None, user_email=actor, action="channel_add_started",
        detail={"username": username, "engage_account_id": account_id,
               "run_id": run.id}, ip=ip))
    await db.commit()
    logger.info("channel_add_started username=%s account=%s run=%s by=%s",
                username, account_id, run.id, actor)
    return run


async def start_join_chain(*, account_id: int, username: str, run_id: int,
                           subscribed_by: str, stage: str,
                           channel_id: int | None = None) -> None:
    """Заказать `join_group` и подписать вебхук возврата так, чтобы `_handle_join`
    знал, куда вести цепочку дальше.

    `stage` различает два прохода одной и той же пары шагов (`join` →
    `chat_info_join`): `"channel"` — подписка на сам канал, `"linked"` — на его
    группу обсуждения, запрошенная вторым проходом уже известным `channel_id`.
    Без общего имени пришлось бы заводить две пары функций ради разницы в
    несколько строк на финализации.
    """
    # `target`, а не `username`: воркер Engage читает из payload `invite_link` или
    # `target` (`app/workers/join_group.py`) и про `username` не знает — с ним
    # вступление уходило в `join_chat(None)`. Проверено 31.08 по коду воркера на
    # проде; на стенде это не всплывало, потому что живого Engage там нет.
    await engage.action(
        account_id=account_id, action="join_group", payload={"target": username},
        webhook_url=engage.webhook_url(kind="join", account_id=account_id, username=username,
                                 run_id=run_id, subscribed_by=subscribed_by, stage=stage,
                                 channel_id=channel_id or 0))
