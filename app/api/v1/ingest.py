"""Приём вебхуков от Engage и запуск бэкфилла.

**Аутентификация — токеном в пути.** У Engage в задаче есть только `webhook_url`,
заголовок он поставить не может, поэтому секрет живёт в самом URL. URL используется
исключительно внутри docker-сети (`http://api-radar:8000/...`), наружу эта ручка
дополнительно закрыта на Caddy. Сравнение секрета — постоянного времени.

**Цепочка бэкфилла.** Оператор называет канал, дальше система разбирается сама:

    get_chat_info(канал) → узнаём peer_id и username связанной группы обсуждения
        → get_chat_info(группа) → узнаём её peer_id
            → get_chat_history(группа) → сообщения падают в БД

Каждый шаг асинхронный: Engage отвечает `task_id`, а результат приносит вебхуком.
Что именно приехало, определяется по параметрам запроса в `webhook_url` — так
корреляция не требует ни таблицы ожидающих задач, ни памяти процесса, переживающей
рестарт. Ответ `get_chat_history` не содержит идентификатора чата вовсе, поэтому
без этого приёма привязать пачку сообщений было бы не к чему.
"""
from __future__ import annotations

import hmac
import logging
from urllib.parse import urlencode

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import select

from app.api.deps import GetDB, requires
from app.core.access import Section
from app.core.config import get_settings
from app.db.models import Channel
from app.services import engage, ingest as ingest_service

logger = logging.getLogger(__name__)

# Две поверхности с разной аудиторией, поэтому и роутера два.
#
# `/api/v1/ingest/*` — машинная: сюда стучится Engage, снаружи она закрыта на Caddy.
# `/api/v1/channels/*` — операторская: её зовёт браузер под обычной сессией. Держать
# кнопку «прочитать историю» под закрытым префиксом означало бы, что она не работает
# ровно у того, для кого сделана.
router = APIRouter(prefix="/api/v1/ingest", tags=["ingest"])
operator_router = APIRouter(prefix="/api/v1/channels", tags=["ingest"])


def _webhook_url(**params) -> str:
    s = get_settings()
    base = s.SELF_BASE_URL.rstrip("/")
    return f"{base}/api/v1/ingest/{s.INGEST_TOKEN}?{urlencode(params)}"


def _check_token(token: str) -> None:
    expected = get_settings().INGEST_TOKEN
    if not expected or not hmac.compare_digest(token, expected):
        # 404, а не 403: не подтверждаем существование ручки тому, кто не знает токен.
        raise HTTPException(404, "not found")


# ── приём ─────────────────────────────────────────────────────────────────────

@router.post("/{token}")
async def receive(token: str, request: Request, db: GetDB):
    """Единая точка приёма всех событий Engage."""
    _check_token(token)

    body = await request.json()
    event = body.get("event")
    q = request.query_params

    if event == "incoming_message":
        result = await ingest_service.ingest_incoming_message(db, body)
        await db.commit()
        return result

    if event == "task_failed":
        # Провал шага бэкфилла — не ошибка приёма. Логируем и отвечаем 200: Engage
        # иначе будет ретраить доставку вебхука, хотя переигрывать тут нечего.
        logger.warning("engage_task_failed task=%s error=%s kind=%s",
                       body.get("task_id"), body.get("error_code"), q.get("kind"))
        return {"accepted": 0, "error": body.get("error_code")}

    if event != "task_complete":
        logger.info("engage_event_ignored event=%s", event)
        return {"accepted": 0, "ignored": event}

    result = body.get("result") or {}
    if not result.get("found", True):
        logger.warning("engage_not_found kind=%s reason=%s target=%s",
                       q.get("kind"), result.get("reason"), q.get("username"))
        return {"accepted": 0, "reason": result.get("reason")}

    kind = q.get("kind")

    if kind == "chat_info":
        return await _handle_chat_info(db, result, q)

    if kind == "history":
        peer_id = q.get("peer_id")
        if not peer_id:
            raise HTTPException(400, "в webhook_url не передан peer_id группы")
        out = await ingest_service.ingest_history(
            db, chat_id=int(peer_id), chat_username=q.get("username"),
            chat_title=q.get("title"), posts=result.get("posts") or [])
        await db.commit()
        return out

    logger.info("engage_task_complete_unknown_kind kind=%s", kind)
    return {"accepted": 0, "ignored": "kind=" + str(kind)}


async def _handle_chat_info(db, result: dict, q) -> dict:
    """Карточка чата: сохраняем и решаем, что делать дальше.

    У канала читать нечего — комментарии живут в связанной группе. Поэтому для канала
    следующим шагом запрашивается карточка его группы, и только у группы — история.
    """
    peer_id = result.get("peer_id")
    chat_type = result.get("type")
    username = result.get("username")
    title = result.get("title")
    linked = result.get("linked_chat_username")

    channel = await ingest_service.get_or_create_channel(
        db, peer_id=peer_id, username=username, title=title)
    channel.members = result.get("members_count")
    channel.linked_chat_username = linked
    await db.commit()

    account_id = int(q.get("account_id") or 0) or None
    limit = int(q.get("limit") or 200)
    next_step = None

    if chat_type == "channel" and linked and account_id:
        # Канал: читаем не его, а связанное обсуждение.
        await engage.action(
            account_id=account_id, action="get_chat_info",
            payload={"username": linked},
            webhook_url=_webhook_url(kind="chat_info", account_id=account_id, limit=limit))
        next_step = f"запрошена карточка группы @{linked}"
    elif chat_type in ("supergroup", "group") and account_id:
        await engage.action(
            account_id=account_id, action="get_chat_history",
            payload={"username": username or str(peer_id), "limit": limit},
            webhook_url=_webhook_url(kind="history", peer_id=peer_id,
                                     username=username or "", title=title or ""))
        next_step = f"запрошена история {limit} сообщений"
    elif chat_type == "channel" and not linked:
        next_step = "у канала нет группы обсуждения — читать нечего"

    logger.info("chat_info peer=%s type=%s linked=%s next=%s",
                peer_id, chat_type, linked, next_step)
    return {"accepted": 1, "channel_id": channel.id, "type": chat_type,
            "linked_chat_username": linked, "next": next_step}


# ── запуск ────────────────────────────────────────────────────────────────────

class BackfillRequest(BaseModel):
    username: str
    account_id: int = Field(gt=0)
    limit: int = Field(200, ge=1, le=1000)


@operator_router.post("/backfill")
async def start_backfill(body: BackfillRequest, db: GetDB,
                         user=requires(Section.CHANNELS)):
    """Запустить цепочку по username канала или группы.

    Ответ приходит сразу и означает «задача принята Engage», а не «сообщения в базе»:
    чтение идёт через очередь аккаунта и упирается в дневной бюджет чтений.
    """
    username = body.username.lstrip("@").strip()
    if not username:
        raise HTTPException(422, "пустой username")

    try:
        task = await engage.action(
            account_id=body.account_id, action="get_chat_info",
            payload={"username": username},
            webhook_url=_webhook_url(kind="chat_info", account_id=body.account_id,
                                     limit=body.limit))
    except engage.EngageUnavailable as e:
        raise HTTPException(503, str(e)) from e
    except ValueError as e:  # закрытый список действий
        raise HTTPException(400, str(e)) from e

    logger.info("backfill_started username=%s account=%s task=%s by=%s",
                username, body.account_id, task.get("task_id"), user.email)
    return {"started": True, "username": username, "task_id": task.get("task_id"),
            "note": "результат придёт вебхуком; следите за каналом в разделе Channels"}


@operator_router.get("/ingest-status")
async def ingest_status(db: GetDB, user=requires(Section.CHANNELS)):
    """Что уже приехало: каналы, сообщения, лиды. Первое, что спрашивают после запуска."""
    from sqlalchemy import func

    from app.db.models import Lead, Message

    channels = (await db.execute(select(func.count(Channel.id)))).scalar_one()
    messages = (await db.execute(select(func.count(Message.id)))).scalar_one()
    passed = (await db.execute(select(func.count(Message.id))
                               .where(Message.cascade_passed.is_(True)))).scalar_one()
    leads = (await db.execute(select(func.count(Lead.id)))).scalar_one()
    return {"channels": channels, "messages": messages,
            "passed_cascade": passed, "leads": leads}
