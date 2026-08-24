"""Приём вебхуков от Engage и запуск бэкфилла.

**Аутентификация — токеном в пути.** У Engage в задаче есть только `webhook_url`,
заголовок он поставить не может, поэтому секрет живёт в самом URL. URL используется
исключительно внутри docker-сети (`http://api-radar:8000/...`), наружу эта ручка
дополнительно закрыта на Caddy. Сравнение секрета — постоянного времени.

**Цепочка бэкфилла.** Оператор называет канал, дальше система разбирается сама:

    get_chat_info(канал) → узнаём peer_id и username связанной группы обсуждения
        → get_chat_info(группа) → узнаём её peer_id
            → get_chat_history(группа) → сообщения падают в БД
                → пока не набрали target, страница за страницей назад по истории

Каждый шаг асинхронный: Engage отвечает `task_id`, а результат приносит вебхуком.
Что именно приехало, определяется по параметрам запроса в `webhook_url` — так
корреляция не требует ни таблицы ожидающих задач, ни памяти процесса, переживающей
рестарт. Ответ `get_chat_history` не содержит идентификатора чата вовсе, поэтому
без этого приёма привязать пачку сообщений было бы не к чему.

**Пагинация — по `max_id`, и это не вкусовщина.** kurigram помечает `offset_id`
устаревшим и безусловно перезатирает его внутри `get_chat_history`, поэтому листание
по нему молча возвращает одну и ту же страницу — проверено живым прогоном на пяти
страницах подряд. Обратный курсор — `max_id`, он инклюзивный на стороне kurigram,
так что следующая страница запрашивается как `max_id = (самый старый id) - 1`.
"""
from __future__ import annotations

import hmac
import logging
from urllib.parse import urlencode

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import func, select

from app.api.deps import GetDB, requires
from app.core.access import Section
from app.core.config import get_settings
from app.db.models import Channel, Message
from app.services import engage, ingest as ingest_service, jobs

logger = logging.getLogger(__name__)

# Сколько сообщений просить за один вызов. Потолок Engage — 1000, но каждая страница
# приезжает одним вебхуком, и на тысяче постов это уже мегабайт JSON в одном запросе.
# 500 — компромисс: страниц вдвое больше, зато ни один ответ не становится проблемой
# сам по себе. Дневной бюджет чтений (2000 вызовов на аккаунт) при этом не жмёт.
PAGE_LIMIT = 500

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
        out["next"] = await _continue_backfill(db, out, q)
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
    limit = int(q.get("limit") or PAGE_LIMIT)
    target = int(q.get("target") or limit)
    run_id = int(q.get("run_id") or 0)
    next_step = None

    if chat_type == "channel" and linked and account_id:
        # Канал: читаем не его, а связанное обсуждение.
        await engage.action(
            account_id=account_id, action="get_chat_info",
            payload={"username": linked},
            webhook_url=_webhook_url(kind="chat_info", account_id=account_id,
                                     limit=limit, target=target, run_id=run_id))
        next_step = f"запрошена карточка группы @{linked}"
    # "forum" — та же супергруппа, только с включёнными темами. Читается ровно так же;
    # без неё в списке @amnezia_vpn молча не дал ни одного сообщения.
    elif chat_type in ("supergroup", "group", "forum") and account_id:
        # Продолжаем с того места, где остановились в прошлый раз. Без этого повторный
        # запуск с большей целью сначала перечитывал бы уже лежащие в базе свежие
        # страницы — идемпотентно, но это выброшенные вызовы к Telegram.
        resume = channel.backfill_cursor
        await _request_page(peer_id=peer_id, username=username, title=title,
                            account_id=account_id, limit=limit, target=target,
                            max_id=(resume - 1) if resume else 0, cursor=resume or 0,
                            run_id=run_id)
        next_step = (f"запрошена страница {'с ' + str(resume) if resume else 'с начала'}, "
                     f"цель {target} сообщений")
        if run_id:
            await jobs.progress(run_id, None, f"канал «{channel.title}»: {next_step}")
    elif chat_type == "channel" and not linked:
        next_step = "у канала нет группы обсуждения — читать нечего"
        if run_id:
            await jobs.finish(run_id, status="done", note=next_step,
                              result={"reason": "нет группы обсуждения"})

    logger.info("chat_info peer=%s type=%s linked=%s next=%s",
                peer_id, chat_type, linked, next_step)
    return {"accepted": 1, "channel_id": channel.id, "type": chat_type,
            "linked_chat_username": linked, "next": next_step}


async def _request_page(*, peer_id: int, username: str | None, title: str | None,
                        account_id: int, limit: int, target: int,
                        max_id: int, cursor: int, run_id: int = 0) -> None:
    """Заказать одну страницу истории. `cursor` едет обратно для защиты от зацикливания.

    `run_id` едет туда же: цепочку двигает Engage, а не наш процесс, и связать
    приехавшую страницу с задачей в `runs` можно только через адрес вебхука.
    """
    payload = {"username": username or str(peer_id), "limit": limit}
    if max_id:
        payload["max_id"] = max_id
    await engage.action(
        account_id=account_id, action="get_chat_history", payload=payload,
        webhook_url=_webhook_url(kind="history", peer_id=peer_id,
                                 username=username or "", title=title or "",
                                 account_id=account_id, limit=limit, target=target,
                                 prev_cursor=cursor, run_id=run_id))


async def _continue_backfill(db, out: dict, q) -> str:
    """Решить, просить ли следующую страницу, и попросить.

    Три причины остановиться, и все три должны быть различимы в логе, иначе
    «бэкфилл встал» превращается в гадание:

    * набрали target — работа сделана;
    * страница пришла пустой — история кончилась;
    * курсор не сдвинулся — Telegram отдал то же самое, и следующий запрос
      отдаст то же ещё раз. Именно так выглядела бы старая грабля с `offset_id`,
      поэтому проверка остаётся навсегда, а не «на время отладки».
    """
    account_id = int(q.get("account_id") or 0)
    target = int(q.get("target") or 0)
    run_id = int(q.get("run_id") or 0)

    async def stop(reason: str, *, ok: bool = True) -> str:
        """Закрыть задачу и вернуть причину остановки одним и тем же текстом.

        Причина обязана быть видна и в логе задачи, и в ответе на вебхук: «бэкфилл
        встал» без неё превращается в гадание, а причин ровно три и они разные.
        """
        if run_id:
            total_now = (await db.execute(
                select(func.count(Message.id))
                .where(Message.channel_id == out.get("channel_id", 0)))).scalar_one()
            await jobs.finish(run_id, status="done" if ok else "failed",
                              result={"reason": reason, "messages": total_now,
                                      "target": target},
                              error=None if ok else reason, note=reason)
        return reason

    if not account_id or not target:
        return "продолжение не запрошено (нет account_id/target)"

    cursor = out.get("backfill_cursor")
    prev_cursor = int(q.get("prev_cursor") or 0)

    if not out.get("accepted"):
        return await stop("история кончилась: страница пустая")
    if cursor is None:
        return await stop("нет курсора — продолжать не от чего")
    if prev_cursor and cursor >= prev_cursor:
        logger.warning("backfill_cursor_stuck channel=%s cursor=%s prev=%s",
                       out.get("channel_id"), cursor, prev_cursor)
        return await stop(
            f"курсор не сдвинулся ({cursor}) — остановка, чтобы не крутиться впустую")

    total = (await db.execute(
        select(func.count(Message.id))
        .where(Message.channel_id == out["channel_id"]))).scalar_one()
    if total >= target:
        return await stop(f"цель достигнута: {total} ≥ {target}")

    if run_id:
        await jobs.progress(run_id, 100 * total / target,
                            f"прочитано {total} из {target}")

    channel = (await db.execute(
        select(Channel).where(Channel.id == out["channel_id"]))).scalar_one()
    try:
        await _request_page(
            peer_id=channel.peer_id, username=channel.username, title=channel.title,
            account_id=account_id, limit=int(q.get("limit") or PAGE_LIMIT),
            target=target, max_id=cursor - 1, cursor=cursor, run_id=run_id)
    except engage.EngageUnavailable as e:
        # Отвечаем 200: сообщения этой страницы уже записаны, и переигрывать доставку
        # вебхука незачем — повтор только заново попросил бы ту же страницу.
        logger.warning("backfill_continue_failed channel=%s error=%s",
                       out.get("channel_id"), e)
        return await stop(f"страница записана, но продолжить не вышло: {e}", ok=False)
    return f"запрошена следующая страница: {total}/{target}, max_id={cursor - 1}"


# ── запуск ────────────────────────────────────────────────────────────────────

class BackfillRequest(BaseModel):
    username: str
    account_id: int = Field(gt=0)
    limit: int = Field(PAGE_LIMIT, ge=1, le=1000)
    # Сколько сообщений хочется набрать в этом канале всего. Страницы будут
    # запрашиваться одна за другой, пока не наберётся или пока история не кончится.
    target: int = Field(PAGE_LIMIT, ge=1, le=100_000)


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

    # Задача заводится ДО обращения к Engage: если он недоступен, строка со статусом
    # «упала» и причиной честнее, чем молчание. Исполнителя внутри API у неё нет —
    # цепочку двигают вебхуки, поэтому `create_external`.
    try:
        run = await jobs.create_external(
            db, kind="backfill",
            params={"username": username, "account_id": body.account_id,
                    "target": body.target, "limit": body.limit},
            name=f"Дочитать историю · @{username}", user_email=user.email)
    except jobs.JobBusy as e:
        raise HTTPException(409, str(e)) from e

    try:
        task = await engage.action(
            account_id=body.account_id, action="get_chat_info",
            payload={"username": username},
            webhook_url=_webhook_url(kind="chat_info", account_id=body.account_id,
                                     limit=body.limit, target=body.target,
                                     run_id=run.id))
    except engage.EngageUnavailable as e:
        await jobs.finish(run.id, status="failed", error=str(e),
                          note=f"Engage недоступен: {e}")
        raise HTTPException(503, str(e)) from e
    except ValueError as e:  # закрытый список действий
        await jobs.finish(run.id, status="failed", error=str(e), note=str(e))
        raise HTTPException(400, str(e)) from e

    await jobs.progress(run.id, 0, f"запрошена карточка @{username}, "
                                   f"цель {body.target} сообщений")
    logger.info("backfill_started username=%s account=%s target=%s task=%s run=%s by=%s",
                username, body.account_id, body.target, task.get("task_id"), run.id,
                user.email)
    return {"started": True, "username": username, "task_id": task.get("task_id"),
            "target": body.target, "run_id": run.id,
            "note": "результат придёт вебхуком; страницы дозапросятся сами, "
                    "ход виден в разделе Runs"}


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
