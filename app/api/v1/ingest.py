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

import hashlib
import hmac
import json
import logging
from collections.abc import Mapping

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select

from app.api.deps import GetDB, permits, requires
from app.core import clock
from app.core.access import Capability, Section
from app.core.config import get_settings
from app.db.models import AuditLog, Channel, Message
from app.services import alerts, channels as channels_service
from app.services import discussions as discussions_service
from app.services import engage
from app.services import ingest as ingest_service
from app.services import jobs, queue

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


def _check_token(token: str) -> None:
    expected = get_settings().INGEST_TOKEN
    # Сравниваем байты, а не строки. `compare_digest` на строках требует, чтобы обе
    # были ASCII, и на кириллице в пути роняет `TypeError` — то есть `500` вместо
    # `404`, да ещё и по-разному на «не тот токен» и «не тот алфавит». Токен приходит
    # из URL, значит там может быть что угодно.
    if not expected or not hmac.compare_digest(token.encode("utf-8"),
                                               expected.encode("utf-8")):
        # 404, а не 403: не подтверждаем существование ручки тому, кто не знает токен.
        raise HTTPException(404, "not found")


# ── приём ─────────────────────────────────────────────────────────────────────

@router.post("/{token}")
async def receive(token: str, request: Request, response: Response, db: GetDB):
    """Единая точка приёма всех событий Engage.

    С включённой очередью ручка делает ровно две вещи: проверяет секрет и ставит
    событие в очередь. Разбор — дело воркера, и ответ `202` честно говорит «принято»,
    а не «сделано».

    Зачем так. Разбор внутри запроса держит соединение Engage на всё время работы с
    базой и моделями, а тяжёлый прогон конкурирует за event loop с ручками интерфейса
    в том же процессе. Сверх того, работа, живущая в памяти процесса API, умирает
    вместе с ним — ровно та беда, из-за которой прогоны сейчас помечаются
    «прерванными» при каждом рестарте.

    **Очередь выключена — старое поведение целиком.** Не «деградация»: на стенде и в
    тестах Redis не нужен и не должен быть нужен, а разбор в запросе там ничему не
    мешает. Обе ветки зовут один и тот же `process_event`, чтобы поведение не могло
    разъехаться между ними.

    **Очередь включена и не отвечает — `503` и тревога.** Разбирать в обход очереди
    в этом случае было бы худшим из решений: под упавшим Redis система тихо
    вернулась бы к тому поведению, от которого уходит, и никто бы не заметил.

    Отказ при этом не означает потерю. Отправитель считает удачей всё, что меньше
    `400` (`webhook_sender.py:41`), поэтому `202` он примет, а `503` перевезёт в
    повторы — их у него пять (`MAX_ATTEMPTS`). Короткая недоступность очереди
    закрывается ими и не доходит до данных. Вторая сеть — бэкфилл: он перечитывает
    историю канала, и сообщение, пропущенное после всех пяти попыток, приезжает
    следующим проходом. Сеть третьей не предусмотрено намеренно: журнал принятых, но
    не разобранных событий был бы четвёртым местом, где живёт правда о сообщениях.
    """
    _check_token(token)

    body = await request.json()
    q = dict(request.query_params)

    if queue.enabled():
        try:
            job = await queue.enqueue(queue.INGEST_EVENT, body, q, _job_id=_event_id(body, q))
        except queue.QueueUnavailable as e:
            await alerts.emit(
                db, key="ingest_queue_down", severity="error",
                text=f"Очередь приёма не отвечает, события Engage не разбираются: {e}")
            await db.commit()
            raise HTTPException(503, "очередь приёма недоступна") from e
        response.status_code = 202
        return {"queued": job or "duplicate"}

    return await process_event(db, body, q)


def _event_id(body: dict, q: Mapping[str, str]) -> str:
    """Устойчивый ключ события — чтобы повторная доставка не разбиралась дважды.

    Считается по содержимому, а не по времени: Engage ретраит доставку при неудачном
    ответе, и один и тот же вебхук может приехать несколько раз. Разбор идемпотентен и
    сам по себе (сообщения кладутся upsert-ом), так что это не единственная защита, а
    вторая — и заодно экономия работы.
    """
    payload = json.dumps([body, sorted(q.items())], sort_keys=True, ensure_ascii=False)
    return "ingest:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


async def process_event(db, body: dict, q: Mapping[str, str]) -> dict:
    """Разбор одного события Engage. Зовётся из ручки и из воркера — один и тот же код.

    Держит `db` параметром, а не берёт сессию сам: в ручке транзакция уже открыта
    зависимостью, а воркер открывает свою. Кто открыл — тот и закрывает.
    """
    event = body.get("event")

    if event == "incoming_message":
        result = await ingest_service.ingest_incoming_message(db, body)
        await db.commit()
        return result

    if event == "task_failed":
        # Провал шага бэкфилла — не ошибка приёма. Логируем и отвечаем 200: Engage
        # иначе будет ретраить доставку вебхука, хотя переигрывать тут нечего.
        #
        # Задачу в `runs`, если она у этого шага есть, закрываем здесь же —
        # раньше не закрывал никто, и строка навсегда оставалась «выполняется» с
        # прогрессом, который уже не сдвинется: ровно та беда, которую в других
        # местах чинил `mark_interrupted`, а тут её было некому чинить вообще.
        run_id = int(q.get("run_id") or 0)
        reason = _translate_engage_reason(str(body.get("error_code") or ""))
        logger.warning("engage_task_failed task=%s error=%s kind=%s run=%s",
                       body.get("task_id"), body.get("error_code"), q.get("kind"), run_id)
        if run_id:
            await jobs.finish(run_id, status="failed", error=reason,
                              note=f"задача Engage не выполнена: {reason}")
        return {"accepted": 0, "error": body.get("error_code")}

    if event != "task_complete":
        logger.info("engage_event_ignored event=%s", event)
        return {"accepted": 0, "ignored": event}

    result = body.get("result") or {}
    if not result.get("found", True):
        reason_code = result.get("reason")
        reason_text = _translate_engage_reason(reason_code)
        logger.warning("engage_not_found kind=%s reason=%s target=%s",
                       q.get("kind"), reason_code, q.get("username"))
        run_id = int(q.get("run_id") or 0)
        if run_id:
            # `stage=linked` — вторая половина подключения канала (вступление в
            # группу обсуждения), и к этому моменту канал уже заведён и
            # отслеживается: неудача здесь не отменяет первую половину. Задача
            # закрывается «готово», а не «упала» — иначе владелец увидел бы
            # красный крест над каналом, который на самом деле подключён.
            if q.get("kind") in ("join", "chat_info_join") and q.get("stage") == "linked":
                await jobs.finish(
                    run_id, status="done",
                    result={"channel_id": int(q.get("channel_id") or 0),
                           "linked_group_joined": False, "reason": reason_code},
                    note=f"канал подключён, но в группу обсуждения вступить не "
                        f"вышло: {reason_text}. Комментарии не будут приходить в "
                        f"реальном времени, пока аккаунт не вступит в неё")
            else:
                await jobs.finish(run_id, status="failed", error=reason_text,
                                  note=f"не найдено: {reason_text}")
        return {"accepted": 0, "reason": reason_code, "reason_text": reason_text}

    kind = q.get("kind")

    if kind == "chat_info":
        return await _handle_chat_info(db, result, q)

    if kind == "history":
        peer_id = q.get("peer_id")
        if not peer_id:
            raise HTTPException(400, "в webhook_url не передан peer_id группы")
        out = await ingest_service.ingest_history(
            db, chat_id=int(peer_id), chat_username=q.get("username"),
            chat_title=q.get("title"), posts=result.get("posts") or [],
            # Аккаунт, читавший страницу, едет параметром вебхука с самого заказа
            # страницы (`_request_page`) — истории без читателя не бывает, но
            # требовать его здесь жёстко значило бы уронить приём старых адресов.
            account_id=int(q.get("account_id") or 0) or None)
        await db.commit()
        out["next"] = await _continue_backfill(db, out, q)
        return out

    if kind == "polled":
        # Ответ на задачу, за результатом которой пошли опросом (`app/services/
        # discussions.py`). Обратный адрес у Engage обязателен по контракту, поэтому
        # вебхук всё равно приезжает — и разбирать его здесь значило бы положить те
        # же сообщения вторым путём. Молчаливое «неизвестный kind» тоже не годится:
        # в логах оно неотличимо от настоящей рассинхронизации адресов.
        return {"accepted": 0, "polled": True}

    if kind == "join":
        return await _handle_join(db, result, q)

    if kind == "chat_info_join":
        return await _handle_chat_info_join(db, result, q)

    logger.info("engage_task_complete_unknown_kind kind=%s", kind)
    return {"accepted": 0, "ignored": "kind=" + str(kind)}


# Причины отказа Engage человеческим текстом. Список собран по кодам, уже
# встречавшимся в переписке с Андреем и в блокерах Engage (`andrey-access.md`,
# `engage-blockers.md`) — не исчерпывающий: неизвестный код отдаётся как есть,
# без выдумывания смысла, которого мы не проверяли.
_ENGAGE_REASON_TEXT: dict[str, str] = {
    "username_not_found": "такого username не существует — проверьте, нет ли опечатки",
    "username_not_occupied": "такого username не существует — проверьте, нет ли опечатки",
    "username_invalid": "некорректный username — проверьте написание",
    "channel_private": "канал приватный: обычная подписка недоступна, нужна инвайт-ссылка",
    "chat_private": "канал приватный: обычная подписка недоступна, нужна инвайт-ссылка",
    "invite_request_sent": "вступление закрыто: заявка отправлена, ждите подтверждения администратора",
    "user_already_participant": "аккаунт уже состоит в этом канале",
    "user_banned_in_channel": "аккаунт заблокирован в этом канале",
    "chat_admin_required": "нужны права администратора канала",
    "flood_wait": "Telegram временно ограничил действия аккаунта — попробуйте позже",
}


def _translate_engage_reason(reason: str | None) -> str:
    if not reason:
        return "Engage не назвал причину"
    return _ENGAGE_REASON_TEXT.get(reason.strip().lower(), f"Engage: {reason}")


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
    channel.chat_type = chat_type
    # Отметка «карточку спрашивали» ставится здесь же, а не только в пакетном
    # разборе: иначе один и тот же факт означал бы разное в зависимости от того,
    # каким путём он получен, и экран показывал бы «не проверяли» у канала, чью
    # карточку только что прочитали (FIXES.md #3).
    channel.linked_checked_at = clock.utcnow()
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
            webhook_url=engage.webhook_url(kind="chat_info", account_id=account_id,
                                     limit=limit, target=target, run_id=run_id))
        next_step = f"запрошена карточка группы @{linked}"
    # "forum" — та же супергруппа, только с включёнными темами. Читается ровно так же;
    # без неё в списке @amnezia_vpn молча не дал ни одного сообщения.
    elif chat_type in ("supergroup", "group", "forum") and account_id:
        # Продолжаем с того места, где остановились в прошлый раз. Без этого повторный
        # запуск с большей целью сначала перечитывал бы уже лежащие в базе свежие
        # страницы — идемпотентно, но это выброшенные вызовы к Telegram.
        resume = channel.backfill_cursor
        await _request_page(peer_id=peer_id, username=username,
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


async def _start_join_chain(*, account_id: int, username: str, run_id: int,
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


async def _handle_join(db, result: dict, q) -> dict:
    """Итог `join_group`: аккаунт вступил, дальше резолвим карточку отдельным
    `get_chat_info` — Engage у `join_group` отдаёт `chat_id: null` (известная и не
    закрытая на его стороне недоделка, engage-blockers.md #7.6), а peer_id и
    название нужны, чтобы завести строку `channels` (или дописать в неё
    `linked_chat_peer_id`, если это второй проход — стадия `"linked"`).
    """
    account_id = int(q.get("account_id") or 0)
    username = q.get("username") or ""
    run_id = int(q.get("run_id") or 0)
    subscribed_by = q.get("subscribed_by") or ""
    stage = q.get("stage") or "channel"
    channel_id = int(q.get("channel_id") or 0) or None

    if run_id:
        await jobs.progress(
            run_id, 40 if stage == "channel" else 80,
            f"аккаунт {account_id} подписан на @{username}"
            + (" (канал)" if stage == "channel" else " (группа обсуждения)")
            + ", запрошена карточка")
    try:
        await engage.action(
            account_id=account_id, action="get_chat_info", payload={"username": username},
            webhook_url=engage.webhook_url(kind="chat_info_join", account_id=account_id,
                                     username=username, run_id=run_id,
                                     subscribed_by=subscribed_by, stage=stage,
                                     channel_id=channel_id or 0))
    except engage.EngageUnavailable as e:
        if run_id:
            note = (f"канал подключён, но карточку группы обсуждения запросить "
                    f"не вышло: {e}" if stage == "linked" else
                    f"подписались, но карточку канала запросить не вышло: {e}")
            # Стадия `linked` идёт после того, как канал уже заведён — упавший
            # запрос карточки группы не отменяет то, что уже сделано.
            await jobs.finish(run_id, status="done" if stage == "linked" else "failed",
                              error=None if stage == "linked" else str(e), note=note,
                              result={"channel_id": channel_id} if stage == "linked" else None)
        raise
    logger.info("engage_join_done account=%s username=%s stage=%s run=%s",
               account_id, username, stage, run_id)
    return {"accepted": 1, "joined": True, "username": username, "stage": stage}


async def _handle_chat_info_join(db, result: dict, q) -> dict:
    """Финал одного прохода подключения канала: карточка получена.

    Стадия `"channel"` заводит строку `channels`, включает отслеживание и, если у
    канала есть группа обсуждения, запускает второй проход `_start_join_chain` тем
    же аккаунтом на неё — без вступления в группу вотчер видит только посты
    канала, а лиды живут в комментариях. Стадия `"linked"` дописывает
    `linked_chat_peer_id` уже существующему каналу и закрывает задачу.

    В отличие от `_handle_chat_info` (бэкфилл), историю здесь не запрашиваем ни
    на одной стадии: добавление канала и «Дочитать историю» — два разных решения
    оператора, FIXES.md #7 их не объединяет.
    """
    peer_id = result.get("peer_id")
    username = result.get("username") or q.get("username")
    title = result.get("title")
    account_id = int(q.get("account_id") or 0)
    run_id = int(q.get("run_id") or 0)
    subscribed_by = q.get("subscribed_by") or None
    stage = q.get("stage") or "channel"

    if stage == "linked":
        channel_id = int(q.get("channel_id") or 0)
        channel = (await db.execute(
            select(Channel).where(Channel.id == channel_id))).scalar_one_or_none()
        if channel is not None:
            channel.linked_chat_peer_id = peer_id
            if username:
                channel.linked_chat_username = username
            # Момент вступления, а не просто «мы знаем про группу». Именно с него у
            # канала появляется живой поток комментариев: историю Telegram отдаёт и
            # постороннему, апдейты — только участнику (FIXES.md #3).
            channel.linked_joined_at = clock.utcnow()
            channel.linked_checked_at = clock.utcnow()
            await db.commit()
        if run_id:
            await jobs.finish(
                run_id, status="done",
                result={"channel_id": channel_id, "linked_chat_peer_id": peer_id,
                       "linked_group_joined": True},
                note=f"канал подключён вместе с группой обсуждения @{username} — "
                    f"комментарии теперь видны вотчеру в реальном времени")
        logger.info("channel_linked_group_joined channel=%s linked_peer=%s account=%s",
                   channel_id, peer_id, account_id)
        return {"accepted": 1, "channel_id": channel_id, "linked_chat_peer_id": peer_id}

    channel = await ingest_service.get_or_create_channel(
        db, peer_id=peer_id, username=username, title=title)
    channel.members = result.get("members_count")
    linked = result.get("linked_chat_username")
    channel.linked_chat_username = linked
    channel.chat_type = result.get("type")
    channel.linked_checked_at = clock.utcnow()
    channel.ingest_enabled = True
    channel.subscribed_account_id = account_id
    channel.subscribed_by = subscribed_by
    channel.subscribed_at = clock.utcnow()
    await db.commit()
    logger.info("channel_added channel=%s peer=%s username=%s account=%s by=%s linked=%s",
               channel.id, peer_id, username, account_id, subscribed_by, linked)

    if linked and account_id:
        try:
            await _start_join_chain(account_id=account_id, username=linked, run_id=run_id,
                                    subscribed_by=subscribed_by or "", stage="linked",
                                    channel_id=channel.id)
        except engage.EngageUnavailable as e:
            if run_id:
                await jobs.finish(
                    run_id, status="done", result={"channel_id": channel.id},
                    note=f"канал «{channel.title}» подключён, но подписаться на "
                        f"группу обсуждения @{linked} не вышло: {e}")
            return {"accepted": 1, "channel_id": channel.id, "peer_id": peer_id,
                   "username": username, "linked_join_failed": str(e)}
        if run_id:
            await jobs.progress(run_id, 60,
                                f"канал «{channel.title}» подключён, вступаем в "
                                f"группу обсуждения @{linked}")
        return {"accepted": 1, "channel_id": channel.id, "peer_id": peer_id,
               "username": username, "next": f"вступление в группу @{linked}"}

    if run_id:
        await jobs.finish(run_id, status="done",
                          result={"channel_id": channel.id, "peer_id": peer_id,
                                  "username": username},
                          note=f"канал «{channel.title}» подключён и отслеживается"
                              + ("" if linked else " (группы обсуждения нет)"))
    return {"accepted": 1, "channel_id": channel.id, "peer_id": peer_id, "username": username}


async def _request_page(*, peer_id: int, username: str | None,
                        account_id: int, limit: int, target: int,
                        max_id: int, cursor: int, run_id: int = 0) -> None:
    """Заказать одну страницу истории. `cursor` едет обратно для защиты от зацикливания.

    `run_id` едет туда же: цепочку двигает Engage, а не наш процесс, и связать
    приехавшую страницу с задачей в `runs` можно только через адрес вебхука.

    Названия группы в адресе нет намеренно, и это не экономия. `tasks.webhook_url` у
    Engage — `varchar(500)`, а кириллическое название в percent-encoding раздувается
    вшестеро: у «ВЭД чат (таможенное оформление, сертификация, грузоперевозки, экспорт,
    импорт)» одно только название заняло больше четырёхсот символов, адрес не влез, и
    Engage ответил `500` на вставку задачи. Цепочка вставала на первой же странице —
    29.08 так потерялся весь бэкфилл @CentrVED. Название и не нужно: строка канала уже
    заведена шагом `chat_info`, а `get_or_create_channel` при пустом названии берёт
    существующее. В адрес возврата кладём только то, без чего страницу не привязать.
    """
    payload = {"username": username or str(peer_id), "limit": limit}
    if max_id:
        payload["max_id"] = max_id
    await engage.action(
        account_id=account_id, action="get_chat_history", payload=payload,
        webhook_url=engage.webhook_url(kind="history", peer_id=peer_id,
                                 username=username or "",
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
            peer_id=channel.peer_id, username=channel.username,
            account_id=account_id, limit=int(q.get("limit") or PAGE_LIMIT),
            target=target, max_id=cursor - 1, cursor=cursor, run_id=run_id)
    except engage.EngageUnavailable as e:
        # Отвечаем 200: сообщения этой страницы уже записаны, и переигрывать доставку
        # вебхука незачем — повтор только заново попросил бы ту же страницу.
        logger.warning("backfill_continue_failed channel=%s error=%s",
                       out.get("channel_id"), e)
        return await stop(f"страница записана, но продолжить не вышло: {e}", ok=False)
    return f"запрошена следующая страница: {total}/{target}, max_id={cursor - 1}"


# ── подключение и отключение каналов (FIXES.md #7) ────────────────────────────

class AddChannelRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    username: str = Field(min_length=1)
    engage_account_id: int = Field(gt=0)


@operator_router.post("", status_code=201)
async def add_channel(body: AddChannelRequest, request: Request, db: GetDB,
                      user=permits(Section.CHANNELS, Capability.CHANNEL_ADD)):
    """Подключить канал по `@username`: резолв через Engage, подписка выбранным
    аккаунтом, заведение строки канала и (если есть) её группы обсуждения.

    Раньше строка канала заводилась только неявно — при первом принятом
    сообщении (`ingest_service.get_or_create_channel`), и подключить канал из
    интерфейса было нельзя вовсе, только со стороны Engage. Ответ приходит сразу
    и означает «подписка запрошена», а не «канал подключён»: `join_group` у
    Engage идёт через хьюманайзер с паузой 60–300 секунд — итог придёт вебхуком,
    ход виден в разделе Runs.
    """
    username = body.username.lstrip("@").strip()
    if not username:
        raise HTTPException(422, "пустой username")

    existing = (await db.execute(
        select(Channel).where(Channel.username == username))).scalar_one_or_none()
    if existing is not None and existing.ingest_enabled:
        raise HTTPException(409, f"канал @{username} уже отслеживается (id {existing.id})")
    if existing is not None and not existing.ingest_enabled:
        raise HTTPException(
            409, f"канал @{username} уже был подключён и отслеживание снято — "
                f"включите его снова (PATCH /channels/{existing.id}), а не "
                f"подключайте заново: аккаунт уже подписан")

    try:
        run = await jobs.create_external(
            db, kind="channel_add",
            params={"username": username, "engage_account_id": body.engage_account_id},
            name=f"Подключение канала · @{username}", user_email=user.email)
    except jobs.JobBusy as e:
        raise HTTPException(409, str(e)) from e

    try:
        await _start_join_chain(account_id=body.engage_account_id, username=username,
                                run_id=run.id, subscribed_by=user.email, stage="channel")
    except engage.EngageUnavailable as e:
        await jobs.finish(run.id, status="failed", error=str(e),
                          note=f"Engage недоступен: {e}")
        raise HTTPException(503, str(e)) from e
    except ValueError as e:  # закрытый список действий у engage.action
        await jobs.finish(run.id, status="failed", error=str(e), note=str(e))
        raise HTTPException(400, str(e)) from e

    db.add(AuditLog(
        user_id=user.id, user_email=user.email, action="channel_add_started",
        detail={"username": username, "engage_account_id": body.engage_account_id,
               "run_id": run.id}, ip=request.client.host if request.client else None))
    await db.commit()
    logger.info("channel_add_started username=%s account=%s run=%s by=%s",
               username, body.engage_account_id, run.id, user.email)
    return {"started": True, "username": username, "run_id": run.id,
           "note": "аккаунт подписывается на канал (и на его группу обсуждения, "
                   "если она есть); результат придёт вебхуком, ход виден в разделе Runs"}


class UpdateChannelRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    ingest_enabled: bool


@operator_router.patch("/{channel_id}")
async def update_channel(channel_id: int, body: UpdateChannelRequest, request: Request,
                         db: GetDB, user=permits(Section.CHANNELS, Capability.CHANNEL_ARCHIVE)):
    """Снять или вернуть отслеживание — отдельно от удаления сообщений (см. ниже).

    Читает и пишет ровно то же поле, что сам себе молча ставит `get_or_create_channel`
    при первом сообщении: включение здесь не заводит канал заново и не трогает
    накопленное, только решает, продолжать ли класть новые сообщения.
    """
    channel = (await db.execute(
        select(Channel).where(Channel.id == channel_id))).scalar_one_or_none()
    if channel is None:
        raise HTTPException(404, f"канал {channel_id} не найден")

    was = channel.ingest_enabled
    channel.ingest_enabled = body.ingest_enabled
    db.add(AuditLog(
        user_id=user.id, user_email=user.email, action="channel_tracking_changed",
        detail={"channel_id": channel_id, "title": channel.title,
               "from": was, "to": body.ingest_enabled},
        ip=request.client.host if request.client else None))
    await db.commit()
    logger.info("channel_tracking_changed channel=%s from=%s to=%s by=%s",
               channel_id, was, body.ingest_enabled, user.email)
    return {"id": channel.id, "ingest_enabled": channel.ingest_enabled}


@operator_router.delete("/{channel_id}/messages")
async def delete_channel_messages(channel_id: int, request: Request, db: GetDB,
                                  user=permits(Section.CHANNELS, Capability.CHANNEL_ARCHIVE)):
    """Удалить накопленные сообщения канала — отдельное решение от снятия
    отслеживания (см. `update_channel`): один щёлкает выключатель, другой стирает
    архив, и объединять их в одну кнопку FIXES.md запрещает прямым текстом.

    Канал не удаляется: строка остаётся, чтобы к ней было куда прийти новым
    сообщениям, если отслеживание снова включат. Обнуляются только производные —
    счётчик лидов и курсор бэкфилла, читать историю заново придётся с начала.
    """
    channel = (await db.execute(
        select(Channel).where(Channel.id == channel_id))).scalar_one_or_none()
    if channel is None:
        raise HTTPException(404, f"канал {channel_id} не найден")

    counts = await channels_service.purge_messages(db, channel_id=channel_id)
    channel.leads_total = 0
    channel.backfill_cursor = None
    db.add(AuditLog(
        user_id=user.id, user_email=user.email, action="channel_messages_purged",
        detail={"channel_id": channel_id, "title": channel.title, **counts},
        ip=request.client.host if request.client else None))
    await db.commit()
    logger.info("channel_messages_purged channel=%s by=%s counts=%s",
               channel_id, user.email, counts)
    return {"channel_id": channel_id, "deleted": counts}


# ── группы обсуждения (FIXES.md #3) ───────────────────────────────────────────

@operator_router.get("/discussions")
async def discussions_summary(db: GetDB, user=requires(Section.CHANNELS)):
    """Сколько каналов в каком состоянии по группам обсуждения.

    Нужен затем же, зачем и весь пункт 3: «ноль сообщений» на экране до сих пор
    значило и «у канала нет обсуждения», и «мы про него не спрашивали», и «группа
    есть, но мы её не читаем». Это три разные причины и три разных следующих шага,
    и сводка называет их по именам, чтобы решение принималось по числам.
    """
    channels = (await db.execute(select(Channel))).scalars().all()
    counts = dict((await db.execute(
        select(Message.channel_id, func.count(Message.id))
        .group_by(Message.channel_id))).all())
    by_name = {c.username.lower(): c for c in channels if c.username}

    out = {"unknown": 0, "none": 0, "unread": 0, "history": 0, "live": 0,
           "groups": 0}
    for c in channels:
        if c.chat_type in discussions_service.GROUP_TYPES:
            out["groups"] += 1
            continue
        state = discussions_service.discussion_state(c, by_name, counts)["state"]
        out[state] = out.get(state, 0) + 1
    return out


class ScanDiscussionsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    # `pending` по умолчанию — «всё, что не разобрано»: карточку не спрашивали или
    # группа известна и не прочитана. Порознь эти два множества обманчивы: на 31.08
    # в проде «известна и не прочитана» — 2 канала при 61 молчащей группе, потому
    # что у остальных карточку просто ни разу не спрашивали.
    scope: str = Field("pending", pattern="^(pending|unread|unknown|all|ids)$")
    channel_ids: list[int] = Field(default_factory=list)
    account_ids: list[int] = Field(default_factory=list)
    target: int = Field(PAGE_LIMIT, ge=1, le=10_000)
    # Только спросить карточки и записать связь, историю не дочитывать. Карте
    # «где у каналов группа обсуждения вообще есть» чтение не нужно, а стоит оно
    # дороже всего остального: страницы истории плюс каскад на каждое сообщение.
    check_only: bool = False


@operator_router.post("/discussions/scan", status_code=202)
async def scan_discussions(body: ScanDiscussionsRequest, request: Request, db: GetDB,
                           user=permits(Section.CHANNELS, Capability.RUN_BACKFILL)):
    """Разобрать группы обсуждения списком: связь + история.

    С `check_only=true` — только связь: карточки опрошены, группы заведены,
    история не читается (граница — в `app/services/discussions.py`, `_scan_one`).

    Кнопка «Запустить бэкфилл всем» на экране Channels этого не умеет и не сможет:
    она перебирает строки, которые видит, а у групп, которые мы ни разу не читали,
    строк в Радаре нет вовсе — их ещё предстоит завести по карточке канала. Отсюда
    отдельный прогон: он идёт от каналов, а не от того, что уже лежит в базе.

    Вступление в группу сюда не входит намеренно. Оно меняет поведение аккаунта в
    Telegram, а не наши данные, живёт в подключении канала (FIXES.md #7) и упирается
    в профиль безопасности флота. Разбор же — только чтение, публичную супергруппу
    Telegram отдаёт и постороннему.
    """
    if body.scope == "ids" and not body.channel_ids:
        raise HTTPException(422, "scope=ids требует непустой channel_ids")

    params = {"scope": body.scope, "channel_ids": body.channel_ids,
              "account_ids": body.account_ids, "target": body.target,
              "check_only": body.check_only}
    try:
        run = await jobs.start(db, kind="discussions", params=params,
                               name="Разбор групп обсуждения", user_email=user.email)
    except jobs.JobBusy as e:
        raise HTTPException(409, str(e)) from e
    except jobs.JobQueueDown as e:
        raise HTTPException(503, str(e)) from e

    db.add(AuditLog(
        user_id=user.id, user_email=user.email, action="discussions_scan_started",
        detail={**params, "run_id": run.id},
        ip=request.client.host if request.client else None))
    await db.commit()
    logger.info("discussions_scan_started scope=%s target=%s run=%s by=%s",
                body.scope, body.target, run.id, user.email)
    if body.check_only:
        note = ("разбор идёт в разделе Runs: у каждого канала спрашиваем карточку "
                "и заводим его группу обсуждения, историю не читаем")
    else:
        note = ("разбор идёт в разделе Runs: у каждого канала спрашиваем "
                "карточку, заводим его группу обсуждения и дочитываем её историю")
    return {"started": True, "run_id": run.id, "note": note}


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
            webhook_url=engage.webhook_url(kind="chat_info", account_id=body.account_id,
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
