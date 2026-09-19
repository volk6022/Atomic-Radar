"""Экран «Переписки»: список диалогов, нитка целиком и отметка о прочтении.

Жил в `screens.py`, пока был чтением. Появилась отметка «прочитано» — побочный
эффект, — и блок переехал сюда ровно по той причине, по которой туда раньше
уехали тревоги и лиды: `screens.py` остаётся набором ручек без побочных
эффектов, и это свойство удобно проверять взглядом на список ручек.

Непрочитанность считается одним правилом — `Conversation.unread` в модели, у
гибрида питоновская и SQL-половины. Копия условия здесь значила бы счётчик,
который однажды разойдётся со списком, и доверия к экрану не останется.

Автоматических отправок в этом модуле нет и не появится: ответ человека идёт
через существующий механизм ручных отправок, здесь только чтение и отметки.
"""
from __future__ import annotations

from datetime import datetime

import logging

from fastapi import APIRouter, Body, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from sqlalchemy import func, select

from app.api.deps import GetDB, permits, requires
from app.api.v1.listing import ListParams, apply_sort, list_params
from app.core import clock
from app.core.access import Capability, Section
from app.db.models import Conversation, ConversationEvent, Lead, Message, WfTarget
from app.services import conversation_reply, engage

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["conversations"])

CONVERSATION_STATES = ("new", "awaiting_reply", "replied", "handed_off", "closed")

CONVERSATION_SORTS = {"created": Conversation.created_at, "sent": Conversation.sent_count,
                      "last": Conversation.last_sent_at, "state": Conversation.state}


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None


async def _peer_info(db, conv: Conversation) -> dict:
    """Имя и username собеседника. Откуда брать — по привязке нитки:

    * старый контур (`lead_id`) — у лида, как раньше;
    * цель нового контура (`target_id`) — у наводки, а если автор там не назвался —
      у сообщения, из которого наводка выросла: публичные посты бывают анонимными;
    * «unsolicited» — то, что запомнилось при первом касании (чаще всего ник).

    Ровно один запрос в первых двух случаях и ни одного в третьем: список
    добирает имена построчно, и лишний запрос здесь удваивал бы плату за страницу.
    """
    if conv.lead_id is not None:
        lead = (await db.execute(
            select(Lead).where(Lead.id == conv.lead_id))).scalar_one_or_none()
        name = lead.author_name if lead else None
        username = lead.author_username if lead else None
    elif conv.target_id is not None:
        row = (await db.execute(
            select(WfTarget, Message)
            .join(Message, WfTarget.message_id == Message.id)
            .where(WfTarget.id == conv.target_id))).first()
        if row is None:
            name = username = None
        else:
            target, message = row
            name = target.author_name or message.author_name
            username = target.author_username or message.author_username
    else:
        name = username = conv.peer_username
    return {"peer_name": name,
            "peer_username": ("@" + username) if username else None}


@router.get("/conversations")
async def conversations(db: GetDB, user=requires(Section.CONVERSATIONS),
                        p: ListParams = Depends(list_params),
                        state: str | None = None, unread_only: bool = True):
    """Диалоги. Пока система в сухом прогоне, их не будет ни одного — и это
    не поломка экрана, а главное свойство режима.

    Фильтр по состоянию считается здесь: на клиенте он работал бы только по уже
    загруженной странице, а диалоги — единственная сущность, которая растёт
    без ограничений сверху.

    `unread_only` включён по умолчанию: человек заходит в раздел раз в сутки
    разгребать новое, а не листать всё подряд. Считается в базе и входит в
    `total` — как `state`. Чипы состояний считаются под тем же фильтром:
    чип с числом, которое не совпадает с длиной списка после клика по нему, —
    это счётчик, которому перестают верить.

    `unread_total` — непрочитанные по всей базе, без фильтров и страницы: это
    значок в боковой панели, и он не должен зависеть от того, какую страницу
    и с каким состоянием открыл человек.
    """
    if state and state not in CONVERSATION_STATES:
        raise HTTPException(422, f"неизвестное состояние «{state}», ожидается одно из "
                                 f"{', '.join(CONVERSATION_STATES)}")

    q = select(Conversation)
    count_q = select(func.count(Conversation.id))
    states_q = select(Conversation.state, func.count(Conversation.id))
    if state:
        q = q.where(Conversation.state == state)
        count_q = count_q.where(Conversation.state == state)
    if unread_only:
        q = q.where(Conversation.unread)
        count_q = count_q.where(Conversation.unread)
        # Своё состояние из разбора не выкидываем: чипы показывают и остальные
        # состояния, чтобы переключаться было куда.
        states_q = states_q.where(Conversation.unread)

    total = (await db.execute(count_q)).scalar_one()
    q = apply_sort(q, p, CONVERSATION_SORTS, default="created", tiebreak=Conversation.id)
    rows = (await db.execute(q.limit(p.limit).offset(p.offset))).scalars().all()
    out = []
    for c in rows:
        out.append({
            "id": c.id, "lead_id": c.lead_id, "peer_id": c.peer_id,
            **await _peer_info(db, c),
            # Аккаунт — id в Engage, как у `wf_outbound`/`manual_sends`: локальное
            # зеркало `accounts` мертво, и нитки на него не ссылаются с 16.1.
            "engage_account_id": c.engage_account_id,
            "source": c.source, "target_id": c.target_id,
            "state": c.state, "sent_count": c.sent_count,
            "last_sent_at": _iso(c.last_sent_at), "last_inbound_at": _iso(c.last_inbound_at),
            "unread": c.unread,
        })
    by_state = dict((await db.execute(states_q.group_by(Conversation.state))).all())
    unread_total = (await db.execute(
        select(func.count(Conversation.id)).where(Conversation.unread))).scalar_one()

    return {**p.page(total), "rows": out, "state": state, "unread_only": unread_only,
            "unread_total": unread_total,
            "states": [{"key": k, "count": by_state.get(k, 0)}
                       for k in CONVERSATION_STATES],
            "note": None if out else
                    ("Диалогов в этом состоянии нет" if state else
                     "Непрочитанных диалогов нет" if unread_only else
                     "Диалогов нет: в сухом прогоне ни одно сообщение не отправляется")}


@router.get("/conversations/{conversation_id}")
async def conversation_thread(conversation_id: int, db: GetDB,
                              user=requires(Section.CONVERSATIONS)):
    """Нитка целиком: журнал событий по возрастанию времени плюс шапка диалога.

    Порядок — `created_at` с `id` вторым ключом: события одной секунды без
    дополнительного ключа Postgres волен вернуть в любом порядке, а перевёрнутая
    пара «вопрос — ответ» в переписке меняет смысл на противоположный.

    Чтение нитки не отмечает её прочитанной: это делает отдельная ручка, и тогда
    значок гаснет в тот момент, когда человек действительно подтвердил прочтение,
    а не когда список догрузился чьим-то запросом.
    """
    conv = (await db.execute(
        select(Conversation).where(Conversation.id == conversation_id))
    ).scalar_one_or_none()
    if conv is None:
        raise HTTPException(404, f"диалог {conversation_id} не найден")

    events = (await db.execute(
        select(ConversationEvent)
        .where(ConversationEvent.conversation_id == conversation_id)
        .order_by(ConversationEvent.created_at.asc(), ConversationEvent.id.asc())
    )).scalars().all()

    peer = await _peer_info(db, conv)

    return {
        "conversation": {
            "id": conv.id, "lead_id": conv.lead_id, "peer_id": conv.peer_id,
            **peer,
            "engage_account_id": conv.engage_account_id,
            "source": conv.source, "target_id": conv.target_id,
            "state": conv.state,
            "sent_count": conv.sent_count,
            "last_sent_at": _iso(conv.last_sent_at),
            "last_inbound_at": _iso(conv.last_inbound_at),
            "waiting_since": _iso(conv.waiting_since),
            "handed_off_at": _iso(conv.handed_off_at),
            "read_at": _iso(conv.read_at), "unread": conv.unread,
        },
        # `created_at` — момент записи в журнал, `at` — момент события по его
        # источнику: у ручной отправки это то, когда человек реально отправил,
        # а не когда записал. Показываются оба, потому что разница содержательна.
        "events": [{"id": e.id, "kind": e.kind, "source": e.source, "actor": e.actor,
                    "text": e.text, "at": _iso(e.at), "payload": e.payload,
                    "created_at": e.created_at.isoformat()} for e in events],
    }


@router.post("/conversations/{conversation_id}/read")
async def conversation_mark_read(conversation_id: int, db: GetDB,
                                 user=requires(Section.CONVERSATIONS)):
    """Отметить нитку прочитанной: `read_at` двигается в «сейчас».

    Перезаписывается и тогда, когда отметка уже стоит: после нового входящего
    диалог снова непрочитан, и повторное прочтение — основной случай, а не
    исключение. У тревог отметка одноразовая, здесь — нет, и это не одно и то же
    свойство, скопированное дважды.
    """
    conv = (await db.execute(
        select(Conversation).where(Conversation.id == conversation_id))
    ).scalar_one_or_none()
    if conv is None:
        raise HTTPException(404, f"диалог {conversation_id} не найден")

    conv.read_at = clock.utcnow()
    await db.commit()
    return {"id": conv.id, "read_at": _iso(conv.read_at), "unread": conv.unread}


async def _conv_or_404(db, conversation_id: int) -> Conversation:
    conv = await db.get(Conversation, conversation_id)
    if conv is None:
        raise HTTPException(404, f"диалог {conversation_id} не найден")
    return conv


@router.get("/conversations/{conversation_id}/reply-preflight")
async def reply_preflight(conversation_id: int, db: GetDB, text: str = "",
                          user=permits(Section.CONVERSATIONS,
                                       Capability.CONVERSATION_REPLY)):
    """Что покажет кнопка «ответить»: аккаунт, вердикт гейта, живая попытка.

    Ничего не пишет — сетевые ходы только читающие. Право — CONVERSATION_REPLY:
    ответ в диалоге — это писать людям от имени заказчика, как DRAFT_SEND.
    """
    conv = await _conv_or_404(db, conversation_id)
    return await conversation_reply.preflight(db, conv=conv, text=text,
                                             now=clock.utcnow())


@router.post("/conversations/{conversation_id}/reply",
             status_code=status.HTTP_202_ACCEPTED)
async def reply(conversation_id: int, db: GetDB,
                user=permits(Section.CONVERSATIONS, Capability.CONVERSATION_REPLY),
                body: dict = Body(default={})):
    """Заказать ответ в диалоге через Engage: `{"text": str}` → 202.

    Заказ принят, сообщение ещё не доставлено — подтверждение приедет вебхуком
    `kind="send"`; до него попытка `pending`. Форма отказа едина с черновиком:
    409 `{detail, reasons}`.
    """
    conv = await _conv_or_404(db, conversation_id)
    try:
        row = await conversation_reply.order(
            db, conv=conv, text=str(body.get("text") or ""), actor=user.email,
            now=clock.utcnow())
    except conversation_reply.ReplyBlocked as e:
        return JSONResponse(status_code=status.HTTP_409_CONFLICT, content={
            "detail": "; ".join(e.reasons) or "ответ заблокирован",
            "reasons": e.reasons})
    except conversation_reply.ReplyConflict as e:
        return JSONResponse(status_code=status.HTTP_409_CONFLICT, content={
            "detail": str(e), "reasons": []})
    except engage.EngageUnavailable as e:
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, str(e)) from e
    logger.info("conversation_reply conversation=%s outbound=%s account=%s by=%s",
                conv.id, row.id, row.engage_account_id, user.email)
    return {"outbound_id": row.id, "task_id": row.engage_task_id,
            "state": row.state, "account_id": row.engage_account_id}


@router.post("/conversations/{conversation_id}/handoff")
async def handoff(conversation_id: int, db: GetDB,
                  user=permits(Section.CONVERSATIONS, Capability.CONVERSATION_STATE)):
    """Передать диалог человеку вне Радара: `state=handed_off`, момент передачи."""
    conv = await conversation_reply.set_state(
        db, conv=await _conv_or_404(db, conversation_id), state="handed_off",
        actor=user.email, now=clock.utcnow())
    return {"id": conv.id, "state": conv.state,
            "handed_off_at": _iso(conv.handed_off_at)}


@router.post("/conversations/{conversation_id}/close")
async def close(conversation_id: int, db: GetDB,
                user=permits(Section.CONVERSATIONS, Capability.CONVERSATION_STATE)):
    """Закрыть диалог: `state=closed`; ответ в закрытый диалог гейт не пропустит."""
    conv = await conversation_reply.set_state(
        db, conv=await _conv_or_404(db, conversation_id), state="closed",
        actor=user.email, now=clock.utcnow())
    return {"id": conv.id, "state": conv.state,
            "handed_off_at": _iso(conv.handed_off_at)}
