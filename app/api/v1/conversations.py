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

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select

from app.api.deps import GetDB, requires
from app.api.v1.listing import ListParams, apply_sort, list_params
from app.core import clock
from app.core.access import Section
from app.db.models import Conversation, ConversationEvent, Lead

router = APIRouter(prefix="/api/v1", tags=["conversations"])

CONVERSATION_STATES = ("new", "awaiting_reply", "replied", "handed_off", "closed")

CONVERSATION_SORTS = {"created": Conversation.created_at, "sent": Conversation.sent_count,
                      "last": Conversation.last_sent_at, "state": Conversation.state}


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None


def _peer(lead: Lead | None) -> dict:
    """Имя собеседника. У диалога оно живёт у лида, а не в самой нитке."""
    return {"peer_name": lead.author_name if lead else None,
            "peer_username": ("@" + lead.author_username)
                             if lead and lead.author_username else None}


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
        lead = (await db.execute(
            select(Lead).where(Lead.id == c.lead_id))).scalar_one_or_none()
        out.append({
            "id": c.id, "lead_id": c.lead_id, "peer_id": c.peer_id, **_peer(lead),
            "account": c.account_id, "state": c.state, "sent_count": c.sent_count,
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

    lead = (await db.execute(
        select(Lead).where(Lead.id == conv.lead_id))).scalar_one_or_none()

    return {
        "conversation": {
            "id": conv.id, "lead_id": conv.lead_id, "peer_id": conv.peer_id,
            **_peer(lead),
            "account": conv.account_id, "state": conv.state,
            "sent_count": conv.sent_count,
            "last_sent_at": _iso(conv.last_sent_at),
            "last_inbound_at": _iso(conv.last_inbound_at),
            "waiting_since": _iso(conv.waiting_since),
            "handed_off_at": _iso(conv.handed_off_at),
            "read_at": _iso(conv.read_at), "unread": conv.unread,
        },
        "events": [{"id": e.id, "kind": e.kind, "payload": e.payload,
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
