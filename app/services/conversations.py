"""Нитки диалогов: заведение, события и факты «кому уже писали».

Диалог в Radar — пара «человек ↔ Radar», а не «человек ↔ аккаунт» (PLAN 16.11):
один человек — одна нитка на весь флот (`uq_conversation_peer` по `peer_id`),
аккаунт — атрибут первого касания и дальше не меняется. Сценарий — атрибут
события, а не нитки: переписка с человеком одна, а сценарии заведутся и закроются.

Писателей у `conversations` до 16.1 не было — первыми стали ручные отправки:
запись о том, что человек написал сам, заводит нитку и событие в ней
(`link_manual_send`). Автомат из черновиков (16.2), история Engage и нитки
«unsolicited» (16.3) придут позже и будут ходить через те же две функции —
`ensure_thread` и `add_event`, — чтобы свёртка состояния нитки жила в одном
месте, а не в каждом писателе отдельно.
"""
from __future__ import annotations

import logging
from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from app.core import clock
from app.db.models import Conversation, ConversationEvent, ManualSend, Message, WfTarget

logger = logging.getLogger(__name__)

# Откуда взялась нитка. CHECK в схеме нет намеренно: справочник живёт в коде,
# и новый источник не должен требовать миграции. NULL-аккаунт законен только
# у «unsolicited» — нитки, которую человек начал сам, без нашего первого касания.
CONVERSATION_SOURCES = ("draft", "manual", "engage_history", "unsolicited")

# Что случилось в переписке. Проверяется в `add_event`: чужое значение — ошибка
# программиста, а не данных, и молча пропустить её значило бы испортить свёртку
# состояния нитки тем же движением, которым пишут журнал.
EVENT_KINDS = ("outbound", "inbound", "manual", "note", "system")

# Состояния, которые человек ставит сам (16.5). События их не меняют: автомат
# не имеет права разрукаживать то, что разручивает оператор.
HUMAN_STATES = ("handed_off", "closed")


async def ensure_thread(db, *, peer_id: int, engage_account_id: int | None, source: str,
                        peer_username: str | None = None,
                        target_id: int | None = None) -> Conversation:
    """Нитка по `peer_id`: достать или завести. Повторный вызов ничего не перезаписывает.

    Аккаунт и цель — первое касание: повторный вызов с другим аккаунтом или целью
    возвращает ту же нитку, не переписывая ни поля, — переписка-то с тем же
    человеком. Единственное исключение — `peer_username`: при первом касании его
    могли не знать, и доехавший позже лучше пустоты.

    Про NULL в `engage_account_id`: у автомата его не бывает — писатели черновиков
    и истории Engage передают аккаунт всегда, пустой законен только у «unsolicited».
    Но `link_manual_send` передаёт сюда то, что помнит запись ручной отправки, а
    человек мог оставить поле аккаунта пустым (список недоступен — «запись факта
    от него не зависит»): запретить это значило бы терять факт отправки ради
    аккуратности справочника.

    Гонку двух писателей на одного человека ловит уникальность
    `uq_conversation_peer`: проигравший откатывается до savepoint (внешняя
    транзакция при этом жива — рядом могут ждать свои INSERT'ы) и перечитывает
    победителя.
    """
    if source not in CONVERSATION_SOURCES:
        raise ValueError(f"неизвестный источник нитки «{source}», ожидается один из "
                         f"{', '.join(CONVERSATION_SOURCES)}")

    conv = (await db.execute(
        select(Conversation).where(Conversation.peer_id == peer_id))).scalar_one_or_none()
    if conv is not None:
        if peer_username and not conv.peer_username:
            conv.peer_username = peer_username
        return conv

    try:
        async with db.begin_nested():
            conv = Conversation(peer_id=peer_id, engage_account_id=engage_account_id,
                                source=source, peer_username=peer_username,
                                target_id=target_id, state="new")
            db.add(conv)
            await db.flush()
    except IntegrityError:
        # Второй писатель успел раньше: savepoint откатился, outer-транзакция цела.
        conv = (await db.execute(
            select(Conversation).where(Conversation.peer_id == peer_id))).scalar_one_or_none()
        if conv is None:
            raise
    return conv


async def add_event(db, conv: Conversation, *, kind: str, source: str, actor: str | None,
                    at: datetime, text: str | None = None,
                    tg_message_id: int | None = None, workflow_id: int | None = None,
                    payload: dict | None = None) -> ConversationEvent:
    """Событие в нитке плюс свёртка состояния нитки из этого события.

    Свёртка — единственная на все писатели, потому и живёт здесь:

    * `outbound`/`manual` — счётчик и время последней отправки двигаются всегда;
      нитка ждёт ответа: `state="awaiting_reply"`, `waiting_since=at`. Это верно
      и после входящего («replied» → снова ждём), иначе повторное касание
      застревало бы в «человек ответил»;
    * `inbound` — время последнего входящего и `state="replied"`, ожидание снято;
    * `note`/`system` — пометки, а не переписка: счётчики и состояния не трогают;
    * `handed_off`/`closed` события не меняют вовсе — их ставит человек (16.5),
      и никакой автомат не должен выводить нитку из решения человека.
    """
    if kind not in EVENT_KINDS:
        raise ValueError(f"неизвестный вид события «{kind}», ожидается один из "
                         f"{', '.join(EVENT_KINDS)}")

    event = ConversationEvent(conversation_id=conv.id, kind=kind, source=source,
                              actor=actor, at=at, text=text, tg_message_id=tg_message_id,
                              workflow_id=workflow_id, payload=payload)
    db.add(event)

    if kind in ("outbound", "manual"):
        conv.sent_count += 1
        if conv.last_sent_at is None or at > conv.last_sent_at:
            conv.last_sent_at = at
        if conv.state not in HUMAN_STATES:
            conv.state = "awaiting_reply"
            conv.waiting_since = at
    elif kind == "inbound":
        if conv.last_inbound_at is None or at > conv.last_inbound_at:
            conv.last_inbound_at = at
        if conv.state not in HUMAN_STATES:
            conv.state = "replied"
            conv.waiting_since = None
    return event


async def contact_facts(db, *, peer_id: int) -> tuple[bool, int, datetime | None]:
    """`(previously_contacted, sent_count, last_sent_at)` по нитке человека.

    Источник для гардрейла «этому человеку уже писали» (16.2 подключит его в
    `check_all` вместо сегодняшних констант `False/0`). Нитки нет — человеку не
    писали, и это `(False, 0, None)`. Нитка без отправок (например, человек
    написал первым) — тоже «не писали»: контакт — это наше исходящее.
    """
    row = (await db.execute(
        select(Conversation.sent_count, Conversation.last_sent_at)
        .where(Conversation.peer_id == peer_id))).first()
    if row is None:
        return False, 0, None
    sent_count, last_sent_at = row
    return sent_count > 0, sent_count, last_sent_at


def _manual_send_time(entry: ManualSend) -> datetime:
    """Момент события для ручной отправки: сказал человек (`sent_at`) или момент
    записи (`recorded_at`). До первого refresh `recorded_at` с сервера может ещё
    не доехать — тогда берём часы процесса: расхождение с базой в пределах
    миллисекунды, а NOT NULL у `at` обязательный."""
    return entry.sent_at or entry.recorded_at or clock.utcnow()


async def link_manual_send(db, entry: ManualSend, *, actor: str) -> Conversation | None:
    """Завести нитку для ручной отправки и положить в неё событие. Идемпотентно.

    Адресат: у наводки-«user» — получатель ЛС; у записи без наводки — автор
    сообщения, если он известен. Публичная наводка (`target_kind="message"`)
    нитки не заводит: диалоги — только про ЛС (решение владельца, PLAN 16.11),
    публичные ответы в переписку не входят. Адресата нет — возвращаем `None`:
    «Андрей мог написать тому, кого Radar не находил», и терять сам факт из-за
    отсутствующего диалога нельзя.
    """
    if entry.conversation_id is not None:
        return await db.get(Conversation, entry.conversation_id)

    target = (await db.get(WfTarget, entry.target_id)
              if entry.target_id is not None else None)
    peer_id = peer_username = None
    if target is not None and target.target_kind == "user":
        peer_id, peer_username = target.recipient_peer_id, target.author_username
    elif target is None and entry.message_id is not None:
        message = await db.get(Message, entry.message_id)
        peer_id = message.author_peer_id if message is not None else None
        peer_username = message.author_username if message is not None else None

    if peer_id is None:
        return None

    conv = await ensure_thread(db, peer_id=peer_id,
                               engage_account_id=entry.engage_account_id,
                               source="manual", peer_username=peer_username,
                               target_id=entry.target_id)
    await add_event(db, conv, kind="manual", source=f"manual_send:{entry.id}",
                    actor=actor, at=_manual_send_time(entry), text=entry.text,
                    workflow_id=entry.workflow_id)
    entry.conversation_id = conv.id
    return conv


async def resync_manual_send_time(db, entry: ManualSend) -> None:
    """Правка `sent_at` у уже привязанной записи: событие и нитка обязаны
    говорить то же время, что и запись.

    `last_sent_at` пересчитывается заново — максимум `at` по событиям
    outbound/manual нитки: «умно» откатывать предыдущее значение значилось бы
    держать в голове порядок правок, а свёртка и так честна.
    """
    if entry.conversation_id is None:
        return
    event = (await db.execute(
        select(ConversationEvent)
        .where(ConversationEvent.conversation_id == entry.conversation_id,
               ConversationEvent.kind == "manual",
               ConversationEvent.source == f"manual_send:{entry.id}"))
    ).scalar_one_or_none()
    if event is None:
        # Запись осталась от версии без событий — двигать нечего.
        return
    at = _manual_send_time(entry)
    event.at = at

    conv = await db.get(Conversation, entry.conversation_id)
    if conv is None:
        return
    conv.last_sent_at = (await db.execute(
        select(func.max(ConversationEvent.at))
        .where(ConversationEvent.conversation_id == conv.id,
               ConversationEvent.kind.in_(("outbound", "manual"))))).scalar_one()


async def backfill_manual_sends(db) -> int:
    """Привязать к ниткам ручные отправки, записанные до 16.1.

    Идемпотентно: второй прогон находит только то, что не смогло привязаться
    в первый (записи без адресата), — и снова не привязывает: диалога для них
    придумать нельзя, а перебирать их на каждом старте дёшево. Возвращает число
    привязанных.
    """
    rows = (await db.execute(
        select(ManualSend).where(ManualSend.conversation_id.is_(None))
        .order_by(ManualSend.id))).scalars().all()
    linked = 0
    for entry in rows:
        if await link_manual_send(db, entry, actor="system:backfill") is not None:
            linked += 1
    if rows:
        logger.info("manual_sends_backfill scanned=%s linked=%s", len(rows), linked)
    return linked
