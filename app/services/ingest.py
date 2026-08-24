"""Приём сообщений из Engage и превращение их в лиды.

Сюда сходятся два пути, и они обязаны давать одинаковый результат:

* **бэкфилл** — `get_chat_history` по группе обсуждения, приходит пачкой в вебхуке
  `task_complete`;
* **реалтайм** — вотчер шлёт `incoming_message` на каждое новое сообщение.

Пути неизбежно пересекаются (бэкфилл догоняет то, что вотчер уже принял), поэтому
запись идемпотентна по паре `(channel_id, tg_message_id)`: повторный приём обновляет
строку, а не плодит дубли. Без этого одно и то же сообщение стало бы двумя лидами,
и человеку в очереди пришлось бы писать одному и тому же человеку дважды.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.core import cascade
from app.db.models import Channel, Lead, Message
from app.services import embeddings, llm

logger = logging.getLogger(__name__)


def parse_dt(raw) -> datetime | None:
    if not raw:
        return None
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


async def get_or_create_channel(db, *, peer_id: int, username: str | None,
                                title: str | None) -> Channel:
    """Канал заводится сам при первом же сообщении из него.

    Требовать предварительной регистрации значило бы терять сообщения из групп,
    про которые оператор ещё не знает, — а именно они и интересны.
    """
    channel = (await db.execute(
        select(Channel).where(Channel.peer_id == peer_id))).scalar_one_or_none()
    if channel is not None:
        # Название группы меняется; храним последнее известное.
        if title and channel.title != title:
            channel.title = title
        if username and channel.username != username:
            channel.username = username
        return channel

    channel = Channel(peer_id=peer_id, username=username,
                      title=title or (username and "@" + username) or str(peer_id),
                      ingest_enabled=True)
    db.add(channel)
    await db.flush()
    logger.info("channel_created peer=%s username=%s title=%s", peer_id, username, title)
    return channel


async def upsert_message(db, *, channel: Channel, tg_message_id: int,
                         tg_date: datetime, text: str | None,
                         author_peer_id: int | None, author_username: str | None,
                         author_name: str | None, author_is_bot: bool,
                         is_automatic_forward: bool, reply_to_message_id: int | None,
                         thread_id: int | None, now: datetime | None = None) -> tuple[Message, bool]:
    """Записать сообщение и прогнать его по дешёвым ступеням каскада.

    На приёме считаются только L0 и L1: они стоят микросекунды. L2 требует похода к
    эмбеддеру, L3 — к модели, и делать это по одному сообщению внутри вебхука значит
    держать Engage в ожидании ради работы, которая пачками идёт в разы быстрее.
    Поэтому сообщение ложится со статусом «ещё в пути» (`cascade_passed = NULL`), а
    `scripts/reclassify.py` дозабирает такие пачкой.

    Возвращает (сообщение, новое ли).
    """
    verdict = cascade.classify(
        text=text, is_automatic_forward=is_automatic_forward, author_is_bot=author_is_bot,
        author_peer_id=author_peer_id, author_username=author_username,
        tg_date=tg_date, now=now,
        l2_enabled=embeddings.enabled(), l3_enabled=llm.enabled(),
    )

    values = {
        "channel_id": channel.id, "tg_message_id": tg_message_id, "tg_date": tg_date,
        "author_peer_id": author_peer_id, "author_username": author_username,
        "author_name": author_name, "author_is_bot": author_is_bot,
        "is_automatic_forward": is_automatic_forward,
        "reply_to_message_id": reply_to_message_id, "thread_id": thread_id,
        "text": text,
        "cascade_level": verdict["level"], "cascade_passed": verdict["passed"],
        "cascade_detail": verdict["detail"],
        "processed_at": datetime.now(timezone.utc),
    }

    # ON CONFLICT, а не «сначала SELECT, потом INSERT»: бэкфилл и вотчер работают
    # параллельно и легко приносят одно сообщение одновременно.
    stmt = (pg_insert(Message).values(**values)
            .on_conflict_do_update(constraint="uq_message_tg",
                                   set_={k: values[k] for k in
                                         ("text", "cascade_level", "cascade_passed",
                                          "cascade_detail", "processed_at")})
            .returning(Message.id, Message.created_at, Message.processed_at))
    row = (await db.execute(stmt)).one()
    message = (await db.execute(select(Message).where(Message.id == row.id))).scalar_one()

    created = row.created_at is not None and abs(
        (row.processed_at - row.created_at).total_seconds()) < 2

    if verdict["passed"]:
        await _ensure_lead(db, channel=channel, message=message, verdict=verdict)

    return message, created


async def _ensure_lead(db, *, channel: Channel, message: Message, verdict: dict) -> Lead | None:
    """Создать лид по прошедшему сообщению — ровно один на сообщение.

    Повторный приём того же сообщения обновляет оценку, но не создаёт второй лид:
    иначе очередь черновиков наполнилась бы копиями одного и того же человека.
    """
    existing = (await db.execute(
        select(Lead).where(Lead.message_id == message.id))).scalar_one_or_none()
    if existing is not None:
        existing.score = verdict["score"]
        existing.score_breakdown = verdict["breakdown"]
        existing.disqualifiers = verdict["disqualifiers"]
        return existing

    lead = Lead(
        message_id=message.id, channel_id=channel.id,
        author_peer_id=message.author_peer_id,
        author_username=message.author_username, author_name=message.author_name,
        pain=verdict["pain"], quote=(message.text or "")[:500],
        score=verdict["score"], score_breakdown=verdict["breakdown"],
        disqualifiers=verdict["disqualifiers"], status="new",
    )
    db.add(lead)
    await db.flush()
    channel.leads_total = (channel.leads_total or 0) + 1
    logger.info("lead_created lead=%s channel=%s score=%s pain=%s",
                lead.id, channel.title, lead.score, lead.pain)
    return lead


# ── разбор конвертов Engage ───────────────────────────────────────────────────

async def ingest_incoming_message(db, payload: dict) -> dict:
    """Событие вотчера: одно новое сообщение."""
    chat_id = payload.get("chat_id")
    if chat_id is None:
        return {"accepted": 0, "reason": "нет chat_id"}

    channel = await get_or_create_channel(
        db, peer_id=chat_id, username=payload.get("chat_username"),
        title=payload.get("chat_title"))

    name = " ".join(x for x in (payload.get("from_first_name"),
                                payload.get("from_last_name")) if x) or None
    tg_date = parse_dt(payload.get("date")) or datetime.now(timezone.utc)

    _, created = await upsert_message(
        db, channel=channel, tg_message_id=payload.get("message_id"),
        tg_date=tg_date, text=payload.get("message"),
        author_peer_id=payload.get("from_peer_id"),
        author_username=payload.get("sender_username"),
        author_name=name, author_is_bot=bool(payload.get("from_is_bot")),
        is_automatic_forward=bool(payload.get("is_automatic_forward")),
        reply_to_message_id=payload.get("reply_to_message_id"),
        thread_id=payload.get("message_thread_id"),
    )
    return {"accepted": 1, "created": int(created)}


async def ingest_history(db, *, chat_id: int, chat_username: str | None,
                         chat_title: str | None, posts: list[dict]) -> dict:
    """Результат `get_chat_history`: пачка сообщений одной группы."""
    channel = await get_or_create_channel(
        db, peer_id=chat_id, username=chat_username, title=chat_title)

    accepted = 0
    for p in posts:
        tg_date = parse_dt(p.get("date"))
        if tg_date is None or p.get("message_id") is None:
            continue
        name = " ".join(x for x in (p.get("from_first_name"),
                                    p.get("from_last_name")) if x) or None
        await upsert_message(
            db, channel=channel, tg_message_id=p["message_id"], tg_date=tg_date,
            text=p.get("text"), author_peer_id=p.get("from_user_id"),
            author_username=p.get("from_username"), author_name=name,
            author_is_bot=bool(p.get("from_is_bot")),
            is_automatic_forward=bool(p.get("is_automatic_forward")),
            reply_to_message_id=p.get("reply_to_message_id"),
            thread_id=p.get("message_thread_id"),
        )
        accepted += 1

    # Курсор бэкфилла: с какого id продолжать листать назад.
    ids = [p["message_id"] for p in posts if p.get("message_id")]
    if ids:
        oldest = min(ids)
        if channel.backfill_cursor is None or oldest < channel.backfill_cursor:
            channel.backfill_cursor = oldest

    logger.info("history_ingested channel=%s accepted=%s", channel.title, accepted)
    return {"accepted": accepted, "channel_id": channel.id,
            "backfill_cursor": channel.backfill_cursor}
