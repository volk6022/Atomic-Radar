"""Управление каналом руками: снять с отслеживания, удалить накопленное.

FIXES.md #7 разводит эти два решения нарочно: «больше не читаем» и «сотри то, что
уже прочитали» — разные последствия и разная цена ошибки. Снятие отслеживания —
одна колонка (`ingest_enabled`, уже есть) и делается прямо в ручке. Удаление
сообщений — здесь, потому что оно тянет за собой пол-схемы: `messages` изнутри
проекта ссылками не защищены каскадом (`ON DELETE CASCADE` есть только у
`wf_verdicts`, остальное — вручную), и порядок удаления обязан быть посчитан один
раз и не разъезжаться с изменениями схемы по памяти того, кто пишет очередную ручку.
"""
from __future__ import annotations

import logging

from sqlalchemy import delete, select, update

from app.db.models import (Conversation, ConversationEvent, Draft, Lead, LlmTrace,
                           ManualSend, Message, OutboundAttempt, WfDraft, WfOutbound,
                           WfTarget)

logger = logging.getLogger("radar.channels")


async def purge_messages(db, *, channel_id: int) -> dict[str, int]:
    """Удалить все сообщения канала и всё, что от них зависит.

    Порядок — снизу вверх по графу внешних ключей: сначала журналы попыток
    отправки (`OutboundAttempt`/`WfOutbound`), затем черновики и события диалогов,
    затем сами диалоги и лиды/цели, и только в конце — сообщения. `wf_verdicts`
    отдельно не трогаем: у него `ON DELETE CASCADE` на `messages.id`, Postgres
    снимет эти строки сам при удалении сообщения.

    `ManualSend` — исключение. Это рассказ человека о том, что он отправил, а не
    производная от сообщения; удалять его вместе с кэшем каскада значило бы стереть
    факт живой переписки заодно с чисткой шума. Вместо удаления обнуляем ссылки на
    цель и сообщение (обе колонки nullable), а текст и снимок предложенного остаются.
    """
    lead_ids = (await db.execute(
        select(Lead.id).where(Lead.channel_id == channel_id))).scalars().all()
    target_ids = (await db.execute(
        select(WfTarget.id).where(WfTarget.channel_id == channel_id))).scalars().all()
    draft_ids = (await db.execute(
        select(Draft.id).where(Draft.lead_id.in_(lead_ids)))).scalars().all() \
        if lead_ids else []
    wf_draft_ids = (await db.execute(
        select(WfDraft.id).where(WfDraft.target_id.in_(target_ids)))).scalars().all() \
        if target_ids else []
    conversation_ids = (await db.execute(
        select(Conversation.id).where(Conversation.lead_id.in_(lead_ids)))
        ).scalars().all() if lead_ids else []

    counts: dict[str, int] = {}

    if draft_ids:
        r = await db.execute(delete(OutboundAttempt).where(
            OutboundAttempt.draft_id.in_(draft_ids)))
        counts["outbound_attempts"] = r.rowcount or 0
    if conversation_ids:
        r = await db.execute(delete(OutboundAttempt).where(
            OutboundAttempt.conversation_id.in_(conversation_ids)))
        counts["outbound_attempts"] = counts.get("outbound_attempts", 0) + (r.rowcount or 0)
        r = await db.execute(delete(ConversationEvent).where(
            ConversationEvent.conversation_id.in_(conversation_ids)))
        counts["conversation_events"] = r.rowcount or 0
    if target_ids:
        r = await db.execute(delete(WfOutbound).where(WfOutbound.target_id.in_(target_ids)))
        counts["wf_outbound"] = r.rowcount or 0

    # ManualSend хранится, но больше не показывает на что ссылался — цель и
    # сообщение исчезают, текст и признак «была наводка» остаются.
    r = await db.execute(update(ManualSend).where(ManualSend.message_id.in_(
        select(Message.id).where(Message.channel_id == channel_id)))
        .values(message_id=None))
    counts["manual_sends_detached"] = r.rowcount or 0
    if target_ids:
        r = await db.execute(update(ManualSend).where(ManualSend.target_id.in_(target_ids))
                             .values(target_id=None))
        counts["manual_sends_detached"] = counts.get("manual_sends_detached", 0) + (r.rowcount or 0)

    if lead_ids:
        r = await db.execute(delete(LlmTrace).where(LlmTrace.lead_id.in_(lead_ids)))
        counts["llm_traces"] = r.rowcount or 0
        r = await db.execute(delete(Draft).where(Draft.lead_id.in_(lead_ids)))
        counts["drafts"] = r.rowcount or 0
    if conversation_ids:
        r = await db.execute(delete(Conversation).where(Conversation.id.in_(conversation_ids)))
        counts["conversations"] = r.rowcount or 0
    if wf_draft_ids:
        r = await db.execute(delete(WfDraft).where(WfDraft.id.in_(wf_draft_ids)))
        counts["wf_drafts"] = r.rowcount or 0
    if target_ids:
        r = await db.execute(delete(WfTarget).where(WfTarget.id.in_(target_ids)))
        counts["wf_targets"] = r.rowcount or 0

    r = await db.execute(delete(Lead).where(Lead.channel_id == channel_id))
    counts["leads"] = r.rowcount or 0

    r = await db.execute(delete(Message).where(Message.channel_id == channel_id))
    counts["messages"] = r.rowcount or 0

    logger.info("channel_messages_purged channel=%s counts=%s", channel_id, counts)
    return counts
