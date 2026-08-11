"""Прогнать сохранённые сообщения по каскаду заново.

Нужен каждый раз, когда меняются правила L0/L1: сообщения уже лежат в базе с прежним
вердиктом, и без переклассификации экран потока продолжит объяснять решения, которых
код больше не принимает.

Лиды, по которым человек уже принял решение, не трогаются — переписывать чужое
решение задним числом нельзя. Лиды в статусе `new`, переставшие проходить каскад,
удаляются: держать в очереди то, что система больше не считает лидом, значит тратить
время оператора на заведомый мусор.

    docker exec api-radar python -m scripts.reclassify
"""
from __future__ import annotations

import asyncio
import logging

from sqlalchemy import select

from app.core import cascade
from app.db.models import Channel, Lead, Message
from app.db.session import get_session_maker

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("reclassify")


async def main() -> None:
    async with get_session_maker()() as db:
        messages = (await db.execute(select(Message))).scalars().all()
        titles = {c.id: c.title for c in
                  (await db.execute(select(Channel))).scalars().all()}
        leads = {l.message_id: l for l in
                 (await db.execute(select(Lead))).scalars().all()}

        changed = created = removed = kept = 0

        for m in messages:
            v = cascade.classify(
                text=m.text, is_automatic_forward=m.is_automatic_forward,
                author_is_bot=m.author_is_bot, author_peer_id=m.author_peer_id,
                author_username=m.author_username, tg_date=m.tg_date)

            was = m.cascade_passed
            m.cascade_level, m.cascade_passed = v["level"], v["passed"]
            m.cascade_detail = v["detail"]
            if was != v["passed"]:
                changed += 1

            lead = leads.get(m.id)

            if v["passed"] and lead is None:
                db.add(Lead(
                    message_id=m.id, channel_id=m.channel_id,
                    author_peer_id=m.author_peer_id, author_username=m.author_username,
                    author_name=m.author_name, pain=v["pain"],
                    quote=(m.text or "")[:500], score=v["score"],
                    score_breakdown=v["breakdown"], disqualifiers=v["disqualifiers"],
                    status="new"))
                created += 1
            elif v["passed"] and lead is not None:
                lead.score, lead.score_breakdown = v["score"], v["breakdown"]
                lead.pain, lead.disqualifiers = v["pain"], v["disqualifiers"]
                kept += 1
            elif not v["passed"] and lead is not None:
                if lead.status == "new":
                    await db.delete(lead)
                    removed += 1
                else:
                    # По лиду уже работали. Оставляем как есть и говорим об этом вслух.
                    log.warning("лид %s больше не проходит каскад, но статус «%s» — "
                                "оставлен как есть", lead.id, lead.status)
                    kept += 1

        # Счётчик лидов в канале — производная величина, пересчитываем целиком.
        for channel_id, title in titles.items():
            total = len([m for m in messages
                         if m.channel_id == channel_id and m.cascade_passed])
            channel = (await db.execute(
                select(Channel).where(Channel.id == channel_id))).scalar_one()
            channel.leads_total = total

        await db.commit()

    log.info("сообщений %s · вердикт изменился у %s · лидов создано %s, удалено %s, "
             "обновлено %s", len(messages), changed, created, removed, kept)


if __name__ == "__main__":
    asyncio.run(main())
