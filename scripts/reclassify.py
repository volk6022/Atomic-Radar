"""Переклассификация из консоли.

Ядро переехало в `app/services/reclassify.py`: тем же кодом пользуется задача,
запускаемая из интерфейса. Здесь остался разбор аргументов и вывод в лог — держать
две реализации одного прогона значило бы однажды получить разные результаты в
зависимости от того, откуда его запустили.

    docker exec api-radar python -m scripts.reclassify                 # всё, все ступени
    docker exec api-radar python -m scripts.reclassify --scope pending # только недосчитанное
    docker exec api-radar python -m scripts.reclassify --no-l3
    docker exec api-radar python -m scripts.reclassify --l3-limit 100
"""
from __future__ import annotations

import argparse
import asyncio
import logging

from app.db.session import get_session_maker
from app.services import embeddings, llm, reclassify

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("reclassify")


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-l2", action="store_true", help="не трогать эмбеддинги")
    ap.add_argument("--no-l3", action="store_true", help="не ходить в модель")
    ap.add_argument("--l3-limit", type=int, default=None,
                    help="задать модели не больше N вопросов за прогон; вопросов, а не "
                         "сообщений — у каждого сценария на L3 свой промпт, и при двух "
                         "контурах N вопросов покрывают вдвое меньше сообщений")
    ap.add_argument("--scope", choices=reclassify.SCOPES, default="all",
                    help="all — все сообщения, pending — только недосчитанные")
    args = ap.parse_args()

    l2_enabled = embeddings.enabled() and not args.no_l2
    l3_enabled = llm.enabled() and not args.no_l3
    log.info("охват: %s; ступени: L0/L1 всегда, L2 %s, L3 %s", args.scope,
             "вкл" if l2_enabled else "выкл", "вкл" if l3_enabled else "выкл")

    async def report(pct, note):
        log.info("%s%s", f"[{pct:.0f}%] " if pct is not None else "", note)

    async with get_session_maker()() as db:
        summary = await reclassify.run(
            db, l2_enabled=l2_enabled, l3_enabled=l3_enabled,
            l3_limit=args.l3_limit, scope=args.scope, report=report)

    await embeddings.close()
    await llm.close()
    log.info("итог: %s", summary)


if __name__ == "__main__":
    asyncio.run(main())
