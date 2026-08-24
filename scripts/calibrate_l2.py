"""Подобрать порог отрыва для L2 по живым сообщениям, а не на глаз.

Порог `cascade.L2_MIN_MARGIN` — единственное число в ступени, и назначать его из
головы нельзя: у bge-m3 сжатая шкала косинусов, разрывы между классами живут в сотых,
и «похоже на 0.8» ничего не значит само по себе. Скрипт берёт сообщения, дошедшие до
L2, считает векторы и печатает три вещи:

* распределение отрывов — где вообще лежит масса решений;
* что отсеется и что пройдёт при каждом кандидате в пороги;
* по несколько живых примеров с каждой стороны границы — читать глазами обязательно,
  проценты не показывают, ЧТО именно теряется.

    docker exec api-radar python -m scripts.calibrate_l2 --sample 800

Ничего не пишет в базу. Решение о пороге принимает человек, посмотрев на примеры.
"""
from __future__ import annotations

import argparse
import asyncio
import logging

from sqlalchemy import select

from app.core import cascade
from app.db.models import Message
from app.db.session import get_session_maker
from app.services import embeddings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("calibrate")

CANDIDATES = (0.0, 0.005, 0.01, 0.02, 0.03, 0.05, 0.08, 0.12)


def _short(text: str | None, n: int = 90) -> str:
    body = " ".join((text or "").split())
    return body[:n] + ("…" if len(body) > n else "")


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=600,
                    help="сколько сообщений прогнать (0 — все, что дошли до L2)")
    ap.add_argument("--examples", type=int, default=6)
    args = ap.parse_args()

    async with get_session_maker()() as db:
        messages = (await db.execute(select(Message))).scalars().all()

    # Кандидаты на L2 — те же, что отберёт боевой проход: прошли L0 и L1 в режиме
    # полноты. Считать распределение по всем сообщениям подряд бессмысленно: порог
    # применяется не к ним.
    candidates = []
    for m in messages:
        v = cascade.classify(
            text=m.text, is_automatic_forward=m.is_automatic_forward,
            author_is_bot=m.author_is_bot, author_peer_id=m.author_peer_id,
            author_username=m.author_username, tg_date=m.tg_date, l2_enabled=True)
        if v["passed"] is None and v["level"] == 1:
            candidates.append(m)

    log.info("сообщений всего %s, дошло до L2 %s", len(messages), len(candidates))
    if args.sample and len(candidates) > args.sample:
        step = len(candidates) / args.sample
        candidates = [candidates[int(i * step)] for i in range(args.sample)]
        log.info("берём равномерную выборку %s", len(candidates))
    if not candidates:
        return

    protos = await embeddings.prototype_vectors()
    vectors = await embeddings.embed([(m.text or "")[:2000] for m in candidates])

    rows = []
    for m, vec in zip(candidates, vectors):
        ranked = embeddings.rank(vec, protos)
        top_kind, top_label, top_sim = ranked[0]
        other = next(((k, l, s) for k, l, s in ranked if k != top_kind), None)
        margin = top_sim - other[2] if other else top_sim
        rows.append({"m": m, "kind": top_kind, "label": top_label,
                     "sim": top_sim, "margin": margin})

    pos = [r for r in rows if r["kind"] == "pos"]
    neg = [r for r in rows if r["kind"] == "neg"]
    log.info("ближе к боли: %s, ближе к шуму: %s", len(pos), len(neg))

    log.info("\nраспределение отрыва у тех, кто ближе к боли:")
    ordered = sorted(r["margin"] for r in pos)
    for q in (10, 25, 50, 75, 90):
        if ordered:
            log.info("  p%-3s %.4f", q, ordered[int(len(ordered) * q / 100) - 1])

    log.info("\nсколько пройдёт L2 при разных порогах (из %s кандидатов):", len(rows))
    for t in CANDIDATES:
        n = sum(1 for r in pos if r["margin"] >= t)
        mark = "  ← сейчас в коде" if abs(t - cascade.L2_MIN_MARGIN) < 1e-9 else ""
        log.info("  порог %.3f → %4s (%.1f%% кандидатов, %.2f%% всех сообщений)%s",
                 t, n, 100 * n / len(rows), 100 * n / len(messages), mark)

    log.info("\nсамые уверенные «это боль» (пройдут при любом пороге):")
    for r in sorted(pos, key=lambda r: -r["margin"])[:args.examples]:
        log.info("  +%.4f [%s] %s", r["margin"], r["label"], _short(r["m"].text))

    log.info("\nграница: ближе к боли, но отрыв мал (их и режет порог):")
    near = sorted((r for r in pos if r["margin"] < 0.05), key=lambda r: -r["margin"])
    for r in near[:args.examples]:
        log.info("  +%.4f [%s] %s", r["margin"], r["label"], _short(r["m"].text))

    log.info("\nотсеяно как шум (проверить, что там нет настоящих лидов):")
    for r in sorted(neg, key=lambda r: -r["margin"])[:args.examples]:
        log.info("  -%.4f [%s] %s", r["margin"], r["label"], _short(r["m"].text))

    await embeddings.close()


if __name__ == "__main__":
    asyncio.run(main())
