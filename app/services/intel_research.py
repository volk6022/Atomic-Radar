"""Прогон пачки ресёрчей через Intel (SPEC-intel-screens §5, контракт `_TASK-intel-screens.md` §3).

Одна пачка = N одиночных задач Intel: пакетной постановки у него нет, поэтому
прогон сам держит не больше `concurrency` задач в полёте, сам опрашивает статусы
(первый раз через 90 с, дальше раз в 45 с — чаще Intel всё равно отдаёт кэш) и
сам решает, что «ждёт модель» дольше часа — это `stalled`, а 404 по своему
`task_id` — потерянная задача. Состояние опроса живёт в памяти прогона; в базе —
только статусы строк, чтобы после перезапуска процесса опрос продолжился по
`intel_task_id`, а не ставил те же запросы второй раз.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime

from sqlalchemy import func, select

from app.core import clock
from app.db.models import IntelKey, ResearchBatch, ResearchItem
from app.db.session import get_session_maker
from app.services import intel_client

logger = logging.getLogger("radar.intel_research")

FIRST_POLL_AFTER = 90.0
POLL_EVERY = 45.0
WAITING_LLM_LIMIT = 60 * 60.0
RETRY_AFTER_CAP = 120
TICK = 3.0

IN_FLIGHT = ("submitted", "running", "waiting_llm")
TERMINAL = ("completed", "failed", "stalled", "cancelled")


def _critic(result: dict | None) -> float | None:
    critic = (result or {}).get("critic")
    if isinstance(critic, dict):
        critic = critic.get("score")
    try:
        return float(critic) if critic is not None else None
    except (TypeError, ValueError):
        return None


async def _counts(db, batch_id: int) -> dict:
    rows = (await db.execute(
        select(ResearchItem.status, func.count())
        .where(ResearchItem.batch_id == batch_id).group_by(ResearchItem.status))).all()
    by = {s: n for s, n in rows}
    return {"done": by.get("completed", 0),
            "failed": by.get("failed", 0) + by.get("stalled", 0) + by.get("cancelled", 0),
            "in_flight": sum(by.get(s, 0) for s in IN_FLIGHT),
            "pending": by.get("pending", 0)}


async def _sync_batch(db, batch: ResearchBatch) -> dict:
    c = await _counts(db, batch.id)
    batch.done, batch.failed = c["done"], c["failed"]
    await db.commit()
    return c


async def _finish_item(db, item: ResearchItem, *, status: str, **fields) -> None:
    item.status = status
    item.finished_at = clock.utcnow()
    for k, v in fields.items():
        setattr(item, k, v)
    await db.commit()


async def run_batch(run_id: int, *, batch_id: int, report, cancelled) -> dict:
    """Тело прогона `intel_research`; обвязка (`_tracked`) — в `jobs.py`.

    Отмена — мягкая: новые запросы не ставятся, уже поставленные дорабатываются
    (Intel их всё равно считает и спишет квоту — бросать результат незачем),
    остальные `pending` помечаются `cancelled`.
    """
    loop = asyncio.get_running_loop()
    async with get_session_maker()() as db:
        batch = await db.get(ResearchBatch, batch_id)
        if batch is None:
            raise RuntimeError(f"пачка {batch_id} не найдена")
        key = (await db.execute(
            select(IntelKey).where(IntelKey.key == "default"))).scalar_one_or_none()
        concurrency = max(1, key.concurrency if key is not None else 2)
        endpoint = await intel_client.endpoint(db)

        batch.status = "running"
        batch.started_at = batch.started_at or clock.utcnow()
        await db.commit()
        await report(0, f"пачка «{batch.name}»: {batch.total} строк, "
                        f"одновременно {concurrency}")

        # Опрос: id строки → (когда спрашивать, с какого момента «ждёт модель»).
        # После перезапуска строки в полёте подхватываются здесь же — по task_id.
        polls: dict[int, list[float | None]] = {}
        for item in (await db.execute(
                select(ResearchItem).where(ResearchItem.batch_id == batch_id,
                                           ResearchItem.status.in_(IN_FLIGHT),
                                           ResearchItem.intel_task_id.is_not(None))
        )).scalars().all():
            polls[item.id] = [loop.time() + POLL_EVERY, None]

        pct = 0.0
        stopping = False
        while True:
            if cancelled() and not stopping:
                stopping = True
                n = (await db.execute(
                    select(func.count()).select_from(ResearchItem)
                    .where(ResearchItem.batch_id == batch_id,
                           ResearchItem.status == "pending"))).scalar_one()
                for item in (await db.execute(
                        select(ResearchItem).where(ResearchItem.batch_id == batch_id,
                                                   ResearchItem.status == "pending")
                )).scalars().all():
                    await _finish_item(db, item, status="cancelled")
                await report(pct, f"отмена: {n} строк не ставились, {len(polls)} дорабатывают")

            # ── постановка ────────────────────────────────────────────────────
            if not stopping:
                free = concurrency - len(polls)
                if free > 0:
                    todo = (await db.execute(
                        select(ResearchItem)
                        .where(ResearchItem.batch_id == batch_id,
                               ResearchItem.status == "pending")
                        .order_by(ResearchItem.row_no).limit(free))).scalars().all()
                    for item in todo:
                        try:
                            task_id = await intel_client.run(
                                endpoint, query=item.query, mode=batch.mode,
                                output_schema=batch.schema_json, language=batch.language)
                        except intel_client.IntelRateLimited as e:
                            wait = min(max(int(e.retry_after or 1), 1), RETRY_AFTER_CAP)
                            await report(None, f"429 {e.scope}: ждём {wait} с")
                            await asyncio.sleep(wait)
                            break
                        except (intel_client.IntelForbidden,
                                intel_client.IntelNotConfigured) as e:
                            await report(None, f"Intel отказал: {e}")
                            batch.status = "failed"
                            batch.finished_at = clock.utcnow()
                            await _sync_batch(db, batch)
                            return {"batch_id": batch_id, "total": batch.total,
                                    "done": batch.done, "failed": batch.failed,
                                    "error": str(e)}
                        except intel_client.IntelUnavailable as e:
                            await report(None, f"Intel недоступен: {e} — пауза")
                            await asyncio.sleep(RETRY_AFTER_CAP)
                            break
                        item.status = "submitted"
                        item.intel_task_id = task_id
                        item.submitted_at = clock.utcnow()
                        await db.commit()
                        polls[item.id] = [loop.time() + FIRST_POLL_AFTER, None]
                        await report(None, f"строка {item.row_no} поставлена: {task_id}")

            # ── опрос ─────────────────────────────────────────────────────────
            now = loop.time()
            for item_id in [i for i, p in polls.items() if p[0] <= now]:
                item = await db.get(ResearchItem, item_id)
                if item is None or item.intel_task_id is None:
                    polls.pop(item_id, None)
                    continue
                try:
                    data = await intel_client.status(endpoint, item.intel_task_id)
                except intel_client.IntelNotFound:
                    await _finish_item(db, item, status="failed",
                                       error="задача потеряна Intel (404)")
                    polls.pop(item_id, None)
                    await report(None, f"строка {item.row_no}: задача потеряна Intel")
                    continue
                except intel_client.IntelRateLimited as e:
                    polls[item_id][0] = now + min(max(int(e.retry_after or 1), 1),
                                                  RETRY_AFTER_CAP)
                    continue
                except intel_client.IntelUnavailable:
                    polls[item_id][0] = now + POLL_EVERY
                    continue
                if key is not None:
                    key.last_ok_at = clock.utcnow()
                status = data.get("status")
                if status == "completed":
                    result = data.get("result") or {}
                    stats = result.get("stats") or {}
                    await _finish_item(
                        db, item, status="completed", result=result,
                        critic_score=_critic(result), tokens=stats.get("tokens"),
                        elapsed_seconds=stats.get("elapsed_seconds"), error=None)
                    polls.pop(item_id, None)
                elif status == "failed":
                    msg = (data.get("progress") or {}).get("message") or "Intel: failed"
                    await _finish_item(db, item, status="failed", error=str(msg)[:500])
                    polls.pop(item_id, None)
                elif status == "queued_waiting_llm":
                    if polls[item_id][1] is None:
                        polls[item_id][1] = now
                    if now - polls[item_id][1] > WAITING_LLM_LIMIT:
                        await _finish_item(db, item, status="stalled",
                                           error="ждёт модель дольше часа")
                        polls.pop(item_id, None)
                        await report(None, f"строка {item.row_no}: ждёт модель дольше часа — stalled")
                    else:
                        if item.status != "waiting_llm":
                            item.status = "waiting_llm"
                            await db.commit()
                        polls[item_id][0] = now + POLL_EVERY
                else:  # pending | running | что-то новое — просто ждём дальше
                    if item.status != "running":
                        item.status = "running"
                        await db.commit()
                    polls[item_id][0] = now + POLL_EVERY

            c = await _sync_batch(db, batch)
            finished = c["done"] + c["failed"]
            pct = 100.0 * finished / batch.total if batch.total else 100.0
            await report(pct, f"готово {c['done']}, ошибок {c['failed']}, в полёте "
                              f"{c['in_flight']}, ждут {c['pending']}")
            if not polls and (stopping or c["pending"] == 0):
                break
            await asyncio.sleep(TICK)

        c = await _counts(db, batch_id)
        if stopping:
            batch.status = "cancelled"
        elif batch.total and c["failed"] == batch.total:
            batch.status = "failed"
        else:
            batch.status = "done"
        batch.finished_at = clock.utcnow()
        await _sync_batch(db, batch)
        await report(100, f"пачка «{batch.name}»: {batch.status}, готово {batch.done}, "
                          f"ошибок {batch.failed}")
        return {"batch_id": batch_id, "total": batch.total, "done": batch.done,
                "failed": batch.failed, "status": batch.status}
