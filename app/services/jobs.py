"""Исполнитель задач: длинные операции с прогрессом, отменой и историей.

До него бэкфилл жил «где-то в вебхуках», а переклассификация запускалась руками через
`docker exec`. Снаружи не было видно ни что идёт, ни сколько осталось, ни чем
кончилось, а оператор не мог запустить ни то, ни другое вовсе.

Почему в процессе API, а не отдельный воркер с брокером. Задачи редкие (несколько
в день), минутные, и все упираются во внешние ожидания — модель, Telegram, — а не в
процессор. Отдельная инфраструктура здесь стоила бы дороже, чем даёт: ещё один
контейнер, ещё одно место, где всё встало, и ещё один способ разойтись с базой.

Источник истины — строка в `runs`, а не переменная в памяти. Отсюда два свойства,
которых иначе не получить:

* **отмена работает между процессами.** Кнопка ставит флаг в базе, исполнитель
  проверяет его между пачками. Не нужно ни сигналов, ни общей памяти.
* **перезапуск контейнера виден.** Процесс, умерший вместе с контейнером, оставил бы
  строку навсегда висеть в «выполняется» с прогрессом 43%. На старте такие строки
  помечаются прерванными — честно и один раз, вместо вечного вранья на экране.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable

from sqlalchemy import select, update

from app.core import clock
from app.db.models import Run
from app.db.session import get_session_maker
from app.services import alerts, embeddings, llm, reclassify

logger = logging.getLogger("radar.jobs")

# Сколько строк лога держим у задачи. Лог нужен, чтобы понять, на чём она стоит и
# чем кончилась, а не чтобы хранить всю историю: полный вывод живёт в логах контейнера.
LOG_LIMIT = 200

ACTIVE = ("queued", "running")

# Виды задач и права, которые для них нужны. Список закрытый: `kind` приходит из
# браузера, и запускать по нему произвольную функцию нельзя.
KINDS = ("reclassify", "backfill", "export")


class JobBusy(Exception):
    """Задача этого вида уже идёт. Две сразу поссорятся за один ресурс."""


class JobUnknown(Exception):
    """Неизвестный вид задачи."""


# ── ведение строки в базе ─────────────────────────────────────────────────────

async def _touch(run_id: int, **fields) -> None:
    """Записать изменения задачи отдельной короткой сессией.

    Своя сессия на каждое обновление, а не общая на весь прогон: прогресс обязан
    быть виден снаружи по ходу дела, а внутри длинной транзакции его никто не
    увидит до самого коммита.
    """
    async with get_session_maker()() as db:
        await db.execute(update(Run).where(Run.id == run_id).values(**fields))
        await db.commit()


async def _append_log(run_id: int, line: str) -> None:
    async with get_session_maker()() as db:
        run = (await db.execute(select(Run).where(Run.id == run_id))).scalar_one_or_none()
        if run is None:
            return
        lines = list(run.log or [])
        lines.append(f"{clock.utcnow():%H:%M:%S} {line}")
        run.log = lines[-LOG_LIMIT:]
        await db.commit()


async def _cancel_requested(run_id: int) -> bool:
    async with get_session_maker()() as db:
        return bool((await db.execute(
            select(Run.cancel_requested).where(Run.id == run_id))).scalar_one_or_none())


# ── сами задачи ───────────────────────────────────────────────────────────────

async def _job_reclassify(run_id: int, params: dict) -> dict:
    """Прогнать каскад заново. Ступени можно выключать по одной: без модели прогон
    идёт минуты вместо часа, и это нормальный режим, когда правится только L1."""
    scope = params.get("scope") or "pending"
    l2 = embeddings.enabled() and params.get("l2", True)
    l3 = llm.enabled() and params.get("l3", True)
    limit = params.get("l3_limit")

    last = {"pct": -1.0}

    async def report(pct, note):
        # Пишем в базу не чаще, чем меняется целый процент: прогресс на 12 тысячах
        # сообщений дёргается сотни раз, и каждый раз ходить в базу незачем.
        if pct is not None:
            if pct - last["pct"] < 1 and pct < 100:
                return
            last["pct"] = pct
            await _touch(run_id, progress=round(pct, 2))
        await _append_log(run_id, note)

    # Отмену спрашиваем у базы, но не на каждой итерации: кнопку нажимают раз, а
    # проверок были бы тысячи. Раз в три секунды достаточно, чтобы человек не ждал.
    seen = {"at": 0.0, "value": False}

    def cancelled() -> bool:
        now = asyncio.get_running_loop().time()
        if now - seen["at"] > 3:
            seen["at"] = now
            seen["value"] = _CANCEL_CACHE.get(run_id, False)
        return seen["value"]

    async def poll_cancel():
        while True:
            await asyncio.sleep(3)
            _CANCEL_CACHE[run_id] = await _cancel_requested(run_id)

    poller = asyncio.create_task(poll_cancel())
    try:
        async with get_session_maker()() as db:
            return await reclassify.run(db, l2_enabled=l2, l3_enabled=l3,
                                        l3_limit=limit, scope=scope,
                                        report=report, cancelled=cancelled)
    finally:
        poller.cancel()
        _CANCEL_CACHE.pop(run_id, None)


# Кеш флага отмены: опрашивается фоновой корутиной, читается синхронной проверкой
# внутри прогона. Синхронной — потому что она вызывается из глубины цикла, где
# await ради одного булева значения только мешал бы.
_CANCEL_CACHE: dict[int, bool] = {}

RUNNERS: dict[str, Callable[[int, dict], Awaitable[dict]]] = {
    "reclassify": _job_reclassify,
}


# ── запуск и жизненный цикл ───────────────────────────────────────────────────

_TASKS: dict[int, asyncio.Task] = {}


async def active_run(db, kind: str) -> Run | None:
    return (await db.execute(
        select(Run).where(Run.kind == kind, Run.status.in_(ACTIVE))
        .limit(1))).scalar_one_or_none()


async def start(db, *, kind: str, params: dict, name: str, user_email: str) -> Run:
    """Завести задачу и запустить её.

    Одна задача одного вида за раз. Два бэкфилла поссорятся за аккаунты и лимиты
    чтения, две переклассификации — за видеокарту, на которой к тому же живут
    ступени работающего каскада.
    """
    if kind not in RUNNERS:
        raise JobUnknown(f"вид задачи «{kind}» неизвестен, доступны: "
                         f"{', '.join(sorted(RUNNERS))}")
    busy = await active_run(db, kind)
    if busy is not None:
        raise JobBusy(f"задача «{kind}» уже идёт (#{busy.id}, {busy.progress}%)")

    run = Run(name=name, kind=kind, params=params, status="queued", progress=0,
              created_by=user_email, log=[])
    db.add(run)
    await db.commit()
    await db.refresh(run)

    _TASKS[run.id] = asyncio.create_task(_execute(run.id, kind, params))
    logger.info("job_started run=%s kind=%s by=%s", run.id, kind, user_email)
    return run


async def _execute(run_id: int, kind: str, params: dict) -> None:
    await _touch(run_id, status="running", started_at=clock.utcnow())
    try:
        result = await RUNNERS[kind](run_id, params)
        cancelled = bool(result.get("cancelled"))
        await _touch(run_id, status="cancelled" if cancelled else "done",
                     progress=100 if not cancelled else None,
                     result=result, finished_at=clock.utcnow())
        await _append_log(run_id, "остановлено оператором" if cancelled else "готово")
        logger.info("job_finished run=%s kind=%s cancelled=%s", run_id, kind, cancelled)
    except Exception as e:  # noqa: BLE001 — падение задачи не должно ронять сервис
        logger.exception("job_failed run=%s kind=%s", run_id, kind)
        await _touch(run_id, status="failed", error=f"{type(e).__name__}: {e}",
                     finished_at=clock.utcnow())
        await _append_log(run_id, f"упало: {type(e).__name__}: {e}")
        # Упавшая задача обязана попасть в ленту: строка в списке прогонов видна
        # только тому, кто в него зашёл, а пересчёт запускают и уходят.
        async with get_session_maker()() as db:
            await alerts.emit(db, key=f"run_failed:{kind}", severity="error",
                              text=f"Задача «{kind}» (#{run_id}) упала: "
                                   f"{type(e).__name__}: {e}")
            await db.commit()
    finally:
        _TASKS.pop(run_id, None)


async def request_cancel(db, run: Run) -> None:
    """Попросить задачу остановиться. Флаг в базе, а не отмена корутины: прогон
    обязан дописать посчитанное, а не оборваться посреди транзакции."""
    run.cancel_requested = True
    await db.commit()
    _CANCEL_CACHE[run.id] = True


async def mark_interrupted() -> int:
    """На старте пометить задачи, пережившие смерть процесса.

    Иначе строка остаётся в «выполняется» навсегда, и экран показывает прогресс,
    который уже никогда не сдвинется.

    Помечаются только задачи, которые крутились **в этом процессе**. Бэкфилл живёт
    иначе: его двигает Engage своими вебхуками, и перезапуск контейнера цепочку не
    обрывает — следующая страница приедет и продолжит. Пометить его прерванным
    значило бы соврать ровно наоборот.
    """
    async with get_session_maker()() as db:
        rows = (await db.execute(
            select(Run).where(Run.status.in_(ACTIVE),
                              Run.kind.in_(list(RUNNERS))))).scalars().all()
        for run in rows:
            run.status = "interrupted"
            run.error = "процесс перезапущен во время выполнения"
            run.finished_at = clock.utcnow()
        if rows:
            await db.commit()
        return len(rows)


# ── задачи, которые движет не наш процесс ─────────────────────────────────────

async def create_external(db, *, kind: str, params: dict, name: str,
                          user_email: str) -> Run:
    """Завести задачу, у которой нет исполнителя внутри API.

    Так устроен бэкфилл: страницы просит Engage, а мы только узнаём о них из
    вебхуков. Строка в `runs` всё равно нужна — иначе снаружи не видно ни что
    идёт, ни сколько набрано, ни чем кончилось.
    """
    busy = await active_run(db, kind)
    if busy is not None:
        raise JobBusy(f"задача «{kind}» уже идёт (#{busy.id})")
    run = Run(name=name, kind=kind, params=params, status="running", progress=0,
              created_by=user_email, log=[], started_at=clock.utcnow())
    db.add(run)
    await db.commit()
    await db.refresh(run)
    return run


async def progress(run_id: int, pct: float | None, note: str) -> None:
    """Отметить продвижение задачи, которую двигают снаружи."""
    if pct is not None:
        await _touch(run_id, progress=round(min(max(pct, 0), 100), 2))
    await _append_log(run_id, note)


async def finish(run_id: int, *, status: str, result: dict | None = None,
                 error: str | None = None, note: str | None = None) -> None:
    await _touch(run_id, status=status, result=result, error=error,
                 finished_at=clock.utcnow(),
                 **({"progress": 100} if status == "done" else {}))
    if note:
        await _append_log(run_id, note)
