"""Исполнитель задач: длинные операции с прогрессом, отменой и историей.

До него бэкфилл жил «где-то в вебхуках», а переклассификация запускалась руками через
`docker exec`. Снаружи не было видно ни что идёт, ни сколько осталось, ни чем
кончилось, а оператор не мог запустить ни то, ни другое вовсе.

**Где живёт исполнение — решает адрес Redis.** С заданным `RADAR_REDIS_URL` прогон
уезжает в отдельный процесс (`app/workers/jobs.py`), с пустым — остаётся
`asyncio.create_task` внутри API. Второе не «деградация»: на стенде и в тестах Redis не
нужен и не должен стать нужен. Обе ветки зовут один и тот же `execute` — разойдись они,
разница вылезла бы ровно там, где второй ветки нет, то есть на проде.

Прежний довод в пользу задач внутри API («редкие, минутные, упираются во внешние
ожидания, отдельная инфраструктура дороже, чем даёт») не пережил
`reclassify --scope all`: он идёт до часа, всё это время делит event loop с ручками
интерфейса и умирает вместе с процессом API при каждой выкатке.

Источник истины — строка в `runs`, а не переменная в памяти. Отсюда два свойства,
которых иначе не получить:

* **отмена работает между процессами.** Кнопка ставит флаг в базе, исполнитель
  проверяет его между пачками. Не нужно ни сигналов, ни общей памяти — и именно
  поэтому переезд прогона в воркер отмену не сломал.
* **перезапуск исполнителя виден.** Процесс, умерший вместе с контейнером, оставил бы
  строку навсегда висеть в «выполняется» с прогрессом 43%. На старте такие строки
  помечаются прерванными — честно и один раз, вместо вечного вранья на экране. Метит
  их тот, кто их и исполняет: с включённой очередью рестарт API работу воркера не
  обрывает, и пометка от API была бы враньём наоборот — см. `mark_interrupted`.
"""
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Awaitable, Callable

from sqlalchemy import select, update

from app.core import clock
from app.db.models import Run
from app.db.session import get_session_maker
from app.services import (alerts, discussions, embeddings, engage, llm, queue,
                          reclassify)

logger = logging.getLogger("radar.jobs")

# Сколько строк лога держим у задачи. Лог нужен, чтобы понять, на чём она стоит и
# чем кончилась, а не чтобы хранить всю историю: полный вывод живёт в логах контейнера.
LOG_LIMIT = 200

ACTIVE = ("queued", "running")

# Виды задач и права, которые для них нужны. Список закрытый: `kind` приходит из
# браузера, и запускать по нему произвольную функцию нельзя.
KINDS = ("reclassify", "backfill", "channel_add", "export", "discussions")


class JobBusy(Exception):
    """Задача этого вида уже идёт. Две сразу поссорятся за один ресурс."""


class JobUnknown(Exception):
    """Неизвестный вид задачи."""


class JobQueueDown(Exception):
    """Очередь включена и не приняла работу. Строка заведена и помечена упавшей.

    Отдельный вид отказа, а не `JobBusy` и не общее «упало»: занятость проходит сама,
    а очередь, которая не отвечает, требует человека — и ответ ручке нужен другой.
    """


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

@asynccontextmanager
async def _tracked(run_id: int):
    """Дать прогону два инструмента: чем отчитываться и чем спрашивать про отмену.

    Оба нужны каждому длинному прогону и оба неочевидны: прогресс нельзя писать
    на каждой итерации, а флаг отмены нельзя читать из базы синхронно из глубины
    цикла. Держать это в каждом прогоне отдельно значило бы, что второй по счёту
    получит их немного другими.
    """
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
        yield report, cancelled
    finally:
        poller.cancel()
        _CANCEL_CACHE.pop(run_id, None)


async def _job_reclassify(run_id: int, params: dict) -> dict:
    """Прогнать каскад заново. Ступени можно выключать по одной: без модели прогон
    идёт минуты вместо часа, и это нормальный режим, когда правится только L1."""
    scope = params.get("scope") or "pending"
    l2 = embeddings.enabled() and params.get("l2", True)
    l3 = llm.enabled() and params.get("l3", True)
    limit = params.get("l3_limit")

    async with _tracked(run_id) as (report, cancelled):
        async with get_session_maker()() as db:
            return await reclassify.run(db, l2_enabled=l2, l3_enabled=l3,
                                        l3_limit=limit, scope=scope,
                                        report=report, cancelled=cancelled)


async def _job_discussions(run_id: int, params: dict) -> dict:
    """Разобрать группы обсуждения списком (FIXES.md #3).

    Список каналов и аккаунты раскрываются здесь, а не в ручке: между нажатием
    кнопки и началом работы прогон может простоять в очереди, и «все, у кого не
    прочитана группа» обязано считаться на момент старта, а не на момент клика.
    """
    async with _tracked(run_id) as (report, cancelled):
        async with get_session_maker()() as db:
            channel_ids = await discussions.select_channels(
                db, scope=params.get("scope") or "unread",
                channel_ids=params.get("channel_ids"))

        accounts = list(params.get("account_ids") or [])
        if not accounts:
            # Аккаунты берём у Engage, а не из настроек: флот меняется, и список,
            # записанный в параметры задачи месяц назад, увёл бы чтение на
            # заблокированный аккаунт.
            accounts = [a["account_id"] for a in await engage.list_accounts()
                        if a.get("status") == "active"]
        if not accounts:
            raise RuntimeError("во флоте Engage нет активных аккаунтов")

        await report(0, f"каналов к разбору {len(channel_ids)}, "
                        f"аккаунтов {len(accounts)}")
        return await discussions.scan(
            channel_ids=channel_ids, account_ids=accounts,
            target=int(params.get("target") or discussions.PAGE_LIMIT),
            report=report, cancelled=cancelled)


# Кеш флага отмены: опрашивается фоновой корутиной, читается синхронной проверкой
# внутри прогона. Синхронной — потому что она вызывается из глубины цикла, где
# await ради одного булева значения только мешал бы.
_CANCEL_CACHE: dict[int, bool] = {}

RUNNERS: dict[str, Callable[[int, dict], Awaitable[dict]]] = {
    "reclassify": _job_reclassify,
    "discussions": _job_discussions,
}


# ── запуск и жизненный цикл ───────────────────────────────────────────────────

# Ссылки на корутины прогонов, идущих в этом процессе. Словарь непустой только при
# выключенной очереди: с включённой прогон живёт в воркере, и держать здесь было бы
# нечего. Нужен он ровно за тем, за чем и заведён, — чтобы задачу не собрал сборщик
# мусора, пока на неё никто не смотрит.
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

    Куда уедет исполнение, решает адрес Redis: есть — в воркер прогонов, нет — в
    корутину этого процесса. Строка в `runs` заводится до развилки и одинаково, чтобы
    ответ ручке не зависел от того, включена ступень или нет.
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

    if queue.enabled():
        try:
            await queue.enqueue(queue.RUN_JOB, run.id, kind, params)
        except queue.QueueUnavailable as e:
            # Строка уже закоммичена, а везти работу некуда. Оставить её в «в очереди»
            # значило бы вечно показывать на экране «сейчас начнётся»: начать её больше
            # некому. Второй попытки постановки здесь нет намеренно — занять видеокарту
            # на час решает человек, а не цикл повторов, и решение это он примет
            # осознаннее, увидев отказ сразу.
            await _touch(run.id, status="failed", error=f"очередь недоступна: {e}",
                         finished_at=clock.utcnow())
            await _append_log(run.id, f"не поставлено в очередь: {e}")
            # Тревога — потому что причина не в задаче: прогоны не запускаются вообще,
            # и узнать об этом иначе можно только попробовав ещё раз.
            await alerts.emit(
                db, key="runs_queue_down", severity="error",
                text=f"Очередь прогонов не отвечает, задача «{kind}» (#{run.id}) "
                     f"не запущена: {e}")
            await db.commit()
            logger.error("job_enqueue_failed run=%s kind=%s error=%s", run.id, kind, e)
            raise JobQueueDown(f"очередь прогонов недоступна: {e}") from e
        logger.info("job_queued run=%s kind=%s by=%s", run.id, kind, user_email)
        return run

    _TASKS[run.id] = asyncio.create_task(execute(run.id, kind, params))
    logger.info("job_started run=%s kind=%s by=%s", run.id, kind, user_email)
    return run


async def execute(run_id: int, kind: str, params: dict) -> None:
    """Провести задачу от «выполняется» до терминального статуса.

    Зовётся из `start` (когда очереди нет) и из воркера прогонов (когда есть) — один
    и тот же код на обе ветки. Наружу не бросает ничего, кроме `CancelledError`:
    падение прогона — это статус строки и тревога, а не исключение, которое кто-то
    выше по стеку обязан ловить.
    """
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
    обязан дописать посчитанное, а не оборваться посреди транзакции.

    Флаг в базе — единственный путь, который работает всегда: прогон опрашивает её и
    тогда, когда идёт в другом процессе. Запись в `_CANCEL_CACHE` — короткий путь
    поверх этого, и он имеет смысл только без очереди: там прогон живёт в этом же
    процессе и увидит отмену сразу, не дожидаясь очередного опроса, до трёх секунд.

    С включённой очередью записи в кэше не будет, и это не экономия строчки. Смотреть
    в него здесь некому — прогон идёт в воркере, — а вычистить запись некому тем
    более: убирает её сам прогон в ветке `finally` (`_job_reclassify`), то есть в
    другом процессе. Словарь копил бы по записи на каждую отмену до рестарта API.
    """
    run.cancel_requested = True
    await db.commit()
    if not queue.enabled():
        _CANCEL_CACHE[run.id] = True


async def mark_interrupted(statuses: tuple[str, ...] = ACTIVE) -> int:
    """На старте пометить задачи, пережившие смерть процесса-исполнителя.

    Иначе строка остаётся в «выполняется» навсегда, и экран показывает прогресс,
    который уже никогда не сдвинется.

    **Зовёт это тот, кто исполняет.** Без очереди — API на старте (`app/main.py`):
    прогон жил его корутиной и умер вместе с ним. С очередью — воркер прогонов
    (`app/workers/jobs.py`), и только он: рестарт API работу воркера не обрывает, и
    пометка оттуда была бы прямой ложью — вдобавок к вранью на экране она освободила
    бы `active_run`, и оператор запустил бы вторую переклассификацию поверх идущей.
    Верно это ровно пока воркер прогонов один; его единственность закреплена
    `container_name` в `docker-compose.yml`.

    `statuses` сужается воркером до одного «выполняется». Строка в «в очереди» при
    включённой очереди смерти процесса не пережила — она лежит в Redis (`appendonly`)
    и ждёт, когда её возьмут; пометить её прерванной значило бы соврать за миг до
    того, как работа начнётся.

    Помечаются только виды, у которых исполнитель у нас вообще есть. Бэкфилл живёт
    иначе: его двигает Engage своими вебхуками, и перезапуск контейнера цепочку не
    обрывает — следующая страница приедет и продолжит. Пометить его прерванным
    значило бы соврать ровно наоборот.
    """
    async with get_session_maker()() as db:
        rows = (await db.execute(
            select(Run).where(Run.status.in_(statuses),
                              Run.kind.in_(list(RUNNERS))))).scalars().all()
        for run in rows:
            run.status = "interrupted"
            run.error = "процесс-исполнитель перезапущен во время выполнения"
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
