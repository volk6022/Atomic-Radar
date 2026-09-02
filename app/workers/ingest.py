"""Воркер приёма: разбирает события Engage, снятые с очереди.

Запускается тем же образом, что и API, но другой командой:

    arq app.workers.ingest.WorkerSettings

**Почему приём — отдельный процесс.** Разбор внутри запроса держал соединение Engage
на всё время работы с базой, а любая тяжёлая работа в процессе API конкурирует за event
loop с ручками интерфейса. Сверх того, работа, живущая в памяти процесса API, умирает
вместе с ним; воркер этого не чинит сам по себе, но делает возможным всё остальное —
ретраи, изоляцию и видимость состояния из другого процесса.

**Своя сессия на задачу, а не одна на воркер.** Долгоживущая сессия SQLAlchemy копит
объекты и держит транзакцию открытой между задачами: одна неудачная задача тогда
отравляет следующую. Сессия открывается и закрывается вокруг каждого события.

**Ретраи.** `MAX_TRIES` больше единицы, потому что типичный сбой здесь — временный:
Postgres перезапускается, Engage не отвечает на дозапрос страницы. Разбор идемпотентен
(сообщения кладутся upsert-ом, вердикты — `ON CONFLICT`), поэтому повтор безопасен.
Исключение — ошибка в самом событии: `HTTPException` от `process_event` означает, что
Engage прислал негодный запрос, и повторять его бессмысленно. Такие не ретраятся.

Граница «повторять / не повторять» при этом проведена по типу исключения, а не по
природе ошибки, и это её слабое место: детерминированный сбой, который автор ветки не
завернул в `HTTPException` (скажем, `int()` от нечислового параметра в `webhook_url`),
трижды повторится впустую. Обходится это не угадыванием видов ошибок, а тем, что
**исчерпанные попытки перестают быть тихими** — см. ниже.

**Ни один отказ не остаётся молчаливым.** Это главное отличие от прежнего устройства и
причина, по которой воркер вообще пишет в базу. Раньше исключение долетало до Engage
кодом ответа, тот повторял пять раз и в конце концов сдавался — и это было видно хотя
бы в его журнале. Теперь Engage получил `202` и ушёл: если работа умрёт в очереди, о
ней не узнает никто. Поэтому и негодное событие, и исчерпанные попытки поднимают
тревогу оператору. Особенно это важно для бэкфилла: его строка в `runs` при обрыве
цепочки не помечается упавшей ничем — `mark_interrupted` намеренно не трогает
`kind="backfill"`, — и без тревоги прогон висел бы «выполняется» бесконечно.
"""
from __future__ import annotations

import logging

from fastapi import HTTPException

from app.api.v1.ingest import process_event
from app.core.config import get_settings
from app.db.session import get_engine, get_session_maker
from app.services import alerts, cascade_registry, engage, engage_registry, queue

# Уровень логов настраивается здесь, а не только в `app/main.py`: воркер поднимает
# arq напрямую по `WorkerSettings`, `app.main` он не импортирует никогда, и потому
# `basicConfig` оттуда до него не доходит. Корневой логгер оставался на WARNING, и
# НИ ОДНА info-строка воркера никуда не шла — ни «воркер запустился», ни перечитка
# таксономии, ни ход бэкфилла. Процесс при этом выглядел исправным: молчание нельзя
# отличить от «всё хорошо».
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")

logger = logging.getLogger(__name__)

# Сколько раз пробуем событие. Три, а не пять: временный сбой закрывается первым же
# повтором, а детерминированный не закроется никогда — держать его в очереди дольше
# значит откладывать тревогу, ради которой всё это и написано.
MAX_TRIES = 3


async def _alert(key: str, text: str) -> None:
    """Тревога отдельной сессией.

    Своей — потому что зовут её после `rollback`, из ветки, где транзакция задачи уже
    негодна. Писать тревогу в сломанную транзакцию значит потерять её вместе с той
    ошибкой, о которой она сообщает.
    """
    try:
        async with get_session_maker()() as db:
            await alerts.emit(db, key=key, text=text, severity="error")
            await db.commit()
    except Exception:  # noqa: BLE001 — тревога не вправе добить задачу
        logger.exception("ingest_alert_failed key=%s", key)


def _what(body: dict, q: dict) -> str:
    return f"событие {body.get('event')!r}, kind={q.get('kind')!r}"


async def ingest_event(ctx: dict, body: dict, q: dict) -> dict:
    """Разобрать одно событие Engage.

    Возвращает то же, что вернула бы ручка приёма, — arq сохранит это как результат
    работы, и по нему видно, что именно случилось с конкретным вебхуком.
    """
    maker = get_session_maker()
    async with maker() as db:
        try:
            return await process_event(db, body, q)
        except HTTPException as e:
            # Негодное событие. Повторять нечего: тот же вход даст тот же отказ.
            # Но и промолчать нельзя: Engage прислал то, чего мы не понимаем, и до
            # него эта новость уже не дойдёт — он получил `202` и ушёл.
            await db.rollback()
            logger.error("ingest_event_rejected status=%s detail=%s event=%s kind=%s",
                         e.status_code, e.detail, body.get("event"), q.get("kind"))
            await _alert("ingest_event_rejected",
                         f"Engage прислал событие, которое не удалось разобрать: "
                         f"{_what(body, q)} — {e.detail}")
            return {"accepted": 0, "rejected": e.detail}
        except Exception as e:
            await db.rollback()
            logger.exception("ingest_event_failed event=%s kind=%s",
                             body.get("event"), q.get("kind"))
            if ctx.get("job_try", 1) >= MAX_TRIES:
                # Последняя попытка. Дальше работа просто исчезнет из очереди, и это
                # единственный момент, когда о ней можно сказать вслух.
                await _alert("ingest_event_failed",
                             f"Событие Engage не разобрано после {MAX_TRIES} попыток: "
                             f"{_what(body, q)} — {type(e).__name__}: {e}. "
                             f"Пропущенное доберётся бэкфиллом; если это была цепочка "
                             f"бэкфилла — её прогон оборвался и висит «выполняется»")
            raise


async def startup(ctx: dict) -> None:
    """То же, что делает `lifespan` API, минус всё, что относится к ручкам.

    Реестр инстансов Engage подключается обязательно: без него `engage.*` не знает,
    в какой инстанс идти, и дозапрос следующей страницы бэкфилла падает уже в воркере
    — то есть в месте, куда никто не смотрит.

    Таксономию каскада (`cascade_registry.reload`) читаем по той же причине: приём
    считает L0/L1 прямо здесь (`upsert_message` → `cascade.classify`), и воркер с
    таксономией по умолчанию из констант кода молча судил бы новые сообщения по
    старым якорям, пока правка не доедет случайным перезапуском контейнера.
    """
    get_settings().validate_runtime()
    if not queue.enabled():
        # Воркер без очереди не «работает вхолостую» — он не работает вовсе, а
        # процесс, который поднялся и молчит, выглядит исправным ровно до первого
        # вопроса «почему ничего не разбирается».
        raise RuntimeError(
            "RADAR_REDIS_URL не задан: воркеру приёма неоткуда брать события")
    async with get_session_maker()() as db:
        engage_registry.install()
        await engage_registry.reload(db)
        await cascade_registry.reload(db)
    cascade_registry.start_watch()
    logger.info("ingest_worker_started")


async def shutdown(ctx: dict) -> None:
    await cascade_registry.stop_watch()
    await engage.close()
    await queue.close()
    await get_engine().dispose()
    logger.info("ingest_worker_stopping")


class WorkerSettings:
    functions = [ingest_event]
    on_startup = startup
    on_shutdown = shutdown
    max_tries = MAX_TRIES
    job_timeout = 300
    # Результат держится сутки. Он же — ключ от повторной доставки: `_job_id` считается
    # по содержимому события, и пока результат жив, тот же вебхук второй раз не
    # разбирается. Сутки — с запасом больше любого разумного окна ретраев Engage.
    keep_result = 60 * 60 * 24

    # Считается при импорте, и отложить это некуда: arq читает `__dict__` класса
    # настроек и никаких функций не зовёт (`arq.worker.get_kwargs`). Поэтому импорт
    # обязан быть безобидным, а незаданный адрес ловится в `startup` — см. там.
    # Своя очередь на исполнителя. Общая означала бы, что событие приёма может
    # подобрать воркер прогонов и молча выбросить: он не знает этой функции.
    queue_name = queue.QUEUE_NAMES[queue.INGEST_EVENT]
    redis_settings = queue.redis_settings(fallback=True)
