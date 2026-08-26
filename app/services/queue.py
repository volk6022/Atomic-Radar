"""Очередь фоновой работы: arq поверх Redis.

**Ступень включается адресом.** Пустой `RADAR_REDIS_URL` означает «очереди нет», и это
рабочий режим, а не поломка: приём тогда разбирает вебхук прямо в запросе — ровно так,
как делал до появления воркеров. Тот же приём, что у моделей L2/L3, и по той же
причине: ступень, которой нет, должна называть себя вслух, а не притворяться пустым
результатом.

**Пул не живёт в глобали вечно.** Это прямой урок из Engage, повторять который нельзя.
Там `_get_pool()` кэширует пул arq в модульной переменной и **не сбрасывает его при
сбое**: соединение с Redis умирает, пул остаётся в кэше битым, каждая следующая
постановка падает об него же, строки `WebhookDelivery` копятся в `pending`, и ни один
из двух cron-джобов их не перевозит. Нашлось это QA-аудитом, а не мониторингом, потому
что снаружи всё выглядело работающим. Здесь любая ошибка постановки **роняет пул**,
и следующая попытка создаёт соединение заново.

**Потеря постановки — не потеря сообщения.** Приёмная сторона считается ненадёжной
намеренно: то, что не доехало вебхуком, добирается бэкфиллом, который перечитывает
историю канала. Поэтому сбой очереди — это `503` отправителю и тревога оператору, а не
героические попытки разобрать событие в обход очереди: разбор в запросе под упавшим
Redis означал бы, что под нагрузкой система тихо возвращается к поведению, от которого
уходила.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any

from app.core.config import get_settings

logger = logging.getLogger(__name__)

# Имена задач. Держатся здесь, а не строками по месту вызова: постановщик и воркер
# живут в разных процессах, и опечатка в имени с одной стороны выглядит как молчаливо
# пропавшая работа — задача ставится, исполнителя для неё нет.
INGEST_EVENT = "ingest_event"
RUN_JOB = "run_job"

_pool: Any = None
_lock = asyncio.Lock()


class QueueDisabled(RuntimeError):
    """Очередь выключена настройкой. Вызывающий обязан иметь путь без неё."""


class QueueUnavailable(RuntimeError):
    """Очередь включена, но недоступна. Работа не поставлена и не выполнена."""


def enabled() -> bool:
    return bool(get_settings().REDIS_URL)


def redis_settings(fallback: bool = False):
    """Настройки подключения arq. Их же читает воркер.

    `fallback=True` — отдать настройки по умолчанию вместо отказа. Нужно ровно одному
    вызывающему и по неустранимой причине: arq читает `__dict__` класса настроек
    воркера при импорте модуля и никаких функций не зовёт
    (`arq.worker.get_kwargs`), а импорт, падающий из-за незаданной переменной, ломает
    заодно и сбор тестов. Воркер поэтому импортируется всегда, а адрес проверяет на
    старте — там отказ видит оператор, а не тот, кто запустил pytest.
    """
    from arq.connections import RedisSettings

    url = get_settings().REDIS_URL
    if url:
        return RedisSettings.from_dsn(url)
    if fallback:
        return RedisSettings()
    raise QueueDisabled("RADAR_REDIS_URL не задан")


async def _get_pool():
    global _pool
    if _pool is not None:
        return _pool
    async with _lock:
        if _pool is None:
            from arq import create_pool

            _pool = await create_pool(redis_settings())
    return _pool


async def close() -> None:
    """Отпустить соединение. Зовётся при остановке процесса и при любой ошибке."""
    global _pool
    pool, _pool = _pool, None
    if pool is None:
        return
    with contextlib.suppress(Exception):
        await pool.aclose()


async def enqueue(task: str, *args: Any, **kwargs: Any) -> str:
    """Поставить задачу. Возвращает идентификатор работы.

    `QueueDisabled` — очередь выключена настройкой, вызывающий идёт своим путём.
    `QueueUnavailable` — очередь включена и не отвечает; работа НЕ выполнена, и делать
    вид, что выполнена, нельзя.
    """
    if not enabled():
        raise QueueDisabled("RADAR_REDIS_URL не задан")

    try:
        pool = await _get_pool()
        job = await pool.enqueue_job(task, *args, **kwargs)
    except Exception as e:  # noqa: BLE001 — вид ошибки Redis тут не важен, важен сброс
        await close()
        logger.error("queue_enqueue_failed task=%s error=%s", task, e)
        raise QueueUnavailable(f"очередь не приняла задачу {task}: {e}") from e

    if job is None:
        # arq отдаёт None, когда работа с таким `_job_id` уже стоит. Это не сбой:
        # повторная доставка одного вебхука не должна разбираться дважды.
        logger.info("queue_job_already_queued task=%s", task)
        return ""

    logger.info("queue_job_enqueued task=%s job=%s", task, job.job_id)
    return job.job_id


async def ping() -> str:
    """Статус для плитки сервисов: `ok` | `off` | текст ошибки — как у моделей.

    `off` и ошибка — разные вещи, и путать их нельзя. Выключенная очередь означает, что
    приём разбирает события сам и всё работает; недоступная — что приём отвечает
    отказом и события не разбираются вовсе.
    """
    if not enabled():
        return "off"
    try:
        pool = await _get_pool()
        await pool.ping()
    except Exception as e:  # noqa: BLE001
        await close()
        return f"нет связи: {type(e).__name__}"
    return "ok"
