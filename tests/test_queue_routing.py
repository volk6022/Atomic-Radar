"""Работа обязана попасть тому исполнителю, который умеет её делать.

29.08. Оба воркера слушали общую очередь arq, а знает каждый по одной функции, разной.
arq отдаёт работу подобравшему первым, и событие приёма регулярно доставалось воркеру
прогонов, который выбрасывал его со строкой «function 'ingest_event' not found» в
собственный лог. Снаружи это выглядело безупречно: постановщик получил идентификатор,
ручка ответила `202`, провалов ноль, очередь пуста.

Цена — не одно событие. Цепочку бэкфилла двигают вебхуки, поэтому потерянное событие
обрывает её целиком, а прогон остаётся «выполняется» навсегда и блокирует следующий
бэкфилл: одновременно разрешён один. На живом прогоне из 22 событий пропало одно;
на следующем канале — первое же.
"""
import os

import pytest

os.environ.setdefault("RADAR_SECRET_KEY", "x" * 32)
os.environ.setdefault("RADAR_DATABASE_URL", "postgresql+asyncpg://u:p@localhost/db")

from app.services import queue  # noqa: E402
from app.workers.ingest import WorkerSettings as IngestWorker  # noqa: E402
from app.workers.jobs import WorkerSettings as JobsWorker  # noqa: E402


def test_every_task_name_has_a_queue():
    """Задача без очереди уедет в общую — то есть туда, где её могут не понять."""
    for task in (queue.INGEST_EVENT, queue.RUN_JOB):
        assert task in queue.QUEUE_NAMES


def test_the_two_workers_do_not_share_a_queue():
    assert IngestWorker.queue_name != JobsWorker.queue_name


def test_each_worker_listens_where_its_own_work_is_put():
    """Имя очереди у воркера и у постановщика — одно и то же значение, не две строки.

    Разъехавшись, они дали бы ту же тишину: работа стоит, слушающего нет.
    """
    assert IngestWorker.queue_name == queue.QUEUE_NAMES[queue.INGEST_EVENT]
    assert JobsWorker.queue_name == queue.QUEUE_NAMES[queue.RUN_JOB]


def test_a_worker_knows_only_the_functions_of_its_own_queue():
    """Проверяется причина, а не только следствие: именно несовпадение набора функций
    и общей очереди делало потерю возможной."""
    assert [f.__name__ for f in IngestWorker.functions] == [queue.INGEST_EVENT]
    assert [f.__name__ for f in JobsWorker.functions] == [queue.RUN_JOB]


@pytest.mark.asyncio
async def test_an_unknown_task_is_refused_rather_than_queued_blindly(monkeypatch):
    """Новая задача, для которой забыли завести очередь, обязана падать сразу.

    Молчаливая постановка в общую очередь — это возвращение к исходному дефекту.
    """
    monkeypatch.setattr(queue, "enabled", lambda: True)

    async def _no_pool():  # pragma: no cover — до пула дойти не должно
        raise AssertionError("проверка имени обязана случиться до обращения к Redis")

    monkeypatch.setattr(queue, "_get_pool", _no_pool)

    with pytest.raises(queue.QueueUnavailable) as e:
        await queue.enqueue("нет_такой_задачи")
    assert "очередь" in str(e.value)


@pytest.mark.asyncio
async def test_the_queue_name_is_passed_to_arq(monkeypatch):
    monkeypatch.setattr(queue, "enabled", lambda: True)
    seen = {}

    class _Job:
        job_id = "j1"

    class _Pool:
        async def enqueue_job(self, task, *args, **kw):
            seen["task"] = task
            seen["queue"] = kw.get("_queue_name")
            return _Job()

    async def _pool():
        return _Pool()

    monkeypatch.setattr(queue, "_get_pool", _pool)

    await queue.enqueue(queue.INGEST_EVENT, {"event": "x"}, {})
    assert seen["queue"] == queue.QUEUE_NAMES[queue.INGEST_EVENT]
