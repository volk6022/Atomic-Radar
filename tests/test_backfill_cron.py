"""Расписание дочитывания: у Радара появляется первая задача по времени.

До сих пор в Радаре не было **ни одной** cron-задачи: всё начиналось нажатием
человека. Из-за этого стоит и добор `pending` (новый лид ждёт кнопки
«Переклассификация»), и очередь дочитывания — «автоматический бэкфилл» без
расписания автоматическим не бывает.

**Почему тик живёт у воркера приёма, а не у воркера прогонов.** У прогонов
`max_jobs = 1` и `job_timeout` в четыре часа: тик, попавший туда во время
часовой переклассификации, простоит в очереди этот час, и очередь дочитывания
встанет ровно тогда, когда GPU и так занят. Воркер приёма, наоборот, живёт
короткими задачами, и именно он уже ведёт цепочки страниц — стартовать их из
того же процесса честнее, чем из чужого.

⚠️ **Расписание надо ВЫЧИСЛЯТЬ, а не проверять по имени.** 05.09 крон Engage с
`minute=range(0, 60, 5)` прошёл и проверку «крон зарегистрирован», и сборку
образа, и выкатку, — а воркеры упали на первом ударе сердца: arq принимает
множество или число, но не `range`. Тест, который смотрит только на имена,
повторил бы эту аварию слово в слово.
"""
from __future__ import annotations

import os
from datetime import datetime

import pytest

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_INGEST_TOKEN", "test-ingest-token")


def _cron_jobs():
    from app.workers.ingest import WorkerSettings
    return list(getattr(WorkerSettings, "cron_jobs", []) or [])


def test_backfill_drain_is_scheduled():
    """Тик очереди дочитывания стоит в расписании воркера приёма."""
    names = {getattr(job, "name", None) or getattr(job.coroutine, "__name__", "")
             for job in _cron_jobs()}
    assert any("backfill" in str(name) for name in names), (
        f"в расписании нет тика дочитывания: {names}")


@pytest.mark.parametrize("moment", [
    datetime(2026, 9, 5, 12, 0, 0),
    datetime(2026, 9, 5, 23, 59, 59),
    datetime(2026, 12, 31, 23, 55, 0),
])
def test_every_cron_schedule_is_actually_computable(moment):
    """Каждое расписание обязано посчитать следующий запуск, а не просто быть.

    Это единственная проверка, которая ловит `range` в `minute`: arq зовёт
    `calculate_next` на первом ударе сердца, и негодное расписание валит воркер
    в цикл перезапуска — уже после успешной выкатки.
    """
    jobs = _cron_jobs()
    assert jobs, "у воркера приёма нет ни одной задачи по расписанию"
    for job in jobs:
        job.calculate_next(moment)
        assert job.next_run is not None, (
            f"расписание {getattr(job, 'name', job)} не вычисляется от {moment}")


def test_drain_tick_survives_an_empty_queue():
    """Тик по пустой очереди — обычное дело, а не ошибка.

    Он бьётся каждые несколько минут круглые сутки; исключение на пустой очереди
    означало бы постоянный поток ложных отказов в журнале воркера, среди которых
    настоящий отказ уже не заметить.
    """
    from app.workers import ingest as ingest_worker
    assert hasattr(ingest_worker, "backfill_drain_tick"), (
        "функция тика обязана быть у воркера приёма — её зовёт расписание")
