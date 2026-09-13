"""Расписания автоматики в воркере приёма: сценарий 2 (волна В) и сценарий 3
(волна Г).

Тики `autoflow.reclassify_tick` и `autoflow.scan_tick` живут в `cron_jobs`
воркера приёма — там же, где очередь дочитывания и проверка кандидатов:
воркер прогонов с его `max_jobs = 1` и таймаутом в четыре часа тик бы просто
откладывал на время часовой переклассификации.

**Расписание вычисляется, а не проверяется по имени.** 05.09 крон с
`minute=range(0, 60, 5)` прошёл и проверку «крон зарегистрирован», и выкатку,
а воркеры упали на первом ударе сердца: arq принимает множество или число, но
не `range` (разбор — `tests/test_backfill_cron.py`, там же повторяющиеся
моменты). Здесь та же ловушка, только для новых тиков: запись обязана быть в
расписании, `minute` обязан быть множеством, и каждое расписание воркера
обязано реально посчитать следующий запуск.
"""
from __future__ import annotations

import os
from datetime import datetime

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_INGEST_TOKEN", "test-ingest-token")


def _cron_jobs():
    from app.workers.ingest import WorkerSettings
    return list(getattr(WorkerSettings, "cron_jobs", []) or [])


def _coroutine_name(job) -> str:
    return getattr(getattr(job, "coroutine", None), "__name__", "")


def test_reclassify_cron_registered_and_computable():
    """T-14: тик сценария 2 стоит в расписании воркера приёма с `minute` —
    множеством {0, 5, …, 55}, и все расписания воркера вычисляют следующий
    запуск (негодное `minute` валит `calculate_next` — ровно авария 05.09)."""
    jobs = _cron_jobs()
    names = [_coroutine_name(job) for job in jobs]
    mine = [job for job in jobs if _coroutine_name(job) == "reclassify_tick"]
    assert mine, (f"в расписании воркера приёма нет тика переклассификации: "
                  f"{names}")
    assert mine[0].minute == set(range(0, 60, 5)), (
        f"minute у тика переклассификации — {mine[0].minute!r}, "
        f"ожидалось множество {set(range(0, 60, 5))!r}")
    assert jobs, "у воркера приёма нет ни одной задачи по расписанию"
    for job in jobs:
        job.calculate_next(datetime(2026, 9, 12, 12, 0, 0))
        assert job.next_run is not None, (
            f"расписание {_coroutine_name(job)!r} не вычисляется от "
            f"2026-09-12 12:00 — воркер упал бы на первом ударе сердца")


def test_scan_cron_registered():
    """T-23: тик сценария 3 стоит в расписании раз в час — `minute` ровно
    множество {0}, не число и не range; следующий запуск вычисляется и от
    полудня, и от последней минуты года (граница смены суток и года — там,
    где арифметика расписания чаще всего ломается)."""
    jobs = _cron_jobs()
    names = [_coroutine_name(job) for job in jobs]
    mine = [job for job in jobs if _coroutine_name(job) == "scan_tick"]
    assert mine, f"в расписании воркера приёма нет тика автоскана: {names}"
    assert mine[0].minute == {0}, (
        f"minute у тика автоскана — {mine[0].minute!r}, ожидалось множество {{0}}")
    for moment in (datetime(2026, 9, 12, 12, 0, 0),
                   datetime(2026, 12, 31, 23, 59, 0)):
        mine[0].calculate_next(moment)
        assert mine[0].next_run is not None, (
            f"расписание тика автоскана не вычисляется от {moment} — "
            f"воркер упал бы на первом ударе сердца")
