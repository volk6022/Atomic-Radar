"""Сколько шла задача — первое, что спрашивают, открыв список прогонов.

Замечание Андрея по интерфейсу: в разделе Runs не видно длительности. Строка
показывает, когда задача создана и когда закончилась, а «час двадцать» человек
вынужден вычитать в уме из двух меток времени — причём у идущей задачи вычитать
не из чего, второй метки ещё нет.

Здесь проверяется ровно контракт ручки, без базы: `_row()` — чистая функция от
строки `Run`, и её можно позвать на объекте, собранном в памяти.

Три случая, и все три разные:

* задача закончилась — длительность это разница меток, число фиксированное;
* задача идёт — длительность растёт от `started_at` до «сейчас», и показывать её
  всё равно надо: «идёт 40 минут» и «идёт 6 часов» — разные новости;
* задача не стартовала — длительности нет, и подставлять ноль нельзя: ноль читается
  как «отработала мгновенно», а не как «ещё не начиналась».
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_DEBUG", "true")

from app.api.v1.runs import _row  # noqa: E402
from app.db.models import Run  # noqa: E402


def _run(**kw) -> Run:
    """Строка `Run` в памяти: `_row` читает атрибуты и в базу не ходит."""
    base = dict(
        id=1, kind="reclassify", status="done", progress=1.0, params={},
        error=None, result=None, cancel_requested=False, created_by="test",
        created_at=datetime(2026, 9, 1, 12, 0, 0, tzinfo=timezone.utc),
        started_at=None, finished_at=None,
    )
    base.update(kw)
    r = Run()
    for k, v in base.items():
        setattr(r, k, v)
    return r


def test_finished_run_reports_the_time_it_actually_took():
    started = datetime(2026, 9, 1, 12, 0, 0, tzinfo=timezone.utc)
    row = _row(_run(started_at=started, finished_at=started + timedelta(minutes=80)))

    assert "duration_seconds" in row, "у завершённой задачи нет поля длительности"
    assert row["duration_seconds"] == pytest.approx(4800, abs=1)


def test_running_run_reports_how_long_it_has_been_going():
    started = datetime.now(timezone.utc) - timedelta(minutes=40)
    row = _row(_run(status="running", started_at=started, finished_at=None))

    # Идущая задача — главный случай: именно на неё смотрят, чтобы решить, ждать
    # или отменять. Ноль или None здесь означали бы, что смотреть не на что.
    assert row["duration_seconds"] is not None, "у идущей задачи длительность пуста"
    assert row["duration_seconds"] == pytest.approx(2400, abs=5)


def test_run_that_never_started_has_no_duration():
    row = _row(_run(status="queued", started_at=None, finished_at=None))

    # Именно None, а не 0: ноль читается как «отработала мгновенно».
    assert row["duration_seconds"] is None


def test_duration_survives_a_naive_started_at():
    """`started_at` из базы может прийти без часового пояса.

    Postgres отдаёт `timestamptz`, но строка, собранная в тестах или пришедшая из
    миграции, бывает наивной. Вычитание наивного из aware даёт TypeError и роняет
    весь список прогонов — то есть один кривой ряд гасит целый экран.
    """
    naive = datetime.now() - timedelta(minutes=10)
    row = _row(_run(status="running", started_at=naive, finished_at=None))

    assert row["duration_seconds"] == pytest.approx(600, abs=120)
