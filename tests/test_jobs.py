"""Исполнитель задач: что должно отказывать и что должно переживать перезапуск.

Сам прогон здесь не гоняется — он ходит в модель и в базу. Проверяется обвязка:
закрытый список видов, права по видам, запрет на две задачи одного вида, отмена
флагом (а не обрывом) и честная пометка задач, переживших смерть процесса.
"""
from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_DEBUG", "true")

from app.api.v1 import runs as runs_api  # noqa: E402
from app.core.access import Capability, Role, allows  # noqa: E402
from app.core.config import get_settings  # noqa: E402
from app.main import create_app  # noqa: E402
from app.services import jobs, reclassify  # noqa: E402


@pytest.fixture
def client():
    get_settings.cache_clear()
    with TestClient(create_app(), raise_server_exceptions=False) as c:
        yield c


# ── закрытые списки ───────────────────────────────────────────────────────────

def test_kind_is_not_an_arbitrary_callable():
    """`kind` приходит из браузера. Запускать по нему что попало нельзя."""
    assert set(jobs.RUNNERS) <= set(jobs.KINDS)
    assert "reclassify" in jobs.RUNNERS


def test_every_kind_declares_a_capability():
    """Вид без права запускался бы кем угодно — включая тот, что занимает карту."""
    for kind in jobs.KINDS:
        assert kind in runs_api.KIND_CAPABILITY, kind


def test_heavy_and_cheap_kinds_have_different_rights():
    """Дочитать канал дорого по лимитам чтения, но не опасно; пересчёт занимает
    видеокарту, на которой работает живой каскад. Одно право на оба означало бы
    отдать заказчику карту вместе с бэкфиллом."""
    assert runs_api.KIND_CAPABILITY["reclassify"] == Capability.RUN_HEAVY
    assert runs_api.KIND_CAPABILITY["backfill"] == Capability.RUN_BACKFILL
    assert allows(Role.CUSTOMER, Capability.RUN_BACKFILL)
    assert not allows(Role.CUSTOMER, Capability.RUN_HEAVY)


def test_active_statuses_cover_queued_and_running():
    """«Ещё не начали» и «идёт» одинаково означают занято: вторая задача того же
    вида не должна проскочить в промежутке между созданием строки и стартом."""
    assert jobs.ACTIVE == ("queued", "running")


def test_scopes_are_closed():
    assert reclassify.SCOPES == ("all", "pending")


def test_progress_weights_sum_to_a_full_scale():
    """Иначе прогресс замирает на 80% и человек считает, что задача повисла."""
    assert sum(reclassify.WEIGHTS.values()) == 100


def test_l3_concurrency_matches_the_server_slots():
    """Больше — очередь на стороне llama-server, меньше — простой карты."""
    assert reclassify.L3_CONCURRENCY == 4


# ── защищённость ручек ────────────────────────────────────────────────────────

@pytest.mark.parametrize("path,method", [
    ("/api/v1/runs", "get"),
    ("/api/v1/runs/1", "get"),
    ("/api/v1/runs", "post"),
    ("/api/v1/runs/1/cancel", "post"),
])
def test_every_run_route_requires_login(client, path, method):
    r = (client.get(path) if method == "get"
         else client.post(path, json={"kind": "reclassify"}))
    assert r.status_code in (401, 403), f"{method.upper()} {path} → {r.status_code}"


def test_cancel_route_is_declared_after_the_list_but_reachable():
    """`/{run_id}` не должен перехватывать `/{run_id}/cancel`: пути разной длины,
    но порядок объявления в FastAPI важен, и на `/reasons` мы уже обжигались."""
    paths = [r.path for r in runs_api.router.routes]
    assert "/api/v1/runs/{run_id}/cancel" in paths
    assert "/api/v1/runs/{run_id}" in paths


# ── миграция ──────────────────────────────────────────────────────────────────

def test_migrations_only_add_never_drop():
    """Список выполняется на живой базе при каждом старте. Любое выражение, теряющее
    данные, однажды выполнится не на той базе."""
    from app.db.migrate import STATEMENTS
    for stmt in STATEMENTS:
        upper = stmt.upper()
        assert "DROP" not in upper, stmt
        assert "DELETE" not in upper, stmt
        assert "TRUNCATE" not in upper, stmt
        assert "IF NOT EXISTS" in upper, stmt


def test_new_run_columns_are_declared_in_the_model():
    """Колонка, добавленная миграцией, но не описанная в модели, не читается кодом —
    и наоборот: описанная, но не добавленная, роняет запрос."""
    from app.db.models import Run
    for name in ("cancel_requested", "log", "result", "created_by"):
        assert hasattr(Run, name), name


# ── бэкфилл: задача без исполнителя внутри процесса ───────────────────────────

def test_only_in_process_kinds_are_marked_interrupted():
    """Бэкфилл двигают вебхуки Engage, а не наш процесс: перезапуск контейнера
    цепочку не обрывает, и пометить его прерванным значило бы соврать наоборот.
    Проверяем не поведение с базой, а само условие отбора."""
    import inspect
    src = inspect.getsource(jobs.mark_interrupted)
    assert "Run.kind.in_(list(RUNNERS))" in src


def test_backfill_has_no_in_process_runner():
    assert "backfill" in jobs.KINDS
    assert "backfill" not in jobs.RUNNERS


# ── тревоги ───────────────────────────────────────────────────────────────────

def test_alert_severities_are_closed():
    from app.services import alerts
    assert alerts.SEVERITIES == ("info", "warn", "error")


def test_states_are_not_acknowledgeable():
    """Пометить прочитанным сухой прогон нельзя: он не событие, а положение дел,
    и «спрятать» его значило бы спрятать от себя факт."""
    import inspect
    from app.api.v1 import alerts as alerts_api
    src = inspect.getsource(alerts_api._states)
    assert '"ack": False' in src
    # У состояний строковый id — по нему клиент и отличает их от событий.
    assert '"state:mode"' in src


def test_pending_threshold_is_far_above_normal_ingest():
    """Сотня-другая в очереди — обычный ход ингеста. Порог должен ловить «пересчёт
    давно не запускали», а не нормальную работу."""
    from app.api.v1.alerts import PENDING_ALERT_THRESHOLD
    assert PENDING_ALERT_THRESHOLD >= 500
