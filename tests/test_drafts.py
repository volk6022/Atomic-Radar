"""Очередь черновиков: курсор, закрытый справочник причин и защита от повторных решений.

Отправка здесь не проверяется — это работа `test_outbound_gate.py`. Проверяется то,
что специфично для экрана: оператор не может проскочить черновик, принять по нему
решение дважды или отклонить с причиной, которой нет в справочнике.

Успешный путь одобрения требует БД (аудит-запись и чтение режима), поэтому он остаётся
за интеграционным прогоном. Все отказы разрешаются до первого обращения к базе, и
именно они здесь и проверяются.
"""
from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_DEBUG", "true")

from app.api.deps import current_user  # noqa: E402
from app.api.v1 import drafts  # noqa: E402
from app.core.config import get_settings  # noqa: E402
from app.db.models import User  # noqa: E402
from app.main import create_app  # noqa: E402


@pytest.fixture
def app():
    get_settings.cache_clear()
    a = create_app()
    # Вход подменяем: проверяется логика экрана, а не вход, у которого свои тесты.
    a.dependency_overrides[current_user] = lambda: User(
        id=1, email="ivan@atomic-automation.net", role="owner", is_active=True)
    return a


@pytest.fixture
def client(app):
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


@pytest.fixture(autouse=True)
def clean_decisions():
    """Решения живут в памяти модуля — между тестами очередь обязана быть чистой."""
    drafts._DECISIONS.clear()
    yield
    drafts._DECISIONS.clear()


# ── курсор ────────────────────────────────────────────────────────────────────

def test_cursor_walks_the_queue_in_order():
    q = drafts.QUEUE
    assert drafts._pick(q, None)["id"] == q[0]["id"]
    assert drafts._pick(q, q[0]["id"])["id"] == q[1]["id"]
    assert drafts._pick(q, q[1]["id"])["id"] == q[2]["id"]


def test_cursor_wraps_around_at_the_end():
    """J в конце очереди возвращает к началу, а не упирается в пустоту."""
    q = drafts.QUEUE
    assert drafts._pick(q, q[-1]["id"])["id"] == q[0]["id"]


def test_cursor_skips_decided_drafts():
    drafts._DECISIONS[drafts.QUEUE[1]["id"]] = {"decision": "rejected"}
    pending = drafts._pending()
    assert drafts.QUEUE[1]["id"] not in [d["id"] for d in pending]
    assert drafts._pick(pending, drafts.QUEUE[0]["id"])["id"] == drafts.QUEUE[2]["id"]


def test_empty_queue_is_a_state_not_an_error(client):
    for d in drafts.QUEUE:
        drafts._DECISIONS[d["id"]] = {"decision": "approved"}
    r = client.get("/api/v1/drafts/next")
    assert r.status_code == 200
    assert r.json() == {"remaining": 0, "draft": None}


def test_next_hides_internal_context(client):
    """Служебные id аккаунта и пира экрану не нужны и наружу не уходят."""
    body = client.get("/api/v1/drafts/next").json()
    assert body["remaining"] == len(drafts.QUEUE)
    assert "context" not in body["draft"]
    assert body["draft"]["variants"], "варианты обязаны приходить вместе с черновиком"


# ── справочник причин ─────────────────────────────────────────────────────────

def test_reasons_match_the_hotkeys(client):
    """Причин ровно девять и они пронумерованы 1..9: в интерфейсе это горячие клавиши."""
    reasons = client.get("/api/v1/drafts/reasons").json()
    assert [r["n"] for r in reasons] == list(range(1, 10))
    assert all(r["label"] for r in reasons)


def test_reject_requires_a_reason_from_the_list(client):
    d = drafts.QUEUE[0]["id"]
    assert client.post(f"/api/v1/drafts/{d}/reject", json={"reason_n": 99}).status_code == 422
    assert client.post(f"/api/v1/drafts/{d}/reject", json={}).status_code == 422
    assert drafts._DECISIONS == {}, "неудачная попытка не должна ничего записывать"


# ── защита решений ────────────────────────────────────────────────────────────

def test_unknown_draft_is_404(client):
    assert client.post("/api/v1/drafts/999999/approve",
                       json={"variant_index": 0}).status_code == 404


def test_nonexistent_variant_is_rejected(client):
    d = drafts.QUEUE[0]
    r = client.post(f"/api/v1/drafts/{d['id']}/approve",
                    json={"variant_index": len(d["variants"])})
    assert r.status_code == 422


def test_second_decision_on_the_same_draft_is_refused(client):
    """Одобрить уже отклонённый черновик нельзя: решение оператора — точка, а не черновик."""
    d = drafts.QUEUE[0]["id"]
    drafts._DECISIONS[d] = {"decision": "rejected", "reason": "Не та боль"}
    for path, body in ((f"/api/v1/drafts/{d}/approve", {"variant_index": 0}),
                       (f"/api/v1/drafts/{d}/reject", {"reason_n": 1})):
        r = client.post(path, json=body)
        assert r.status_code == 409, path


def test_decisions_require_auth(app):
    """Без входа решения не принимаются — матрица прав в GUI ничего не защищает."""
    app.dependency_overrides.clear()
    with TestClient(app, raise_server_exceptions=False) as anon:
        d = drafts.QUEUE[0]["id"]
        assert anon.post(f"/api/v1/drafts/{d}/approve",
                         json={"variant_index": 0}).status_code == 401
        assert anon.post(f"/api/v1/drafts/{d}/reject",
                         json={"reason_n": 1}).status_code == 401
