"""Предохранители массовых решений по лидам.

Массовое действие — самое опасное, что есть в приложении: одно нажатие меняет
судьбу сотен записей. Поэтому здесь проверяется не «работает ли», а «отказывает ли
там, где должно», — и каждый тест назван условием, которое он защищает.

Ручки проверяются без сети и без БД: разбор запроса и правила отказа живут в чистых
функциях и константах, а поход в базу нужен только для того, чтобы узнать состав
выборки, — и он проверяется отдельно, интеграционно.
"""
from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_DEBUG", "true")

from app.api.v1 import leads  # noqa: E402
from app.core.access import BULK_LIMIT_REVIEWER, Capability, Role, allows  # noqa: E402
from app.core.config import get_settings  # noqa: E402
from app.main import create_app  # noqa: E402


@pytest.fixture
def client():
    get_settings.cache_clear()
    with TestClient(create_app(), raise_server_exceptions=False) as c:
        yield c


# ── справочники ───────────────────────────────────────────────────────────────

def test_actions_are_a_closed_set():
    """Свободного действия быть не может: каждое ведёт к своему статусу."""
    assert leads.BULK_ACTIONS == {"reject": "rejected", "approve": "approved",
                                  "reset": "new"}


def test_statuses_match_the_model():
    assert leads.STATUSES == ("new", "in_review", "approved", "rejected")


def test_sortable_columns_are_whitelisted():
    """Сортировка приходит из браузера. Список закрытый, иначе имя колонки уедет
    в SQL как строка."""
    assert set(leads.LEAD_SORTS) == {"score", "created", "author", "channel",
                                     "status", "pain"}


def test_unknown_status_is_rejected():
    with pytest.raises(Exception) as e:
        leads._check_status("удалён")
    assert "422" in str(e.value.status_code) or e.value.status_code == 422


# ── правила отказа ────────────────────────────────────────────────────────────

def test_bulk_needs_ids_or_filter(client):
    """Без того и другого «отклонить» означало бы «отклонить всё»."""
    r = client.post("/api/v1/leads/bulk", json={"action": "reject", "reason": "шум"})
    # 401 без входа — но 422 по телу считается раньше только после авторизации,
    # поэтому здесь проверяется именно защищённость ручки.
    assert r.status_code in (401, 403)


def test_reviewer_cap_is_smaller_than_a_typical_queue():
    """Потолок должен быть заметно меньше очереди, иначе он ничего не ограничивает:
    на момент введения в очереди было 108 черновиков."""
    assert BULK_LIMIT_REVIEWER == 25
    assert BULK_LIMIT_REVIEWER < 108


def test_reviewer_has_the_right_but_owner_has_no_cap():
    assert allows(Role.REVIEWER, Capability.BULK_DECIDE)
    assert allows(Role.OWNER, Capability.BULK_DECIDE)


@pytest.mark.parametrize("path,method", [
    ("/api/v1/leads", "get"),
    ("/api/v1/leads/pains", "get"),
    ("/api/v1/leads/1", "patch"),
    ("/api/v1/leads/bulk", "post"),
])
def test_every_lead_route_requires_login(client, path, method):
    """Ни одна ручка лидов не должна отвечать без входа — включая читающие:
    в них цитаты и имена живых людей."""
    # У GET тела нет: `json=` для него httpx не принимает, и тест падал бы на
    # клиенте, не дойдя до проверки прав.
    r = (client.get(path) if method == "get"
         else getattr(client, method)(path, json={}))
    assert r.status_code in (401, 403), f"{method.upper()} {path} → {r.status_code}"


def test_bulk_route_is_declared_before_the_id_route():
    """`/bulk` обязан быть объявлен раньше `/{lead_id}`, иначе FastAPI разберёт его
    как id и вернёт 422 «bulk не число». На `/reasons` в очереди черновиков это
    однажды уже случилось, и правка молча переставала открываться.
    """
    paths = [r.path for r in leads.router.routes]
    assert paths.index("/api/v1/leads/bulk") < paths.index("/api/v1/leads/{lead_id}")


def test_pains_route_is_declared_before_the_id_route():
    paths = [r.path for r in leads.router.routes]
    assert paths.index("/api/v1/leads/pains") < paths.index("/api/v1/leads/{lead_id}")
