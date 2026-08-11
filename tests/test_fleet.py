"""Флот: живые данные Engage и поведение при его отказе.

Главное, что здесь проверяется, — отказ Engage виден как отказ. Экран флота нужен,
чтобы решать, ставить ли аккаунт на паузу; молча показать на нём пустой список или
вчерашний мок значит подтолкнуть к решению на выдуманных данных.
"""
from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_DEBUG", "true")

from app.api.deps import current_user  # noqa: E402
from app.api.v1 import screens  # noqa: E402
from app.core.config import get_settings  # noqa: E402
from app.db.models import User  # noqa: E402
from app.main import create_app  # noqa: E402
from app.services import engage  # noqa: E402

# Ответ Engage дословно с боевого инстанса `vertsanov` (5 аккаунтов Андрея).
ENGAGE_ACCOUNTS = [
    {"account_id": 1, "phone": "+12159021784", "phone_country": "US", "status": "active",
     "warmup_tier": "fresh", "use_case": "cold_dm", "warmup_day": 0,
     "proxy": {"id": 1, "country": "US", "type": "residential", "is_healthy": True}},
    {"account_id": 2, "phone": "+33750664952", "phone_country": "FR", "status": "active",
     "warmup_tier": "fresh", "use_case": "cold_dm", "warmup_day": 0,
     "proxy": {"id": 2, "country": "US", "type": "residential", "is_healthy": True}},
]
ENGAGE_SAFETY = {"warmup_totals": {"cold_dm": 30, "inviting": 45}}


@pytest.fixture
def app():
    get_settings.cache_clear()
    a = create_app()
    a.dependency_overrides[current_user] = lambda: User(
        id=1, email="ivan@atomic-automation.net", role="owner", is_active=True)
    return a


@pytest.fixture
def client(app):
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


@pytest.fixture
def engage_ok(monkeypatch):
    async def accounts():
        return ENGAGE_ACCOUNTS

    async def safety():
        return ENGAGE_SAFETY

    monkeypatch.setattr(screens.engage, "list_accounts", accounts)
    monkeypatch.setattr(screens.engage, "safety_config", safety)


def test_phone_is_masked():
    """Полный номер наружу не уходит: для опознания аккаунта хватает краёв."""
    assert screens._mask_phone("+12159021784") == "+1215•••1784"
    assert screens._mask_phone(None) == "—"
    assert "9021" not in screens._mask_phone("+12159021784")


def test_fleet_returns_live_accounts(client, engage_ok):
    rows = client.get("/api/v1/accounts").json()
    assert [r["id"] for r in rows] == [1, 2]
    assert rows[0]["status"] == "active"
    assert rows[0]["warmup_total"] == 30, "потолок прогрева берётся из конфига Engage"


def test_geo_mismatch_is_computed(client, engage_ok):
    """Французский номер на американском прокси — тот самый рассинхрон, из-за которого
    гейт Engage усыплял аккаунты."""
    rows = client.get("/api/v1/accounts").json()
    by_id = {r["id"]: r for r in rows}
    assert by_id[1]["geo_match"] is True      # US / US
    assert by_id[2]["geo_match"] is False     # FR / US


def test_engage_down_is_503_not_empty_list(client, monkeypatch):
    async def boom():
        raise engage.EngageUnavailable("Engage недоступен: ConnectError")

    monkeypatch.setattr(screens.engage, "list_accounts", boom)
    r = client.get("/api/v1/accounts")
    assert r.status_code == 503
    assert "Engage" in r.json()["detail"]


def test_fleet_requires_auth(app):
    app.dependency_overrides.clear()
    with TestClient(app, raise_server_exceptions=False) as anon:
        assert anon.get("/api/v1/accounts").status_code == 401
