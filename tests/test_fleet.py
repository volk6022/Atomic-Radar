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

# Ответ `GET /v1/limits` (E1) — форма по `_REF-engage-limits.py`: показаны только
# действия, которые читает экран. У аккаунта 1 остаток связывает per-account
# (2 < 5), у аккаунта 2 — агрегат (1 < 3): обе ветки формулы R5 §2.
ENGAGE_LIMITS = {
    "generated_at": "2026-09-11T12:00:00+00:00",
    "accounts": [
        {"account_id": 1, "use_case": "cold_dm", "api_credential_id": 7,
         "cap_profile": "conservative", "actions": [
             {"action": "joins_per_day", "kind": "write",
              "per_account": {"cap": 3, "used": 1, "remaining": 2,
                              "resets_in_seconds": 61234},
              "aggregate": {"scope": "api_credential", "api_credential_id": 7,
                            "use_case": "cold_dm", "account_count": 2,
                            "cap": 10, "used": 5, "remaining": 5,
                            "resets_in_seconds": 61234},
              "binding": "per_account", "remaining": 2},
             {"action": "messages_per_day", "kind": "write",
              "per_account": {"cap": 20, "used": 11, "remaining": 9,
                              "resets_in_seconds": 61234},
              "aggregate": {"scope": "api_credential", "api_credential_id": 7,
                            "use_case": "cold_dm", "account_count": 2,
                            "cap": 40, "used": 31, "remaining": 9,
                            "resets_in_seconds": 61234},
              "binding": "per_account", "remaining": 9},
         ]},
        {"account_id": 2, "use_case": "cold_dm", "api_credential_id": 7,
         "cap_profile": "conservative", "actions": [
             {"action": "joins_per_day", "kind": "write",
              "per_account": {"cap": 3, "used": 0, "remaining": 3,
                              "resets_in_seconds": 61234},
              "aggregate": {"scope": "api_credential", "api_credential_id": 7,
                            "use_case": "cold_dm", "account_count": 2,
                            "cap": 10, "used": 9, "remaining": 1,
                            "resets_in_seconds": 61234},
              "binding": "aggregate", "remaining": 1},
             {"action": "messages_per_day", "kind": "write",
              "per_account": {"cap": 20, "used": 16, "remaining": 4,
                              "resets_in_seconds": 61234},
              "aggregate": {"scope": "api_credential", "api_credential_id": 7,
                            "use_case": "cold_dm", "account_count": 2,
                            "cap": 40, "used": 31, "remaining": 9,
                            "resets_in_seconds": 61234},
              "binding": "aggregate", "remaining": 4},
         ]},
    ],
    "missing": [],
}


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

    async def limits():
        return ENGAGE_LIMITS

    monkeypatch.setattr(screens.engage, "list_accounts", accounts)
    monkeypatch.setattr(screens.engage, "safety_config", safety)
    monkeypatch.setattr(screens.engage, "limits", limits)


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


def test_fleet_shows_engage_remaining(client, engage_ok, monkeypatch):
    """Остатки E1 читаются в строку флота по формуле R5 §2: `remaining` — уже
    min(per_account, aggregate) по правилам E1 (у аккаунта 1 связывает per-account,
    у аккаунта 2 — агрегат), resets — TTL скользящего окна, агрегат — отдельно.
    Вызов `limits()` на запрос ручки — ровно один."""
    calls = []

    async def limits():
        calls.append(1)
        return ENGAGE_LIMITS

    monkeypatch.setattr(screens.engage, "limits", limits)
    rows = client.get("/api/v1/accounts").json()
    by_id = {r["id"]: r for r in rows}

    assert by_id[1]["joins_remaining"] == 2, "act.remaining, не per_account.remaining"
    assert by_id[1]["joins_resets_in_seconds"] == 61234, \
        "TTL скользящего окна Engage, не время до полуночи UTC"
    assert by_id[1]["joins_aggregate_remaining"] == 5, \
        "«сколько осталось флоту на api_id» — отдельное поле, не смешано с per-account"
    assert by_id[1]["messages_remaining"] == 9
    assert by_id[2]["joins_remaining"] == 1, "связывает агрегат — remaining это учитывает"
    assert by_id[2]["joins_aggregate_remaining"] == 1
    assert by_id[2]["messages_remaining"] == 4
    assert len(calls) == 1, "один вызов limits() на запрос, а не один на строку"


def test_limits_down_is_dash_not_503(client, monkeypatch):
    """Опрос остатка упал, флот жив: экран 200, четыре новых поля — прочерк (null,
    не 0: нуль читался бы как «лимит исчерпан»), существующие поля на месте."""
    async def accounts():
        return ENGAGE_ACCOUNTS

    async def safety():
        return ENGAGE_SAFETY

    async def limits():
        raise engage.EngageUnavailable("Engage недоступен: ConnectError")

    monkeypatch.setattr(screens.engage, "list_accounts", accounts)
    monkeypatch.setattr(screens.engage, "safety_config", safety)
    monkeypatch.setattr(screens.engage, "limits", limits)

    r = client.get("/api/v1/accounts")
    assert r.status_code == 200
    by_id = {row["id"]: row for row in r.json()}
    for row in by_id.values():
        assert row["joins_remaining"] is None
        assert row["joins_resets_in_seconds"] is None
        assert row["joins_aggregate_remaining"] is None
        assert row["messages_remaining"] is None
    assert by_id[1]["status"] == "active"
    assert by_id[1]["warmup_total"] == 30


def test_account_missing_from_limits_is_dash(client, monkeypatch):
    """Аккаунт флота, которого нет в ответе E1 (попал в `missing`), — прочерк в
    остатках при живой строке: нет данных — не ноль и не падение."""
    async def accounts():
        return ENGAGE_ACCOUNTS

    async def safety():
        return ENGAGE_SAFETY

    async def limits():
        return {**ENGAGE_LIMITS, "accounts": [ENGAGE_LIMITS["accounts"][0]],
                "missing": [2]}

    monkeypatch.setattr(screens.engage, "list_accounts", accounts)
    monkeypatch.setattr(screens.engage, "safety_config", safety)
    monkeypatch.setattr(screens.engage, "limits", limits)

    by_id = {row["id"]: row for row in client.get("/api/v1/accounts").json()}
    assert by_id[1]["joins_remaining"] == 2
    assert by_id[2]["status"] == "active"
    assert by_id[2]["joins_remaining"] is None
    assert by_id[2]["joins_aggregate_remaining"] is None
    assert by_id[2]["messages_remaining"] is None


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
