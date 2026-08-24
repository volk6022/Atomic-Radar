"""Приложение поднимается, маршруты на месте, без входа наружу ничего не отдаётся."""
from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient

# Ключ подписи проверяется на старте — задаём до импорта приложения.
os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_DEBUG", "true")

from app.core.config import get_settings  # noqa: E402
from app.main import create_app  # noqa: E402


@pytest.fixture(scope="module")
def app():
    get_settings.cache_clear()
    return create_app()


@pytest.fixture(scope="module")
def client(app):
    # БД в этом тесте не нужна: сессия создаётся лениво, а проверка cookie до неё
    # не доходит — анонимный запрос обязан получить 401 даже при мёртвой базе.
    #
    # `raise_server_exceptions=False` — чтобы ручка, которой БД всё-таки нужна,
    # вернула 500 как настоящий сервер, а не уронила сам тест исключением.
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


def test_health_is_open(client):
    r = client.get("/api/v1/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


@pytest.mark.parametrize("path", [
    "/api/v1/dashboard", "/api/v1/leads", "/api/v1/drafts/next", "/api/v1/accounts",
    "/api/v1/limits", "/api/v1/users", "/api/v1/audit", "/api/v1/system/mode",
    # Реестр сценариев открыт любой роли, но не анониму: «любой вошедшей» — не «любой».
    "/api/v1/workflows", "/api/v1/workflows/cold_dm",
])
def test_data_endpoints_require_auth(client, path):
    """Ни одна ручка с данными не отвечает анонимному запросу."""
    assert client.get(path).status_code == 401


def test_login_rejects_unknown_user(client):
    r = client.post("/api/v1/auth/login",
                    json={"username": "nobody@example.com", "password": "x"})
    # 401 (нет пользователя) или 500 (нет БД) — но точно не успех.
    assert r.status_code != 200


def test_totp_without_login_is_rejected(client):
    assert client.post("/api/v1/auth/totp", json={"code": "123456"}).status_code == 401


def test_every_contract_endpoint_is_registered(app):
    """Сверка с `radar-api-contract.md`: ручка, которую ждёт GUI, должна существовать.

    Список берётся из OpenAPI, а не из `app.routes`: подключённый роутер лежит там
    завёрнутым в `_IncludedRouter`, без плоского `.path`. Заодно это ровно та
    поверхность, которую видит фронтенд.
    """
    paths = set(app.openapi()["paths"])
    for expected in (
        "/api/v1/alerts", "/api/v1/counters", "/api/v1/dashboard", "/api/v1/accounts",
        "/api/v1/channels", "/api/v1/messages", "/api/v1/leads", "/api/v1/drafts/next",
        "/api/v1/conversations", "/api/v1/profile", "/api/v1/runs", "/api/v1/evaluations",
        "/api/v1/attribution", "/api/v1/traces", "/api/v1/limits", "/api/v1/users",
        "/api/v1/audit", "/api/v1/system/mode", "/api/v1/system/kill",
        "/api/v1/auth/login", "/api/v1/auth/totp", "/api/v1/auth/logout", "/api/v1/auth/me",
        "/api/v1/workflows", "/api/v1/workflows/{key}", "/api/v1/workflows/{key}/sections",
    ):
        assert expected in paths, f"нет маршрута {expected}"
