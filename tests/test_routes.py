"""Порядок маршрутов: именованные пути не должны перехватываться параметром.

Написан после того, как `/api/v1/drafts/reasons` уехал в `/{draft_id}` и стал
отвечать 422 «draft_id не число». На экране это выглядело не как ошибка маршрутов,
а как «не открывается правка черновика» — искать пришлось от симптома.
"""
from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient

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
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


# Именованный путь → параметрический шаблон, который мог бы его перехватить.
NAMED_BEFORE_PARAM = [
    ("/api/v1/drafts/reasons", "/api/v1/drafts/{draft_id}"),
    ("/api/v1/drafts/next", "/api/v1/drafts/{draft_id}"),
    ("/api/v1/drafts/list", "/api/v1/drafts/{draft_id}"),
]


@pytest.mark.parametrize("named,param", NAMED_BEFORE_PARAM)
def test_named_route_declared_before_parametrised(app, named, param):
    paths = list(app.openapi()["paths"])
    assert named in paths and param in paths
    assert paths.index(named) < paths.index(param), (
        f"{named} объявлен после {param} и будет им перехвачен")


@pytest.mark.parametrize("named,_param", NAMED_BEFORE_PARAM)
def test_named_route_is_not_validated_as_id(client, named, _param):
    """Признак перехвата — 422 вместо 401: путь дошёл до валидации `draft_id`,
    а не до проверки сессии."""
    assert client.get(named).status_code == 401, (
        f"{named} перехвачен параметрическим маршрутом")
