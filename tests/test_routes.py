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
    # Ручных отправок это касается на будущее: сейчас `/{entry_id}` только PATCH, и
    # перехватить GET он не может. Но стоит завести `GET /{entry_id}` — и `/form`
    # начнёт отвечать «entry_id не число». Порядок объявления удержим заранее.
    ("/api/v1/manual-sends/form", "/api/v1/manual-sends/{entry_id}"),
    ("/api/v1/manual-sends/list", "/api/v1/manual-sends/{entry_id}"),
    # Те же грабли во втором конвейере: у сценария своя очередь и свой курсор.
    ("/api/v1/workflows/{key}/drafts/next",
     "/api/v1/workflows/{key}/drafts/{draft_id}"),
    # Литерал против параметра при разных методах сегодня не сталкивается, но
    # порядок держим такой же — чтобы правило не пришлось вспоминать заново, когда
    # у целей появится `GET /{target_id}`.
    ("/api/v1/workflows/{key}/targets/bulk",
     "/api/v1/workflows/{key}/targets/{target_id}"),
]

# Пути, у которых литерал и параметр разведены по методам: `GET` есть только у
# одного из них, поэтому проверять перехват запросом нечем.
ONLY_ORDER = {"/api/v1/workflows/{key}/targets/bulk"}


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
    if named in ONLY_ORDER:
        pytest.skip("литерал и параметр разведены по методам — перехвата нет")
    # У маршрутов сценария ключ в пути настоящий: `{key}` — это шаблон OpenAPI,
    # а запрос ходит по конкретному адресу.
    assert client.get(named.replace("{key}", "cold_dm")).status_code == 401, (
        f"{named} перехвачен параметрическим маршрутом")
