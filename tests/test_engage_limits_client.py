"""Клиентский метод `engage.limits()` — форма запроса, не форма ответа.

Radar спрашивает у Engage остатки лимитов (`GET /v1/limits`, E1) перед
планированием вступлений. За что отвечает клиент и что здесь проверяется:

* путь с перечнем аккаунтов: `account_ids` уезжают в query через запятую;
* обязательный параметр выбора инстанса `instance=` — тот же паттерн, что у
  `list_accounts` / `safety_config` / `fleet_health`; отсутствие — инстанс
  `default` (решает `_get`, клиент лишь передаёт);
* ответ проходит наружу как есть: клиент не пересобирает и не выбрасывает поля.

Деградация — свойство `_get`: и 404 старой версии Engage, и обрыв соединения
превращаются в `EngageUnavailable`, и различать их клиент сознательно не умеет.
Реакция потребителя (план вступлений) проверяется в
`test_discussions_join_db.py`, здесь она не дублируется.
"""
from __future__ import annotations

import asyncio

from app.services import engage


def _capture(monkeypatch) -> dict:
    """`_get`, который запоминает путь и инстанс и отвечает пустым телом."""
    seen: dict = {}

    async def fake_get(path, *, instance=None):
        seen["path"] = path
        seen["instance"] = instance
        return {"accounts": [], "missing": []}

    monkeypatch.setattr(engage, "_get", fake_get)
    return seen


def test_limits_builds_account_ids_path(monkeypatch):
    seen = _capture(monkeypatch)
    out = asyncio.run(engage.limits(account_ids=[1, 2]))
    assert seen["path"] == "/v1/limits?account_ids=1,2"
    assert seen["instance"] is None
    assert out == {"accounts": [], "missing": []}, (
        "ответ Engage обязан пройти наружу как есть")


def test_limits_passes_instance(monkeypatch):
    seen = _capture(monkeypatch)
    asyncio.run(engage.limits(instance="clienta"))
    assert seen["instance"] == "clienta"


def test_limits_without_accounts_asks_the_whole_fleet(monkeypatch):
    seen = _capture(monkeypatch)
    asyncio.run(engage.limits())
    assert seen["path"] == "/v1/limits"


def test_limits_with_empty_account_ids_asks_the_whole_fleet(monkeypatch):
    seen = _capture(monkeypatch)
    asyncio.run(engage.limits(account_ids=[]))
    assert seen["path"] == "/v1/limits"
