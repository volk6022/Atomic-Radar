"""Белый список действий `engage.action` пополнился поиском Discovery.

По CONTRACT-discovery.md §8.2 Радар расширяет закрытый список двумя чтениями —
`get_similar_channels` и `search_public_chats`. Смысл списка при этом не меняется:
из Radar возможны только чтения и подписка на канал, `send_message` по-прежнему
невозможен физически, потому что отправка идёт через `OutboundGate`.

Сеть в этих тестах не нужна: проверка списка стоит в `action` ДО обращения к
HTTP, поэтому подменённый клиент служит здесь только доказательством, что
проверка пройдена и дело дошло до постановки задачи.
"""
from __future__ import annotations

import pytest

from app.services import engage


class _StubResponse:
    """Достаточно `action`: он смотрит только `status_code` и `json()`."""

    status_code = 200

    def json(self) -> dict:
        return {"task_id": "t-1"}


class _StubClient:
    def __init__(self) -> None:
        self.actions: list[str] = []

    async def post(self, path: str, json: dict) -> _StubResponse:
        self.actions.append(json["action"])
        return _StubResponse()


@pytest.fixture
def stub_client(monkeypatch):
    """Подмена клиента на уровне `_get_client`: попадание в `post` означает,
    что белый список пропустил действие, а реального сетевого хода нет."""
    client = _StubClient()
    monkeypatch.setattr(engage, "_get_client", lambda instance=None: client)
    return client


async def test_discovery_search_actions_pass_the_whitelist(stub_client):
    """Пункт 1: новые действия поиска больше не вызывают ValueError — проверка
    списка проходима, задача доходит до постановки в Engage (стаб, не сеть)."""
    for name in ("get_similar_channels", "search_public_chats"):
        await engage.action(account_id=1, action=name, payload={"query": "x"},
                            webhook_url="http://radar/hook")
    assert stub_client.actions == ["get_similar_channels", "search_public_chats"]


async def test_send_message_is_still_rejected_with_the_listing():
    """Пункт 2: постороннее действие отвергается белым списком, и текст ошибки
    перечисляет разрешённые имена — оператору не нужно лезть в код за списком."""
    with pytest.raises(ValueError) as exc:
        await engage.action(account_id=1, action="send_message",
                            payload={"peer_id": 1, "text": "x"},
                            webhook_url="http://radar/hook")
    text = str(exc.value)
    assert "send_message" in text
    for name in ("get_chat_info", "get_chat_history", "get_dialogs", "join_group",
                 "get_similar_channels", "search_public_chats"):
        assert name in text


async def test_whitelist_is_exactly_eight_reads():
    """Пункт 3: в списке ровно восемь имён, и среди них нет ни одного пишущего.

    Сам список — локальная переменная внутри `action`; наружу он виден только
    перечислением в тексте ошибки, поэтому достаём его оттуда. Равенство
    эталонному набору чтений (плюс документированное исключение `join_group`)
    одновременно исключает и любое пишущее имя.
    """
    with pytest.raises(ValueError) as exc:
        await engage.action(account_id=1, action="send_message",
                            payload={}, webhook_url="http://radar/hook")
    listing = str(exc.value).split("(")[1].split(")")[0]
    allowed = {name.strip() for name in listing.split(",")}
    assert allowed == {
        "get_chat_info", "get_chat_history", "get_chat_admins",
        "resolve_username", "get_dialogs", "join_group",
        "get_similar_channels", "search_public_chats",
    }
