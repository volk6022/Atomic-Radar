"""Пул клиентов Engage: несколько инстансов и отсутствие залипшего адреса.

Раньше адрес был один на весь Radar, а клиент кэшировался в модульной глобали. Второй
заказчик со своим инстансом подключиться не мог в принципе.

Второй смысл этих тестов — не повторить чужую ошибку. В Engage кэш пула в модульной
глобали привёл к тому, что доставка вебхуков вставала намертво и это никто не замечал:
объект в кэше переставал работать, а его продолжали брать. Поэтому здесь проверяется
не только «пул переиспользуется», но и «смена настроек даёт новый клиент».
"""
from __future__ import annotations

import pytest

from app.services import engage


@pytest.fixture(autouse=True)
def clean_pool():
    """Пул и резолвер — глобальные. Без сброса тесты влияли бы друг на друга."""
    engage._clients.clear()
    engage.set_resolver(None)
    yield
    engage._clients.clear()
    engage.set_resolver(None)


def endpoints(mapping):
    return lambda key: mapping.get(key)


def test_resolver_decides_where_we_go():
    engage.set_resolver(endpoints({
        "vertsanov": engage.Endpoint("vertsanov", "http://engage-a:8103", "key-a"),
    }))
    client = engage._get_client("vertsanov")
    assert str(client.base_url).rstrip("/") == "http://engage-a:8103"
    assert client.headers["X-API-Key"] == "key-a"


def test_two_instances_get_two_clients():
    engage.set_resolver(endpoints({
        "a": engage.Endpoint("a", "http://engage-a:8103", "key-a"),
        "b": engage.Endpoint("b", "http://engage-b:8103", "key-b"),
    }))
    a, b = engage._get_client("a"), engage._get_client("b")
    assert a is not b
    assert str(a.base_url).rstrip("/") == "http://engage-a:8103"
    assert str(b.base_url).rstrip("/") == "http://engage-b:8103"


def test_same_config_reuses_the_client():
    """Клиент httpx держит пул соединений — пересоздавать его на каждый запрос значит
    терять keep-alive."""
    engage.set_resolver(endpoints({
        "a": engage.Endpoint("a", "http://engage-a:8103", "key-a"),
    }))
    assert engage._get_client("a") is engage._get_client("a")


def test_changed_address_gives_a_new_client():
    """Тот самый случай, ради которого кэш ключуется отпечатком настроек: инстанс
    переехал, а запросы продолжали бы уходить по старому адресу."""
    mapping = {"a": engage.Endpoint("a", "http://engage-old:8103", "key-a")}
    engage.set_resolver(endpoints(mapping))
    old = engage._get_client("a")

    mapping["a"] = engage.Endpoint("a", "http://engage-new:8103", "key-a")
    new = engage._get_client("a")

    assert new is not old
    assert str(new.base_url).rstrip("/") == "http://engage-new:8103"


def test_changed_api_key_gives_a_new_client():
    """Ключ отозвали и выдали новый — клиент со старым заголовком будет получать 401
    до перезапуска процесса, и никто не поймёт почему."""
    mapping = {"a": engage.Endpoint("a", "http://engage-a:8103", "old-key")}
    engage.set_resolver(endpoints(mapping))
    old = engage._get_client("a")

    mapping["a"] = engage.Endpoint("a", "http://engage-a:8103", "new-key")
    new = engage._get_client("a")

    assert new is not old
    assert new.headers["X-API-Key"] == "new-key"


def test_unknown_instance_falls_back_to_process_settings():
    """Реестр ещё не поднят или инстанс из него удалён. Молча вернуть None значило бы
    уронить экран флота на ровном месте — берём настройки процесса."""
    engage.set_resolver(endpoints({}))
    client = engage._get_client("нет-такого")
    assert client is not None
    assert str(client.base_url)


def test_no_resolver_at_all_still_works():
    """Путь первого запуска: реестр не установлен, адрес есть только в окружении."""
    client = engage._get_client()
    assert client is not None


async def test_close_empties_the_pool():
    engage.set_resolver(endpoints({
        "a": engage.Endpoint("a", "http://engage-a:8103", "key-a"),
        "b": engage.Endpoint("b", "http://engage-b:8103", "key-b"),
    }))
    engage._get_client("a")
    engage._get_client("b")
    assert len(engage._clients) == 2

    await engage.close()
    assert engage._clients == {}


async def test_sending_is_still_impossible_from_radar():
    """Инвариант, который переезд на пул не должен был сломать: отправка идёт только
    через OutboundGate, и клиент Engage физически не умеет её выполнить."""
    with pytest.raises(ValueError, match="send_message"):
        await engage.action(account_id=1, action="send_message",
                            payload={"peer_id": 1, "text": "x"},
                            webhook_url="http://radar/hook")


async def test_sending_is_impossible_on_any_instance():
    """И на новом инстансе тоже — запрет не должен зависеть от того, куда ходим."""
    engage.set_resolver(endpoints({
        "b": engage.Endpoint("b", "http://engage-b:8103", "key-b"),
    }))
    with pytest.raises(ValueError):
        await engage.action(account_id=1, action="send_message",
                            payload={"peer_id": 1, "text": "x"},
                            webhook_url="http://radar/hook", instance="b")
