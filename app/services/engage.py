"""Клиент Engage — единственное место, откуда Radar узнаёт про флот.

Engage держит аккаунты, прокси и лимиты; Radar ходит в него как обычный клиент по
API-ключу. Здесь только чтение: всё, что отправляет сообщения, обязано идти через
`OutboundGate`, и отдельная дырка в обход него не появляется даже случайно.

Три решения, которые видно в коде:

* **Короткий таймаут.** Экран флота не должен зависать вместе с Engage. Пять секунд —
  это «сервис жив, но задумался»; всё, что дольше, для интерфейса неотличимо от отказа.
* **Отказ не подменяется заглушкой.** Если Engage недоступен, наверх летит
  `EngageUnavailable`, а ручка отдаёт 503. Показать вместо живых аккаунтов мок —
  худшее, что можно сделать на экране, по которому судят о здоровье флота.
* **Инстансов несколько.** Раньше адрес был один на весь сервис, и второй клиент со
  своим инстансом Engage подключить было физически некуда: каждый заказчик получает
  свой инстанс на своём сервере. Теперь клиенты живут в пуле по ключу инстанса.

Про кэш клиентов. Он ключуется не только именем инстанса, но и его адресом с ключом
API: смена настроек обязана дать новый клиент, а не молча продолжить ходить по старому
адресу. Это не паранойя — в Engage ровно такой кэш пула в модульной глобали привёл к
тому, что доставка вебхуков вставала намертво и никто этого не замечал.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

import httpx

from app.core.config import get_settings

logger = logging.getLogger(__name__)

TIMEOUT = httpx.Timeout(5.0, connect=3.0)

# Ключ инстанса, который используется, когда вызов не указал никакого. Совпадает со
# значением, которым заполняется реестр при первом запуске (см. `engage_registry`).
DEFAULT_INSTANCE = "default"


class EngageUnavailable(RuntimeError):
    """Engage не ответил или ответил ошибкой. Текст уходит оператору как есть."""


@dataclass(frozen=True)
class Endpoint:
    """Куда и с каким ключом ходить. Значение ключа, а не имя переменной окружения:
    разрешение имени в значение — забота реестра, сюда приезжает уже готовое."""
    key: str
    base_url: str
    api_key: str


# {ключ инстанса: (отпечаток настроек, клиент)}
_clients: dict[str, tuple[tuple[str, str], httpx.AsyncClient]] = {}

# Как разрешить ключ инстанса в endpoint. Проставляется на старте приложения
# (`engage_registry.install`), чтобы этот модуль не лез в базу сам.
_resolver = None


def set_resolver(fn) -> None:
    """Задать функцию `key -> Endpoint`. Вызывается один раз при старте."""
    global _resolver
    _resolver = fn


def _resolve(key: str | None) -> Endpoint:
    key = key or DEFAULT_INSTANCE
    if _resolver is not None:
        ep = _resolver(key)
        if ep is not None:
            return ep
    # Реестр ещё не поднят или инстанс в нём не найден — берём настройки процесса.
    # Это путь первого запуска, когда таблица реестра пуста, и он же страховка:
    # молча вернуть None значило бы уронить экран флота на ровном месте.
    s = get_settings()
    return Endpoint(key=key, base_url=s.ENGAGE_BASE_URL, api_key=s.ENGAGE_API_KEY)


def _get_client(instance: str | None = None) -> httpx.AsyncClient:
    ep = _resolve(instance)
    fingerprint = (ep.base_url, ep.api_key)
    cached = _clients.get(ep.key)
    if cached is not None and cached[0] == fingerprint:
        return cached[1]
    if cached is not None:
        # Настройки инстанса изменились. Старого клиента закрыть отсюда нельзя —
        # мы не в асинхронном контексте, — но убрать из пула обязаны, иначе запросы
        # продолжат уходить по прежнему адресу.
        logger.info("engage_endpoint_changed instance=%s", ep.key)
    client = httpx.AsyncClient(
        base_url=ep.base_url.rstrip("/"),
        headers={"X-API-Key": ep.api_key},
        timeout=TIMEOUT,
    )
    _clients[ep.key] = (fingerprint, client)
    return client


async def close() -> None:
    """Закрыть всех клиентов. Вызывается на остановке приложения."""
    global _clients
    for _, client in _clients.values():
        await client.aclose()
    _clients = {}


async def _get(path: str, *, instance: str | None = None) -> dict:
    try:
        r = await _get_client(instance).get(path)
    except httpx.HTTPError as e:
        logger.warning("engage_unreachable instance=%s path=%s error=%s",
                       instance or DEFAULT_INSTANCE, path, e)
        raise EngageUnavailable(f"Engage недоступен: {type(e).__name__}") from e

    if r.status_code >= 400:
        logger.warning("engage_error instance=%s path=%s status=%s",
                       instance or DEFAULT_INSTANCE, path, r.status_code)
        raise EngageUnavailable(f"Engage ответил {r.status_code} на {path}")
    return r.json()


async def list_accounts(*, instance: str | None = None) -> list[dict]:
    """Флот целиком. Engage отдаёт `{count, accounts:[...]}`."""
    return (await _get("/v1/accounts/", instance=instance)).get("accounts", [])


async def safety_config(*, instance: str | None = None) -> dict:
    """Конфиг безопасности: из него берутся дневные потолки прогрева по сценариям."""
    return await _get("/v1/admin/safety", instance=instance)


async def fleet_health(*, instance: str | None = None) -> dict:
    return await _get("/v1/fleet/health", instance=instance)


async def action(*, account_id: int, action: str, payload: dict, webhook_url: str,
                 priority: int = 5, instance: str | None = None) -> dict:
    """Поставить задачу в Engage. Отсюда доступны ТОЛЬКО read-действия.

    Список закрытый и проверяется здесь, а не по договорённости: `send_message` из
    этого клиента невозможен физически, потому что отправка обязана идти через
    `OutboundGate`. Забытая ветка кода, дергающая Engage напрямую, обнулила бы
    условие сухого прогона, и лучше поймать её падением на старте, чем в проде.
    """
    allowed = {"get_chat_info", "get_chat_history", "get_chat_admins",
               "resolve_username", "get_dialogs"}
    if action not in allowed:
        raise ValueError(
            f"действие {action!r} недоступно из Radar: разрешены только чтения "
            f"({', '.join(sorted(allowed))}); отправка идёт через OutboundGate"
        )

    body = {"account_id": account_id, "action": action, "payload": payload,
            "webhook_url": webhook_url, "priority": priority}
    try:
        r = await _get_client(instance).post("/v1/action", json=body)
    except httpx.HTTPError as e:
        logger.warning("engage_action_unreachable instance=%s action=%s error=%s",
                       instance or DEFAULT_INSTANCE, action, e)
        raise EngageUnavailable(f"Engage недоступен: {type(e).__name__}") from e

    if r.status_code >= 400:
        raise EngageUnavailable(f"Engage ответил {r.status_code}: {r.text[:200]}")
    return r.json()
