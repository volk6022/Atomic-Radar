"""Клиент Engage — единственное место, откуда Radar узнаёт про флот.

Engage держит аккаунты, прокси и лимиты; Radar ходит в него как обычный клиент по
API-ключу. Здесь только чтение: всё, что отправляет сообщения, обязано идти через
`OutboundGate`, и отдельная дырка в обход него не появляется даже случайно.

Два решения, которые видно в коде:

* **Короткий таймаут.** Экран флота не должен зависать вместе с Engage. Пять секунд —
  это «сервис жив, но задумался»; всё, что дольше, для интерфейса неотличимо от отказа.
* **Отказ не подменяется заглушкой.** Если Engage недоступен, наверх летит
  `EngageUnavailable`, а ручка отдаёт 503. Показать вместо живых аккаунтов мок —
  худшее, что можно сделать на экране, по которому судят о здоровье флота.
"""
from __future__ import annotations

import logging

import httpx

from app.core.config import get_settings

logger = logging.getLogger(__name__)

TIMEOUT = httpx.Timeout(5.0, connect=3.0)

_client: httpx.AsyncClient | None = None


class EngageUnavailable(RuntimeError):
    """Engage не ответил или ответил ошибкой. Текст уходит оператору как есть."""


def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        s = get_settings()
        _client = httpx.AsyncClient(
            base_url=s.ENGAGE_BASE_URL.rstrip("/"),
            headers={"X-API-Key": s.ENGAGE_API_KEY},
            timeout=TIMEOUT,
        )
    return _client


async def close() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


async def _get(path: str) -> dict:
    try:
        r = await _get_client().get(path)
    except httpx.HTTPError as e:
        logger.warning("engage_unreachable path=%s error=%s", path, e)
        raise EngageUnavailable(f"Engage недоступен: {type(e).__name__}") from e

    if r.status_code >= 400:
        logger.warning("engage_error path=%s status=%s", path, r.status_code)
        raise EngageUnavailable(f"Engage ответил {r.status_code} на {path}")
    return r.json()


async def list_accounts() -> list[dict]:
    """Флот целиком. Engage отдаёт `{count, accounts:[...]}`."""
    return (await _get("/v1/accounts/")).get("accounts", [])


async def safety_config() -> dict:
    """Конфиг безопасности: из него берутся дневные потолки прогрева по сценариям."""
    return await _get("/v1/admin/safety")


async def fleet_health() -> dict:
    return await _get("/v1/fleet/health")


async def action(*, account_id: int, action: str, payload: dict, webhook_url: str,
                 priority: int = 5) -> dict:
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
        r = await _get_client().post("/v1/action", json=body)
    except httpx.HTTPError as e:
        logger.warning("engage_action_unreachable action=%s error=%s", action, e)
        raise EngageUnavailable(f"Engage недоступен: {type(e).__name__}") from e

    if r.status_code >= 400:
        raise EngageUnavailable(f"Engage ответил {r.status_code}: {r.text[:200]}")
    return r.json()
