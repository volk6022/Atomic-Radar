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
