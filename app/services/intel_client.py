"""Клиент Intel — единственное место, откуда Radar узнаёт про исследования.

Intel держит задачи LLM; Radar ходит в него как обычный клиент по API-ключу.
Три решения, которые видно в коде:

* **Короткий таймаут.** Экран Intel не должен зависать вместе с Intel.
* **Отказ не подменяется заглушкой.** Если Intel недоступен, наверх летит
  `IntelUnavailable`, а ручка отдаёт 503.
* **Конкурентность и квота.** Лимиты задаются в `IntelKey`, клиент их читает.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass

import httpx

from app.core.config import get_settings

logger = logging.getLogger(__name__)

TIMEOUT = httpx.Timeout(connect=10, read=60, write=30, pool=10)
# Поиск организаций в Яндексе идёт десятки секунд — чтение ждём дольше обычного.
YANDEX_TIMEOUT = httpx.Timeout(connect=10, read=180, write=30, pool=10)
YANDEX_KINDS = ("extract", "card", "reviews")
DEFAULT_KEY = "default"


class IntelNotConfigured(Exception):
    """Intel не настроен: нет ключа или адреса."""


class IntelUnavailable(Exception):
    """Intel не ответил или ответил ошибкой после ретраев."""


class IntelRateLimited(Exception):
    """Intel ограничил запросы."""

    def __init__(self, retry_after: int, scope: str):
        super().__init__(f"Intel rate limited: {scope}, retry_after={retry_after}")
        self.retry_after = retry_after
        self.scope = scope


class IntelNotFound(Exception):
    """Задача Intel не найдена."""


class IntelForbidden(Exception):
    """Intel отклонил запрос: ключ отозван."""


class IntelYandexCaptcha(Exception):
    """Яндекс показал капчу при разборе карт: Intel ответил 503 с «yandex captcha»
    в detail. Отдельное исключение, чтобы экран сказал «Яндекс просит капчу»,
    а не «Intel недоступен»."""


@dataclass(frozen=True)
class Endpoint:
    """Куда и с каким ключом ходить. Значение ключа, а не имя переменной окружения:
    разрешение имени в значение — забота реестра, сюда приезжает уже готовое."""
    key: str
    base_url: str
    api_key: str


# {ключ: (отпечаток настроек, клиент)}
_clients: dict[str, tuple[tuple[str, str], httpx.AsyncClient]] = {}


async def endpoint(db) -> Endpoint:
    """Читает IntelKey(key='default') из БД, разрешает api_key_env в значение."""
    from app.db.models import IntelKey

    from sqlalchemy import select

    row = (await db.execute(
        select(IntelKey).where(IntelKey.key == DEFAULT_KEY))).scalar_one_or_none()
    # Строки ещё нет (первый запуск) — адрес из настроек процесса, ключ из RADAR_INTEL_API_KEY.
    base_url = row.base_url if row is not None else get_settings().INTEL_BASE_URL
    api_key_env = row.api_key_env if row is not None else "RADAR_INTEL_API_KEY"
    api_key = os.environ.get(api_key_env) or os.environ.get("RADAR_INTEL_API_KEY")

    if not api_key:
        raise IntelNotConfigured(f"Intel API key не задан ({api_key_env})")

    return Endpoint(key=DEFAULT_KEY, base_url=base_url, api_key=api_key)


def _get_client(ep: Endpoint) -> httpx.AsyncClient:
    fingerprint = (ep.base_url, ep.api_key)
    cached = _clients.get(ep.key)
    if cached is not None and cached[0] == fingerprint:
        return cached[1]
    if cached is not None:
        logger.info("intel_endpoint_changed key=%s", ep.key)
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


async def healthz(ep: Endpoint) -> bool:
    """GET /intel/healthz без ключа."""
    client = _get_client(ep)
    try:
        r = await client.get("/intel/healthz")
        return r.status_code == 200
    except httpx.HTTPError:
        return False


async def run(ep: Endpoint, *, query: str, mode: str, output_schema: dict | None,
              language: str, max_tokens: int | None = None) -> str:
    """POST /intel/api/v1/research/run → task_id."""
    payload = {
        "query": query,
        "mode": mode,
        "language": language,
    }
    if output_schema is not None:
        payload["output_schema"] = output_schema
    if max_tokens is not None:
        payload["max_tokens"] = max_tokens

    client = _get_client(ep)
    last_exc: Exception | None = None

    for attempt in range(3):
        try:
            r = await client.post("/intel/api/v1/research/run", json=payload)
            _note_ratelimit(r)
            if r.status_code == 429:
                body = r.json()
                raise IntelRateLimited(
                    int(body.get("retry_after", 0)),
                    body.get("scope", "work"),
                )
            if r.status_code == 403:
                raise IntelForbidden("Intel API key revoked")
            if r.status_code >= 500:
                raise httpx.HTTPError(f"Intel {r.status_code}")
            if r.status_code >= 400:
                raise httpx.HTTPError(f"Intel {r.status_code}")
            return r.json()["task_id"]
        except httpx.HTTPError as e:
            last_exc = e
            if attempt < 2:
                await asyncio.sleep([5, 15, 45][attempt])
        except IntelRateLimited:
            raise
        except IntelForbidden:
            raise

    raise IntelUnavailable(f"Intel недоступен: {last_exc}")


def _error_detail(r: httpx.Response) -> str:
    """Текст `detail` из ответа Intel; не-JSON и не-строка — строковым представлением."""
    try:
        body = r.json()
    except ValueError:
        return r.text[:500]
    detail = body.get("detail") if isinstance(body, dict) else None
    if detail is None:
        return r.text[:500]
    return detail if isinstance(detail, str) else json.dumps(detail, ensure_ascii=False)[:500]


async def yandex(ep: Endpoint, *, kind: str, payload: dict) -> dict:
    """POST /intel/api/v1/yandex-maps/{kind} → ответ Intel как есть.

    Отличия от `run()`: длинный таймаут чтения (поиск организаций идёт десятки
    секунд) и 4xx без ретраев — Intel на ту же строку ответит тем же, повторы
    только жгут время синхронной пачки; плохая строка уезжает наверх как
    `ValueError`, ручка показывает её как `bad_request`.
    """
    if kind not in YANDEX_KINDS:
        raise ValueError(f"kind: ожидается {YANDEX_KINDS}")
    client = _get_client(ep)
    last_exc: Exception | None = None

    for attempt in range(3):
        try:
            r = await client.post(f"/intel/api/v1/yandex-maps/{kind}",
                                  json=payload, timeout=YANDEX_TIMEOUT)
            _note_ratelimit(r)
            if r.status_code == 429:
                body = r.json()
                raise IntelRateLimited(
                    int(body.get("retry_after", 0)),
                    body.get("scope", "work"),
                )
            if r.status_code == 403:
                raise IntelForbidden("Intel API key revoked")
            if r.status_code >= 500:
                if r.status_code == 503:
                    detail = _error_detail(r)
                    if "yandex captcha" in detail.lower():
                        raise IntelYandexCaptcha(detail)
                raise httpx.HTTPError(f"Intel {r.status_code}")
            if r.status_code >= 400:
                raise ValueError(f"Intel {r.status_code}: {_error_detail(r)}")
            return r.json()
        except httpx.HTTPError as e:
            last_exc = e
            if attempt < 2:
                await asyncio.sleep([5, 15, 45][attempt])
        except (IntelRateLimited, IntelForbidden, IntelYandexCaptcha, ValueError):
            raise

    raise IntelUnavailable(f"Intel недоступен: {last_exc}")


async def status(ep: Endpoint, task_id: str) -> dict:
    """GET /intel/api/v1/research/status/{task_id}."""
    client = _get_client(ep)
    last_exc: Exception | None = None

    for attempt in range(3):
        try:
            r = await client.get(f"/intel/api/v1/research/status/{task_id}")
            _note_ratelimit(r)
            if r.status_code == 404:
                raise IntelNotFound(f"Intel task {task_id} not found")
            if r.status_code == 429:
                body = r.json()
                raise IntelRateLimited(
                    int(body.get("retry_after", 0)),
                    body.get("scope", "work"),
                )
            if r.status_code == 403:
                raise IntelForbidden("Intel API key revoked")
            if r.status_code >= 500:
                raise httpx.HTTPError(f"Intel {r.status_code}")
            if r.status_code >= 400:
                raise httpx.HTTPError(f"Intel {r.status_code}")
            return r.json()
        except httpx.HTTPError as e:
            last_exc = e
            if attempt < 2:
                await asyncio.sleep([5, 15, 45][attempt])
        except IntelRateLimited:
            raise
        except IntelForbidden:
            raise
        except IntelNotFound:
            raise

    raise IntelUnavailable(f"Intel недоступен: {last_exc}")


def ratelimit_from(headers) -> tuple[int | None, int | None]:
    """Извлечь X-RateLimit-Limit/Remaining из заголовков (httpx отдаёт их без учёта регистра)."""
    limit = headers.get("X-RateLimit-Limit")
    remaining = headers.get("X-RateLimit-Remaining")
    try:
        return (int(limit) if limit else None, int(remaining) if remaining else None)
    except ValueError:
        return (None, None)


# Последний ответ Intel с заголовками лимита: (limit, remaining) или None. Прогон
# переписывает это в `intel_keys.last_ratelimit_*` при каждом удачном опросе — до
# 22.09 помощник выше никто не звал, и колонки на проде оставались пустыми, хотя
# Intel шлёт `x-ratelimit-limit: 1000` в каждом ответе.
last_ratelimit: tuple[int | None, int | None] | None = None


def _note_ratelimit(r) -> None:
    global last_ratelimit
    lim, rem = ratelimit_from(r.headers)
    if lim is not None or rem is not None:
        last_ratelimit = (lim, rem)
