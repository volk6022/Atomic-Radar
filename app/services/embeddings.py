"""Клиент домашнего эмбеддера (bge-m3) и кэш эталонных векторов.

Модель живёт на машине Ивана и выставлена наружу туннелем tuna. Отсюда два решения:

* **Долгий таймаут, в отличие от Engage.** Экран флота не должен ждать — эмбеддинги
  считает фоновый проход, и «медленно» для него не то же самое, что «сломано».
* **Недоступность не равна отказу.** Если эмбеддер не ответил, ступень L2 обязана
  сказать «не запускалась», а не «не прошло»: разница между «мы проверили и отсеяли»
  и «мы не смогли проверить» — это разница между лидом, которого нет, и лидом,
  которого мы не увидели.

Эталоны считаются один раз за процесс: их полсотни, они не меняются между вызовами,
и пересчитывать их на каждую пачку означало бы утроить трафик через туннель.
"""
from __future__ import annotations

import logging
import math

import httpx

from app.core import prototypes
from app.core.config import get_settings

logger = logging.getLogger(__name__)

_client: httpx.AsyncClient | None = None
_prototype_cache: list[tuple[str, str, list[float]]] | None = None


class EmbeddingsUnavailable(RuntimeError):
    """Эмбеддер не ответил. Текст уходит в объяснение ступени как есть."""


def enabled() -> bool:
    return bool(get_settings().EMBED_BASE_URL)


def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        s = get_settings()
        if not s.EMBED_BASE_URL:
            raise EmbeddingsUnavailable("адрес эмбеддера не задан (RADAR_EMBED_BASE_URL)")
        _client = httpx.AsyncClient(base_url=s.EMBED_BASE_URL.rstrip("/"),
                                    timeout=httpx.Timeout(s.EMBED_TIMEOUT, connect=10.0))
    return _client


async def close() -> None:
    global _client, _prototype_cache
    if _client is not None:
        await _client.aclose()
        _client = None
    _prototype_cache = None


async def ping() -> str:
    """Статус для плитки сервисов на дашборде: `ok` | `off` | текст ошибки.

    Отдельный короткий таймаут, а не общий: дашборд не должен ждать две минуты,
    даже если пачка эмбеддингов столько живёт по праву.
    """
    if not enabled():
        return "off"
    try:
        r = await _get_client().get("/health", timeout=httpx.Timeout(4.0, connect=3.0))
    except httpx.HTTPError as e:
        return f"нет связи: {type(e).__name__}"
    return "ok" if r.status_code < 400 else f"ответ {r.status_code}"


# Сколько символов текста уходит в эмбеддер. Ограничение не наше, а сервера: bge-m3
# поднят с физической пачкой в 512 токенов, и текст длиннее её llama.cpp не берёт
# вовсе — отвечает 500 на весь запрос. 28.08 на этом оборвалась переклассификация
# всего прода: одно сообщение на 535 токенов уронило прогон по 16 тысячам.
# Тысяча символов русского — это 350–450 токенов, то есть с запасом. Для ближайшего
# эталона хвост длинного сообщения всё равно ничего не решает: тема видна по началу.
EMBED_TEXT_LIMIT = 1000


async def embed(texts: list[str]) -> list[list[float]]:
    """Векторы для списка текстов. Порядок ответа совпадает с порядком запроса."""
    if not texts:
        return []
    s = get_settings()
    texts = [t[:EMBED_TEXT_LIMIT] for t in texts]
    out: list[list[float]] = []
    for start in range(0, len(texts), s.EMBED_BATCH):
        chunk = texts[start:start + s.EMBED_BATCH]
        try:
            r = await _get_client().post("/v1/embeddings",
                                         json={"model": s.EMBED_MODEL, "input": chunk})
        except httpx.HTTPError as e:
            raise EmbeddingsUnavailable(f"эмбеддер недоступен: {type(e).__name__}") from e
        if r.status_code >= 400:
            raise EmbeddingsUnavailable(f"эмбеддер ответил {r.status_code}: {r.text[:200]}")
        data = r.json().get("data") or []
        if len(data) != len(chunk):
            raise EmbeddingsUnavailable(
                f"эмбеддер вернул {len(data)} векторов на {len(chunk)} текстов")
        # Ответ OpenAI-совместимый, но порядок гарантируется полем index, а не позицией.
        out.extend(item["embedding"] for item in sorted(data, key=lambda d: d.get("index", 0)))
    return out


async def prototype_vectors() -> list[tuple[str, str, list[float]]]:
    """`(kind, label, вектор)` для всех эталонов. Считается один раз за процесс —
    или один раз за версию таксономии, если её подменил `cascade_registry`
    (`set_prototype_cache`) векторами, уже посчитанными и сохранёнными в базе.
    """
    global _prototype_cache
    if _prototype_cache is None:
        protos = prototypes.all_prototypes()
        vectors = await embed([text for _, _, text in protos])
        _prototype_cache = [(kind, label, vec)
                            for (kind, label, _), vec in zip(protos, vectors)]
        logger.info("prototypes_embedded n=%s dim=%s",
                    len(_prototype_cache), len(_prototype_cache[0][2]))
    return _prototype_cache


def set_prototype_cache(vectors: list[tuple[str, str, list[float]]]) -> None:
    """Подставить уже посчитанные векторы эталонов, минуя обращение к эмбеддеру.

    Зовёт `cascade_registry` при перечитке активной версии таксономии: векторы
    для неё уже лежат в `l2_prototypes.vector`, посчитанные один раз при сохранении
    правки. Пересчитывать их заново на каждый рестарт процесса значило бы держать
    старт в зависимости от того, жив ли сейчас туннель до bge-m3 (см. FIXES.md #1) —
    а он падал уже на два часа.
    """
    global _prototype_cache
    _prototype_cache = vectors


def reset_prototype_cache() -> None:
    """Сбросить кэш эталонов без закрытия HTTP-клиента.

    В отличие от `close()`: правка таксономии не должна рвать соединение к
    эмбеддеру, которое, возможно, прямо сейчас считает эмбеддинги для соседнего
    запроса. Следующий `prototype_vectors()` пересчитает кэш по актуальным данным.
    """
    global _prototype_cache
    _prototype_cache = None


def cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    return dot / (na * nb) if na and nb else 0.0


def rank(vector: list[float],
         protos: list[tuple[str, str, list[float]]]) -> list[tuple[str, str, float]]:
    """Эталоны по убыванию близости: `(kind, label, similarity)`.

    Схлопываем до лучшего представителя каждого класса: у «VPN не работает» пять
    формулировок, и без этого весь верх списка занимал бы один класс, а сравнивать
    нужно классы между собой.
    """
    best: dict[tuple[str, str], float] = {}
    for kind, label, proto in protos:
        sim = cosine(vector, proto)
        key = (kind, label)
        if sim > best.get(key, -1.0):
            best[key] = sim
    return sorted(((kind, name, s) for (kind, name), s in best.items()),
                  key=lambda t: t[2], reverse=True)
