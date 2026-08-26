"""Очередь: выключенное состояние, сброс пула при сбое, ключ повторной доставки.

Redis для этих тестов не нужен и не должен быть нужен. Проверяется не то, что arq
умеет разговаривать с Redis (это его забота), а три наших решения, каждое из которых
уже было чьей-то дорогой ошибкой.
"""
from __future__ import annotations

import os

import pytest

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")

from app.api.v1.ingest import _event_id  # noqa: E402
from app.services import queue  # noqa: E402


class _Settings:
    def __init__(self, url: str) -> None:
        self.REDIS_URL = url


@pytest.fixture(autouse=True)
def _clean_pool():
    """Пул — модульная глобаль; тест, оставивший его за собой, врёт следующему."""
    queue._pool = None
    yield
    queue._pool = None


def _on(monkeypatch, url: str = "redis://localhost:6379/0") -> None:
    monkeypatch.setattr(queue, "get_settings", lambda: _Settings(url))


# ── выключенная ступень ───────────────────────────────────────────────────────

async def test_disabled_queue_says_so_instead_of_pretending(monkeypatch):
    """Пустой адрес — это `QueueDisabled`, а не тихий отказ.

    Разница существенная: вызывающий обязан иметь путь без очереди, и он должен
    выбирать его осознанно. Молчаливое «ничего не поставили» выглядело бы как успех.
    """
    _on(monkeypatch, "")
    assert queue.enabled() is False
    with pytest.raises(queue.QueueDisabled):
        await queue.enqueue(queue.INGEST_EVENT, {}, {})


async def test_disabled_queue_pings_off_not_broken(monkeypatch):
    """`off` и ошибка — разные состояния. Плитка сервисов не должна гореть красным
    там, где ступень просто не включали."""
    _on(monkeypatch, "")
    assert await queue.ping() == "off"


# ── сброс пула при сбое: тот самый урок из Engage ─────────────────────────────

class _DeadPool:
    """Пул, доживший до сбоя соединения. Именно такой оставался в кэше у Engage."""

    def __init__(self) -> None:
        self.closed = False

    async def enqueue_job(self, *a, **k):
        raise ConnectionError("соединение с Redis закрыто")

    async def ping(self):
        raise ConnectionError("соединение с Redis закрыто")

    async def aclose(self):
        self.closed = True


async def test_failed_enqueue_drops_the_pool(monkeypatch):
    """Сбой постановки роняет кэш пула — иначе он битым переживёт восстановление.

    В Engage этого не было, и последствие оказалось не «одна задача не поставилась»,
    а «доставка вебхуков встала целиком и молча»: каждая следующая попытка падала об
    тот же мёртвый пул, строки копились в `pending`, снаружи всё выглядело живым.
    """
    _on(monkeypatch)
    dead = _DeadPool()
    queue._pool = dead

    with pytest.raises(queue.QueueUnavailable):
        await queue.enqueue(queue.INGEST_EVENT, {}, {})

    assert queue._pool is None, "битый пул остался в кэше — ровно ошибка Engage"
    assert dead.closed, "соединение не отпущено"


async def test_failed_ping_drops_the_pool_too(monkeypatch):
    """Проверка состояния ходит тем же пулом, значит и ронять его обязана тем же
    способом. Иначе дашборд, опрошенный первым, оставляет за собой мёртвый кэш."""
    _on(monkeypatch)
    queue._pool = _DeadPool()

    assert await queue.ping() != "ok"
    assert queue._pool is None


async def test_duplicate_job_is_not_a_failure(monkeypatch):
    """arq отдаёт `None`, когда работа с таким ключом уже стоит. Это успех.

    Повторная доставка одного вебхука — штатное поведение Engage при неудачном ответе,
    и превращать её в ошибку значило бы будить оператора на ровном месте.
    """
    class _Pool:
        async def enqueue_job(self, *a, **k):
            return None

        async def aclose(self):
            pass

    _on(monkeypatch)
    queue._pool = _Pool()
    assert await queue.enqueue(queue.INGEST_EVENT, {}, {}) == ""
    assert queue._pool is not None, "успешная постановка пул не роняет"


# ── ключ повторной доставки ───────────────────────────────────────────────────

BODY = {"event": "incoming_message", "message": {"id": 7, "text": "привет"}}


def test_event_id_is_stable_across_key_order():
    """Один и тот же вебхук обязан дать один и тот же ключ независимо от порядка
    полей в JSON — иначе повтор разберётся вторым разом."""
    a = _event_id({"event": "x", "a": 1, "b": 2}, {"kind": "history", "peer_id": "5"})
    b = _event_id({"b": 2, "event": "x", "a": 1}, {"peer_id": "5", "kind": "history"})
    assert a == b


def test_event_id_separates_different_events():
    """Разное содержимое — разные ключи. Схлопнись они, второе событие молча
    исчезло бы: arq считал бы его уже поставленным."""
    assert _event_id(BODY, {}) != _event_id({**BODY, "message": {"id": 8}}, {})


def test_event_id_separates_pages_of_one_backfill():
    """Страницы бэкфилла отличаются только параметрами запроса, тело у них похожее.
    Корреляция вся живёт в query — значит и в ключе она обязана быть."""
    body = {"event": "task_complete", "result": {"posts": []}}
    assert _event_id(body, {"kind": "history", "max_id": "100"}) != \
           _event_id(body, {"kind": "history", "max_id": "200"})
