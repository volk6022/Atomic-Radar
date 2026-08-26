"""Приём вебхуков Engage: очередь включена, выключена и сломана — на живом Postgres.

Ручка приёма — единственная дверь, через которую в систему попадают данные, и до
этого среза она не была покрыта ни одним тестом. Здесь проверяется не разбор события
(его делают сервисы, у них свои тесты), а **развилка**: кто разбирает, что при этом
отвечается наружу и что остаётся в базе.

Три состояния, и все три разные:

* очереди нет — разбор в запросе, ответ `200` с результатом, поведение как до воркеров;
* очередь есть — разбор откладывается, ответ `202`, в базе за время запроса **ничего**;
* очередь есть и не отвечает — `503` и тревога, и снова ничего в базе.

Redis ни в одном случае не поднимается: наружу торчит `queue.enqueue`, его и
подменяем. Проверять здесь работоспособность arq незачем — она проверена его авторами.

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_DEBUG", "true")
os.environ.setdefault("RADAR_INGEST_TOKEN", "test-ingest-token")

from app.core.config import get_settings  # noqa: E402
from app.db.models import Alert, Base, EngageInstance, Message, Workflow  # noqa: E402
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.main import create_app  # noqa: E402
from app.services import queue  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

TOKEN = "test-ingest-token"
NOW = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)

# Конверт вотчера Engage. Поля — те, что читает `ingest_incoming_message`.
MESSAGE = {
    "event": "incoming_message",
    "chat_id": 900100,
    "chat_username": "@stroy_chat",
    "chat_title": "Стройка и подряд",
    "message_id": 4242,
    "message": "Ищу подрядчика на монтаж вентиляции, горит",
    "from_peer_id": 700100,
    "from_first_name": "Пётр",
    "sender_username": "@petr",
    "date": "2026-08-26T12:00:00Z",
}


async def _seed() -> None:
    """Минимум, при котором приём работает: инстанс Engage и один активный сценарий.

    Без сценария `bind_active` вернёт пустой список, сообщение ляжет, а вердиктов не
    появится — и тест «сообщение доехало» проходил бы, ничего не проверив по существу.
    """
    engine = create_async_engine(DB_URL, poolclass=None)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)

    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        inst = EngageInstance(key="test", client_label="Тестовый клиент",
                              base_url="http://engage.invalid",
                              api_key_env="RADAR_ENGAGE_API_KEY", is_active=True)
        db.add(inst)
        await db.flush()
        db.add(Workflow(
            key="cold_dm", title="Личные сообщения", target_kind="user", action="dm",
            visibility="private", engage_instance_id=inst.id, engage_use_case="cold_dm",
            cascade_profile="dm_v1", sort_order=10, is_active=True))
        await db.commit()

    await engine.dispose()


@pytest.fixture(scope="module")
def client():
    os.environ["RADAR_DATABASE_URL"] = DB_URL
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_session_maker.cache_clear()
    asyncio.run(_seed())
    with TestClient(create_app()) as c:
        yield c


@pytest.fixture(autouse=True)
def _clean_pool():
    queue._pool = None
    yield
    queue._pool = None


async def _rows(model) -> list:
    """Смотрим в базу мимо приложения: ответ ручки — не доказательство записи."""
    engine = create_async_engine(DB_URL, poolclass=None)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        out = list((await db.execute(select(model))).scalars().all())
    await engine.dispose()
    return out


def _post(client, body: dict, token: str = TOKEN, **params):
    return client.post(f"/api/v1/ingest/{token}", json=body, params=params)


# ── секрет ────────────────────────────────────────────────────────────────────

def test_wrong_token_is_not_found_not_forbidden(client):
    """`404`, а не `403`: `403` подтвердил бы, что ручка существует, тому, кто
    секрета не знает. Проверка идёт до всего остального — до разбора тела тоже.

    Секрет кириллицей — не придирка, а найденное этим тестом. `hmac.compare_digest`
    на строках требует ASCII с обеих сторон и на чём угодно другом бросает
    `TypeError`, то есть отдаёт `500`. Токен приходит из URL: подставить туда любой
    алфавит может кто угодно, и ответ обязан быть одинаковым — `404` и на «не тот
    секрет», и на «не тот алфавит».
    """
    for wrong in ("не-тот-секрет", "wrong-token", "", "тест✓"):
        r = _post(client, MESSAGE, token=wrong or "-")
        assert r.status_code == 404, f"токен {wrong!r} дал {r.status_code}"


# ── очередь выключена: прежнее поведение целиком ──────────────────────────────

def test_without_a_queue_the_request_does_the_work(client, monkeypatch):
    monkeypatch.setattr(queue, "enabled", lambda: False)

    r = _post(client, MESSAGE)

    assert r.status_code == 200, r.text
    assert r.json()["accepted"] == 1
    saved = asyncio.run(_rows(Message))
    assert [m.tg_message_id for m in saved] == [4242], "сообщение не доехало до базы"


# ── очередь включена: ручка только принимает ──────────────────────────────────

def test_with_a_queue_the_request_only_accepts(client, monkeypatch):
    """`202` и пустая база — ровно то, что значит «принято, но не сделано».

    Проверка «ничего не записано» тут не придирка: если бы ручка и ставила в очередь,
    и разбирала сама, событие разобралось бы дважды, и заметили бы это не скоро —
    разбор идемпотентен и молча стерпел бы второй проход.
    """
    seen: dict = {}

    async def _fake(task, *args, **kwargs):
        seen["task"], seen["args"], seen["kwargs"] = task, args, kwargs
        return "job-1"

    monkeypatch.setattr(queue, "enabled", lambda: True)
    monkeypatch.setattr(queue, "enqueue", _fake)

    before = len(asyncio.run(_rows(Message)))
    r = _post(client, MESSAGE, kind="watcher")

    assert r.status_code == 202, r.text
    assert r.json() == {"queued": "job-1"}
    assert len(asyncio.run(_rows(Message))) == before, "ручка разобрала событие сама"

    assert seen["task"] == queue.INGEST_EVENT
    body, q = seen["args"]
    assert body["message_id"] == 4242
    assert q == {"kind": "watcher"}, "параметры запроса обязаны доехать до воркера"
    assert seen["kwargs"]["_job_id"].startswith("ingest:")


def test_redelivery_reports_duplicate_not_failure(client, monkeypatch):
    """Пустой идентификатор от `enqueue` означает «такая работа уже стоит».
    Ответ обязан остаться успешным: Engage ретраит доставку штатно."""
    monkeypatch.setattr(queue, "enabled", lambda: True)

    async def _dup(task, *args, **kwargs):
        return ""

    monkeypatch.setattr(queue, "enqueue", _dup)
    r = _post(client, MESSAGE)
    assert r.status_code == 202
    assert r.json() == {"queued": "duplicate"}


# ── очередь включена и молчит ─────────────────────────────────────────────────

def test_dead_queue_refuses_and_raises_an_alert(client, monkeypatch):
    """`503` — и это правильный ответ, а не капитуляция.

    Разобрать событие в обход упавшей очереди было бы худшим вариантом: система
    тихо вернулась бы к поведению, от которого уходит, и никто бы не узнал. Отказ
    видно сразу — отправителю кодом, оператору тревогой, — а пропущенное доберётся
    бэкфиллом.
    """
    async def _dead(task, *args, **kwargs):
        raise queue.QueueUnavailable("соединение с Redis закрыто")

    monkeypatch.setattr(queue, "enabled", lambda: True)
    monkeypatch.setattr(queue, "enqueue", _dead)

    before = len(asyncio.run(_rows(Message)))
    r = _post(client, MESSAGE)

    assert r.status_code == 503
    assert len(asyncio.run(_rows(Message))) == before

    alerts = [a for a in asyncio.run(_rows(Alert)) if a.key == "ingest_queue_down"]
    assert alerts, "отказ приёма не поднял тревогу — снаружи он невидим"
    assert alerts[0].severity == "error"


def test_alert_is_refreshed_not_multiplied(client, monkeypatch):
    """Очередь лежит минутами, вебхуки идут потоком. Каждый отказ, заводящий новую
    строку, превратил бы список тревог в журнал и утопил бы в нём всё остальное."""
    async def _dead(task, *args, **kwargs):
        raise queue.QueueUnavailable("соединение с Redis закрыто")

    monkeypatch.setattr(queue, "enabled", lambda: True)
    monkeypatch.setattr(queue, "enqueue", _dead)

    for _ in range(3):
        assert _post(client, MESSAGE).status_code == 503

    alerts = [a for a in asyncio.run(_rows(Alert)) if a.key == "ingest_queue_down"]
    assert len(alerts) == 1, f"тревог накопилось {len(alerts)}"
