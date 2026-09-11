"""Ручки адресного обхода L1 — PATCH канала и превью цены (T15, T16).

Контракт каскада §5.1/§5.2: включение обхода — действие владельца с записью в
журнал аудита, а цена включения видна ДО включения. Проверяется через HTTP на
настоящем Postgres, потому что смысл этих ручек — в правах, кодах ответов и в
том, что легло в базу: колонку канала и строку журнала ответ ручки не доказывает.

Отдельный файл, а не `test_channels_filters_db.py` (там фильтры чтения) и не
`test_l1_bypass_db.py` (дом сервисной волны B): HTTP-проверки ручек каналов —
здесь, по образцу соседних `test_*_api_db.py`.

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

from app.core.config import get_settings  # noqa: E402
from app.core.security import SessionSigner  # noqa: E402
from app.db.models import (AuditLog, Base, Channel,  # noqa: E402
                           EngageInstance, Message, User)
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.main import create_app  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")


async def _seed() -> dict:
    """Канал и трое из штата: владелец, заказчик, разборщик.

    Инстанс Engage нужен не ручкам, а старту приложения: реестр инстансов
    поднимается на старте, и пустой реестр — это несобранное приложение.
    """
    engine = create_async_engine(DB_URL, poolclass=None)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)

    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        db.add(EngageInstance(key="default", client_label="Тестовый",
                              base_url="http://engage.invalid",
                              api_key_env="RADAR_ENGAGE_API_KEY", is_active=True))
        channel = Channel(peer_id=-1001, username="corpostrovokru", title="Канал",
                          ingest_enabled=True)
        db.add(channel)

        users = {}
        for role in ("owner", "customer", "reviewer"):
            u = User(email=f"{role}@local", name=role, initials=role[:2].upper(),
                     role=role, password_hash="!нельзя-войти", totp_secret="X" * 32,
                     totp_confirmed=True, is_active=True)
            db.add(u)
            users[role] = u
        await db.commit()
        out = {"uids": {r: u.id for r, u in users.items()}, "channel": channel.id}
    await engine.dispose()
    return out


@pytest.fixture
def seeded():
    """Посев в собственном цикле событий: живую сессию TestClient'у отдавать
    нельзя — он крутит приложение в своём, а соединение asyncpg привязано к
    тому, где создано."""
    return asyncio.run(_seed())


@pytest.fixture
def client(seeded):
    previous = os.environ.get("RADAR_DATABASE_URL")
    os.environ["RADAR_DATABASE_URL"] = DB_URL
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_session_maker.cache_clear()

    with TestClient(create_app(), raise_server_exceptions=False) as c:
        yield c

    if previous is None:
        os.environ.pop("RADAR_DATABASE_URL", None)
    else:
        os.environ["RADAR_DATABASE_URL"] = previous
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_session_maker.cache_clear()


def _login(client: TestClient, uid: int) -> None:
    token = SessionSigner(get_settings().SECRET_KEY).dumps({"uid": uid, "totp_ok": True})
    client.cookies.set(get_settings().SESSION_COOKIE, token)


def _logout(client: TestClient) -> None:
    client.cookies.set(get_settings().SESSION_COOKIE, "")


def _own_engine():
    """Свой движок под каждый поход в базу: фабрика сессий закеширована и держит
    пул, созданный в цикле событий приложения, — второй `asyncio.run` поверх
    того же пула не падает, а зависает."""
    return create_async_engine(DB_URL, poolclass=None)


def _channel(channel_id: int) -> Channel:
    """Прочитать канал мимо приложения: ответ ручки — не доказательство записи."""

    async def go():
        engine = _own_engine()
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with maker() as db:
            row = (await db.execute(
                select(Channel).where(Channel.id == channel_id))).scalar_one()
        await engine.dispose()
        return row

    return asyncio.run(go())


def _audit_detail(action: str) -> dict | None:
    """Последняя строка журнала с этим действием — её detail целиком."""

    async def go():
        engine = _own_engine()
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with maker() as db:
            row = (await db.execute(
                select(AuditLog).where(AuditLog.action == action)
                .order_by(AuditLog.id.desc()).limit(1))).scalar_one_or_none()
        await engine.dispose()
        return row.detail if row else None

    return asyncio.run(go())


def _add_messages(messages: list[Message]) -> None:
    async def go():
        engine = _own_engine()
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with maker() as db:
            db.add_all(messages)
            await db.commit()
        await engine.dispose()

    asyncio.run(go())


def _message(channel_id: int, tg_id: int, *, text: str | None,
             level: int | None, passed: bool | None) -> Message:
    return Message(channel_id=channel_id, tg_message_id=tg_id,
                   tg_date=datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc),
                   text=text, cascade_level=level, cascade_passed=passed)


# ── T15: выключатель обхода в PATCH канала ────────────────────────────────────

def test_patch_channel_toggles_l1_bypass(client, seeded):
    ch = seeded["channel"]
    url = f"/api/v1/channels/{ch}"

    # Владелец включает обход: колонка ложится в базу, журнал — с from/to.
    _login(client, seeded["uids"]["owner"])
    r = client.patch(url, json={"l1_bypass_enabled": True})
    assert r.status_code == 200, r.text
    assert _channel(ch).l1_bypass_enabled is True
    assert _audit_detail("channel_l1_bypass_changed") == {
        "channel_id": ch, "title": "Канал", "from": False, "to": True}

    # Право прежнее — CHANNEL_ARCHIVE, только владелец: у разборщика раздел
    # читается, а выключатель обхода не нажимается.
    _login(client, seeded["uids"]["reviewer"])
    denied = client.patch(url, json={"l1_bypass_enabled": True})
    assert denied.status_code == 403, denied.text

    # extra="forbid" сохраняется: чужое имя поля — отказ, а не молчаливое игнор.
    _login(client, seeded["uids"]["owner"])
    assert client.patch(url, json={"l1_bypass_on": True}).status_code == 422

    # Пустое тело — менять нечего: иначе опечатка в имени поля выглядела бы
    # исполненным «ничего не делать».
    r = client.patch(url, json={})
    assert r.status_code == 422, r.text
    assert "нечего менять" in r.text

    # Запрос с одним ingest_enabled — прежнее поведение слово в слово:
    # отслеживание снимается, флаг обхода не трогается, журнал — прежний.
    r = client.patch(url, json={"ingest_enabled": False})
    assert r.status_code == 200, r.text
    row = _channel(ch)
    assert row.ingest_enabled is False
    assert row.l1_bypass_enabled is True, "запрос без нового поля тронул обход"
    assert _audit_detail("channel_tracking_changed") == {
        "channel_id": ch, "title": "Канал", "from": True, "to": False}


# ── T16: превью цены до включения ─────────────────────────────────────────────

def test_l1_bypass_preview_counts(client, seeded):
    ch = seeded["channel"]
    _add_messages([
        # два убитых словарём длиннее 200 символов — кандидаты обхода
        _message(ch, 1, text="ведомость банковского контроля не сходится "
                             "с декларацией, " + "а" * 180, level=1, passed=False),
        _message(ch, 2, text="б" * 250, level=1, passed=False),
        # убитое словарём короткое — в кандидаты не попадает
        _message(ch, 3, text="ведомость банковского контроля", level=1, passed=False),
        # прошедшее и ожидающее — мимо обоих счётчиков
        _message(ch, 4, text="не могу оплатить инвойс, помогите пожалуйста",
                 level=3, passed=True),
        _message(ch, 5, text="б" * 250, level=2, passed=None),
    ])

    _login(client, seeded["uids"]["owner"])
    r = client.get(f"/api/v1/channels/{ch}/l1-bypass-preview")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["killed_by_l1"] == 3, body
    assert body["killed_long"] == 2, body
    assert body["expected_l3_questions_max"] == 2, body
    assert body["expected_minutes_max"] == 1, body  # ceil(2 × 1,27 / 60)
    assert body["pos_min"] == 0.57, body

    # Чтение — как у списка каналов: разборщику цифры видны, анониму — нет.
    _login(client, seeded["uids"]["reviewer"])
    assert client.get(f"/api/v1/channels/{ch}/l1-bypass-preview").status_code == 200
    _logout(client)
    assert client.get(f"/api/v1/channels/{ch}/l1-bypass-preview").status_code == 401
