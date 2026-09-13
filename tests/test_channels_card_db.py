"""Карточка канала и происхождение в реестре — через HTTP, на настоящем Postgres.

Контракт автоматики §4.4 (волна Е2): в строках `GET /api/v1/screens/channels`
появляются `source` и `discovery_seed`, рядом — новое `GET /api/v1/channels/{id}`
для drill. Проверяется через HTTP на настоящем Postgres, потому что смысл этих
ручек — в правах, кодах ответов и в вычислении над кандидатами: форму `source`
ни ответ списка, ни сериализация модели не доказывают.

Отдельный файл, а не `test_channels_filters_db.py` (фильтры чтения) и не
`test_l1_bypass_api_db.py` (другая волна): HTTP-проверки волны Е2 — здесь,
по образцу соседних `test_*_api_db.py`.

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
"""
from __future__ import annotations

import asyncio
import os

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_DEBUG", "true")

from app.core.config import get_settings  # noqa: E402
from app.core.security import SessionSigner  # noqa: E402
from app.db.models import (AuditLog, Base, Channel, ChannelCandidate,  # noqa: E402
                           EngageInstance, User)
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.main import create_app  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")


async def _seed() -> dict:
    """Четыре канала на все значения `source` и трое из штата.

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
        disc = Channel(peer_id=-1001, username="disc", title="Донор подбора",
                       ingest_enabled=True, discovery_seed=True)
        man = Channel(peer_id=-1002, username="man", title="Ручной",
                      subscribed_by="owner@x")
        auto = Channel(peer_id=-1003, username="auto", title="Автовступление",
                       subscribed_by="auto:approve")
        org = Channel(peer_id=-1004, username="org", title="Самозаведённый")
        db.add_all([disc, man, auto, org])
        await db.flush()
        # Кандидат, дошедший до подключения: имя в посеве — в том регистре, в
        # каком его записал бы поиск; сравнение на экране обязано быть без
        # регистра (§4.4), поэтому и проверяем именно это.
        db.add(ChannelCandidate(username="disc", title=disc.title,
                                source="similar", found_by_account_id=12,
                                decision="connected", seed_channel_id=disc.id))

        users = {}
        for role in ("owner", "customer", "viewer"):
            u = User(email=f"{role}@local", name=role, initials=role[:2].upper(),
                     role=role, password_hash="!нельзя-войти", totp_secret="X" * 32,
                     totp_confirmed=True, is_active=True)
            db.add(u)
            users[role] = u
        await db.commit()
        out = {"uids": {r: u.id for r, u in users.items()},
               "disc": disc.id, "man": man.id, "auto": auto.id, "org": org.id}
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


def _own_engine():
    """Свой движок под каждый поход в базу: фабрика сессий закеширована и держит
    пул, созданный в цикле событий приложения, — второй `asyncio.run` поверх
    того же пула не падает, а зависает."""
    return create_async_engine(DB_URL, poolclass=None)


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


# ── T-32: source и discovery_seed в строках реестра ──────────────────────────

def test_channels_source_and_seed_fields(client, seeded):
    _login(client, seeded["uids"]["owner"])
    r = client.get("/api/v1/channels")
    assert r.status_code == 200, r.text
    body = r.json()
    # Остальная форма не меняется: конверт пагинации и белый список сортировок.
    assert body["total"] == 4, body
    assert set(body) >= {"total", "rows", "sorts"}

    rows = {row["username"]: row for row in body["rows"]}
    # Кандидат в «connected» сильнее всего: найденный подбором канал остаётся
    # «discovery», даже когда у него стоит флаг донора.
    assert rows["disc"]["source"] == "discovery"
    assert rows["disc"]["discovery_seed"] is True
    # Человек подключил через POST /channels — «manual»; цепочка автоматики
    # («auto:…») — «join»; строка, заведённая сама при первом сообщении, — null.
    assert rows["man"]["source"] == "manual"
    assert rows["man"]["discovery_seed"] is False
    assert rows["auto"]["source"] == "join"
    assert rows["org"]["source"] is None


# ── T-33: карточка канала и PATCH discovery_seed ─────────────────────────────

def test_channel_card_and_patch_seed(client, seeded):
    ch = seeded["disc"]
    url = f"/api/v1/channels/{ch}"

    # Владелец читает карточку: состав тела — дословно §4.4, форма источника
    # считается так же, как в реестре.
    _login(client, seeded["uids"]["owner"])
    r = client.get(url)
    assert r.status_code == 200, r.text
    body = r.json()
    assert set(body) == {"id", "title", "username", "chat_type", "members",
                         "ingest_enabled", "l1_bypass_enabled", "discovery_seed",
                         "source", "linked_chat_username", "subscribed_by",
                         "created_at"}, body
    assert body["id"] == ch
    assert body["username"] == "disc"
    assert body["discovery_seed"] is True
    assert body["source"] == "discovery"
    assert body["created_at"] is not None

    # Несуществующий канал — 404, а не 500 и не пустая карточка.
    assert client.get("/api/v1/channels/999999").status_code == 404

    # Раздел «channels» — штат: зритель в него не пускается вовсе.
    _login(client, seeded["uids"]["viewer"])
    assert client.get(url).status_code == 403

    # PATCH: флаг донора — право попроще (CHANNEL_EDIT, владелец и заказчик),
    # запись в журнале — с from/to.
    _login(client, seeded["uids"]["customer"])
    r = client.patch(url, json={"discovery_seed": False})
    assert r.status_code == 200, r.text
    assert r.json()["discovery_seed"] is False
    assert _audit_detail("channel_discovery_seed_changed") == {
        "channel_id": ch, "title": "Донор подбора", "from": True, "to": False}

    _login(client, seeded["uids"]["viewer"])
    assert client.patch(url, json={"discovery_seed": True}).status_code == 403

    # Отслеживание — прежнее право (CHANNEL_ARCHIVE, только владелец):
    # заказчик по-прежнему не может снять мониторинг канала.
    _login(client, seeded["uids"]["customer"])
    denied = client.patch(url, json={"ingest_enabled": False})
    assert denied.status_code == 403, denied.text
    _login(client, seeded["uids"]["owner"])
    assert client.patch(url, json={"ingest_enabled": False}).status_code == 200

    # Пустое тело — менять нечего: волна Д сохранила отказ слово в слово.
    r = client.patch(url, json={})
    assert r.status_code == 422, r.text
    assert "нечего менять" in r.text
