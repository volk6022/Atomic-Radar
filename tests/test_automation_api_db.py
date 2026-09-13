"""Ручки автоматики и фильтры очереди/прогонов (T-28…T-31).

Экран «Автоматика» и его фильтры проверяются через HTTP на настоящем Postgres,
потому что смысл ручек — в правах, кодах ответов и в том, что легло в базу:
строку журнала аудита и значения `limits` ответ ручки не доказывает.

Посев (блок Е TESTS): строки `limits` со значениями умолчаний; два прогона
`reclassify` — авто (`auto:reclassify`) и ручной (`owner@x`), причём ручной
закончен ПОЗЖЕ авто: `next_at` обязан считаться от последнего завершённого
прогона любого автора (окно бережёт карту независимо от того, кто запускал),
а `last_run` сценария — остаться авто. Прогон `channel_add` без автора
и элемент очереди без `requested_by` досеиваются, чтобы фильтр `manual`
держал NULL (не-`LIKE` в SQL на NULL даёт NULL и старые строки выкинул бы).

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_DEBUG", "true")

from app.core.config import get_settings  # noqa: E402
from app.core.security import SessionSigner  # noqa: E402
from app.db.models import (AuditLog, BackfillItem, Base, Channel,  # noqa: E402
                           EngageInstance, Limit, Run, User)
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.main import create_app  # noqa: E402
from app.services import autoflow  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

AUTO_RESULT = {"created": 3, "kept": 26}


async def _seed() -> dict:
    """Штат из четырёх ролей, прогоны трёх авторов и очередь из трёх источников.

    Инстанс Engage нужен не ручкам, а старту приложения: реестр инстансов
    поднимается на старте, и пустой реестр — это несобранное приложение.
    Строки `limits` сеются значениями умолчаний: `ensure_bootstrap` существующие
    строки не трогает, так что посев и старт приложения не спорят.
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
        users = {}
        for role in ("owner", "customer", "reviewer", "viewer"):
            u = User(email=f"{role}@x", name=role, initials=role[:2].upper(),
                     role=role, password_hash="!нельзя-войти", totp_secret="X" * 32,
                     totp_confirmed=True, is_active=True)
            db.add(u)
            users[role] = u

        channels = {}
        for i, tag in enumerate(("auto", "owner", "anon")):
            ch = Channel(peer_id=-1001 - i, username=f"{tag}-channel",
                         title=f"Канал {tag}")
            db.add(ch)
            channels[tag] = ch
        await db.flush()

        for key, spec in autoflow.LIMIT_SPECS.items():
            db.add(Limit(key=key, value=float(spec["default"]),
                         unit=spec["unit"], description=spec["description"]))

        now = datetime.now(timezone.utc)
        auto_run = Run(name="Переклассификация · недосчитанное · каналы 7",
                       kind="reclassify", created_by="auto:reclassify",
                       status="done", params={"scope": "pending"},
                       result=AUTO_RESULT,
                       started_at=now - timedelta(minutes=12),
                       finished_at=now - timedelta(minutes=10))
        # Ручной закончен позже авто (см. докстринг модуля).
        manual_run = Run(name="Переклассификация · всё", kind="reclassify",
                         created_by="owner@x", status="done",
                         params={"scope": "all"}, result={"created": 1},
                         started_at=now - timedelta(minutes=5),
                         finished_at=now - timedelta(minutes=2))
        # Прогон без автора вовсе: ручное — не только «чужой email», но и NULL.
        anon_run = Run(name="Подключение канала · @anon-channel",
                       kind="channel_add", created_by=None, status="done",
                       started_at=now - timedelta(minutes=40),
                       finished_at=now - timedelta(minutes=30))
        db.add_all([auto_run, manual_run, anon_run])

        db.add_all([
            BackfillItem(channel_id=channels["auto"].id, state="queued",
                         position=1, requested_by="auto:join"),
            BackfillItem(channel_id=channels["owner"].id, state="queued",
                         position=2, requested_by="owner@x"),
            BackfillItem(channel_id=channels["anon"].id, state="queued",
                         position=3, requested_by=None),
        ])
        await db.commit()
        out = {"uids": {r: u.id for r, u in users.items()},
               "auto_run": auto_run.id,
               "manual_finished": manual_run.finished_at}
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


def _limit_row(key: str) -> Limit | None:
    """Строка `limits` мимо приложения: ответ ручки — не доказательство записи."""

    async def go():
        engine = _own_engine()
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with maker() as db:
            row = (await db.execute(
                select(Limit).where(Limit.key == key))).scalar_one_or_none()
        await engine.dispose()
        return row

    return asyncio.run(go())


def _expected_defaults() -> dict:
    return {k: spec["default"] for k, spec in autoflow.LIMIT_SPECS.items()}


# ── T-28: сводка GET /api/v1/automation ───────────────────────────────────────

def test_get_automation_summary(client, seeded):
    # без cookies — 401
    _logout(client)
    assert client.get("/api/v1/automation").status_code == 401

    # раздел runs — штат: reviewer читает, viewer нет
    _login(client, seeded["uids"]["viewer"])
    assert client.get("/api/v1/automation").status_code == 403
    _login(client, seeded["uids"]["reviewer"])
    assert client.get("/api/v1/automation").status_code == 200

    _login(client, seeded["uids"]["owner"])
    r = client.get("/api/v1/automation")
    assert r.status_code == 200, r.text
    body = r.json()

    # настройки: все девять ключей, значения — посев (умолчания)
    assert body["settings"] == _expected_defaults()

    # сценарии — ровно четыре, объектом по ключам (решение ревью п. 1)
    assert set(body["scenarios"]) == {"join_backfill", "reclassify", "autoscan",
                                      "autoapprove"}

    # last_run — форма строки GET /runs (решение ревью п. 2): result, не summary
    last_run = body["scenarios"]["reclassify"]["last_run"]
    assert last_run["id"] == seeded["auto_run"]
    assert last_run["result"] == AUTO_RESULT
    assert "summary" not in last_run

    # next_at — от последнего завершённого прогона ЛЮБОГО автора: ручной
    # закончен позже авто, и окно обязано считаться от него
    expected_next = (seeded["manual_finished"]
                     + timedelta(minutes=60)).isoformat()
    assert body["scenarios"]["reclassify"]["next_at"] == expected_next

    # сценарий 1 прогонов не заводит; в очереди стоит одна авто-постановка
    assert body["scenarios"]["join_backfill"]["last_run"] is None
    assert body["scenarios"]["join_backfill"]["queue_standing_auto"] == 1


# ── T-29: POST /api/v1/automation/settings ────────────────────────────────────

def test_post_automation_settings(client, seeded):
    _login(client, seeded["uids"]["owner"])
    r = client.post("/api/v1/automation/settings",
                    json={"autoflow_reclassify_enabled": 1,
                          "autoflow_reclassify_interval_min": 30})
    assert r.status_code == 200, r.text
    settings = r.json()["settings"]
    # переданное — сохранено, остальные семь — не тронуты
    assert settings["autoflow_reclassify_enabled"] == 1
    assert settings["autoflow_reclassify_interval_min"] == 30
    assert settings == {**_expected_defaults(),
                        "autoflow_reclassify_enabled": 1,
                        "autoflow_reclassify_interval_min": 30}
    # строка `limits` действительно перезаписана, а не только ответ
    assert int(_limit_row("autoflow_reclassify_enabled").value) == 1

    # журнал: из чего → во что, какие ключи
    detail = _audit_detail("automation_settings_saved")
    assert detail["from"]["autoflow_reclassify_enabled"] == 0
    assert detail["to"]["autoflow_reclassify_enabled"] == 1
    assert set(detail["keys"]) == {"autoflow_reclassify_enabled",
                                   "autoflow_reclassify_interval_min"}

    # CONFIG_EDIT — только владелец
    _login(client, seeded["uids"]["customer"])
    assert client.post("/api/v1/automation/settings",
                       json={"autoflow_reclassify_enabled": 1}).status_code == 403
    _login(client, seeded["uids"]["reviewer"])
    assert client.post("/api/v1/automation/settings",
                       json={"autoflow_reclassify_enabled": 1}).status_code == 403

    # кривые тела — 422, и ничего не записано
    _login(client, seeded["uids"]["owner"])
    r = client.post("/api/v1/automation/settings", json={"bogus_key": 1})
    assert r.status_code == 422, r.text
    # перечень известных обязателен (иначе опечатку не исправить)
    assert "известны" in r.text
    assert "autoflow_join_backfill_enabled" in r.text

    assert client.post("/api/v1/automation/settings",
                       json={"autoflow_backfill_depth_days": 31}).status_code == 422
    r = client.post("/api/v1/automation/settings", json={})
    assert r.status_code == 422, r.text
    assert "нечего менять" in r.text
    # bool — не число: в lax-режиме pydantic превратил бы true в 1
    assert client.post("/api/v1/automation/settings",
                       json={"autoflow_reclassify_enabled": True}).status_code == 422

    assert _limit_row("bogus_key") is None
    assert int(_limit_row("autoflow_backfill_depth_days").value) == 30


# ── T-30: фильтр источника в очереди дочитывания ─────────────────────────────

def test_backfill_queue_source_filter(client, seeded):
    _login(client, seeded["uids"]["owner"])
    base = client.get("/api/v1/backfill/queue")
    assert base.status_code == 200, base.text
    base = base.json()
    assert base["total"] == 3
    assert set(base) >= {"items", "summary"}

    r = client.get("/api/v1/backfill/queue", params={"source": "auto"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["total"] == 1
    assert [i["requested_by"] for i in body["items"]] == ["auto:join"]
    # сводка — по всей очереди, фильтру не подчиняется (как у state)
    assert body["summary"] == base["summary"]

    # manual — всё, что не auto:*, включая строку без автора вовсе
    r = client.get("/api/v1/backfill/queue", params={"source": "manual"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["total"] == 2
    assert {i["requested_by"] for i in body["items"]} == {"owner@x", None}

    # источник — бинарный признак: чужое значение — отказ, не пустая страница
    r = client.get("/api/v1/backfill/queue", params={"source": "bogus"})
    assert r.status_code == 422, r.text
    assert "auto, manual" in r.text

    # без параметра — все строки, форма ответа не изменилась
    assert base["total"] == 3 and "summary" in base


# ── T-31: фильтр автора в списке прогонов ─────────────────────────────────────

def test_runs_author_filter(client, seeded):
    _login(client, seeded["uids"]["owner"])
    base = client.get("/api/v1/runs")
    assert base.status_code == 200, base.text
    base = base.json()
    assert base["total"] == 3
    assert all("created_by" in row for row in base["rows"])

    r = client.get("/api/v1/runs", params={"author": "auto"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["total"] == 1
    assert body["rows"][0]["created_by"] == "auto:reclassify"

    # manual — всё, что не auto:*, включая прогон без автора вовсе
    r = client.get("/api/v1/runs", params={"author": "manual"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["total"] == 2
    assert {row["created_by"] for row in body["rows"]} == {"owner@x", None}

    # конкретный автор — точное совпадение
    r = client.get("/api/v1/runs", params={"author": "owner@x"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["total"] == 1
    assert body["rows"][0]["created_by"] == "owner@x"

    # авторы — не закрытый список: чужой адрес — честная пустая страница
    r = client.get("/api/v1/runs", params={"author": "ghost@x"})
    assert r.status_code == 200, r.text
    assert r.json()["total"] == 0

    # без параметра — без фильтра
    assert base["total"] == 3
