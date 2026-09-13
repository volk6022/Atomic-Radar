"""Счётчик «Черновиков в очереди» видит оба контура — на настоящем Postgres.

Плитка дашборда, элемент очередей и бейдж меню читают одно и то же число из
`_counts`. Черновики сценариев живут в `wf_drafts`, старый контур — в `drafts`;
считать только один из двух значило бы показать «очередь пуста» там, где ждут
ревью сотни заготовок (прод 13.09: `drafts: 0` при 276 ждущих в сценариях).
Проверять это можно только целиком: счётчик, разбивка по сценариям и плитка —
разные места одного ответа, и разойтись они могут независимо.

Разбивка `drafts_by_workflow` проверяется на двух сценариях с pending в каждом
и одним `approved` рядом: счётчик, берущий из `wf_drafts` все состояния подряд,
а не только «ждёт ревью», на таких данных уезжает в 4 вместо 3.

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_DEBUG", "true")

from app.core.config import get_settings  # noqa: E402
from app.core.security import SessionSigner  # noqa: E402
from app.db.models import (Base, Channel, Draft, EngageInstance, Lead, Message,  # noqa: E402
                           User, WfDraft, WfTarget, Workflow)
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.main import create_app  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

NOW = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)


async def _seed() -> dict:
    """Один черновик старого контура и по два черновика в каждом из сценариев.

    `cold_dm`: 1 pending. `public_reply`: 1 pending + 1 approved — разобранный
    черновик обязана молчать, иначе «очередь» растёт от каждого решения
    оператора. Старый контур: ровно один pending, чтобы был виден вклад каждого
    из трёх источников в сумму 3.
    """
    engine = create_async_engine(DB_URL, poolclass=None)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)

    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        instance = EngageInstance(key="default", client_label="Основной",
                                  base_url="http://engage:8103",
                                  api_key_env="RADAR_ENGAGE_API_KEY")
        db.add(instance)
        await db.flush()

        dm = Workflow(key="cold_dm", title="Личные сообщения", target_kind="user",
                      action="dm", visibility="private",
                      engage_instance_id=instance.id, engage_use_case="cold_dm",
                      cascade_profile="dm_v1", sort_order=10, is_active=True)
        public = Workflow(key="public_reply", title="Публичные ответы",
                          target_kind="message", action="reply", visibility="public",
                          engage_instance_id=instance.id,
                          engage_use_case="public_reply",
                          cascade_profile="public_v1", sort_order=20, is_active=True)
        db.add_all([dm, public])

        channel = Channel(peer_id=-1001, username="chat", title="Обсуждение")
        db.add(channel)
        await db.flush()

        def msg(tg_id, *, body="платёж за рубеж не проходит, ищу через кого оплатить"):
            return Message(channel_id=channel.id, tg_message_id=tg_id, tg_date=NOW,
                           author_peer_id=500, author_username="user",
                           author_name="Имя", author_is_bot=False,
                           is_automatic_forward=False, text=body, processed_at=NOW)

        m_old, m_dm, m_pub, m_pub2 = msg(2000), msg(2001), msg(2002), msg(2003)
        db.add_all([m_old, m_dm, m_pub, m_pub2])
        await db.flush()

        # Старый контур: лид и его черновик, ждущий ревью.
        lead = Lead(message_id=m_old.id, channel_id=channel.id, author_peer_id=500,
                    pain="не может оплатить за рубеж", quote=m_old.text, score=70)
        db.add(lead)
        await db.flush()
        db.add(Draft(lead_id=lead.id, variants=[{"text": "вариант"}], state="pending"))

        # Цели сценариев: адресация — по оси сценария (CHECK следит).
        target_dm = WfTarget(workflow_id=dm.id, target_kind="user", message_id=m_dm.id,
                             channel_id=channel.id, recipient_peer_id=500,
                             author_peer_id=500, author_username="user",
                             author_name="Имя", score=70, status="in_review")
        target_pub = WfTarget(workflow_id=public.id, target_kind="message",
                              message_id=m_pub.id, channel_id=channel.id,
                              chat_peer_id=channel.peer_id,
                              reply_to_message_id=m_pub.tg_message_id, score=65,
                              status="in_review")
        target_pub2 = WfTarget(workflow_id=public.id, target_kind="message",
                               message_id=m_pub2.id, channel_id=channel.id,
                               chat_peer_id=channel.peer_id,
                               reply_to_message_id=m_pub2.tg_message_id, score=55,
                               status="in_review")
        db.add_all([target_dm, target_pub, target_pub2])
        await db.flush()

        db.add_all([
            WfDraft(workflow_id=dm.id, target_id=target_dm.id,
                    variants=[{"text": "вариант"}], state="pending"),
            WfDraft(workflow_id=public.id, target_id=target_pub.id,
                    variants=[{"text": "вариант"}], state="pending"),
            # Разобранный: в очередь «ждёт ревью» попасть не должен.
            WfDraft(workflow_id=public.id, target_id=target_pub2.id,
                    variants=[{"text": "вариант"}], state="approved"),
        ])

        users = {}
        for role in ("owner", "customer", "reviewer", "viewer"):
            u = User(email=f"{role}@local", name=role, initials=role[:2].upper(),
                     role=role, password_hash="!нельзя-войти", totp_secret="X" * 32,
                     totp_confirmed=True, is_active=True)
            db.add(u)
            users[role] = u
        await db.commit()
        out = {"uids": {r: u.id for r, u in users.items()}}

    await engine.dispose()
    return out


@pytest.fixture
def seeded():
    """Посев в собственном цикле событий, полностью закрытый за собой.

    Живую `AsyncSession` в `TestClient` отдавать нельзя: он крутит приложение в своём
    цикле, а соединение asyncpg привязано к тому, где создано.
    """
    return asyncio.run(_seed())


@pytest.fixture
def client(seeded):
    previous = os.environ.get("RADAR_DATABASE_URL")
    os.environ["RADAR_DATABASE_URL"] = DB_URL
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_session_maker.cache_clear()

    app = create_app()
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c

    if previous is None:
        os.environ.pop("RADAR_DATABASE_URL", None)
    else:
        os.environ["RADAR_DATABASE_URL"] = previous
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_session_maker.cache_clear()


def _login(client, uid):
    token = SessionSigner(get_settings().SECRET_KEY).dumps({"uid": uid, "totp_ok": True})
    client.cookies.set(get_settings().SESSION_COOKIE, token)
    return client


@pytest.fixture
def authed(client, seeded):
    return _login(client, seeded["uids"]["owner"])


# ── счётчики ──────────────────────────────────────────────────────────────────

def test_counters_count_both_contours(authed):
    """Бейдж меню: 1 старый + 1 cold_dm + 1 public_reply. Ноль здесь — та самая
    «пустая очередь», из-за которой счётчик и переписан."""
    assert authed.get("/api/v1/counters").json()["drafts"] == 3


def test_dashboard_tile_and_queue_agree_with_the_badge(authed):
    """Плитка и элемент очередей читают тот же `_counts`: разошестись они могут
    только если кто-то посчитает очередь второй раз по-своему."""
    d = authed.get("/api/v1/dashboard").json()
    tile = next(t for t in d["tiles"] if t["key"] == "drafts")
    queue = next(q for q in d["queues"] if q["key"] == "drafts")

    assert tile["value"] == 3
    assert queue["count"] == 3
    # Маршрут решает оболочка: go остаётся общим «на очередь черновиков»,
    # разбивка по сценариям — рядом, а не вместо него.
    assert tile["go"] == "drafts"
    assert queue["go"] == "drafts"


def test_dashboard_breaks_the_queue_down_by_workflow(authed):
    """Два элемента — по одному на сценарий с ждущими, в порядке меню; approved
    из public_reply в разбивку не попадает, title — из `workflows`."""
    rows = authed.get("/api/v1/dashboard").json()["drafts_by_workflow"]

    assert [(r["key"], r["count"]) for r in rows] == [("cold_dm", 1),
                                                      ("public_reply", 1)]
    assert [r["title"] for r in rows] == ["Личные сообщения", "Публичные ответы"]
    # Разбивка покрывает сценарную часть счётчика: сумма + старый контур = 3.
    assert sum(r["count"] for r in rows) + 1 == 3
