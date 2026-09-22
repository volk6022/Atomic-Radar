"""Метка «этому человеку уже отправлено» в очереди черновиков сценария.

Случай с прода 22.09: у одного адресата (`recipient_peer_id` один и тот же) два
черновика — один доставлен вебхуком, у второго одобренного две попытки упали
(«PEER_ID_INVALID»). Правило экрана «одобрен + последняя попытка failed → кнопку
вернуть» показывает такому черновику кнопку «Отправить», хотя человек сообщение
уже получил другим черновиком. Ключ `already_sent` в отдаче черновиков — данные
для метки вместо кнопки; здесь проверяется, кому он достаётся.

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
Посев стирает схему public этой базы — она должна быть одноразовой.
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
from app.db.models import (Base, Channel, Conversation, EngageInstance,  # noqa: E402
                           Message, User, WfDraft, WfOutbound, WfTarget,
                           Workflow)
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.main import create_app  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

NOW = datetime(2026, 9, 20, 8, 14, tzinfo=timezone.utc)
TEXT = "Добрый день! Судя по описанию, дело в валютном контроле."


async def _seed(*, freshest: bool = False) -> dict:
    """Прод-случай в миниатюре: один адресат, два черновика — и чужой адресат.

    * `d1` — черновик, чья отправка ДОСТАВЛЕНА (`o1`, как продовый #120);
    * `d2` — одобренный с двумя неудачными попытками (`o2`, `o3`, как #119);
    * `d3` — черновик на ДРУГОГО адресата: ему метка не положена;
    * `d4` — черновик ВТОРОГО сценария тому же адресату: доставка первого
      сценария сквозь границу сценариев не течёт;
    * `d5`/`o5` (только при `freshest`) — вторая доставка тому же адресу,
      сделанная ПОЗЖЕ первой: метка обязана назвать её, а не первую.
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

        def workflow(key, order):
            return Workflow(key=key, title=key, target_kind="user", action="dm",
                            visibility="private",
                            engage_instance_id=instance.id, engage_use_case="cold_dm",
                            cascade_profile="dm_v1", sort_order=order, is_active=True)

        wf_a, wf_b = workflow("cold_dm", 10), workflow("other_dm", 20)
        db.add_all([wf_a, wf_b])

        channel = Channel(peer_id=-1001, username="chat", title="Обсуждение")
        db.add(channel)
        await db.flush()

        async def target(wf, tg_id, peer, username, *, status):
            m = Message(channel_id=channel.id, tg_message_id=tg_id, tg_date=NOW,
                        author_peer_id=peer, author_username=username,
                        author_name=username.title(), author_is_bot=False,
                        is_automatic_forward=False,
                        text="платёж за рубеж не проходит, ищу через кого оплатить",
                        processed_at=NOW)
            db.add(m)
            await db.flush()
            t = WfTarget(workflow_id=wf.id, target_kind="user", message_id=m.id,
                         channel_id=channel.id, recipient_peer_id=peer,
                         author_peer_id=peer, author_username=username,
                         author_name=username.title(), pain=None, quote=m.text,
                         score=70, score_breakdown=[], disqualifiers=[],
                         status=status)
            db.add(t)
            await db.flush()
            return t

        # d1 доставлен — его цель «contacted», как после вебхука доставки;
        # d2/d3 одобрены и ждут кнопки; у d2 попытки падали.
        t1 = await target(wf_a, 3000, 500, "ivan", status="contacted")
        t2 = await target(wf_a, 3001, 500, "ivan", status="approved")
        t3 = await target(wf_a, 3002, 600, "olga", status="approved")
        # Сообщение своё: у `messages` уникальность (channel, tg_message_id)
        # глобальная, а не по сценариям, как у целей.
        t4 = await target(wf_b, 3004, 500, "ivan", status="approved")

        drafts = {}
        for name, t, state in (("d1", t1, "sent"), ("d2", t2, "approved"),
                               ("d3", t3, "approved")):
            d = WfDraft(workflow_id=wf_a.id, target_id=t.id,
                        variants=[{"text": TEXT}], final_text=TEXT, state=state,
                        decided_by="andrey@local", decided_at=NOW,
                        prompt_version="template-v0")
            db.add(d)
            drafts[name] = d
        d4 = WfDraft(workflow_id=wf_b.id, target_id=t4.id,
                     variants=[{"text": TEXT}], final_text=TEXT, state="approved",
                     decided_by="andrey@local", decided_at=NOW,
                     prompt_version="template-v0")
        db.add(d4)
        drafts["d4"] = d4

        conv = Conversation(peer_id=500, engage_account_id=3, source="draft",
                            state="new", sent_count=1)
        db.add(conv)
        await db.flush()

        def outbound(d, t, *, state, peer=500, conv_id=None, **extra):
            return WfOutbound(workflow_id=wf_a.id, target_id=t.id, draft_id=d.id,
                              engage_account_id=3, recipient_peer_id=peer,
                              allowed=True, reasons=[], mode="manual",
                              text_snapshot=TEXT, state=state,
                              conversation_id=conv_id, **extra)

        # Порядок вставки — порядок id: метка «свежайшая» считается по нему.
        o1 = outbound(drafts["d1"], t1, state="delivered", conv_id=conv.id,
                      delivered_message_id=555)
        o2 = outbound(drafts["d2"], t2, state="failed", error="PEER_ID_INVALID")
        o3 = outbound(drafts["d2"], t2, state="failed", error="PEER_ID_INVALID")
        db.add_all([o1, o2, o3])

        if freshest:
            t5 = await target(wf_a, 3003, 500, "ivan", status="contacted")
            d5 = WfDraft(workflow_id=wf_a.id, target_id=t5.id,
                         variants=[{"text": TEXT}], final_text=TEXT, state="sent",
                         decided_by="andrey@local", decided_at=NOW,
                         prompt_version="template-v0")
            db.add(d5)
            await db.flush()
            drafts["d5"] = d5
            db.add(outbound(d5, t5, state="delivered", conv_id=conv.id,
                            delivered_message_id=556))

        users = {}
        for role in ("owner", "customer"):
            u = User(email=f"{role}@local", name=role, initials=role[:2].upper(),
                     role=role, password_hash="!нельзя-войти", totp_secret="X" * 32,
                     totp_confirmed=True, is_active=True)
            db.add(u)
            users[role] = u
        await db.commit()

        out = {"uids": {r: u.id for r, u in users.items()},
               "drafts": {k: d.id for k, d in drafts.items()},
               "conversation_id": conv.id,
               "at": o1.created_at.isoformat()}
    await engine.dispose()
    return out


@pytest.fixture
def seeded():
    return asyncio.run(_seed())


@pytest.fixture
def seeded_freshest():
    return asyncio.run(_seed(freshest=True))


@pytest.fixture
def client(seeded):
    """Посев в собственном цикле событий, приложение — в своём (см. `test_wf_queues_db`)."""
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
    token = SessionSigner(get_settings().SECRET_KEY).dumps(
        {"uid": uid, "totp_ok": True})
    client.cookies.set(get_settings().SESSION_COOKIE, token)
    return client


@pytest.fixture
def authed(client, seeded):
    return _login(client, seeded["uids"]["owner"])


@pytest.fixture
def client_freshest(seeded_freshest):
    """Тот же подъём приложения, что у `client`, — но на посеве с двумя доставками."""
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


@pytest.fixture
def authed_freshest(client_freshest, seeded_freshest):
    return _login(client_freshest, seeded_freshest["uids"]["owner"])


def _by_id(payload: dict) -> dict:
    return {row["id"]: row for row in payload["rows"]}


def test_second_draft_for_the_same_peer_carries_already_sent(authed, seeded):
    """Черновику #119 метка положена, доставленному #119-другому (#120) — нет,
    чужому адресату — нет. Неудачные попытки самого черновика меткой не живут:
    они дают бейдж «не отправлено», а не факт доставки человеку."""
    rows = _by_id(authed.get("/api/v1/workflows/cold_dm/drafts").json())
    d1, d2, d3 = (seeded["drafts"][k] for k in ("d1", "d2", "d3"))

    # Доставленному самому: своя доставка не считается — у него и так «sent».
    assert rows[d1]["state"] == "sent"
    assert rows[d1]["already_sent"] is None

    # Второй черновик тому же адресату: метка называет доставленный черновик.
    assert rows[d2]["state"] == "approved"
    assert rows[d2]["outbound"]["state"] == "failed"
    assert rows[d2]["already_sent"] == {
        "draft_id": d1, "at": seeded["at"],
        "conversation_id": seeded["conversation_id"]}

    # Другому адресату доставок нет — метки нет.
    assert rows[d3]["already_sent"] is None


def test_delivery_of_another_scenario_does_not_leak(authed, seeded):
    """Ключ отвечает за СВОЙ сценарий: доставка первого не гасит кнопку второго —
    там у оператора свои черновики и свой гейт при заказе."""
    rows = _by_id(authed.get("/api/v1/workflows/other_dm/drafts").json())
    assert rows[seeded["drafts"]["d4"]]["state"] == "approved"
    assert rows[seeded["drafts"]["d4"]]["already_sent"] is None


def test_cursor_and_card_carry_the_same_label(authed, seeded):
    """Три входа в очередь — список, курсор, прямая ссылка — обязаны показать
    одну и ту же метку: экран у них общий."""
    d2 = seeded["drafts"]["d2"]
    listed = _by_id(authed.get("/api/v1/workflows/cold_dm/drafts").json())[d2]

    cursor = authed.get(
        "/api/v1/workflows/cold_dm/drafts/next?state=approved").json()
    assert cursor["draft"]["id"] == d2
    direct = authed.get(
        f"/api/v1/workflows/cold_dm/drafts/{d2}").json()["draft"]

    assert cursor["draft"]["already_sent"] == listed["already_sent"]
    assert direct["already_sent"] == listed["already_sent"]


def test_several_deliveries_name_the_freshest(authed_freshest, seeded_freshest):
    """Доставляли дважды — метка зовёт последнюю доставку: оператору видно
    последнее касание человека, а не первое."""
    rows = _by_id(authed_freshest.get("/api/v1/workflows/cold_dm/drafts").json())
    d2, d5 = seeded_freshest["drafts"]["d2"], seeded_freshest["drafts"]["d5"]
    assert rows[d2]["already_sent"]["draft_id"] == d5
