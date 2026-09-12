"""Комментарии-отзывы к черновикам — на настоящем Postgres.

Без живой базы здесь не обойтись, и не из-за SQL. Отзыв — это запись, у которой
проверяются связи, а не только форма: снимок версии промпта берётся из конкретного
варианта, фильтр списка обязан сойтись с `total`, чужой сценарий обязан вернуть 404
и не оставить строки в таблице. Каждая из этих проверок сравнивает **два** места —
ответ ручки и то, что реально легло в базу, — и разъехаться они могут незаметно.

Шесть свойств, вокруг которых собраны проверки:

1. Отзыв к варианту снимает версию промпта этого варианта и попадает в карточку.
2. Индекс вне границ отвергается, таблица остаётся пустой.
3. `has_comments` в списке режет и строки, и `total`.
4. Черновик чужого сценария недостижим — на запись тоже.
5. Лента смешивает оба контура, свежие сверху, с контекстом черновика.
6. Удаление: чужое не-владельцем — 403, своё и владельцем — 200.

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_DEBUG", "true")

from app.core.config import get_settings  # noqa: E402
from app.core.security import SessionSigner  # noqa: E402
from app.db.models import (AuditLog, Base, Channel, Draft, DraftComment,  # noqa: E402
                           EngageInstance, Lead, Message, User, WfDraft, WfTarget,
                           Workflow)
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.main import create_app  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

NOW = datetime(2026, 8, 25, 12, 0, tzinfo=timezone.utc)

# У каждого варианта своя версия промпта — иначе «отзыв снял версию варианта»
# нечем отличить от «снял версию черновика». Черновик при этом v1_draft.
VARIANTS = [
    {"text": "Здравствуйте. С валютным контролем помогаем регулярно.",
     "kind": "template", "prompt_version": "tpl_v1"},
    {"text": "Привет. Похоже на типовой отказ банка — подскажу, как обходят.",
     "kind": "template", "prompt_version": "tpl_v2"},
]

COMMENT_TEXT = "Боль попала точно, но второе предложение звучит как реклама"


async def _seed() -> dict:
    """Оба контура с черновиками и четыре пользователя.

    Черновиков в контуре `lead` два: один получит отзыв, второй останется чистым —
    иначе фильтр `has_comments` проверял бы не «отделяет», а «показывает». Черновики
    в контуре `wf` живут в РАЗНЫХ сценариях: публичный получит отзыв, попытка
    оставить его через `cold_dm` обязана вернуть 404 ещё до таблицы.
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

        # ── контур lead: два лида с черновиками ───────────────────────────────
        lead_draft_ids = []
        for n in range(2):
            m = Message(channel_id=channel.id, tg_message_id=2000 + n, tg_date=NOW,
                        author_peer_id=900 + n, author_username=f"leadauthor{n}",
                        author_name=f"Лид {n}", author_is_bot=False,
                        is_automatic_forward=False,
                        text="не можем оплатить счёт поставщику за рубеж",
                        processed_at=NOW)
            db.add(m)
            await db.flush()
            lead = Lead(message_id=m.id, channel_id=channel.id,
                        author_peer_id=900 + n, author_username=f"leadauthor{n}",
                        author_name=f"Лид {n}", pain="не может оплатить за рубеж",
                        quote=m.text, score=70 - n, score_breakdown=[],
                        disqualifiers=[], status="in_review")
            db.add(lead)
            await db.flush()
            d = Draft(lead_id=lead.id, variants=VARIANTS, thread_context=[],
                      state="pending", prompt_version="v1_draft",
                      source_message_link=f"https://t.me/chat/{2000 + n}")
            db.add(d)
            lead_draft_ids.append(d)

        # ── контур wf: цель с черновиком в каждом из двух сценариев ───────────
        wf_messages = []
        for n in range(2):
            m = Message(channel_id=channel.id, tg_message_id=3000 + n, tg_date=NOW,
                        author_peer_id=800 + n, author_username=f"wfauthor{n}",
                        author_name=f"Автор {n}", author_is_bot=False,
                        is_automatic_forward=False,
                        text="платёж за рубеж не проходит, ищу решение",
                        processed_at=NOW)
            db.add(m)
            wf_messages.append(m)
        await db.flush()

        pub_t = WfTarget(workflow_id=public.id, target_kind="message",
                         message_id=wf_messages[0].id, channel_id=channel.id,
                         chat_peer_id=channel.peer_id,
                         reply_to_message_id=wf_messages[0].tg_message_id,
                         author_peer_id=wf_messages[0].author_peer_id,
                         author_username=wf_messages[0].author_username,
                         author_name=wf_messages[0].author_name,
                         pain="не может оплатить за рубеж", quote=wf_messages[0].text,
                         score=60, score_breakdown=[], disqualifiers=[], status="new")
        dm_t = WfTarget(workflow_id=dm.id, target_kind="user",
                        message_id=wf_messages[1].id, channel_id=channel.id,
                        recipient_peer_id=wf_messages[1].author_peer_id,
                        author_peer_id=wf_messages[1].author_peer_id,
                        author_username=wf_messages[1].author_username,
                        author_name=wf_messages[1].author_name,
                        pain="не может оплатить за рубеж", quote=wf_messages[1].text,
                        score=60, score_breakdown=[], disqualifiers=[], status="new")
        db.add_all([pub_t, dm_t])
        await db.flush()

        pub_d = WfDraft(workflow_id=public.id, target_id=pub_t.id, variants=VARIANTS,
                        thread_context=[], state="pending", prompt_version="v1_draft",
                        source_message_link="https://t.me/chat/3000")
        dm_d = WfDraft(workflow_id=dm.id, target_id=dm_t.id, variants=VARIANTS,
                       thread_context=[], state="pending", prompt_version="v1_draft",
                       source_message_link="https://t.me/chat/3001")
        db.add_all([pub_d, dm_d])

        users = {}
        for role in ("owner", "customer", "reviewer", "viewer"):
            u = User(email=f"{role}@local", name=role, initials=role[:2].upper(),
                     role=role, password_hash="!нельзя-войти", totp_secret="X" * 32,
                     totp_confirmed=True, is_active=True)
            db.add(u)
            users[role] = u
        await db.commit()

        out = {
            "uids": {r: u.id for r, u in users.items()},
            "lead_drafts": [d.id for d in lead_draft_ids],
            "wf_drafts": {"public": pub_d.id, "dm": dm_d.id},
        }

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


def _rows(query):
    """Прочитать базу мимо приложения — своим соединением и своим циклом."""
    async def go():
        engine = create_async_engine(DB_URL, poolclass=None)
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with maker() as db:
            out = (await db.execute(query)).all()
        await engine.dispose()
        return out
    return asyncio.run(go())


def _comment_count():
    return _rows(select(func.count(DraftComment.id)))[0][0]


# ── запись отзыва ─────────────────────────────────────────────────────────────

def test_comment_snapshots_variant_prompt_and_reaches_the_card(authed, seeded):
    """Отзыв к варианту снимает версию промпта ЭТОГО варианта, попадает в карточку
    и оставляет запись в журнале. Версию здесь важно проверить дословно: если бы
    снимок брался с черновика, в ленте стояло бы v1_draft, и промпт tpl_v2 правился
    бы по отзыву, оставленному «не про него»."""
    draft_id = seeded["lead_drafts"][0]
    r = authed.post(f"/api/v1/drafts/{draft_id}/comments",
                    json={"text": COMMENT_TEXT, "variant_index": 1})
    assert r.status_code == 201
    body = r.json()
    assert body["contour"] == "lead"
    assert body["draft_id"] == draft_id
    assert body["variant_index"] == 1
    assert body["prompt_version"] == "tpl_v2"
    assert body["author"] == "owner@local"
    assert body["text"] == COMMENT_TEXT

    card = authed.get(f"/api/v1/drafts/{draft_id}").json()["draft"]
    assert card["comments_count"] == 1
    assert [c["id"] for c in card["comments"]] == [body["id"]]
    assert card["comments"][0]["prompt_version"] == "tpl_v2"

    rows = _rows(select(AuditLog).where(AuditLog.action == "draft_comment"))
    assert len(rows) == 1
    (log,) = rows[0]
    assert log.detail == {"draft_id": draft_id, "contour": "lead",
                          "comment_id": body["id"], "variant_index": 1}
    assert log.user_email == "owner@local"


def test_variant_index_out_of_bounds_is_refused_and_writes_nothing(authed, seeded):
    """Варианта нет — отзыва нет. За границей и минус один: его pydantic-схема
    не отсекает, границей занимается сервис."""
    draft_id = seeded["lead_drafts"][0]
    for idx in (2, -1):
        r = authed.post(f"/api/v1/drafts/{draft_id}/comments",
                        json={"text": COMMENT_TEXT, "variant_index": idx})
        assert r.status_code == 422
    assert _comment_count() == 0


def test_comment_to_missing_draft_is_404(authed, seeded):
    r = authed.post("/api/v1/drafts/999999/comments",
                    json={"text": COMMENT_TEXT})
    assert r.status_code == 404
    assert _comment_count() == 0


# ── фильтр списка ─────────────────────────────────────────────────────────────

def test_draft_list_filters_by_has_comments(authed, seeded):
    """true — только черновики с отзывами, false — только остальные; `total`
    и счётчик в строке обязаны сходиться со списком, иначе экран покажет
    «один комментарий», а открыть его не получится."""
    commented, clean = seeded["lead_drafts"][0], seeded["lead_drafts"][1]
    r = authed.post(f"/api/v1/drafts/{commented}/comments",
                    json={"text": COMMENT_TEXT, "variant_index": 0})
    assert r.status_code == 201

    yes = authed.get("/api/v1/drafts/list",
                     params={"has_comments": "true"}).json()
    assert yes["total"] == 1
    assert [row["id"] for row in yes["rows"]] == [commented]
    assert yes["rows"][0]["comments_count"] == 1

    no = authed.get("/api/v1/drafts/list",
                    params={"has_comments": "false"}).json()
    assert no["total"] == 1
    assert [row["id"] for row in no["rows"]] == [clean]
    assert no["rows"][0]["comments_count"] == 0


def test_wf_list_carries_comments_count(authed, seeded):
    """Счётчик приезжает и в строки сценария — одним запросом на страницу."""
    pub_id = seeded["wf_drafts"]["public"]
    authed.post(f"/api/v1/workflows/public_reply/drafts/{pub_id}/comments",
                json={"text": COMMENT_TEXT})
    rows = authed.get("/api/v1/workflows/public_reply/drafts",
                      params={"limit": 50}).json()["rows"]
    assert {row["id"]: row["comments_count"] for row in rows} == {pub_id: 1}


# ── контур wf ─────────────────────────────────────────────────────────────────

def test_wf_comment_scoped_to_its_workflow(authed, seeded):
    """Черновик публичного сценария недостижим через `cold_dm` — и попытка
    не оставляет после себя строки в таблице."""
    pub_id = seeded["wf_drafts"]["public"]
    r = authed.post(f"/api/v1/workflows/public_reply/drafts/{pub_id}/comments",
                    json={"text": COMMENT_TEXT})
    assert r.status_code == 201
    body = r.json()
    assert body["contour"] == "wf"
    assert body["variant_index"] is None
    # Индекса нет — снимок берётся с черновика, а не с варианта.
    assert body["prompt_version"] == "v1_draft"

    other = authed.post(f"/api/v1/workflows/cold_dm/drafts/{pub_id}/comments",
                        json={"text": COMMENT_TEXT})
    assert other.status_code == 404
    assert _comment_count() == 1

    card = authed.get(
        f"/api/v1/workflows/public_reply/drafts/{pub_id}").json()["draft"]
    assert card["comments_count"] == 1
    assert card["comments"][0]["id"] == body["id"]


def test_wf_delete_is_scoped_to_its_workflow_too(authed, seeded):
    """Удаление — та же проверка сценария: отзыв публичного черновика не удаляется
    через чужой ключ, а через свой — удаляется."""
    pub_id = seeded["wf_drafts"]["public"]
    comment = authed.post(
        f"/api/v1/workflows/public_reply/drafts/{pub_id}/comments",
        json={"text": COMMENT_TEXT}).json()

    wrong = authed.delete(
        f"/api/v1/workflows/cold_dm/drafts/{pub_id}/comments/{comment['id']}")
    assert wrong.status_code == 404
    assert _comment_count() == 1

    right = authed.delete(
        f"/api/v1/workflows/public_reply/drafts/{pub_id}/comments/{comment['id']}")
    assert right.status_code == 200
    assert right.json() == {"deleted": comment["id"]}
    assert _comment_count() == 0


# ── общая лента ───────────────────────────────────────────────────────────────

def test_feed_merges_both_contours_newest_first(authed, seeded):
    """Лента одна на оба контура: свежие сверху, у `wf` заполнен сценарий,
    у `lead` — null, текст черновика взят из варианта, к которому отзыв."""
    lead_id = seeded["lead_drafts"][0]
    first = authed.post(f"/api/v1/drafts/{lead_id}/comments",
                        json={"text": "Первый отзыв", "variant_index": 1}).json()
    pub_id = seeded["wf_drafts"]["public"]
    second = authed.post(f"/api/v1/workflows/public_reply/drafts/{pub_id}/comments",
                         json={"text": "Второй отзыв"}).json()

    body = authed.get("/api/v1/drafts/comments").json()
    assert body["total"] == 2
    assert [row["id"] for row in body["rows"]] == [second["id"], first["id"]]

    wf_row, lead_row = body["rows"]
    assert wf_row["contour"] == "wf"
    assert wf_row["workflow"] == "public_reply"
    assert wf_row["draft_state"] == "pending"
    assert wf_row["channel"] == "Обсуждение"
    assert wf_row["draft_text"] == VARIANTS[0]["text"]

    assert lead_row["contour"] == "lead"
    assert lead_row["workflow"] is None
    assert lead_row["draft_state"] == "pending"
    assert lead_row["channel"] == "Обсуждение"
    assert lead_row["draft_text"] == VARIANTS[1]["text"]

    only_wf = authed.get("/api/v1/drafts/comments",
                         params={"contour": "wf"}).json()
    assert only_wf["total"] == 1
    assert only_wf["rows"][0]["id"] == second["id"]


# ── удаление ──────────────────────────────────────────────────────────────────

def test_delete_rules_own_vs_stranger_vs_owner(client, seeded):
    """Чужой отзыв не-владельцу не даётся (403, запись на месте), владельцу —
    даётся; свой человек удаляет сам. Порядок именно такой: после второго шага
    записи уже нет, и третий нельзя было бы проверить."""
    lead_id = seeded["lead_drafts"][0]
    _login(client, seeded["uids"]["owner"])
    comment = client.post(f"/api/v1/drafts/{lead_id}/comments",
                          json={"text": COMMENT_TEXT, "variant_index": 0}).json()
    cid = comment["id"]

    _login(client, seeded["uids"]["reviewer"])
    stranger = client.delete(f"/api/v1/drafts/{lead_id}/comments/{cid}")
    assert stranger.status_code == 403
    assert _comment_count() == 1

    _login(client, seeded["uids"]["owner"])
    owner = client.delete(f"/api/v1/drafts/{lead_id}/comments/{cid}")
    assert owner.status_code == 200
    assert owner.json() == {"deleted": cid}
    assert _comment_count() == 0

    own = client.post(f"/api/v1/drafts/{lead_id}/comments",
                      json={"text": "мой отзыв, сам и убираю"}).json()
    mine = client.delete(f"/api/v1/drafts/{lead_id}/comments/{own['id']}")
    assert mine.status_code == 200
    assert _comment_count() == 0


def test_delete_of_missing_comment_is_404(authed, seeded):
    r = authed.delete(f"/api/v1/drafts/{seeded['lead_drafts'][0]}/comments/999999")
    assert r.status_code == 404


# ── журнал ────────────────────────────────────────────────────────────────────

def test_delete_writes_audit(authed, seeded):
    lead_id = seeded["lead_drafts"][0]
    comment = authed.post(f"/api/v1/drafts/{lead_id}/comments",
                          json={"text": COMMENT_TEXT}).json()
    authed.delete(f"/api/v1/drafts/{lead_id}/comments/{comment['id']}")
    rows = _rows(select(AuditLog).where(AuditLog.action == "draft_comment_delete"))
    assert rows and rows[0][0].detail == {"draft_id": lead_id, "contour": "lead",
                                          "comment_id": comment["id"]}
