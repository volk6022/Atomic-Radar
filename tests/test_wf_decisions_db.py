"""Решения по целям и черновикам сценария — на настоящем Postgres.

Ради чего этот файл существует отдельно от `test_wf_queues_db.py`: там проверяется,
что два конвейера **показывают** разное, здесь — что они разное **меняют**. Это
разные способы сломаться. Список, случайно захвативший чужой сценарий, выглядит
странно и заметен глазом; массовое решение, случайно захватившее чужой сценарий,
выглядит нормально и обнаруживается через неделю по отклонённым лидам, которых никто
не отклонял.

Три свойства, вокруг которых собраны проверки:

1. **Сценарий ограничивает выборку.** И перечислением `ids`, и отбором по фильтру.
2. **Доставленное неприкосновенно.** Единственная точка невозврата — запись в
   `wf_outbound` с `delivered_message_id`; всё остальное человек вправе передумать.
3. **Гейт отвечает только за личные сообщения.** У публичного ответа он не считается,
   и ответ говорит об этом прямо, а не показывает зелёный, посчитанный не про то.

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
from app.db.models import (AuditLog, Base, Channel, EngageInstance,  # noqa: E402
                           Message, User, WfDraft, WfOutbound, WfTarget, Workflow)
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.main import create_app  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

NOW = datetime(2026, 8, 25, 12, 0, tzinfo=timezone.utc)

VARIANTS = [{"text": "Привет. Судя по описанию, дело в валютном контроле.", "kind": "template"},
            {"text": "Похоже на типовой отказ банка — могу подсказать.", "kind": "template"}]


async def _seed() -> dict:
    """Два сценария, по паре целей с черновиками в каждом, и один отправленный.

    Отправленный нужен непременно живой записью в `wf_outbound`, а не флагом на
    черновике: проверка невозвратности читает именно журнал, и подделка флагом
    подтвердила бы не то, что проверяется.
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
        other = Channel(peer_id=-1002, username="chat2", title="Второй чат")
        db.add_all([channel, other])
        await db.flush()

        messages = []
        for n in range(4):
            m = Message(channel_id=channel.id, tg_message_id=1000 + n, tg_date=NOW,
                        author_peer_id=500 + n, author_username=f"user{n}",
                        author_name=f"Имя {n}", author_is_bot=False,
                        is_automatic_forward=False,
                        text="платёж за рубеж не проходит, ищу через кого оплатить",
                        processed_at=NOW)
            messages.append(m)
        db.add_all(messages)
        await db.flush()

        def dm_target(m, *, score, pain, channel_id=None):
            return WfTarget(workflow_id=dm.id, target_kind="user", message_id=m.id,
                            channel_id=channel_id or channel.id,
                            recipient_peer_id=m.author_peer_id,
                            author_peer_id=m.author_peer_id,
                            author_username=m.author_username,
                            author_name=m.author_name, pain=pain, quote=m.text,
                            score=score, score_breakdown=[], disqualifiers=[],
                            status="new")

        def public_target(m, *, score, pain):
            return WfTarget(workflow_id=public.id, target_kind="message",
                            message_id=m.id, channel_id=channel.id,
                            chat_peer_id=channel.peer_id,
                            reply_to_message_id=m.tg_message_id,
                            author_peer_id=m.author_peer_id,
                            author_username=m.author_username,
                            author_name=m.author_name, pain=pain, quote=m.text,
                            score=score, score_breakdown=[], disqualifiers=[],
                            status="new")

        # Боли и каналы разведены нарочно: без этого отбор по фильтру нечем проверить.
        dm_a = dm_target(messages[0], score=70, pain="не может оплатить за рубеж")
        dm_b = dm_target(messages[1], score=30, pain="выплаты людям за границей",
                         channel_id=other.id)
        dm_sent = dm_target(messages[2], score=61, pain="не может оплатить за рубеж")
        pub_a = public_target(messages[0], score=65, pain="не может оплатить за рубеж")
        pub_b = public_target(messages[3], score=40, pain="ищет, через кого платить")
        db.add_all([dm_a, dm_b, dm_sent, pub_a, pub_b])
        await db.flush()

        drafts = {}
        for name, t, wf in (("dm_a", dm_a, dm), ("dm_b", dm_b, dm),
                            ("dm_sent", dm_sent, dm), ("pub_a", pub_a, public),
                            ("pub_b", pub_b, public)):
            d = WfDraft(workflow_id=wf.id, target_id=t.id, variants=VARIANTS,
                        thread_context=[], state="pending",
                        prompt_version="template-v0",
                        source_message_link="https://t.me/chat/1000")
            db.add(d)
            drafts[name] = d
        await db.flush()

        # Единственная точка невозврата: сообщение действительно доставлено.
        db.add(WfOutbound(workflow_id=dm.id, target_id=dm_sent.id,
                          draft_id=drafts["dm_sent"].id, engage_account_id=12,
                          recipient_peer_id=dm_sent.recipient_peer_id, allowed=True,
                          reasons=[], mode="LIVE", delivered_message_id=99,
                          text_snapshot=VARIANTS[0]["text"]))

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
            "targets": {"dm_a": dm_a.id, "dm_b": dm_b.id, "dm_sent": dm_sent.id,
                        "pub_a": pub_a.id, "pub_b": pub_b.id},
            "drafts": {k: d.id for k, d in drafts.items()},
            "channels": {"main": channel.id, "other": other.id},
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


def _status(target_id):
    return _rows(select(WfTarget.status).where(WfTarget.id == target_id))[0][0]


def _state(draft_id):
    return _rows(select(WfDraft.state).where(WfDraft.id == draft_id))[0][0]


WRITES = [
    ("post", "/api/v1/workflows/cold_dm/targets/bulk",
     {"action": "reject", "reason": "шум", "ids": [1]}),
    ("patch", "/api/v1/workflows/cold_dm/targets/1", {"status": "rejected"}),
    ("post", "/api/v1/workflows/cold_dm/drafts/1/approve", {"variant_index": 0}),
    ("post", "/api/v1/workflows/cold_dm/drafts/1/edit",
     {"variant_index": 0, "text": "текст"}),
    ("post", "/api/v1/workflows/cold_dm/drafts/1/reject", {"reason_n": 1}),
    ("post", "/api/v1/workflows/cold_dm/drafts/1/reopen", {}),
]


# ── доступ ────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("method,path,body", WRITES)
def test_anonymous_changes_nothing(client, method, path, body):
    assert getattr(client, method)(path, json=body).status_code == 401


@pytest.mark.parametrize("method,path,body", WRITES)
def test_guest_changes_nothing(client, seeded, method, path, body):
    """Гостю закрыты и лиды, и черновики — значит закрыты и решения по ним."""
    _login(client, seeded["uids"]["viewer"])
    assert getattr(client, method)(path, json=body).status_code == 403


def test_unknown_workflow_is_404_not_a_silent_success(authed, seeded):
    """Опечатка в ключе не должна выглядеть как выполненное действие."""
    r = authed.patch(f"/api/v1/workflows/нет-такого/targets/{seeded['targets']['dm_a']}",
                     json={"status": "rejected"})
    assert r.status_code == 404
    assert _status(seeded["targets"]["dm_a"]) == "new"


# ── чужой сценарий ────────────────────────────────────────────────────────────

def test_target_of_another_workflow_is_not_reachable_by_direct_id(authed, seeded):
    """Прямая ссылка на чужую цель — 404, а не правка через соседний конвейер."""
    r = authed.patch(f"/api/v1/workflows/cold_dm/targets/{seeded['targets']['pub_a']}",
                     json={"status": "rejected"})
    assert r.status_code == 404
    assert _status(seeded["targets"]["pub_a"]) == "new"


def test_draft_of_another_workflow_is_not_reachable_by_direct_id(authed, seeded):
    r = authed.post(f"/api/v1/workflows/cold_dm/drafts/{seeded['drafts']['pub_a']}/reject",
                    json={"reason_n": 1})
    assert r.status_code == 404
    assert _state(seeded["drafts"]["pub_a"]) == "pending"


def test_bulk_by_ids_ignores_targets_of_another_workflow(authed, seeded):
    """Перечисление чужих id не даёт до них добраться — выборка сужена сценарием."""
    r = authed.post("/api/v1/workflows/cold_dm/targets/bulk",
                    json={"action": "reject", "reason": "шум",
                          "ids": [seeded["targets"]["dm_a"],
                                  seeded["targets"]["pub_a"]]})
    assert r.status_code == 200
    assert r.json()["changed"] == 1
    assert _status(seeded["targets"]["dm_a"]) == "rejected"
    assert _status(seeded["targets"]["pub_a"]) == "new"


def test_bulk_by_filter_does_not_leak_into_another_workflow(authed, seeded):
    """«Отклонить всё под фильтром» в одном блоке не косит соседний.

    Боль у `dm_a` и `pub_a` одна и та же нарочно: без ограничения по сценарию фильтр
    «не может оплатить за рубеж» захватил бы обе цели, и в публичном блоке отклонилось бы то,
    чего человек не видел.
    """
    r = authed.post("/api/v1/workflows/cold_dm/targets/bulk",
                    json={"action": "reject", "reason": "шум",
                          "filter": {"pain": "не может оплатить за рубеж"}})
    assert r.status_code == 200
    assert _status(seeded["targets"]["pub_a"]) == "new"


# ── доставленное неприкосновенно ──────────────────────────────────────────────

def test_delivered_target_is_skipped_by_bulk_and_named_in_the_answer(authed, seeded):
    """Молча пропустить мало: экран обязан показать, что именно не тронулось."""
    r = authed.post("/api/v1/workflows/cold_dm/targets/bulk",
                    json={"action": "reject", "reason": "шум",
                          "ids": [seeded["targets"]["dm_a"],
                                  seeded["targets"]["dm_sent"]]})
    body = r.json()
    assert body["changed"] == 1
    assert body["skipped_sent"] == [seeded["targets"]["dm_sent"]]
    assert _status(seeded["targets"]["dm_sent"]) == "new"


def test_delivered_target_cannot_be_patched(authed, seeded):
    r = authed.patch(f"/api/v1/workflows/cold_dm/targets/{seeded['targets']['dm_sent']}",
                     json={"status": "rejected"})
    assert r.status_code == 409
    assert _status(seeded["targets"]["dm_sent"]) == "new"


@pytest.mark.parametrize("suffix,body", [
    ("approve", {"variant_index": 0}),
    ("edit", {"variant_index": 0, "text": "другой текст"}),
    ("reject", {"reason_n": 1}),
    ("reopen", {}),
])
def test_delivered_draft_is_frozen_for_every_decision(authed, seeded, suffix, body):
    """Заморожены все четыре решения, а не только отклонение.

    Правка текста здесь тоже запрещена намеренно: сообщение уже увидел живой человек,
    и «поправить» его задним числом значит разойтись с тем, что он прочитал.
    """
    draft_id = seeded["drafts"]["dm_sent"]
    r = authed.post(f"/api/v1/workflows/cold_dm/drafts/{draft_id}/{suffix}", json=body)
    assert r.status_code == 409
    assert "уже совершено" in r.json()["detail"]
    assert _state(draft_id) == "pending"


# ── предохранители массового решения ──────────────────────────────────────────

def test_expect_mismatch_refuses_instead_of_deciding_more(authed, seeded):
    """Экран показывал одно число, под условие подходит другое — не решаем."""
    r = authed.post("/api/v1/workflows/cold_dm/targets/bulk",
                    json={"action": "reject", "reason": "шум",
                          "filter": {"status": "new"}, "expect": 99})
    assert r.status_code == 409
    assert _status(seeded["targets"]["dm_a"]) == "new"


def test_reviewer_is_capped(client, seeded, monkeypatch):
    """Потолок разборщика считается по всей выборке, а не по изменённым строкам."""
    monkeypatch.setattr("app.api.v1.wf_queues.BULK_LIMIT_REVIEWER", 1)
    _login(client, seeded["uids"]["reviewer"])
    r = client.post("/api/v1/workflows/cold_dm/targets/bulk",
                    json={"action": "reject", "reason": "шум",
                          "filter": {"status": "new"}})
    assert r.status_code == 403
    assert _status(seeded["targets"]["dm_a"]) == "new"


def test_mass_rejection_without_a_reason_is_refused(authed):
    r = authed.post("/api/v1/workflows/cold_dm/targets/bulk",
                    json={"action": "reject", "ids": [1]})
    assert r.status_code == 422


def test_unknown_bulk_action_is_rejected_not_ignored(authed, seeded):
    r = authed.post("/api/v1/workflows/cold_dm/targets/bulk",
                    json={"action": "удалить", "ids": [seeded["targets"]["dm_a"]]})
    assert r.status_code == 422
    assert _status(seeded["targets"]["dm_a"]) == "new"


def test_bulk_without_ids_and_without_filter_is_refused(authed):
    """Пустое тело не должно означать «все»."""
    assert authed.post("/api/v1/workflows/cold_dm/targets/bulk",
                       json={"action": "approve"}).status_code == 422


def test_bulk_carries_pending_drafts_along(authed, seeded):
    """Отклонённая цель не имеет права остаться в очереди на ревью."""
    r = authed.post("/api/v1/workflows/cold_dm/targets/bulk",
                    json={"action": "reject", "reason": "шум",
                          "ids": [seeded["targets"]["dm_a"]]})
    assert r.json()["drafts_changed"] == 1
    assert _state(seeded["drafts"]["dm_a"]) == "rejected"


def test_reset_leaves_drafts_alone(authed, seeded):
    """Возврат цели в «новые» не должен трогать уже принятое решение по черновику."""
    authed.post(f"/api/v1/workflows/cold_dm/drafts/{seeded['drafts']['dm_a']}/reject",
                json={"reason_n": 1})
    r = authed.post("/api/v1/workflows/cold_dm/targets/bulk",
                    json={"action": "reset", "ids": [seeded["targets"]["dm_a"]]})
    assert r.json()["drafts_changed"] == 0
    assert _state(seeded["drafts"]["dm_a"]) == "rejected"


# ── решения по черновику ──────────────────────────────────────────────────────

def test_approve_moves_the_target_too(authed, seeded):
    r = authed.post(f"/api/v1/workflows/cold_dm/drafts/{seeded['drafts']['dm_a']}/approve",
                    json={"variant_index": 0})
    assert r.status_code == 200
    assert _state(seeded["drafts"]["dm_a"]) == "approved"
    assert _status(seeded["targets"]["dm_a"]) == "approved"


def test_approve_prefers_the_saved_edit_over_the_generated_variant(authed, seeded):
    """Одобрение после «сохранить» обязано взять текст человека, а не генерацию."""
    draft_id = seeded["drafts"]["dm_a"]
    authed.post(f"/api/v1/workflows/cold_dm/drafts/{draft_id}/edit",
                json={"variant_index": 0, "text": "мой собственный текст"})
    r = authed.post(f"/api/v1/workflows/cold_dm/drafts/{draft_id}/approve",
                    json={"variant_index": 0})
    assert r.json()["edited"] is True
    final = _rows(select(WfDraft.final_text).where(WfDraft.id == draft_id))[0][0]
    assert final == "мой собственный текст"


def test_edit_saves_without_deciding(authed, seeded):
    """«Поправил, ещё думаю» — рабочее состояние, а не полурешение."""
    draft_id = seeded["drafts"]["dm_a"]
    r = authed.post(f"/api/v1/workflows/cold_dm/drafts/{draft_id}/edit",
                    json={"variant_index": 1, "text": "правка"})
    assert r.status_code == 200
    assert _state(draft_id) == "pending"
    assert _status(seeded["targets"]["dm_a"]) == "new"


def test_nonexistent_variant_is_refused(authed, seeded):
    r = authed.post(f"/api/v1/workflows/cold_dm/drafts/{seeded['drafts']['dm_a']}/approve",
                    json={"variant_index": 9})
    assert r.status_code == 422
    assert _state(seeded["drafts"]["dm_a"]) == "pending"


def test_empty_text_is_refused(authed, seeded):
    r = authed.post(f"/api/v1/workflows/cold_dm/drafts/{seeded['drafts']['dm_a']}/edit",
                    json={"variant_index": 0, "text": "   "})
    assert r.status_code == 422


def test_reject_writes_the_reason_from_the_shared_catalogue(authed, seeded):
    """Справочник причин один на контуры — вторая копия начала бы расходиться."""
    r = authed.post(f"/api/v1/workflows/cold_dm/drafts/{seeded['drafts']['dm_a']}/reject",
                    json={"reason_n": 3})
    assert r.json()["reason"] == "Звучит как реклама"
    reason = _rows(select(WfTarget.reject_reason)
                   .where(WfTarget.id == seeded["targets"]["dm_a"]))[0][0]
    assert reason == "Звучит как реклама"


def test_reopen_returns_the_target_to_in_review_not_to_new(authed, seeded):
    """«Новая» означало бы, что до цели ещё не доходили руки, — а черновик уже есть."""
    draft_id, target_id = seeded["drafts"]["dm_a"], seeded["targets"]["dm_a"]
    authed.post(f"/api/v1/workflows/cold_dm/drafts/{draft_id}/reject",
                json={"reason_n": 1})
    r = authed.post(f"/api/v1/workflows/cold_dm/drafts/{draft_id}/reopen", json={})
    assert r.status_code == 200
    assert _state(draft_id) == "pending"
    assert _status(target_id) == "in_review"


def test_reopen_of_a_queued_draft_is_a_conflict_not_a_no_op(authed, seeded):
    r = authed.post(f"/api/v1/workflows/cold_dm/drafts/{seeded['drafts']['dm_a']}/reopen",
                    json={})
    assert r.status_code == 409


def test_remaining_counts_only_this_workflow(authed, seeded):
    """Счётчик «осталось» в блоке сценария — про этот блок, а не про сумму по всем."""
    r = authed.post(f"/api/v1/workflows/cold_dm/drafts/{seeded['drafts']['dm_a']}/reject",
                    json={"reason_n": 1})
    # Было три черновика ЛС, один разобран — остаётся два, публичные не в счёт.
    assert r.json()["remaining"] == 2


# ── гейт ──────────────────────────────────────────────────────────────────────

def test_dm_approval_runs_the_gate(authed, seeded):
    """Оператор обязан видеть, что после «одобрить» отправка всё равно заперта."""
    r = authed.post(f"/api/v1/workflows/cold_dm/drafts/{seeded['drafts']['dm_a']}/approve",
                    json={"variant_index": 0})
    send = r.json()["send"]
    assert send["checked"] is True
    assert send["allowed"] is False          # система в сухом прогоне
    assert send["reasons"]


def test_public_approval_says_the_gate_was_not_run(authed, seeded):
    """Зелёный, посчитанный не про то, хуже отсутствующего: на него смотрят как на
    разрешение. Проверки написаны про переписку с человеком, а у публичного ответа
    адресата-человека нет."""
    r = authed.post(
        f"/api/v1/workflows/public_reply/drafts/{seeded['drafts']['pub_a']}/approve",
        json={"variant_index": 0})
    send = r.json()["send"]
    assert send["checked"] is False
    assert send["allowed"] is None
    assert "не заведён" in " ".join(send["reasons"])


# ── журнал ────────────────────────────────────────────────────────────────────

def test_audit_names_the_workflow(authed, seeded):
    """Без имени сценария в журнале два конвейера сливаются в один поток решений."""
    authed.post(f"/api/v1/workflows/public_reply/drafts/{seeded['drafts']['pub_b']}/reject",
                json={"reason_n": 1})
    rows = _rows(select(AuditLog.action, AuditLog.detail)
                 .where(AuditLog.action == "wf_draft_reject"))
    assert rows and rows[0][1]["workflow"] == "public_reply"
