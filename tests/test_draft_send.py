"""Сервис ручной отправки `draft_send` — правила, которые не требуют базы.

Здесь чистая логика: выбор аккаунта по читателю сообщения, гейт на фактах нитки,
«уже заказано», форма preflight, двухфазный заказ со строкой журнала ДО сетевого
вызова. Проверки, где решение держат внешние ключи, уникальность и настоящие
запросы (первый читатель по `first_seen_at`, «уже писали» сквозь вебхук, права
ручек), живут в `test_draft_send_db.py` — на Postgres, без подделок.

Сессия подменяется скриптом результатов: порядок обращений к базе в `_plan`
детерминирован, поэтому результаты раздаются по порядку, а утверждения проверяют
и то, что в этот порядок не пришлось вмешиваться.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_INGEST_TOKEN", "test-ingest-token")

from app.services import draft_send, engage  # noqa: E402
from app.db.models import AuditLog, WfOutbound, WfTarget  # noqa: E402

NOW = datetime(2026, 9, 19, 14, 0, tzinfo=timezone.utc)
TEXT = "Добрый день! Судя по описанию, дело в валютном контроле."


# ── подделки ──────────────────────────────────────────────────────────────────

class _Rows:
    def __init__(self, rows):
        self._rows = list(rows)

    def first(self):
        return self._rows[0] if self._rows else None

    def scalars(self):
        return self

    def all(self):
        return list(self._rows)

    def scalar_one(self):
        return self._rows[0]

    def scalar_one_or_none(self):
        return self._rows[0] if self._rows else None


class FakeDB:
    """Сессия-скрипт: scalar/execute/get раздают заготовки по порядку исполнения."""

    def __init__(self, script=(), gets=None):
        self.script = list(script)
        self.gets = dict(gets or {})
        self.added: list = []
        self.commits = 0

    async def scalar(self, stmt):
        return self.script.pop(0)

    async def execute(self, stmt):
        return _Rows(self.script.pop(0))

    async def get(self, model, pk):
        return self.gets.get((model, pk))

    def add(self, obj):
        self.added.append(obj)
        if getattr(obj, "id", None) is None:
            obj.id = len(self.added)

    async def commit(self):
        self.commits += 1


def workflow(**over) -> SimpleNamespace:
    base = dict(id=2, key="cold_dm", action="dm", engage_instance_id=4)
    base.update(over)
    return SimpleNamespace(**base)


def draft(**over) -> SimpleNamespace:
    base = dict(id=9, target_id=5, state="approved", variants=[{"text": TEXT}],
                chosen_variant=None, final_text=TEXT)
    base.update(over)
    return SimpleNamespace(**base)


def target(**over) -> SimpleNamespace:
    base = dict(id=5, target_kind="user", message_id=11, recipient_peer_id=123456,
                author_username="ivan_p", author_name="Иван П.")
    base.update(over)
    return SimpleNamespace(**base)


def state_row(mode="DRY_RUN") -> SimpleNamespace:
    return SimpleNamespace(mode=mode, killed=False)


def plan_script(*, tgt=None, reader=(3,), with_reader=True, facts=None,
                already=None, instance_key="default",
                state=None) -> FakeDB:
    """Скрипт сессии в порядке `_plan`: инстанс → читатель → режим → факты нитки
    → последняя попытка. Флот и остатки приходят через engage-заглушки, цель —
    через `db.get`. `with_reader=False` — для планов, где аккаунт не ищется
    вовсе (нет адресата-человека)."""
    script: list = [instance_key]                 # _instance_key → db.scalar
    if with_reader:
        script.append([(reader[0],)] if reader else [])   # читатели сообщения
    script += [
        [state or state_row()],                   # current_mode
        [facts] if facts is not None else [None],  # contact_facts
        [already] if already is not None else [None],  # последняя попытка
    ]
    db = FakeDB(script=script)
    db.gets[(WfTarget, 5)] = tgt or target()
    return db


def stub_engage(monkeypatch, *, fleet=None, limits=None, send=None):
    """Заглушки `engage` на уровне модуля — как их зовёт `draft_send`."""
    calls = {"send": [], "webhook": []}

    async def list_accounts(*, instance=None):
        return fleet if fleet is not None else [{"account_id": 3, "status": "active", "warmup_tier": "ready"}]

    async def limits_stub(*, account_ids=None, instance=None):
        if limits is not None:
            return limits
        return {"accounts": [{"account_id": a, "actions": [
            {"action": "messages_per_day", "remaining": 17}]}
            for a in (account_ids or [])]}

    async def send_message(**kw):
        if send is not None and isinstance(send, Exception):
            raise send
        calls["send"].append(kw)
        return {"task_id": "task-1", "status": "queued"}

    def webhook(**params):
        calls["webhook"].append(dict(params))
        return "http://radar.test/ingest/tok?" + "&".join(
            f"{k}={v}" for k, v in sorted(params.items()))

    monkeypatch.setattr(engage, "list_accounts", list_accounts)
    monkeypatch.setattr(engage, "limits", limits_stub)
    monkeypatch.setattr(engage, "send_message", send_message)
    monkeypatch.setattr(engage, "webhook_url", webhook)
    return calls


def successful_db(monkeypatch, **kw):
    calls = stub_engage(monkeypatch, **kw)
    db = plan_script()
    return db, calls


# ── order: успех ──────────────────────────────────────────────────────────────

async def test_order_writes_the_row_then_reaches_the_task_id(monkeypatch):
    """Заказ: строка журнала ДО сетевого вызова, `task_id` — после ответа Engage.

    Регресс дефекта приёмки: `OutboundGate` создаётся с `journal=None` (строку
    уже записал сам `order`), и звонки журнала из гейта не должны ронять заказ
    ПОСЛЕ того, как Engage принял задачу.
    """
    db, calls = successful_db(monkeypatch)
    row = await draft_send.order(db, workflow=workflow(), draft=draft(),
                                 actor="owner@local", now=NOW)

    assert row.state == "pending"
    assert row.engage_task_id == "task-1", (
        "заказ обязан дойти до номера задачи Engage при journal=None")
    assert row.workflow_id == 2 and row.draft_id == 9 and row.target_id == 5
    assert row.engage_account_id == 3 and row.recipient_peer_id == 123456
    assert row.allowed is True and row.reasons == []
    assert row.mode == "DRY_RUN", "снимок режима обязателен и при ручной отправке"
    assert row.text_snapshot == TEXT
    assert db.commits == 2  # строка журнала, затем задача + аудит


async def test_order_sends_with_the_contract_fields(monkeypatch):
    db, calls = successful_db(monkeypatch)
    row = await draft_send.order(db, workflow=workflow(), draft=draft(),
                                 actor="owner@local", now=NOW)

    [sent] = calls["send"]
    assert sent["account_id"] == 3
    # Адресация: известны оба — в заказ уходят оба, выбор делает воркер Engage.
    assert sent["recipient_peer_id"] == 123456
    assert sent["recipient_username"] == "ivan_p"
    assert sent["text"] == TEXT
    assert sent["idempotency_key"] == f"radar-wf-outbound-{row.id}"
    assert sent["instance"] == "default"
    [hook] = calls["webhook"]
    assert hook == {"kind": "send", "account_id": 3, "outbound_id": row.id}

    audits = [a for a in db.added if isinstance(a, AuditLog)]
    assert len(audits) == 1
    assert audits[0].action == "wf_draft_send"
    assert audits[0].detail == {"workflow": "cold_dm", "draft_id": 9,
                                "outbound_id": row.id, "account_id": 3,
                                "origin": "manual", "task_id": "task-1"}


async def test_order_keeps_the_draft_approved(monkeypatch):
    """«Заказано» живёт в `wf_outbound`, черновик двигает только доставка."""
    db, _ = successful_db(monkeypatch)
    d = draft()
    await draft_send.order(db, workflow=workflow(), draft=d, actor="o@l", now=NOW)
    assert d.state == "approved"


async def test_engage_failure_marks_the_row_failed(monkeypatch):
    """HTTP-ошибка при заказе: попытка `failed` с причиной, черновик остаётся
    `approved` — можно заказать повторно."""
    db, _ = successful_db(
        monkeypatch,
        send=engage.EngageUnavailable("Engage ответил 502: boom"))
    d = draft()

    with pytest.raises(engage.EngageUnavailable):
        await draft_send.order(db, workflow=workflow(), draft=d,
                               actor="o@l", now=NOW)

    [row] = [a for a in db.added if isinstance(a, WfOutbound)]
    assert row.state == "failed"
    assert "502" in row.error
    assert d.state == "approved"
    assert db.commits == 2  # строка журнала, затем фиксация отказа


# ── order: отказы ─────────────────────────────────────────────────────────────

async def test_no_reader_is_a_block(monkeypatch):
    calls = stub_engage(monkeypatch, fleet=[])
    db = plan_script(reader=None)

    with pytest.raises(draft_send.DraftSendBlocked) as e:
        await draft_send.order(db, workflow=workflow(), draft=draft(),
                               actor="o@l", now=NOW)
    assert any("не найден аккаунт, читавший сообщение" in r for r in e.value.reasons)
    assert calls["send"] == []


async def test_inactive_account_is_a_block(monkeypatch):
    stub_engage(monkeypatch, fleet=[{"account_id": 3, "status": "paused"}])
    db = plan_script()

    with pytest.raises(draft_send.DraftSendBlocked) as e:
        await draft_send.order(db, workflow=workflow(), draft=draft(),
                               actor="o@l", now=NOW)
    assert any("не активен" in r for r in e.value.reasons)


async def test_account_still_warming_is_a_block(monkeypatch):
    """20.09: шесть заказов упали 409 «Account not warmed» после зелёного preflight —
    тир прогрева обязан быть причиной, а не сюрпризом Engage."""
    stub_engage(monkeypatch, fleet=[{"account_id": 3, "status": "active",
                                     "warmup_tier": "intermediate"}])
    db = plan_script()

    with pytest.raises(draft_send.DraftSendBlocked) as e:
        await draft_send.order(db, workflow=workflow(), draft=draft(),
                               actor="o@l", now=NOW)
    assert any("в прогреве" in r and "intermediate" in r for r in e.value.reasons)


async def test_unreachable_fleet_is_a_block_not_a_crash(monkeypatch):
    async def dead(*a, **k):
        raise engage.EngageUnavailable("Engage недоступен: ConnectError")

    monkeypatch.setattr(engage, "list_accounts", dead)
    monkeypatch.setattr(engage, "limits", dead)
    monkeypatch.setattr(engage, "send_message", dead)
    monkeypatch.setattr(engage, "webhook_url", lambda **p: "http://w")
    db = plan_script()

    with pytest.raises(draft_send.DraftSendBlocked) as e:
        await draft_send.order(db, workflow=workflow(), draft=draft(),
                               actor="o@l", now=NOW)
    assert any("Engage недоступен" in r for r in e.value.reasons)


async def test_gate_blocks_when_the_person_was_already_contacted(monkeypatch):
    """Факты нитки (решение владельца №3): нитка с отправками — «уже писали»."""
    stub_engage(monkeypatch)
    # Строка запроса `contact_facts` — (sent_count, last_sent_at): нитка с двумя
    # отправками. Возвращает сервис свёртку (True, 2, at) — «уже писали».
    db = plan_script(facts=(2, NOW - timedelta(days=2)))

    with pytest.raises(draft_send.DraftSendBlocked) as e:
        await draft_send.order(db, workflow=workflow(), draft=draft(),
                               actor="o@l", now=NOW)
    assert any("этому человеку уже писали" in r for r in e.value.reasons)


async def test_active_outbound_means_conflict(monkeypatch):
    stub_engage(monkeypatch)
    db = plan_script(already=WfOutbound(id=5, workflow_id=2, draft_id=9,
                                        state="pending"))

    with pytest.raises(draft_send.DraftSendConflict) as e:
        await draft_send.order(db, workflow=workflow(), draft=draft(),
                               actor="o@l", now=NOW)
    assert e.value.outbound.id == 5


async def test_failed_outbound_allows_a_new_order(monkeypatch):
    """`failed` — попытка кончилась: конфликт не возникает, заказ идёт дальше."""
    db, calls = successful_db(monkeypatch)
    db.script[-1] = [WfOutbound(id=5, workflow_id=2, draft_id=9, state="failed")]

    row = await draft_send.order(db, workflow=workflow(), draft=draft(),
                                 actor="o@l", now=NOW)
    assert row.state == "pending"
    assert len(calls["send"]) == 1


async def test_non_dm_workflow_is_a_block(monkeypatch):
    stub_engage(monkeypatch)
    db = plan_script()

    with pytest.raises(draft_send.DraftSendBlocked) as e:
        await draft_send.order(db, workflow=workflow(action="reply"),
                               draft=draft(), actor="o@l", now=NOW)
    assert any("только для личных сообщений" in r for r in e.value.reasons)


async def test_public_target_is_a_block(monkeypatch):
    stub_engage(monkeypatch)
    db = plan_script(tgt=target(target_kind="message", recipient_peer_id=None),
                     with_reader=False)

    with pytest.raises(draft_send.DraftSendBlocked) as e:
        await draft_send.order(db, workflow=workflow(), draft=draft(),
                               actor="o@l", now=NOW)
    assert any("публичный ответ — не ЛС" in r for r in e.value.reasons)


# ── preflight: форма показа ───────────────────────────────────────────────────

async def test_preflight_shape_and_writes_nothing(monkeypatch):
    db, _ = successful_db(monkeypatch)
    out = await draft_send.preflight(db, workflow=workflow(), draft=draft(),
                                     now=NOW)

    assert out["draft_id"] == 9 and out["state"] == "approved"
    assert out["action"] == "dm"
    assert out["recipient"] == {"peer_id": 123456, "username": "ivan_p",
                                "name": "Иван П."}
    assert out["account"] == {"id": 3, "status": "active",
                              "remaining_messages": 17}
    assert out["gate"] == {"allowed": True, "reasons": []}
    assert out["already"] is None
    assert out["text"] == TEXT
    assert db.commits == 0, "preflight ничего не пишет"
    assert db.added == []


async def test_preflight_reports_the_active_outbound(monkeypatch):
    stub_engage(monkeypatch)
    db = plan_script(already=WfOutbound(
        id=5, workflow_id=2, draft_id=9, state="pending", engage_task_id="t-9",
        conversation_id=None, delivered_message_id=None, error=None))
    out = await draft_send.preflight(db, workflow=workflow(), draft=draft(),
                                     now=NOW)

    assert out["already"] == {"outbound_id": 5, "state": "pending",
                              "task_id": "t-9", "delivered_message_id": None,
                              "conversation_id": None, "error": None}
    assert out["gate"]["allowed"] is False


async def test_preflight_without_account_shows_null_account(monkeypatch):
    """Аккаунта нет — `account: null`, причина в gate.reasons (для GUI)."""
    stub_engage(monkeypatch, fleet=[])
    db = plan_script(reader=None)
    out = await draft_send.preflight(db, workflow=workflow(), draft=draft(),
                                     now=NOW)

    assert out["account"] is None
    assert any("не найден аккаунт" in r for r in out["gate"]["reasons"])


# ── транспорт: форма заказа клиентом Engage ───────────────────────────────────

class _Resp:
    status_code = 200
    text = ""

    def json(self):
        return {"task_id": "t-1"}


class _Client:
    def __init__(self, sink):
        self._sink = sink

    async def post(self, path, json=None):
        self._sink["path"] = path
        self._sink["body"] = json
        return _Resp()


def test_send_message_payload_carries_both_addresses_and_context(monkeypatch):
    """Адресация — часть контракта: оба адреса, если известны, и контекст, где
    аккаунт видел человека. 20.09 один peer_id дал PEER_ID_INVALID на всех шести
    живых отправках — «min»-пользователя из группы в сессии нет."""
    sink = {}
    monkeypatch.setattr(engage, "_get_client", lambda instance=None: _Client(sink))

    asyncio.run(engage.send_message(
        account_id=3, recipient_peer_id=123456, recipient_username="@ivan_p",
        text=TEXT, webhook_url="http://w", idempotency_key="radar-wf-outbound-1",
        context_chat_id=-1001, context_message_id=77))

    assert sink["path"] == "/v1/action"
    assert sink["body"]["action"] == "send_message"
    assert sink["body"]["account_id"] == 3
    assert sink["body"]["webhook_url"] == "http://w"
    assert sink["body"]["payload"] == {"text": TEXT,
                                       "idempotency_key": "radar-wf-outbound-1",
                                       "peer_id": 123456, "recipient_username": "ivan_p",
                                       "context_chat_id": -1001, "context_message_id": 77}


def test_send_message_by_username_without_peer_id(monkeypatch):
    sink = {}
    monkeypatch.setattr(engage, "_get_client", lambda instance=None: _Client(sink))

    asyncio.run(engage.send_message(
        account_id=3, recipient_peer_id=None, recipient_username="ivan_p",
        text=TEXT, webhook_url="http://w", idempotency_key="k"))

    assert sink["body"]["payload"]["recipient_username"] == "ivan_p"
    assert "peer_id" not in sink["body"]["payload"]
