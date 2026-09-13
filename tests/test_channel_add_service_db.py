"""Сценарий 4 «одобрен → вступил» (волна Д): одна дорога подключения, тик.

Что здесь проверяется:

* **одна дорога подключения** (T-24): ручка `POST /channels` и прямой вызов
  службы идут через `channel_add.start` — ответ ручки побайтно прежний,
  отказы прежними текстами, аудит пишет служба; `start_join_chain` у службы
  ровно один на подключение (вторая дорога в ручке была бы видна счётчиком);
* **выключенный тик** (T-25): `discovery_autoconnect_enabled = 0` — пустой
  итог, ждущие одобренные посчитаны (2), постановок нет;
* **тик в остатке суток** (T-26): room = per_day − автоподключения сегодня
  (окно `_utc_day_start`: решённый час назад считается, 25 часов назад — нет),
  берутся первые по `(decided_at, id)` — любого `decided_by` (решение ревью
  по §9.3), кандидаты остаются `approved` — их закроет `_connect_approved`;
* **`JobBusy` — стоп перебора, не ошибка** (T-27): занятый вид молча
  заканчивает удар, освободившийся доедается следующим тиком.

Инфраструктура — по соседям: TestClient с cookie владельца
(`test_l1_bypass_api_db.py`), заглушки `engage.list_accounts` и перехват
постановки (`test_autoflow_scan_db.py`), посев кандидатов
(`test_discovery_api.py`). `jobs.create_external` в T-24 оставлен настоящим —
он и в стенде создаёт строку `runs`, исполнителя у внешнего прогона нет;
в тиковых тестах (T-26, доест T-27) он перехватывается, иначе первый же
созданный «running»-прогон вида замкнул бы `JobBusy` на втором кандидате —
свойство вида, а не тика.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_INGEST_TOKEN", "test-ingest-token")
os.environ.setdefault("RADAR_DEBUG", "true")

from app.core import clock  # noqa: E402
from app.core.config import get_settings  # noqa: E402
from app.core.security import SessionSigner  # noqa: E402
from app.db.models import (AuditLog, Base, Channel, ChannelCandidate,  # noqa: E402
                           EngageInstance, Limit, Run, User)
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.main import create_app  # noqa: E402
from app.services import autoflow, channel_add, discovery, engage, jobs  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

NOTE = ("аккаунт подписывается на канал (и на его группу обсуждения, "
        "если она есть); результат придёт вебхуком, ход виден в разделе Runs")


# ── посев ─────────────────────────────────────────────────────────────────────

def _cand(username: str | None, *, decision: str = "approved",
          decided_by: str | None, decided_at: datetime | None) -> ChannelCandidate:
    return ChannelCandidate(username=username, title=f"Кандидат {username or 'без имени'}",
                            source="similar", found_by_account_id=5, found_at=decided_at,
                            decision=decision, decided_by=decided_by,
                            decided_at=decided_at)


def _day_safe_fit(now: datetime) -> datetime:
    """«Сегодняшнее» автоодобрение для контроля окна `_utc_day_start`: час назад,
    но если удар пришёлся на первый час UTC-суток — от начала суток, иначе
    решённый «час назад» оказался бы вчера, и контроль окна вырождался бы в
    лотерею полуночи."""
    return max(now - timedelta(hours=1),
               discovery._utc_day_start() + timedelta(minutes=1))


async def _reset(spec: dict) -> dict:
    """Схема заново + штат (владелец/заказчик/наблюдатель), отслеживаемый канал,
    кандидаты, строки `limits` и при необходимости занятый прогон
    `channel_add`. Инстанс Engage — старту приложения: пустой реестр — это
    несобранное приложение, а не «нет клиентов»."""
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
        for role in ("owner", "customer", "viewer"):
            u = User(email=f"{role}@local", name=role, initials=role[:2].upper(),
                     role=role, password_hash="!нельзя-войти", totp_secret="X" * 32,
                     totp_confirmed=True, is_active=True)
            db.add(u)
            users[role] = u
        channel = Channel(peer_id=-1001, username="tracked", title="Канал",
                          chat_type="channel", ingest_enabled=True)
        db.add(channel)
        now = clock.utcnow()
        cands = spec.get("candidates", ())
        if callable(cands):  # фабрика посева — время решения считается тут
            cands = cands(now)
        for c in cands:
            db.add(c(now) if callable(c) else c)
        for key, value in (spec.get("limits") or {}).items():
            db.add(Limit(key=key, value=value))
        busy_id = None
        if spec.get("busy"):
            run = Run(name="чужое подключение", kind="channel_add", params={},
                      status="running", progress=0, created_by="owner@local",
                      started_at=now, log=[])
            db.add(run)
            await db.flush()
            busy_id = run.id
        await db.commit()
        out = {"uids": {r: u.id for r, u in users.items()}, "channel": channel.id,
               "busy_run": busy_id}
    await engine.dispose()
    return out


@pytest.fixture()
def db_ready(request):
    """Пересеянная база и кеши движка под тестовую; в таблице параметров —
    кандидаты, `limits`, занятый прогон."""
    spec = getattr(request, "param", None) or {}
    previous = os.environ.get("RADAR_DATABASE_URL")
    os.environ["RADAR_DATABASE_URL"] = DB_URL
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_session_maker.cache_clear()
    seeded = asyncio.run(_reset(spec))
    yield SimpleNamespace(**seeded, spec=spec)
    if previous is None:
        os.environ.pop("RADAR_DATABASE_URL", None)
    else:
        os.environ["RADAR_DATABASE_URL"] = previous
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_session_maker.cache_clear()


@pytest.fixture()
def client(db_ready):
    with TestClient(create_app(), raise_server_exceptions=False) as c:
        yield c


def _login(client: TestClient, uid: int) -> None:
    token = SessionSigner(get_settings().SECRET_KEY).dumps({"uid": uid, "totp_ok": True})
    client.cookies.set(get_settings().SESSION_COOKIE, token)


# ── чтение мимо приложения и заглушки ─────────────────────────────────────────

def _own_session():
    """Свой движок под каждый поход в базу: кешированная фабрика держит пул,
    созданный в цикле событий приложения, — второй `asyncio.run` поверх него
    не падает, а зависает."""
    engine = create_async_engine(DB_URL, poolclass=None)
    return engine, async_sessionmaker(engine, expire_on_commit=False)


def _read(query):
    async def go():
        engine, maker = _own_session()
        async with maker() as db:
            rows = (await db.execute(query)).scalars().all()
        await engine.dispose()
        return rows

    return asyncio.run(go())


def _runs(kind: str) -> list[Run]:
    return _read(select(Run).where(Run.kind == kind).order_by(Run.id))


def _audits(action: str) -> list[AuditLog]:
    return _read(select(AuditLog).where(AuditLog.action == action).order_by(AuditLog.id))


def _candidates() -> dict[str | None, ChannelCandidate]:
    rows = _read(select(ChannelCandidate))
    return {c.username: c for c in rows}


def _set_run_done(run_id: int) -> None:
    async def go():
        engine, maker = _own_session()
        async with maker() as db:
            run = await db.get(Run, run_id)
            run.status = "done"
            run.finished_at = clock.utcnow()
            await db.commit()
        await engine.dispose()

    asyncio.run(go())


def _stub_chain(monkeypatch) -> list[dict]:
    """`channel_add.start_join_chain` со счётчиком: заказ chain_group и подпись
    вебхука — работа Engage, службе в тесте она не нужна."""
    calls: list[dict] = []

    async def fake_chain(**kw):
        calls.append(kw)

    monkeypatch.setattr(channel_add, "start_join_chain", fake_chain)
    return calls


def _stub_create_external(monkeypatch) -> list[dict]:
    """Перехват постановки: записывает дословные аргументы и возвращает прогон
    с id. Настоящий оставил бы после первого кандидата занятый вид, и второй
    упёрся бы в `JobBusy` — свойство вида, а не тика."""
    calls: list[dict] = []

    async def fake_create_external(db, *, kind, params, name, user_email):
        run_id = 101 + len(calls)
        calls.append({"kind": kind, "params": params, "name": name,
                      "user_email": user_email, "run_id": run_id})
        return SimpleNamespace(id=run_id)

    monkeypatch.setattr(jobs, "create_external", fake_create_external)
    return calls


def _stub_fleet(monkeypatch, accounts: list[dict]) -> None:
    async def list_accounts(*, instance=None):
        return list(accounts)

    monkeypatch.setattr(engage, "list_accounts", list_accounts)


def _stub_no_engage(monkeypatch) -> None:
    """Любой незаглушённый заказ Engage — падение, а не молчаливый пропуск."""
    async def action(*, account_id, action, payload, webhook_url, **kw):
        raise AssertionError(f"неожиданный заказ Engage: {action} {payload}")

    monkeypatch.setattr(engage, "action", action)


def _tick() -> dict:
    async def go():
        engine, maker = _own_session()
        async with maker() as db:
            out = await autoflow.approve_tick(db)
        await engine.dispose()
        return out

    return asyncio.run(go())


# ── T-24: одна дорога подключения ─────────────────────────────────────────────

@pytest.mark.parametrize("db_ready", [{}], indirect=True)
def test_channel_add_service_single_road(db_ready, client, monkeypatch):
    """T-24: ручка и прямой вызов идут через службу — ответ ручки побайтно
    прежний, `start_join_chain` у службы один на подключение, отказы прежними
    текстами, аудит пишет служба."""
    _stub_no_engage(monkeypatch)
    chain = _stub_chain(monkeypatch)
    _login(client, db_ready.uids["owner"])

    # Ручка: 201, тело побайтно как до выноса.
    r = client.post("/api/v1/channels",
                    json={"username": "newchan", "engage_account_id": 5})
    assert r.status_code == 201, r.text
    run_id = r.json()["run_id"]
    assert r.json() == {"started": True, "username": "newchan", "run_id": run_id,
                        "note": NOTE}

    # В runs — прогон службы с автором-владельцем; цепочка заказана один раз.
    (run,) = _runs("channel_add")
    assert run.id == run_id
    assert run.params == {"username": "newchan", "engage_account_id": 5}
    assert run.name == "Подключение канала · @newchan"
    assert run.created_by == "owner@local"
    assert chain == [{"account_id": 5, "username": "newchan", "run_id": run_id,
                      "subscribed_by": "owner@local", "stage": "channel"}]

    # Аудит пишет служба: действие одно, автор — владелец.
    (audit,) = _audits("channel_add_started")
    assert audit.user_email == "owner@local"
    assert audit.detail == {"username": "newchan", "engage_account_id": 5,
                            "run_id": run_id}

    # Повтор ручкой для уже отслеживаемого — 409 с текстом дословно прежним.
    r2 = client.post("/api/v1/channels",
                     json={"username": "tracked", "engage_account_id": 5})
    assert r2.status_code == 409, r2.text
    assert r2.json()["detail"] == (f"канал @tracked уже отслеживается "
                                   f"(id {db_ready.channel})")
    assert len(_audits("channel_add_started")) == 1, "отказ не пишет аудит"
    assert len(chain) == 1, "отказ не заказывает цепочку"

    # Прямой вызов службы — та же дорога, автор авто.
    _set_run_done(run_id)  # иначе вид остался бы занятым первым прогоном
    async def go():
        engine, maker = _own_session()
        async with maker() as db:
            return await channel_add.start(db, username="x", account_id=5,
                                           actor="auto:approve")

    run2 = asyncio.run(go())
    assert run2.created_by == "auto:approve"
    assert run2.name == "Подключение канала · @x"
    assert chain[-1] == {"account_id": 5, "username": "x", "run_id": run2.id,
                         "subscribed_by": "auto:approve", "stage": "channel"}
    (audit2,) = _audits("channel_add_started")[1:]
    assert audit2.user_email == "auto:approve"
    assert audit2.detail == {"username": "x", "engage_account_id": 5,
                             "run_id": run2.id}

    # Служба тем же текстом отказывает для существующего канала.
    async def go_exists():
        engine, maker = _own_session()
        async with maker() as db:
            await channel_add.start(db, username="tracked", account_id=5,
                                    actor="auto:approve")

    with pytest.raises(channel_add.ChannelExists,
                       match=r"уже отслеживается \(id \d+\)"):
        asyncio.run(go_exists())
    assert len(chain) == 2, "отказ службы не заказывает цепочку"


# ── посев тиковых тестов ──────────────────────────────────────────────────────

def _tick_candidates(*, fit_pair: bool = True) -> list:
    """Базовый посев блока Д (TESTS): c1/c2 одобрены (часы назад), c3 — pending,
    c4 — одобрен без имени. `fit_pair=False` — одобрения за человеком: они не
    считаются автоодключениями, и остаток суток остаётся полным (контроль
    T-27 ждёт room=3)."""
    actor = discovery.FIT_ACTOR if fit_pair else "owner@local"
    return [
        lambda n: _cand("cand1", decided_by=actor,
                        decided_at=n - timedelta(hours=2)),
        lambda n: _cand("cand2", decided_by=actor,
                        decided_at=n - timedelta(hours=1)),
        lambda n: _cand("cand3", decision="pending", decided_by="owner@local",
                        decided_at=n - timedelta(minutes=30)),
        lambda n: _cand(None, decided_by=actor, decided_at=n),
    ]


# ── T-25: выключенный тик ─────────────────────────────────────────────────────

@pytest.mark.parametrize("db_ready", [{
    "candidates": _tick_candidates(),
    "limits": {"discovery_autoconnect_enabled": 0},
}], indirect=True)
def test_approve_tick_disabled(db_ready, monkeypatch):
    """T-25: выключен — постановок нет, ждущие посчитаны, остаток нулевой."""
    _stub_no_engage(monkeypatch)
    chain = _stub_chain(monkeypatch)
    _stub_fleet(monkeypatch, [{"account_id": 5, "status": "active"}])

    out = _tick()
    assert out == {"started": 0, "approved_waiting": 2, "room": 0}, out
    assert chain == []
    assert _runs("channel_add") == []
    assert _audits("channel_add_started") == []


# ── T-26: тик в остатке суток ─────────────────────────────────────────────────

def _budget_candidates(now) -> list:
    """Посев T-26: три одобренных с именем (кандидат «час назад» — сегодняшнее
    автоодобрение FIT, остальные двое одобрены человеком), плюс pending с
    именем, одобренный без имени и вчерашнее автоодобрение — контроль окна
    `_utc_day_start` (25 часов назад не считается)."""
    fit_at = _day_safe_fit(now)
    return [
        lambda n: _cand("cand2", decided_by=discovery.FIT_ACTOR, decided_at=fit_at),
        lambda n: _cand("cand1", decided_by="owner@local",
                        decided_at=fit_at - timedelta(hours=1)),
        lambda n: _cand("cand0", decided_by="owner@local",
                        decided_at=fit_at - timedelta(hours=2)),
        lambda n: _cand("pending1", decision="pending", decided_by="owner@local",
                        decided_at=n - timedelta(minutes=30)),
        lambda n: _cand(None, decided_by="owner@local", decided_at=n),
        lambda n: _cand(None, decided_by=discovery.FIT_ACTOR,
                        decided_at=n - timedelta(hours=25)),
    ]


@pytest.mark.parametrize("db_ready", [
    {"candidates": _budget_candidates,
     "limits": {"discovery_autoconnect_enabled": 1,
                "discovery_auto_joins_per_day": 3}},
    {"candidates": _budget_candidates,
     "limits": {"discovery_autoconnect_enabled": 1,
                "discovery_auto_joins_per_day": 1}},
], indirect=True)
def test_approve_tick_connects_within_budget(db_ready, monkeypatch):
    """T-26: ровно `room` подключений — первые по `(decided_at, id)`, любого
    `decided_by`; кандидаты остаются `approved`, аудит — по одному на
    подключение. При per_day=1 остаток исчерпан сегодняшним автоодобрением —
    тик холостой."""
    _stub_no_engage(monkeypatch)
    chain = _stub_chain(monkeypatch)
    created = _stub_create_external(monkeypatch)
    _stub_fleet(monkeypatch, [{"account_id": 5, "status": "active"}])
    per_day = db_ready.spec["limits"]["discovery_auto_joins_per_day"]

    out = _tick()
    if per_day == 1:
        assert out == {"started": 0, "approved_waiting": 3, "room": 0}, out
        assert chain == [] and created == []
        assert _audits("channel_add_started") == []
        return

    assert out == {"started": 2, "approved_waiting": 3, "room": 2}, out
    # Порядок — FIFO по (decided_at, id), контракт §3.4: cand2 (самый свежий) ждёт остатка.
    assert [c["username"] for c in chain] == ["cand0", "cand1"], chain
    assert all(c["stage"] == "channel" for c in chain), chain
    assert [c["kind"] for c in created] == ["channel_add"] * 2, created
    assert [c["name"] for c in created] == ["Подключение канала · @cand0",
                                            "Подключение канала · @cand1"], created
    assert all(c["user_email"] == "auto:approve" for c in created), created
    assert all(c["params"] == {"username": u, "engage_account_id": 5}
               for c, u in zip(created, ("cand0", "cand1"))), created
    assert all(c["run_id"] == ch["run_id"] for c, ch in zip(created, chain)), created

    # Кандидатов закрывает штатный `_connect_approved`, не постановка.
    after = _candidates()
    assert after["cand1"].decision == "approved"
    assert after["cand2"].decision == "approved"

    audits = _audits("channel_add_started")
    assert len(audits) == 2, audits
    assert all(a.user_email == "auto:approve" for a in audits), audits
    assert {a.detail["username"] for a in audits} == {"cand0", "cand1"}, audits


# ── T-27: JobBusy — стоп перебора, не ошибка ──────────────────────────────────

@pytest.mark.parametrize("db_ready", [{
    "candidates": _tick_candidates(fit_pair=False),
    "limits": {"discovery_autoconnect_enabled": 1,
               "discovery_auto_joins_per_day": 3},
    "busy": True,
}], indirect=True)
def test_approve_tick_stops_on_busy(db_ready, monkeypatch):
    """T-27: занятый вид `channel_add` (настоящий `create_external` ловит его
    по строке runs) молча заканчивает удар; освободившийся вид доедается
    следующим тиком — два подключения."""
    _stub_no_engage(monkeypatch)
    chain = _stub_chain(monkeypatch)
    _stub_fleet(monkeypatch, [{"account_id": 5, "status": "active"}])

    # Первый удар: занято — исключение не покинуло тик, постановок нет.
    out = _tick()
    assert out == {"started": 0, "approved_waiting": 2, "room": 3}, out
    assert chain == []
    assert [r.id for r in _runs("channel_add")] == [db_ready.busy_run]
    assert _audits("channel_add_started") == []

    # Вид освободился — следующий тик доедает обоих.
    _set_run_done(db_ready.busy_run)
    created = _stub_create_external(monkeypatch)
    out2 = _tick()
    assert out2 == {"started": 2, "approved_waiting": 2, "room": 3}, out2
    assert [c["username"] for c in chain] == ["cand1", "cand2"], chain
    assert [c["user_email"] for c in created] == ["auto:approve"] * 2, created
    assert len(_audits("channel_add_started")) == 2
