"""Ручки раздела Discovery — через HTTP, на настоящем Postgres.

Служба (`app/services/discovery.py`) и модели уже есть; здесь проверяется именно
поверхность: конверт списка, коды отказов и права. Сервисный уровень эти ручки
не показывает — анониму ручка отвечать 401, чужой фильтр 422 с перечнем
допустимого, а не 500 или молчаливый пустой список.

Отдельно закрепляется отсутствие ручки `connect` (§3.4): подключение идёт через
существующий `POST /api/v1/channels`, и второй дороги быть не должно — тест
стоит дешевле, чем спор о «неудобстве».

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
"""
from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_INGEST_TOKEN", "test-ingest-token")
os.environ.setdefault("RADAR_DEBUG", "true")

from app.core.config import get_settings  # noqa: E402
from app.core.security import SessionSigner  # noqa: E402
from app.db.models import (AuditLog, Base, Channel, ChannelCandidate,  # noqa: E402
                           DiscoveryQuery, EngageInstance, Run, User)
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.main import create_app  # noqa: E402
from app.services import engage  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")


def _ts(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


async def _seed() -> dict:
    engine = create_async_engine(DB_URL, poolclass=None)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)

    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        # Инстанс Engage нужен даже там, где ручка в него не ходит: реестр
        # инстансов поднимается на старте приложения, и пустой реестр — это не
        # «нет клиентов», а несобранное приложение.
        db.add(EngageInstance(key="default", client_label="Тестовый",
                              base_url="http://engage.invalid",
                              api_key_env="RADAR_ENGAGE_API_KEY", is_active=True))
        seed_channel = Channel(peer_id=-1001, username="seedchannel",
                               title="Семя", chat_type="channel",
                               ingest_enabled=True, members=150_000)

        users = {}
        for role in ("owner", "customer", "viewer"):
            u = User(email=f"{role}@local", name=role, initials=role[:2].upper(),
                     role=role, password_hash="!нельзя-войти", totp_secret="X" * 32,
                     totp_confirmed=True, is_active=True)
            db.add(u)
            users[role] = u

        # found_at задан руками, а не умолчанием: в одной транзакции func.now()
        # даёт всем строкам ОДНО время, и сортировка по `created` стала бы
        # сортировкой по tiebreak — проверить «created — это found_at» было бы
        # нечем.
        pending_old = ChannelCandidate(
            username="oldbank", title="Старый банк", members=800,
            chat_type="channel", source="similar", seed_channel_id=1,
            found_by_account_id=3, found_at=_ts(2026, 9, 1, 10, 0),
            decision="pending")
        pending_new = ChannelCandidate(
            username="newbank", title="Новый банк", members=2_700_000,
            chat_type="channel", source="similar", seed_channel_id=1,
            found_by_account_id=3, found_at=_ts(2026, 9, 3, 10, 5),
            decision="pending")
        connected = ChannelCandidate(
            username="linkbank", title="Подключённый", members=5_000,
            chat_type="channel", source="similar", seed_channel_id=1,
            found_by_account_id=3, found_at=_ts(2026, 9, 2, 10, 0),
            decision="connected", decided_by="owner@local",
            decided_at=_ts(2026, 9, 2, 11, 0))
        rejected = ChannelCandidate(
            username="badbank", title="Отклонённый", members=900,
            chat_type="channel", source="search", found_by_account_id=3,
            found_at=_ts(2026, 9, 2, 12, 0), decision="rejected",
            decided_by="owner@local", decided_at=_ts(2026, 9, 2, 12, 30),
            decision_reason="не та тема", llm_verdict="unfit", llm_score=10,
            llm_reason="не та аудитория", llm_at=_ts(2026, 9, 2, 12, 20))
        db.add_all([seed_channel, pending_old, pending_new, connected, rejected])
        await db.commit()
        out = {"uids": {r: u.id for r, u in users.items()},
               "seed_channel": seed_channel.id,
               "pending_old": pending_old.id, "pending_new": pending_new.id,
               "connected": connected.id, "rejected": rejected.id}
    await engine.dispose()
    return out


@pytest.fixture
def seeded():
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


def _login(client: TestClient, uid: int) -> None:
    token = SessionSigner(get_settings().SECRET_KEY).dumps({"uid": uid, "totp_ok": True})
    client.cookies.set(get_settings().SESSION_COOKIE, token)


def _audit_actions() -> list[str]:
    async def go():
        engine = create_async_engine(DB_URL, poolclass=None)
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with maker() as db:
            rows = list((await db.execute(
                select(AuditLog.action).order_by(AuditLog.id))).all())
        await engine.dispose()
        return [a for (a,) in rows]

    return asyncio.run(go())


def _candidates() -> list[ChannelCandidate]:
    async def go():
        engine = create_async_engine(DB_URL, poolclass=None)
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with maker() as db:
            rows = list((await db.execute(
                select(ChannelCandidate).order_by(ChannelCandidate.id)
            )).scalars().all())
        await engine.dispose()
        return rows

    return asyncio.run(go())


def _count(model) -> int:
    async def go():
        engine = create_async_engine(DB_URL, poolclass=None)
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with maker() as db:
            n = len(list((await db.execute(select(model))).scalars().all()))
        await engine.dispose()
        return n

    return asyncio.run(go())


# ── §3.1 — список кандидатов ──────────────────────────────────────────────────

def test_list_returns_envelope_with_sorts_and_states(client, seeded):
    """Конверт — как у `/channels`: total/limit/offset/rows плюс sorts.
    Конверт `items` очереди дочитывания здесь не образец."""
    _login(client, seeded["uids"]["owner"])
    r = client.get("/api/v1/discovery/candidates")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["total"] == 4
    assert body["limit"] == 50 and body["offset"] == 0
    assert len(body["rows"]) == 4
    assert body["sorts"] == ["created", "members", "title"]
    assert body["states"] == [{"key": "pending", "count": 2},
                              {"key": "approved", "count": 0},
                              {"key": "rejected", "count": 1},
                              {"key": "connected", "count": 1}]
    # Даты — ISO-строки, не объекты: экран подставляет их как есть.
    assert body["rows"][0]["found_at"].endswith("+00:00")
    row = next(r_ for r_ in body["rows"] if r_["username"] == "badbank")
    assert row["decision"] == "rejected" and row["decision_reason"] == "не та тема"
    assert row["llm_verdict"] == "unfit" and row["llm_score"] == 10


def test_unknown_state_filter_names_the_allowed_ones(client, seeded):
    """Чужое значение фильтра — 422 с перечнем допустимого, а не пустой список:
    пустой список читается как «данных нет», а не «фильтр кривой»."""
    _login(client, seeded["uids"]["owner"])
    r = client.get("/api/v1/discovery/candidates", params={"state": "bogus"})
    assert r.status_code == 422, r.text
    for allowed in ("pending", "approved", "rejected", "connected"):
        assert allowed in r.text, r.text


def test_unknown_sort_name_is_rejected_by_apply_sort(client, seeded):
    """Неизвестную сортировку отвергает общий `apply_sort` — проверяем, что
    запрос дошёл до него, а не отвергнут кем-то по дороге: в тексте отказа
    перечислены допустимые поля."""
    _login(client, seeded["uids"]["owner"])
    r = client.get("/api/v1/discovery/candidates", params={"sort": "password"})
    assert r.status_code == 422, r.text
    assert "created" in r.text and "members" in r.text and "title" in r.text


def test_sort_created_is_sorted_by_found_at_both_ways(client, seeded):
    """`sort=created` — это колонка `found_at` (REVIEW §2): проверяем оба
    направления по разным меткам времени, а не по tiebreak."""
    _login(client, seeded["uids"]["owner"])
    desc = client.get("/api/v1/discovery/candidates",
                      params={"sort": "created", "order": "desc"}).json()["rows"]
    asc = client.get("/api/v1/discovery/candidates",
                     params={"sort": "created", "order": "asc"}).json()["rows"]
    assert [r_["username"] for r_ in desc] == [
        "newbank", "badbank", "linkbank", "oldbank"]
    assert [r_["username"] for r_ in asc] == list(
        reversed([r_["username"] for r_ in desc]))


def test_viewer_is_outside_the_whole_section(client, seeded):
    """Раздел целиком за `channels`, а тот — штат (owner/customer/reviewer):
    гостю (viewer) закрыт и список, и история, и тем более решения с поиском."""
    _login(client, seeded["uids"]["viewer"])
    assert client.get("/api/v1/discovery/candidates").status_code == 403
    assert client.get("/api/v1/discovery/queries").status_code == 403
    denied = client.post(
        f"/api/v1/discovery/candidates/{seeded['pending_old']}/decide",
        json={"decision": "rejected", "reason": "не пускают"})
    assert denied.status_code == 403, denied.text
    denied_scan = client.post("/api/v1/discovery/scan",
                              json={"kind": "search", "query": "бухгалтерия"})
    assert denied_scan.status_code == 403, denied_scan.text
    assert _count(Run) == 0


# ── §3.3 — решения ────────────────────────────────────────────────────────────

def test_decide_rejected_without_reason_is_refused(client, seeded):
    """Отказ без причины — не данные (B.2): ручка его не принимает, и пустая
    строка из пробелов — тоже отказ без причины."""
    _login(client, seeded["uids"]["owner"])
    no_reason = client.post(
        f"/api/v1/discovery/candidates/{seeded['pending_old']}/decide",
        json={"decision": "rejected"})
    assert no_reason.status_code == 422, no_reason.text
    blank = client.post(
        f"/api/v1/discovery/candidates/{seeded['pending_old']}/decide",
        json={"decision": "rejected", "reason": "   "})
    assert blank.status_code == 422, blank.text
    assert _candidates()[0].decision == "pending"


def test_decide_on_connected_candidate_is_conflict(client, seeded):
    """`connected` — точка невозврата: подключение уже факт, решения по нему
    больше не меняются."""
    _login(client, seeded["uids"]["owner"])
    r = client.post(
        f"/api/v1/discovery/candidates/{seeded['connected']}/decide",
        json={"decision": "rejected", "reason": "поздно"})
    assert r.status_code == 409, r.text
    row = next(c for c in _candidates() if c.id == seeded["connected"])
    assert row.decision == "connected" and row.decided_by == "owner@local"


def test_decide_on_unknown_id_is_not_found(client, seeded):
    _login(client, seeded["uids"]["owner"])
    r = client.post("/api/v1/discovery/candidates/999999/decide",
                    json={"decision": "approved"})
    assert r.status_code == 404, r.text


def test_decide_writes_the_decision_and_the_audit_row(client, seeded):
    """Позитивная ветка перехода pending → rejected: поля решения, email
    оператора и строка журнала действий — по образцу соседей."""
    _login(client, seeded["uids"]["owner"])
    r = client.post(
        f"/api/v1/discovery/candidates/{seeded['pending_old']}/decide",
        json={"decision": "rejected", "reason": "не та тема"})
    assert r.status_code == 200, r.text
    row = next(c for c in _candidates() if c.id == seeded["pending_old"])
    assert row.decision == "rejected" and row.decided_by == "owner@local"
    assert row.decided_at is not None and row.decision_reason == "не та тема"
    assert "discovery_candidate_decided" in _audit_actions()


# ── §3.2 — запуск поиска ──────────────────────────────────────────────────────

def _seed_searched(*, query: str) -> None:
    """Строка «эту цель уже искали сегодня»: дедуп цели ручка проверяет по
    строкам `discovery_queries`, и в seeded-данных их нет."""
    async def go():
        engine = create_async_engine(DB_URL, poolclass=None)
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with maker() as db:
            db.add(DiscoveryQuery(kind="search", query=query,
                                  account_id=3, found_total=0, new_total=0))
            await db.commit()
        await engine.dispose()

    asyncio.run(go())


def _stub_engage_for_scan(monkeypatch) -> list:
    """Engage из двух шагов для фоновых прогонов scan. Очереди в тестах нет,
    прогон живёт корутиной в процессе API, и без заглушки ходил бы в
    http://engage.invalid. Отвечает парой каналов — прогон доживает до
    кандидатов и своей строки `discovery_queries` (шаг §3.2 целиком)."""
    calls: list = []

    async def action(*, account_id, action, payload, webhook_url, **kw):
        calls.append((account_id, action, payload))
        return {"task_id": f"t{len(calls)}"}

    async def wait_for_task(task_id, **kw):
        return {"channels": [{"title": "Банк один", "username": "bankone",
                              "members_count": 5000},
                             {"title": "Банк два", "username": "banktwo",
                              "members_count": 9000}]}

    monkeypatch.setattr(engage, "action", action)
    monkeypatch.setattr(engage, "wait_for_task", wait_for_task)
    return calls


def _run_statuses() -> list[str]:
    async def go():
        engine = create_async_engine(DB_URL, poolclass=None)
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with maker() as db:
            rows = list((await db.execute(
                select(Run.status).where(Run.kind == "discovery_scan")
                .order_by(Run.id))).all())
        await engine.dispose()
        return [s for (s,) in rows]

    return asyncio.run(go())


def _await_scan_settled(timeout: float = 10.0) -> None:
    """Дождаться, пока фоновые прогоны scan дойдут до терминального статуса.
    Без очереди прогон исполняется корутиной в процессе API; не дождавшись,
    следующий POST /scan упёрся бы в `JobBusy` — это была бы гонка теста,
    а не свойство ручки."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not {"queued", "running"} & set(_run_statuses()):
            return
        time.sleep(0.05)
    pytest.fail("фоновые прогоны discovery_scan не завершились вовремя")


def test_six_distinct_searches_a_day_all_start_runs(client, seeded, monkeypatch):
    """Суточного лимита поисков больше нет (Решение 1): шесть разных поисков
    за одни UTC-сутки — шесть прогонов, и 409 «продолжайте завтра» не существует.
    Число «5» теперь про объём одного прогона и живёт в службе, а не в ручке."""
    _stub_engage_for_scan(monkeypatch)
    _login(client, seeded["uids"]["owner"])
    for i in range(6):
        r = client.post("/api/v1/discovery/scan",
                        json={"kind": "search", "query": f"поиск номер {i}",
                              "account_id": 3})
        assert r.status_code == 202, r.text
        assert r.json()["started"] is True
        _await_scan_settled()
    statuses = _run_statuses()
    assert len(statuses) == 6 and all(s == "done" for s in statuses), statuses
    # 202 = «прогон заведён», и каждый прогон отработал до конца: свои строки
    # `discovery_queries` на месте
    assert _count(DiscoveryQuery) == 6


def test_duplicate_target_still_conflicts(client, seeded):
    """Регресс: уникальность цели за UTC-сутки — дедуп, а не счётчик, Решение 1
    её не отменял. Повторный поиск по той же строке — 409, и прогона нет."""
    _seed_searched(query="бухгалтерия")
    _login(client, seeded["uids"]["owner"])
    r = client.post("/api/v1/discovery/scan",
                    json={"kind": "search", "query": "бухгалтерия",
                          "account_id": 3})
    assert r.status_code == 409, r.text
    assert "уже искали" in r.text, r.text
    assert _count(Run) == 0


def test_queries_reports_per_run_not_today(client, seeded):
    """Вместо суточного остатка — объём прогона (Решение 1): `per_run.queries`
    из порогов службы; поля `today` не существует."""
    _login(client, seeded["uids"]["owner"])
    body = client.get("/api/v1/discovery/queries").json()
    assert body["per_run"] == {"queries": 5}
    assert "today" not in body


def test_scan_similar_with_search_query_is_refused(client, seeded):
    """Адресация ровно одна: «похожие» со строкой поиска — ошибка тела,
    а не молчаливое игнорирование лишнего поля."""
    _login(client, seeded["uids"]["owner"])
    r = client.post("/api/v1/discovery/scan",
                    json={"kind": "similar", "query": "бухгалтерия"})
    assert r.status_code == 422, r.text
    assert _count(Run) == 0


# ── §3.4 — ручки connect НЕТ ──────────────────────────────────────────────────

def test_connect_route_does_not_exist(client, seeded):
    """Закрепляем отсутствие ручки (§3.4): подключение зовёт существующий
    POST /api/v1/channels. Если кто-то заведёт /connect по недосмотру — тест
    упадёт раньше, чем появится вторая дорога «нашёл → вступил»."""
    _login(client, seeded["uids"]["owner"])
    r = client.post(
        f"/api/v1/discovery/candidates/{seeded['pending_old']}/connect")
    assert r.status_code == 404, r.text


# ── права: каждая ручка обязана объявить пользователя ─────────────────────────

def test_every_handle_is_closed_to_anonymous(client, seeded):
    """Без пользователя — 401 на всех четырёх ручках. Ручка, которая отдаёт
    данные анониму до проверки прав, не должна ждать обнаружения на проде."""
    assert client.get("/api/v1/discovery/candidates").status_code == 401
    assert client.post("/api/v1/discovery/scan",
                       json={"kind": "search", "query": "бухгалтерия"}
                       ).status_code == 401
    assert client.post(
        f"/api/v1/discovery/candidates/{seeded['pending_old']}/decide",
        json={"decision": "approved"}).status_code == 401
    assert client.get("/api/v1/discovery/queries").status_code == 401
