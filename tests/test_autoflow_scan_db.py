"""Сценарий 3 «скан подбора по расписанию» (волна Г): доноры, мульти-семя, тик.

Что здесь проверяется:

* **колонка `discovery_seed` и её DDL** (T-16): колонку на проде досоздаёт
  только строка `STATEMENTS` — `create_all` существующие таблицы не меняет,
  и забыть DDL значило бы уронить первый же запрос «column does not exist»;
* **мульти-семя под рамкой потолка** (T-17): один прогон обходит список
  доноров, заказов у Engage всё равно не больше `discovery_queries_per_scan`;
* **гонка с ручным сканом не валит прогон** (T-18): семя, иска́нное сегодня,
  ловится уникальностью суток и пропускается;
* **тик**: выключенный — пустой итог с числом доноров (T-19), включённый
  ставит прогон дословно с семенами по `members DESC NULLS LAST, id` (T-20),
  сутки следят за собой (T-21), окно отложенного возврата бережёт бюджет
  (T-22).

Skip живёт на фикстуре, а не `pytestmark`: первый пункт T-16 обязан идти без
базы (проверка списка `STATEMENTS`). Инфраструктура — по соседям
(`test_autoflow_reclassify_db.py`): `RADAR_TEST_DATABASE_URL`, `_reset()`
пересеивает схему, один `asyncio.run` на тест, `jobs.start` перехватывается
заглушкой — тик обязан только ПОСТАВИТЬ прогон.

Посев блока Г (TESTS): четыре донора `d4000/d3000/d2000/d1000` по числу
участников, плюс три канала-ловушки — без флага донора, без username и с
выключенным приёмом: сломанный фильтр отбора тут же взял бы лишних (7000 и
6000 участников больше, чем у любого донора).
"""
from __future__ import annotations

import asyncio
import os
from datetime import timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_INGEST_TOKEN", "test-ingest-token")

from app.core import clock  # noqa: E402
from app.db.models import (AuditLog, Base, Channel, DiscoveryQuery,  # noqa: E402
                           Limit, Run)
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.services import autoflow, discovery, engage, jobs  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

# Посев блока Г по умолчанию (TESTS): сценарий включён, потолок поисков —
# умолчание 5, доноров 4.
DEFAULT_LIMITS = {"discovery_autoscan_enabled": 1}


async def _reset(limits: dict[str, int], *, queries=(), runs=None
                 ) -> dict[str, int]:
    """Схема заново + доноры с каналами-ловушками; возвращает id по имени.

    `queries` — строки `discovery_queries` сегодня (формат: `{"kind": "similar",
    "seed": <имя>}` или `{"kind": "search", "query": <строка>}`); `runs` —
    отложенный прогон скана (`offset_min`, `retry` — названо ли окно).
    """
    engine = create_async_engine(DB_URL, poolclass=None)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        # (имя, участники, донор, приём включён). Ловушки впереди: у них
        # участников больше, чем у любого донора, — сломанный фильтр отбора
        # поставил бы их в порцию первыми.
        spec = [("nodonor", 5000, False, True),
                ("nouser", 6000, True, True),
                ("off", 7000, True, False),
                ("d1000", 1000, True, True),
                ("d3000", 3000, True, True),
                ("d2000", 2000, True, True),
                ("d4000", 4000, True, True)]
        ids: dict[str, int] = {}
        peer = -100_200
        for name, members, seed, enabled in spec:
            peer += 1
            # Ловушка «nouser» — донор без username: только у неё имя канала
            # пустое, у остальных имя канала совпадает с именем посева.
            ch = Channel(peer_id=peer,
                         username=None if name == "nouser" else name,
                         title=f"Канал {name}", chat_type="channel",
                         members=members, ingest_enabled=enabled,
                         discovery_seed=seed)
            db.add(ch)
            await db.flush()
            ids[name] = ch.id
        now = clock.utcnow()
        # Прогон-хозяин id=1: `discovery_queries.run_id` — FK на `runs`, и прямой
        # вызов `run_scan(run_id=1)` без строки в runs ловил бы ForeignKey вместо
        # дела. Статус «done» и старый finished_at: вид не занят (`active_run`
        # пуст), окно отложенного не названо — посев T-22 кладёт свой deferred
        # свежее этого прогона.
        db.add(Run(id=1, name="контрольный скан", kind="discovery_scan",
                   params={}, status="done", progress=100, created_by="owner@x",
                   started_at=now - timedelta(hours=4),
                   finished_at=now - timedelta(hours=3), result={}, log=[]))
        # Последовательность id — за явной единицей, иначе следующий run без
        # явного id (посев T-22) получил бы тот же id=1 и упал на pkey.
        await db.execute(text(
            "SELECT setval(pg_get_serial_sequence('runs', 'id'), 1)"))
        for q in queries:
            if q["kind"] == "similar":
                db.add(DiscoveryQuery(kind="similar",
                                      seed_channel_id=ids[q["seed"]],
                                      account_id=1, created_at=now))
            else:
                db.add(DiscoveryQuery(kind="search", query=q["query"],
                                      seed_channel_id=None, account_id=1,
                                      created_at=now))
        if runs is not None:
            result: dict = {"deferred": True}
            if runs.get("retry"):
                result["retry_after_s"] = 3600
            db.add(Run(name="автоскан", kind="discovery_scan", params={},
                       status="deferred", progress=0, created_by="auto:scan",
                       finished_at=now - timedelta(minutes=runs["offset_min"]),
                       result=result, log=[]))
        for key, value in limits.items():
            db.add(Limit(key=key, value=value))
        await db.commit()
    await engine.dispose()
    return ids


@pytest.fixture()
def db_ready(request):
    """Пересеянная база + кеши движка под тестовую; в таблице параметров —
    `limits` (переопределение посева), `queries` (иски сегодня) и `runs`
    (отложенный прогон скана). Отдаёт `ids` (id каналов по имени) и `params`.
    """
    if not DB_URL:
        pytest.skip("нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")
    params = getattr(request, "param", None) or {}
    limits = dict(DEFAULT_LIMITS)
    limits.update(params.get("limits") or {})
    os.environ["RADAR_DATABASE_URL"] = DB_URL
    from app.core.config import get_settings

    get_settings.cache_clear()
    get_engine.cache_clear()
    get_session_maker.cache_clear()
    ids = asyncio.run(_reset(limits, queries=params.get("queries", ()),
                             runs=params.get("runs")))
    yield SimpleNamespace(ids=ids, params=params, limits=limits)


def _stub_start(monkeypatch, log: dict, *, busy_flag: list | None = None) -> None:
    """`jobs.start`, который записывает аргументы и возвращает прогон с id.

    Счётчик `attempts` видит и неудачные попытки: «тик не дошёл до постановки»
    и «дошёл, но поймал JobBusy» — разные исходы с одним тихим итогом.
    """

    async def fake_start(db, *, kind, params, name, user_email):
        log["attempts"] += 1
        if busy_flag is not None and busy_flag[0]:
            raise jobs.JobBusy(f"задача «{kind}» уже идёт (#99, 42%)")
        log["calls"].append({"kind": kind, "params": params, "name": name,
                             "user_email": user_email})
        return SimpleNamespace(id=1)

    monkeypatch.setattr(jobs, "start", fake_start)


def _log() -> dict:
    return {"calls": [], "attempts": 0}


def _stub_fleet(monkeypatch, accounts: list[dict]) -> None:
    """`engage.list_accounts` без сети: флот из списка теста."""

    async def list_accounts(*, instance=None):
        return list(accounts)

    monkeypatch.setattr(engage, "list_accounts", list_accounts)


def _stub_engage_scan(monkeypatch) -> list:
    """Поиск из двух шагов со счётчиком: `action` фиксирует заказ,
    `wait_for_task` отвечает одним найденным каналом с уникальным именем —
    у каждого семени `found_total == 1` и `new_total == 1`."""
    calls: list = []

    async def action(*, account_id, action, payload, webhook_url, **kw):
        calls.append((account_id, action, payload))
        return {"task_id": f"t{len(calls)}"}

    async def wait_for_task(task_id, **kw):
        n = int(str(task_id).lstrip("t") or 0)
        return {"channels": [{"username": f"found_{n}",
                              "title": f"Найден {n}", "members_count": 900}]}

    monkeypatch.setattr(engage, "action", action)
    monkeypatch.setattr(engage, "wait_for_task", wait_for_task)
    return calls


def _report_sink():
    notes = []

    async def report(pct, note):
        notes.append(note)

    return report, notes


# ── T-16: колонка и DDL ───────────────────────────────────────────────────────

def test_discovery_seed_ddl_in_statements():
    """T-16, первый пункт (без базы): DDL-строка донора живёт в STATEMENTS
    дословно. `create_all` существующие таблицы не меняет — колонку на проде
    досоздаёт только этот список, и без неё первый запрос падает
    «column does not exist» (образец — test_jobs.py::test_migrations_only_
    add_never_drop)."""
    from app.db.migrate import STATEMENTS
    expected = ("ALTER TABLE channels ADD COLUMN IF NOT EXISTS discovery_seed "
                "BOOLEAN NOT NULL DEFAULT FALSE")
    assert expected in STATEMENTS, (
        "в STATEMENTS нет дословной строки DDL для channels.discovery_seed")


def test_discovery_seed_column_and_ddl(db_ready):
    """T-16, второй пункт (с базой): у свежего канала `discovery_seed` — False
    сразу после flush: DEFAULT FALSE выкатку не меняет, включение — осознанное
    действие владельца."""
    async def go():
        async with get_session_maker()() as db:
            ch = Channel(peer_id=-100_900_001, username="fresh",
                         title="Свежий", chat_type="channel")
            db.add(ch)
            await db.flush()
            flag = ch.discovery_seed
            await db.rollback()
            return flag

    assert asyncio.run(go()) is False


# ── T-17: мульти-семя под рамкой потолка ──────────────────────────────────────

@pytest.mark.parametrize("db_ready", [
    {},
    {"limits": {"discovery_queries_per_scan": 2}},
], indirect=True)
def test_run_scan_multi_seed_under_cap(db_ready, monkeypatch):
    """T-17: прогон обходит семена в порядке передачи, по строке на семя
    (run_id, found_total, new_total у каждой); рамка потолка одна на прогон —
    при `per_scan = 2` заказов ровно 2 и обрабатываются первые два семени."""
    calls = _stub_engage_scan(monkeypatch)
    ids = db_ready.ids
    seed_order = [ids["d1000"], ids["d2000"], ids["d3000"], ids["d4000"]]

    async def go():
        report, _notes = _report_sink()
        out = await discovery.run_scan(
            1, params={"kind": "similar", "seed_channel_ids": seed_order,
                       "account_id": 1},
            report=report, cancelled=lambda: False)
        async with get_session_maker()() as db:
            rows = (await db.execute(select(DiscoveryQuery)
                                     .order_by(DiscoveryQuery.id)))
            rows = rows.scalars().all()
        return out, rows

    out, rows = asyncio.run(go())
    cap = db_ready.params.get("limits", {}).get("discovery_queries_per_scan", 5)
    if cap == 2:
        assert len(calls) == 2, calls
        assert [p["username"] for _, _a, p in calls] == ["d1000", "d2000"]
        assert out == {"seeds": 2, "found_total": 2, "new_total": 2,
                       "skipped_window": 0}, out
        assert [q.seed_channel_id for q in rows] == seed_order[:2], rows
    else:
        assert len(calls) == 4, calls
        assert out == {"seeds": 4, "found_total": 4, "new_total": 4,
                       "skipped_window": 0}, out
        assert len(rows) == 4, rows
        assert all(q.run_id == 1 for q in rows), rows
        assert all(q.found_total == 1 and q.new_total == 1 for q in rows), rows
        assert {q.seed_channel_id for q in rows} == set(seed_order), rows


# ── T-18: гонка с ручным сканом и кривые params ───────────────────────────────

@pytest.mark.parametrize("db_ready",
                         [{"queries": [{"kind": "similar", "seed": "d3000"}]}],
                         indirect=True)
def test_run_scan_skips_searched_seed(db_ready, monkeypatch):
    """T-18: семя, иска́нное сегодня, ловится уникальностью суток внутри прогона
    — откат, `skipped_window`, следующий семен; прогон завершается, а не падает.
    Кривые params (оба способа выбрать семя, пустой список, не-similar) —
    RuntimeError до первого заказа."""
    calls = _stub_engage_scan(monkeypatch)
    ids = db_ready.ids

    async def go():
        report, _notes = _report_sink()
        out = await discovery.run_scan(
            1, params={"kind": "similar",
                       "seed_channel_ids": [ids["d1000"], ids["d3000"],
                                            ids["d2000"]],
                       "account_id": 1},
            report=report, cancelled=lambda: False)
        async with get_session_maker()() as db:
            rows = (await db.execute(select(DiscoveryQuery))).scalars().all()
        # Кривые params (оба способа выбрать семя, пустой список, не-similar) —
        # RuntimeError до первого заказа. Один asyncio.run на тест: движок
        # кешируется между вызовами.
        base = {"kind": "similar", "account_id": 1}
        for extra in ({"seed_channel_ids": [ids["d1000"]],
                       "seed_channel_id": ids["d1000"]},
                      {"seed_channel_ids": []},
                      {"seed_channel_ids": [ids["d1000"]], "kind": "search",
                       "query": "бухгалтерия"}):
            with pytest.raises(RuntimeError):
                await discovery.run_scan(1, params={**base, **extra},
                                         report=report,
                                         cancelled=lambda: False)
        return out, rows

    out, rows = asyncio.run(go())
    assert out == {"seeds": 2, "found_total": 2, "new_total": 2,
                   "skipped_window": 1}, out
    assert {q.seed_channel_id for q in rows if q.kind == "similar"} == {
        ids["d1000"], ids["d2000"], ids["d3000"]}, rows
    # Заказов три: гонка обнаруживается на коммите, когда заказ уже сделан, —
    # цена договорённости «окно ловит уникальность суток, не предопрос».
    assert len(calls) == 3, calls


# ── T-19: выключенный тик ─────────────────────────────────────────────────────

@pytest.mark.parametrize("db_ready",
                         [{"limits": {"discovery_autoscan_enabled": 0}}],
                         indirect=True)
def test_scan_tick_disabled(db_ready, monkeypatch):
    """T-19: выключенный сценарий не ставит прогонов, но доноры считаются —
    возврат тика обязан показывать, что флот доноров есть и ждёт включения."""
    log = _log()
    _stub_start(monkeypatch, log)

    out = asyncio.run(autoflow.scan_tick({}))

    assert log["attempts"] == 0, "выключенный сценарий не ставит прогонов"
    assert out == {"started": False, "busy": False, "window_wait": False,
                   "seeds": 0, "donors": 4}, out


# ── T-20: тик ставит прогон по донорам ────────────────────────────────────────

@pytest.mark.parametrize("db_ready", [
    {},
    {"limits": {"discovery_queries_per_scan": 3}},
], indirect=True)
def test_scan_tick_starts_multi_seed_run(db_ready, monkeypatch):
    """T-20: первый удар ставит ровно один прогон дословно — семена в порядке
    `members DESC NULLS LAST, id`, аккаунт из флота, автор `auto:scan`, аудит
    с run_id; потолок режет порцию до K = min(доноры, per_scan); ловушки
    (без username, с выключенным приёмом, без флага) в отбор не попадают."""
    log = _log()
    _stub_start(monkeypatch, log)
    _stub_fleet(monkeypatch, [{"account_id": 7, "status": "active"}])
    ids = db_ready.ids

    async def go():
        out = await autoflow.scan_tick({})
        async with get_session_maker()() as db:
            audits = [(a.action, a.user_email, a.user_id, a.detail)
                      for a in (await db.execute(select(AuditLog)))
                      .scalars().all()]
        return out, audits

    out, audits = asyncio.run(go())
    cap = db_ready.params.get("limits", {}).get("discovery_queries_per_scan", 5)
    expected = [ids["d4000"], ids["d3000"], ids["d2000"], ids["d1000"]][:cap]
    assert out == {"started": True, "busy": False, "window_wait": False,
                   "seeds": len(expected), "donors": 4}, out
    assert log["attempts"] == 1, log
    call = log["calls"][0]
    assert call["kind"] == "discovery_scan", call
    assert call["user_email"] == "auto:scan", call
    assert call["name"] == (f"Поиск похожих каналов · "
                            f"доноры × {len(expected)}"), call
    assert call["params"] == {"kind": "similar",
                              "seed_channel_ids": expected,
                              "account_id": 7}, call
    assert len(audits) == 1, audits
    action, email, user_id, detail = audits[0]
    assert action == "discovery_scan_started", audits
    assert email == "auto:scan", audits
    assert user_id is None, audits
    assert detail == {"kind": "similar", "seed_channel_ids": expected,
                      "account_id": 7, "run_id": 1}, detail


# ── T-21: сутки следят за собой ───────────────────────────────────────────────

@pytest.mark.parametrize("db_ready", [
    {"queries": [{"kind": "similar", "seed": "d4000"}]},
    {"queries": [{"kind": "similar", "seed": "nodonor"}]},
    {"queries": [{"kind": "search", "query": "бухгалтерия"}]},
], indirect=True)
def test_scan_tick_skips_when_searched_today(db_ready, monkeypatch):
    """T-21: строка `similar` по донору в текущие UTC-сутки закрывает удар до
    полуночи; чужое семя и поиск по строке фильтр не останавливают."""
    log = _log()
    _stub_start(monkeypatch, log)
    _stub_fleet(monkeypatch, [{"account_id": 7, "status": "active"}])
    first = db_ready.params["queries"][0]
    stops = first["kind"] == "similar" and first.get("seed") == "d4000"

    out = asyncio.run(autoflow.scan_tick({}))

    if stops:
        assert out["started"] is False and out["seeds"] == 0, out
        assert out["donors"] == 4, out
        assert log["attempts"] == 0, log
    else:
        assert out["started"] is True, out
        assert log["attempts"] == 1, log


# ── T-22: окно отложенного возврата ───────────────────────────────────────────

@pytest.mark.parametrize("db_ready", [
    {"runs": {"offset_min": 10, "retry": True}},
    {"runs": {"offset_min": 120, "retry": True}},
    {"runs": {"offset_min": 10, "retry": False}},
], indirect=True)
def test_scan_tick_respects_deferred_window(db_ready, monkeypatch):
    """T-22: свежий отложенный прогон с названным окном (`retry_after_s = 3600`)
    держит удар — час бьётся впустую, лимит ещё не вернулся; окно истекло или
    не названо вовсе — прежний часовой ритм, прогон ставится."""
    log = _log()
    _stub_start(monkeypatch, log)
    _stub_fleet(monkeypatch, [{"account_id": 7, "status": "active"}])
    runs = db_ready.params["runs"]

    out = asyncio.run(autoflow.scan_tick({}))

    if runs["offset_min"] == 10 and runs["retry"]:
        assert out == {"started": False, "busy": False, "window_wait": True,
                       "seeds": 0, "donors": 4}, out
        assert log["attempts"] == 0, log
    else:
        assert out["started"] is True and not out["window_wait"], out
        assert log["attempts"] == 1, log
