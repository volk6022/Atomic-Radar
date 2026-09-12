"""Сценарий 2 «дочитал/принял → доклассифицировал» (волна В): тик и его условия.

Тик `autoflow.reclassify_tick` ставит автопрогон доклассификации ждущих L2 без
человека. Что здесь проверяется:

* **выключенный сценарий не ставит прогонов**, но ждущие считаются всегда:
  возврат тика — единственный отчёт arq-задачи о том, чем дышит очередь;
* **первый удар ставит прогон дословно**: вид, автор `auto:reclassify`, имя
  ручного запуска и `params` с порцией каналов — всё, что читают экраны Runs;
* **порция — топ по числу ждущих** (польза прогона упирается в потолок вопросов
  L3), при равенстве — меньший id первым: порядок обязан быть детерминирован;
* **окно интервала отсчитывается от любого завершённого прогона вида**: свежий
  ручной прогон разобрал тех же ждущих — второй заход сразу же не нужен;
* **«занято» проходит молча**: гонка постановки ловится `JobBusy` (и прямой
  активный прогон, и гонка между проверкой и `jobs.start`) — тик возвращает
  словарь, а не роняет воркер приёма;
* **автопрогон create-only**: `channel_ids` в params сам включает защиту —
  существующий лид не перезаписывается, сообщение досчитывается до L3.

`jobs.start` перехватывается заглушкой: тик обязан только ПОСТАВИТЬ прогон,
исполнять его здесь нечего (T-15 исполняет `reclassify.run` сам, с заглушками
ступеней по образцу `tests/test_reclassify_leads_db.py`). Заглушка `get_session_
maker` не нужна: фикстура ставит `RADAR_DATABASE_URL` и сбрасывает кеши — тик,
как `discovery_check_tick`, берёт сессию сам и приходит в тестовую базу (образец
— `tests/test_backfill_drain_db.py`).

Посев блока В: три канала с ждущими сообщениями 40/10/1 (в сумме 51), порция
по умолчанию 2. Инфраструктура — по соседям: skip-guard на
`RADAR_TEST_DATABASE_URL`, `_reset()` пересеивает схему, один `asyncio.run`
на тест (движок кешируется между вызовами).
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_INGEST_TOKEN", "test-ingest-token")

from app.core import clock  # noqa: E402
from app.db.models import (AuditLog, Base, Channel, Lead, Limit,  # noqa: E402
                           Message, Run)
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.services import autoflow, embeddings, jobs, llm, reclassify  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

NOW = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)

# Посев блока В по умолчанию (TESTS): сценарий включён, порция 2, потолок L3
# 200, интервал 60 минут; ждущих 40+10+1 = 51.
DEFAULT_LIMITS = {"autoflow_reclassify_enabled": 1,
                  "autoflow_reclassify_batch_channels": 2,
                  "autoflow_reclassify_l3_limit": 200,
                  "autoflow_reclassify_interval_min": 60}


async def _reset(limits: dict[str, int], *, tie: bool = False, solo: bool = False,
                 run: dict | None = None) -> dict[str, int]:
    """Схема заново + каналы с ждущими сообщениями; возвращает id по username.

    `tie` — два дополнительных канала с равным числом ждущих (проверка
    детерминированного порядка порции). `solo` — посев T-15: один канал с одним
    ждущим сообщением и существующим лидом. `run` — чужая строка `runs`
    (завершённая для окна интервала, активная для «занято»).
    """
    engine = create_async_engine(DB_URL, poolclass=None)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        spec = ([("auto_a", 1)] if solo
                else [("auto_a", 40), ("auto_b", 10), ("auto_c", 1)])
        if tie:
            spec += [("auto_d", 40), ("auto_e", 40)]
        ids: dict[str, int] = {}
        peer = -100_101
        for username, waiting in spec:
            peer += 1
            ch = Channel(peer_id=peer, username=username,
                         title=f"Канал {username}", chat_type="supergroup",
                         ingest_enabled=True,
                         # solo (T-15) — по образцу test_reclassify_leads_db.py:
                         # обход L1, чтобы прогон с заглушками дошёл до L3.
                         l1_bypass_enabled=solo)
            db.add(ch)
            await db.flush()
            ids[username] = ch.id
            for n in range(waiting):
                db.add(Message(channel_id=ch.id, tg_message_id=n + 1,
                               tg_date=NOW, author_peer_id=700 + n,
                               author_username=f"u{n}", author_name=f"Имя {n}",
                               author_is_bot=False, is_automatic_forward=False,
                               # solo — текст длиннее L1_BYPASS_MIN_TEXT (200):
                               # короче обход L1 уронил бы его на месте.
                               text=("сообщение ждёт доклассификации " * 8)
                               if solo else "сообщение ждёт доклассификации",
                               cascade_level=2, cascade_passed=None))
            if solo:
                # Лид по единственному сообщению (T-15): оценка посажена руками,
                # create-only обязан её сохранить.
                m1 = (await db.execute(select(Message).where(
                    Message.channel_id == ch.id))).scalar_one()
                db.add(Lead(message_id=m1.id, channel_id=ch.id,
                            author_peer_id=m1.author_peer_id,
                            author_username=m1.author_username, pain="боль",
                            quote="цитата", score=50,
                            score_breakdown=[{"label": "свежесть", "value": 10}],
                            status="new"))
        if run is not None:
            db.add(Run(name="чужой прогон", kind="reclassify", params={},
                       status=run["status"], progress=0,
                       created_by=run.get("created_by", "owner@x"),
                       finished_at=run.get("finished_at"), log=[]))
        for key, value in limits.items():
            db.add(Limit(key=key, value=value))
        await db.commit()
    await engine.dispose()
    return ids


@pytest.fixture()
def db_ready(request):
    """Пересеянная база + кеши движка под тестовую; в таблице параметров —
    `limits` (переопределение посева), `tie`/`solo` (варианты посева) и
    `run_offset_min`/`run` (чужая строка `runs`). Отдаёт `ids` (id каналов по
    username) и сами `params` — тестам надо различать варианты посева."""
    params = getattr(request, "param", None) or {}
    limits = dict(DEFAULT_LIMITS)
    limits.update(params.get("limits") or {})
    os.environ["RADAR_DATABASE_URL"] = DB_URL
    from app.core.config import get_settings

    get_settings.cache_clear()
    get_engine.cache_clear()
    get_session_maker.cache_clear()
    run: dict | None = None
    if "run_offset_min" in params:
        offset = params["run_offset_min"]
        if offset is not None:
            run = {"status": "done",
                   "finished_at": clock.utcnow() - timedelta(minutes=offset)}
    if "run" in params:
        run = params["run"]
    ids = asyncio.run(_reset(limits, tie=params.get("tie", False),
                             solo=params.get("solo", False), run=run))
    yield SimpleNamespace(ids=ids, params=params, limits=limits)


def _stub_start(monkeypatch, log: dict, *, busy_flag: list | None = None) -> None:
    """`jobs.start`, который записывает аргументы и возвращает прогон с id.

    Счётчик `attempts` видит и неудачные попытки: «тип не вызван» и «вызван, но
    поймал JobBusy» — разные исходы с одним тихим итогом. `busy_flag` имитирует
    гонку: активный прогон исчез между проверкой тика и постановкой.
    """

    async def fake_start(db, *, kind, params, name, user_email):
        log["attempts"] += 1
        if busy_flag is not None and busy_flag[0]:
            raise jobs.JobBusy("задача «reclassify» уже идёт (#99, 42%)")
        log["calls"].append({"kind": kind, "params": params, "name": name,
                             "user_email": user_email})
        return SimpleNamespace(id=1)

    monkeypatch.setattr(jobs, "start", fake_start)


def _log() -> dict:
    return {"calls": [], "attempts": 0}


# Заглушки ступеней T-15 — по образцу `tests/test_reclassify_leads_db.py`:
# эмбеддер всегда отдаёт одну картину близости, модель всегда соглашается.
RANKED_POS = [("pos", "банк не пропускает платёж", 0.60), ("neg", "офтоп", 0.40)]
LLM_YES = {"real_problem": True, "is_seller": False,
           "answering_someone_else": False, "why": "…"}


def stub_stages(monkeypatch) -> None:
    async def fake_prototype_vectors():
        return []

    async def fake_embed(texts):
        return [[0.0] for _ in texts]

    async def fake_verdict(*, text, context, prompt_key):
        return dict(LLM_YES), None

    monkeypatch.setattr(embeddings, "enabled", lambda: True)
    monkeypatch.setattr(embeddings, "prototype_vectors", fake_prototype_vectors)
    monkeypatch.setattr(embeddings, "embed", fake_embed)
    monkeypatch.setattr(embeddings, "rank", lambda vector, protos: list(RANKED_POS))
    monkeypatch.setattr(llm, "verdict", fake_verdict)


@pytest.mark.parametrize("db_ready",
                         [{"limits": {"autoflow_reclassify_enabled": 0}}],
                         indirect=True)
def test_reclassify_tick_disabled(db_ready, monkeypatch):
    """T-09: выключенный сценарий не ставит прогонов, но ждущие считаются —
    возврат тика обязан показывать, что очередь доклассификации не пуста."""
    log = _log()
    _stub_start(monkeypatch, log)

    out = asyncio.run(autoflow.reclassify_tick({}))

    assert log["attempts"] == 0, "выключенный сценарий не ставит прогонов"
    assert out == {"started": False, "busy": False, "interval_wait": False,
                   "waiting_channels": 3, "waiting_messages": 51}, out


def test_reclassify_tick_starts_run(db_ready, monkeypatch):
    """T-10: первый удар ставит прогон дословно — вид, автор `auto:reclassify`,
    имя как у ручного запуска и порция [A, B]; аудит `run_start` с run_id,
    видом и params, `user_id` пуст."""
    log = _log()
    _stub_start(monkeypatch, log)

    async def go():
        out = await autoflow.reclassify_tick({})
        async with get_session_maker()() as db:
            audits = [(a.action, a.user_email, a.user_id, a.detail)
                      for a in (await db.execute(select(AuditLog))).scalars().all()]
        return out, audits

    out, audits = asyncio.run(go())
    a_id, b_id = db_ready.ids["auto_a"], db_ready.ids["auto_b"]
    assert out["started"] is True and not out["busy"] and not out["interval_wait"], out
    assert out["waiting_channels"] == 3 and out["waiting_messages"] == 51, out
    assert log["attempts"] == 1, log
    call = log["calls"][0]
    assert call["kind"] == "reclassify", call
    assert call["user_email"] == "auto:reclassify", call
    assert call["name"] == (f"Переклассификация · недосчитанное · "
                            f"каналы {a_id}, {b_id}"), call
    assert call["params"] == {"scope": "pending", "channel_ids": [a_id, b_id],
                              "l3_limit": 200}, call
    assert len(audits) == 1, audits
    action, email, user_id, detail = audits[0]
    assert action == "run_start", audits
    assert email == "auto:reclassify", audits
    assert user_id is None, audits
    assert detail == {"run_id": 1, "kind": "reclassify",
                      "params": {"scope": "pending", "channel_ids": [a_id, b_id],
                                 "l3_limit": 200}}, detail


@pytest.mark.parametrize("db_ready", [
    {"limits": {"autoflow_reclassify_batch_channels": 2}},
    {"limits": {"autoflow_reclassify_batch_channels": 10}},
    {"tie": True},
], indirect=True)
def test_reclassify_batch_top_channels(db_ready, monkeypatch):
    """T-11: порция — топ по числу ждущих убыванием; C (1 сообщение) не входит
    при порции 2 и входит при порции 10; при равенстве ждущих первым идёт
    меньший id (посев с двумя равными каналами)."""
    log = _log()
    _stub_start(monkeypatch, log)

    async def go():
        out = await autoflow.reclassify_tick({})
        return out, log["calls"]

    out, calls = asyncio.run(go())
    assert out["started"] is True, out
    ids = db_ready.ids
    if "tie" in db_ready.params:  # равенство: A, D, E по 40 — меньшие id первыми
        assert calls[0]["params"]["channel_ids"] == [ids["auto_a"], ids["auto_d"]], \
            calls
    elif db_ready.params["limits"]["autoflow_reclassify_batch_channels"] == 10:
        # Порция 10: влезли все три канала.
        assert calls[0]["params"]["channel_ids"] == [
            ids["auto_a"], ids["auto_b"], ids["auto_c"]], calls
    else:  # порция 2 по умолчанию: C (1 сообщение) не входит
        assert calls[0]["params"]["channel_ids"] == [ids["auto_a"], ids["auto_b"]], \
            calls


@pytest.mark.parametrize("db_ready", [
    {"run_offset_min": 30},
    {"run_offset_min": 61},
    {"run_offset_min": None},
], indirect=True)
def test_reclassify_interval_gates_tick(db_ready, monkeypatch):
    """T-12: свежий завершённый прогон вида ЛЮБОГО автора держит окно —
    30 минут назад тише (интервал 60), 61 минуту назад — удар ставит прогон;
    прогонов нет вовсе — ждать нечего."""
    log = _log()
    _stub_start(monkeypatch, log)

    out = asyncio.run(autoflow.reclassify_tick({}))

    assert out["waiting_channels"] == 3 and out["waiting_messages"] == 51, out
    offset = db_ready.params["run_offset_min"]
    if offset == 30:  # внутри окна интервала (60 мин)
        assert out["started"] is False and out["interval_wait"] is True, out
        assert not out["busy"], out
        assert log["attempts"] == 0, log
    else:  # 61 минута — окно истекло; прогонов нет вовсе — нечего ждать
        assert out["started"] is True and not out["interval_wait"], out
        assert log["attempts"] == 1, log


@pytest.mark.parametrize("db_ready",
                         [{"run": {"status": "running", "finished_at": None}}],
                         indirect=True)
def test_reclassify_tick_busy_skips(db_ready, monkeypatch):
    """T-13: активный прогон вида — тихий пропуск (`busy=True`, постановки нет);
    гонка, где `jobs.start` бросает `JobBusy`, — тот же тихий итог, словарь
    наружу, воркер приёма не падает."""
    log = _log()
    busy_flag = [False]
    _stub_start(monkeypatch, log, busy_flag=busy_flag)

    async def go():
        out1 = await autoflow.reclassify_tick({})
        attempts1 = log["attempts"]
        # Гонка: активный прогон исчез после проверки, занятое поймал jobs.start.
        async with get_session_maker()() as db:
            await db.execute(text("DELETE FROM runs"))
            await db.commit()
        busy_flag[0] = True
        out2 = await autoflow.reclassify_tick({})
        return out1, out2, attempts1, log

    out1, out2, attempts1, log = asyncio.run(go())
    assert out1 == {"started": False, "busy": True, "interval_wait": False,
                    "waiting_channels": 3, "waiting_messages": 51}, out1
    assert attempts1 == 0, "прямо занятый вид не доходит до постановки"
    assert isinstance(out2, dict), "тип возвращает словарь, а не роняет воркер"
    assert out2["started"] is False and out2["busy"] is True, out2
    assert log["attempts"] == 1, "гонка доходит до постановки и ловит JobBusy"


@pytest.mark.parametrize("db_ready", [{"solo": True}], indirect=True)
def test_reclassify_auto_run_is_create_only(db_ready, monkeypatch):
    """T-15: `channel_ids` в params сам включает create-only — сообщение
    досчитано до L3, существующий лид не тронут (`skipped_updates == 1`)."""
    log = _log()
    _stub_start(monkeypatch, log)
    stub_stages(monkeypatch)

    async def go():
        out = await autoflow.reclassify_tick({})
        params = log["calls"][0]["params"]
        summary = None
        async with get_session_maker()() as db:
            a_id = db_ready.ids["auto_a"]
            m1 = (await db.execute(select(Message).where(
                Message.channel_id == a_id))).scalar_one()
            # Исполнение — реальный прогон с теми params, что записал тик.
            summary = await reclassify.run(db, l2_enabled=True, l3_enabled=True,
                                           l3_limit=params["l3_limit"],
                                           scope=params["scope"],
                                           channel_ids=params["channel_ids"])
            lead = (await db.execute(select(Lead).where(
                Lead.message_id == m1.id))).scalar_one()
            return (out, params, summary,
                    (m1.cascade_level, m1.cascade_passed),
                    (lead.score, lead.score_breakdown, lead.status))

    out, params, summary, msg_state, lead_state = asyncio.run(go())
    a_id = db_ready.ids["auto_a"]
    assert out["started"] is True, out
    assert params["channel_ids"] == [a_id], params
    assert msg_state == (3, True), \
        "ждущее L2 сообщение досчитано до вердикта L3"
    assert lead_state == (50, [{"label": "свежесть", "value": 10}], "new"), \
        "оценка существующего лида не сдвинулась"
    assert summary["skipped_updates"] == 1, summary
