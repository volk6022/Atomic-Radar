"""Intel без сети: клиент по кодам ответа и прогон пачки на заглушке клиента.

Клиент — `httpx.MockTransport`: 429 обязан нести `retry_after` и `scope`, 404 по
своему `task_id` — не «нет сети», а потерянная задача, 5xx — три попытки и
`IntelUnavailable`. Прогон — на подменённом `intel_client`: не больше
`concurrency` задач в полёте, `queued_waiting_llm` дольше часа → `stalled`,
отмена не бросает уже поставленные. База — `RADAR_TEST_DATABASE_URL`, без неё
DB-часть пропускается; клиент проверяется всегда.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone

import httpx
import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_DEBUG", "true")

from app.db.models import Base, IntelKey, ResearchBatch, ResearchItem  # noqa: E402
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.core.config import get_settings  # noqa: E402
from app.services import intel_client, intel_research  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")
EP = intel_client.Endpoint(key="t", base_url="http://intel.test", api_key="k")


def _client_with(handler, monkeypatch):
    """Клиент с транспортом-заглушкой; без сна между ретраями."""
    client = httpx.AsyncClient(base_url=EP.base_url, transport=httpx.MockTransport(handler),
                               headers={"X-API-Key": EP.api_key})
    monkeypatch.setattr(intel_client, "_get_client", lambda ep: client)
    monkeypatch.setattr(intel_client.asyncio, "sleep", _nosleep)
    return client


async def _nosleep(_):
    return None


async def test_run_returns_task_id_and_sends_key(monkeypatch):
    seen = {}

    def handler(req: httpx.Request):
        seen["auth"] = req.headers.get("X-API-Key")
        seen["body"] = req.read()
        return httpx.Response(202, json={"task_id": "t-1", "status": "pending"})

    _client_with(handler, monkeypatch)
    assert await intel_client.run(EP, query="кто такие", mode="quality",
                                  output_schema={"type": "object"}, language="ru") == "t-1"
    assert seen["auth"] == "k" and b'"output_schema"' in seen["body"]


async def test_429_carries_retry_after_and_scope(monkeypatch):
    def handler(req):
        return httpx.Response(429, json={"detail": "quota", "retry_after": 37, "scope": "work"},
                              headers={"Retry-After": "37"})
    _client_with(handler, monkeypatch)
    with pytest.raises(intel_client.IntelRateLimited) as e:
        await intel_client.run(EP, query="q", mode="quality", output_schema=None, language="ru")
    assert (e.value.retry_after, e.value.scope) == (37, "work")


async def test_status_404_is_lost_task_and_5xx_retries_then_unavailable(monkeypatch):
    calls = {"n": 0}

    def handler(req):
        calls["n"] += 1
        if req.url.path.endswith("/lost"):
            return httpx.Response(404, json={"detail": "not found"})
        return httpx.Response(503, text="down")
    _client_with(handler, monkeypatch)
    with pytest.raises(intel_client.IntelNotFound):
        await intel_client.status(EP, "lost")
    with pytest.raises(intel_client.IntelUnavailable):
        await intel_client.status(EP, "t-2")
    assert calls["n"] == 1 + 3, "404 — сразу, 5xx — три попытки"


async def test_403_is_forbidden_not_unavailable(monkeypatch):
    _client_with(lambda req: httpx.Response(403, json={"detail": "bad key"}), monkeypatch)
    with pytest.raises(intel_client.IntelForbidden):
        await intel_client.status(EP, "t-3")


# ── прогон на заглушке клиента ────────────────────────────────────────────────

pytestmark_db = pytest.mark.skipif(not DB_URL, reason="нет RADAR_TEST_DATABASE_URL")


class FakeIntel:
    """Intel в памяти: задача проходит статусы по списку при каждом опросе."""

    def __init__(self, script):
        self.script = script          # task_id → список статусов по опросам
        self.polls: dict[str, int] = {}
        self.submitted: list[str] = []
        self.in_flight_max = 0
        self.open: set[str] = set()

    async def endpoint(self, db):
        return EP

    async def run(self, ep, *, query, mode, output_schema, language):
        task_id = f"t{len(self.submitted) + 1}"
        self.submitted.append(query)
        self.open.add(task_id)
        self.in_flight_max = max(self.in_flight_max, len(self.open))
        return task_id

    async def status(self, ep, task_id):
        steps = self.script.get(task_id, ["completed"])
        n = self.polls.get(task_id, 0)
        self.polls[task_id] = n + 1
        st = steps[min(n, len(steps) - 1)]
        if st in ("completed", "failed"):
            self.open.discard(task_id)
        if st == "404":
            raise intel_client.IntelNotFound(task_id)
        if st == "completed":
            # Форма `stats` — дамп настоящего Intel (прод 21.09), не выдумка заглушки:
            # `tokens` там словарь, и число ломало UPDATE `tokens INTEGER`.
            return {"status": "completed", "result": {"structured_output": {"ok": True},
                    "critic": {"score": 7.5},
                    "stats": {"turns": 4, "tool_calls": {"web_scrape": 2, "web_serp": 1},
                              "tokens": {"main": {"prompt": 13630, "completion": 3323,
                                                  "last_prompt": 3761},
                                         "aux": {"prompt": 0, "completion": 0},
                                         "grand_total": 16953},
                              "elapsed_seconds": 77.2, "mode_used": "quality"}}}
        if st == "failed":
            return {"status": "failed", "progress": {"message": "модель молчит"}}
        return {"status": st, "result": None}


def _fast(monkeypatch, fake):
    for name in ("endpoint", "run", "status"):
        monkeypatch.setattr(intel_research.intel_client, name, getattr(fake, name))
    monkeypatch.setattr(intel_research, "FIRST_POLL_AFTER", 0.0)
    monkeypatch.setattr(intel_research, "POLL_EVERY", 0.0)
    monkeypatch.setattr(intel_research, "TICK", 0.0)


@pytest.fixture
async def db():
    if not DB_URL:
        pytest.skip("нет RADAR_TEST_DATABASE_URL")
    os.environ["RADAR_DATABASE_URL"] = DB_URL
    get_settings.cache_clear(); get_engine.cache_clear(); get_session_maker.cache_clear()
    engine = create_async_engine(DB_URL, poolclass=None)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as s:
        s.add(IntelKey(key="default", base_url="http://intel.test", concurrency=2))
        batch = ResearchBatch(name="тест", status="queued", prompt_template="кто {{name}}",
                              source_kind="json", total=4, mode="quality", language="ru")
        s.add(batch)
        await s.flush()
        for i in range(1, 5):
            s.add(ResearchItem(batch_id=batch.id, row_no=i, input={"name": f"к{i}"},
                               query=f"кто к{i}", status="pending"))
        await s.commit()
        yield s
    await engine.dispose()


async def _log():
    lines = []

    async def report(pct, note):
        lines.append((pct, note))
    return lines, report


async def test_batch_runs_within_concurrency_and_records_results(db, monkeypatch):
    fake = FakeIntel({"t2": ["running", "queued_waiting_llm", "completed"], "t3": ["failed"],
                      "t4": ["404"]})
    _fast(monkeypatch, fake)
    lines, report = await _log()
    out = await intel_research.run_batch(0, batch_id=1, report=report, cancelled=lambda: False)
    assert out == {"batch_id": 1, "total": 4, "done": 2, "failed": 2, "status": "done"}
    assert fake.in_flight_max <= 2, "не больше concurrency задач в полёте"
    items = {r.row_no: r for r in (await db.execute(select(ResearchItem))).scalars().all()}
    assert items[1].status == "completed" and float(items[1].critic_score) == 7.5 and items[1].tokens == 16953
    assert items[3].status == "failed" and items[3].error == "модель молчит"
    assert items[4].status == "failed" and "404" in items[4].error
    batch = await db.get(ResearchBatch, 1)
    await db.refresh(batch)
    assert (batch.status, batch.done, batch.failed) == ("done", 2, 2) and batch.finished_at is not None
    assert lines[-1][0] == 100


async def test_waiting_llm_longer_than_limit_is_stalled(db, monkeypatch):
    fake = FakeIntel({f"t{i}": ["queued_waiting_llm"] * 50 for i in range(1, 5)})
    _fast(monkeypatch, fake)
    monkeypatch.setattr(intel_research, "WAITING_LLM_LIMIT", -1.0)
    lines, report = await _log()
    out = await intel_research.run_batch(0, batch_id=1, report=report, cancelled=lambda: False)
    assert out["status"] == "failed" and out["failed"] == 4
    statuses = {r.status for r in (await db.execute(select(ResearchItem))).scalars().all()}
    assert statuses == {"stalled"}


async def test_cancel_keeps_in_flight_and_cancels_pending(db, monkeypatch):
    fake = FakeIntel({"t1": ["running", "running", "completed"], "t2": ["running", "completed"]})
    _fast(monkeypatch, fake)
    flag = {"n": 0}

    def cancelled():
        flag["n"] += 1
        return flag["n"] > 2          # отмена приходит после первой постановки
    lines, report = await _log()
    out = await intel_research.run_batch(0, batch_id=1, report=report, cancelled=cancelled)
    assert out["status"] == "cancelled"
    items = {r.row_no: r.status for r in (await db.execute(select(ResearchItem))).scalars().all()}
    assert sorted(items.values()) == ["cancelled", "cancelled", "completed", "completed"], items
    assert len(fake.submitted) == 2, "после отмены новые запросы не ставились"


def test_tokens_accepts_real_and_stub_shapes():
    assert intel_research._tokens({"tokens": {"main": {"prompt": 10, "completion": 5},
                                              "aux": {}, "grand_total": 15}}) == 15
    assert intel_research._tokens({"tokens": {"main": {"prompt": 10, "completion": 5}}}) == 15
    assert intel_research._tokens({"tokens": 1200}) == 1200
    assert intel_research._tokens({"tokens": "x"}) is None
    assert intel_research._tokens({}) is None
    assert intel_research._elapsed({"elapsed_seconds": "77.2"}) == 77.2
    assert intel_research._elapsed({}) is None
