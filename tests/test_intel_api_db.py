"""Ручки Intel: проверка файла и выгрузка — без сети; пачка, права и ревью — по HTTP на
настоящем Postgres (`RADAR_TEST_DATABASE_URL`, без неё DB-часть пропускается).

Каждая проверка — про одну ошибку, которую иначе увидит пользователь: дырявый
шаблон уходит в квоту, CSV без `;` не открывается в русском Excel, заказчик
меняет ключ, ревьюер запускает пачку, правка ломает схему.
"""
from __future__ import annotations

import asyncio
import os

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_DEBUG", "true")

from app.api.v1 import intel as intel_api  # noqa: E402
from app.core.config import get_settings  # noqa: E402
from app.core.security import SessionSigner  # noqa: E402
from app.db.models import Base, IntelKey, ResearchBatch, ResearchItem, Run, User  # noqa: E402
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.main import create_app  # noqa: E402
from app.services import jobs  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")
ROWS = [{"name": "Ромашка", "city": "Тула"}, {"name": "Василёк", "city": "Орёл"}]


def test_validate_reports_holes_length_duplicates_and_schema():
    out = intel_api.validate(ROWS + [{"name": "Ромашка", "city": "Тула"}, {"city": "Курск"}],
                             "Кто такие {{name}} из {{city}}", {"type": "object"}, 500)
    assert out["ok"] is False and out["total"] == 4
    assert out["errors"] == [{"row": 4, "field": "prompt_template",
                              "message": "в строке нет колонки name"}]
    assert out["warnings"] == [{"row": 3, "field": "query", "message": "дубликат строки 1"}]
    bad = intel_api.validate(ROWS, "{{name}}", {"type": "nope"}, 500)
    assert bad["errors"][0]["field"] == "schema_json"
    ok = intel_api.validate(ROWS, "Кто {{ name }}", None, 1)
    assert ok["ok"] is True and ok["estimated_quota_hours"] == 2


def test_csv_has_bom_semicolons_and_flattened_nested_output():
    class I:  # noqa: N801 — плоская подделка строки
        row_no, query, status, notes = 1, "Кто Ромашка", "completed", None
        input = {"name": "Ромашка"}
        result = {"structured_output": {"site": "r.ru", "tags": ["a", "b"], "geo": {"city": "Тула"}},
                  "sources": [{"url": "https://r.ru"}]}
        edited = {"geo.city": "Тула-2"}
        critic_score, review_status = 8.5, "accepted"
    rows = intel_api.export_rows([I()])
    assert rows[0]["output"] == {"site": "r.ru", "tags": ["a", "b"], "geo": {"city": "Тула-2"}}
    csv_text = intel_api.to_csv(rows)
    assert csv_text.startswith("\ufeff")
    head, line = csv_text.splitlines()[:2]
    assert "output.geo.city" in head and ";" in head
    assert "Тула-2" in line and "a;b" in csv_text


# ── HTTP на настоящей базе ────────────────────────────────────────────────────

pytestmark_db = pytest.mark.skipif(not DB_URL, reason="нет RADAR_TEST_DATABASE_URL")


async def _seed() -> dict:
    engine = create_async_engine(DB_URL, poolclass=None)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    uids = {}
    async with maker() as db:
        for role in ("owner", "customer", "reviewer"):
            u = User(email=f"{role}@local", name=role, initials=role[:2].upper(), role=role,
                     password_hash="!нельзя-войти", totp_secret="X" * 32,
                     totp_confirmed=True, is_active=True)
            db.add(u)
            await db.flush()
            uids[role] = u.id
        db.add(IntelKey(key="default", base_url="http://intel.test", concurrency=2))
        await db.commit()
    await engine.dispose()
    return uids


@pytest.fixture(scope="module")
def stand():
    if not DB_URL:
        pytest.skip("нет RADAR_TEST_DATABASE_URL")
    os.environ["RADAR_DATABASE_URL"] = DB_URL
    get_settings.cache_clear(); get_engine.cache_clear(); get_session_maker.cache_clear()
    uids = asyncio.run(_seed())
    with TestClient(create_app(), raise_server_exceptions=False) as c:
        yield c, uids


def _as(stand, role):
    c, uids = stand
    c.cookies.set(get_settings().SESSION_COOKIE,
                  SessionSigner(get_settings().SECRET_KEY).dumps({"uid": uids[role], "totp_ok": True}))
    return c


@pytest.fixture(autouse=True)
def no_worker(monkeypatch):
    """Прогон не запускаем: `jobs.start` заводит строку runs и на этом всё."""
    async def start(db, *, kind, params, name, user_email):
        run = Run(kind=kind, params=params, name=name, status="queued", progress=0,
                  created_by=user_email)
        db.add(run)
        await db.commit()
        return run
    monkeypatch.setattr(jobs, "start", start)


def test_key_is_read_by_staff_and_written_by_owner_only(stand):
    r = _as(stand, "reviewer").get("/api/v1/intel/key")
    assert r.status_code == 200 and r.json()["base_url"] == "http://intel.test"
    assert r.json()["configured"] is False and r.json()["masked"] is None
    r = _as(stand, "customer").put("/api/v1/intel/key", json={"base_url": "http://x.test"})
    assert r.status_code == 403
    r = _as(stand, "owner").put("/api/v1/intel/key", json={
        "base_url": "http://intel2.test/", "concurrency": 3, "quota_per_hour": 100})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["base_url"] == "http://intel2.test" and body["concurrency"] == 3
    assert body["last_error"], "без ключа в окружении проба обязана сказать почему"


def test_customer_starts_a_batch_reviewer_cannot_second_batch_is_409(stand):
    # Имя пачки — предельные 255 символов: `runs.name` короче (120), и без обрезки старт
    # падал 500-й (прод 21.09).
    payload = {"name": "т" * 255, "rows": ROWS, "prompt_template": "Кто {{name}} ({{city}})",
               "schema_json": {"type": "object", "properties": {"site": {"type": "string"}}}}
    assert _as(stand, "reviewer").post("/api/v1/intel/batches", json=payload).status_code == 403
    r = _as(stand, "customer").post("/api/v1/intel/batches/validate", json=payload)
    assert r.status_code == 200 and r.json()["ok"] is True
    r = _as(stand, "customer").post("/api/v1/intel/batches", json=payload)
    assert r.status_code == 202, r.text
    bid = r.json()["batch_id"]
    assert r.json()["total"] == 2 and r.json()["run_id"]
    r = _as(stand, "customer").post("/api/v1/intel/batches", json=payload)
    assert r.status_code == 409, "одна активная пачка на инстанс"
    r = _as(stand, "reviewer").get(f"/api/v1/intel/batches/{bid}")
    assert r.status_code == 200 and r.json()["status"] == "queued" and r.json()["run_id"]
    r = _as(stand, "reviewer").get(f"/api/v1/intel/batches/{bid}/items", params={"q": "Василёк"})
    assert r.json()["total"] == 1 and r.json()["rows"][0]["query"] == "Кто Василёк (Орёл)"
    bad = dict(payload, rows=[{"name": "без города"}])
    r = _as(stand, "customer").post("/api/v1/intel/batches", json=bad)
    assert r.status_code == 422 and r.json()["detail"]["errors"][0]["row"] == 1


def test_review_patch_validates_edits_against_schema_and_export_works(stand):
    async def finish():
        # Локальный движок в СВОЁМ loop'е: глобальный к этому моменту создан в
        # loop'е TestClient (портал в другом потоке) — переиспользование из
        # asyncio.run главного потока виснет навсегда (см. докстринг conftest).
        engine = create_async_engine(DB_URL)
        try:
            maker = async_sessionmaker(engine, expire_on_commit=False)
            async with maker() as db:
                item = (await db.execute(select(ResearchItem).order_by(ResearchItem.row_no))).scalars().first()
                item.status = "completed"
                item.result = {"structured_output": {"site": "r.ru"}, "critic": 7,
                               "sources": [{"url": "https://r.ru", "what_it_provided": "сайт"}]}
                item.critic_score = 7
                await db.commit()
                return item.id, item.batch_id
        finally:
            await engine.dispose()
    item_id, bid = asyncio.run(finish())
    r = _as(stand, "reviewer").patch(f"/api/v1/intel/items/{item_id}",
                                     json={"edited": {"site": 42}})
    assert r.status_code == 422 and "схему" in r.json()["detail"]
    r = _as(stand, "reviewer").patch(f"/api/v1/intel/items/{item_id}",
                                     json={"review_status": "accepted", "notes": "ок",
                                           "edited": {"site": "ромашка.рф"}})
    assert r.status_code == 200 and r.json()["output"] == {"site": "ромашка.рф"}
    r = _as(stand, "reviewer").get(f"/api/v1/intel/batches/{bid}/export",
                                   params={"format": "csv", "review_status": "accepted"})
    assert r.status_code == 200 and r.headers["content-type"].startswith("text/csv")
    assert "ромашка.рф" in r.text and r.text.splitlines()[0].startswith("\ufeffrow_no;")
    r = _as(stand, "reviewer").get(f"/api/v1/intel/batches/{bid}/export")
    assert r.status_code == 200 and len(r.json()) == 2
    r = _as(stand, "customer").post(f"/api/v1/intel/batches/{bid}/cancel")
    assert r.status_code == 200
    r = _as(stand, "reviewer").get(f"/api/v1/intel/batches/{bid}/items", params={"status": "cancelled"})
    assert r.json()["total"] == 1, "pending → cancelled сразу, завершённая строка не тронута"
