"""Ручка `POST /api/v1/intel/yandex` — пачка разборов Яндекс-карт по HTTP на
настоящем Postgres (`RADAR_TEST_DATABASE_URL`, без неё DB-часть пропускается).
Intel замокан на уровне `intel_client.yandex` — сеть не трогаем.

Каждая проверка — про одно свойство экрана: порядок строк сохранён, капча в
середине пачку не прерывает, дырявая строка ловится до Intel с номером, без
ключа — 409, без раздела — 403.
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

from app.core.config import get_settings  # noqa: E402
from app.core.security import SessionSigner  # noqa: E402
from app.db.models import AuditLog, Base, IntelKey, User  # noqa: E402
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.main import create_app  # noqa: E402
from app.services import intel_client  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")
EXTRACT_ROWS = [{"query": "ромашка тула"}, {"query": "василёк орёл"}]


async def _seed() -> dict:
    engine = create_async_engine(DB_URL, poolclass=None)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    uids = {}
    async with maker() as db:
        for role in ("owner", "customer", "reviewer", "viewer"):
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


def _with_intel_key(monkeypatch):
    """Ключ Intel в окружении: без него `intel_client.endpoint` роняет ручку в 409."""
    monkeypatch.setenv("RADAR_INTEL_API_KEY", "test-intel-key")


async def _tweak(fn):
    """Чтение сцены напрямую в базе стенда. Локальный движок в СВОЁМ loop'е:
    глобальный принадлежит loop'у TestClient (см. test_intel_api_db.py)."""
    engine = create_async_engine(DB_URL)
    try:
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with maker() as db:
            return await fn(db)
    finally:
        await engine.dispose()


def test_happy_path_two_extracts_order_kept_and_audited(stand, monkeypatch):
    seen = []

    async def fake_yandex(ep, *, kind, payload):
        seen.append((kind, payload["query"]))
        return {"found": payload["query"]}

    monkeypatch.setattr(intel_client, "yandex", fake_yandex)
    _with_intel_key(monkeypatch)
    r = _as(stand, "customer").post("/api/v1/intel/yandex",
                                    json={"kind": "extract", "rows": EXTRACT_ROWS})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["kind"] == "extract" and body["total"] == 2
    assert body["ok"] == 2 and body["failed"] == 0
    assert [row["i"] for row in body["results"]] == [0, 1], "порядок сохранён"
    assert all(row["ok"] is True and row["error"] is None for row in body["results"])
    assert body["results"][0]["data"] == {"found": "ромашка тула"}
    assert body["results"][1]["data"] == {"found": "василёк орёл"}
    assert seen == [("extract", "ромашка тула"), ("extract", "василёк орёл")], \
        "строки идут последовательно, в порядке пачки"

    async def check_audit(db):
        log = (await db.execute(select(AuditLog).where(
            AuditLog.action == "intel_yandex"))).scalars().one()
        assert log.detail == {"kind": "extract", "rows": 2}, \
            "в аудите только kind и число строк, без содержимого"

    asyncio.run(_tweak(check_audit))


def test_captcha_in_the_middle_does_not_break_the_batch(stand, monkeypatch):
    async def fake_yandex(ep, *, kind, payload):
        if "капча" in payload["query"]:
            raise intel_client.IntelYandexCaptcha("yandex captcha: подтвердите, что вы человек")
        return {"found": payload["query"]}

    monkeypatch.setattr(intel_client, "yandex", fake_yandex)
    _with_intel_key(monkeypatch)
    # Капча в середине трёх: пачка доходит до конца, порядок не плавает.
    r = _as(stand, "customer").post("/api/v1/intel/yandex", json={
        "kind": "extract",
        "rows": [{"query": "ромашка"}, {"query": "капча-ловушка"}, {"query": "василёк"}]})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["total"] == 3 and body["ok"] == 2 and body["failed"] == 1
    assert [row["i"] for row in body["results"]] == [0, 1, 2], "порядок сохранён"
    mid = body["results"][1]
    assert mid["ok"] is False and mid["data"] is None
    assert mid["error"]["code"] == "captcha"
    assert "yandex captcha" in mid["error"]["message"]
    assert body["results"][2]["data"] == {"found": "василёк"}, "строка после капчи исполнена"
    # Те же счётчики на двух строках: упавшая вторая даёт ok=1 failed=1.
    r = _as(stand, "customer").post("/api/v1/intel/yandex", json={
        "kind": "extract", "rows": [{"query": "ромашка"}, {"query": "капча-ловушка"}]})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["total"] == 2 and body["ok"] == 1 and body["failed"] == 1
    assert body["results"][1]["error"]["code"] == "captcha"


def test_rows_bounds_and_required_keys_report_row_numbers(stand):
    c = _as(stand, "customer")
    r = c.post("/api/v1/intel/yandex", json={"kind": "extract", "rows": []})
    assert r.status_code == 422
    r = c.post("/api/v1/intel/yandex", json={
        "kind": "extract", "rows": [{"query": "q"} for _ in range(51)]})
    assert r.status_code == 422
    r = c.post("/api/v1/intel/yandex", json={"kind": "extract", "rows": [{"name": "ромашка"}]})
    assert r.status_code == 422
    assert r.json()["detail"]["errors"] == [
        {"i": 0, "field": "query", "message": "в строке нет ключа query"}]
    r = c.post("/api/v1/intel/yandex", json={"kind": "card", "rows": [
        {"business_oid": "123", "seoname": "tula"},
        {"business_oid": "456"},
        {"business_oid": "12a", "seoname": "orel"},
        {"seoname": "kursk"},
    ]})
    assert r.status_code == 422
    errors = r.json()["detail"]["errors"]
    assert [e["i"] for e in errors] == [1, 2, 3], "номер строки указан"
    assert [e["field"] for e in errors] == ["seoname", "business_oid", "business_oid"]


def test_without_intel_key_is_409(stand, monkeypatch):
    async def must_not_be_called(ep, *, kind, payload):
        raise AssertionError("intel_client.yandex не зовётся, пока нет ключа")

    monkeypatch.setattr(intel_client, "yandex", must_not_be_called)
    monkeypatch.delenv("RADAR_INTEL_API_KEY", raising=False)
    r = _as(stand, "customer").post("/api/v1/intel/yandex",
                                    json={"kind": "extract", "rows": EXTRACT_ROWS})
    assert r.status_code == 409, r.text


def test_viewer_without_intel_section_is_403(stand):
    r = _as(stand, "viewer").post("/api/v1/intel/yandex",
                                  json={"kind": "extract", "rows": EXTRACT_ROWS})
    assert r.status_code == 403, "у viewer нет раздела INTEL"


def test_reviewer_cannot_run_yandex(stand):
    # Ручка тратит чужую квоту и минуты, поэтому право то же, что у запуска пачки
    # (INTEL_RUN), а не просто «есть раздел Intel»: ревьюер смотрит, но не запускает.
    r = _as(stand, "reviewer").post("/api/v1/intel/yandex",
                                    json={"kind": "extract", "rows": EXTRACT_ROWS})
    assert r.status_code == 403, "у reviewer нет права INTEL_RUN"
