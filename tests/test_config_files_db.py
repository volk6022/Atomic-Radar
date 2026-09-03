"""Настройки отбора как файлы: выгрузить, загрузить, переключиться, удалить — через HTTP.

Это внешняя половина `config_bundle`: сама служба уже умеет собрать набор и применить
его целиком (`test_config_bundle_db.py`), но человеку нужна не служба, а четыре
действия — забрать текущее себе, залить правленое обратно, переключиться между тем,
что уже заливал, и выбросить лишнее.

Три вещи проверяются здесь и нигде больше:

1. **Круг замыкается через HTTP.** Выгруженное тело принимается загрузкой без правок
   руками. Иначе «поправить снаружи» превращается в «собрать заново».
2. **Загрузка применяет сразу.** Отдельного «включить» нет — именно затем, чтобы
   с экрана исчезла лесенка версий с кнопками.
3. **Отказ не оставляет следов в настройках, но не теряет файл.** Битый файл не
   применяется и не сохраняется вовсе; файл правильный, но не сумевший примениться
   (мёртвый эмбеддер), — сохраняется и честно помечается непримененным, чтобы человек
   мог повторить, а не заливать заново.

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
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
os.environ.setdefault("RADAR_INGEST_TOKEN", "test-ingest-token")

from app.core.config import get_settings  # noqa: E402
from app.core.security import SessionSigner  # noqa: E402
from app.db.models import Base, CascadeVersion, ProfileVersion, User  # noqa: E402
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.main import create_app  # noqa: E402
from app.services import embeddings  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

BASE = "/api/v1/profile/config"


def bundle(name: str, *, pain: str, description: str) -> dict:
    """Минимальный правильный набор. Ярлык боли и описание разные у разных наборов —
    по ним и видно, какой из них сейчас применён."""
    return {
        "format": "atomic-radar-config",
        "version": 1,
        "name": name,
        "business": {"description": description},
        "pains": {pain: {"anchors": ["валютный контроль", "банк отказал"],
                         "prototypes": ["банк завернул платёж, требует контракт"]}},
        "noise": {"офтоп": ["всем привет, как дела"]},
        "disqualifiers": {"вакансия": ["вакансия", "резюме"]},
        "l3_prompts": {"dm_v1": f"Промпт набора «{name}». Ответь JSON."},
    }


FIRST = bundle("курс-первый", pain="банк не пропускает платеж",
               description="КУРС — оплата счетов зарубежных поставщиков.")
SECOND = bundle("курс-второй", pain="нет валютного счета",
                description="КУРС — платежи за рубеж без валютного счёта.")


@pytest.fixture(autouse=True)
def fake_embedder(monkeypatch):
    """Эмбеддер — единственное внешнее HTTP-плечо на этом пути, и только оно
    подменяется. Тест, который ходил бы в живой эмбеддер, проверял бы сеть."""
    monkeypatch.setattr(embeddings, "enabled", lambda: True)

    async def fake(phrases):
        return [[0.1, 0.2, 0.3] for _ in phrases]

    monkeypatch.setattr(embeddings, "embed", fake)


async def _seed() -> None:
    engine = create_async_engine(DB_URL, poolclass=None)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        for email, role, initials in (("owner@local", "owner", "OW"),
                                      ("customer@local", "customer", "CU")):
            db.add(User(email=email, name=role, initials=initials, role=role,
                        password_hash="!нельзя-войти", totp_secret="X" * 32,
                        totp_confirmed=True, is_active=True))
        await db.commit()
    await engine.dispose()


async def _uid(email: str) -> int:
    engine = create_async_engine(DB_URL, poolclass=None)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        uid = (await db.execute(select(User.id).where(User.email == email))).scalar_one()
    await engine.dispose()
    return uid


@pytest.fixture
def client():
    asyncio.run(_seed())
    previous = os.environ.get("RADAR_DATABASE_URL")
    os.environ["RADAR_DATABASE_URL"] = DB_URL
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_session_maker.cache_clear()

    app = create_app()
    with TestClient(app, raise_server_exceptions=False) as c:
        c.login = lambda email: c.cookies.set(  # type: ignore[attr-defined]
            get_settings().SESSION_COOKIE,
            SessionSigner(get_settings().SECRET_KEY).dumps(
                {"uid": asyncio.run(_uid(email)), "totp_ok": True}))
        c.login("owner@local")  # type: ignore[attr-defined]
        yield c

    if previous is None:
        os.environ.pop("RADAR_DATABASE_URL", None)
    else:
        os.environ["RADAR_DATABASE_URL"] = previous
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_session_maker.cache_clear()


async def _active() -> tuple[str | None, set[str]]:
    """Что сейчас реально управляет отбором: описание бизнеса и ярлыки болей."""
    engine = create_async_engine(DB_URL, poolclass=None)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        profile = (await db.execute(select(ProfileVersion).where(
            ProfileVersion.is_active.is_(True)))).scalar_one_or_none()
        cascade = (await db.execute(select(CascadeVersion).where(
            CascadeVersion.is_active.is_(True)))).scalar_one_or_none()
    await engine.dispose()
    return (profile.business_description if profile else None,
            set(cascade.pain_anchors) if cascade else set())


def upload(client, body: dict):
    return client.post(f"{BASE}/files", json=body)


# ── загрузка ──────────────────────────────────────────────────────────────────

def test_upload_applies_the_settings_at_once(client):
    """Никакого отдельного «включить»: залил — работает. Ради этого и затевалось."""
    r = upload(client, FIRST)
    assert r.status_code == 201, r.text

    description, pains = asyncio.run(_active())
    assert description == FIRST["business"]["description"]
    assert pains == set(FIRST["pains"])


def test_uploaded_file_shows_up_in_the_list_as_applied(client):
    assert upload(client, FIRST).status_code == 201

    body = client.get(f"{BASE}/files").json()
    assert [f["name"] for f in body["files"]] == ["курс-первый"]
    only = body["files"][0]
    assert only["is_current"] is True
    assert only["applied_at"] is not None
    assert only["pains"] == 1


def test_switching_between_two_files_changes_what_actually_selects(client):
    """Смысл переключения — не отметка в списке, а другой отбор."""
    assert upload(client, FIRST).status_code == 201
    assert upload(client, SECOND).status_code == 201

    description, pains = asyncio.run(_active())
    assert description == SECOND["business"]["description"]
    assert pains == set(SECOND["pains"])

    files = {f["name"]: f["id"] for f in client.get(f"{BASE}/files").json()["files"]}
    r = client.post(f"{BASE}/files/{files['курс-первый']}/apply")
    assert r.status_code == 200, r.text

    description, pains = asyncio.run(_active())
    assert description == FIRST["business"]["description"]
    assert pains == set(FIRST["pains"])


def test_only_one_file_is_current_after_switching(client):
    assert upload(client, FIRST).status_code == 201
    assert upload(client, SECOND).status_code == 201
    files = {f["name"]: f["id"] for f in client.get(f"{BASE}/files").json()["files"]}
    client.post(f"{BASE}/files/{files['курс-первый']}/apply")

    listed = client.get(f"{BASE}/files").json()["files"]
    current = [f["name"] for f in listed if f["is_current"]]
    assert current == ["курс-первый"]


def test_newest_file_comes_first(client):
    upload(client, FIRST)
    upload(client, SECOND)
    names = [f["name"] for f in client.get(f"{BASE}/files").json()["files"]]
    assert names == ["курс-второй", "курс-первый"]


# ── выгрузка и круг ───────────────────────────────────────────────────────────

def test_export_returns_what_is_actually_active(client):
    upload(client, FIRST)
    body = client.get(BASE).json()
    assert body["format"] == "atomic-radar-config"
    assert set(body["pains"]) == set(FIRST["pains"])
    assert body["business"]["description"] == FIRST["business"]["description"]
    assert body["l3_prompts"]["dm_v1"] == FIRST["l3_prompts"]["dm_v1"]


def test_the_circle_closes_without_editing_by_hand(client):
    """Выгруженное тело принимается загрузкой как есть."""
    upload(client, FIRST)
    exported = client.get(BASE).json()

    assert upload(client, exported).status_code == 201
    description, pains = asyncio.run(_active())
    assert description == FIRST["business"]["description"]
    assert pains == set(FIRST["pains"])


def test_a_saved_file_can_be_downloaded_back_byte_for_byte(client):
    upload(client, FIRST)
    file_id = client.get(f"{BASE}/files").json()["files"][0]["id"]
    stored = client.get(f"{BASE}/files/{file_id}").json()
    assert stored["pains"] == FIRST["pains"]
    assert stored["l3_prompts"] == FIRST["l3_prompts"]


# ── отказы ────────────────────────────────────────────────────────────────────

BROKEN = [
    pytest.param({"format": "чужой", "version": 1}, id="чужой формат"),
    pytest.param({"format": "atomic-radar-config", "version": 99}, id="версия формата"),
    pytest.param({"format": "atomic-radar-config", "version": 1, "pains": {}},
                 id="без болей"),
]


@pytest.mark.parametrize("body", BROKEN)
def test_a_broken_file_is_refused_and_changes_nothing(client, body):
    upload(client, FIRST)
    before = asyncio.run(_active())

    r = upload(client, body)
    assert r.status_code == 422, r.text
    assert asyncio.run(_active()) == before


@pytest.mark.parametrize("body", BROKEN)
def test_a_broken_file_is_not_even_saved(client, body):
    """Список наборов — это то, что можно применить. Заведомо неприменимому файлу
    там не место: он превратил бы список в свалку неудачных попыток."""
    upload(client, body)
    assert client.get(f"{BASE}/files").json()["files"] == []


def test_a_valid_file_that_failed_to_apply_is_kept_but_marked_unapplied(client,
                                                                        monkeypatch):
    """Эмбеддер молчит — файл правильный, применить его не вышло.

    Терять его нельзя: человек только что собрал набор снаружи, и заставлять его
    заливать заново после чужой поломки — наказание за не свою ошибку.

    Набор здесь нарочно уникальный. Приложение на старте заводит стартовую
    таксономию, а `save_taxonomy` не зовёт эмбеддер для фраз, которые не менялись, —
    набор, случайно совпавший со стартовым, применился бы и при мёртвом эмбеддере,
    и тест проверял бы не то, что написано в его имени.
    """
    monkeypatch.setattr(embeddings, "enabled", lambda: False)
    doomed = bundle("курс-непримененный", pain="боль только для этого теста",
                    description="Описание, которого нет ни в одном другом наборе.")
    doomed["pains"]["боль только для этого теста"]["prototypes"] = [
        "эталонная фраза, которой нет ни в одном другом наборе"]

    before = asyncio.run(_active())
    r = upload(client, doomed)
    assert r.status_code == 422, r.text

    files = client.get(f"{BASE}/files").json()["files"]
    assert [f["name"] for f in files] == ["курс-непримененный"]
    assert files[0]["applied_at"] is None
    assert files[0]["is_current"] is False
    assert asyncio.run(_active()) == before


# ── удаление ──────────────────────────────────────────────────────────────────

def test_deleting_a_file_removes_it_from_the_list(client):
    upload(client, FIRST)
    upload(client, SECOND)
    files = {f["name"]: f["id"] for f in client.get(f"{BASE}/files").json()["files"]}

    assert client.delete(f"{BASE}/files/{files['курс-первый']}").status_code == 200
    assert [f["name"] for f in client.get(f"{BASE}/files").json()["files"]] \
        == ["курс-второй"]


def test_deleting_a_file_does_not_touch_the_live_settings(client):
    """Файл — снимок, а не источник работы. Удалить снимок применённого набора можно,
    и отбор от этого меняться не должен: иначе удаление строки в списке молча
    остановило бы классификацию."""
    upload(client, FIRST)
    before = asyncio.run(_active())
    file_id = client.get(f"{BASE}/files").json()["files"][0]["id"]

    assert client.delete(f"{BASE}/files/{file_id}").status_code == 200
    assert asyncio.run(_active()) == before


def test_applying_a_file_that_does_not_exist_is_404(client):
    assert client.post(f"{BASE}/files/999999/apply").status_code == 404


# ── права ─────────────────────────────────────────────────────────────────────

def test_customer_may_look_but_not_load(client):
    """Заказчик видит настройки и не может подменить их целиком: загрузка применяет
    сразу, то есть это то же самое действие, что включение версии."""
    upload(client, FIRST)
    client.login("customer@local")  # type: ignore[attr-defined]

    assert client.get(BASE).status_code == 200
    assert client.get(f"{BASE}/files").status_code == 200
    assert upload(client, SECOND).status_code == 403

    file_id = client.get(f"{BASE}/files").json()["files"][0]["id"]
    assert client.post(f"{BASE}/files/{file_id}/apply").status_code == 403
    assert client.delete(f"{BASE}/files/{file_id}").status_code == 403
