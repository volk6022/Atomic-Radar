"""Фильтры реестра каналов: поиск, тип чата, состояние обсуждения — через HTTP.

Проверяется не «эндпоинт отвечает», а то, ради чего фильтры существуют: каждый из
 них меняет и выдачу, и `total`. Фильтр, режущий только страницу, а не весь список,
оставил бы `total` от всех каналов — и номера страниц разошлись бы с содержимым.
Состояние обсуждения — самый коварный случай: колонки в базе нет, состояние
вычисляется (`app/services/discussions.py`), и в SQL его не положить.

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_DEBUG", "true")
os.environ.setdefault("RADAR_INGEST_TOKEN", "test-ingest-token")

from app.core.config import get_settings  # noqa: E402
from app.core.security import SessionSigner  # noqa: E402
from app.db.models import Base, Channel, Message, User  # noqa: E402
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.main import create_app  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

NOW = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)

# Пять строк покрывают все пять состояний обсуждения и оба типа чата.
#   alpha      — «живая» пара: канал с группой, аккаунт в группе, история прочитана → live
#   alpha_chat — сама группа, заведённая первым сообщением: карточку не спрашивали → unknown
#   beta       — спрашивали, обсуждения нет → none
#   gamma      — связь есть, а строки группы в базе нет → unread
#   delta      — без username и без опроса → unknown
# leads_total убывает по списку: сортировка по умолчанию (по лидам) становится
# предсказуемой, и на страницах лимит в одну строку проверяется по имени.
SEED = [
    dict(key="alpha", peer_id=-101, username="alphaclub", title="Прокси Клуб",
         chat_type="channel", linked="alphaclub_chat", checked=True,
         joined=False, leads=5),
    dict(key="alpha_chat", peer_id=-102, username="alphaclub_chat",
         title="Прокси Клуб Chat", chat_type="supergroup", linked=None,
         checked=False, joined=True, leads=1),
    dict(key="beta", peer_id=-103, username="vpstoday", title="Впс Обзор",
         chat_type="channel", linked=None, checked=True, joined=False, leads=4),
    dict(key="gamma", peer_id=-104, username="corpostrovokru", title="Командировки",
         chat_type="channel", linked="ghost_chat", checked=True,
         joined=False, leads=3),
    dict(key="delta", peer_id=-105, username=None, title="Островок",
         chat_type=None, linked=None, checked=False, joined=False, leads=2),
]

# Ожидаемое состояние каждой строки — тот же расчёт, что на экране.
# Ключ — то, чем строка опознаётся на экране: `username`, а у канала без него —
# заголовок. Не внутренний ключ засева: сверять надо с тем, что видит человек.
EXPECTED_STATES = {"alphaclub": "live", "alphaclub_chat": "unknown",
                   "vpstoday": "none", "corpostrovokru": "unread",
                   "Островок": "unknown"}


async def _seed() -> int:
    engine = create_async_engine(DB_URL, poolclass=None)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)

    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        db.add(User(email="owner@local", name="owner", initials="OW", role="owner",
                    password_hash="!нельзя-войти", totp_secret="X" * 32,
                    totp_confirmed=True, is_active=True))
        ids = {}
        for row in SEED:
            channel = Channel(peer_id=row["peer_id"], username=row["username"],
                              title=row["title"], chat_type=row["chat_type"],
                              linked_chat_username=row["linked"],
                              linked_checked_at=NOW if row["checked"] else None,
                              linked_joined_at=NOW if row["joined"] else None,
                              leads_total=row["leads"], ingest_enabled=True)
            db.add(channel)
            ids[row["key"]] = channel
        await db.flush()
        group_id = ids["alpha_chat"].id
        # История «живой» группы: три сообщения. Без них live не отличить от unread.
        for i in range(1, 4):
            db.add(Message(channel_id=group_id, tg_message_id=i,
                           tg_date=NOW - timedelta(minutes=i), text=f"комментарий {i}"))
        await db.commit()

    await engine.dispose()
    return group_id


@pytest.fixture
def client():
    """Приложение против свежезасеянной тестовой базы — см. test_workflows_api_db.py:
    посев в собственном цикле, наружу ничего живого не отдаётся."""
    asyncio.run(_seed())
    previous = os.environ.get("RADAR_DATABASE_URL")
    os.environ["RADAR_DATABASE_URL"] = DB_URL
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_session_maker.cache_clear()

    app = create_app()
    with TestClient(app, raise_server_exceptions=False) as c:
        owner = asyncio.run(_owner_id())
        token = SessionSigner(get_settings().SECRET_KEY).dumps(
            {"uid": owner, "totp_ok": True})
        c.cookies.set(get_settings().SESSION_COOKIE, token)
        yield c

    if previous is None:
        os.environ.pop("RADAR_DATABASE_URL", None)
    else:
        os.environ["RADAR_DATABASE_URL"] = previous
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_session_maker.cache_clear()


async def _owner_id() -> int:
    from sqlalchemy import select
    engine = create_async_engine(DB_URL, poolclass=None)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        uid = (await db.execute(select(User.id).where(User.email == "owner@local"))
               ).scalar_one()
    await engine.dispose()
    return uid


def get(client, **params) -> dict:
    r = client.get("/api/v1/channels", params=params)
    assert r.status_code == 200, r.text
    return r.json()


def usernames(body: dict) -> set:
    """Чем строка опознаётся: `username`, а у канала без него — заголовок.

    Голый `username` давал бы `None` для приватного канала, и множество из двух
    таких строк схлопывалось бы в одну — проверка «строка не попала на две
    страницы» тогда проходила бы там, где на деле сломано.
    """
    return {row["username"] or row["title"] for row in body["rows"]}


# ── без фильтров ──────────────────────────────────────────────────────────────

def test_unfiltered_listing_shows_every_channel_and_declares_sorts(client):
    """Точка отсчёта: без фильтров видны все пять строк, и экран получает список
    допустимых сортировок — тот же белый список, что проверяет apply_sort."""
    body = get(client)
    assert body["total"] == 5
    assert len(body["rows"]) == 5
    assert body["sorts"] == ["leads_total", "members", "title"]
    # Выданное состояние совпадает с расчётом из discussions.py — на этих же пяти
    # строках ниже проверяется, что фильтр отбирает ровно его.
    assert {r["username"] or r["title"]: r["discussion"]["state"]
            for r in body["rows"]} == EXPECTED_STATES


# ── поиск `q` ─────────────────────────────────────────────────────────────────

def test_q_matches_title_substring_case_insensitively(client):
    """«клуб» в нижнем регистре находит «Прокси Клуб» и «Прокси Клуб Chat», а total
    падает с пяти до двух — счётчик считается по отфильтрованному, а не по всей таблице."""
    body = get(client, q="клуб")
    assert body["total"] == 2
    assert usernames(body) == {"alphaclub", "alphaclub_chat"}


def test_q_matches_username_too(client):
    """Заглавными буквами по username: регистр не важен и здесь."""
    body = get(client, q="CORPOSTROVOKRU")
    assert body["total"] == 1
    assert usernames(body) == {"corpostrovokru"}


def test_q_without_hits_is_empty_not_everything(client):
    body = get(client, q="такого-канала-нет")
    assert body["total"] == 0
    assert body["rows"] == []


# ── тип чата ──────────────────────────────────────────────────────────────────

def test_chat_type_filter_changes_rows_and_total(client):
    channels = get(client, chat_type="channel")
    assert channels["total"] == 3
    assert usernames(channels) == {"alphaclub", "vpstoday", "corpostrovokru"}

    groups = get(client, chat_type="supergroup")
    assert groups["total"] == 1
    assert usernames(groups) == {"alphaclub_chat"}

    assert get(client, chat_type="forum")["total"] == 0


# ── состояние обсуждения ──────────────────────────────────────────────────────

@pytest.mark.parametrize("state,expected", [
    ("live", {"alphaclub"}),
    ("none", {"vpstoday"}),
    ("unread", {"corpostrovokru"}),
    ("unknown", {"alphaclub_chat", "Островок"}),   # у delta username нет
])
def test_discussion_filter_changes_rows_and_total(client, state, expected):
    """Каждое состояние отбирает свои строки, total им же и считается, а выданное
    поле `discussion` совпадает с фильтром — иначе экран фильтровал бы не то, что
    показывает бейджем."""
    body = get(client, discussion=state)
    assert body["total"] == len(expected)
    assert body["total"] != 5, "фильтр обязан убирать строки, а не только подписывать их"
    found = {r["username"] or r["title"] for r in body["rows"]}
    assert found == expected
    assert all(r["discussion"]["state"] == state for r in body["rows"])


def test_discussion_filter_applies_to_everything_not_to_the_page(client):
    """Главный грех, ради которого тест: срезать состояние по уже выданной странице.

    unknown — две строки; при лимите 1 обе страницы обязаны показать total=2 и
    вместе накрыть обе строки. Фильтр по странице выдал бы на второй странице
    пустоту (на первой остались бы обе, порезанные лимитом) или врал бы total'ом.
    """
    first = get(client, discussion="unknown", limit=1, offset=0)
    second = get(client, discussion="unknown", limit=1, offset=1)
    assert first["total"] == second["total"] == 2
    assert len(first["rows"]) == 1 and len(second["rows"]) == 1
    assert usernames(first) | usernames(second) == {"alphaclub_chat", "Островок"}
    assert usernames(first).isdisjoint(usernames(second)), "одна строка на двух страницах"


def test_unknown_discussion_state_is_rejected_not_silently_empty(client):
    """Опечатка в значении — ошибка запроса с перечнем допустимых, а не «каналов
    нет»: пустой список выглядел бы как «обсуждения нигде не живые»."""
    r = client.get("/api/v1/channels", params={"discussion": "alive"})
    assert r.status_code == 422
    assert "live" in r.json()["detail"] and "unread" in r.json()["detail"]


# ── совместимость фильтров ────────────────────────────────────────────────────

def test_filters_combine_and_total_follows_both(client):
    body = get(client, q="клуб", chat_type="channel")
    assert body["total"] == 1
    assert usernames(body) == {"alphaclub"}

    body = get(client, discussion="unread", chat_type="channel")
    assert body["total"] == 1
    assert usernames(body) == {"corpostrovokru"}
