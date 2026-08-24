"""Ручки реестра сценариев — через HTTP, на настоящем Postgres.

Проверять это на уровне сервиса недостаточно. Смысл ручек в том, что по ним оболочка
строит меню, а значит важны ровно те вещи, которые сервисный вызов не показывает:
отвечает ли она анониму, в каком порядке приходят строки, что происходит с
несуществующим ключом. Всё это живёт в HTTP-слое.

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
"""
from __future__ import annotations

import asyncio
import os

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_DEBUG", "true")

from app.core.config import get_settings  # noqa: E402
from app.core.security import SessionSigner  # noqa: E402
from app.db.models import Base, EngageInstance, User, Workflow  # noqa: E402
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.main import create_app  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")


async def _seed() -> dict[str, int]:
    """Три сценария: два действующих с разным `sort_order` и один выключенный.

    Выключенный нужен обязательно: без него «отдаём только действующие» проверялось бы
    на выборке, где выключенных и так нет, то есть не проверялось бы вовсе.
    """
    engine = create_async_engine(DB_URL, poolclass=None)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)

    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        instance = EngageInstance(key="default", client_label="Основной",
                                  base_url="http://engage:8103",
                                  api_key_env="RADAR_ENGAGE_API_KEY")
        db.add(instance)
        await db.flush()

        db.add_all([
            # Заведён вторым, а показан обязан быть первым: порядок задаёт sort_order.
            Workflow(key="public_reply", title="Публичные ответы",
                     target_kind="message", action="reply", visibility="public",
                     engage_instance_id=instance.id, engage_use_case="public_reply",
                     cascade_profile="dm_v1", sort_order=5, is_active=True),
            Workflow(key="cold_dm", title="Личные сообщения",
                     target_kind="user", action="dm", visibility="private",
                     engage_instance_id=instance.id, engage_use_case="cold_dm",
                     cascade_profile="dm_v1", sort_order=10, is_active=True),
            Workflow(key="retired", title="Отключённый",
                     target_kind="message", action="react", visibility="public",
                     engage_instance_id=instance.id, engage_use_case="reactions",
                     cascade_profile="dm_v1", sort_order=1, is_active=False),
        ])
        # По пользователю на роль: реестр обязан открываться всем вошедшим, и проверить
        # это можно только четырьмя разными сессиями, а не одной с перебором ролей.
        users = {}
        for role in ("owner", "customer", "reviewer", "viewer"):
            u = User(email=f"{role}@local", name=role, initials=role[:2].upper(),
                     role=role, password_hash="!нельзя-войти", totp_secret="X" * 32,
                     totp_confirmed=True, is_active=True)
            db.add(u)
            users[role] = u
        await db.commit()
        uids = {role: u.id for role, u in users.items()}

    await engine.dispose()
    return uids


@pytest.fixture
def seeded():
    """Посев выполняется в собственном цикле событий и полностью закрывается за собой.

    Отдать сюда живую `AsyncSession` нельзя: `TestClient` крутит приложение в своём
    цикле, а соединение asyncpg привязано к тому, в котором создано. Сессия, переданная
    из одного цикла в другой, валится не сразу и не понятно — «got result for unknown
    protocol state». Поэтому наружу отдаются только идентификаторы, а приложение
    открывает соединения само, уже в своём цикле.
    """
    return asyncio.run(_seed())


@pytest.fixture
def client(seeded):
    # Приложение должно смотреть в ту же тестовую базу. Движок и фабрика сессий
    # закешированы через lru_cache, поэтому кеши сбрасываются вместе с настройками —
    # иначе приложение осталось бы на движке, созданном для другой базы.
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


def _login(client, uid):
    token = SessionSigner(get_settings().SECRET_KEY).dumps({"uid": uid, "totp_ok": True})
    client.cookies.set(get_settings().SESSION_COOKIE, token)
    return client


@pytest.fixture
def authed(client, seeded):
    return _login(client, seeded["owner"])


# ── доступ ────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("path", [
    "/api/v1/workflows", "/api/v1/workflows/cold_dm",
    "/api/v1/workflows/cold_dm/sections",
])
def test_anonymous_gets_nothing(client, path):
    assert client.get(path).status_code == 401


@pytest.mark.parametrize("role", ["owner", "customer", "reviewer", "viewer"])
def test_any_logged_in_role_may_read_the_registry(client, seeded, role):
    """Реестр открыт любому вошедшему, а не разделу из матрицы.

    Меню рисуется до входа в какой-либо раздел. Потребуй ручка права на конкретный
    раздел — гость получил бы 403 при отрисовке оболочки и не увидел бы даже того,
    что ему разрешено. Особенно важен `viewer`: у него всего два раздела, и именно
    он первым упрётся в лишнюю проверку.
    """
    r = _login(client, seeded[role]).get("/api/v1/workflows")
    assert r.status_code == 200, role
    assert r.json()["total"] == 2


# ── список ────────────────────────────────────────────────────────────────────

def test_only_active_workflows_are_listed(authed):
    body = authed.get("/api/v1/workflows").json()
    keys = [w["key"] for w in body["rows"]]
    assert "retired" not in keys, "выключенный сценарий попал в меню"
    assert body["total"] == 2


def test_order_is_sort_order_not_insertion(authed):
    """Порядок блоков в меню задаётся `sort_order`. `retired` заведён с наименьшим
    порядком нарочно: попади он в выборку, он оказался бы первым и это было бы видно."""
    keys = [w["key"] for w in authed.get("/api/v1/workflows").json()["rows"]]
    assert keys == ["public_reply", "cold_dm"]


# ── состав разделов выводится из осей ─────────────────────────────────────────

def test_dm_block_has_conversations_and_no_activity(authed):
    body = authed.get("/api/v1/workflows/cold_dm/sections").json()
    keys = [s["key"] for s in body["sections"]]
    assert "conversations" in keys
    assert "activity" not in keys


def test_public_block_has_activity_and_no_conversations(authed):
    """Переписки у публичного ответа нет, и подменять её лентой активности нельзя:
    экран «переписок», собранный из одиночных комментариев, врал бы оператору."""
    keys = [s["key"] for s in
            authed.get("/api/v1/workflows/public_reply/sections").json()["sections"]]
    assert "activity" in keys
    assert "conversations" not in keys


def test_every_section_comes_with_a_title(authed):
    """Пункт без названия отрисовался бы пустой строкой в меню и ничем себя не выдал."""
    for key in ("cold_dm", "public_reply"):
        for section in authed.get(f"/api/v1/workflows/{key}/sections").json()["sections"]:
            assert section["title"], f"{key}/{section['key']} без названия"


def test_describe_and_sections_agree(authed):
    """Две ручки отдают один и тот же состав. Разойдясь, они дали бы меню, которое
    меняется от того, каким путём его открыли."""
    one = authed.get("/api/v1/workflows/cold_dm").json()
    sections = authed.get("/api/v1/workflows/cold_dm/sections").json()
    assert one["sections"] == sections["sections"]


# ── неизвестный ключ ──────────────────────────────────────────────────────────

@pytest.mark.parametrize("path", [
    "/api/v1/workflows/nope", "/api/v1/workflows/nope/sections",
])
def test_unknown_key_is_404_not_empty(authed, path):
    """404, а не пустой ответ: пустой состав разделов оболочка нарисует как блок без
    пунктов, и отличить «сценария нет» от «у сценария нет разделов» будет нельзя."""
    assert authed.get(path).status_code == 404
