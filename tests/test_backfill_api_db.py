"""Ручки очереди дочитывания — через HTTP, на настоящем Postgres.

Служба очереди (`app/services/backfill_queue.py`) слита 03.09 и с тех пор **не
используется никем**: ни одной ручки, ни экрана, ни исполнителя. Это и есть
незакрытый пункт 2.2 плана — «экран бэкфилла»; экран невозможно проверить, пока
поверхности нет.

Проверять на уровне сервиса недостаточно ровно потому же, почему и у реестра
сценариев: смысл этих ручек в том, что по ним человек принимает решение потратить
дневной бюджет чтений. Значит важны вещи, которых сервисный вызов не показывает —
отвечает ли ручка анониму, что видит заказчик, каким кодом отвечает отказ и назван
ли в тексте отказа следующий шаг.

Отдельно проверяется главное правило постановки: **группа, в которую аккаунт не
вступал, в очередь не ставится**, и человек узнаёт об этом в момент нажатия, а не
через сутки по красной строке в очереди.

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
"""
from __future__ import annotations

import asyncio
import os
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
from app.db.models import (BackfillItem, Base, Channel,  # noqa: E402
                           EngageInstance, User)
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.main import create_app  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

JOINED_AT = datetime(2026, 9, 4, 22, 6, tzinfo=timezone.utc)


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
        channel = Channel(peer_id=-1001, username="corpostrovokru", title="Канал",
                          chat_type="channel", ingest_enabled=True)
        joined = Channel(peer_id=-1002, username="corpostrovokru_chat", title="Чат",
                         chat_type="supergroup", ingest_enabled=True,
                         linked_joined_at=JOINED_AT, subscribed_account_id=3)
        stranger = Channel(peer_id=-1003, username="zloytam_chat", title="Чужой чат",
                           chat_type="supergroup", ingest_enabled=True)
        db.add_all([channel, joined, stranger])

        users = {}
        for role in ("owner", "customer", "viewer"):
            u = User(email=f"{role}@local", name=role, initials=role[:2].upper(),
                     role=role, password_hash="!нельзя-войти", totp_secret="X" * 32,
                     totp_confirmed=True, is_active=True)
            db.add(u)
            users[role] = u
        await db.commit()
        out = {"uids": {r: u.id for r, u in users.items()},
               "channel": channel.id, "joined": joined.id, "stranger": stranger.id}
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


def _items() -> list[BackfillItem]:
    """Прочитать очередь СВОИМ соединением, а не общей фабрикой сессий.

    Фабрика закеширована и держит пул, созданный в цикле событий приложения:
    `TestClient` крутит его в своём. Второй `asyncio.run` поверх того же пула не
    падает, а ЗАВИСАЕТ — на этом уже потерян час 05.09. Поэтому здесь тот же приём,
    что и в посеве: собственный движок, закрывающийся за собой.
    """
    async def go():
        engine = create_async_engine(DB_URL, poolclass=None)
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with maker() as db:
            rows = list((await db.execute(
                select(BackfillItem).order_by(BackfillItem.id))).scalars().all())
        await engine.dispose()
        return rows

    return asyncio.run(go())


def test_queue_is_closed_to_anonymous(client):
    """Очередь показывает, какие каналы мы читаем и каким аккаунтом. Это карта
    флота, и анониму её видеть незачем."""
    assert client.get("/api/v1/backfill/queue").status_code == 401


def test_owner_sees_an_empty_queue_with_a_summary(client, seeded):
    """Пустая очередь — это ответ, а не ошибка. И сводка обязана приходить сразу:
    экран не должен различать «пусто» и «не пришло»."""
    _login(client, seeded["uids"]["owner"])
    r = client.get("/api/v1/backfill/queue")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["items"] == []
    assert body["total"] == 0
    assert "summary" in body and body["summary"]["states"]["queued"] == 0


def test_enqueue_puts_channels_in_order_and_reports_what_it_did(client, seeded):
    """Ответ обязан называть поимённо, что поставлено, а что пропущено.

    «Поставлено 2 из 5» без перечня — это приглашение гадать, какие три и почему.
    """
    _login(client, seeded["uids"]["owner"])
    r = client.post("/api/v1/backfill/queue",
                    json={"channel_ids": [seeded["channel"], seeded["joined"]]})
    assert r.status_code in (200, 201, 202), r.text
    body = r.json()
    assert len(body["queued"]) == 2, body

    items = _items()
    assert [i.channel_id for i in items] == [seeded["channel"], seeded["joined"]]
    # Глубина проставлена сервером, а не оставлена на исполнителя.
    assert all(i.target and i.min_date for i in items)
    # Группа привязана к тому аккаунту, который в неё вступил.
    by_channel = {i.channel_id: i for i in items}
    assert by_channel[seeded["joined"]].account_id == 3
    assert by_channel[seeded["channel"]].account_id is None


def test_enqueueing_a_group_we_never_joined_is_refused_by_name(client, seeded):
    """Отказ на нажатии, с названным следующим шагом.

    Поставить такую группу в очередь значит через сутки показать человеку красную
    строку «не удалось» вместо простого «сначала вступите». Ставить нечего —
    значит и ответ не 200: иначе неверный запрос выглядит исполненным.
    """
    _login(client, seeded["uids"]["owner"])
    r = client.post("/api/v1/backfill/queue",
                    json={"channel_ids": [seeded["stranger"]]})
    assert r.status_code == 409, r.text
    assert "zloytam_chat" in r.text or "вступ" in r.text.lower(), r.text
    assert _items() == [], "после отказа в очереди не должно остаться ничего"


def test_one_bad_group_does_not_sink_the_whole_batch(client, seeded):
    """Кнопка «дочитать всем» на реестре из шестидесяти каналов не должна падать
    целиком из-за одной группы, в которую не вступили.

    Отказ по одному каналу — обычное дело на такой пачке. Ответ обязан назвать
    поимённо и поставленное, и отвергнутое с причиной: «поставлено 59 из 60» без
    перечня — это приглашение гадать, какой шестидесятый и почему.
    """
    _login(client, seeded["uids"]["owner"])
    r = client.post("/api/v1/backfill/queue",
                    json={"channel_ids": [seeded["channel"], seeded["stranger"],
                                          seeded["joined"]]})
    assert r.status_code in (200, 201, 202), r.text
    body = r.json()
    assert len(body["queued"]) == 2, body
    assert len(body["refused"]) == 1, body
    refused = body["refused"][0]
    assert refused["channel_id"] == seeded["stranger"]
    assert "zloytam_chat" in str(refused) or "вступ" in str(refused).lower(), refused

    queued = {i.channel_id for i in _items()}
    assert queued == {seeded["channel"], seeded["joined"]}, (
        "годные каналы обязаны встать в очередь, несмотря на соседа с отказом")


def test_depth_can_be_asked_for_and_is_bounded(client, seeded):
    """Глубину можно сузить, но не расширить сверх правил.

    2000 сообщений и месяц — потолки из постановки, а не значения по умолчанию,
    которые вежливо предлагаются. Просьба о большем — отказ, а не молчаливое
    урезание: молча урезанный запрос выглядит исполненным.
    """
    _login(client, seeded["uids"]["owner"])
    ok = client.post("/api/v1/backfill/queue",
                     json={"channel_ids": [seeded["channel"]], "target": 300,
                           "depth_days": 7})
    assert ok.status_code in (200, 201, 202), ok.text
    item = _items()[0]
    assert item.target == 300
    assert (datetime.now(timezone.utc) - item.min_date).days == 7

    too_much = client.post("/api/v1/backfill/queue",
                           json={"channel_ids": [seeded["joined"]], "target": 50_000})
    assert too_much.status_code == 422, too_much.text


def test_a_standing_item_can_be_taken_off_the_queue(client, seeded):
    _login(client, seeded["uids"]["owner"])
    client.post("/api/v1/backfill/queue", json={"channel_ids": [seeded["channel"]]})
    item_id = _items()[0].id

    r = client.delete(f"/api/v1/backfill/queue/{item_id}")
    assert r.status_code == 200, r.text
    assert _items()[0].state == "canceled"

    # Второй раз — отказ с объяснением, а не молчаливое «ок».
    again = client.delete(f"/api/v1/backfill/queue/{item_id}")
    assert again.status_code == 409, again.text


def test_viewer_may_look_but_not_spend_the_budget(client, seeded):
    """Смотреть очередь можно всем, кто вошёл: замершая очередь иначе выглядит
    поломкой. А ставить в неё — значит тратить дневной лимит чтений аккаунта, и
    это право отдельное."""
    _login(client, seeded["uids"]["viewer"])
    assert client.get("/api/v1/backfill/queue").status_code == 200
    denied = client.post("/api/v1/backfill/queue",
                         json={"channel_ids": [seeded["channel"]]})
    assert denied.status_code == 403, denied.text
    assert _items() == []


def test_unknown_channel_is_refused_not_silently_skipped(client, seeded):
    """Несуществующий канал — это ошибка вызывающего, и она обязана быть видна.
    Молча пропустить его значит показать «поставлено 0» без причины."""
    _login(client, seeded["uids"]["owner"])
    r = client.post("/api/v1/backfill/queue", json={"channel_ids": [999_999]})
    assert r.status_code in (404, 422), r.text
    assert _items() == []
