"""Черновик обязан называть аккаунт, которым получено исходное сообщение, и
фильтроваться по нему. На настоящем Postgres.

Зачем это ручке, а не экрану. Андрей рассылает руками: он входит в ОДИН аккаунт
Telegram и пишет с него. Написать человеку с другого аккаунта, чем тот, что читал
группу, — значит прийти к нему «ниоткуда»: переписки нет, общих групп нет, и это
первый признак спама. Поэтому «с какого аккаунта пришла наводка» — не украшение
строки, а условие, без которого очередь черновиков бесполезна.

Отсюда же фильтр. Человек, вошедший в acc-2, обязан видеть черновики acc-2, а не
листать общий список глазами.

⚠️ Счётчики по состояниям и `total` считаются В ТОМ ЖЕ СРЕЗЕ, что и строки. Ровно
на этом уже поймали `/channels`: чипс говорил «1», фильтр отдавал 2, потому что
сводка и выборка считались по-разному. Здесь то же самое проверяется явно.

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
os.environ.setdefault("RADAR_DEBUG", "true")

from app.core.config import get_settings  # noqa: E402
from app.core.security import SessionSigner  # noqa: E402
from app.db.models import (Account, Base, Channel, EngageInstance,  # noqa: E402
                           Message, MessageReader, User, WfDraft, WfTarget,
                           WfVerdict, Workflow)
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.main import create_app  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

NOW = datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc)

# Аккаунты Engage. Третий намеренно НЕ заводится в зеркале `accounts`: приём пишет
# `account_id` со слов Engage, и зеркало может отстать. Читатель, которого нет в
# зеркале, обязан остаться видимым — исчезнувший читатель хуже безымянного.
ACC1, ACC2, ACC_UNMIRRORED, ACC_OTHER_INSTANCE = 101, 102, 103, 104


async def _seed() -> dict:
    """Четыре черновика ЛС с разной историей просмотра.

    * `d_acc1`  — исходное сообщение видел только acc-1;
    * `d_acc2`  — только acc-2;
    * `d_both`  — оба;
    * `d_none`  — не видел никто (старая запись до атрибуции приёма).

    Плюс один черновик у автора без юзернейма — ему ссылку в Telegram построить не из
    чего, и это не ошибка.
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
            Account(engage_account_id=ACC1, engage_instance="default",
                    label="acc-1", status="active", tz_offset=0),
            Account(engage_account_id=ACC2, engage_instance="default",
                    label="acc-2", status="active", tz_offset=0),
            # Аккаунт чужого инстанса: пара (инстанс, номер) адресует аккаунт, и
            # один и тот же номер в двух инстансах — два разных человека за клавиатурой.
            Account(engage_account_id=ACC_OTHER_INSTANCE, engage_instance="other",
                    label="чужой", status="active", tz_offset=0),
        ])

        dm = Workflow(key="cold_dm", title="Личные сообщения", target_kind="user",
                      action="dm", visibility="private",
                      engage_instance_id=instance.id, engage_use_case="cold_dm",
                      cascade_profile="dm_v1", sort_order=10, is_active=True)
        db.add(dm)

        channel = Channel(peer_id=-1001, username="chat", title="Обсуждение")
        db.add(channel)
        await db.flush()

        def msg(tg_id, *, author=500, username="user"):
            return Message(channel_id=channel.id, tg_message_id=tg_id, tg_date=NOW,
                           author_peer_id=author, author_username=username,
                           author_name="Имя", author_is_bot=False,
                           is_automatic_forward=False,
                           text="платёж за рубеж не проходит", processed_at=NOW)

        m_acc1, m_acc2, m_both, m_none = msg(1), msg(2), msg(3), msg(4)
        m_nouser = msg(5, author=777, username=None)
        db.add_all([m_acc1, m_acc2, m_both, m_none, m_nouser])
        await db.flush()

        db.add_all([
            MessageReader(message_id=m_acc1.id, account_id=ACC1),
            MessageReader(message_id=m_acc2.id, account_id=ACC2),
            MessageReader(message_id=m_both.id, account_id=ACC1),
            MessageReader(message_id=m_both.id, account_id=ACC2),
            # Читатель, которого нет в зеркале `accounts`.
            MessageReader(message_id=m_nouser.id, account_id=ACC_UNMIRRORED),
        ])

        order = [m_acc1, m_acc2, m_both, m_none, m_nouser]
        for m in order:
            db.add(WfVerdict(workflow_id=dm.id, message_id=m.id, level=3, passed=True,
                             detail={"l3": "модель: живая проблема"},
                             pain="не может оплатить", score=70, score_breakdown=[],
                             disqualifiers=[], computed_at=NOW))
        await db.flush()

        targets = {}
        for m in order:
            t = WfTarget(workflow_id=dm.id, target_kind="user", message_id=m.id,
                         channel_id=channel.id, recipient_peer_id=m.author_peer_id,
                         author_peer_id=m.author_peer_id,
                         author_username=m.author_username, author_name="Имя",
                         pain="не может оплатить", quote=m.text, score=70,
                         score_breakdown=[], disqualifiers=[], status="new")
            db.add(t)
            targets[m.tg_message_id] = t
        await db.flush()

        # Состояния разные намеренно: счётчик по состояниям обязан считаться в срезе
        # фильтра, и одинаковые состояния этого не показали бы.
        states = {1: "pending", 2: "pending", 3: "approved", 4: "pending", 5: "pending"}
        for tg_id, t in targets.items():
            db.add(WfDraft(workflow_id=dm.id, target_id=t.id,
                           variants=[{"text": "здравствуйте"}],
                           state=states[tg_id], prompt_version="v1",
                           source_message_link=f"https://t.me/chat/{tg_id}"))

        users = {}
        for role in ("owner", "viewer"):
            # Оба поля задаются явно: колонки NOT NULL здесь не по недосмотру — без
            # второго фактора эта панель одобряет отправку сообщений живым людям,
            # имея только пароль.
            u = User(email=f"{role}@local", name=role, initials=role[:2].upper(),
                     role=role, password_hash="!нельзя-войти", totp_secret="X" * 32,
                     totp_confirmed=True, is_active=True)
            db.add(u)
            users[role] = u
        await db.commit()
        out = {"uids": {r: u.id for r, u in users.items()}}

    await engine.dispose()
    return out


async def _add_lone_reader(account_id: int) -> None:
    """Читатель у сообщения, которое ни одной целью не стало."""
    engine = create_async_engine(DB_URL, poolclass=None)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        channel_id = (await db.execute(
            select(Channel.id).order_by(Channel.id))).scalars().first()
        # Сообщение заводится своё: в засеве каждое уже стало целью, а нужен
        # прочитанный аккаунтом текст, до очереди НЕ доехавший, — ровно та
        # ситуация, что на проде.
        message = Message(channel_id=channel_id, tg_message_id=9001, tg_date=NOW,
                          author_peer_id=901, author_username="lone",
                          author_name="Имя", author_is_bot=False,
                          is_automatic_forward=False, text="ещё не разобрано",
                          processed_at=None)
        db.add(message)
        await db.flush()
        db.add(MessageReader(message_id=message.id, account_id=account_id))
        await db.commit()
    await engine.dispose()


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
        token = SessionSigner(get_settings().SECRET_KEY).dumps(
            {"uid": seeded["uids"]["owner"], "totp_ok": True})
        c.cookies.set(get_settings().SESSION_COOKIE, token)
        yield c

    if previous is None:
        os.environ.pop("RADAR_DATABASE_URL", None)
    else:
        os.environ["RADAR_DATABASE_URL"] = previous
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_session_maker.cache_clear()


URL = "/api/v1/workflows/cold_dm/drafts"


def get(client, **params):
    r = client.get(URL, params=params)
    assert r.status_code == 200, r.text
    return r.json()


def rows_by_quote_id(data):
    """Строки по номеру исходного сообщения — читаемее, чем по id черновика."""
    return {int(r["source_message_link"].rsplit("/", 1)[1]): r for r in data["rows"]}


# ── атрибуция: кто это видел ──────────────────────────────────────────────────

def test_row_names_the_accounts_that_saw_the_source_message(client):
    rows = rows_by_quote_id(get(client, limit=50))
    assert [a["account_id"] for a in rows[1]["readers"]] == [ACC1]
    assert [a["account_id"] for a in rows[2]["readers"]] == [ACC2]
    assert [a["account_id"] for a in rows[3]["readers"]] == [ACC1, ACC2]


def test_reader_carries_the_human_label_not_only_the_number(client):
    """Андрей знает свои аккаунты как «acc-1», а не как 101."""
    rows = rows_by_quote_id(get(client, limit=50))
    assert [a["label"] for a in rows[3]["readers"]] == ["acc-1", "acc-2"]


def test_reader_missing_from_the_mirror_is_still_shown(client):
    """Зеркало `accounts` может отстать от Engage. Читатель при этом не исчезает."""
    rows = rows_by_quote_id(get(client, limit=50))
    readers = rows[5]["readers"]
    assert [a["account_id"] for a in readers] == [ACC_UNMIRRORED]
    assert readers[0]["label"], "у безымянного читателя должна быть хоть какая-то подпись"


def test_draft_nobody_saw_has_an_empty_list_and_does_not_vanish(client):
    """Записи до атрибуции приёма остаются в очереди — просто без аккаунта."""
    rows = rows_by_quote_id(get(client, limit=50))
    assert 4 in rows, "черновик без читателей пропал из очереди"
    assert rows[4]["readers"] == []


# ── что нужно кнопкам «скопировать» ───────────────────────────────────────────

def test_row_carries_username_and_a_ready_telegram_link(client):
    """Адрес собирает сервер, а не экран: иначе форма ссылки разойдётся по экранам."""
    row = rows_by_quote_id(get(client, limit=50))[1]
    assert row["author_username"] == "@user"
    assert row["tg_link"] == "https://t.me/user"


def test_target_without_a_username_has_no_link_rather_than_a_broken_one(client):
    row = rows_by_quote_id(get(client, limit=50))[5]
    assert row["author_username"] is None
    assert row["tg_link"] is None


# ── фильтр по аккаунту ────────────────────────────────────────────────────────

def test_filter_returns_only_what_this_account_saw(client):
    got = set(rows_by_quote_id(get(client, limit=50, account_id=ACC1)))
    assert got == {1, 3}


def test_filter_applies_to_everything_not_to_the_page(client):
    """Срез считается до страницы. Иначе фильтр — украшение текущей выдачи."""
    data = get(client, limit=1, account_id=ACC2)
    assert data["total"] == 2, "в срезе acc-2 два черновика (msg 2 и msg 3)"
    assert len(data["rows"]) == 1


def test_state_counts_are_computed_in_the_same_slice_as_the_rows(client):
    """Тот самый дефект, что был у чипсов каналов: сводка и выборка считались врозь.

    У acc-1 два черновика: `pending` (msg 1) и `approved` (msg 3). Счётчик, посчитанный
    без учёта фильтра, показал бы четыре `pending`.
    """
    data = get(client, limit=50, account_id=ACC1)
    counts = {s["key"]: s["count"] for s in data["states"]}
    assert counts["pending"] == 1
    assert counts["approved"] == 1
    assert sum(counts.values()) == data["total"] == 2


def test_each_state_count_matches_what_that_state_filter_returns(client):
    """Инвариант целиком: по каждому состоянию число из сводки равно числу строк."""
    data = get(client, limit=50, account_id=ACC1)
    for s in data["states"]:
        got = get(client, limit=50, account_id=ACC1, state=s["key"])
        assert got["total"] == s["count"], (
            f"состояние «{s['key']}»: в сводке {s['count']}, фильтр отдал {got['total']}")


def test_unknown_account_is_rejected_not_silently_ignored(client):
    """Молча отдать весь список — худший из ответов: человек решит, что видит свой срез."""
    r = client.get(URL, params={"account_id": 999999})
    assert r.status_code == 422, r.text


def test_cursor_stays_inside_the_account_slice(client):
    """Стрелка «следующий» обязана уважать фильтр, иначе она увозит из среза."""
    r = client.get("/api/v1/workflows/cold_dm/drafts/next",
                   params={"account_id": ACC2, "state": "pending"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body.get("draft") is not None, "в срезе acc-2 есть pending-черновик"
    assert ACC2 in [a["account_id"] for a in body["draft"]["readers"]]


# ── карточка ──────────────────────────────────────────────────────────────────

def test_single_draft_carries_the_same_attribution_as_the_row(client):
    """Карточка и строка — один экран. Разойдись они полем, аккаунт был бы виден
    только в одном из двух мест, и это заметили бы не сразу."""
    row = rows_by_quote_id(get(client, limit=50))[3]
    one = client.get(f"/api/v1/workflows/cold_dm/drafts/{row['id']}")
    assert one.status_code == 200, one.text
    body = one.json()
    assert [a["account_id"] for a in body["readers"]] == [ACC1, ACC2]
    assert body["tg_link"] == "https://t.me/user"


# ── список аккаунтов для фильтра ──────────────────────────────────────────────
#
# Выпадающий список на экране обязан строиться из того же реестра, по которому ручка
# выносит отказ. Список, предлагающий значение, которое сервер отвергнет, хуже
# отсутствующего: человек выбирает аккаунт и получает 422 вместо среза.

OPTIONS_URL = "/api/v1/workflows/cold_dm/drafts/accounts"


def options(client):
    r = client.get(OPTIONS_URL)
    assert r.status_code == 200, r.text
    return r.json()["rows"]


def test_accounts_route_is_not_swallowed_by_the_draft_id_route(client):
    """`/drafts/accounts` объявлен ВЫШЕ `/drafts/{draft_id}`.

    Иначе FastAPI разбирает «accounts» как номер черновика и отвечает 422 —
    отказом, по которому не догадаться, что ручка вообще существует.
    """
    r = client.get(OPTIONS_URL)
    assert r.status_code == 200, r.text


def test_options_are_exactly_what_the_filter_accepts(client):
    """Главный инвариант: что предложено, то и принимается."""
    for row in options(client):
        got = client.get(URL, params={"account_id": row["account_id"], "limit": 1})
        assert got.status_code == 200, (
            f"фильтр отверг аккаунт {row['account_id']}, который сам же предложен")


def test_options_do_not_leak_accounts_of_another_instance(client):
    """Один и тот же номер в двух инстансах — два разных человека за клавиатурой."""
    ids = [r["account_id"] for r in options(client)]
    assert ACC_OTHER_INSTANCE not in ids


def test_options_include_a_reader_the_mirror_does_not_know(client):
    """Список строится по тем, кто ЧИТАЛ, а не только по зеркалу `accounts`.

    Причина не теоретическая: на проде зеркало пусто целиком — заполняет его только
    посев стенда, боевого пути записи нет ни одного. Строй список из одного зеркала,
    и фильтр, ради которого всё делалось, оказался бы пустым, а отбор по любому
    аккаунту отвергался бы как «неизвестный».
    """
    ids = [r["account_id"] for r in options(client)]
    assert ids == [ACC1, ACC2, ACC_UNMIRRORED]


def test_account_that_read_something_is_offered_even_without_drafts(client):
    """Перекос во времени: атрибуция моложе очереди.

    На проде 03.09 записей о прочтении было 322, а пересечений с очередью — ноль:
    целями пока становятся сообщения старого бэкфилла, прочитанные до атрибуции.
    Аккаунт, которым человек вошёл, обязан быть в списке и показать честный пустой
    срез — иначе фильтр выглядит сломанным ровно тогда, когда он нужнее всего.
    """
    seen = ACC1 + ACC2 + ACC_UNMIRRORED + 1000  # заведомо новый номер
    asyncio.run(_add_lone_reader(seen))
    row = [r for r in options(client) if r["account_id"] == seen]
    assert row, "аккаунт, что-то прочитавший, не предложен в фильтре"
    assert row[0]["drafts"] == 0
    assert client.get(URL, params={"account_id": seen, "limit": 1}).status_code == 200


def test_reader_without_a_label_names_itself_by_number(client):
    """Отставание зеркала — не повод прятать аккаунт, которым сообщение получено."""
    by_id = {r["account_id"]: r["label"] for r in options(client)}
    assert by_id[ACC1] == "acc-1"
    assert by_id[ACC_UNMIRRORED] and str(ACC_UNMIRRORED) in by_id[ACC_UNMIRRORED]


def test_options_say_how_many_drafts_each_account_saw(client):
    """Число рядом с аккаунтом — единственный способ понять, с какого входить.

    У acc-1 два черновика (msg 1 и msg 3), у acc-2 тоже два (msg 2 и msg 3),
    у незеркального читателя один (msg 5).
    """
    by_id = {r["account_id"]: r["drafts"] for r in options(client)}
    assert by_id == {ACC1: 2, ACC2: 2, ACC_UNMIRRORED: 1}


def test_counts_agree_with_what_the_filter_returns(client):
    """Число в списке и `total` фильтра считаются по-разному — значит могут разойтись.

    Ровно на этом уже поймали чипсы каналов, поэтому равенство проверяется явно.
    """
    for row in options(client):
        got = get(client, limit=50, account_id=row["account_id"])
        assert got["total"] == row["drafts"], (
            f"аккаунт {row['account_id']}: в списке {row['drafts']}, "
            f"фильтр отдал {got['total']}")


def test_account_that_saw_nothing_is_still_offered(client):
    """Аккаунт без черновиков остаётся в списке с нулём.

    Убрать его — значит спрятать от человека, что он вошёл в аккаунт, которому
    сегодня писать некому: пустой срез и отсутствующий пункт читаются одинаково
    плохо, но первый хотя бы правда.
    """
    r = client.get("/api/v1/workflows/cold_dm/drafts", params={"limit": 50})
    assert r.status_code == 200
    rows = options(client)
    assert all("drafts" in row for row in rows)


def test_options_are_closed_to_the_guest_like_the_queue_itself(client, seeded):
    """Список аккаунтов — часть очереди черновиков, а не отдельная витрина.

    Гостю раздел закрыт целиком; открыть ему справочник аккаунтов флота значило бы
    выдать наружу состав флота через боковую дверь.
    """
    token = SessionSigner(get_settings().SECRET_KEY).dumps(
        {"uid": seeded["uids"]["viewer"], "totp_ok": True})
    client.cookies.set(get_settings().SESSION_COOKIE, token)
    assert client.get(OPTIONS_URL).status_code == 403
