"""Исполнитель очереди дочитывания (SCENARIOS S4, пункт плана 2.2).

Очередь `backfill_queue` умеет всё, кроме главного: `take_next` не зовёт никто.
Служба, ручки и правила глубины слиты 05.09, но работа из очереди не уезжает —
то есть «автоматический бэкфилл» из постановки Ивана от 04.09 не существует.
Этот файл описывает исполнителя до того, как он написан.

**Форма исполнителя — тик, а не пул с ожиданием.** Цепочку страниц двигает не наш
процесс: Радар просит у Engage одну страницу, Engage возвращает её вебхуком, и
`_continue_backfill` просит следующую. Значит воркеру нечего ждать — ждать пришлось
бы опросом состояния строки, а это и таймауты, и живой процесс, держащий очередь.
Тик поступает иначе: смотрит, у кого из аккаунтов нет начатого чтения, выдаёт
такому ровно один канал и завершается. Дальше цепочку ведут вебхуки, а следующий
тик по расписанию подберёт освободившихся.

**Один канал на аккаунт за раз** — не осторожность, а свойство очереди Engage: она
поаккаунтная и строго FIFO, два одновременных чтения одним аккаунтом встанут друг
за другом, потратив дневной бюджет вдвое быстрее без выигрыша по времени.

**Оборванная цепочка обязана возвращаться.** Вебхук может не прийти вовсе (задача
Engage убита таймаутом, доставка исчерпала попытки) — тогда элемент навсегда
остаётся `running`, и этот аккаунт больше никогда не получит работы. Поэтому тик
первым делом отбирает просроченные: `STALE_AFTER` без движения — назад в очередь,
а после `MAX_ATTEMPTS` попыток — `failed`. Здесь же закрывается долг, названный
агентом 05.09: колонка `attempts` считалась, а порога у неё не было.

**Границу окна везём в Engage, а не фильтруем после.** `get_chat_history` принимает
`min_date`, и «не глубже месяца» — это его параметр. Постфильтр означал бы, что
страницы за пределами окна всё равно прочитаны и списаны с дневного бюджета.

**Что реально прочитано, считается разницей.** `_continue_backfill` знает только
полное число сообщений канала, а элементу нужно своё: канал мог быть дочитан наполовину
раньше. Поэтому в адрес возврата едет `read0` — счётчик на момент старта цепочки, и
`read_total = total - read0`. Ставить `read_total` равным полному счётчику канала —
соврать в обе стороны сразу.

Проверяемый контракт:

    app/services/backfill_drain.py
        STALE_AFTER: timedelta         — сколько цепочка может молчать
        MAX_ATTEMPTS: int              — после скольких попыток элемент закрывается
        async tick(*, account_ids=None, now=None) -> dict
        async close_chain(db, *, item_id, ok, reason, read_total) -> None

    app/api/v1/ingest._request_page(..., min_date=None, item_id=0, read0=0)
        — `min_date` едет в payload, `item_id` и `read0` в адрес возврата;
    app/api/v1/ingest._continue_backfill
        — когда в параметрах есть `item_id`, остановка цепочки закрывает элемент.

⚠️ Один `asyncio.run` на тест. Движок и фабрика сессий кешируются, и второй
`asyncio.run` в том же тесте достаёт из пула соединение, привязанное к закрытому
циклу: в тестах базы это падение, в тестах поверх клиента — зависание без единого
сообщения.

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_INGEST_TOKEN", "test-ingest-token")
os.environ.setdefault("RADAR_SELF_BASE_URL", "http://radar.invalid")

from app.core.config import get_settings  # noqa: E402
from app.db.models import (BackfillItem, Base, Channel, EngageInstance,  # noqa: E402
                           Message)
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.services import engage  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

NOW = datetime(2026, 9, 5, 12, 0, tzinfo=timezone.utc)
HORIZON = datetime(2026, 8, 6, 12, 0, tzinfo=timezone.utc)   # месяц назад на постановке

# Четыре группы, в каждую вступил свой аккаунт: ровно то состояние, в котором прод
# стоит после первых вступлений. Пятая строка — канал без привязки: его может читать
# любой свободный аккаунт.
GROUPS = [("chat_one", 1), ("chat_two", 1), ("chat_three", 2), ("chat_four", 2)]


async def _reset() -> None:
    engine = create_async_engine(DB_URL, poolclass=None)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        db.add(EngageInstance(key="test", client_label="Тестовый клиент",
                              base_url="http://engage.invalid",
                              api_key_env="RADAR_ENGAGE_API_KEY", is_active=True))
        peer = -100_000
        for name, account_id in GROUPS:
            peer += 1
            db.add(Channel(peer_id=peer, username=name, title=f"Чат {name}",
                           chat_type="supergroup", ingest_enabled=True,
                           linked_joined_at=NOW, subscribed_account_id=account_id))
        peer += 1
        db.add(Channel(peer_id=peer, username="plain_channel", title="Канал",
                       chat_type="channel", ingest_enabled=True))
        await db.commit()
    await engine.dispose()


@pytest.fixture()
def db_ready():
    os.environ["RADAR_DATABASE_URL"] = DB_URL
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_session_maker.cache_clear()
    asyncio.run(_reset())
    yield


def _stub_engage(monkeypatch, calls: list, *, unavailable: bool = False):
    """Engage, который умеет ровно одно действие.

    `get_chat_info` здесь — ошибка, а не оптимизация: канал уже заведён, его
    `peer_id` и `username` лежат в строке, и лишняя карточка стоила бы чтения из
    дневного бюджета на каждый элемент очереди.
    """
    async def action(*, account_id, action, payload, webhook_url, **kw):
        assert action == "get_chat_history", (
            f"исполнитель очереди позвал {action}, а должен только читать историю")
        if unavailable:
            raise engage.EngageUnavailable("Engage не отвечает")
        calls.append({"account_id": account_id, "payload": payload,
                      "webhook_url": webhook_url})
        return {"task_id": f"t{len(calls)}"}

    monkeypatch.setattr(engage, "action", action)


async def _enqueue(items: list[dict]) -> list[int]:
    """Положить элементы в очередь напрямую: правила постановки проверены отдельно
    (`test_backfill_rules_db.py`), здесь проверяется выдача."""
    async with get_session_maker()() as db:
        made = []
        for i, item in enumerate(items):
            channel = (await db.execute(select(Channel).where(
                Channel.username == item["username"]))).scalar_one()
            row = BackfillItem(
                channel_id=channel.id, position=i + 1,
                state=item.get("state", "queued"),
                account_id=item.get("account_id"),
                target=item.get("target", 2000), min_date=item.get("min_date", HORIZON),
                attempts=item.get("attempts", 0),
                started_at=item.get("started_at"),
                read_total=item.get("read_total", 0))
            db.add(row)
            await db.flush()
            made.append(row.id)
        await db.commit()
        return made


async def _items() -> dict[int, BackfillItem]:
    """Читать состояние своим движком, закрывающимся за собой: общий кешированный
    движок после `asyncio.run` держит соединения от закрытого цикла."""
    engine = create_async_engine(DB_URL, poolclass=None)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        rows = (await db.execute(select(BackfillItem))).scalars().all()
        out = {row.id: row for row in rows}
    await engine.dispose()
    return out


def test_each_free_account_gets_exactly_one_channel(db_ready, monkeypatch):
    """Аккаунту выдаётся один канал за тик — очередь Engage поаккаунтная и FIFO.

    Второй канал того же аккаунта встал бы в ту же очередь за первым: время не
    выиграно, а дневной бюджет чтений потрачен вдвое быстрее.
    """
    from app.services import backfill_drain

    calls: list = []
    _stub_engage(monkeypatch, calls)

    async def go():
        await _enqueue([{"username": name, "account_id": acc}
                        for name, acc in GROUPS])
        return await backfill_drain.tick(account_ids=[1, 2], now=NOW)

    stats = asyncio.run(go())

    assert stats["started"] == 2, f"на два свободных аккаунта два чтения: {stats}"
    assert sorted(c["account_id"] for c in calls) == [1, 2]
    items = asyncio.run(_items())
    running = [i for i in items.values() if i.state == "running"]
    assert len(running) == 2
    assert {i.account_id for i in running} == {1, 2}
    assert all(i.attempts == 1 for i in running), "выдача обязана считать попытку"


def test_account_already_reading_gets_nothing(db_ready, monkeypatch):
    """Занятому аккаунту второй канал не выдаётся, даже если очередь полна."""
    from app.services import backfill_drain

    calls: list = []
    _stub_engage(monkeypatch, calls)

    async def go():
        await _enqueue([
            {"username": "chat_one", "account_id": 1, "state": "running",
             "started_at": NOW - timedelta(minutes=5), "attempts": 1},
            {"username": "chat_two", "account_id": 1},
            {"username": "chat_three", "account_id": 2},
        ])
        return await backfill_drain.tick(account_ids=[1, 2], now=NOW)

    stats = asyncio.run(go())

    assert stats["started"] == 1, f"работу получает только свободный аккаунт: {stats}"
    assert [c["account_id"] for c in calls] == [2]
    assert stats.get("busy") == 1, "занятость аккаунта обязана быть видна в итоге"


def test_first_page_carries_the_horizon_frozen_at_enqueue(db_ready, monkeypatch):
    """Границу окна везёт Engage, и это ровно та дата, что записана в элементе.

    Пересчитать «месяц назад» на момент выдачи — прочитать на неделю больше, чем
    показали человеку при нажатии, и потратить на это дневной бюджет.
    """
    from app.services import backfill_drain

    calls: list = []
    _stub_engage(monkeypatch, calls)

    async def go():
        ids = await _enqueue([{"username": "chat_one", "account_id": 1,
                               "target": 2000, "min_date": HORIZON}])
        await backfill_drain.tick(account_ids=[1], now=NOW)
        return ids[0]

    item_id = asyncio.run(go())

    assert len(calls) == 1
    payload = calls[0]["payload"]
    assert payload["min_date"].startswith("2026-08-06"), (
        f"в Engage не уехала граница окна: {payload}")
    url = calls[0]["webhook_url"]
    assert f"item_id={item_id}" in url, f"адрес возврата не знает про элемент: {url}"
    assert "target=2000" in url
    assert "read0=0" in url, "стартовый счётчик обязан ехать в адресе возврата"


def test_engage_unavailable_returns_the_item_to_the_queue(db_ready, monkeypatch):
    """Недоступный Engage — не вина канала: элемент возвращается в очередь.

    Закрыть его `failed` значило бы вычеркнуть канал из-за сетевого сбоя; попытка
    при этом уже посчитана, и три таких подряд элемент всё-таки закроют.
    """
    from app.services import backfill_drain

    calls: list = []
    _stub_engage(monkeypatch, calls, unavailable=True)

    async def go():
        ids = await _enqueue([{"username": "chat_one", "account_id": 1}])
        stats = await backfill_drain.tick(account_ids=[1], now=NOW)
        return ids[0], stats

    item_id, stats = asyncio.run(go())

    item = asyncio.run(_items())[item_id]
    assert item.state == "queued", f"элемент обязан вернуться в очередь, а не {item.state}"
    assert item.attempts == 1, "попытка посчитана и не теряется при возврате"
    assert item.started_at is None, "возвращённый элемент не выглядит начатым"
    assert stats["started"] == 0


def test_silent_chain_returns_to_the_queue_after_the_lease(db_ready, monkeypatch):
    """Цепочка, от которой не пришло ни одного вебхука, не держит аккаунт вечно.

    Так выглядит убитая задача Engage: элемент остаётся `running`, и без отбора по
    сроку этот аккаунт больше никогда не получит работы.
    """
    from app.services import backfill_drain

    calls: list = []
    _stub_engage(monkeypatch, calls)

    async def go():
        ids = await _enqueue([
            {"username": "chat_one", "account_id": 1, "state": "running",
             "attempts": 1,
             "started_at": NOW - backfill_drain.STALE_AFTER - timedelta(minutes=1)},
        ])
        stats = await backfill_drain.tick(account_ids=[1], now=NOW)
        return ids[0], stats

    item_id, stats = asyncio.run(go())

    assert stats["requeued"] == 1, f"просроченный элемент обязан вернуться: {stats}"
    item = asyncio.run(_items())[item_id]
    # Тот же тик тут же выдаёт его снова — свободный аккаунт и стоящая работа.
    assert item.state == "running"
    assert item.attempts == 2, "вторая выдача — вторая попытка"


def test_item_that_burned_its_attempts_is_closed_failed(db_ready, monkeypatch):
    """После `MAX_ATTEMPTS` элемент закрывается, а не возвращается в очередь вечно.

    Это и есть порог, которого у колонки `attempts` не было: без него канал,
    падающий каждый раз, крутится в очереди бесконечно и занимает аккаунт.
    """
    from app.services import backfill_drain

    calls: list = []
    _stub_engage(monkeypatch, calls)

    async def go():
        ids = await _enqueue([
            {"username": "chat_one", "account_id": 1, "state": "running",
             "attempts": backfill_drain.MAX_ATTEMPTS,
             "started_at": NOW - backfill_drain.STALE_AFTER - timedelta(minutes=1)},
        ])
        stats = await backfill_drain.tick(account_ids=[1], now=NOW)
        return ids[0], stats

    item_id, stats = asyncio.run(go())

    item = asyncio.run(_items())[item_id]
    assert item.state == "failed", f"исчерпавший попытки обязан закрыться: {item.state}"
    assert item.error, "закрытый элемент обязан объяснить, почему"
    assert item.finished_at is not None
    assert stats["failed"] == 1
    assert not calls, "закрытый элемент не заказывает чтений"


def test_finished_chain_closes_the_item_with_what_was_actually_read(db_ready,
                                                                    monkeypatch):
    """Конец цепочки закрывает элемент, и `read_total` — разница, а не счётчик канала.

    Канал мог быть дочитан наполовину раньше: полный счётчик приписал бы этому
    элементу чужую работу.
    """
    from app.api.v1 import ingest

    async def go():
        ids = await _enqueue([{"username": "chat_one", "account_id": 1,
                               "state": "running", "attempts": 1,
                               "started_at": NOW}])
        async with get_session_maker()() as db:
            channel = (await db.execute(select(Channel).where(
                Channel.username == "chat_one"))).scalar_one()
            for n in range(30):
                db.add(Message(channel_id=channel.id, tg_message_id=n + 1,
                               tg_date=NOW, text="сообщение", author_peer_id=7))
            await db.commit()
            reason = await ingest._continue_backfill(
                db, {"accepted": 10, "channel_id": channel.id,
                     "backfill_cursor": None},
                {"account_id": "1", "target": "2000", "item_id": str(ids[0]),
                 "read0": "10"})
            # Коммита здесь намеренно нет: закрытие элемента обязан сохранить сам
            # приём. Приём коммитит страницу ДО продолжения цепочки и после уже
            # ничего не коммитит — тест, коммитящий за него, описывал бы мир, где
            # у вызывающего есть шаг, которого у него нет (так `running` с нулём
            # и доехал до прода 05.09).
        return ids[0], reason

    item_id, reason = asyncio.run(go())

    assert "истори" in reason.lower() or "курсор" in reason.lower(), reason
    item = asyncio.run(_items())[item_id]
    assert item.state == "done", f"дочитанный элемент закрывается done: {item.state}"
    assert item.read_total == 20, (
        f"прочитано этим элементом 30-10=20, а записано {item.read_total}")
    assert item.finished_at is not None


def test_broken_chain_closes_the_item_failed(db_ready, monkeypatch):
    """Оборвавшаяся цепочка закрывает элемент отказом, а не оставляет его `running`.

    Страница записана, следующую попросить не вышло — это `failed` с причиной:
    молчащий `running` неотличим от идущей работы.
    """
    from app.api.v1 import ingest

    async def broken(**kw):
        raise engage.EngageUnavailable("Engage не отвечает")

    monkeypatch.setattr(ingest, "_request_page", broken)

    async def go():
        ids = await _enqueue([{"username": "chat_one", "account_id": 1,
                               "state": "running", "attempts": 1,
                               "started_at": NOW}])
        async with get_session_maker()() as db:
            channel = (await db.execute(select(Channel).where(
                Channel.username == "chat_one"))).scalar_one()
            db.add(Message(channel_id=channel.id, tg_message_id=1, tg_date=NOW,
                           text="сообщение", author_peer_id=7))
            await db.commit()
            await ingest._continue_backfill(
                db, {"accepted": 1, "channel_id": channel.id,
                     "backfill_cursor": 900},
                {"account_id": "1", "target": "2000", "prev_cursor": "1000",
                 "item_id": str(ids[0]), "read0": "0", "limit": "500"})
            # Коммита здесь нет по той же причине, что и выше.
        return ids[0]

    item_id = asyncio.run(go())

    item = asyncio.run(_items())[item_id]
    assert item.state == "failed", f"оборванная цепочка закрывает элемент: {item.state}"
    assert item.error, "у отказа обязана быть причина"


def test_unbound_channel_goes_to_any_free_account(db_ready, monkeypatch):
    """Канал без привязки читается любым свободным: членства он не требует.

    Привязка обязательна только для групп — туда живой поток идёт участнику.
    """
    from app.services import backfill_drain

    calls: list = []
    _stub_engage(monkeypatch, calls)

    async def go():
        await _enqueue([{"username": "plain_channel", "account_id": None}])
        return await backfill_drain.tick(account_ids=[3], now=NOW)

    stats = asyncio.run(go())

    assert stats["started"] == 1, f"непривязанный канал обязан уехать: {stats}"
    assert calls[0]["account_id"] == 3


def test_the_next_page_keeps_the_depth_window(db_ready, monkeypatch):
    """Окно глубины едет со страницы на страницу, а не действует на первую.

    05.09 `min_date` лежал только в payload первой страницы: продолжение
    собирается из адреса возврата, окна там не было, и заказ «не глубже месяца»
    дочитал канал до ноября 2020-го — 2032 сообщения через каскад вместо трёх
    десятков. Проверяется именно ВТОРАЯ страница: на первой параметр выглядел
    живым и в тестах, и глазами.
    """
    from app.api.v1 import ingest

    asked: list[dict] = []

    async def spy(**kw):
        asked.append(kw)

    monkeypatch.setattr(ingest, "_request_page", spy)

    window = HORIZON.isoformat()

    async def go():
        ids = await _enqueue([{"username": "chat_one", "account_id": 1,
                               "state": "running", "attempts": 1,
                               "started_at": NOW}])
        async with get_session_maker()() as db:
            channel = (await db.execute(select(Channel).where(
                Channel.username == "chat_one"))).scalar_one()
            db.add(Message(channel_id=channel.id, tg_message_id=1, tg_date=NOW,
                           text="сообщение", author_peer_id=7))
            await db.commit()
            await ingest._continue_backfill(
                db, {"accepted": 500, "channel_id": channel.id,
                     "backfill_cursor": 900},
                {"account_id": "1", "target": "2000", "prev_cursor": "1500",
                 "item_id": str(ids[0]), "read0": "0", "limit": "500",
                 "min_date": window})
        return ids[0]

    asyncio.run(go())

    assert asked, "цепочка обязана попросить следующую страницу"
    assert asked[0].get("min_date") == window, (
        f"окно потерялось на второй странице: {asked[0].get('min_date')!r}")
