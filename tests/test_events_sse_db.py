"""Ручка потока событий целиком: `GET /api/v1/events` на настоящем Postgres.

Это первый в проекте тест долгоживущего ответа, и устроен он не так, как остальные, —
по необходимости, а не из вкуса. Дальше объяснение, чтобы следующему такому тесту не
пришлось выяснять это заново.

**`TestClient` здесь не годится.** Он синхронный: `client.get()` не вернётся, пока
ответ не кончился, а этот ответ не кончается никогда. Прогон повис бы целиком.

**`httpx.AsyncClient` с `ASGITransport` — тоже, и это не очевидно.** Транспорт выглядит
подходящим, но он **буферизует ответ целиком**: `handle_async_request` ждёт, пока
приложение договорит (`await self.app(...)`), и только потом склеивает накопленные
куски в тело. Бесконечный ответ он будет собирать вечно. Хуже того, его `receive()`
отдаёт `http.disconnect` только после того, как ответ завершён, — то есть разорвать
связь снаружи, как это делает закрытая вкладка, через него физически нельзя, а
разрыв — половина того, что здесь надо проверить. Для конечных ответов (отказ анониму)
он подходит, и там он и используется.

**Поэтому приложение вызывается напрямую как ASGI-приложение** — тонкой обвязкой
`Stream` ниже. Она даёт ровно то, чего не хватает: кадры по мере поступления, разрыв
связи по требованию и отдельно — отмену корутины, то есть два разных способа, которыми
подписчик исчезает в жизни. `scope` собран так же, как его собирает uvicorn; важна
там одна деталь — `spec_version` ниже `2.4`, иначе `StreamingResponse` перестаёт
слушать `http.disconnect` и полагается на ошибку записи в сокет, которого у нас нет.

Каждая проверка заканчивается сама: любое ожидание кадра — с таймаутом, каждый поток
закрывается, а рассыльщик после теста приводится в исходное состояние фикстурой. Тест,
который заканчивается по таймауту прогона, — это тест, который никто больше не запустит.

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
Проверки самого рассыльщика, которым база не нужна, — в `tests/test_events_hub.py`.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
from collections import namedtuple

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
# В отличие от остальных тестов с базой — без `RADAR_DEBUG`: он включает эхо SQL, а
# опрос идёт тактами всё время, пока открыт поток. Полезный вывод утонул бы в логе
# одних и тех же четырёх запросов.
os.environ.setdefault("RADAR_DEBUG", "false")

from app.core.config import get_settings  # noqa: E402
from app.core.security import SessionSigner  # noqa: E402
from app.db.models import Alert, Base, Run, User  # noqa: E402
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.main import create_app  # noqa: E402
from app.services import events  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

# Такт опроса и сердцебиение на время теста. Сердцебиение заметно длиннее такта:
# иначе «пришёл пинг, значит события не было» перестало бы что-либо доказывать.
TICK = 0.05
HEARTBEAT = 0.4

ENV_KEYS = ("RADAR_DATABASE_URL", "RADAR_EVENTS_POLL_INTERVAL", "RADAR_EVENTS_HEARTBEAT")


# ── посев ─────────────────────────────────────────────────────────────────────

async def _create_schema() -> None:
    engine = create_async_engine(DB_URL)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)
    await engine.dispose()


@pytest.fixture(scope="module")
def schema():
    """Схема заводится один раз на файл, данные — на каждый тест.

    Не из экономии ради экономии: тестовая база отвечает медленно, и `create_all` на
    полсотни таблиц перед каждой проверкой занимал бы больше времени, чем сами
    проверки. Изоляция при этом не страдает — данные всё равно вычищаются целиком.
    """
    asyncio.run(_create_schema())


async def _seed() -> dict:
    """Четыре роли и одна непрочитанная тревога поверх пустой схемы.

    Тревога нужна ненулевая: снимок из одних нулей одинаково выглядит и когда он
    посчитан, и когда он не посчитан вовсе.

    `RESTART IDENTITY` — не косметика: тесты сверяют идентификаторы в кадрах, и без
    сброса счётчиков второй прогон в том же файле сверял бы их с другими числами.
    """
    engine = create_async_engine(DB_URL)
    tables = ", ".join(t.name for t in Base.metadata.sorted_tables)
    async with engine.begin() as conn:
        await conn.execute(text(f"TRUNCATE TABLE {tables} RESTART IDENTITY CASCADE"))

    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        db.add(Alert(key="seed", text="Тревога из посева", severity="warn"))
        users = {}
        for role in ("owner", "customer", "reviewer", "viewer"):
            u = User(email=f"{role}@local", name=role, initials=role[:2].upper(),
                     role=role, password_hash="!нельзя-войти", totp_secret="X" * 32,
                     totp_confirmed=True, is_active=True)
            db.add(u)
            users[role] = u
        await db.commit()
        out = {r: u.id for r, u in users.items()}

    await engine.dispose()
    return out


@pytest.fixture
def seeded(schema):
    """Посев в собственном цикле событий, полностью закрытый за собой: соединение
    asyncpg привязано к тому циклу, где создано, и отдавать его тесту нельзя."""
    return asyncio.run(_seed())


@pytest.fixture
async def app(seeded):
    """Приложение поверх тестовой базы — и уборка рассыльщика после.

    Уборка обязательна и делается фикстурой, а не тестами: подписчик, оставшийся в
    наборе, тянет за собой живой опрос, а живой опрос переживает конец теста и
    начинает сыпать в закрытый цикл событий. Симптом при этом всплывает в чужом
    тесте, и искать его приходится от чужого симптома.
    """
    saved = {k: os.environ.get(k) for k in ENV_KEYS}
    os.environ["RADAR_DATABASE_URL"] = DB_URL
    os.environ["RADAR_EVENTS_POLL_INTERVAL"] = str(TICK)
    os.environ["RADAR_EVENTS_HEARTBEAT"] = str(HEARTBEAT)
    _clear_caches()

    yield create_app()

    for sub in list(events._subscribers):
        events.unsubscribe(sub)
    poll = events._poller
    if poll is not None:
        # Гасим просьбой и дожидаемся, а не отменяем: отмена посреди запроса оставляет
        # соединение с открытой транзакцией, и следующий же посев встаёт намертво на
        # `DROP SCHEMA` в ожидании чужой блокировки. Так это и нашлось.
        with contextlib.suppress(TimeoutError, asyncio.CancelledError):
            await asyncio.wait_for(poll, 2)
    events._poller = None
    events._stop = None
    events._state = None
    events._failure = None

    await get_engine().dispose()
    for key, value in saved.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
    _clear_caches()


def _clear_caches() -> None:
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_session_maker.cache_clear()


def token_for(uid: int) -> str:
    return SessionSigner(get_settings().SECRET_KEY).dumps({"uid": uid, "totp_ok": True})


# ── обвязка над ASGI ──────────────────────────────────────────────────────────

Frame = namedtuple("Frame", "event data")
COMMENT = ":"


def parse(raw: str) -> Frame:
    """Кадр SSE → имя события и разобранное тело. Комментарий (строка с двоеточия)
    возвращается под именем `:` — клиент его игнорирует, а тесту он говорит, что
    поток жив и ничего другого не приехало."""
    if raw.startswith(COMMENT):
        return Frame(COMMENT, raw[1:].strip())
    name, payload = None, None
    for line in raw.splitlines():
        field, _, value = line.partition(":")
        if field == "event":
            name = value.strip()
        elif field == "data":
            payload = value.strip()
    assert name is not None and payload is not None, f"кадр без event/data: {raw!r}"
    return Frame(name, json.loads(payload))


class Stream:
    """Открытый ответ, читаемый покадрово, с двумя способами оборвать связь."""

    def __init__(self, app, token: str | None = None):
        self._app = app
        self._token = token
        self._chunks: asyncio.Queue[bytes] = asyncio.Queue()
        self._disconnect = asyncio.Event()
        self._asked = False
        self._started = asyncio.Event()
        self._buffer = ""
        self.status: int | None = None
        self.headers: dict[str, str] = {}
        self.task: asyncio.Task | None = None

    def _scope(self) -> dict:
        headers = [(b"host", b"testserver")]
        if self._token:
            headers.append((b"cookie",
                            f"{get_settings().SESSION_COOKIE}={self._token}".encode()))
        return {
            "type": "http",
            # `spec_version` ниже 2.4 — не декорация: с 2.4 `StreamingResponse`
            # перестаёт слушать `http.disconnect` и ждёт ошибки записи в сокет.
            # Сокета здесь нет, и разрыв связи стало бы нечем изобразить. Uvicorn,
            # за которым мы работаем в проде, объявляет ровно эту ветку.
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "http_version": "1.1", "method": "GET", "scheme": "http",
            "path": "/api/v1/events", "raw_path": b"/api/v1/events",
            "query_string": b"", "root_path": "", "headers": headers,
            "client": ("127.0.0.1", 51234), "server": ("testserver", 80),
        }

    async def _receive(self) -> dict:
        if not self._asked:
            self._asked = True
            return {"type": "http.request", "body": b"", "more_body": False}
        await self._disconnect.wait()
        return {"type": "http.disconnect"}

    async def _send(self, message) -> None:
        if message["type"] == "http.response.start":
            self.status = message["status"]
            self.headers = {k.decode().lower(): v.decode()
                            for k, v in message.get("headers", [])}
            self._started.set()
        elif message["type"] == "http.response.body":
            body = message.get("body", b"")
            if body:
                self._chunks.put_nowait(body)
            if not message.get("more_body", False):
                self._chunks.put_nowait(b"")  # признак конца ответа

    async def open(self, timeout: float = 15.0) -> Stream:
        # Срок щедрый намеренно. Ответ начинается после проверки пользователя, то есть
        # после похода в базу, а холодное соединение к тестовой базе занимает секунды.
        # Жёсткий срок здесь ловил бы не ошибку в коде, а медленную базу; поток при
        # этом всё равно заканчивается сам — сроком, а не зависанием прогона.
        self.task = asyncio.create_task(
            self._app(self._scope(), self._receive, self._send))
        try:
            await asyncio.wait_for(self._started.wait(), timeout)
        except TimeoutError:
            if self.task.done():
                self.task.result()  # настоящая причина, если приложение упало
            raise
        return self

    async def frame(self, timeout: float = 5.0) -> Frame:
        while "\n\n" not in self._buffer:
            chunk = await asyncio.wait_for(self._chunks.get(), timeout)
            assert chunk, "поток закончился, не отдав кадра целиком"
            self._buffer += chunk.decode()
        raw, self._buffer = self._buffer.split("\n\n", 1)
        return parse(raw)

    async def until(self, *names: str, timeout: float = 8.0) -> dict:
        """Кадры с этими именами, в любом порядке, пропуская сердцебиение.

        Пропускать приходится: сердцебиение приезжает по своему расписанию и на
        медленной базе легко опережает данные. Проверка, написанная как «следующие
        три кадра», разваливалась бы от скорости базы, а не от ошибки в коде.

        Общий срок ограничен: ожидание события, которое не придёт, обязано падать,
        а не висеть до таймаута всего прогона.
        """
        clock = asyncio.get_running_loop()
        deadline = clock.time() + timeout
        got: dict[str, dict] = {}
        while len(got) < len(names):
            left = deadline - clock.time()
            assert left > 0, f"не дождались событий: {set(names) - set(got)}"
            frame = await self.frame(left)
            if frame.event in names:
                got[frame.event] = frame.data
        return got

    async def close(self, timeout: float = 3.0) -> None:
        """Разрыв связи — так уходит закрытая вкладка."""
        self._disconnect.set()
        await asyncio.wait_for(self.task, timeout)

    async def abort(self, timeout: float = 3.0) -> None:
        """Отмена корутины — так уходит поток при остановке процесса."""
        self.task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(self.task, timeout)


async def add(*rows) -> None:
    """Дописать строки той же сессией, которой пользуется приложение: изменение
    обязано доехать до подписчика через опрос, а не через подставленный объект."""
    async with get_session_maker()() as db:
        db.add_all(list(rows))
        await db.commit()


# ── вход ──────────────────────────────────────────────────────────────────────

async def test_anonymous_is_refused(app):
    """Отказ — ответ конечный, поэтому здесь обычный клиент, а не обвязка выше."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport,
                                 base_url="http://testserver") as client:
        r = await client.get("/api/v1/events")
    assert r.status_code == 401
    assert events.subscriber_count() == 0
    assert not events.poller_alive(), "отказ не должен поднимать опрос"


async def test_a_forged_cookie_is_refused(app):
    """Подпись проверяется той же функцией, что и на всех ручках: поток не должен
    оказаться дверью с другим замком."""
    stream = Stream(app, token="совершенно-не-подписанный-токен")
    await stream.open()
    assert stream.status == 401
    await stream.close()


# ── форма ответа ──────────────────────────────────────────────────────────────

async def test_the_response_is_a_stream_and_nothing_buffers_it(app, seeded):
    stream = await Stream(app, token_for(seeded["owner"])).open()

    assert stream.status == 200
    assert stream.headers["content-type"].startswith("text/event-stream")
    assert stream.headers["cache-control"] == "no-store"
    # Без этого прокси копит кадры в своём буфере, и «реальное время» превращается
    # в случайную задержку.
    assert stream.headers["x-accel-buffering"] == "no"
    # Длины у бесконечного ответа быть не может, и заявлять её нечем.
    assert "content-length" not in stream.headers

    await stream.close()


async def test_hello_comes_first_with_the_interval_and_the_sections(app, seeded):
    stream = await Stream(app, token_for(seeded["owner"])).open()

    hello = await stream.frame()
    assert hello.event == "hello"
    assert hello.data["interval"] == TICK
    assert "runs" in hello.data["sections"]
    assert hello.data["events"] == ["alerts", "counters", "runs"]

    await stream.close()


async def test_the_whole_snapshot_arrives_without_waiting_for_a_change(app, seeded):
    """Иначе только что открытый экран слеп до тех пор, пока в системе что-нибудь
    не произойдёт, — а произойти может и через час."""
    stream = await Stream(app, token_for(seeded["owner"])).open()
    assert (await stream.frame()).event == "hello"

    snapshot = await stream.until(*events.EVENT_ORDER)
    assert snapshot["alerts"] == {"unread": 1}
    assert snapshot["counters"] == {"drafts": 0, "conversations": 0}
    assert snapshot["runs"] == {"rows": []}

    await stream.close()


# ── изменения ─────────────────────────────────────────────────────────────────

async def test_a_change_in_the_database_arrives_as_an_event(app, seeded):
    """То, ради чего всё и делалось: событие рождается из опроса базы, а не из
    публикации в месте правки — писать тревогу можно откуда угодно, хоть из воркера."""
    stream = await Stream(app, token_for(seeded["owner"])).open()
    assert (await stream.frame()).event == "hello"
    assert (await stream.until("alerts"))["alerts"] == {"unread": 1}

    await add(Alert(key="new", text="Ещё одна тревога", severity="error"))

    assert (await stream.until("alerts"))["alerts"] == {"unread": 2}
    await stream.close()


async def test_a_run_reaches_the_staff_and_never_the_guest(app, seeded):
    """Права разных событий различаются, и одного `requires(...)` на ручку не хватило
    бы: гостю тревоги положены, прогоны — нет. Проверяем обе половины сразу, потому
    что «никому не пришло» тоже прошло бы половинчатую проверку.
    """
    owner = await Stream(app, token_for(seeded["owner"])).open()
    guest = await Stream(app, token_for(seeded["viewer"])).open()

    assert (await owner.frame()).event == "hello"
    await owner.until(*events.EVENT_ORDER)

    hello = await guest.frame()
    assert hello.data["events"] == ["alerts", "counters"], "гостю прогоны не положены"
    await guest.until("alerts", "counters")

    await add(Run(name="Переклассификация", kind="reclassify", status="running",
                  progress=42, log=[], params={}))

    assert (await owner.until("runs"))["runs"]["rows"] == [
        {"id": 1, "kind": "reclassify", "status": "running", "progress": 42.0}]
    # А гость за то же время получает только сердцебиение: кадра `runs` для него нет.
    assert (await guest.frame()).event == COMMENT

    await owner.close()
    await guest.close()


async def test_a_quiet_stream_is_kept_alive_by_a_comment(app, seeded):
    """Через соединение, по которому долго ничего не идёт, прокси и клиент считают
    связь мёртвой. Комментарий — часть формата SSE и клиентом игнорируется."""
    stream = await Stream(app, token_for(seeded["owner"])).open()
    assert (await stream.frame()).event == "hello"
    await stream.until(*events.EVENT_ORDER)

    # Дальше в системе ничего не происходит, и единственное, что вправе приехать по
    # молчащему потоку, — комментарий.
    assert (await stream.frame()).event == COMMENT
    await stream.close()


# ── уход подписчика ───────────────────────────────────────────────────────────

async def test_leaving_removes_the_subscription_and_the_last_one_puts_the_poll_out(
        app, seeded):
    first = await Stream(app, token_for(seeded["owner"])).open()
    second = await Stream(app, token_for(seeded["reviewer"])).open()
    assert (await first.frame()).event == "hello"
    assert (await second.frame()).event == "hello"
    assert events.subscriber_count() == 2
    poll = events._poller

    await first.close()
    assert events.subscriber_count() == 1
    assert events.poller_alive(), "ушёл не последний — опрос обязан продолжаться"

    await second.close()
    assert events.subscriber_count() == 0
    # Цикл выходит сам, на границе такта, где не открыто ни сессии, ни транзакции.
    await asyncio.wait_for(poll, 2)
    assert not events.poller_alive()


async def test_cancelling_the_coroutine_removes_the_subscription_too(app, seeded):
    """Второй способ, которым подписчик исчезает: не разрыв связи, а отмена задачи —
    так уходит поток при остановке процесса. Уборка живёт в `finally`, и если бы она
    была написана через `await`, здесь бы она не доработала."""
    stream = await Stream(app, token_for(seeded["owner"])).open()
    assert (await stream.frame()).event == "hello"
    assert events.subscriber_count() == 1
    poll = events._poller

    await stream.abort()
    assert events.subscriber_count() == 0
    await asyncio.wait_for(poll, 2)
    assert not events.poller_alive()


# ── соединения с базой ────────────────────────────────────────────────────────

async def test_the_stream_holds_no_database_connection(app, seeded, monkeypatch):
    """Самое неочевидное свойство ручки — и единственное, чья поломка выглядит как
    «завис весь сайт», а не «сломался поток».

    Возьми ручка сессию через `GetDB`, соединение из пула держалось бы до конца отдачи
    тела, то есть часами; при `pool_size=5, max_overflow=10` полтора десятка вкладок
    оставили бы без базы весь остальной API. Опрос на время проверки подменён — он
    берёт соединение на доли секунды каждый такт, и его мелькание в пуле не давало бы
    отличить «ручка держит» от «опрос как раз читает».
    """
    async def _no_database() -> dict:
        return {"alerts": {"unread": 0},
                "counters": {"drafts": 0, "conversations": 0},
                "runs": {"rows": []}}

    monkeypatch.setattr(events, "_snapshot", _no_database)

    streams = []
    for role in ("owner", "customer", "reviewer"):
        stream = await Stream(app, token_for(seeded[role])).open()
        assert (await stream.frame()).event == "hello"
        streams.append(stream)

    assert get_engine().sync_engine.pool.checkedout() == 0, (
        "поток держит соединение из пула — при десятке вкладок API останется без базы")

    for stream in streams:
        await stream.close()
