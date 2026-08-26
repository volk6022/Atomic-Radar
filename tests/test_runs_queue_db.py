"""Запуск прогона: очередь есть, очереди нет и очередь молчит — на живом Postgres.

Ручка `POST /api/v1/runs` — единственная кнопка, занимающая видеокарту на час, и до
этого среза за ней стоял `asyncio.create_task` внутри процесса API. Здесь проверяется
не сам прогон (он ходит в модель, у него свои тесты), а **развилка**: кто его
исполняет, что при этом отвечается человеку и в каком состоянии остаётся строка.

Три состояния, и все три разные:

* очереди нет — прогон идёт в этом же процессе и доходит до «готово», как раньше;
* очередь есть — ручка только ставит работу, строка ждёт в «в очереди», прогон здесь
  не выполняется;
* очередь есть и не отвечает — `503`, строка помечена упавшей, тревога поднята.

Третье состояние — главное. Строка `Run` к моменту отказа уже закоммичена, и оставить
её в «в очереди» значило бы не только вечно врать на экране, но и запереть кнопку:
`active_run` считает такую строку идущей задачей и не пустит следующую попытку.

Redis ни в одном случае не поднимается: наружу торчит `queue.enqueue`, его и
подменяем. База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты
пропускаются.
"""
from __future__ import annotations

import asyncio
import os
import time
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_DEBUG", "true")

from app.core import clock  # noqa: E402
from app.core.config import get_settings  # noqa: E402
from app.core.security import SessionSigner  # noqa: E402
from app.db.models import Alert, Base, EngageInstance, Run, User  # noqa: E402
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.main import create_app  # noqa: E402
from app.services import jobs, queue  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")


async def _seed() -> dict:
    """Владелец и инстанс Engage — минимум, при котором ручка запуска отвечает.

    Инстанс нужен не прогону, а старту приложения: без него `ensure_bootstrap`
    ругается в лог, и шум маскировал бы настоящие сообщения.
    """
    engine = create_async_engine(DB_URL, poolclass=None)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)

    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        db.add(EngageInstance(key="default", client_label="Основной",
                              base_url="http://engage.invalid",
                              api_key_env="RADAR_ENGAGE_API_KEY", is_active=True))
        owner = User(email="owner@local", name="owner", initials="OW", role="owner",
                     password_hash="!нельзя-войти", totp_secret="X" * 32,
                     totp_confirmed=True, is_active=True)
        db.add(owner)
        await db.commit()
        out = {"owner": owner.id}

    await engine.dispose()
    return out


@pytest.fixture
def seeded():
    """Посев в собственном цикле событий, полностью закрытый за собой.

    Живую `AsyncSession` в `TestClient` отдавать нельзя: он крутит приложение в своём
    цикле, а соединение asyncpg привязано к тому, где создано.
    """
    return asyncio.run(_seed())


@pytest.fixture
def client(seeded):
    previous = os.environ.get("RADAR_DATABASE_URL")
    os.environ["RADAR_DATABASE_URL"] = DB_URL
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_session_maker.cache_clear()

    with TestClient(create_app(), raise_server_exceptions=False) as c:
        token = SessionSigner(get_settings().SECRET_KEY).dumps(
            {"uid": seeded["owner"], "totp_ok": True})
        c.cookies.set(get_settings().SESSION_COOKIE, token)
        yield c

    if previous is None:
        os.environ.pop("RADAR_DATABASE_URL", None)
    else:
        os.environ["RADAR_DATABASE_URL"] = previous
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_session_maker.cache_clear()


@pytest.fixture(autouse=True)
def _clean_process_state():
    """Кэш пула и кэш отмены — модульные глобали; тест, оставивший их за собой,
    врёт следующему."""
    queue._pool = None
    jobs._CANCEL_CACHE.clear()
    yield
    queue._pool = None
    jobs._CANCEL_CACHE.clear()


async def _fetch(model) -> list:
    """Смотрим в базу мимо приложения: ответ ручки — не доказательство записи."""
    engine = create_async_engine(DB_URL, poolclass=None)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        out = list((await db.execute(select(model))).scalars().all())
    await engine.dispose()
    return out


def _runs() -> list:
    return asyncio.run(_fetch(Run))


def _await_status(run_id: int, status: str, limit: float = 10.0) -> str:
    """Дождаться статуса строки: прогон без очереди идёт в цикле событий приложения,
    а тест живёт в другом потоке и о его расписании ничего не знает."""
    deadline = time.monotonic() + limit
    seen = "—"
    while time.monotonic() < deadline:
        rows = [r for r in _runs() if r.id == run_id]
        seen = rows[0].status if rows else "—"
        if seen == status:
            return seen
        time.sleep(0.05)
    return seen


def _start(client, **params):
    return client.post("/api/v1/runs",
                       json={"kind": "reclassify", "params": params or {"scope": "pending"}})


# ── очереди нет: прежнее поведение целиком ────────────────────────────────────

def test_without_a_queue_the_run_goes_through_in_this_very_process(client, monkeypatch):
    """Стенд и тесты живут без Redis, и это рабочий режим, а не заглушка.

    Проверяем не «create_task вызван», а результат: строка доходит до «готово».
    Прогон при этом подменён — настоящий ходит в модель, — но путь от ручки до
    терминального статуса пройден целиком, тем самым кодом, что и с очередью.
    """
    calls: list[int] = []

    async def _fake_runner(run_id: int, params: dict) -> dict:
        calls.append(run_id)
        return {"checked": 0}

    monkeypatch.setattr(queue, "enabled", lambda: False)
    monkeypatch.setitem(jobs.RUNNERS, "reclassify", _fake_runner)

    r = _start(client)
    assert r.status_code == 200, r.text
    run_id = r.json()["id"]

    assert _await_status(run_id, "done") == "done"
    assert calls == [run_id], "прогон не пошёл в этом процессе"


# ── очередь есть: ручка только ставит работу ──────────────────────────────────

def test_with_a_queue_the_request_only_hands_the_work_over(client, monkeypatch):
    """Ручка обязана вернуться сразу, ничего не посчитав.

    Иначе весь переезд бессмыслен: тяжёлый прогон снова делил бы event loop с
    ручками интерфейса, а строка снова умирала бы вместе с процессом API.
    """
    seen: dict = {}
    ran: list[int] = []

    async def _fake_enqueue(task, *args, **kwargs):
        seen["task"], seen["args"] = task, args
        return "job-1"

    async def _fake_runner(run_id: int, params: dict) -> dict:
        ran.append(run_id)
        return {}

    monkeypatch.setattr(queue, "enabled", lambda: True)
    monkeypatch.setattr(queue, "enqueue", _fake_enqueue)
    monkeypatch.setitem(jobs.RUNNERS, "reclassify", _fake_runner)

    r = _start(client, scope="all")
    assert r.status_code == 200, r.text
    run_id = r.json()["id"]

    assert seen["task"] == queue.RUN_JOB
    assert seen["args"] == (run_id, "reclassify", {"scope": "all"})

    row = next(x for x in _runs() if x.id == run_id)
    assert row.status == "queued", "строка обязана ждать исполнителя, а не бежать сама"
    time.sleep(0.3)
    assert ran == [], "ручка исполнила прогон сама — переезд не состоялся"


def test_the_worker_gets_everything_it_needs_to_find_the_row(client, monkeypatch):
    """В очередь уезжает `run_id`, а не объект и не сессия.

    Строка в `runs` — источник истины, и воркер обязан читать её сам: любой снимок
    состояния, уехавший в Redis, устарел бы к моменту, когда его достанут.
    """
    seen: dict = {}

    async def _fake_enqueue(task, *args, **kwargs):
        seen["args"] = args
        return "job-1"

    monkeypatch.setattr(queue, "enabled", lambda: True)
    monkeypatch.setattr(queue, "enqueue", _fake_enqueue)

    run_id = _start(client).json()["id"]
    assert seen["args"][0] == run_id
    assert all(isinstance(a, (int, str, dict)) for a in seen["args"]), \
        "в Redis обязано уехать только то, что переживёт сериализацию"


# ── очередь есть и молчит ─────────────────────────────────────────────────────

def _dead_queue(monkeypatch) -> None:
    async def _dead(task, *args, **kwargs):
        raise queue.QueueUnavailable("соединение с Redis закрыто")

    monkeypatch.setattr(queue, "enabled", lambda: True)
    monkeypatch.setattr(queue, "enqueue", _dead)


def test_a_dead_queue_answers_503_not_500(client, monkeypatch):
    """`503` говорит «попробуй позже», `500` — «здесь баг».

    Разница не косметическая: по первому оператор ждёт и жмёт кнопку снова, по
    второму идёт искать поломку, которой нет.
    """
    _dead_queue(monkeypatch)
    r = _start(client)
    assert r.status_code == 503, r.text
    assert "очеред" in r.json()["detail"].lower()


def test_a_dead_queue_leaves_the_row_failed_not_queued(client, monkeypatch):
    """Строка уже закоммичена к моменту отказа, и «в очереди» было бы враньём:
    везти её некуда и начинать её некому."""
    _dead_queue(monkeypatch)
    assert _start(client).status_code == 503

    row = max(_runs(), key=lambda r: r.id)
    assert row.status == "failed"
    assert row.finished_at is not None, "незакрытая строка попадёт под метлу воркера"
    assert "очеред" in (row.error or "").lower()
    assert any("очеред" in line for line in (row.log or [])), \
        "причину надо видеть в самой задаче, а не только в логах контейнера"


def test_a_dead_queue_does_not_lock_the_button_for_the_next_attempt(client, monkeypatch):
    """Главная причина не оставлять строку в «в очереди».

    `active_run` считает «в очереди» идущей задачей: такая строка заперла бы кнопку
    навсегда, и вернувшаяся очередь ничего бы не изменила — вторую попытку ручка
    отбила бы `409` со ссылкой на прогон, который не начинался.
    """
    _dead_queue(monkeypatch)
    assert _start(client).status_code == 503
    assert _start(client).status_code == 503, "вторая попытка упёрлась в первую строку"
    assert len(_runs()) == 2


def test_a_dead_queue_wakes_the_operator(client, monkeypatch):
    """Прогон запускают и уходят. Молчаливый отказ означал бы, что о вставшей
    очереди узнают, когда кто-нибудь заметит непосчитанные сообщения."""
    _dead_queue(monkeypatch)
    assert _start(client).status_code == 503

    raised = [a for a in asyncio.run(_fetch(Alert)) if a.key == "runs_queue_down"]
    assert raised, "отказ запуска не поднял тревогу — снаружи он невидим"
    assert raised[0].severity == "error"


def test_the_alert_is_refreshed_not_multiplied(client, monkeypatch):
    """Очередь лежит минутами, кнопку жмут повторно. Каждая новая строка тревоги
    утопила бы в списке всё остальное."""
    _dead_queue(monkeypatch)
    for _ in range(3):
        assert _start(client).status_code == 503

    raised = [a for a in asyncio.run(_fetch(Alert)) if a.key == "runs_queue_down"]
    assert len(raised) == 1, f"тревог накопилось {len(raised)}"


# ── отмена через границу процессов ────────────────────────────────────────────

async def _make_run(status: str) -> int:
    engine = create_async_engine(DB_URL, poolclass=None)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        run = Run(name="Переклассификация · всё", kind="reclassify", params={},
                  status=status, progress=42, created_by="owner@local", log=[],
                  started_at=clock.utcnow())
        db.add(run)
        await db.commit()
        run_id = run.id
    await engine.dispose()
    return run_id


def test_cancel_reaches_a_run_living_in_another_process(client, monkeypatch):
    """Кнопка ставит флаг в базе, и это единственный путь, который работает всегда.

    Прогон опрашивает базу раз в три секунды и увидит флаг, в каком бы процессе он
    ни шёл. Кэш в памяти API при включённой очереди не нужен и не заполняется: смотреть
    в него здесь некому, а вычистить запись — тем более (это делает `execute` там, где
    прогон действительно шёл).
    """
    monkeypatch.setattr(queue, "enabled", lambda: True)
    run_id = asyncio.run(_make_run("running"))

    r = client.post(f"/api/v1/runs/{run_id}/cancel")
    assert r.status_code == 200, r.text

    row = next(x for x in _runs() if x.id == run_id)
    assert row.cancel_requested is True
    assert jobs._CANCEL_CACHE == {}, "процесс-локальный путь при очереди бессмыслен"


def test_cancel_without_a_queue_still_shortcuts_through_memory(client, monkeypatch):
    """Без очереди прогон идёт здесь же, и кэш экономит ему до трёх секунд ожидания.
    Ветка оставлена ровно за этим, а не по инерции."""
    monkeypatch.setattr(queue, "enabled", lambda: False)
    run_id = asyncio.run(_make_run("running"))

    assert client.post(f"/api/v1/runs/{run_id}/cancel").status_code == 200
    assert jobs._CANCEL_CACHE == {run_id: True}


# ── пометка прерванных ────────────────────────────────────────────────────────

async def _sweep(statuses: tuple[str, ...]) -> int:
    """Метла в собственном цикле: движок приложения привязан к чужому."""
    engine = create_async_engine(DB_URL, poolclass=None)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    with patch.object(jobs, "get_session_maker", lambda: maker):
        marked = await jobs.mark_interrupted(statuses=statuses)
    await engine.dispose()
    return marked


def test_the_worker_sweep_spares_the_row_that_is_still_waiting_in_redis(client):
    """Воркер метёт «выполняется» и не трогает «в очереди».

    Строка в «в очереди» смерти процесса не пережила: работа лежит в Redis и ждёт,
    когда её возьмут — а взять её собирается тот же воркер секундой позже. Пометь её
    прерванной, и экран соврал бы за миг до старта работы.
    """
    waiting = asyncio.run(_make_run("queued"))
    dead = asyncio.run(_make_run("running"))

    assert asyncio.run(_sweep(("running",))) == 1

    rows = {r.id: r for r in _runs()}
    assert rows[waiting].status == "queued"
    assert rows[dead].status == "interrupted"
    assert rows[dead].finished_at is not None


def test_the_api_sweep_takes_both_states_when_it_owns_the_runs(client):
    """Без очереди «в очереди» означает «корутина заведена и умерла, не начавшись».
    Такую строку не подберёт никто — её метут вместе с идущими."""
    waiting = asyncio.run(_make_run("queued"))
    dead = asyncio.run(_make_run("running"))

    assert asyncio.run(_sweep(jobs.ACTIVE)) == 2

    rows = {r.id: r for r in _runs()}
    assert rows[waiting].status == "interrupted"
    assert rows[dead].status == "interrupted"


def test_a_swept_run_frees_the_button(client):
    """Ради этого пометка и делается. Строка, навсегда застрявшая в «выполняется»,
    запирает `active_run`, и переклассификацию больше не запустить никогда."""
    asyncio.run(_make_run("running"))
    asyncio.run(_sweep(("running",)))

    async def _check() -> bool:
        engine = create_async_engine(DB_URL, poolclass=None)
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with maker() as db:
            busy = await jobs.active_run(db, "reclassify")
        await engine.dispose()
        return busy is None

    assert asyncio.run(_check()), "помеченная строка всё ещё считается идущей"
