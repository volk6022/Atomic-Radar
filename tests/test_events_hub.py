"""Рассыльщик событий сам по себе: без HTTP и без базы.

Проверяется здесь то, что через ручку видно плохо или не видно вовсе: жизненный цикл
опроса, отбор событий по правам, поведение при отставшем подписчике и при упавшей базе.
Всё это свойства `app/services/events.py`, а не потока байтов, и гонять ради них
настоящий Postgres значило бы сделать быстрые проверки медленными и хрупкими.

Опрос базы подменяется целиком (`_snapshot`). Подмена честная: снимок — единственное
место, где модуль вообще ходит в базу, всё остальное к её содержимому равнодушно.
Заодно это даёт то, чего с настоящей базой дёшево не получить: падение опроса по
требованию и возврат к жизни на следующем такте.

Проверки ручки целиком — в `tests/test_events_sse_db.py`, там нужен Postgres.
"""
from __future__ import annotations

import asyncio
import contextlib
import os

import pytest

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")

from app.core.config import get_settings  # noqa: E402
from app.services import events  # noqa: E402

# Такт опроса в тестах. Достаточно мал, чтобы проверка не ждала, и достаточно велик,
# чтобы между тактами успевало проехать всё, что мы кладём в очередь руками.
TICK = 0.02

SNAP_A = {"alerts": {"unread": 1},
          "counters": {"drafts": 0, "conversations": 0},
          "runs": {"rows": []}}
SNAP_B = {"alerts": {"unread": 2},
          "counters": {"drafts": 0, "conversations": 0},
          "runs": {"rows": []}}


@pytest.fixture(autouse=True)
async def fast_poll(monkeypatch):
    """Короткий такт на время теста — и обязательная уборка рассыльщика после.

    Уборка автоматическая, а не по месту: подписчик, забытый в наборе, не уронил бы
    сам тест — он оставил бы вечно работающий опрос, и упал бы следующий, ищущий
    погасший. Связи между тестами через общий модуль разбирать дороже всего.

    Гасим просьбой и дожидаемся, а не отменяем: отмена посреди запроса оставляет
    соединение с открытой транзакцией — ровно то, из-за чего цикл и научился гаснуть
    на границе такта (см. `events.unsubscribe`).
    """
    monkeypatch.setenv("RADAR_EVENTS_POLL_INTERVAL", str(TICK))
    get_settings.cache_clear()
    yield
    for sub in list(events._subscribers):
        events.unsubscribe(sub)
    poll = events._poller
    if poll is not None:
        with contextlib.suppress(TimeoutError, asyncio.CancelledError):
            await asyncio.wait_for(poll, 2)
    events._poller = None
    events._stop = None
    events._state = None
    events._failure = None
    get_settings.cache_clear()


def snapshots(*values):
    """Подменить опрос базы последовательностью исходов.

    Элемент-исключение — такт, на котором база недоступна. Последний исход
    повторяется дальше: тесту почти всегда важно «а что потом, если ничего не
    меняется», и перечислять это руками было бы шумом.
    """
    queue = list(values)

    async def _fake() -> dict:
        value = queue.pop(0) if len(queue) > 1 else queue[0]
        if isinstance(value, Exception):
            raise value
        return value

    return _fake


async def take(sub: events.Subscriber, timeout: float = 2.0):
    """Ближайший кадр подписчика. С таймаутом, чтобы сломавшаяся проверка падала,
    а не висела до конца прогона."""
    return await asyncio.wait_for(sub.queue.get(), timeout)


async def collect(sub: events.Subscriber, count: int) -> list:
    return [await take(sub) for _ in range(count)]


def taken(sub: events.Subscriber) -> list:
    """Всё, что лежит в очереди прямо сейчас, — не дожидаясь ничего."""
    out = []
    while not sub.queue.empty():
        out.append(sub.queue.get_nowait())
    return out


# ── права ─────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("role,expected", [
    ("owner", {"alerts", "counters", "runs"}),
    ("customer", {"alerts", "counters", "runs"}),
    ("reviewer", {"alerts", "counters", "runs"}),
    # Гостю раздел прогонов закрыт (`Section.RUNS` — только штат), а тревоги и бейджи
    # живут в `dashboard`, открытом всем ролям.
    ("viewer", {"alerts", "counters"}),
])
async def test_events_are_filtered_by_the_access_matrix(role, expected):
    sub = events.Subscriber(role)
    assert {n for n in events.EVENT_ORDER if sub.wants(n)} == expected


async def test_service_events_reach_everyone():
    """`hello` и `error` — про сам поток, а не про данные: прятать их не от чего."""
    guest = events.Subscriber("viewer")
    assert guest.wants("hello") and guest.wants("error")


async def test_a_closed_event_is_not_even_queued():
    """Отбор происходит при укладке, а не при чтении: иначе кадр, который гостю не
    положен, всё равно занимал бы его очередь и вытеснял разрешённые."""
    guest = events.Subscriber("viewer")
    guest.push("runs", {"rows": [{"id": 1}]})
    assert guest.queue.empty()


# ── жизненный цикл опроса ─────────────────────────────────────────────────────

async def test_the_poll_starts_with_the_first_subscriber(monkeypatch):
    monkeypatch.setattr(events, "_snapshot", snapshots(SNAP_A))
    assert not events.poller_alive()

    sub = events.subscribe("owner")
    assert events.poller_alive()
    # Первый такт — без задержки: только что открытый экран не должен ждать целый
    # период опроса, чтобы увидеть хоть что-нибудь.
    assert await take(sub) == ("alerts", SNAP_A["alerts"])


async def test_the_last_one_to_leave_puts_the_poll_out(monkeypatch):
    monkeypatch.setattr(events, "_snapshot", snapshots(SNAP_A))
    first, second = events.subscribe("owner"), events.subscribe("owner")
    await take(first)
    poll = events._poller

    events.unsubscribe(first)
    assert events.subscriber_count() == 1
    assert events.poller_alive(), "ушёл не последний — опрос обязан продолжаться"

    events.unsubscribe(second)
    assert events.subscriber_count() == 0
    # Цикл выходит сам, на границе такта. Ждём именно его завершения, а не «пары
    # оборотов цикла событий»: такое ожидание врёт ровно в тот день, когда сломается.
    await asyncio.wait_for(poll, 2)
    assert not events.poller_alive()
    assert events._state is None, "снимок пережил опрос — следующий получит старьё"


async def test_a_second_wave_of_subscribers_revives_the_poll(monkeypatch):
    """Опрос гаснет и зажигается заново, а не гаснет однажды навсегда: между двумя
    рабочими днями подписчиков нет вообще, и процесс это переживает."""
    monkeypatch.setattr(events, "_snapshot", snapshots(SNAP_A))
    sub = events.subscribe("owner")
    await take(sub)
    poll = events._poller
    events.unsubscribe(sub)
    await asyncio.wait_for(poll, 2)

    again = events.subscribe("owner")
    assert events.poller_alive()
    assert await take(again) == ("alerts", SNAP_A["alerts"])


async def test_a_reload_does_not_restart_the_poll(monkeypatch):
    """Перезагрузка страницы — это «ушёл последний» и сразу «пришёл первый» с
    разницей в миллисекунды. Опрос, которому сказали гаснуть, но который ещё не
    догорел, просто передумывает: второй рядом с доживающим первым не поднимается.
    """
    monkeypatch.setattr(events, "_snapshot", snapshots(SNAP_A))
    first = events.subscribe("owner")
    await take(first)
    poll = events._poller

    events.unsubscribe(first)
    second = events.subscribe("owner")

    assert events._poller is poll
    await asyncio.sleep(TICK * 5)
    assert events.poller_alive()
    # Снимок не терялся — иначе пришедший получил бы пустоту вместо состояния.
    assert [n for n, _ in taken(second)] == list(events.EVENT_ORDER)


# ── снимок и разница ──────────────────────────────────────────────────────────

async def test_a_subscriber_gets_the_whole_state_at_once(monkeypatch):
    monkeypatch.setattr(events, "_snapshot", snapshots(SNAP_A))
    first = events.subscribe("owner")
    assert [n for n, _ in await collect(first, 3)] == list(events.EVENT_ORDER)

    # Пришедший позже получает снимок в момент подписки, не дожидаясь такта: экран,
    # открытый в тихий час, иначе был бы слеп до первого изменения в системе.
    late = events.subscribe("owner")
    assert [n for n, _ in taken(late)] == list(events.EVENT_ORDER)


async def test_only_the_changed_part_is_broadcast(monkeypatch):
    """Ради этого опрос и сравнивает снимки: слать три кадра каждые две секунды в
    каждую вкладку — это тот же опрос из браузера, только наизнанку."""
    monkeypatch.setattr(events, "_snapshot", snapshots(SNAP_A, SNAP_B))
    sub = events.subscribe("owner")
    await collect(sub, 3)

    assert await take(sub) == ("alerts", SNAP_B["alerts"])
    # Счётчики и прогоны не менялись — и не приехали.
    await asyncio.sleep(TICK * 5)
    assert sub.queue.empty()


# ── отставший подписчик ───────────────────────────────────────────────────────

async def test_a_slow_subscriber_is_caught_up_and_the_others_are_untouched():
    """Переполнение очереди — не повод рвать поток и не повод растить её без предела.

    Отставшему выбрасывают накопленное и выдают состояние заново: каждое событие несёт
    текущее значение целиком, поэтому в выброшенном не было ничего, чего нет в снимке.
    Проверяется здесь и второе, ради чего всё затевалось, — что сосед, читающий
    вовремя, ничего не заметил.
    """
    fast, slow = events.Subscriber("owner"), events.Subscriber("owner")
    events._subscribers.update({fast, slow})
    events._state = SNAP_A

    while slow.push("alerts", {"unread": 0}):
        pass
    assert slow.queue.qsize() == events.QUEUE_LIMIT

    events._broadcast([("alerts", SNAP_B["alerts"])])

    assert taken(fast) == [("alerts", SNAP_B["alerts"])]
    assert fast.resyncs == 0
    assert slow.resyncs == 1
    # Не хвост истории, а текущее состояние целиком.
    assert taken(slow) == events._frames(SNAP_A)


async def test_a_caught_up_guest_gets_only_what_he_may_see():
    """Выдача состояния заново идёт через ту же укладку, что и обычная рассылка, —
    иначе отставший гость получил бы прогоны в обход матрицы прав."""
    guest = events.Subscriber("viewer")
    events._subscribers.add(guest)
    events._state = SNAP_A

    while guest.push("alerts", {"unread": 0}):
        pass
    events._broadcast([("alerts", SNAP_B["alerts"])])

    assert [n for n, _ in taken(guest)] == ["alerts", "counters"]


# ── падение опроса ────────────────────────────────────────────────────────────

async def test_a_broken_poll_is_announced_and_so_is_the_recovery(monkeypatch):
    """Молчание — худший ответ: экран замирает на старых числах и выглядит исправным."""
    monkeypatch.setattr(events, "_snapshot",
                        snapshots(OSError("нет связи с базой"), SNAP_A))
    sub = events.subscribe("owner")

    name, data = await take(sub)
    assert name == "error"
    assert "нет связи с базой" in data["text"]

    assert await take(sub) == ("error", {"text": None})
    # После выздоровления — полный снимок: во время аварии могло измениться что угодно.
    assert await take(sub) == ("alerts", SNAP_A["alerts"])


async def test_a_broken_poll_is_announced_once(monkeypatch):
    """Тридцать одинаковых кадров за минуту недоступности — это не «не молчит»,
    это спам, в котором тонет всё остальное."""
    monkeypatch.setattr(events, "_snapshot", snapshots(OSError("нет связи с базой")))
    sub = events.subscribe("owner")

    assert (await take(sub))[0] == "error"
    await asyncio.sleep(TICK * 5)
    assert sub.queue.empty()


async def test_someone_arriving_during_an_outage_is_warned(monkeypatch):
    """Иначе он получил бы последний удачный снимок и считал бы его свежим."""
    monkeypatch.setattr(events, "_snapshot", snapshots(OSError("нет связи с базой")))
    first = events.subscribe("owner")
    assert (await take(first))[0] == "error"

    late = events.subscribe("owner")
    assert (await take(late))[0] == "error"


async def test_the_poll_survives_a_broken_database(monkeypatch):
    """Цикл не выходит по ошибке: база возвращается сама, а рвать из-за неё все
    открытые потоки значило бы заставить каждый браузер переподключаться."""
    monkeypatch.setattr(events, "_snapshot", snapshots(OSError("нет связи с базой")))
    sub = events.subscribe("owner")
    await take(sub)

    await asyncio.sleep(TICK * 5)
    assert events.poller_alive()
