"""Сценарий 1 «вступил → дочитал» (волна Б): швы постановки и `after_join`.

Два шва ставят канал и его группу обсуждения в очередь дочитывания без человека:
финал вступления в группу (`discussions._join_one`, автор `auto:join`) и финал
подключения канала вместе с группой (стадия `linked` вебхука `channel_add`,
автор `auto:channel_add`). Что здесь проверяется:

* **ставятся оба.** Партнёр ищется по `linked_chat_username`, а поле
  двунаправленное, поэтому в очередь встают и канал, и группа — из какого бы
  шва ни пришёл вызов;
* **выключенный сценарий не оставляет следов** — ни очереди, ни аудита, ни
  прогонов: выкатка не меняет поведение, включение — действие владельца;
* **окно — из настроек, не из констант**: глубина и потолок замораживаются в
  строке на момент постановки, правка строки `limits` действует сразу;
* **идемпотентно и без сюрпризов**: стоящий канал не дублируется, не вступавшая
  группа фильтруется превентивно (шов не роняет `NotJoined`), несуществующий
  канал — пустой итог.

Посев — канал `chan` и группа `chan_chat` со встречными `linked_chat_username`;
`peer_id` группы — дословно `-10077` из TESTS, чтобы `result` стадии `linked`
нашёл в `get_or_create_channel` посеянную группу, а не завёл новую.

Engage подменяется на уровне `engage.action` / `engage.wait_for_task` — нужная
часть `_stub_engage` из `tests/test_discussions_join_db.py` (чужой файл не
трогается). Инфраструктура — та же: skip-guard на `RADAR_TEST_DATABASE_URL`,
`_reset()` пересеивает схему, один `asyncio.run` на тест (движок кешируется
между вызовами — там же).

`run_id="0"` в T-04 — не опечатка из TESTS, а ветка кода: `int("0")` даёт 0,
`if run_id:` ложно, и `jobs.finish` не зовётся вовсе, поэтому заглушка ему не
нужна.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_INGEST_TOKEN", "test-ingest-token")

from app.api.v1 import ingest as ingest_api  # noqa: E402
from app.core import clock  # noqa: E402
from app.db.models import (AuditLog, BackfillItem, Base, Channel,  # noqa: E402
                           Limit, Run)
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.services import autoflow, discussions, engage  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

NOW = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)

# peer_id группы — дословно из TESTS (T-04): `result{"peer_id": -10077}` обязан
# найти в `get_or_create_channel` именно посеянную группу, а не завести новую.
GROUP_PEER = -100_077
CHAN_PEER = -100_076

# Посев блока Б по умолчанию: сценарий включён, прочие ключи — умолчания
# (`thresholds` пустую таблицу покрывает DEFAULTS, сеять их не нужно).
DEFAULT_LIMITS = {"autoflow_join_backfill_enabled": 1}


async def _reset(limits: dict[str, int]) -> None:
    engine = create_async_engine(DB_URL, poolclass=None)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        db.add(Channel(peer_id=CHAN_PEER, username="chan", title="Канал chan",
                       chat_type="channel", ingest_enabled=True,
                       linked_chat_username="chan_chat", linked_checked_at=NOW))
        db.add(Channel(peer_id=GROUP_PEER, username="chan_chat",
                       title="Чат chan_chat", chat_type="supergroup",
                       ingest_enabled=True, linked_chat_username="chan",
                       linked_checked_at=NOW))
        for key, value in limits.items():
            db.add(Limit(key=key, value=value))
        await db.commit()
    await engine.dispose()


@pytest.fixture()
def db_ready(request):
    limits = getattr(request, "param", None) or dict(DEFAULT_LIMITS)
    os.environ["RADAR_DATABASE_URL"] = DB_URL
    from app.core.config import get_settings

    get_settings.cache_clear()
    get_engine.cache_clear()
    get_session_maker.cache_clear()
    asyncio.run(_reset(limits))
    yield


def _stub_engage(monkeypatch, calls: list):
    """Engage, который умеет ровно `join_group` (нужная часть `_stub_engage`
    из `tests/test_discussions_join_db.py`). Любое другое действие — падение
    теста, а не молчаливый пропуск: «вступили и заодно дочитали» запрещено."""
    tasks: dict[str, str] = {}

    async def action(*, account_id, action, payload, webhook_url, **kw):
        assert action == "join_group", f"сценарий 1 вступает, а не {action}"
        target = payload["target"]
        calls.append((account_id, target))
        task_id = f"t{len(tasks) + 1}"
        tasks[task_id] = target
        return {"task_id": task_id}

    async def wait_for_task(task_id, **kw):
        return {"found": True, "chat_id": None, "target": tasks[task_id]}

    monkeypatch.setattr(engage, "action", action)
    monkeypatch.setattr(engage, "wait_for_task", wait_for_task)


async def _pairs(db) -> tuple[Channel, Channel]:
    """(канал, группа) посева — по username, в посеве они уникальны."""
    rows = (await db.execute(select(Channel))).scalars().all()
    by_name = {c.username: c for c in rows}
    return by_name["chan"], by_name["chan_chat"]


async def _snapshot(db) -> dict:
    """Всё, что проверяют тесты, одним чтением: очередь, аудит, прогоны.

    Плоские кортежи вместо ORM-объектов: сессия закрывается до проверок, и
    отсоединённый объект работал бы только при `expire_on_commit=False`.
    """
    items = [(i.channel_id, i.state, i.requested_by, i.target, i.min_date)
             for i in (await db.execute(select(BackfillItem))).scalars().all()]
    audits = [(a.action, a.user_email, a.user_id, a.detail)
              for a in (await db.execute(select(AuditLog))).scalars().all()]
    runs = (await db.execute(select(func.count(Run.id)))).scalar_one()
    return {"items": items, "audits": audits, "runs": runs}


def _queued(snap: dict) -> list[tuple]:
    return [it for it in snap["items"] if it[1] == "queued"]


def test_join_seam_enqueues_channel_and_group(db_ready, monkeypatch):
    """T-03: шов в `_join_one` ставит в очередь канал и его группу от `auto:join`
    с окном по умолчанию (2000 / 30 суток), пишет аудит и не заводит прогонов."""
    calls: list = []
    _stub_engage(monkeypatch, calls)

    async def go():
        before = clock.utcnow()
        async with get_session_maker()() as db:
            chan, group = await _pairs(db)
            out = await discussions._join_one(db, group.id, 1,
                                              subscribed_by="ivan@test")
            joined = (await db.get(Channel, group.id)).linked_joined_at
            snap = await _snapshot(db)
            return (out, joined is not None, snap,
                    chan.id, group.id, before, clock.utcnow())

    (out, joined, snap, chan_id, group_id,
     before, after) = asyncio.run(go())
    assert out == {"joined": True, "username": "chan_chat", "account_id": 1}, out
    assert joined, "у группы должна появиться отметка вступления"
    assert sorted(target for _, target in calls) == ["chan_chat"], calls
    queued = _queued(snap)
    assert {it[0] for it in queued} == {chan_id, group_id}, snap["items"]
    assert all(it[2] == "auto:join" for it in queued), queued
    assert all(it[3] == 2000 for it in queued), queued
    for it in queued:
        assert before - timedelta(days=30) <= it[4] <= after - timedelta(days=30), it
    assert len(snap["audits"]) == 1, snap["audits"]
    action, email, user_id, detail = snap["audits"][0]
    assert action == "backfill_enqueue", snap["audits"]
    assert email == "auto:join", snap["audits"]
    assert user_id is None, snap["audits"]
    assert set(detail["queued"]) == {chan_id, group_id}, detail
    assert detail["target"] == 2000 and detail["depth_days"] == 30, detail
    assert snap["runs"] == 0, "сценарий 1 прогонов не заводит"


def test_channel_add_finale_enqueues(db_ready):
    """T-04: финал `channel_add` (стадия `linked`) ставит канал и группу от
    `auto:channel_add` и пишет аудит. `run_id="0"` — `jobs.finish` не зовётся
    (`if run_id:` ложно), поэтому заглушка не нужна (см. докстринг модуля)."""

    async def go():
        async with get_session_maker()() as db:
            chan, _group = await _pairs(db)
            result = {"peer_id": GROUP_PEER, "username": "chan_chat",
                      "title": "Чат", "type": "supergroup"}
            q = {"account_id": "1", "run_id": "0", "subscribed_by": "owner@x",
                 "stage": "linked", "channel_id": str(chan.id)}
            out = await ingest_api._handle_chat_info_join(db, result, q)
            _chan, group = await _pairs(db)
            snap = await _snapshot(db)
            return {"out": out, "chan_id": chan.id, "group_id": group.id,
                    "group_joined": group.linked_joined_at is not None,
                    "group_subscribed_by": group.subscribed_by,
                    "group_account": group.subscribed_account_id,
                    "group_linked_username": group.linked_chat_username,
                    "snap": snap}

    out = asyncio.run(go())
    assert out["out"] == {"accepted": 1, "channel_id": out["chan_id"],
                          "linked_chat_peer_id": GROUP_PEER}, out["out"]
    # Внешние поля группы заполнены обработчиком стадии linked.
    assert out["group_joined"] is True
    assert out["group_subscribed_by"] == "owner@x"
    assert out["group_account"] == 1
    assert out["group_linked_username"] == "chan"
    queued = _queued(out["snap"])
    assert ({it[0] for it in queued}
            == {out["chan_id"], out["group_id"]}), out["snap"]["items"]
    assert all(it[2] == "auto:channel_add" for it in queued), queued
    assert len(out["snap"]["audits"]) == 1, out["snap"]["audits"]
    action, email, _user_id, _detail = out["snap"]["audits"][0]
    assert action == "backfill_enqueue"
    assert email == "auto:channel_add"


@pytest.mark.parametrize("db_ready", [{"autoflow_join_backfill_enabled": 0}],
                         indirect=True)
def test_after_join_disabled_does_nothing(db_ready, monkeypatch):
    """T-05: выключенный сценарий не оставляет следов — ни очереди, ни аудита,
    ни прогонов, и после `_join_one`, и при прямом вызове `after_join`."""
    calls: list = []
    _stub_engage(monkeypatch, calls)

    async def go():
        async with get_session_maker()() as db:
            _chan, group = await _pairs(db)
            out = await discussions._join_one(db, group.id, 1,
                                              subscribed_by="ivan@test")
            snap_after_join = await _snapshot(db)
            res = await autoflow.after_join(db, channel_id=group.id,
                                            source="auto:join")
            return out, snap_after_join, res, await _snapshot(db)

    out, snap_after_join, res, snap_end = asyncio.run(go())
    assert out["joined"] is True, out
    assert snap_after_join["items"] == [] and snap_after_join["audits"] == []
    assert snap_after_join["runs"] == 0, snap_after_join
    assert res == {"enabled": False, "queued": [], "skipped": []}, res
    assert snap_end == snap_after_join, "выключенный сценарий ничего не меняет"


@pytest.mark.parametrize(
    "db_ready",
    [{"autoflow_join_backfill_enabled": 1, "autoflow_backfill_target": 500,
      "autoflow_backfill_depth_days": 7}],
    indirect=True)
def test_after_join_window_from_settings(db_ready, monkeypatch):
    """T-06: окно постановки берётся из строк `limits`, а не из констант
    `DEFAULT_TARGET/DEFAULT_DEPTH`; граница замораживается на постановке."""
    calls: list = []
    _stub_engage(monkeypatch, calls)

    async def go():
        before = clock.utcnow()
        async with get_session_maker()() as db:
            chan, group = await _pairs(db)
            await discussions._join_one(db, group.id, 1, subscribed_by="ivan@test")
            snap = await _snapshot(db)
            return snap, chan.id, group.id, before, clock.utcnow()

    snap, chan_id, group_id, before, after = asyncio.run(go())
    queued = _queued(snap)
    assert {it[0] for it in queued} == {chan_id, group_id}, snap["items"]
    assert all(it[3] == 500 for it in queued), queued
    for it in queued:
        assert before - timedelta(days=7) <= it[4] <= after - timedelta(days=7), it


def test_after_join_idempotent_and_skips_unjoined(db_ready):
    """T-07: стоящий канал не дублируется; группа без вступления фильтруется
    превентивно (`NotJoined` не выйдет из `after_join`); несуществующий канал —
    пустой итог, не исключение."""

    async def go():
        out: dict = {}
        async with get_session_maker()() as db:
            chan, group = await _pairs(db)
            out["chan_id"], out["group_id"] = chan.id, group.id

            # Вариант 1: канал уже стоит — повторная постановка молча пропускается.
            db.add(BackfillItem(state="queued", channel_id=chan.id, position=1))
            await db.commit()
            out["res1"] = await autoflow.after_join(db, channel_id=chan.id,
                                                    source="auto:join")
            out["snap1"] = await _snapshot(db)

            # Вариант 2: очередь чиста, группа НЕ вступала — ставится только канал.
            await db.execute(text("DELETE FROM backfill_queue"))
            await db.commit()
            out["res2"] = await autoflow.after_join(db, channel_id=chan.id,
                                                    source="auto:join")
            out["snap2"] = await _snapshot(db)

            # Вариант 3: несуществующий канал — пустой итог, не исключение.
            out["res3"] = await autoflow.after_join(db, channel_id=10 ** 9,
                                                    source="auto:join")
            out["snap3"] = await _snapshot(db)
        return out

    out = asyncio.run(go())
    chan_id, group_id = out["chan_id"], out["group_id"]

    queued1 = _queued(out["snap1"])
    assert [it[0] for it in queued1] == [chan_id], out["snap1"]["items"]
    assert out["res1"] == {"enabled": True, "queued": [], "skipped": [group_id]}
    assert out["snap1"]["audits"] == [], "ничего не поставлено — аудита нет"

    queued2 = _queued(out["snap2"])
    assert [(it[0], it[1], it[2]) for it in queued2] == \
        [(chan_id, "queued", "auto:join")], out["snap2"]["items"]
    assert out["res2"] == {"enabled": True, "queued": [chan_id],
                           "skipped": [group_id]}, out["res2"]
    assert len(out["snap2"]["audits"]) == 1, out["snap2"]["audits"]

    assert out["res3"] == {"enabled": True, "queued": [], "skipped": []}, out["res3"]
    assert out["snap3"]["items"] == out["snap2"]["items"], out["snap3"]
    assert out["snap3"]["runs"] == 0, out["snap3"]
