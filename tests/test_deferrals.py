"""Единая трактовка откладывания (R3): модуль `deferrals` и пять точек на нём.

Проверяется то, что контракт называет «одно место решения»: таблица `interpret`
(все четыре строки), новые статусы строк `runs` (`deferred` — терминальный без
тревоги, `waiting` — только внешние цепочки), приём события `task_deferred`
(E4) и то, что отложенный канал/группа больше не помечаются проваленными.

Сеть и база подменяются целиком: Engage — заглушками на уровне `engage.*` или
уже готовых функций службы, база — словарём-обманком. Без Postgres эти тесты
выполняются, а не пропускаются.
"""
from __future__ import annotations

import os
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_INGEST_TOKEN", "test-ingest-token")

from app.api.v1.ingest import _ENGAGE_REASON_TEXT, process_event  # noqa: E402
from app.core import clock  # noqa: E402
from app.db.models import BackfillItem, ChannelCandidate  # noqa: E402
from app.services import deferrals, discussions, discovery, engage, jobs  # noqa: E402

# Тело события `task_deferred` — дословно по форме E4
# (`_REF-engage-webhook_events.py`, копия `webhook_events.py` Engage).
BODY = {"event": "task_deferred", "task_id": "t-77", "account_id": 3,
        "error_code": "READ_BUDGET_EXCEEDED",
        "deferred_until": "2026-09-11T13:00:00Z"}


# ── заглушки без базы и сети ──────────────────────────────────────────────────

class _Rows:
    """Результат запроса: хватает на all()/scalars()/scalar_one*()."""

    def __init__(self, rows):
        self._rows = list(rows)

    def all(self):
        return list(self._rows)

    def scalars(self):
        return self

    def scalar_one(self):
        return self._rows[0]

    def scalar_one_or_none(self):
        return self._rows[0] if self._rows else None


class _DB:
    """Сессия-минимум: коммит считается, запросы отвечают пусто."""

    def __init__(self):
        self.commits = 0

    async def execute(self, stmt):
        return _Rows([])

    def add(self, obj):
        pass

    async def commit(self):
        self.commits += 1


class _Maker:
    """`get_session_maker`-обманка: каждый вход в контекст отдаёт тот же db."""

    def __init__(self, db):
        self._db = db

    def __call__(self):
        return self

    async def __aenter__(self):
        return self._db

    async def __aexit__(self, *exc):
        return False


def _report_sink():
    notes = []

    async def report(pct, note):
        notes.append(note)

    return report, notes


def _candidate(**over) -> ChannelCandidate:
    base = {"username": "bank", "title": "Банк", "source": "similar",
            "found_by_account_id": 3, "decision": "pending"}
    base.update(over)
    return ChannelCandidate(**base)


def _patch_jobs_recorders(monkeypatch):
    """Записывающие двойники `_touch`/`progress`/`finish` модуля jobs."""
    touched, notes, finished = [], [], []

    async def touch(run_id, **fields):
        touched.append((run_id, fields))

    async def progress(run_id, pct, note):
        notes.append((run_id, pct, note))

    async def finish(run_id, **kw):
        finished.append((run_id, kw))

    monkeypatch.setattr(jobs, "_touch", touch)
    monkeypatch.setattr(jobs, "progress", progress)
    monkeypatch.setattr(jobs, "finish", finish)
    return touched, notes, finished


# ── 1. таблица interpret — все четыре строки (R3 §5.1) ────────────────────────

@pytest.mark.parametrize("exc,kind,code", [
    (engage.EngageTaskDeferred("Engage отложил задачу: READ_BUDGET_EXCEEDED",
                               code="READ_BUDGET_EXCEEDED"),
     "deferred", "READ_BUDGET_EXCEEDED"),
    (engage.EngageTaskDeferred("Engage отложил задачу: BUDGET_PER_ACCOUNT",
                               code="BUDGET_PER_ACCOUNT"),
     "deferred", "BUDGET_PER_ACCOUNT"),
    (engage.EngageTaskDeferred("Engage отложил задачу: BUDGET_AGGREGATE",
                               code="BUDGET_AGGREGATE"),
     "deferred", "BUDGET_AGGREGATE"),
    (engage.EngageUnavailable("Engage недоступен: ConnectError"),
     "unavailable", None),
    (engage.EngageTaskFailed("задача не выполнена: channel_private",
                             code="channel_private"),
     "failed", "channel_private"),
    (ValueError("бум"), "failed", "ValueError"),
])
def test_interpret_table(exc, kind, code):
    interp = deferrals.interpret(exc)
    assert interp.kind == kind
    assert interp.code == code
    assert interp.note, "нота обязана быть — она едет в лог прогона"


def test_interpret_deferred_note_names_the_budget():
    """Нота deferred читается как «лимит кончился, вернётся сам» — это слова,
    закреплённые за откладыванием контрактом (R3 §2)."""
    interp = deferrals.interpret(
        engage.EngageTaskDeferred("отложено", code="BUDGET_PER_ACCOUNT"))
    assert "кончился дневной лимит" in interp.note
    assert "вернётся сам" in interp.note


# ── 2. discussions.scan: deferred не «failed канала» (R3 §5.2) ────────────────

async def test_scan_deferred_channel_is_not_failed(monkeypatch):
    async def deferred_scan_one(db, channel_id, account_id, **kw):
        raise engage.EngageTaskDeferred(
            "Engage отложил задачу: READ_BUDGET_EXCEEDED",
            code="READ_BUDGET_EXCEEDED")

    monkeypatch.setattr(discussions, "_scan_one", deferred_scan_one)
    monkeypatch.setattr(discussions, "get_session_maker", _Maker(_DB()))
    report, notes = _report_sink()

    stats = await discussions.scan(channel_ids=[1, 2], account_ids=[1], target=10,
                                   report=report, cancelled=lambda: False)

    assert stats["deferred"] == 2
    assert stats["failed"] == 0
    assert stats["done"] == 2
    assert stats["cancelled"] is False  # прогон завершён, не упал
    assert any("кончился дневной лимит" in n for n in notes)


async def test_scan_unavailable_channel_is_not_failed_either(monkeypatch):
    """`unavailable` — не вина канала, последствия те же, что у deferred
    (семантика `deferrals`): счёт тот же, поля не тронуты, «упавшим» канал не
    помечается."""
    async def unavailable_scan_one(db, channel_id, account_id, **kw):
        raise engage.EngageUnavailable("Engage недоступен: ConnectError")

    monkeypatch.setattr(discussions, "_scan_one", unavailable_scan_one)
    monkeypatch.setattr(discussions, "get_session_maker", _Maker(_DB()))
    report, notes = _report_sink()

    stats = await discussions.scan(channel_ids=[1], account_ids=[1], target=10,
                                   report=report, cancelled=lambda: False)

    assert stats["deferred"] == 1 and stats["failed"] == 0
    assert any("недоступен" in n for n in notes)


async def test_scan_real_failure_is_still_failed(monkeypatch):
    """Регресс: настоящий отказ (EngageTaskFailed) в «failed» остаётся."""
    async def failed_scan_one(db, channel_id, account_id, **kw):
        raise engage.EngageTaskFailed("нельзя", code="channels_too_much")

    monkeypatch.setattr(discussions, "_scan_one", failed_scan_one)
    monkeypatch.setattr(discussions, "get_session_maker", _Maker(_DB()))
    report, _ = _report_sink()

    stats = await discussions.scan(channel_ids=[1], account_ids=[1], target=10,
                                   report=report, cancelled=lambda: False)
    assert stats["failed"] == 1 and stats["deferred"] == 0


# ── 3. join worker: поведение сохранено (R3 §2, точка 1) ──────────────────────

async def test_join_worker_counts_deferred_and_keeps_the_note(monkeypatch):
    async def fake_limits(*, account_ids=None, instance=None):
        return {"accounts": [
            {"account_id": a, "api_credential_id": 1, "use_case": "cold_dm",
             "actions": [{"action": "joins_per_day",
                          "per_account": {"remaining": 5},
                          "aggregate": {"remaining": 99}}]}
            for a in (account_ids or [])]}

    async def deferred_join_one(db, group_id, account_id, *, subscribed_by):
        raise engage.EngageTaskDeferred("Engage отложил задачу: BUDGET_PER_ACCOUNT",
                                        code="BUDGET_PER_ACCOUNT")

    monkeypatch.setattr(engage, "limits", fake_limits)
    monkeypatch.setattr(discussions, "_join_one", deferred_join_one)
    monkeypatch.setattr(discussions, "get_session_maker", _Maker(_DB()))
    report, notes = _report_sink()

    stats = await discussions.join_groups(group_ids=[11, 12], account_ids=[1],
                                          per_account=None, subscribed_by="t",
                                          report=report, cancelled=lambda: False)

    assert stats["deferred"] == 2
    assert stats["failed"] == 0 and stats["joined"] == 0
    assert any("кончился дневной лимит" in n for n in notes)


# ── 4. run_scan deferred + execute ставит «deferred» без тревоги (R3 §5.3) ────

def _stub_scan_engage(monkeypatch, *, deferred_error=None,
                      limits_result=None, limits_error=None):
    calls = []

    async def action(*, account_id, action, payload, webhook_url, **kw):
        calls.append((action, payload))
        return {"task_id": "t1"}

    async def wait_for_task(task_id, **kw):
        if deferred_error is not None:
            raise deferred_error
        return {"channels": [{"title": "Банк", "username": "bank",
                              "members_count": 5000}]}

    async def limits(*, account_ids=None, instance=None):
        calls.append(("limits", list(account_ids or [])))
        if limits_error is not None:
            raise limits_error
        return limits_result

    monkeypatch.setattr(engage, "action", action)
    monkeypatch.setattr(engage, "wait_for_task", wait_for_task)
    monkeypatch.setattr(engage, "limits", limits)
    return calls


_PARAMS = {"kind": "search", "query": "бухгалтерия", "account_id": 3}


async def test_run_scan_deferred_completes_with_deferred_result(monkeypatch):
    calls = _stub_scan_engage(
        monkeypatch,
        deferred_error=engage.EngageTaskDeferred(
            "Engage отложил задачу: READ_BUDGET_EXCEEDED",
            code="READ_BUDGET_EXCEEDED"),
        limits_error=engage.EngageUnavailable("маршрута нет"))
    monkeypatch.setattr(discovery, "get_session_maker", _Maker(_DB()))
    report, notes = _report_sink()

    out = await discovery.run_scan(7, params=dict(_PARAMS), report=report,
                                   cancelled=lambda: False)

    assert out["deferred"] is True
    assert out["code"] == "READ_BUDGET_EXCEEDED"
    # Engage был недоступен для опроса остатка — окна ожидания нет (R4).
    assert "retry_after_s" not in out
    assert any("кончился дневной лимит" in n for n in notes)
    # Остаток запрошен ровно один раз и у того аккаунта, которым искали (R4:
    # один опрос на отложенный прогон, не на тик).
    assert [c for c in calls if c[0] == "limits"] == [("limits", [3])]


async def test_execute_closes_deferred_run_without_alarm(monkeypatch):
    touches, logs = [], []

    async def touch(run_id, **fields):
        touches.append((run_id, fields))

    async def append_log(run_id, line):
        logs.append(line)

    async def fake_runner(run_id, params):
        return {"deferred": True, "code": "BUDGET_AGGREGATE", "retry_after_s": 1800}

    alert_emit = AsyncMock()
    monkeypatch.setattr(jobs, "_touch", touch)
    monkeypatch.setattr(jobs, "_append_log", append_log)
    monkeypatch.setattr(jobs, "RUNNERS", {"discovery_scan": fake_runner})
    monkeypatch.setattr(jobs, "alerts", MagicMock(emit=alert_emit))

    await jobs.execute(5, "discovery_scan", {})

    # Первый _touch — «выполняется», финальный — терминальный статус.
    [run_id, fields] = touches[-1]
    assert run_id == 5
    assert fields["status"] == "deferred"
    assert fields["result"]["deferred"] is True
    assert fields["finished_at"] is not None
    assert any("отложено лимитом" in line for line in logs)
    # Тревог `run_failed:*` нет: откладывание — штатный исход, а не падение.
    alert_emit.assert_not_awaited()


async def test_execute_deferred_count_in_stats_is_not_a_deferred_run(monkeypatch):
    """«deferred» в статистике scan/join/check — счётчик отложенных каналов,
    а не маркер исхода прогона: прогон с одним отложенным каналом из шестидесяти
    завершён штатно и обязан остаться «готово»."""
    touches, logs = [], []

    async def touch(run_id, **fields):
        touches.append((run_id, fields))

    async def append_log(run_id, line):
        logs.append(line)

    async def scan_runner(run_id, params):
        return {"total": 3, "done": 3, "deferred": 1, "failed": 0}

    alert_emit = AsyncMock()
    monkeypatch.setattr(jobs, "_touch", touch)
    monkeypatch.setattr(jobs, "_append_log", append_log)
    monkeypatch.setattr(jobs, "RUNNERS", {"discussions": scan_runner})
    monkeypatch.setattr(jobs, "alerts", MagicMock(emit=alert_emit))

    await jobs.execute(8, "discussions", {})

    assert touches[-1][1]["status"] == "done"
    assert not any("отложено лимитом" in line for line in logs)


async def test_execute_failure_still_raises_the_alarm(monkeypatch):
    """Регресс: тревога живёт только в ветке исключения — падение обязано
    остаться в ленте, deferred лишь добавил соседний исход."""
    touches, logs = [], []

    async def touch(run_id, **fields):
        touches.append((run_id, fields))

    async def append_log(run_id, line):
        logs.append(line)

    async def broken_runner(run_id, params):
        raise RuntimeError("упало")

    alert_emit = AsyncMock()
    monkeypatch.setattr(jobs, "_touch", touch)
    monkeypatch.setattr(jobs, "_append_log", append_log)
    monkeypatch.setattr(jobs, "RUNNERS", {"discovery_scan": broken_runner})
    monkeypatch.setattr(jobs, "alerts", MagicMock(emit=alert_emit))

    await jobs.execute(6, "discovery_scan", {})

    assert touches[-1][1]["status"] == "failed"
    alert_emit.assert_awaited_once()
    assert alert_emit.await_args.kwargs["key"] == "run_failed:discovery_scan"


# ── 5. приём task_deferred: waiting, идемпотентность, продление (R3 §5.4) ─────

class _ItemDB:
    """Сессия с одним элементом очереди для `db.get(BackfillItem, ...)`."""

    def __init__(self, item=None):
        self.item = item
        self.commits = 0

    async def get(self, model, pk):
        if model is BackfillItem and self.item is not None and self.item.id == pk:
            return self.item
        return None

    async def commit(self):
        self.commits += 1


def _item(**over) -> BackfillItem:
    base = {"id": 5, "channel_id": 1, "position": 1, "state": "running",
            "attempts": 1,
            # Полчаса назад: молчание ещё меньше STALE-часа, но долго — разница
            # с продлённым `started_at` видна заведомо.
            "started_at": clock.utcnow() - timedelta(minutes=30)}
    base.update(over)
    return BackfillItem(**base)


async def test_task_deferred_puts_the_run_in_waiting(monkeypatch):
    touched, notes, _finished = _patch_jobs_recorders(monkeypatch)

    out = await process_event(_ItemDB(), BODY, {"kind": "join", "run_id": "7"})

    assert out == {"accepted": 0, "deferred": "READ_BUDGET_EXCEEDED"}
    [entry] = [t for t in touched if t[0] == 7]
    assert entry[1]["status"] == "waiting"
    assert entry[1]["error"] is None
    [note] = [n for n in notes if n[0] == 7]
    assert note[1] is None  # процент не трогается
    assert "ждёт лимита" in note[2]
    assert "суточный бюджет Engage исчерпан" in note[2]


async def test_task_deferred_replay_changes_nothing(monkeypatch):
    """Повторная доставка того же вебхука (at-least-once) разбирается
    идемпотентно: статус и нота перезаписываются теми же значениями, второй
    записи о «новом» событии нет."""
    touched, notes, _finished = _patch_jobs_recorders(monkeypatch)

    await process_event(_ItemDB(), BODY, {"kind": "join", "run_id": "7"})
    await process_event(_ItemDB(), BODY, {"kind": "join", "run_id": "7"})

    waiting = [t for t in touched if t[1].get("status") == "waiting"]
    assert len(waiting) == 2
    assert waiting[0] == waiting[1]
    assert notes[0] == notes[1]


async def test_task_deferred_extends_the_history_item(monkeypatch):
    """history-страница отложена — элемент очереди продлевается (R4): таймер
    STALE сброшен, состояние `running`, попытка не растёт."""
    _patch_jobs_recorders(monkeypatch)
    item = _item()
    db = _ItemDB(item)

    out = await process_event(db, BODY, {"kind": "history", "item_id": "5"})

    assert out == {"accepted": 0, "deferred": "READ_BUDGET_EXCEEDED"}
    assert item.state == "running"
    assert item.attempts == 1
    assert item.started_at is not None
    assert item.started_at > clock.utcnow() - timedelta(minutes=30)
    assert db.commits == 1
    # STALE-предикат тика (`backfill_drain`) элемент больше не возвращает:
    # «молчание дольше часа» с новым `started_at` не выполнено.
    from app.services.backfill_drain import STALE_AFTER
    assert not (item.started_at <= clock.utcnow() - STALE_AFTER)


async def test_task_deferred_leaves_closed_item_alone(monkeypatch):
    """Элемент, которого нет или который уже закрыт, продлением не трогается."""
    _patch_jobs_recorders(monkeypatch)

    done = _item(state="done", started_at=None)
    db = _ItemDB(done)
    await process_event(db, BODY, {"kind": "history", "item_id": "5"})
    assert done.state == "done" and db.commits == 0

    db = _ItemDB(None)
    out = await process_event(db, BODY, {"kind": "history", "item_id": "404"})
    assert out == {"accepted": 0, "deferred": "READ_BUDGET_EXCEEDED"}
    assert db.commits == 0


async def test_task_failed_branch_works_as_before(monkeypatch):
    """Регресс: ветка `task_failed` (`process_event`) не пережила правку —
    отказ по-прежнему закрывает прогон «упала» с переведённой причиной."""
    _touched, _notes, finished = _patch_jobs_recorders(monkeypatch)

    body = {"event": "task_failed", "task_id": "t-1", "account_id": 3,
            "error_code": "channel_private", "retry_count": 1}
    out = await process_event(_ItemDB(), body, {"kind": "history", "run_id": "9"})

    assert out == {"accepted": 0, "error": "channel_private"}
    [entry] = finished
    assert entry[0] == 9
    assert entry[1]["status"] == "failed"
    assert "приватный" in entry[1]["error"]


def test_budget_deferral_codes_are_translated():
    """Тексты трёх кодов E4 — в переводчике причин, для оператора, а не кодом."""
    for code in ("READ_BUDGET_EXCEEDED", "BUDGET_PER_ACCOUNT", "BUDGET_AGGREGATE"):
        text = _ENGAGE_REASON_TEXT.get(code.strip().lower())
        assert text and "отложена" in text, code


# ── 6. check_card через interpret: регресс (R3 §5.5) ──────────────────────────

async def test_check_card_deferred_stays_pending(monkeypatch):
    async def deferred_info(*a, **kw):
        raise engage.EngageTaskDeferred(
            "Engage отложил задачу: READ_BUDGET_EXCEEDED",
            code="READ_BUDGET_EXCEEDED")

    monkeypatch.setattr(discovery, "_fetch_info", deferred_info)
    db, cand = _DB(), _candidate()

    assert await discovery.check_card(db, cand, account_id=3) == "defer"
    assert cand.decision == "pending"
    assert cand.decision_reason is None
    assert db.commits == 0


async def test_check_card_failed_is_rejected_with_translated_reason(monkeypatch):
    async def failed_info(*a, **kw):
        raise engage.EngageTaskFailed("нельзя", code="channel_private")

    monkeypatch.setattr(discovery, "_fetch_info", failed_info)
    db, cand = _DB(), _candidate()

    assert await discovery.check_card(db, cand, account_id=3) == "reject"
    assert cand.decision == "rejected"
    assert "приватный" in cand.decision_reason
    assert db.commits == 1
