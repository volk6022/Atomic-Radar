"""Прекращение шторма повторов (R4): тик уважает `retry_after_s`, элемент очереди
продлевается отложенной страницей.

Проверяется три механизма одной идеи «всё событие, без будильников»:

* `run_check`, отложенный лимитом Engage, сам называет окно: маркер
  `deferred: True` в его результате ставит прогону статус `deferred`
  (`jobs.execute`), а `retry_after_s` — один опрос `limits()` по аккаунтам
  отложенных проверок;
* `discovery_check_tick` не заводит новый прогон, пока окно последнего
  deferred-прогона не истекло;
* вебхук `task_deferred` для history-цепочки продлевает элемент очереди, и
  STALE-предикат тика дочитывания его больше не отбирает.

Строку `runs` для тиковых тестов собирает только продакшн-путь: настоящий
`run_check` с Engage, который откладывает карточку, затем настоящий
`jobs.execute` закрывает прогон, — перехвачен лишь `_touch`, чтобы прочитать,
что он записал бы в таблицу. Никакой ручной сборки `status="deferred"`: именно
такую строку прод-код прежде не мог создать, и на этом держался БЛОКЕР-1
(разбор wave3b §5).

Сеть и база подменяются целиком — образец tests/test_deferrals.py и
tests/test_discovery_service.py. Без Postgres тесты выполняются, а не
пропускаются.
"""
from __future__ import annotations

import os
from datetime import timedelta

import pytest

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_INGEST_TOKEN", "test-ingest-token")

from app.api.v1.ingest import process_event  # noqa: E402
from app.core import clock  # noqa: E402
from app.db.models import BackfillItem, ChannelCandidate, Run  # noqa: E402
from app.services import discovery, engage, jobs  # noqa: E402

# Тело события `task_deferred` — дословно по форме E4
# (`_REF-engage-webhook_events.py`).
BODY = {"event": "task_deferred", "task_id": "t-9", "account_id": 3,
        "error_code": "READ_BUDGET_EXCEEDED",
        "deferred_until": "2026-09-11T13:00:00Z"}

_PARAMS = {"kind": "search", "query": "бухгалтерия", "account_id": 3}


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


_CANDIDATE_ID = 11


def _candidate() -> ChannelCandidate:
    """Единственный pending-кандидат прогона: аккаунт 3 — он же во флоте."""
    return ChannelCandidate(id=_CANDIDATE_ID, username="bank", title="Банк",
                            source="similar", found_by_account_id=3,
                            decision="pending")


class _DB:
    """Сессия-обманка: ответ выбирается по метке в тексте запроса.

    В `str(stmt)` нет значений-параметров (только плейсхолдеры), поэтому
    различители — имена столбцов и функций, а не значения: последний
    завершённый run (`FROM runs`), тиковый счётчик pending (`count(` — именно
    со скобкой: подстрока «count» без неё была бы и в `found_by_account_id`),
    запросы с `username` в списке/условии (список approved прогона и поиск
    по username в `run_scan`) — пустой ответ, список pending прогона
    (`decision` без username) — один кандидат. Пороги (`FROM limits`) не
    ловится ни одной веткой → умолчания из кода. `get` отдаёт строку
    кандидата #11 — её проверяет `run_check`.
    """

    def __init__(self, last_run=None):
        self.last_run = last_run

    async def execute(self, stmt):
        sql = str(stmt)
        if "FROM runs" in sql:
            return _Rows([self.last_run] if self.last_run is not None else [])
        if "channel_candidates" not in sql:
            return _Rows([])
        if "count(" in sql.lower():
            return _Rows([1])
        if "username" in sql:
            return _Rows([])               # approved и поиск по username — пусто
        if "decision" in sql:
            return _Rows([_CANDIDATE_ID])  # список pending прогона
        return _Rows([])

    async def get(self, model, pk):
        if model is ChannelCandidate and pk == _CANDIDATE_ID:
            return _candidate()
        return None

    def add(self, obj):
        pass

    async def commit(self):
        pass


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


def _limits_stub(monkeypatch, *, resets=7200, error=None,
                 actions=("search_public_chats",)):
    """Заглушка `engage.limits`: остаток с окном возврата `resets` у заданных
    действий каждого запрошенного аккаунта."""
    calls = []

    async def limits(*, account_ids=None, instance=None):
        calls.append(list(account_ids or []))
        if error is not None:
            raise error
        return {"accounts": [
            {"account_id": a, "actions": [
                {"action": name,
                 "per_account": {"remaining": 49, "resets_in_seconds": resets},
                 "aggregate": {"remaining": 499, "resets_in_seconds": resets}}
                for name in actions]}
            for a in (account_ids or [])]}

    monkeypatch.setattr(engage, "limits", limits)
    return calls


def _check_engage_stub(monkeypatch, *, fail=None):
    """Проверки кандидатов без сети: флот из одного активного аккаунта,
    карточка отвечает заготовкой; `fail` — {действие: исключение} — бросается
    из `wait_for_task` (`get_chat_info`, `get_chat_history`)."""
    fail = fail or {}
    tasks: dict[str, str] = {}

    async def list_accounts():
        return [{"account_id": 3, "status": "active"}]

    async def action(*, account_id, action, payload, webhook_url, **kw):
        task_id = f"t{len(tasks) + 1}"
        tasks[task_id] = action
        return {"task_id": task_id}

    async def wait_for_task(task_id, **kw):
        act = tasks[task_id]
        if act in fail:
            raise fail[act]
        if act == "get_chat_info":
            return {"found": True, "peer_id": -1001234, "title": "Банк",
                    "type": "channel", "linked_chat_username": "bank_chat",
                    "members_count": 5000}
        return {"posts": [{"message_id": i, "text": "текст"} for i in range(3)]}

    monkeypatch.setattr(engage, "list_accounts", list_accounts)
    monkeypatch.setattr(engage, "action", action)
    monkeypatch.setattr(engage, "wait_for_task", wait_for_task)


_DEFERRED = engage.EngageTaskDeferred(
    "Engage отложил задачу: READ_BUDGET_EXCEEDED", code="READ_BUDGET_EXCEEDED")


async def _closed_check_run(monkeypatch, *, fail=None, limits_error=None) -> Run:
    """Строка `runs` — только продакшн-путь: настоящий `run_check` (Engage
    из `_check_engage_stub`), затем настоящий `jobs.execute` закрывает прогон.
    Перехвачен `_touch`: строка собирается из того, что он записал бы в
    таблицу, — статус и результат делает прод-код, не тест."""
    _check_engage_stub(monkeypatch, fail=fail)
    _limits_stub(monkeypatch, resets=7200, error=limits_error,
                 actions=("get_chat_info", "get_chat_history"))
    touches: list[dict] = []

    async def touch(run_id, **fields):
        touches.append(dict(fields))

    async def append_log(run_id, line):
        pass

    monkeypatch.setattr(jobs, "_touch", touch)
    monkeypatch.setattr(jobs, "_append_log", append_log)
    monkeypatch.setattr(discovery, "get_session_maker", _Maker(_DB()))

    await jobs.execute(501, "discovery_check", {})
    return Run(kind="discovery_check", name="Проверка кандидатов Discovery",
               **touches[-1])


def _scan_engage_stub(monkeypatch, *, deferred_error):
    """Поиск из двух шагов, где `wait_for_task` бросает заданное исключение."""

    async def action(*, account_id, action, payload, webhook_url, **kw):
        return {"task_id": "t1"}

    async def wait_for_task(task_id, **kw):
        raise deferred_error

    monkeypatch.setattr(engage, "action", action)
    monkeypatch.setattr(engage, "wait_for_task", wait_for_task)


# ── 1–2. тик уважает окно последнего deferred-прогона (R4 §5.1–5.2) ───────────
# Строка `runs` в каждом тиковом тесте — продакшн-путь: `run_check` откладывает
# карточку лимитом, `jobs.execute` закрывает прогон, тик читает то, что вышло.

async def _tick(monkeypatch, last_run):
    starts = []
    monkeypatch.setattr(discovery, "get_session_maker", _Maker(_DB(last_run)))

    async def active_run(db, kind):
        return None

    async def start(db, *, kind, params, name, user_email):
        starts.append(kind)

    monkeypatch.setattr(jobs, "active_run", active_run)
    monkeypatch.setattr(jobs, "start", start)
    return await discovery.discovery_check_tick({}), starts


async def test_fresh_deferred_window_holds_the_tick_back(monkeypatch):
    """Отложенная карточка → прогон закрыт «deferred» с окном → тик молчит:
    прогон не заводится, тик вернул причину."""
    row = await _closed_check_run(monkeypatch, fail={"get_chat_info": _DEFERRED})

    assert row.status == "deferred"
    assert row.result["deferred"] is True
    assert row.result["retry_after_s"] == 7200

    out, starts = await _tick(monkeypatch, row)
    assert starts == []
    assert out["started"] is False
    assert out["waiting_budget"] is True
    assert out["pending"] == 1


async def test_expired_window_releases_the_tick(monkeypatch):
    """Окно 7200 с у свежего прогона, часы переведены на 2 часа вперёд ⇒
    тик заводит прогон как обычно."""
    row = await _closed_check_run(monkeypatch, fail={"get_chat_info": _DEFERRED})

    later = clock.utcnow() + timedelta(hours=2)
    monkeypatch.setattr(clock, "utcnow", lambda: later)

    out, starts = await _tick(monkeypatch, row)
    assert starts == ["discovery_check"]
    assert out["started"] is True
    assert out["waiting_budget"] is False


async def test_deferred_without_window_keeps_the_five_minute_rhythm(monkeypatch):
    """Окна нет (опрос остатка не удался) ⇒ прежний ритм: молча ждать
    «до никогда» нельзя (R4 §2). Статус прогона при этом всё равно «deferred»."""
    row = await _closed_check_run(
        monkeypatch, fail={"get_chat_info": _DEFERRED},
        limits_error=engage.EngageUnavailable("маршрута нет"))

    assert row.status == "deferred"
    assert "retry_after_s" not in (row.result or {})

    out, starts = await _tick(monkeypatch, row)
    assert starts == ["discovery_check"]
    assert out["waiting_budget"] is False


async def test_check_run_without_deferral_is_done_and_the_tick_starts(monkeypatch):
    """Обратное: проверки прошли без откладывания лимитом → `execute` закрывает
    прогон «готово», окна и маркера в результате нет — тик заводит следующий
    прогон обычным ритмом. Кандидат при этом реально проверен (карточка и
    живость отвечали), а не отклонён по отказу Engage."""
    row = await _closed_check_run(monkeypatch)

    assert row.status == "done"
    assert row.result["checked"] == 1
    assert row.result["deferred_candidates"] == 0
    assert row.result.get("deferred") is not True
    assert "retry_after_s" not in (row.result or {})

    out, starts = await _tick(monkeypatch, row)
    assert starts == ["discovery_check"]
    assert out["waiting_budget"] is False


async def test_no_finished_runs_at_all_starts_the_run(monkeypatch):
    out, starts = await _tick(monkeypatch, None)

    assert starts == ["discovery_check"]
    assert out["waiting_budget"] is False


# ── 2б. сам писатель окна: run_check при откладывании лимитом (R4-fix) ────────

async def test_run_check_marks_budget_deferral_and_names_the_window(monkeypatch):
    """Отложенная карточка делает исход прогона «отложено»: маркер для
    `execute`, счётчик кандидатов — под своим именем `deferred_candidates`,
    окно — ровно один опрос `limits()` тем аккаунтом, которым проверяли."""
    calls = _limits_stub(monkeypatch, resets=7200, actions=("get_chat_info",))
    _check_engage_stub(monkeypatch, fail={"get_chat_info": _DEFERRED})
    monkeypatch.setattr(discovery, "get_session_maker", _Maker(_DB()))
    report, _notes = _report_sink()

    stats = await discovery.run_check(report=report, cancelled=lambda: False)

    assert stats["deferred"] is True
    assert stats["deferred_candidates"] == 1
    assert stats["code"] == "READ_BUDGET_EXCEEDED"
    assert stats["retry_after_s"] == 7200
    assert calls == [[3]]


async def test_run_check_liveness_deferral_names_the_history_action(monkeypatch):
    """Отложенная живость: окно читается у того действия, которым проверяли
    (`get_chat_history`), код Engage — в результате прогона."""
    calls = _limits_stub(monkeypatch, resets=5400, actions=("get_chat_history",))
    _check_engage_stub(monkeypatch, fail={
        "get_chat_history": engage.EngageTaskDeferred(
            "Engage отложил задачу: BUDGET_PER_ACCOUNT",
            code="BUDGET_PER_ACCOUNT")})
    monkeypatch.setattr(discovery, "get_session_maker", _Maker(_DB()))
    report, _notes = _report_sink()

    stats = await discovery.run_check(report=report, cancelled=lambda: False)

    assert stats["deferred"] is True
    assert stats["deferred_candidates"] == 1
    assert stats["passed_card"] == 1
    assert stats["code"] == "BUDGET_PER_ACCOUNT"
    assert stats["retry_after_s"] == 5400
    assert calls == [[3]]


async def test_run_check_unavailable_is_not_a_budget_deferral(monkeypatch):
    """Недоступность Engage — не «лимит кончился»: маркера и окна нет (прогон
    останется «готово»), `limits()` не спрашивается вовсе; кандидат при этом
    посчитан отложенным — под своим именем счётчика."""
    calls = _limits_stub(monkeypatch, actions=("get_chat_info",))
    _check_engage_stub(monkeypatch, fail={
        "get_chat_info": engage.EngageUnavailable("Engage недоступен")})
    monkeypatch.setattr(discovery, "get_session_maker", _Maker(_DB()))
    report, _notes = _report_sink()

    stats = await discovery.run_check(report=report, cancelled=lambda: False)

    assert stats["checked"] == 1
    assert stats["deferred_candidates"] == 1
    assert "deferred" not in stats
    assert "retry_after_s" not in stats
    assert calls == []


# ── 3. run_scan записывает окно возврата (R4 §5.3) ────────────────────────────

async def test_run_scan_records_retry_after_from_limits(monkeypatch):
    calls = _limits_stub(monkeypatch, resets=7200)
    _scan_engage_stub(monkeypatch, deferred_error=engage.EngageTaskDeferred(
        "Engage отложил задачу: READ_BUDGET_EXCEEDED",
        code="READ_BUDGET_EXCEEDED"))
    monkeypatch.setattr(discovery, "get_session_maker", _Maker(_DB()))
    report, _notes = _report_sink()

    out = await discovery.run_scan(7, params=dict(_PARAMS), report=report,
                                   cancelled=lambda: False)

    assert out["deferred"] is True
    assert out["code"] == "READ_BUDGET_EXCEEDED"
    assert out["retry_after_s"] == 7200
    # Ровно один опрос остатка на отложенный прогон — тем аккаунтом, которым
    # искали; на тике опросов нет вовсе (R2/R4).
    assert calls == [[3]]


async def test_run_scan_without_limits_answer_has_no_window(monkeypatch):
    """Опрос остатка не удался ⇒ ключа нет: тик вернётся к пятиминутному ритму."""
    _limits_stub(monkeypatch, error=engage.EngageUnavailable("маршрута нет"))
    _scan_engage_stub(monkeypatch, deferred_error=engage.EngageTaskDeferred(
        "Engage отложил задачу: BUDGET_PER_ACCOUNT", code="BUDGET_PER_ACCOUNT"))
    monkeypatch.setattr(discovery, "get_session_maker", _Maker(_DB()))
    report, _notes = _report_sink()

    out = await discovery.run_scan(7, params=dict(_PARAMS), report=report,
                                   cancelled=lambda: False)

    assert out["deferred"] is True
    assert "retry_after_s" not in out


async def test_run_scan_window_falls_back_to_aggregate_bucket(monkeypatch):
    """`resets_in_seconds` — свойство действия: если его нет у per_account,
    берётся окно агрегата того же действия (TTL у ключей одного окна одинаков)."""
    calls = []

    async def limits(*, account_ids=None, instance=None):
        calls.append(list(account_ids or []))
        return {"accounts": [{"account_id": 3, "actions": [
            {"action": "search_public_chats",
             "per_account": {"remaining": 49},
             "aggregate": {"remaining": 499, "resets_in_seconds": 5400}}]}]}

    async def action(*, account_id, action, payload, webhook_url, **kw):
        return {"task_id": "t1"}

    async def wait_for_task(task_id, **kw):
        raise engage.EngageTaskDeferred("отложено", code="BUDGET_AGGREGATE")

    monkeypatch.setattr(engage, "limits", limits)
    monkeypatch.setattr(engage, "action", action)
    monkeypatch.setattr(engage, "wait_for_task", wait_for_task)
    monkeypatch.setattr(discovery, "get_session_maker", _Maker(_DB()))
    report, _notes = _report_sink()

    out = await discovery.run_scan(7, params=dict(_PARAMS), report=report,
                                   cancelled=lambda: False)
    assert out["retry_after_s"] == 5400
    assert calls == [[3]]


async def test_run_scan_without_deferral_has_no_window_and_one_order(monkeypatch):
    """Регресс: удачный поиск не зовёт `limits()` вовсе и в результате нет ни
    `deferred`, ни окна (R7: потолок прогона и его прежний итог на месте)."""
    calls = _limits_stub(monkeypatch, resets=7200)

    async def action(*, account_id, action, payload, webhook_url, **kw):
        return {"task_id": "t1"}

    async def wait_for_task(task_id, **kw):
        return {"channels": [{"title": "Банк", "username": "bank",
                              "members_count": 5000}]}

    monkeypatch.setattr(engage, "action", action)
    monkeypatch.setattr(engage, "wait_for_task", wait_for_task)
    monkeypatch.setattr(discovery, "get_session_maker", _Maker(_DB()))
    report, _notes = _report_sink()

    out = await discovery.run_scan(7, params=dict(_PARAMS), report=report,
                                   cancelled=lambda: False)
    assert out == {"found_total": 1, "new_total": 1}
    assert calls == []  # остаток спрашивается только в момент откладывания


# ── 4. вебхук продлевает элемент очереди (R4 §5.4) ────────────────────────────

class _ItemDB:
    def __init__(self, item):
        self.item = item
        self.commits = 0

    async def get(self, model, pk):
        if model is BackfillItem and self.item.id == pk:
            return self.item
        return None

    async def commit(self):
        self.commits += 1


async def test_deferred_webhook_rearms_the_stale_timer(monkeypatch):
    from datetime import datetime, UTC
    from unittest.mock import AsyncMock

    async def touch(run_id, **fields):
        raise AssertionError("у history-элемента без прогона run_id нет")

    async def progress(run_id, pct, note):
        raise AssertionError("у history-элемента без прогона run_id нет")

    monkeypatch.setattr(jobs, "_touch", touch)
    monkeypatch.setattr(jobs, "progress", progress)
    monkeypatch.setattr(jobs, "finish", AsyncMock())

    item = BackfillItem(id=5, channel_id=1, position=1, state="running",
                        attempts=1,
                        started_at=clock.utcnow() - timedelta(minutes=50))
    db = _ItemDB(item)

    out = await process_event(db, BODY, {"kind": "history", "item_id": "5"})

    assert out == {"accepted": 0, "deferred": "READ_BUDGET_EXCEEDED"}
    assert item.state == "running"
    assert item.attempts == 1
    assert item.started_at is not None
    assert isinstance(item.started_at, datetime)
    assert item.started_at.tzinfo is not None
    assert item.started_at <= clock.utcnow()
    assert db.commits == 1
    # STALE-предикат тика (`backfill_drain.tick`: `started_at <= now - час`)
    # элемент больше не возвращает — повторного заказа страницы у Engage нет.
    from app.services.backfill_drain import STALE_AFTER
    assert not (item.started_at <= clock.utcnow() - STALE_AFTER)


@pytest.mark.parametrize("state", ["queued", "done", "failed"])
async def test_not_running_items_are_not_touched(state, monkeypatch):
    """Продлевается только живая цепочка: элемент в очереди ещё не начал ждать,
    закрытый — ждать больше не должен."""
    from unittest.mock import AsyncMock
    monkeypatch.setattr(jobs, "_touch", AsyncMock())
    monkeypatch.setattr(jobs, "progress", AsyncMock())
    monkeypatch.setattr(jobs, "finish", AsyncMock())

    item = BackfillItem(id=5, channel_id=1, position=1, state=state, attempts=1,
                        started_at=clock.utcnow() - timedelta(minutes=50))
    db = _ItemDB(item)

    await process_event(db, BODY, {"kind": "history", "item_id": "5"})

    assert item.state == state
    assert db.commits == 0
