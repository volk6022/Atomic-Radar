"""Служба discovery: пороги и три проверки кандидата (T4, §4–§6 контракта).

Проверяется конвейер по步 за шагом: карточка → живость → модель, — и главное
свойство порядка: следующая ступень не зовётся, пока кандидат не прошёл
предыдущую («модель зовём последней», B.3). Поэтому «модель не звалась» здесь
всегда проверяется самой заглушкой `llm.verdict` (список её вызовов пуст), а не
косвенными признаками вроде пустого вердикта.

Сеть и база подменяются целиком: Engage — двухшаговой заглушкой
`engage.action`/`wait_for_task` (как в tests/test_discussions_join_db.py), сессия
— словарём-обманком, маршрутизирующим запросы по имени таблицы из текста
запроса. Правки настроек не нужны: пороги без строки `limits` берутся из кода.
"""
from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_INGEST_TOKEN", "test-ingest-token")

from app.db.models import ChannelCandidate, LlmTrace, ProfileVersion
from app.services import discovery, engage, llm

NOW = datetime(2026, 9, 8, 12, 0, tzinfo=UTC)


# ── заглушки без базы и сети ──────────────────────────────────────────────────

class _Rows:
    """Результат запроса: хватает на all()/scalars()/scalar_one*() службы."""

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


class _FakeDB:
    """Сессия без базы: ответ выбирается по имени таблицы из текста запроса.

    Служба читает четыре таблицы (limits, llm_traces, profile_versions,
    channel_candidates); подстрока в SQL надёжнее разбора внутренностей
    SQLAlchemy-выражения и переживает любые правки формы запроса.
    """

    def __init__(self, *, limit_rows=(), trace_count=0, auto_joins=0, profile=None):
        self.limit_rows = list(limit_rows)
        self.trace_count = trace_count
        self.auto_joins = auto_joins
        self.profile = profile
        self.added = []
        self.commits = 0

    async def execute(self, stmt):
        sql = str(stmt)
        if "llm_traces" in sql:
            return _Rows([self.trace_count])
        if "decided_by" in sql and "channel_candidates" in sql:
            return _Rows([self.auto_joins])
        if "FROM limits" in sql:
            return _Rows(self.limit_rows)
        if "profile_versions" in sql:
            return _Rows([self.profile] if self.profile is not None else [])
        if "channel_candidates" in sql:
            return _Rows([0])
        return _Rows([])

    def add(self, obj):
        self.added.append(obj)

    async def commit(self):
        self.commits += 1


def _candidate(**over) -> ChannelCandidate:
    base = {"username": "bank", "title": "Банк", "source": "similar",
            "found_by_account_id": 3, "decision": "pending"}
    base.update(over)
    return ChannelCandidate(**base)


def _stub_engage(monkeypatch, calls, *, card=None, history=None, fail=None):
    """Engage из двух шагов: `action` ставит задачу, `wait_for_task` отвечает.

    card — ответ `get_chat_info`; history(payload) — ответ `get_chat_history`
    по payload; fail — {(действие, username): исключение}.
    """
    fail = fail or {}
    tasks: dict[str, tuple] = {}

    async def action(*, account_id, action, payload, webhook_url, **kw):
        calls.append((action, payload))
        task_id = f"t{len(tasks) + 1}"
        tasks[task_id] = (action, payload)
        return {"task_id": task_id}

    async def wait_for_task(task_id, **kw):
        act, payload = tasks[task_id]
        key = (act, payload.get("username"))
        if key in fail:
            raise fail[key]
        if act == "get_chat_info":
            return card
        return history(payload)

    monkeypatch.setattr(engage, "action", action)
    monkeypatch.setattr(engage, "wait_for_task", wait_for_task)


def _stub_llm(monkeypatch, parsed=None, error=None):
    """`llm.verdict` без сети: копирует вызовы и отвечает заготовкой.

    Возвращает список вызовов — пустой список и есть доказательство «не звалась».
    """
    calls = []

    async def verdict(*, text, context, prompt_key, system=None, user=None):
        calls.append({"text": text, "context": context, "prompt_key": prompt_key,
                      "system": system, "user": user})
        if error is not None:
            raise error
        trace = {"stage": "l3", "model": "qwen-local",
                 "prompt_version": "channel-fit-v1", "temperature": 0.0,
                 "prompt": user, "response": "", "tokens_in": 1, "tokens_out": 1,
                 "latency_ms": 1, "cost_usd": 0}
        return parsed, trace

    monkeypatch.setattr(llm, "verdict", verdict)
    return calls


def _report_sink():
    notes = []

    async def report(pct, note):
        notes.append(note)

    return report, notes


def _card(members=10_000, linked="bank_chat") -> dict:
    return {"found": True, "peer_id": -1001234, "title": "Банк", "type": "channel",
            "linked_chat_username": linked, "members_count": members}


# ── 1. пороги: строка limits побеждает умолчание ──────────────────────────────

async def test_db_limit_row_wins_and_missing_keys_come_from_code():
    db = _FakeDB(limit_rows=[("discovery_min_members", Decimal(1500)),
                             ("discovery_min_posts_7d", Decimal(7))])
    lim = await discovery.thresholds(db)
    assert lim["discovery_min_members"] == 1500
    assert lim["discovery_min_posts_7d"] == 7
    # отсутствующие ключи — из кода, включая выключатель автовключения (B.4)
    assert lim["discovery_min_comments_7d"] == 50
    assert lim["discovery_llm_checks_per_day"] == 100
    assert lim["discovery_autoconnect_enabled"] == 0
    # пустая таблица limits — рабочее состояние (§2: строки никем не заводятся)
    assert await discovery.thresholds(_FakeDB()) == dict(discovery.DEFAULTS)


# ── 2. кандидат без username ──────────────────────────────────────────────────

async def test_candidate_without_username_is_rejected_before_engage(monkeypatch):
    async def boom(*a, **kw):
        raise AssertionError("карточку без username спросить нельзя — "
                             "до Engage дело доходить не должно")

    monkeypatch.setattr(engage, "action", boom)
    db = _FakeDB()
    cand = _candidate(username=None, title="Безымянный")

    assert await discovery.check_card(db, cand, account_id=3) == "reject"
    assert cand.decision == "rejected"
    assert cand.decided_by == "auto:card"
    assert "username" in cand.decision_reason
    assert cand.decided_at is not None


# ── 3. members ниже порога ────────────────────────────────────────────────────

async def test_members_below_threshold_rejects_with_number_before_liveness(
        monkeypatch):
    calls: list = []
    _stub_engage(monkeypatch, calls, card=_card(members=120))
    db = _FakeDB()
    cand = _candidate()

    assert await discovery.check_card(db, cand, account_id=3) == "reject"
    assert cand.decision == "rejected"
    assert cand.decided_by == "auto:card"
    # число и порог — в причине, как требует приёмка
    assert "120" in cand.decision_reason and "500" in cand.decision_reason
    # живость не запрашивалась: единственный вызов — карточка
    assert [a for a, _ in calls] == ["get_chat_info"]


# ── 4. нет группы обсуждения ──────────────────────────────────────────────────

async def test_missing_discussion_group_rejects_before_liveness(monkeypatch):
    calls: list = []
    _stub_engage(monkeypatch, calls, card=_card(linked=None))
    db = _FakeDB()
    cand = _candidate()

    assert await discovery.check_card(db, cand, account_id=3) == "reject"
    assert cand.decision == "rejected"
    assert "группы обсуждения" in cand.decision_reason
    assert [a for a, _ in calls] == ["get_chat_info"]


# ── 5. окно живости уезжает в get_chat_history параметром min_date ────────────

async def test_liveness_window_goes_to_get_chat_history_as_min_date(monkeypatch):
    calls: list = []
    posts = [{"message_id": i, "text": f"пост {i}"} for i in (3, 2, 1)]
    group = [{"message_id": 1000 + i, "text": "комментарий"} for i in range(60)]

    def history(payload):
        return {"posts": posts if payload["username"] == "bank" else group}

    _stub_engage(monkeypatch, calls, history=history)
    db = _FakeDB()
    cand = _candidate(members=10_000, linked_chat_username="bank_chat")

    out = await discovery.check_liveness(db, cand, account_id=3, now=NOW)
    assert out == "pass"

    histories = [p for a, p in calls if a == "get_chat_history"]
    assert len(histories) == 2
    expected = (NOW - timedelta(days=7)).isoformat()
    for payload in histories:
        # окно — параметр самого действия, а не постфильтр у нас
        assert payload["min_date"] == expected
        assert payload["limit"] == 100
    assert cand.liveness_posts_7d == 3
    assert cand.liveness_comments_7d == 60
    assert cand.liveness_checked_at == NOW
    assert cand.decision == "pending"  # 3 ≥ 2 и 60 ≥ 50 — живой идёт дальше


# ── 6. мало постов / мало сообщений: отказ с числами, модель не зовётся ───────

async def test_low_liveness_rejects_with_numbers_and_never_calls_llm(monkeypatch):
    verdict_calls = _stub_llm(
        monkeypatch, parsed={"verdict": "fit", "score": "80", "reason": "подходит"})
    report, _ = _report_sink()

    # а) мало постов: 1 < 2
    calls: list = []
    _stub_engage(monkeypatch, calls, card=_card(), history=lambda payload: {
        "posts": ([{"message_id": 5, "text": "пост"}] if payload["username"] == "bank"
                  else [{"message_id": i, "text": "ок"} for i in range(60)])})
    db = _FakeDB()
    cand = _candidate(members=10_000, linked_chat_username="bank_chat")
    outcome = await discovery._check_one(db, cand, fleet={3}, report=report)
    assert outcome == {"card": "pass", "liveness": "reject", "fit": None,
                       "asked": False, "note": outcome["note"]}
    assert cand.decision == "rejected"
    assert cand.decided_by == "auto:liveness"
    assert "1 пост" in cand.decision_reason and "порога 2" in cand.decision_reason

    # б) постов хватает, сообщений в группе мало: 49 < 50
    _stub_engage(monkeypatch, calls, card=_card(), history=lambda payload: {
        "posts": ([{"message_id": i, "text": f"пост {i}"} for i in (5, 4, 3)]
                  if payload["username"] == "bank"
                  else [{"message_id": i, "text": "ок"} for i in range(49)])})
    db = _FakeDB()
    cand = _candidate(members=10_000, linked_chat_username="bank_chat")
    outcome = await discovery._check_one(db, cand, fleet={3}, report=report)
    assert outcome["liveness"] == "reject" and outcome["fit"] is None
    assert "49" in cand.decision_reason and "порога 50" in cand.decision_reason

    # модель не звалась ни разу — в обеих ветках отсеялись раньше неё
    assert verdict_calls == []


# ── 7. EngageUnavailable на карточке ─────────────────────────────────────────

async def test_engage_unavailable_on_card_defers_and_touches_nothing(monkeypatch):
    calls: list = []
    _stub_engage(monkeypatch, calls, card=_card(), fail={
        ("get_chat_info", "bank"): engage.EngageUnavailable(
            "Engage недоступен: ConnectError")})
    db = _FakeDB()
    cand = _candidate()

    assert await discovery.check_card(db, cand, account_id=3) == "defer"
    assert cand.decision == "pending"
    assert cand.decided_by is None and cand.decision_reason is None
    # поля не тронуты: сбой сети — не вина канала
    assert cand.peer_id is None and cand.members is None
    assert cand.linked_chat_username is None
    assert db.commits == 0


# ── 8. EngageTaskDeferred на живости ──────────────────────────────────────────

async def test_deferred_task_on_liveness_defers_not_rejects(monkeypatch):
    calls: list = []
    _stub_engage(monkeypatch, calls, history=lambda payload: {"posts": []}, fail={
        ("get_chat_history", "bank"): engage.EngageTaskDeferred(
            "Engage отложил задачу: дневной лимит", code="BUDGET_ACCOUNT")})
    db = _FakeDB()
    cand = _candidate(members=10_000, linked_chat_username="bank_chat")
    report, notes = _report_sink()

    out = await discovery.check_liveness(db, cand, account_id=3, report=report)
    assert out == "defer"
    assert cand.decision == "pending"
    # поля живости не писались: шаг не доделан, а не «мёртв»
    assert cand.liveness_posts_7d is None and cand.liveness_comments_7d is None
    assert cand.liveness_checked_at is None
    assert any("отлож" in note for note in notes)


# ── 9. вердикт без обоснования не сохраняется вовсе ───────────────────────────

@pytest.mark.parametrize("parsed", [
    {"verdict": "fit", "score": "90", "reason": ""},
    {"verdict": "fit", "score": "90"},
])
async def test_verdict_without_reason_is_not_saved_at_all(monkeypatch, parsed):
    calls = _stub_llm(monkeypatch, parsed=parsed)
    db = _FakeDB()
    cand = _candidate(members=10_000, linked_chat_username="bank_chat")
    report, notes = _report_sink()

    assert await discovery.check_fit(db, cand, report=report,
                                     posts=["текст"]) == "defer"
    assert cand.llm_verdict is None and cand.llm_score is None
    assert cand.llm_reason is None and cand.llm_at is None
    assert cand.decision == "pending"
    assert len(calls) == 1
    assert db.added == []  # ни трейса, ни чего-либо ещё
    assert db.commits == 0
    assert any("обосновани" in note for note in notes)


# ── 10. unfit → rejected с причиной модели ────────────────────────────────────

async def test_unfit_rejects_candidate_with_llm_reason(monkeypatch):
    _stub_llm(monkeypatch, parsed={"verdict": "unfit", "score": "10",
                                   "reason": "не та аудитория"})
    db = _FakeDB()
    cand = _candidate(members=10_000, linked_chat_username="bank_chat")
    report, _ = _report_sink()

    assert await discovery.check_fit(db, cand, report=report,
                                     posts=["текст"]) == "reject"
    assert cand.decision == "rejected"
    assert cand.decided_by == "auto:channel_fit_v1"
    assert cand.decision_reason == "не та аудитория"
    assert cand.llm_verdict == "unfit" and cand.llm_score == 10
    assert cand.llm_reason == "не та аудитория" and cand.llm_at is not None
    assert len(db.added) == 1 and isinstance(db.added[0], LlmTrace)
    assert db.added[0].prompt_version == "channel-fit-v1"
    assert db.commits == 1


# ── 11. fit при выключенном автовыключателе остаётся pending ──────────────────

async def test_fit_does_not_approve_by_itself_with_switch_off(monkeypatch):
    _stub_llm(monkeypatch, parsed={"verdict": "fit", "score": "85",
                                   "reason": "бухгалтеры МСБ, боли совпадают"})
    db = _FakeDB()  # limits пусты → выключатель 0, умолчание B.4
    cand = _candidate(members=10_000, linked_chat_username="bank_chat")
    report, _ = _report_sink()

    assert await discovery.check_fit(db, cand, report=report,
                                     posts=["текст"]) == "pass"
    assert cand.llm_verdict == "fit" and cand.llm_score == 85
    assert cand.decision == "pending"
    assert cand.decided_by is None and cand.decided_at is None


# ── 12. модель недоступна — «не досчитали», не отказ ──────────────────────────

async def test_llm_unavailable_defers_not_rejects(monkeypatch):
    _stub_llm(monkeypatch, error=llm.LlmUnavailable("LLM недоступна: ConnectError"))
    db = _FakeDB()
    cand = _candidate(members=10_000, linked_chat_username="bank_chat")
    report, _ = _report_sink()

    assert await discovery.check_fit(db, cand, report=report,
                                     posts=["текст"]) == "defer"
    assert cand.decision == "pending"
    assert cand.llm_verdict is None and cand.llm_score is None
    assert cand.llm_reason is None and cand.llm_at is None
    assert db.added == [] and db.commits == 0


# ── 13. исчерпанный дневной лимит проверок ────────────────────────────────────

async def test_exhausted_llm_budget_defers_without_calling_model(monkeypatch):
    verdict_calls = _stub_llm(
        monkeypatch, parsed={"verdict": "fit", "score": "80", "reason": "подходит"})
    db = _FakeDB(trace_count=100)  # лимит по умолчанию — 100, исчерпан
    cand = _candidate(members=10_000, linked_chat_username="bank_chat")
    report, notes = _report_sink()

    assert await discovery.check_fit(db, cand, report=report,
                                     posts=["текст"]) == "defer"
    assert verdict_calls == []  # модель не звалась — сам факт, а не пустой вердикт
    assert cand.decision == "pending"
    assert cand.llm_verdict is None and cand.llm_reason is None
    assert any("лимит" in note for note in notes)


# ── 14. скобки в описании бизнеса не ломают системный промпт ──────────────────

async def test_braces_in_business_description_do_not_break_system_prompt(
        monkeypatch):
    descr = "продаём отчёты вида {выручка} и шаблоны } обратные {"
    profile = ProfileVersion(id=1, version="v1", business_description=descr,
                             is_active=True)
    calls = _stub_llm(monkeypatch, parsed={"verdict": "unfit", "score": "5",
                                           "reason": "мимо"})
    db = _FakeDB(profile=profile)
    cand = _candidate(members=10_000, linked_chat_username="bank_chat")
    report, _ = _report_sink()

    assert await discovery.check_fit(db, cand, report=report,
                                     posts=["текст"]) == "reject"
    [call] = calls
    assert call["prompt_key"] == "channel_fit_v1"
    assert call["text"] == "" and call["context"] == []
    # подстановка дословная: .replace, а не .format (REVIEW.md §1)
    assert descr in call["system"]
    assert "{business_description}" not in call["system"]


# ── вход модели (§5) ──────────────────────────────────────────────────────────

def test_channel_fit_input_holds_the_card_and_trims_the_sample():
    cand = _candidate(username="bank", title="Банк", members=2_700_000,
                      chat_type="channel", linked_chat_username="bank_chat")
    posts = [f"текст {i} " + "x" * 300 for i in range(12)]
    text = discovery.build_channel_fit_input(cand, posts)
    assert "Название: Банк" in text
    assert "@bank" in text and "@bank_chat" in text
    assert "2 700 000".replace(" ", "") in text.replace(" ", "")
    assert text.count("текст ") == 10  # не больше десяти текстов выборки
    for line in text.splitlines():
        assert len(line) <= 210 or not line.startswith("- «")
    empty = discovery.build_channel_fit_input(cand, [])
    assert "выборки нет" in empty


# ── проводка прогонов (§4.4) ──────────────────────────────────────────────────

def test_jobs_kinds_and_runners_are_registered():
    from app.services import jobs
    assert "discovery_scan" in jobs.KINDS
    assert "discovery_check" in jobs.KINDS
    assert set(jobs.RUNNERS) <= set(jobs.KINDS)
    assert callable(jobs.RUNNERS["discovery_scan"])
    assert callable(jobs.RUNNERS["discovery_check"])


def test_discovery_tick_is_scheduled_and_computable():
    """Расписание обязано вычисляться, а не просто быть: `range` в `minute` уже
    валил воркера после успешной выкатки (tests/test_backfill_cron.py — та же
    авария 05.09, ловится только `calculate_next`)."""
    from app.workers.ingest import WorkerSettings
    cron_jobs = list(WorkerSettings.cron_jobs)
    names = {getattr(job, "name", None) or getattr(job.coroutine, "__name__", "")
             for job in cron_jobs}
    # arq даёт кронам имя с префиксом «cron:» — сверяем вхождением, как сосед
    assert any("discovery_check_tick" in str(name) for name in names), names
    for job in cron_jobs:
        job.calculate_next(datetime(2026, 9, 5, 12, 0, 0, tzinfo=UTC))
        assert job.next_run is not None
