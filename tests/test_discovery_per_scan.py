"""R7, служебный уровень: «пять поисков» — объём прогона (Решение 1).

Проверяется то, что контракт называет потолком в службе: `run_scan` читает
`discovery_queries_per_scan` из `thresholds` (строка `limits` побеждает
умолчание) и с жёсткой границей режет поисковые заказы Engage — откуда бы
прогон ни запустили, ручкой поиска или общей `POST /runs`. Плюс fallback-миграция
переименованного ключа: старая строка `discovery_searches_per_day` в `limits`
на проде возможна, и терять выставленное на ней число молча нельзя.

Сеть и база подменяются целиком — образец tests/test_discovery_service.py:
Engage — заглушкой `engage.action` со счётчиком заказов, сессия — словарём-
обманком, маршрутизирующим запросы по имени таблицы из текста запроса.
Без Postgres эти тесты выполняются, а не пропускаются.
"""
from __future__ import annotations

import logging
import os
from decimal import Decimal

import pytest

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_INGEST_TOKEN", "test-ingest-token")

from app.db.models import DiscoveryQuery
from app.services import discovery, engage

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

    `run_scan` читает только `limits` (пороги) и `channel_candidates` (дедуп
    по username) — остальное вставка.
    """

    def __init__(self, *, limit_rows=()):
        self.limit_rows = list(limit_rows)
        self.added = []
        self.commits = 0

    async def execute(self, stmt):
        sql = str(stmt)
        if "FROM limits" in sql:
            return _Rows(self.limit_rows)
        return _Rows([])

    def add(self, obj):
        self.added.append(obj)

    async def commit(self):
        self.commits += 1


class _Maker:
    """`get_session_maker`-обманка: каждый вход в контекст отдаёт тот же _FakeDB."""

    def __init__(self, db):
        self._db = db

    def __call__(self):
        return self

    async def __aenter__(self):
        return self._db

    async def __aexit__(self, *exc):
        return False


def _stub_engage(monkeypatch, calls: list) -> None:
    """Поиск из двух шагов со счётчиком: `action` фиксирует заказ,
    `wait_for_task` отвечает заготовкой — один канал с именем и числом."""

    async def action(*, account_id, action, payload, webhook_url, **kw):
        calls.append((account_id, action, payload))
        return {"task_id": f"t{len(calls)}"}

    async def wait_for_task(task_id, **kw):
        return {"channels": [{"title": "Банк", "username": "bank",
                              "members_count": 5000}]}

    monkeypatch.setattr(engage, "action", action)
    monkeypatch.setattr(engage, "wait_for_task", wait_for_task)


def _report_sink():
    notes = []

    async def report(pct, note):
        notes.append(note)

    return report, notes


_PARAMS = {"kind": "search", "query": "бухгалтерия", "account_id": 3}


@pytest.fixture
def rename_flag(monkeypatch):
    """Флаг подавления переименования — на процесс; тест управляет им явно,
    чтобы порядок тестов не решал, увиден ли warning."""
    monkeypatch.setattr(discovery, "_KEY_RENAME_LOGGED", False)


# ── 1. DEFAULTS: новое имя, старого нет ──────────────────────────────────────

def test_defaults_name_the_per_scan_ceiling():
    assert discovery.DEFAULTS["discovery_queries_per_scan"] == 5
    assert "discovery_searches_per_day" not in discovery.DEFAULTS


# ── 2. потолок живёт в службе и читается из thresholds ────────────────────────

async def test_ceiling_comes_from_thresholds_and_bites(monkeypatch):
    """Строка `limits` побеждает умолчание, и граница — жёсткая: потолок 0
    значит, что единственный поиск не закажется вовсе, прогон не уйдёт в Engage
    ни разу (общая ручка прогонов тот же код и вызывает)."""
    calls: list = []
    _stub_engage(monkeypatch, calls)
    db = _FakeDB(limit_rows=[("discovery_queries_per_scan", Decimal(0))])
    monkeypatch.setattr(discovery, "get_session_maker", _Maker(db))
    report, _ = _report_sink()

    with pytest.raises(RuntimeError, match="потолок"):
        await discovery.run_scan(1, params=dict(_PARAMS), report=report,
                                 cancelled=lambda: False)
    assert calls == []  # ни одного заказа у Engage


async def test_scan_orders_exactly_one_search(monkeypatch):
    """Сегодняшний прогон — ровно один поисковый заказ (шаг §3.2), и итог —
    кандидаты плюс строка `discovery_queries` с run_id прогона."""
    calls: list = []
    _stub_engage(monkeypatch, calls)
    db = _FakeDB()
    monkeypatch.setattr(discovery, "get_session_maker", _Maker(db))
    report, notes = _report_sink()

    out = await discovery.run_scan(7, params=dict(_PARAMS), report=report,
                                   cancelled=lambda: False)
    assert [(a, p) for _, a, p in calls] == [("search_public_chats",
                                              {"query": "бухгалтерия"})]
    assert calls[0][0] == 3
    assert out == {"found_total": 1, "new_total": 1}
    [q] = [o for o in db.added if isinstance(o, DiscoveryQuery)]
    assert q.run_id == 7 and q.found_total == 1 and q.new_total == 1
    assert any("найдено" in n for n in notes)


# ── 3. fallback-миграция старого ключа `limits` ───────────────────────────────

async def test_old_limits_row_becomes_the_new_key(caplog, rename_flag):
    """Строка со старым именем — источник значения нового ключа; само старое
    имя в порогах не живёт, читателей у него больше нет."""
    db = _FakeDB(limit_rows=[("discovery_searches_per_day", Decimal(7))])
    with caplog.at_level(logging.WARNING, logger="app.services.discovery"):
        lim = await discovery.thresholds(db)
    assert lim["discovery_queries_per_scan"] == 7
    assert "discovery_searches_per_day" not in lim
    warnings = [r.getMessage() for r in caplog.records
                if r.levelno >= logging.WARNING]
    assert warnings == ["limits_key_renamed old=discovery_searches_per_day "
                        "new=discovery_queries_per_scan"]


async def test_rename_warning_is_logged_once_per_process(caplog, rename_flag):
    """Warning — один на процесс, а не на каждый прогон: `thresholds` зовётся
    часто, и спамить одной и той же строкой незачем."""
    db = _FakeDB(limit_rows=[("discovery_searches_per_day", Decimal(7))])
    with caplog.at_level(logging.WARNING, logger="app.services.discovery"):
        for _ in range(3):
            lim = await discovery.thresholds(db)
            assert lim["discovery_queries_per_scan"] == 7
    assert sum(1 for r in caplog.records
               if r.levelno >= logging.WARNING) == 1


async def test_new_key_row_beats_the_old_one(rename_flag):
    """При обеих строках побеждает новая: fallback — путь перехода, а не второе
    зеркало, которое перебивает актуальную настройку."""
    db = _FakeDB(limit_rows=[("discovery_searches_per_day", Decimal(7)),
                             ("discovery_queries_per_scan", Decimal(2))])
    lim = await discovery.thresholds(db)
    assert lim["discovery_queries_per_scan"] == 2


async def test_no_rows_means_defaults(rename_flag):
    """Пустая таблица — рабочее состояние: потолок из кода, пять за прогон."""
    lim = await discovery.thresholds(_FakeDB())
    assert lim == dict(discovery.DEFAULTS)
    assert lim["discovery_queries_per_scan"] == 5
