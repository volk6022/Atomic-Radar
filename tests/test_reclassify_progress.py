"""Прогресс и отмена в стадии сверки лидов — без базы.

Дефект 14.09 (прогон #186): после ступеней стадия сверки (`_reconcile_leads`,
`_reconcile_targets`) шла по всем сообщениям выборки молча — ни строки `report`,
ни проверки `cancelled`. На стенде это 12 минут тишины при нажатой отмене и
«готово» вместо «отменено».

Здесь `run` исполняется с подменённым `db` и без Postgres. Это возможно, потому
что сверка — обычный обход списка сообщений над уже посчитанными вердиктами:
база нужна ей только чтобы прочитать и дописать строки. При выключенных L2/L3
ступени не зовутся вовсе, `targeting.bind_active`/`sync_message` подменяются,
а `FakeDB` отдаёт заготовленные результаты `execute` по порядку вызовов
(выборка сообщений → каналы → лиды → черновики → счётчик по каналам). Модели
данных здесь настоящие (`Message`, `Lead`) — без базы это просто объекты.

Чего здесь нет и быть не может: правил внешних ключей. Удаление лидов с
черновиками и отмена посреди L3 проверяются на настоящем Postgres — в
`test_reclassify_leads_db.py` и `test_reclassify_cancel.py`.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from app.core import cascade
from app.db.models import Message
from app.services import reclassify

NOW = datetime(2026, 9, 14, 12, 0, tzinfo=timezone.utc)

# Текст с якорем боли: проходит строгий L1 без всякой модели (тот же, что в
# DB-тестах), поэтому каждое сверенное сообщение доезжает до очереди лидов.
TEXT = "не могу оплатить инвойс, помогите разобраться"

TOTAL = 1200  # ≥ 1200 из условия: две строки прогресса по шагу 500 плюс финальная

# Канал и сценарий — не модели, а достаточные по полям заглушки: сверка и цикл
# L0/L1 читают из них ровно эти атрибуты. Профиль — настоящий: `_wf_verdicts`
# судит им сообщения и рассчитывает на все поля каскадного профиля.
CHANNEL = SimpleNamespace(id=7, l1_bypass_enabled=False)
BOUND = SimpleNamespace(workflow=SimpleNamespace(id=1, target_kind="user"),
                        profile=cascade.DM_V1)

# Вердикт «лид» для прямых вызовов `_reconcile_leads` без каскада.
PASS = {"level": 1, "passed": True, "detail": {}, "pain": "нужна помощь",
        "score": 60, "breakdown": [], "disqualifiers": []}


def _messages(n: int) -> list[Message]:
    out = []
    for i in range(n):
        m = Message(channel_id=CHANNEL.id, tg_message_id=1000 + i,
                    tg_date=NOW - timedelta(minutes=i), author_peer_id=500 + i,
                    author_username=f"user{i}", author_name="Имя",
                    author_is_bot=False, is_automatic_forward=False, text=TEXT,
                    processed_at=NOW)
        m.id = i + 1  # без базы идентификатор никто не выдаёт — нумеруем сами
        out.append(m)
    return out


class _Result:
    """Отданный результат запроса: хватает `.scalars().all()` и `.all()`."""

    def __init__(self, rows):
        self._rows = rows

    def scalars(self):
        return self

    def all(self):
        return list(self._rows)

    def scalar_one_or_none(self):
        return self._rows[0] if self._rows else None


class FakeDB:
    """Отдаёт заготовленные результаты `execute` по порядку вызовов.

    Порядок для `run` (scope="all", L2/L3 выключены, сценарий привязан):
    выборка сообщений, каналы, лиды, черновики, счётчик по каналам. Второй
    лишний запрос — ошибка с понятным текстом, а не молчаливая сдача чужих строк.
    """

    def __init__(self, results):
        self._results = list(results)
        self.added = []
        self.deleted = []
        self.commits = 0

    async def execute(self, stmt):
        if not self._results:
            raise AssertionError(f"неожиданный запрос к базе: {stmt}")
        return _Result(self._results.pop(0))

    def add(self, obj):
        self.added.append(obj)

    async def delete(self, obj):
        self.deleted.append(obj)

    async def commit(self):
        self.commits += 1


def run_db(messages: list[Message]) -> FakeDB:
    total = len(messages)
    return FakeDB([messages, [CHANNEL], [], [], [(CHANNEL.id, total)]])


def stub_targeting(monkeypatch, calls: dict):
    """`bind_active` возвращает один привязанный сценарий, `sync_message` считает."""
    async def fake_bind(db):
        return [BOUND]

    async def fake_sync(db, bound_list, *, message, channel, **kwargs):
        calls["synced"] += 1

    monkeypatch.setattr(reclassify.targeting, "bind_active", fake_bind)
    monkeypatch.setattr(reclassify.targeting, "sync_message", fake_sync)


def stub_stages(monkeypatch):
    """Ступени L2/L3 — в ноль: их поведение здесь не проверяется."""
    async def no_l2(db, waiting, verdicts, **kwargs):
        return 0

    async def no_l3(db, jobs, **kwargs):
        return 0

    monkeypatch.setattr(reclassify, "_stage_l2", no_l2)
    monkeypatch.setattr(reclassify, "_stage_l3", no_l3)


def collect_report(notes: list):
    async def report(pct, note):
        notes.append((pct, note))
    return report


# ── (а) человеческая строка про выключенные ступени ───────────────────────────

async def test_disabled_stages_are_named_in_the_log(monkeypatch):
    stub_targeting(monkeypatch, {"synced": 0})
    cases = [
        (False, False, "L2 выключена, L3 выключена — "
                       "лиды считаются по строгому L1 (без модели)"),
        (True, False, "L3 выключена — лиды считаются словарём L1 "
                      "и близостью L2 (без модели)"),
        (False, True, "L2 выключена — лиды считаются по строгому L1 (без модели)"),
    ]
    for l2, l3, expected in cases:
        if l2 or l3:
            stub_stages(monkeypatch)
        notes: list = []
        summary = await reclassify.run(
            run_db(_messages(3)), l2_enabled=l2, l3_enabled=l3, scope="all",
            report=collect_report(notes))
        assert any(expected in note for _, note in notes), \
            f"L2={l2}, L3={l3}: строки «{expected}» нет в логе"
        assert summary["cancelled"] is False


async def test_no_disabled_line_when_both_enabled(monkeypatch):
    stub_targeting(monkeypatch, {"synced": 0})
    stub_stages(monkeypatch)
    notes: list = []
    await reclassify.run(run_db(_messages(3)), l2_enabled=True, l3_enabled=True,
                         scope="all", report=collect_report(notes))
    assert not any("выключена" in note for _, note in notes), \
        "при включённых ступенях строка про выключение не пишется"


# ── (б) прогресс обеих сверок ─────────────────────────────────────────────────

async def test_reconcile_progress_lines_and_band(monkeypatch):
    calls = {"synced": 0}
    stub_targeting(monkeypatch, calls)
    messages = _messages(TOTAL)
    db = run_db(messages)
    notes: list = []
    summary = await reclassify.run(
        db, l2_enabled=False, l3_enabled=False, scope="all",
        report=collect_report(notes))

    # Строки начала стадий — без процента, с полными числами.
    assert any(note == f"сверка лидов: сообщений {TOTAL}" for _, note in notes)
    assert any(note == f"сверка целей: {TOTAL}, сценариев 1" for _, note in notes)

    # Прогресс лидов: несколько строк «… из …», проценты растут в полосе [95, 100].
    lead_lines = [(pct, note) for pct, note in notes
                  if note.startswith("сверка лидов: ") and " из " in note]
    assert len(lead_lines) >= 2, "прогресс сверки лидов не доходит до лога"
    pcts = [pct for pct, _ in lead_lines]
    assert all(95 <= pct <= 100 for pct in pcts), pcts
    assert all(b > a for a, b in zip(pcts, pcts[1:])), pcts

    # Прогресс целей: полоса от 97.5 до 100, сценарий непустой.
    target_lines = [(pct, note) for pct, note in notes
                    if note.startswith("сверка целей: ") and " из " in note]
    assert len(target_lines) >= 2, "прогресс сверки целей не доходит до лога"
    assert all(97.5 <= pct <= 100 for pct, _ in target_lines)

    assert calls["synced"] == TOTAL, "цели сверены для всех сообщений"
    assert summary["reconciled"] == {"leads": TOTAL, "targets": TOTAL,
                                     "of": TOTAL}
    assert summary["cancelled"] is False
    assert db.commits == 1, "коммит один на весь прогон"
    assert notes[-1][0] == 100 and notes[-1][1].startswith("готово"), \
        "штатный прогон кончается итоговой строкой «готово»"


# ── (в) отмена во время сверки лидов ──────────────────────────────────────────

async def test_cancel_during_leads_reconcile(monkeypatch):
    calls = {"synced": 0}
    stub_targeting(monkeypatch, calls)
    db = run_db(_messages(TOTAL))
    notes: list = []
    checks = {"n": 0}

    def cancelled():
        checks["n"] += 1
        return checks["n"] >= 2  # первая проверка (500-е сообщение) — ещё рано

    summary = await reclassify.run(
        db, l2_enabled=False, l3_enabled=False, scope="all",
        report=collect_report(notes), cancelled=cancelled)

    assert summary["cancelled"] is True
    reconciled = summary["reconciled"]
    assert 0 < reconciled["leads"] < TOTAL, "сверка лидов остановилась посреди"
    assert reconciled["targets"] == 0, "цели после отмены на лидах не начинаются"
    assert reconciled["of"] == TOTAL
    assert summary["created"] == reconciled["leads"], \
        "посчитанное по сверенным сообщениям доезжает до очереди лидов"
    assert db.commits == 1, "коммит один и выполняется даже при отмене"
    assert calls["synced"] == 0, "сверка целей не начиналась"
    assert any("остановлено на сверке лидов" in note for _, note in notes)
    assert not any(note.startswith("готово") for _, note in notes), \
        "итоговая строка «готово» при отмене не пишется"


# ── (г) отмена во время сверки целей ──────────────────────────────────────────

async def test_cancel_during_targets_reconcile(monkeypatch):
    calls = {"synced": 0}
    stub_targeting(monkeypatch, calls)
    db = run_db(_messages(TOTAL))
    notes: list = []
    checks = {"n": 0}

    def cancelled():
        checks["n"] += 1
        return checks["n"] >= 5  # три проверки лидов, одна в run, потом — стоп

    summary = await reclassify.run(
        db, l2_enabled=False, l3_enabled=False, scope="all",
        report=collect_report(notes), cancelled=cancelled)

    assert summary["cancelled"] is True
    reconciled = summary["reconciled"]
    assert reconciled["leads"] == TOTAL, "лиды сверены целиком"
    assert 0 < reconciled["targets"] < TOTAL, "цели сверены только частично"
    assert reconciled["of"] == TOTAL
    assert 0 < calls["synced"] < TOTAL
    assert db.commits == 1
    assert any("остановлено на сверке целей" in note for _, note in notes)
    assert not any(note.startswith("готово") for _, note in notes)


# ── (д) старые вызовы без новых аргументов ────────────────────────────────────

async def test_reconcile_leads_old_signature_still_works():
    messages = _messages(3)
    db = FakeDB([[], []])  # лидов и черновиков нет
    created, removed, kept = await reclassify._reconcile_leads(
        db, messages, {m.id: dict(PASS) for m in messages})
    assert (created, removed, kept) == (3, 0, 0)
    assert len(db.added) == 3
    assert db.commits == 0, "функция не коммитит сама — как и раньше"


async def test_reconcile_targets_old_signature_still_works():
    messages = _messages(3)
    summary = await reclassify._reconcile_targets(
        FakeDB([]), [], messages, l2_enabled=False, l3_enabled=False,
        ranked={}, llm_answers={}, channels={CHANNEL.id: CHANNEL})
    assert summary == {}, "без сценариев функция ничего не делает"
