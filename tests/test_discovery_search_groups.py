"""Поиск по строке находит группы — и конвейер их больше не теряет (3.f/3.g).

Два дефекта одного происхождения: `search_public_chats` отдаёт и каналы, и
группы, а конвейер был устроен только под каналы. Список найденного читался
«первым списком» ответа — при найденных одних группах он пуст (3.f); карточка
резала кандидата без группы обсуждения, живость требовала посты канала и
комментарии чужой группы (3.g). Здесь группа проходит конвейер как сама чат,
а прежнее поведение каналов и «похожих» не изменилось.

Сеть и база подменяются целиком — образец tests/test_discovery_service.py:
Engage — двухшаговой заглушкой `engage.action`/`wait_for_task`, сессия —
словарём-обманком. Без Postgres тесты выполняются, а не пропускаются.
"""
from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_INGEST_TOKEN", "test-ingest-token")

from app.db.models import ChannelCandidate, DiscoveryQuery
from app.services import discovery, engage

NOW = datetime(2026, 9, 18, 12, 0, tzinfo=UTC)


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

    `run_scan` читает `limits` и ищет дубликат по `channel_candidates` (пусто —
    новый кандидат); проверкам хватает того же: пороги и коммит.
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

    async def get(self, model, id_):
        return None

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


def _stub_engage(monkeypatch, calls, *, card=None, history=None, search=None):
    """Engage из двух шагов: `action` ставит задачу, `wait_for_task` отвечает.

    card — ответ `get_chat_info`; history(payload) — ответ `get_chat_history`;
    search — ответ `search_public_chats` (форма из кода воркера Engage).
    """

    async def action(*, account_id, action, payload, webhook_url, **kw):
        calls.append((account_id, action, payload))
        return {"task_id": f"t{len(calls)}"}

    async def wait_for_task(task_id, **kw):
        _account, act, payload = calls[int(task_id[1:]) - 1]
        if act == "get_chat_info":
            return card
        if act == "search_public_chats":
            return search
        return history(payload)

    monkeypatch.setattr(engage, "action", action)
    monkeypatch.setattr(engage, "wait_for_task", wait_for_task)


def _candidate(**over) -> ChannelCandidate:
    base = {"username": "buhchat", "title": "Бухгалтерия — чат",
            "source": "search", "found_by_account_id": 3, "decision": "pending",
            "chat_type": "supergroup", "members": 800}
    base.update(over)
    return ChannelCandidate(**base)


def _group_card(members=800, linked=None) -> dict:
    """Карточка группы от `get_chat_info`: `linked_chat_username` — родительский
    канал (у группы без родителя его нет)."""
    return {"found": True, "peer_id": -1001234, "title": "Бухгалтерия — чат",
            "type": "supergroup", "linked_chat_username": linked,
            "members_count": members}


def _channel_card(members=10_000, linked=None) -> dict:
    return {"found": True, "peer_id": -1001235, "title": "Банк",
            "type": "channel", "linked_chat_username": linked,
            "members_count": members}


# ── 3.f: список найденного из ответа поиска ──────────────────────────────────

G1 = {"peer_id": -2001, "username": "buhchat", "title": "Бухгалтерия — чат",
      "members": 5000, "type": "supergroup"}
G2 = {"peer_id": -2002, "username": "buhclub", "title": "Клуб бухгалтеров",
      "members": 4300, "type": "group"}
C1 = {"peer_id": -1001, "username": "bank", "title": "Банк",
      "members": 9000, "type": "channel"}
C2 = {"peer_id": -1002, "username": "bank2", "title": "Банк 2",
      "members": 8000, "type": "channel"}


def test_found_items_search_takes_results_whole():
    """Форма воркера `search_public_chats`: полный список — в `results`,
    `channels`/`chats` — его разрез. Пустой разрез каналов больше не прячет
    группы."""
    result = {"found": True, "query": "бухгалтерия", "count": 2,
              "results": [G1, G2], "channels": [], "chats": [G1, G2]}
    assert discovery._found_items(result, kind="search") == [G1, G2]
    # не словари в списках отсеиваются, как и прежде
    noisy = {"results": [G1, "мусор", None], "channels": [], "chats": [G1]}
    assert discovery._found_items(noisy, kind="search") == [G1]


def test_found_items_without_kind_keeps_the_first_list():
    """Без `kind` — прежнее поведение: первый список среди известных ключей.
    На форме поиска он и был дефектом (пустой `channels`), на «похожих» —
    по-прежнему единственно верный путь."""
    result = {"found": True, "count": 2, "results": [G1, G2],
              "channels": [], "chats": [G1, G2]}
    assert discovery._found_items(result) == []
    similar = {"found": True, "count": 2, "channels": [C1, C2]}
    assert discovery._found_items(similar) == [C1, C2]
    assert discovery._found_items(similar, kind="similar") == [C1, C2]


def test_found_items_search_without_results_stacks_chats_and_channels():
    """Старая форма без `results`: группы, потом каналы — порядок зафиксирован."""
    result = {"found": True, "count": 3, "channels": [C1], "chats": [G1, G2]}
    assert discovery._found_items(result, kind="search") == [G1, G2, C1]
    assert discovery._found_items({"channels": [C1]}, kind="search") == [C1]


# ── 3.g: карточка ─────────────────────────────────────────────────────────────

async def test_card_lets_supergroup_without_linked_chat_through(monkeypatch):
    """Группа — сама чат: правило «нет группы обсуждения» к ней не применяется.
    Родительский канал в `linked_chat_username` карточка группы не заносит —
    и не должна: он нужен `channel_add`, не живости."""
    calls: list = []
    _stub_engage(monkeypatch, calls, card=_group_card(members=800, linked=None))
    db = _FakeDB()
    cand = _candidate()

    assert await discovery.check_card(db, cand, account_id=3) == "pass"
    assert cand.decision == "pending" and cand.decided_by is None
    assert cand.chat_type == "supergroup"
    assert cand.linked_chat_username is None
    assert cand.members == 800
    assert [(a, p) for _, a, p in calls] == \
        [("get_chat_info", {"username": "buhchat"})]


async def test_card_still_cuts_channel_without_discussion_group(monkeypatch):
    calls: list = []
    _stub_engage(monkeypatch, calls, card=_channel_card(linked=None))
    db = _FakeDB()
    cand = _candidate(username="bank", title="Банк", chat_type=None)

    assert await discovery.check_card(db, cand, account_id=3) == "reject"
    assert cand.decision == "rejected"
    assert "группы обсуждения" in cand.decision_reason


async def test_card_applies_members_threshold_to_group_as_is(monkeypatch):
    """Порог участников действует без скидок на тип: малая группа режется
    числом и порогом, как канал."""
    calls: list = []
    _stub_engage(monkeypatch, calls, card=_group_card(members=120))
    db = _FakeDB()
    cand = _candidate(members=None)

    assert await discovery.check_card(db, cand, account_id=3) == "reject"
    assert "120" in cand.decision_reason and "500" in cand.decision_reason


# ── 3.g: живость группы ───────────────────────────────────────────────────────

async def test_liveness_for_supergroup_reads_the_group_itself(monkeypatch):
    calls: list = []

    def history(payload):
        return {"posts": [{"message_id": 1000 + i, "text": f"сообщение {i}"}
                          for i in range(60)]}

    _stub_engage(monkeypatch, calls, history=history)
    db = _FakeDB()
    cand = _candidate(linked_chat_username=None)
    sample: list[str] = []

    out = await discovery.check_liveness(db, cand, account_id=3, now=NOW,
                                         sample=sample)
    assert out == "pass"
    # посты канала у группы не считаются: «не измерялось», а не ноль
    assert cand.liveness_posts_7d is None
    assert cand.liveness_comments_7d == 60
    assert cand.liveness_checked_at == NOW
    assert cand.decision == "pending"
    # история заказана самой группой (`candidate.username`) и только ей
    assert [(a, p["username"]) for _, a, p in calls] == \
        [("get_chat_history", "buhchat")]
    # окно живости — параметр самого действия, как у канала
    assert all(p["min_date"] == (NOW - timedelta(days=7)).isoformat()
               for _, _, p in calls)
    # выборка для модели — тексты первой страницы группы
    assert sample == [f"сообщение {i}" for i in range(60)]


async def test_liveness_for_supergroup_rejects_below_comments_threshold(
        monkeypatch):
    calls: list = []

    def history(payload):
        return {"posts": [{"message_id": i, "text": "ок"} for i in range(49)]}

    _stub_engage(monkeypatch, calls, history=history)
    db = _FakeDB()
    cand = _candidate(linked_chat_username=None)

    out = await discovery.check_liveness(db, cand, account_id=3, now=NOW)
    assert out == "reject"
    assert cand.decided_by == "auto:liveness"
    assert "49" in cand.decision_reason and "порога 50" in cand.decision_reason
    assert cand.liveness_posts_7d is None
    assert cand.liveness_comments_7d == 49


async def test_liveness_for_supergroup_pages_history_by_cursor(monkeypatch):
    """Та же петля листания, что у группы обсуждения канала: курсор `max_id`
    сдвигается, пока порог не закрыт (поднят строкой `limits` до 150)."""
    calls: list = []

    def history(payload):
        if not payload.get("max_id"):
            return {"posts": [{"message_id": 2000 - i, "text": "страница 1"}
                              for i in range(100)]}
        return {"posts": [{"message_id": 1000 - i, "text": "страница 2"}
                          for i in range(60)]}

    _stub_engage(monkeypatch, calls, history=history)
    db = _FakeDB(limit_rows=[("discovery_min_comments_7d", Decimal(150))])
    cand = _candidate(linked_chat_username=None)

    assert await discovery.check_liveness(db, cand, account_id=3, now=NOW) == "pass"
    assert cand.liveness_comments_7d == 160
    assert [p.get("max_id") for _, _, p in calls] == [None, 1900]


# ── 3.f: прогон поиска целиком ────────────────────────────────────────────────

async def test_run_scan_search_counts_groups_found_and_new(monkeypatch):
    """Прогон `kind="search"` на живой форме ответа: найдено = `count` из
    ответа, новые кандидаты — записи с `title` (каналы и группы, с именем
    и без; запись без title считается найденной, но строки не заводит)."""
    channels = [C1, C2,
                {"peer_id": -1003, "username": "bank3", "title": "Банк 3",
                 "members": 7000, "type": "channel"}]
    chats = [G1, G2,
             {"peer_id": -2003, "username": None, "title": "Чат без имени",
              "members": 3000, "type": "supergroup"},
             {"peer_id": -2004, "username": "notitle", "members": 2000,
              "type": "supergroup"}]
    answer = {"found": True, "query": "бухгалтерия", "count": 7,
              "results": channels + chats,
              "channels": channels, "chats": chats}
    calls: list = []
    _stub_engage(monkeypatch, calls, search=answer)
    db = _FakeDB()
    monkeypatch.setattr(discovery, "get_session_maker", _Maker(db))
    notes: list = []

    async def report(pct, note):
        notes.append(note)

    out = await discovery.run_scan(
        11, params={"kind": "search", "query": "бухгалтерия", "account_id": 3},
        report=report, cancelled=lambda: False)

    assert out == {"found_total": 7, "new_total": 6}
    assert [(a, p) for _, a, p in calls] == \
        [("search_public_chats", {"query": "бухгалтерия"})]
    [q] = [o for o in db.added if isinstance(o, DiscoveryQuery)]
    assert q.kind == "search" and q.query == "бухгалтерия"
    assert q.run_id == 11 and q.found_total == 7 and q.new_total == 6
    assert any("найдено 7" in n for n in notes)
    inserted = [o for o in db.added if isinstance(o, ChannelCandidate)]
    assert len(inserted) == 6
    assert {c.username for c in inserted} == \
        {"bank", "bank2", "bank3", "buhchat", "buhclub", None}
