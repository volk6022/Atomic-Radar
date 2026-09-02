"""`check_only` в разборе групп обсуждения: проверить связь, историю не трогать.

Разбор делает два дела одним движением: спрашивает карточки, записывает связь —
и тут же дочитывает историю найденной группы. Для карты «где группа вообще есть»
второе не нужно, а стоит оно дороже всего остального: страницы истории плюс каскад
на каждое сообщение.

Флаг проверяется по всем этажам цепочки, и все этажи — без базы:

* модель запроса (`ScanDiscussionsRequest`) и params прогона, который собирает
  ручка `scan_discussions`;
* исполнитель (`_job_discussions` читает `params["check_only"]` и передаёт его
  в `discussions.scan` — как он делает это с target и scope);
* сам разбор (`_scan_one`): при `check_only=True` — ни одного вызова
  `get_chat_history`, при `check_only=False` и вовсе без флага — чтение как раньше.

Сессии базы и Engage подменяются, поэтому тест проверяет ветвление, а не
пропускается за неимением Postgres (судьба соседних `*_db`-тестов).
"""
from __future__ import annotations

import os

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_INGEST_TOKEN", "test-ingest-token")

from app.api.v1 import ingest  # noqa: E402
from app.db.models import Channel  # noqa: E402
from app.services import discussions, engage, jobs  # noqa: E402

# Настоящая пара из прода (как в `test_discussions_scan_db.py`) плюс строка,
# которая сама и есть группа обсуждения (`type` = supergroup): у `_scan_one` два
# места чтения, и оба обязаны уважать флаг.
CHANNEL = {"found": True, "type": "channel", "peer_id": -100_1,
           "username": "corpostrovokru", "title": "Островок Командировки",
           "members_count": 12000, "linked_chat_username": "corpostrovokru_chat"}
GROUP = {"found": True, "type": "supergroup", "peer_id": -100_2,
         "username": "corpostrovokru_chat", "title": "Островок Командировки Chat",
         "members_count": 3400, "linked_chat_username": "corpostrovokru"}
OWN = {"found": True, "type": "supergroup", "peer_id": -100_3,
       "username": "flat_chat", "title": "Флэт Чат", "members_count": 800,
       "linked_chat_username": "corpostrovokru"}


def _posts(start: int, count: int) -> list[dict]:
    return [{"message_id": start - i, "date": "2026-09-01T09:51:00Z",
             "text": f"комментарий {start - i}", "from_user_id": 500 + i,
             "from_username": f"user{i}", "from_first_name": "Имя"}
            for i in range(count)]


def _stub_engage(monkeypatch, *, pages: dict, calls: list) -> None:
    """Подмена Engage той же двухшаговой формы, что и у настоящего и как в
    `test_discussions_scan_db`: `action` регистрирует задачу, результат отдаёт
    `wait_for_task`. Что позвано и с каким payload — записывается в `calls`."""
    tasks: dict[str, dict] = {}
    info_by_name = {"corpostrovokru": CHANNEL, "corpostrovokru_chat": GROUP,
                    "flat_chat": OWN}

    async def action(*, account_id, action, payload, webhook_url, **kw):
        calls.append((account_id, action, dict(payload)))
        task_id = f"t{len(tasks) + 1}"
        if action == "get_chat_info":
            tasks[task_id] = info_by_name.get(
                payload["username"], {"found": False, "reason": "username_not_found"})
        elif action == "get_chat_history":
            got = pages.setdefault(payload.get("username") or payload.get("peer_id"), [])
            tasks[task_id] = {"found": True, "posts": got.pop(0) if got else []}
        else:
            raise AssertionError(f"разбор не должен звать {action}")
        return {"task_id": task_id}

    async def wait_for_task(task_id, **kw):
        return tasks[task_id]

    monkeypatch.setattr(engage, "action", action)
    monkeypatch.setattr(engage, "wait_for_task", wait_for_task)


def _stub_ingest(monkeypatch, ingested: list) -> None:
    """`get_or_create_channel` отдаёт строку без базы, `ingest_history` только
    считает принятые сообщения: кто и когда его позвал — и есть предмет теста."""

    async def fake_get_or_create(db, *, peer_id, username, title):
        row = Channel(peer_id=peer_id, username=username, title=title,
                      ingest_enabled=True)
        row.id = 900
        return row

    async def fake_ingest_history(db, **kw):
        ingested.append(kw)
        return {"accepted": len(kw.get("posts") or [])}

    monkeypatch.setattr(discussions.ingest_service, "get_or_create_channel",
                        fake_get_or_create)
    monkeypatch.setattr(discussions.ingest_service, "ingest_history",
                        fake_ingest_history)


class _FakeResult:
    def __init__(self, value):
        self._value = value

    def scalar_one(self):
        return self._value

    def scalar_one_or_none(self):
        return None


class _FakeSession:
    """Ровно те обращения к сессии, которые делает `_scan_one`: `get` канала,
    подсчёт сообщений группы, коммит. Базы за этим нет."""

    def __init__(self, rows: dict[int, Channel]):
        self._rows = rows

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, model, pk):
        return self._rows.get(pk)

    async def execute(self, q):
        return _FakeResult(0)               # сообщений в группе пока ноль

    async def commit(self):
        pass

    def add(self, obj):
        pass


class _FakeMaker:
    def __init__(self, rows: dict[int, Channel]):
        self._rows = rows

    def __call__(self):
        return _FakeSession(self._rows)


def _fake_session_maker(rows: dict[int, Channel]):
    """Стоит на месте `get_session_maker`: вызов отдаёт «фабрику сессий», второй
    вызов — сессию. Настоящий устроен ровно так же (`get_session_maker()()`)."""
    return _FakeMaker(rows)


def _rows() -> dict[int, Channel]:
    channel = Channel(peer_id=CHANNEL["peer_id"], username="corpostrovokru",
                      title="Островок Командировки", ingest_enabled=True)
    channel.id = 41
    own = Channel(peer_id=OWN["peer_id"], username="flat_chat",
                  title="Флэт Чат", ingest_enabled=True)
    own.id = 42
    return {channel.id: channel, own.id: own}


async def _run_scan(monkeypatch, *, pages: dict, ingested: list,
                    check_only: bool | None = None):
    """Прогон `discussions.scan` по двум каналам: связанная пара и группа сама
    по себе. `check_only=None` значит «флаг не передавать вовсе» — так прогон
    звали до появления поля, и так должно оставаться читаемым."""
    rows = _rows()
    calls: list = []
    _stub_engage(monkeypatch, pages=pages, calls=calls)
    _stub_ingest(monkeypatch, ingested)
    monkeypatch.setattr(discussions, "get_session_maker",
                        lambda: _fake_session_maker(rows))

    notes: list = []

    async def report(pct, note):
        notes.append(note)

    kwargs = {} if check_only is None else {"check_only": check_only}
    stats = await discussions.scan(channel_ids=list(rows), account_ids=[1],
                                   target=40, report=report,
                                   cancelled=lambda: False, **kwargs)
    return stats, calls, rows, notes


async def test_check_only_reads_no_history(monkeypatch):
    """`check_only=True`: карточки спрошены, связь записана, чтения не было."""
    ingested: list = []
    stats, calls, rows, notes = await _run_scan(monkeypatch, pages={},
                                                ingested=ingested, check_only=True)

    assert [c for c in calls if c[1] == "get_chat_history"] == []
    assert ingested == []
    assert stats["messages"] == 0
    assert stats["done"] == stats["total"] == 2
    assert stats["groups_linked"] == 2

    # Проверка при этом состоялась: спрошены обе карточки (канала и его группы),
    # связь записана — из «unknown» канал вышел.
    assert len([c for c in calls if c[1] == "get_chat_info"]) == 3
    assert rows[41].linked_chat_username == "corpostrovokru_chat"
    assert rows[41].linked_checked_at is not None
    assert rows[42].chat_type == "supergroup"

    # И заметка оператору не сообщает, будто что-то прочитано.
    assert not any("прочитано" in n for n in notes)


async def test_without_flag_history_is_read_like_before(monkeypatch):
    """`check_only=False` и вовсе опущенный флаг — прежнее поведение: найденная
    группа дочитывается до target, обе ветки `_scan_one`."""

    for check_only in (False, None):
        ingested: list = []
        pages = {"corpostrovokru_chat": [_posts(2000, 40)],
                 "flat_chat": [_posts(3000, 40)]}
        stats, calls, _, _ = await _run_scan(
            monkeypatch, pages=pages, ingested=ingested, check_only=check_only)

        history = [c for c in calls if c[1] == "get_chat_history"]
        assert len(history) == 2, check_only
        assert sorted(c[2]["username"] for c in history) == \
            ["corpostrovokru_chat", "flat_chat"], check_only
        assert stats["messages"] == 80, check_only
        assert len(ingested) == 2, check_only
        assert stats["groups_linked"] == 2, check_only


async def test_scan_one_marks_checked_only(monkeypatch):
    """Результат `_scan_one` при `check_only=True` помечен `checked_only`, и
    `read` в нём ноль именно потому, что не читали, а не потому, что группа пуста."""
    rows = _rows()
    ingested: list = []
    calls: list = []
    _stub_engage(monkeypatch, pages={}, calls=calls)
    _stub_ingest(monkeypatch, ingested)

    async with _FakeMaker(rows)() as db:
        out = await discussions._scan_one(db, 41, 1, target=40,
                                          cancelled=lambda: False, check_only=True)
    assert out == {"group_id": 900, "linked": "corpostrovokru_chat", "read": 0,
                   "already_had": 0, "checked_only": True}

    async with _FakeMaker(rows)() as db:
        own = await discussions._scan_one(db, 42, 1, target=40,
                                          cancelled=lambda: False, check_only=True)
    assert own == {"group_id": 42, "own_group": True, "checked_only": True}
    assert [c for c in calls if c[1] == "get_chat_history"] == []
    assert ingested == []


class _Recorder:
    def __init__(self):
        self.kwargs = None

    async def __call__(self, **kw):
        self.kwargs = kw
        return {"total": 1, "done": 1, "cancelled": False}


async def test_runner_passes_flag_from_params_to_scan(monkeypatch):
    """Исполнитель обязан донести `params["check_only"]` до `discussions.scan` —
    тем же движением, каким он доносит туда target и scope."""
    rec = _Recorder()

    async def fake_select(db, *, scope, channel_ids):
        return list(channel_ids or [])

    async def fake_accounts():
        return [{"account_id": 9, "status": "active"}]

    monkeypatch.setattr(jobs, "get_session_maker",
                        lambda: _fake_session_maker({}))
    monkeypatch.setattr(discussions, "select_channels", fake_select)
    monkeypatch.setattr(engage, "list_accounts", fake_accounts)
    monkeypatch.setattr(discussions, "scan", rec)

    await jobs._job_discussions(1, {"scope": "ids", "channel_ids": [42],
                                    "check_only": True})
    assert rec.kwargs["check_only"] is True

    rec.kwargs = None
    await jobs._job_discussions(2, {"scope": "ids", "channel_ids": [42]})
    assert rec.kwargs["check_only"] is False, \
        "без флага в params прогон обязан читать, как раньше"


class _Client:
    host = "127.0.0.1"


class _Request:
    client = _Client()


class _User:
    id = 1
    email = "op@example.com"


class _DB:
    def add(self, obj):
        pass

    async def commit(self):
        pass


async def test_endpoint_put_check_only_into_run_params(monkeypatch):
    """Ручка обязана класть флаг в params прогона: params — единственный мост
    от HTTP-запроса к исполнителю, мимо него флаг не доедет вовсе."""
    captured: dict = {}

    class _Run:
        id = 77

    async def fake_start(db, *, kind, params, name, user_email):
        captured.update(kind=kind, params=params, name=name)
        return _Run()

    monkeypatch.setattr(ingest.jobs, "start", fake_start)

    out = await ingest.scan_discussions(
        ingest.ScanDiscussionsRequest(scope="ids", channel_ids=[5], check_only=True),
        _Request(), _DB(), _User())
    assert out["started"] is True and out["run_id"] == 77
    assert captured["kind"] == "discussions"
    assert captured["params"]["check_only"] is True
    assert captured["params"]["channel_ids"] == [5]
    assert captured["params"]["scope"] == "ids"
    assert captured["params"]["target"] == ingest.PAGE_LIMIT

    # По умолчанию флаг едет в params явным False, а не отсутствует.
    await ingest.scan_discussions(
        ingest.ScanDiscussionsRequest(scope="ids", channel_ids=[5]),
        _Request(), _DB(), _User())
    assert captured["params"]["check_only"] is False


def test_request_model_defaults_to_false():
    assert ingest.ScanDiscussionsRequest().check_only is False
    assert ingest.ScanDiscussionsRequest(check_only=True).check_only is True
    assert ingest.ScanDiscussionsRequest(check_only=False).check_only is False
