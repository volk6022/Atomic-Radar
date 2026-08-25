"""Данные сценария по HTTP: поток, цели, черновики — на настоящем Postgres.

Проверять это на уровне сервиса недостаточно, и не из-за SQL. Смысл среза в том, что
**у двух конвейеров над одними и теми же сообщениями разные ответы**, а это свойство
видно только целиком: права, фильтр по сценарию, форма адресации и счётчики считаются
в разных местах, и разойтись они могут независимо.

Отдельная забота — 404 против пустого списка. Ручка, отвечающая пустотой на
несуществующий сценарий, читается как «данных пока нет», и опечатка в ключе живёт
неделями.

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_DEBUG", "true")

from app.core.config import get_settings  # noqa: E402
from app.core.security import SessionSigner  # noqa: E402
from app.db.models import (Base, Channel, EngageInstance, Message, User,  # noqa: E402
                           WfTarget, WfVerdict, Workflow)
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.main import create_app  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

NOW = datetime(2026, 8, 25, 12, 0, tzinfo=timezone.utc)


async def _seed() -> dict:
    """Два сценария разной формы над общим каналом и тремя сообщениями.

    Сообщения подобраны так, чтобы у каждого была своя роль:

    * `msg_both`   — цель в обоих сценариях, разной адресации;
    * `msg_public` — цель только у публичного (пост без автора: писать в личку некому);
    * `msg_none`   — вердикта нет ни у кого, «не считалось».
    """
    engine = create_async_engine(DB_URL, poolclass=None)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)

    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        instance = EngageInstance(key="default", client_label="Основной",
                                  base_url="http://engage:8103",
                                  api_key_env="RADAR_ENGAGE_API_KEY")
        db.add(instance)
        await db.flush()

        dm = Workflow(key="cold_dm", title="Личные сообщения", target_kind="user",
                      action="dm", visibility="private",
                      engage_instance_id=instance.id, engage_use_case="cold_dm",
                      cascade_profile="dm_v1", sort_order=10, is_active=True)
        public = Workflow(key="public_reply", title="Публичные ответы",
                          target_kind="message", action="reply", visibility="public",
                          engage_instance_id=instance.id,
                          engage_use_case="public_reply",
                          cascade_profile="public_v1", sort_order=5, is_active=True)
        db.add_all([dm, public])

        channel = Channel(peer_id=-1001, username="chat", title="Обсуждение")
        db.add(channel)
        await db.flush()

        def msg(tg_id, *, author=500, forward=False, body="впн отваливается, ищу кто настроит"):
            return Message(channel_id=channel.id, tg_message_id=tg_id, tg_date=NOW,
                           author_peer_id=author,
                           author_username="user" if author else None,
                           author_name="Имя" if author else None,
                           author_is_bot=False, is_automatic_forward=forward,
                           text=body, processed_at=NOW)

        both, only_public, neither = msg(1000), msg(1001, author=None, forward=True), msg(1002)
        db.add_all([both, only_public, neither])
        await db.flush()

        # Вердикты: у ЛС только по `both`, у публичного — по обоим первым.
        db.add_all([
            WfVerdict(workflow_id=dm.id, message_id=both.id, level=3, passed=True,
                      detail={"l3": "модель: похоже на живую проблему"},
                      pain="VPN не работает", score=70, score_breakdown=[],
                      disqualifiers=[], computed_at=NOW),
            WfVerdict(workflow_id=dm.id, message_id=only_public.id, level=0,
                      passed=False, detail={"l0": "автопересылка"}, pain=None,
                      score=0, score_breakdown=[], disqualifiers=[], computed_at=NOW),
            WfVerdict(workflow_id=public.id, message_id=both.id, level=3, passed=True,
                      detail={"l3": "модель: ответить по существу можно"},
                      pain="VPN не работает", score=65, score_breakdown=[],
                      disqualifiers=[], computed_at=NOW),
            WfVerdict(workflow_id=public.id, message_id=only_public.id, level=3,
                      passed=True, detail={"l3": "модель: вопрос по теме"},
                      pain="не может настроить сам", score=55, score_breakdown=[],
                      disqualifiers=[], computed_at=NOW),
            # Вердикт «в пути»: ступень включена, вход ещё не посчитан. Нужен, чтобы
            # «ждёт обработки» было чем отличить от «сценарий сюда не доходил» —
            # у `msg_none` в этом же сценарии вердикта нет вовсе.
            WfVerdict(workflow_id=public.id, message_id=neither.id, level=2,
                      passed=None, detail={}, pain=None, score=None,
                      score_breakdown=[], disqualifiers=[], computed_at=None),
        ])

        db.add_all([
            WfTarget(workflow_id=dm.id, target_kind="user", message_id=both.id,
                     channel_id=channel.id, recipient_peer_id=500,
                     author_peer_id=500, author_username="user", author_name="Имя",
                     pain="VPN не работает", quote=both.text, score=70,
                     score_breakdown=[], disqualifiers=[], status="new"),
            WfTarget(workflow_id=public.id, target_kind="message", message_id=both.id,
                     channel_id=channel.id, chat_peer_id=channel.peer_id,
                     reply_to_message_id=1000,
                     author_peer_id=500, author_username="user", author_name="Имя",
                     pain="VPN не работает", quote=both.text, score=65,
                     score_breakdown=[], disqualifiers=[], status="new"),
            WfTarget(workflow_id=public.id, target_kind="message",
                     message_id=only_public.id, channel_id=channel.id,
                     chat_peer_id=channel.peer_id, reply_to_message_id=1001,
                     pain="не может настроить сам", quote=only_public.text, score=55,
                     score_breakdown=[], disqualifiers=[], status="new"),
        ])

        users = {}
        for role in ("owner", "customer", "reviewer", "viewer"):
            u = User(email=f"{role}@local", name=role, initials=role[:2].upper(),
                     role=role, password_hash="!нельзя-войти", totp_secret="X" * 32,
                     totp_confirmed=True, is_active=True)
            db.add(u)
            users[role] = u
        await db.commit()
        out = {"uids": {r: u.id for r, u in users.items()},
               "msg_none_tg": neither.tg_message_id}

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

    app = create_app()
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c

    if previous is None:
        os.environ.pop("RADAR_DATABASE_URL", None)
    else:
        os.environ["RADAR_DATABASE_URL"] = previous
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_session_maker.cache_clear()


def _login(client, uid):
    token = SessionSigner(get_settings().SECRET_KEY).dumps({"uid": uid, "totp_ok": True})
    client.cookies.set(get_settings().SESSION_COOKIE, token)
    return client


@pytest.fixture
def authed(client, seeded):
    return _login(client, seeded["uids"]["owner"])


PATHS = ["/api/v1/workflows/cold_dm/stream", "/api/v1/workflows/cold_dm/targets",
         "/api/v1/workflows/cold_dm/drafts", "/api/v1/workflows/cold_dm/pains"]


# ── доступ ────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("path", PATHS)
def test_anonymous_gets_nothing(client, path):
    assert client.get(path).status_code == 401


@pytest.mark.parametrize("path", PATHS)
def test_guest_is_refused_where_the_registry_would_let_him_in(client, seeded, path):
    """Здесь данные, а не названия сценариев.

    Реестр `/api/v1/workflows` открыт любому вошедшему намеренно — из него рисуется
    меню. Эти же ручки обязаны спрашивать матрицу: иначе гость, которому поток и цели
    закрыты, получал бы их по адресу со сценарием в пути.
    """
    _login(client, seeded["uids"]["viewer"])
    assert client.get(path).status_code == 403


@pytest.mark.parametrize("role", ["owner", "customer", "reviewer"])
def test_staff_may_read(client, seeded, role):
    _login(client, seeded["uids"][role])
    assert client.get("/api/v1/workflows/cold_dm/targets").status_code == 200


@pytest.mark.parametrize("path", ["stream", "targets", "drafts", "pains"])
def test_unknown_workflow_is_404_not_an_empty_list(authed, path):
    """Пустой список читается как «данных пока нет», и опечатка в ключе живёт неделями."""
    r = authed.get(f"/api/v1/workflows/нет-такого/{path}")
    assert r.status_code == 404
    assert "не найден" in r.json()["detail"]


# ── цели ──────────────────────────────────────────────────────────────────────

def test_targets_are_scoped_to_the_workflow(authed):
    """Ровно то, ради чего ресурс параллельный: у двух конвейеров свои цели."""
    dm = authed.get("/api/v1/workflows/cold_dm/targets").json()
    public = authed.get("/api/v1/workflows/public_reply/targets").json()

    assert dm["total"] == 1
    assert public["total"] == 2


def test_addressing_shape_follows_target_kind(authed):
    """Колонка адресации выводится из оси сценария, а не додумывается экраном:
    «Кому» у личных сообщений, «Под каким сообщением» у публичного ответа."""
    dm = authed.get("/api/v1/workflows/cold_dm/targets").json()
    public = authed.get("/api/v1/workflows/public_reply/targets").json()

    assert dm["target_kind"] == "user"
    a = dm["rows"][0]["addressing"]
    assert a["kind"] == "user" and a["label"] == "Кому"
    assert a["recipient_peer_id"] == 500

    assert public["target_kind"] == "message"
    b = public["rows"][0]["addressing"]
    assert b["kind"] == "message" and b["label"] == "Под каким сообщением"
    assert b["reply_to_message_id"] is not None


def test_a_target_without_an_author_still_has_an_address(authed):
    """Пост анонимного админа: автора нет, а отвечать под ним есть куда. У ЛС такой
    цели быть не может — и её там нет."""
    rows = authed.get("/api/v1/workflows/public_reply/targets").json()["rows"]
    anon = [r for r in rows if r["author_username"] is None]
    assert len(anon) == 1
    assert anon[0]["addressing"]["reply_to_message_id"] == 1001


def test_state_counters_are_counted_within_the_workflow(authed):
    """Сводка по всем `wf_targets` показывала бы сумму по конвейерам, и «две новых»
    в блоке ЛС означало бы две где-то ещё."""
    dm = authed.get("/api/v1/workflows/cold_dm/targets").json()
    states = {s["key"]: s["count"] for s in dm["states"]}
    assert states["new"] == 1
    assert sum(states.values()) == 1


def test_targets_filter_and_sort(authed):
    assert authed.get(
        "/api/v1/workflows/public_reply/targets?min_score=60").json()["total"] == 1
    assert authed.get(
        "/api/v1/workflows/public_reply/targets?status=rejected").json()["total"] == 0
    r = authed.get("/api/v1/workflows/public_reply/targets?sort=score&order=asc").json()
    assert [x["score"] for x in r["rows"]] == [55, 65]


def test_unknown_status_is_rejected_not_ignored(authed):
    r = authed.get("/api/v1/workflows/cold_dm/targets?status=удалён")
    assert r.status_code == 422


def test_pains_come_from_this_workflow_only(authed):
    """Показывать в фильтре боль, которой в этом конвейере не было, значит предлагать
    заведомо пустой отбор."""
    dm = {p["pain"] for p in authed.get("/api/v1/workflows/cold_dm/pains").json()["rows"]}
    public = {p["pain"] for p in
              authed.get("/api/v1/workflows/public_reply/pains").json()["rows"]}
    assert dm == {"VPN не работает"}
    assert public == {"VPN не работает", "не может настроить сам"}


# ── поток ─────────────────────────────────────────────────────────────────────

def test_stream_shows_this_workflow_verdict_not_the_legacy_columns(authed):
    """Сообщение, отсеянное правилами ЛС, для публичного ответа — законная цель.
    Показывать в его потоке причины отбраковки по правилам личных сообщений значило бы
    отвечать не на тот вопрос, ради которого экран существует."""
    dm = {r["id"]: r for r in
          authed.get("/api/v1/workflows/cold_dm/stream").json()["rows"]}
    public = {r["id"]: r for r in
              authed.get("/api/v1/workflows/public_reply/stream").json()["rows"]}

    assert dm[1001]["cascade"]["l0"] is False
    assert "автопересылка" in dm[1001]["cascade_notes"]["l0"]

    assert public[1001]["cascade"]["l3"] is True
    assert "по теме" in public[1001]["cascade_notes"]["l3"]


def test_a_message_without_a_verdict_is_not_computed_rather_than_failed(authed, seeded):
    """Четвёртое состояние: строки вердикта нет вовсе. Сваливать его в «не прошло»
    значило бы спрятать сообщения, до которых сценарий ещё не дошёл."""
    rows = {r["id"]: r for r in
            authed.get("/api/v1/workflows/cold_dm/stream").json()["rows"]}
    none = rows[seeded["msg_none_tg"]]
    assert none["computed"] is False
    assert set(none["cascade"].values()) == {None}
    assert none["score"] is None


def test_stream_links_a_message_to_its_target(authed):
    rows = {r["id"]: r for r in
            authed.get("/api/v1/workflows/cold_dm/stream").json()["rows"]}
    assert rows[1000]["target_id"] is not None
    assert rows[1001]["target_id"] is None


def test_stream_filter_by_verdict(authed):
    passed = authed.get("/api/v1/workflows/public_reply/stream?passed=true").json()
    assert passed["total"] == 2
    rejected = authed.get("/api/v1/workflows/cold_dm/stream?passed=false").json()
    assert rejected["total"] == 1


def test_waiting_is_not_the_same_as_never_reached(authed, seeded):
    """Разница, ради которой заведено четвёртое состояние, — и место, где она стёрлась.

    После внешнего соединения у сообщения без вердикта все колонки `wf_verdicts`
    равны NULL, поэтому голое `passed IS NULL` ловит оба состояния разом. Пока фильтр
    был написан так, «ждёт обработки» показывал очередь вместе со всем нетронутым
    остатком — то есть отвечал «конвейер стоит» там, где конвейер просто не начинал.

    В контуре ЛС очереди нет ни одной, зато есть сообщение, до которого он не дошёл;
    в публичном — ровно наоборот. Один сценарий такую подмену не поймал бы.
    """
    dm_waiting = authed.get("/api/v1/workflows/cold_dm/stream?passed=pending").json()
    assert dm_waiting["total"] == 0

    dm_untouched = authed.get(
        "/api/v1/workflows/cold_dm/stream?passed=uncomputed").json()
    assert dm_untouched["total"] == 1
    assert dm_untouched["rows"][0]["id"] == seeded["msg_none_tg"]
    assert dm_untouched["rows"][0]["computed"] is False

    pub_waiting = authed.get(
        "/api/v1/workflows/public_reply/stream?passed=pending").json()
    assert pub_waiting["total"] == 1
    assert pub_waiting["rows"][0]["computed"] is True

    assert authed.get(
        "/api/v1/workflows/public_reply/stream?passed=uncomputed").json()["total"] == 0


def test_unknown_stream_filter_is_rejected_not_ignored(authed):
    r = authed.get("/api/v1/workflows/cold_dm/stream?passed=maybe")
    assert r.status_code == 422


# ── черновики ─────────────────────────────────────────────────────────────────

def test_drafts_queue_is_built_lazily_on_first_read(authed):
    """Ручка не только читает: целям без заготовки она их заводит. Так же устроен и
    старый экран черновиков."""
    first = authed.get("/api/v1/workflows/public_reply/drafts").json()
    assert first["created_now"] == 2
    assert first["total"] == 2

    second = authed.get("/api/v1/workflows/public_reply/drafts").json()
    assert second["created_now"] == 0
    assert second["total"] == 2


def test_drafts_are_scoped_to_the_workflow(authed):
    authed.get("/api/v1/workflows/public_reply/drafts")
    dm = authed.get("/api/v1/workflows/cold_dm/drafts").json()
    assert dm["total"] == 1
    assert dm["action"] == "dm"


def test_public_and_dm_drafts_differ_in_substance(authed):
    """Смысл среза 6, увиденный через ручку: над одним и тем же сообщением два
    конвейера предлагают разное, и публичный не называет контакт."""
    dm = authed.get("/api/v1/workflows/cold_dm/drafts").json()["rows"]
    public = authed.get("/api/v1/workflows/public_reply/drafts").json()["rows"]

    dm_texts = [v["text"] for v in dm[0]["variants"]]
    public_by_msg = {r["addressing"]["reply_to_message_id"]: r for r in public}
    public_texts = [v["text"] for v in public_by_msg[1000]["variants"]]

    assert set(dm_texts) & set(public_texts) == set()
    assert any("@vertsanov_biz" in t for t in dm_texts)
    assert not any("@vertsanov_biz" in t for t in public_texts)


def test_draft_by_id_carries_the_thread(authed):
    listed = authed.get("/api/v1/workflows/public_reply/drafts").json()["rows"][0]
    one = authed.get(
        f"/api/v1/workflows/public_reply/drafts/{listed['id']}").json()["draft"]
    assert one["thread"]
    assert one["action"] == "reply"
    assert one["addressing"]["kind"] == "message"


def test_a_draft_of_another_workflow_is_not_reachable_by_direct_link(authed):
    """Принадлежность проверяется в запросе, а не после выборки: иначе черновик чужого
    конвейера отдавался бы по прямой ссылке любому, кому открыт хоть один."""
    authed.get("/api/v1/workflows/public_reply/drafts")
    dm_draft = authed.get("/api/v1/workflows/cold_dm/drafts").json()["rows"][0]

    r = authed.get(f"/api/v1/workflows/public_reply/drafts/{dm_draft['id']}")
    assert r.status_code == 404


def test_reading_the_queue_moves_targets_into_review(authed):
    """Заготовка есть — значит цель дошла до человека, и повторный проход не должен
    считать её новой."""
    authed.get("/api/v1/workflows/public_reply/drafts")
    states = {s["key"]: s["count"] for s in
              authed.get("/api/v1/workflows/public_reply/targets").json()["states"]}
    assert states["new"] == 0
    assert states["in_review"] == 2


# ── курсор очереди ────────────────────────────────────────────────────────────

def test_next_is_a_route_and_not_a_draft_id(authed):
    """Литеральный путь после параметризованного перехватывается им и начинает
    отвечать «422, это не число». В старой очереди так уже уезжал `/reasons`."""
    r = authed.get("/api/v1/workflows/public_reply/drafts/next")
    assert r.status_code == 200
    assert "draft" in r.json()


def test_next_builds_the_queue_the_same_way_the_list_does(authed):
    """Курсор — второй вход в ту же очередь, и заготовки он обязан заводить так же.
    Иначе экран, открытый сразу карточкой, показал бы пустоту там, где список
    показывает работу."""
    first = authed.get("/api/v1/workflows/public_reply/drafts/next").json()
    assert first["remaining"] == 2
    assert first["draft"] is not None
    assert first["draft"]["action"] == "reply"


def test_next_walks_forward_and_wraps_around(authed):
    """Дойдя до конца, курсор заворачивается на начало — так же ведёт себя стрелка
    в старой очереди."""
    first = authed.get("/api/v1/workflows/public_reply/drafts/next").json()["draft"]
    second = authed.get(
        f"/api/v1/workflows/public_reply/drafts/next?after={first['id']}"
    ).json()["draft"]
    assert second["id"] != first["id"]

    wrapped = authed.get(
        f"/api/v1/workflows/public_reply/drafts/next?after={second['id']}"
    ).json()["draft"]
    assert wrapped["id"] == first["id"]


def test_next_counts_only_this_workflow(authed):
    """«Осталось» в блоке сценария — про этот блок. Сумма по всем конвейерам
    означала бы, что разбор чужой очереди уменьшает твою."""
    authed.get("/api/v1/workflows/public_reply/drafts")
    assert authed.get(
        "/api/v1/workflows/cold_dm/drafts/next").json()["remaining"] == 1


def test_next_of_another_workflow_never_leaks_across(authed):
    authed.get("/api/v1/workflows/public_reply/drafts")
    dm_ids = {r["id"] for r in
              authed.get("/api/v1/workflows/cold_dm/drafts").json()["rows"]}
    seen = set()
    after = None
    for _ in range(4):
        q = "" if after is None else f"?after={after}"
        d = authed.get(f"/api/v1/workflows/public_reply/drafts/next{q}").json()["draft"]
        seen.add(d["id"])
        after = d["id"]
    assert seen & dm_ids == set()


def test_next_returns_null_not_404_when_the_slice_is_empty(authed):
    """Разобранная очередь — нормальное состояние экрана, а не ошибка запроса."""
    r = authed.get("/api/v1/workflows/public_reply/drafts/next?state=approved")
    assert r.status_code == 200
    assert r.json()["draft"] is None
    assert r.json()["remaining"] == 0


def test_next_accepts_all_and_rejects_nonsense(authed):
    assert authed.get(
        "/api/v1/workflows/public_reply/drafts/next?state=all").status_code == 200
    assert authed.get(
        "/api/v1/workflows/public_reply/drafts/next?state=что-нибудь").status_code == 422


def test_next_of_an_unknown_workflow_is_404(authed):
    assert authed.get(
        "/api/v1/workflows/нет-такого/drafts/next").status_code == 404


def test_next_and_direct_link_describe_the_draft_identically(authed):
    """Одна форма на оба входа: иначе карточка, открытая из таблицы, отличалась бы
    от той же карточки, до которой дошли стрелкой."""
    cursor = authed.get("/api/v1/workflows/public_reply/drafts/next").json()["draft"]
    direct = authed.get(
        f"/api/v1/workflows/public_reply/drafts/{cursor['id']}").json()["draft"]
    assert cursor == direct
