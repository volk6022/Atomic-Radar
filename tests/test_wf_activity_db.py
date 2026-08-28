"""Активность сценария по HTTP — на настоящем Postgres.

Всё содержательное в этой ручке живёт в агрегатах, а агрегат ошибается тихо: он
возвращает число, и число выглядит правдоподобно независимо от того, по какому
множеству строк посчитано. Поэтому посев здесь устроен так, чтобы **каждая строка
существовала ради одной ошибки**, а не ради объёма:

* ручная запись в чужом сценарии — ловит потерянный `workflow_id` в отборе;
* запись без наводки (`message_id` пуст) — ловит внутреннее соединение вместо
  внешнего: такая строка обязана дойти до сводки без канала, а не пропасть;
* запись без `sent_at` — ловит сравнение окна по одному лишь `sent_at`: NULL не
  сравнивается ни с чем, и строка исчезла бы из окна целиком;
* запись сорокадневной давности — ловит окно, посчитанное не от `utcnow()`;
* одобренный черновик с доставленной попыткой — ловит `awaiting`, посчитанный по
  одному лишь состоянию.

`wf_outbound` тут заводится руками, и это единственное место, где так можно: в
контуре писателя журнала не существует, на стенде он пуст, и досеивать его туда
запрещено. Здесь же без пары доставленных попыток нечем проверить ни `totals.sent`,
ни `awaiting`, ни то, что лента сливает два журнала.

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_DEBUG", "true")

from app.core.config import get_settings  # noqa: E402
from app.core.security import SessionSigner  # noqa: E402
from app.db.models import (Base, Channel, EngageInstance, ManualSend,  # noqa: E402
                           Message, User, WfDraft, WfOutbound, WfTarget, Workflow)
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.main import create_app  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

ACTIVITY = "/api/v1/workflows/{key}/activity"

# Окно считается от настоящего `utcnow()`, поэтому и посев привязан к настоящему
# «сейчас», а не к застывшей дате: подсунуть 2026 год и спрашивать «за последние семь
# дней» значило бы проверять пустоту.
NOW = datetime.now(timezone.utc)
ACCOUNT = 12


async def _seed() -> dict:
    """Два сценария, два канала и пять ручных отправок с разными изъянами."""
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

        loud = Channel(peer_id=-1001, username="chat", title="Обсуждение")
        quiet = Channel(peer_id=-1002, username="auto", title="Автоматизация")
        db.add_all([loud, quiet])
        await db.flush()

        def msg(channel, tg_id):
            return Message(channel_id=channel.id, tg_message_id=tg_id, tg_date=NOW,
                           author_peer_id=500, author_username="user",
                           author_name="Имя", author_is_bot=False,
                           is_automatic_forward=False,
                           text="платёж за рубеж не проходит, ищу через кого оплатить",
                           processed_at=NOW)

        # Третье сообщение заведено не для полноты картины: у целей стоит уникальность
        # по паре (сценарий, сообщение), и два черновика одного сценария над одним
        # сообщением базой не принимаются.
        m_loud, m_quiet, m_spare = msg(loud, 1000), msg(quiet, 1001), msg(loud, 1002)
        db.add_all([m_loud, m_quiet, m_spare])
        await db.flush()

        def target(wf, message, channel, reply_to):
            return WfTarget(workflow_id=wf.id, target_kind="message",
                            message_id=message.id, channel_id=channel.id,
                            chat_peer_id=channel.peer_id, reply_to_message_id=reply_to,
                            author_peer_id=500, author_username="user",
                            author_name="Имя", pain="не может оплатить за рубеж",
                            quote=message.text, score=65, score_breakdown=[],
                            disqualifiers=[], status="approved")

        t_awaiting = target(public, m_loud, loud, 1000)
        t_delivered = target(public, m_quiet, quiet, 1001)
        t_pending = target(public, m_spare, loud, 1002)
        t_dm = WfTarget(workflow_id=dm.id, target_kind="user", message_id=m_loud.id,
                        channel_id=loud.id, recipient_peer_id=500, author_peer_id=500,
                        author_username="user", author_name="Имя",
                        pain="не может оплатить за рубеж", quote=m_loud.text, score=70,
                        score_breakdown=[], disqualifiers=[], status="approved")
        db.add_all([t_awaiting, t_delivered, t_pending, t_dm])
        await db.flush()

        def draft(wf, tgt, state):
            return WfDraft(workflow_id=wf.id, target_id=tgt.id,
                           variants=[{"text": "ответил по существу"}],
                           chosen_variant=0, final_text="ответил по существу",
                           state=state, prompt_version="test")

        d_awaiting = draft(public, t_awaiting, "approved")
        d_delivered = draft(public, t_delivered, "approved")
        d_pending = draft(public, t_pending, "pending")
        # Одобренный черновик чужого сценария: в `awaiting` публичного его быть не
        # должно, иначе число считается по состоянию, а не по сценарию и состоянию.
        d_dm = draft(dm, t_dm, "approved")
        db.add_all([d_awaiting, d_delivered, d_pending, d_dm])
        await db.flush()

        db.add_all([
            # В окне, канал «Обсуждение», текст слово в слово с подсказкой.
            ManualSend(workflow_id=public.id, target_id=t_awaiting.id,
                       draft_id=d_awaiting.id, message_id=m_loud.id,
                       engage_account_id=ACCOUNT, text="ответил по существу",
                       suggested_text="ответил по существу",
                       sent_at=NOW - timedelta(hours=1), recorded_by="owner@local"),
            # В окне, но не в сутках сегодняшнего дня — канал «Автоматизация».
            ManualSend(workflow_id=public.id, target_id=t_delivered.id,
                       draft_id=d_delivered.id, message_id=m_quiet.id,
                       engage_account_id=ACCOUNT, text="написал своими словами",
                       suggested_text="ответил по существу",
                       sent_at=NOW - timedelta(days=2, hours=1),
                       recorded_by="owner@local"),
            # Без наводки, без канала, без времени отправки и без аккаунта: строка,
            # которая проваливается сразу в четырёх местах, если что-то соединено
            # внутренним джойном или окно сравнивается только по `sent_at`.
            ManualSend(workflow_id=public.id, text="написал тому, кого Radar не нашёл",
                       recorded_by="owner@local"),
            # За окном недели, но внутри девяноста дней.
            ManualSend(workflow_id=public.id, target_id=t_awaiting.id,
                       message_id=m_loud.id, engage_account_id=ACCOUNT,
                       text="давняя переписка", sent_at=NOW - timedelta(days=40),
                       recorded_by="owner@local"),
            # Чужой сценарий: в публичном его не видно ни одним числом.
            ManualSend(workflow_id=dm.id, target_id=t_dm.id, message_id=m_loud.id,
                       engage_account_id=ACCOUNT, text="написал в личку",
                       sent_at=NOW - timedelta(hours=1), recorded_by="owner@local"),
        ])

        db.add_all([
            WfOutbound(workflow_id=public.id, target_id=t_delivered.id,
                       draft_id=d_delivered.id, engage_account_id=ACCOUNT,
                       chat_peer_id=quiet.peer_id, reply_to_message_id=1001,
                       allowed=True, reasons=[], mode="live",
                       delivered_message_id=999, text_snapshot="ответ, который дошёл",
                       created_at=NOW - timedelta(minutes=30)),
            WfOutbound(workflow_id=public.id, target_id=t_awaiting.id,
                       draft_id=d_awaiting.id, chat_peer_id=loud.peer_id,
                       reply_to_message_id=1000, allowed=False,
                       reasons=["тихие часы у получателя"], mode="dry_run",
                       text_snapshot="ответ, который не пустили",
                       created_at=NOW - timedelta(hours=2)),
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
               "channels": {"loud": loud.title, "quiet": quiet.title}}

    await engine.dispose()
    return out


@pytest.fixture
def seeded():
    """Посев в собственном цикле событий, полностью закрытый за собой — как в
    `test_wf_queues_db`: соединение asyncpg привязано к тому циклу, где создано."""
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


def _activity(client, key="public_reply", **params) -> dict:
    r = client.get(ACTIVITY.format(key=key), params=params)
    assert r.status_code == 200, r.text
    return r.json()


# ── доступ ────────────────────────────────────────────────────────────────────

def test_anonymous_gets_nothing(client):
    assert client.get(ACTIVITY.format(key="public_reply")).status_code == 401


def test_guest_is_refused(client, seeded):
    """Активность — внутренняя кухня, а не витрина. Открой её гостю, и он через блок
    публичного ответа увидел бы ровно то, что ему закрыто в блоке личных сообщений."""
    _login(client, seeded["uids"]["viewer"])
    assert client.get(ACTIVITY.format(key="public_reply")).status_code == 403


@pytest.mark.parametrize("role", ["owner", "customer", "reviewer"])
def test_staff_may_read(client, seeded, role):
    _login(client, seeded["uids"][role])
    assert client.get(ACTIVITY.format(key="public_reply")).status_code == 200


def test_unknown_workflow_is_404_not_an_empty_report(authed):
    """Пустой отчёт читается как «за неделю ничего не отправляли» — то есть опечатка в
    ключе выглядит как рабочая неделя без единой отправки."""
    r = authed.get(ACTIVITY.format(key="нет-такого"))
    assert r.status_code == 404
    assert "не найден" in r.json()["detail"]


# ── границы запроса ───────────────────────────────────────────────────────────

@pytest.mark.parametrize("days", [0, 91, -1])
def test_days_outside_the_bounds_is_refused(authed, days):
    assert authed.get(ACTIVITY.format(key="public_reply"),
                      params={"days": days}).status_code == 422


@pytest.mark.parametrize("days", [1, 90])
def test_days_on_the_bounds_is_accepted(authed, days):
    assert len(_activity(authed, days=days)["daily"]) == days


# ── разделение сценариев ──────────────────────────────────────────────────────

def test_a_send_in_one_workflow_does_not_leak_into_the_other(authed):
    """Ровно то, ради чего ручка адресуется ключом сценария: в личку написали один
    раз, и в публичном контуре этой записи быть не должно ни одним числом."""
    public = _activity(authed, "public_reply")
    dm = _activity(authed, "cold_dm")

    assert public["workflow"]["key"] == "public_reply"
    assert public["workflow"]["action"] == "reply"
    assert dm["totals"]["manual"] == 1
    assert public["totals"]["manual"] == 3

    dm_text = "написал в личку"
    assert any(r["text"] == dm_text for r in dm["recent"])
    assert not any(r["text"] == dm_text for r in public["recent"])


def test_awaiting_is_counted_within_the_workflow_and_by_delivery(authed):
    """Одобренных черновиков в публичном сценарии два, но по одному из них ответ уже
    висит под сообщением. Считать `awaiting` по одному состоянию значило бы вечно
    показывать «ждёт отправки» тому, что давно ушло."""
    assert _activity(authed, "public_reply")["totals"]["awaiting"] == 1
    assert _activity(authed, "cold_dm")["totals"]["awaiting"] == 1


def test_awaiting_does_not_move_with_the_window(authed):
    """Число про «сейчас», а не про период: одобренное месяц назад и не отправленное —
    ровно та проблема, которую оно обязано показать."""
    assert _activity(authed, days=1)["totals"]["awaiting"] == 1
    assert _activity(authed, days=90)["totals"]["awaiting"] == 1


# ── итоги ─────────────────────────────────────────────────────────────────────

def test_sent_is_summed_by_the_server(authed):
    """Сумму считает сервер: та же сложность, повторённая на экране, разъедется с этой
    в тот день, когда сюда добавится третий источник отправленного."""
    totals = _activity(authed)["totals"]
    assert totals["manual"] == 3
    assert totals["delivered"] == 1
    assert totals["sent"] == totals["manual"] + totals["delivered"] == 4


def test_a_blocked_attempt_is_not_counted_as_sent(authed):
    totals = _activity(authed)["totals"]
    assert totals["blocked"] == 1
    assert totals["sent"] == 4


def test_a_record_without_a_send_time_still_falls_into_the_window(authed):
    """Голое сравнение по `sent_at` выкинуло бы такую запись из окна целиком: NULL не
    сравнивается ни с чем. Заметно это было бы только по заниженному числу."""
    texts = {r["text"] for r in _activity(authed)["recent"]}
    assert "написал тому, кого Radar не нашёл" in texts


def test_the_window_narrows_the_totals_but_not_the_history(authed):
    """Сорокадневная запись — за окном недели и внутри девяноста дней. Ручка, считающая
    окно не от `utcnow()`, показала бы одно и то же число на оба запроса."""
    assert _activity(authed, days=7)["totals"]["manual"] == 3
    assert _activity(authed, days=90)["totals"]["manual"] == 4


# ── ряд по дням ───────────────────────────────────────────────────────────────

@pytest.mark.parametrize("days", [1, 7, 30])
def test_daily_has_exactly_days_rows_including_the_empty_ones(authed, days):
    """Дырок в ряду быть не должно: пустой день — это данные, а его отсутствие экран
    покажет как сдвинутый график."""
    daily = _activity(authed, days=days)["daily"]
    assert len(daily) == days
    assert len({row["date"] for row in daily}) == days
    assert [row["date"] for row in daily] == sorted(row["date"] for row in daily)


def test_daily_shows_the_days_when_nothing_happened(authed):
    """Отправляли в двух днях из семи; остальные пять обязаны присутствовать нулями."""
    daily = _activity(authed, days=7)["daily"]
    assert sum(1 for row in daily if row["manual"] == 0 and row["delivered"] == 0) >= 4
    assert sum(row["manual"] for row in daily) == 3
    assert sum(row["delivered"] for row in daily) == 1


@pytest.mark.parametrize("days", [1, 7, 30])
def test_the_daily_rows_add_up_to_the_totals(authed, days):
    """Столбики обязаны складываться в плитку — на этом держится доверие к экрану.

    Скользящее окно (`now - days`) этого не давало: оно начиналось в середине суток,
    которых в ряду уже нет, и сумма выходила меньше сводки. Оба числа при этом были
    честными, и именно поэтому расхождение было опасным — искать в нём ошибку негде.
    """
    report = _activity(authed, days=days)
    assert sum(row["manual"] for row in report["daily"]) == report["totals"]["manual"]
    assert (sum(row["delivered"] for row in report["daily"])
            == report["totals"]["delivered"])


def test_daily_ends_on_the_utc_today(authed):
    """Часовых поясов пользователя ручка не знает, и последняя строка ряда — сегодня по
    UTC. Ряд, кончающийся вчера, выглядит на графике совершенно нормально."""
    report = _activity(authed, days=7)
    assert report["daily"][-1]["date"] == report["window"]["until"][:10]


def test_the_window_is_reported_with_offsets(authed):
    window = _activity(authed, days=7)["window"]
    assert window["days"] == 7
    assert window["since"].endswith("+00:00") and window["until"].endswith("+00:00")
    assert window["since"] < window["until"]


# ── каналы и аккаунты ─────────────────────────────────────────────────────────

def test_a_row_without_a_channel_goes_last(authed, seeded):
    """«Неизвестно куда» — не самый тихий канал, и место ему в конце списка, а не в его
    середине по величине. Заодно проверяется, что строка вообще доехала: при внутреннем
    соединении она пропала бы из сводки без следа."""
    channels = _activity(authed)["channels"]
    assert channels[-1]["channel_id"] is None
    assert channels[-1]["title"] == "—"
    assert channels[-1]["manual"] == 1

    named = [c for c in channels if c["channel_id"] is not None]
    assert [c["title"] for c in named] == [seeded["channels"]["quiet"],
                                           seeded["channels"]["loud"]]
    assert named[0]["manual"] == 1 and named[0]["delivered"] == 1
    assert named[1]["manual"] == 1 and named[1]["delivered"] == 0


def test_accounts_carry_the_last_time_and_put_the_nameless_one_last(authed):
    """Имён аккаунтов у нас нет — только id из Engage; строка «аккаунт не указан»
    законна, потому что поле в форме необязательное."""
    accounts = _activity(authed)["accounts"]
    assert [a["engage_account_id"] for a in accounts] == [ACCOUNT, None]
    assert accounts[0]["manual"] == 2 and accounts[0]["delivered"] == 1
    assert accounts[0]["last_at"].endswith("+00:00")
    assert accounts[1]["manual"] == 1


# ── лента ─────────────────────────────────────────────────────────────────────

def test_recent_merges_both_journals_newest_first(authed):
    recent = _activity(authed)["recent"]
    assert {r["kind"] for r in recent} == {"manual", "outbound"}
    assert [r["at"] for r in recent] == sorted((r["at"] for r in recent), reverse=True)


def test_the_two_kinds_answer_different_questions(authed):
    """У ручной записи вопрос «совпало ли с подсказкой», у попытки — «почему не ушло».
    Общего поля под оба ответа нет намеренно: оно бы всегда врало для одного из них."""
    recent = _activity(authed)["recent"]
    manual = [r for r in recent if r["kind"] == "manual"]
    outbound = [r for r in recent if r["kind"] == "outbound"]

    assert all("matches_suggestion" in r and "allowed" not in r for r in manual)
    assert all("allowed" in r and "reasons" in r for r in outbound)

    verbatim = [r for r in manual if r["text"] == "ответил по существу"]
    assert verbatim and verbatim[0]["matches_suggestion"] is True
    reworded = [r for r in manual if r["text"] == "написал своими словами"]
    assert reworded and reworded[0]["matches_suggestion"] is False

    blocked = [r for r in outbound if r["allowed"] is False]
    assert blocked and blocked[0]["reasons"] == ["тихие часы у получателя"]
    assert blocked[0]["status"] == "заблокировано гейтом"


# ── состояние ступени ─────────────────────────────────────────────────────────

def test_the_missing_sender_is_reported_as_a_state(authed):
    """Половина чисел этой ручки — гарантированные нули, потому что автоматической
    отправки в контуре нет. Ручка обязана сказать это словами: молчаливый ноль экран
    нарисует как «за неделю не отправили ничего»."""
    sending = _activity(authed)["sending"]
    assert sending["automatic"] is False
    assert "отправитель не заведён" in sending["note"]
