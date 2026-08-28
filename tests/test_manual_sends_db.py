"""Запись ручных отправок на настоящем Postgres.

Проверять это подделками нечем: половина смысла формы — в связях. Наводка обязана
принадлежать тому же контуру, снимок предложенного берётся из черновика той же
наводки, история фильтруется по контуру. Всё это живёт во внешних ключах и в
запросах, а не в коде, который можно вызвать в вакууме.

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db.models import (Base, Channel, EngageInstance, ManualSend, Message,
                           WfDraft, WfTarget, Workflow)
from app.services import manual_sends

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

NOW = datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
async def db():
    engine = create_async_engine(DB_URL, poolclass=None)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session:
        yield session
    await engine.dispose()


async def seed(db):
    """Два контура, чтобы было чему принадлежать не тому: личные сообщения и
    публичные ответы, у каждого своя наводка на своё сообщение."""
    instance = EngageInstance(key="default", client_label="Основной",
                              base_url="http://engage:8103",
                              api_key_env="RADAR_ENGAGE_API_KEY")
    db.add(instance)
    channel = Channel(peer_id=-1001, username="ch", title="Канал про ВЭД")
    db.add(channel)
    await db.flush()

    dm = Workflow(key="cold_dm", title="Личные сообщения", target_kind="user",
                  action="dm", visibility="private", engage_instance_id=instance.id,
                  engage_use_case="cold_dm", cascade_profile="dm_v1", sort_order=10)
    public = Workflow(key="public_reply", title="Публичные ответы",
                      target_kind="message", action="reply", visibility="public",
                      engage_instance_id=instance.id, engage_use_case="service_testing",
                      cascade_profile="dm_v1", sort_order=20)
    db.add_all([dm, public])

    messages = []
    for i in range(3):
        m = Message(channel_id=channel.id, tg_message_id=1000 + i,
                    tg_date=NOW - timedelta(hours=i), author_peer_id=500 + i,
                    author_username=f"user{i}", author_name=f"Имя {i}",
                    author_is_bot=False, is_automatic_forward=False,
                    text=f"не могу оплатить инвойс, помогите разобраться {i}")
        messages.append(m)
        db.add(m)
    await db.flush()

    dm_target = WfTarget(
        workflow_id=dm.id, target_kind="user", message_id=messages[0].id,
        channel_id=channel.id, recipient_peer_id=500, author_peer_id=500,
        author_username="user0", author_name="Имя 0", pain="не может оплатить за рубеж",
        quote="не могу оплатить инвойс, помогите разобраться 0", score=55, status="new")
    other_dm_target = WfTarget(
        workflow_id=dm.id, target_kind="user", message_id=messages[1].id,
        channel_id=channel.id, recipient_peer_id=501, author_peer_id=501,
        author_username="user1", author_name="Пётр Валютов", pain="выплаты людям за границей",
        quote="банк вернул платёж", score=40, status="new")
    public_target = WfTarget(
        workflow_id=public.id, target_kind="message", message_id=messages[2].id,
        channel_id=channel.id, chat_peer_id=-1002, reply_to_message_id=1002,
        author_username="user2", author_name="Имя 2", quote="что там с оплатой за рубеж",
        score=30, status="new")
    db.add_all([dm_target, other_dm_target, public_target])
    await db.flush()

    db.add(WfDraft(workflow_id=dm.id, target_id=dm_target.id,
                   variants=[{"text": "первый вариант"}, {"text": "второй вариант"}],
                   chosen_variant=1, state="pending", prompt_version="template-v0"))
    await db.commit()
    return {"dm": dm, "public": public, "target": dm_target,
            "other_target": other_dm_target, "public_target": public_target,
            "messages": messages, "channel": channel}


# ── запись ────────────────────────────────────────────────────────────────────

async def test_recording_against_a_target_fills_everything_from_the_server(db):
    """Форма присылает выбор наводки и свой текст. Всё остальное — из базы: иначе
    пара «предложено → отправлено» перестаёт быть свидетельством."""
    s = await seed(db)
    entry = await manual_sends.record(
        db, workflow=s["dm"], text="  привет, видел твой вопрос про оплату  ",
        recorded_by="andrey@example.com", target_id=s["target"].id)
    await db.commit()

    assert entry.text == "привет, видел твой вопрос про оплату", "текст обязан быть обрезан"
    assert entry.target_id == s["target"].id
    assert entry.message_id == s["messages"][0].id
    assert entry.draft_id is not None
    # Выбран был второй вариант — снимок должен быть именно им, а не первым.
    assert entry.suggested_text == "второй вариант"
    assert entry.recorded_by == "andrey@example.com"


async def test_recording_without_a_target_is_allowed(db):
    """Андрей мог написать тому, кого Radar не находил. Отказаться принять это значит
    потерять данные совсем — а именно они и показывают, что каскад пропускает."""
    s = await seed(db)
    entry = await manual_sends.record(
        db, workflow=s["dm"], text="написал человеку из другого чата",
        recorded_by="andrey@example.com")
    await db.commit()

    assert entry.target_id is None
    assert entry.draft_id is None
    assert entry.message_id is None
    assert entry.suggested_text is None


async def test_a_target_from_another_workflow_is_refused(db):
    """Связать ответ одного контура с предложением другого — это порча данных,
    которую обнаружат тогда, когда по ним уже начнут считать."""
    s = await seed(db)
    with pytest.raises(manual_sends.ManualSendError, match="другому workflow"):
        await manual_sends.record(db, workflow=s["dm"], text="текст",
                                  recorded_by="a@b.c",
                                  target_id=s["public_target"].id)


async def test_a_missing_target_is_refused(db):
    s = await seed(db)
    with pytest.raises(manual_sends.ManualSendError, match="не найдена"):
        await manual_sends.record(db, workflow=s["dm"], text="текст",
                                  recorded_by="a@b.c", target_id=999999)


async def test_a_target_without_a_draft_records_no_suggestion(db):
    """Наводка есть, черновика по ней нет. Снимок должен быть пустым, а не выдуманным
    из соседнего черновика — иначе сравнение окажется с чужим текстом."""
    s = await seed(db)
    entry = await manual_sends.record(
        db, workflow=s["dm"], text="написал сам", recorded_by="a@b.c",
        target_id=s["other_target"].id)
    await db.commit()

    assert entry.target_id == s["other_target"].id
    assert entry.draft_id is None
    assert entry.suggested_text is None


async def test_the_database_stamps_the_recording_time(db):
    """`recorded_at` ставит база. Проверяем, что значение доезжает до объекта: без
    этого экран после сохранения показывал бы пустое поле."""
    s = await seed(db)
    entry = await manual_sends.record(db, workflow=s["dm"], text="т",
                                      recorded_by="a@b.c")
    await db.commit()
    await db.refresh(entry)
    assert entry.recorded_at is not None


# ── наводки для формы ─────────────────────────────────────────────────────────

async def test_candidates_are_limited_to_the_workflow(db):
    s = await seed(db)
    rows = await manual_sends.candidates(db, workflow=s["dm"])
    assert {r["target_id"] for r in rows} == {s["target"].id, s["other_target"].id}

    public_rows = await manual_sends.candidates(db, workflow=s["public"])
    assert [r["target_id"] for r in public_rows] == [s["public_target"].id]


async def test_candidates_carry_what_radar_suggested(db):
    """Текст показывается рядом с полем ввода — ради этого форма и делается: человек
    видит заготовку и пишет своё, а мы получаем пару."""
    s = await seed(db)
    rows = {r["target_id"]: r for r in await manual_sends.candidates(db, workflow=s["dm"])}
    assert rows[s["target"].id]["suggested_text"] == "второй вариант"
    assert rows[s["other_target"].id]["suggested_text"] is None


async def test_candidates_are_searchable_by_person_and_by_words(db):
    """Человек помнит либо кому писал, либо про что было — заранее неизвестно, что."""
    s = await seed(db)
    by_name = await manual_sends.candidates(db, workflow=s["dm"], q="валютов")
    assert [r["target_id"] for r in by_name] == [s["other_target"].id]

    by_quote = await manual_sends.candidates(db, workflow=s["dm"], q="вернул")
    assert [r["target_id"] for r in by_quote] == [s["other_target"].id]

    by_username = await manual_sends.candidates(db, workflow=s["dm"], q="user0")
    assert [r["target_id"] for r in by_username] == [s["target"].id]


async def test_candidates_are_sorted_by_recency_not_by_score(db):
    """Человек ищет то, что делал сегодня. Список «самых качественных за всё время»
    ему в этом не помощник, хотя оценка у наводок и разная."""
    s = await seed(db)
    rows = await manual_sends.candidates(db, workflow=s["dm"])
    assert [r["target_id"] for r in rows] == [s["other_target"].id, s["target"].id]
    assert rows[0]["score"] < rows[1]["score"], "иначе тест ничего не доказывает"


async def test_candidate_limit_is_clamped(db):
    """Ограничение приходит из запроса. Без потолка один запрос вытащил бы всю базу."""
    s = await seed(db)
    assert len(await manual_sends.candidates(db, workflow=s["dm"], limit=0)) == 1
    assert len(await manual_sends.candidates(db, workflow=s["dm"], limit=10_000)) == 2


# ── история ───────────────────────────────────────────────────────────────────

async def test_history_pairs_what_was_suggested_with_what_was_sent(db):
    s = await seed(db)
    await manual_sends.record(db, workflow=s["dm"], text="второй вариант",
                              recorded_by="a@b.c", target_id=s["target"].id)
    await manual_sends.record(db, workflow=s["dm"], text="написал по-своему",
                              recorded_by="a@b.c", target_id=s["other_target"].id)
    await db.commit()

    out = await manual_sends.history(db, workflow_id=s["dm"].id)
    assert out["total"] == 2
    by_text = {r["text"]: r for r in out["rows"]}
    assert by_text["второй вариант"]["matches_suggestion"] is True
    assert by_text["второй вариант"]["author_name"] == "Имя 0"
    assert by_text["написал по-своему"]["matches_suggestion"] is False


async def test_history_can_be_filtered_by_workflow(db):
    s = await seed(db)
    await manual_sends.record(db, workflow=s["dm"], text="в лс",
                              recorded_by="a@b.c", target_id=s["target"].id)
    await manual_sends.record(db, workflow=s["public"], text="в тред",
                              recorded_by="a@b.c", target_id=s["public_target"].id)
    await db.commit()

    assert (await manual_sends.history(db))["total"] == 2
    only_public = await manual_sends.history(db, workflow_id=s["public"].id)
    assert only_public["total"] == 1
    assert only_public["rows"][0]["text"] == "в тред"


async def test_history_reports_the_limit_it_actually_applied(db):
    """Экран листает по тому, что ему ответили. Вернув присланный `limit` вместо
    применённого, ручка заставит его считать, что строк было больше, чем есть."""
    s = await seed(db)
    await manual_sends.record(db, workflow=s["dm"], text="раз", recorded_by="a@b.c")
    await db.commit()

    out = await manual_sends.history(db, limit=10_000, offset=-5)
    assert out["limit"] == manual_sends.HISTORY_LIMIT
    assert out["offset"] == 0


async def test_history_shows_records_without_a_target(db):
    """Записи без наводки — самые интересные: это то, что каскад не нашёл. Выпасть
    из списка они не должны, а при внутреннем соединении именно это бы и случилось."""
    s = await seed(db)
    await manual_sends.record(db, workflow=s["dm"], text="кому-то мимо радара",
                              recorded_by="a@b.c")
    await db.commit()

    out = await manual_sends.history(db, workflow_id=s["dm"].id)
    assert out["total"] == 1
    assert out["rows"][0]["text"] == "кому-то мимо радара"
    assert out["rows"][0]["target_id"] is None


async def test_history_names_the_channel_and_leaves_it_empty_without_a_target(db):
    """Канал — колонка экрана, и добирать его построчно значило бы N+1 запрос.

    У записи без наводки канала нет: подставить туда что-нибудь ради непустой ячейки
    значило бы придумать источник, которого не было.
    """
    s = await seed(db)
    await manual_sends.record(db, workflow=s["dm"], text="по наводке",
                              recorded_by="a@b.c", target_id=s["target"].id)
    await manual_sends.record(db, workflow=s["dm"], text="мимо радара",
                              recorded_by="a@b.c")
    await db.commit()

    by_text = {r["text"]: r for r in
               (await manual_sends.history(db, workflow_id=s["dm"].id))["rows"]}
    assert by_text["по наводке"]["channel"] == "Канал про ВЭД"
    assert by_text["мимо радара"]["channel"] is None


async def test_correcting_a_record_survives_a_reread(db):
    s = await seed(db)
    entry = await manual_sends.record(db, workflow=s["dm"], text="с опечаткай",
                                      recorded_by="a@b.c", target_id=s["target"].id)
    await db.commit()

    assert manual_sends.correct(entry, {"text": "без опечатки"}) == ["text"]
    await db.commit()

    stored = (await db.execute(
        select(ManualSend).where(ManualSend.id == entry.id))).scalar_one()
    assert stored.text == "без опечатки"
    # Снимок предложенного правкой не тронут — он и есть свидетельство.
    assert stored.suggested_text == "второй вариант"
    assert (await db.execute(select(func.count(ManualSend.id)))).scalar_one() == 1
