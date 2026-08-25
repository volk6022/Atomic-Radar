"""Вердикты и цели по нескольким сценариям — на настоящем Postgres.

Проверять это моками бессмысленно: половина правил живёт в схеме. Адресация цели
держится `CHECK`-ограничением, «одно сообщение — одна цель в сценарии» —
`UNIQUE`, а порядок удаления черновика и цели — внешним ключом `NOT NULL`. Мок
пропустит ровно то, из-за чего прогон упадёт.

Второй сценарий заводится здесь **публичным ответом на сообщение** (`message/reply/
public`), а не копией ЛС: смысл разделения в том, что у двух конвейеров разная
адресация, и на двух одинаковых сценариях это не проверяется никак.

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
"""
from __future__ import annotations

import os
from dataclasses import replace
from datetime import datetime, timezone

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db.models import (Base, Channel, EngageInstance, Lead, Message, WfDraft,
                           WfTarget, WfVerdict, Workflow)
from app.services import ingest, reclassify, targeting

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — половина правил живёт в схеме")

NOW = datetime(2026, 8, 25, 12, 0, tzinfo=timezone.utc)

# Текст, который проходит L0/L1 обоих профилей: есть и боль, и намерение её решить.
PASSING = "впн отваливается каждый день, ищу кто настроит нормально, готов платить"
# Слишком коротко — отсеивается на L0 у обоих.
FAILING = "ок"


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


async def seed_workflows(db) -> dict[str, Workflow]:
    """Два сценария разной формы. Профиль у обоих `dm_v1` — единственный, что есть в
    коде; проверяется здесь не различие правил отбора, а различие адресации."""
    instance = EngageInstance(key="default", client_label="Основной",
                              base_url="http://engage:8103",
                              api_key_env="RADAR_ENGAGE_API_KEY")
    db.add(instance)
    await db.flush()

    dm = Workflow(key="cold_dm", title="Личные сообщения", target_kind="user",
                  action="dm", visibility="private", engage_instance_id=instance.id,
                  engage_use_case="cold_dm", cascade_profile="dm_v1",
                  sort_order=10, is_active=True)
    public = Workflow(key="public_reply", title="Публичные ответы",
                      target_kind="message", action="reply", visibility="public",
                      engage_instance_id=instance.id, engage_use_case="public_reply",
                      cascade_profile="dm_v1", sort_order=5, is_active=True)
    db.add_all([dm, public])
    await db.commit()
    return {"cold_dm": dm, "public_reply": public}


async def seed_message(db, *, text_body=PASSING, author_peer_id=500,
                       tg_message_id=1000) -> tuple[Channel, Message]:
    channel = (await db.execute(select(Channel))).scalars().first()
    if channel is None:
        channel = Channel(peer_id=-1001, username="chat", title="Обсуждение")
        db.add(channel)
        await db.flush()

    message = Message(channel_id=channel.id, tg_message_id=tg_message_id, tg_date=NOW,
                      author_peer_id=author_peer_id, author_username="user",
                      author_name="Имя", author_is_bot=False,
                      is_automatic_forward=False, text=text_body, processed_at=NOW)
    db.add(message)
    await db.commit()
    return channel, message


async def sync(db, channel, message, **kw):
    bound = await targeting.bind_active(db)
    summary = await targeting.sync_message(
        db, bound, message=message, channel=channel,
        l2_enabled=kw.get("l2_enabled", False), l3_enabled=kw.get("l3_enabled", False),
        ranked=kw.get("ranked"), llm_by_prompt=kw.get("llm_by_prompt"), now=NOW)
    await db.commit()
    return summary


async def targets(db, key: str) -> list[WfTarget]:
    wf = (await db.execute(
        select(Workflow).where(Workflow.key == key))).scalar_one()
    return list((await db.execute(
        select(WfTarget).where(WfTarget.workflow_id == wf.id))).scalars().all())


# ── одно сообщение даёт цель в каждом сценарии ────────────────────────────────

async def test_one_message_becomes_a_target_in_every_workflow(db):
    """Ровно то, чего не умел прежний конвейер: у сообщения два независимых следствия."""
    await seed_workflows(db)
    channel, message = await seed_message(db)

    await sync(db, channel, message)

    assert len(await targets(db, "cold_dm")) == 1
    assert len(await targets(db, "public_reply")) == 1


async def test_addressing_differs_by_target_kind(db):
    """Адресация выводится из формы сценария, а не копируется между ними.

    В ЛС пишут человеку — заполнен адресат. В треде отвечают сообщению — заполнены
    чат и id сообщения, на которое отвечаем. Перепутать эти два набора значит
    отправить ответ не туда, и `ck_target_addressing` — единственное, что это ловит.
    """
    await seed_workflows(db)
    channel, message = await seed_message(db)
    await sync(db, channel, message)

    dm = (await targets(db, "cold_dm"))[0]
    assert dm.recipient_peer_id == 500
    assert dm.chat_peer_id is None and dm.reply_to_message_id is None

    public = (await targets(db, "public_reply"))[0]
    assert public.chat_peer_id == channel.peer_id
    assert public.reply_to_message_id == message.tg_message_id
    assert public.recipient_peer_id is None


async def test_verdict_is_written_for_every_pair(db):
    """Вердикт пишется и на отсеянное тоже: «не прошло» и «не считали» — разные факты,
    и экран потока обязан их различать."""
    await seed_workflows(db)
    channel, message = await seed_message(db, text_body=FAILING)
    await sync(db, channel, message)

    rows = list((await db.execute(select(WfVerdict))).scalars().all())
    assert len(rows) == 2
    assert all(v.passed is False for v in rows)
    assert not await targets(db, "cold_dm")
    assert not await targets(db, "public_reply")


# ── повторный приём ───────────────────────────────────────────────────────────

async def test_second_pass_updates_and_does_not_duplicate(db):
    """Бэкфилл догоняет вотчера постоянно. Вторая цель по тому же сообщению — это
    второй раз одному и тому же человеку, а `uq_target_wf_message` этого не даст."""
    await seed_workflows(db)
    channel, message = await seed_message(db)

    first = await sync(db, channel, message)
    second = await sync(db, channel, message)

    assert first["cold_dm"] == {"created": 1}
    assert second["cold_dm"] == {"updated": 1}
    assert (await db.execute(select(func.count(WfTarget.id)))).scalar_one() == 2
    assert (await db.execute(select(func.count()).select_from(WfVerdict))).scalar_one() == 2


# ── адресовать некому ─────────────────────────────────────────────────────────

async def test_post_without_author_gives_public_target_but_not_dm(db):
    """Пост анонимного админа: ответить в ветке можно, написать в личку — некому.

    Ради этого случая и заводилась развязка адресации. Прежняя схема требовала
    `author_peer_id NOT NULL` у лида, то есть такое сообщение теряла целиком.

    Публичный сценарий здесь идёт с ослабленным профилем, собранным прямо в тесте, и
    это не подгонка под зелёный прогон. В коде сегодня ровно один профиль — `dm_v1`,
    и он отсеивает пост без автора на L0 по `require_author`. Настоящий `public_v1`
    (пункт 10 в `STATE.md`) снимет это ограничение; до тех пор проверять развязку
    адресации можно только так — иначе и сам публичный конвейер проверять нечем.
    """
    await seed_workflows(db)
    channel, message = await seed_message(db, author_peer_id=None)

    bound = await targeting.bind_active(db)
    loosened = [b if b.workflow.key == "cold_dm"
                else targeting.Bound(b.workflow, replace(b.profile,
                                                         require_author=False))
                for b in bound]
    summary = await targeting.sync_message(
        db, loosened, message=message, channel=channel,
        l2_enabled=False, l3_enabled=False, now=NOW)
    await db.commit()

    # У ЛС профиль `dm_v1` отсеивает такое ещё на L0, поэтому до проверки адресации
    # дело не доходит — и это правильный отказ, а не случайный.
    assert summary["cold_dm"] == {"kept": 1}
    assert not await targets(db, "cold_dm")
    assert len(await targets(db, "public_reply")) == 1


async def test_unaddressable_is_named_separately_from_rejected(db):
    """«Писать некому» и «не прошло отбор» считаются по-разному.

    Свести их в один счётчик значило бы прятать поломку адресации за нормальным
    отсевом: цели перестали заводиться, а сводка выглядит как обычно.
    """
    await seed_workflows(db)
    channel, message = await seed_message(db, author_peer_id=None)
    # Профиль ЛС не должен отсеять сообщение раньше, чем дело дойдёт до адресации.
    dm = next(b for b in await targeting.bind_active(db) if b.workflow.key == "cold_dm")
    loose = targeting.Bound(dm.workflow, replace(dm.profile, require_author=False))

    summary = await targeting.sync_message(
        db, [loose], message=message, channel=channel,
        l2_enabled=False, l3_enabled=False, now=NOW)
    await db.commit()

    assert summary["cold_dm"] == {"unaddressable": 1}
    assert not await targets(db, "cold_dm")


# ── цель перестала проходить отбор ────────────────────────────────────────────

async def test_target_nobody_touched_is_removed(db):
    await seed_workflows(db)
    channel, message = await seed_message(db)
    await sync(db, channel, message)
    assert len(await targets(db, "cold_dm")) == 1

    message.text = FAILING
    await db.commit()
    summary = await sync(db, channel, message)

    assert summary["cold_dm"] == {"removed": 1}
    assert not await targets(db, "cold_dm")


async def test_target_with_a_decision_survives(db):
    """Решение человека задним числом не переписывается — ни статусом цели, ни
    удалением её из-под него."""
    await seed_workflows(db)
    channel, message = await seed_message(db)
    await sync(db, channel, message)

    target = (await targets(db, "cold_dm"))[0]
    target.status = "approved"
    await db.commit()

    message.text = FAILING
    await db.commit()
    summary = await sync(db, channel, message)

    assert summary["cold_dm"] == {"kept": 1}
    assert len(await targets(db, "cold_dm")) == 1


async def test_decided_draft_holds_the_target(db):
    """Разобранный черновик держит цель; неразобранный уходит вместе с ней.

    Порядок удаления проверяется здесь же: `wf_drafts.target_id` объявлен `NOT NULL`
    без `ondelete`, и удаление цели раньше черновика — нарушение ключа, откатывающее
    весь прогон. Ровно на этом когда-то падал `reclassify --scope all`.
    """
    await seed_workflows(db)
    channel, message = await seed_message(db)
    await sync(db, channel, message)
    wf = (await db.execute(
        select(Workflow).where(Workflow.key == "cold_dm"))).scalar_one()
    target = (await targets(db, "cold_dm"))[0]

    db.add(WfDraft(workflow_id=wf.id, target_id=target.id,
                   variants=[{"text": "заготовка"}], state="approved"))
    await db.commit()

    message.text = FAILING
    await db.commit()
    assert (await sync(db, channel, message))["cold_dm"] == {"kept": 1}
    assert len(await targets(db, "cold_dm")) == 1


async def test_pending_draft_leaves_with_the_target(db):
    await seed_workflows(db)
    channel, message = await seed_message(db)
    await sync(db, channel, message)
    wf = (await db.execute(
        select(Workflow).where(Workflow.key == "cold_dm"))).scalar_one()
    target = (await targets(db, "cold_dm"))[0]

    db.add(WfDraft(workflow_id=wf.id, target_id=target.id,
                   variants=[{"text": "заготовка"}], state="pending"))
    await db.commit()

    message.text = FAILING
    await db.commit()
    assert (await sync(db, channel, message))["cold_dm"] == {"removed": 1}
    assert not await targets(db, "cold_dm")
    assert (await db.execute(select(func.count(WfDraft.id)))).scalar_one() == 0


# ── «ещё в пути» ──────────────────────────────────────────────────────────────

async def test_undecided_message_neither_creates_nor_removes(db):
    """`passed is None` — «не досчитали», а не «не прошло».

    Записать по такому отказ значило бы потерять цель из-за того, что своя же машина
    с эмбеддингами была недоступна.
    """
    await seed_workflows(db)
    channel, message = await seed_message(db)
    await sync(db, channel, message)

    # L2 включён, вектора нет — каскад обязан вернуть «ожидает».
    summary = await sync(db, channel, message, l2_enabled=True)

    assert summary["cold_dm"] == {"kept": 1}
    assert len(await targets(db, "cold_dm")) == 1
    verdict = (await db.execute(
        select(WfVerdict).where(WfVerdict.message_id == message.id)
        .limit(1))).scalars().first()
    assert verdict.passed is None


# ── выключенный сценарий ──────────────────────────────────────────────────────

# ── приём: обе витрины из одного события ──────────────────────────────────────

async def test_incoming_message_fills_both_the_old_queue_and_the_new_targets(db):
    """Событие вотчера обязано наполнить и старую очередь лидов, и цели сценариев.

    Это и есть точка, ради которой всё писалось: пока экраны живут на `leads`, а
    новые конвейеры — на `wf_targets`, обе витрины должны наполняться из одного
    события. Проверять их порознь бессмысленно — сломается ровно связка.
    """
    await seed_workflows(db)
    db.add(Channel(peer_id=-1001, username="chat", title="Обсуждение"))
    await db.commit()

    result = await ingest.ingest_incoming_message(db, {
        "chat_id": -1001, "chat_username": "chat", "chat_title": "Обсуждение",
        "message_id": 7001, "date": NOW.isoformat(), "message": PASSING,
        "from_peer_id": 500, "sender_username": "user",
        "from_first_name": "Имя", "from_is_bot": False,
    })
    await db.commit()

    assert result["accepted"] == 1
    assert (await db.execute(select(func.count(Lead.id)))).scalar_one() == 1
    assert len(await targets(db, "cold_dm")) == 1
    assert len(await targets(db, "public_reply")) == 1


async def test_ingest_reports_what_each_workflow_got(db):
    """Сводка приёма разложена по сценариям.

    Одно число на всех прятало бы самый вероятный отказ: конвейер, который перестал
    давать цели, — общая сумма при этом почти не меняется.
    """
    await seed_workflows(db)
    db.add(Channel(peer_id=-1001, username="chat", title="Обсуждение"))
    await db.commit()

    result = await ingest.ingest_history(db, chat_id=-1001, chat_username="chat",
                                         chat_title="Обсуждение", posts=[
        {"message_id": 8001, "date": NOW.isoformat(), "text": PASSING,
         "from_user_id": 500, "from_username": "user", "from_first_name": "Имя"},
        {"message_id": 8002, "date": NOW.isoformat(), "text": FAILING,
         "from_user_id": 501, "from_username": "other", "from_first_name": "Другой"},
    ])
    await db.commit()

    assert result["accepted"] == 2
    assert result["workflows"]["cold_dm"] == {"created": 1, "kept": 1}
    assert result["workflows"]["public_reply"] == {"created": 1, "kept": 1}


# ── новый сценарий догоняет накопленное ───────────────────────────────────────

async def test_pending_scope_picks_up_messages_a_new_workflow_never_saw(db):
    """Дешёвый инкрементальный прогон обязан замечать сценарий, заведённый позже.

    «Недосчитано» раньше означало «пусто в `messages.cascade_*`». По этой мерке
    двенадцать тысяч накопленных сообщений досчитаны давно, и сценарий, заведённый
    сегодня, не получил бы по ним ни одного вердикта — ни сейчас, ни потом. Раздел
    второго конвейера остался бы пустым навсегда, и выглядело бы это как «никого не
    нашли», а не как поломка.
    """
    wfs = await seed_workflows(db)
    wfs["public_reply"].is_active = False
    await db.commit()

    channel, message = await seed_message(db)
    await reclassify.run(db, l2_enabled=False, l3_enabled=False, scope="all")
    assert len(await targets(db, "cold_dm")) == 1

    # Сценарий появляется после того, как всё уже посчитано.
    wfs["public_reply"].is_active = True
    await db.commit()

    summary = await reclassify.run(db, l2_enabled=False, l3_enabled=False,
                                   scope="pending")

    assert summary["messages"] == 1, "инкрементальный прогон не увидел сообщение"
    assert summary["workflows"]["public_reply"] == {"created": 1}
    assert len(await targets(db, "public_reply")) == 1


async def test_pending_scope_stays_cheap_when_everything_is_settled(db):
    """Обратная проверка: расширенная мерка не должна тянуть в прогон всё подряд.

    Без неё правка выше «работала» бы всегда — просто перебирая базу целиком каждый
    раз, и разницы с `scope=all` не осталось бы.
    """
    await seed_workflows(db)
    channel, message = await seed_message(db)
    await reclassify.run(db, l2_enabled=False, l3_enabled=False, scope="all")

    summary = await reclassify.run(db, l2_enabled=False, l3_enabled=False,
                                   scope="pending")
    assert summary["messages"] == 0


async def test_inactive_workflow_gets_neither_verdicts_nor_targets(db):
    """Выключенный сценарий перестаёт копить работу. Иначе «выключить» означало бы
    только «убрать из меню», а конвейер продолжал бы считать в стол."""
    wfs = await seed_workflows(db)
    wfs["public_reply"].is_active = False
    await db.commit()

    channel, message = await seed_message(db)
    await sync(db, channel, message)

    assert len(await targets(db, "cold_dm")) == 1
    assert not await targets(db, "public_reply")
    assert (await db.execute(select(func.count()).select_from(WfVerdict))).scalar_one() == 1
