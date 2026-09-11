"""Адресный обход L1 у вызывающих: приём и прогон — на настоящем Postgres.

Ядро каскада (`app/core/cascade.py`) — чистая функция, его ветки проверены без базы.
Здесь проверяется то, что моками не доказать: флаг `channels.l1_bypass_enabled`
**доезжает** до каскада по обоим путям — приём (`upsert_message`) и прогон
(`reclassify.run`) — и до второго конвейера (`wf_verdicts`), плюс потолок
`l3_limit` откладывает, а не убивает обходные сообщения (T7, T9, T14 из
`_TESTS-cascade.md`).

Заглушки стоят ровно на границе сервисов: эмбеддер всегда отдаёт одну и ту же
картину близости, модель всегда соглашается. Проверять здесь сами правила
близости незачем — у них свои тесты без базы.

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db.models import Base, Channel, EngageInstance, Message, WfVerdict, Workflow
from app.services import embeddings, ingest, llm, reclassify, targeting

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

NOW = datetime(2026, 9, 12, 12, 0, tzinfo=timezone.utc)

# Длинно, без якорей боли: при выключенном обходе умирает на L1, при включённом —
# ждёт вектора (граница длины и правило близости проверены в тестах ядра).
LONG_ANCHORLESS = "а" * 250

# Картина близости от заглушки эмбеддера: верхний эталон — боль 0.60 (выше
# порога 0.57), шум ниже. Для обходных решает близость, для якорных — отрыв.
RANKED_POS = [("pos", "банк не пропускает платёж", 0.60), ("neg", "офтоп", 0.40)]

# Ответ модели: «настоящая проблема автора» — L3 пропускает.
LLM_YES = {"real_problem": True, "is_seller": False,
           "answering_someone_else": False, "why": "…"}


def stub_stages(monkeypatch, ranked=RANKED_POS):
    """Подменить дорогие ступени на границе сервисов.

    `embed`/`rank` согласованы: на любой вектор — одна и та же картина близости.
    `llm.verdict` возвращает пару (ответ, trace), как настоящий клиент.
    """

    async def fake_prototype_vectors():
        return []

    async def fake_embed(texts):
        return [[0.0] for _ in texts]

    async def fake_verdict(*, text, context, prompt_key):
        return dict(LLM_YES), None

    monkeypatch.setattr(embeddings, "enabled", lambda: True)
    monkeypatch.setattr(embeddings, "prototype_vectors", fake_prototype_vectors)
    monkeypatch.setattr(embeddings, "embed", fake_embed)
    monkeypatch.setattr(embeddings, "rank", lambda vector, protos: list(ranked))
    monkeypatch.setattr(llm, "verdict", fake_verdict)


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


async def seed_dm_workflow(db) -> Workflow:
    """Сценарий ЛС: без него `bind_active` вернёт пустой список, и второй конвейер
    (`wf_verdicts`) было бы нечем проверить."""
    inst = EngageInstance(key="test", client_label="Тестовый клиент",
                          base_url="http://engage.invalid",
                          api_key_env="RADAR_ENGAGE_API_KEY", is_active=True)
    db.add(inst)
    await db.flush()
    wf = Workflow(key="cold_dm", title="Личные сообщения", target_kind="user",
                  action="dm", visibility="private", engage_instance_id=inst.id,
                  engage_use_case="cold_dm", cascade_profile="dm_v1",
                  sort_order=10, is_active=True)
    db.add(wf)
    await db.commit()
    return wf


async def seed_channel(db, *, peer_id: int, bypass: bool) -> Channel:
    channel = Channel(peer_id=peer_id, username=f"ch{peer_id}", title="Канал",
                      l1_bypass_enabled=bypass)
    db.add(channel)
    await db.flush()
    return channel


# ── T7: флаг канала доезжает до каскада при приёме ────────────────────────────

async def test_ingest_passes_channel_flag_into_verdict(db, monkeypatch):
    """Один и тот же текст, два канала, разные флаги: обходное сообщение ложится
    «в пути» (`passed IS NULL`), обычное — убитым словарём. И то же самое во
    втором конвейере: разойдись они, разницу никто бы не увидел."""
    wf = await seed_dm_workflow(db)
    a = await seed_channel(db, peer_id=-1001, bypass=True)
    b = await seed_channel(db, peer_id=-1002, bypass=False)
    a_id, b_id, wf_id = a.id, b.id, wf.id
    monkeypatch.setattr(embeddings, "enabled", lambda: True)
    monkeypatch.setattr(llm, "enabled", lambda: False)
    bound = await targeting.bind_active(db)
    assert [x.workflow.key for x in bound] == ["cold_dm"], "сценарий не найден"

    for channel in (a, b):
        await ingest.upsert_message(
            db, channel=channel, tg_message_id=1000, tg_date=NOW,
            text=LONG_ANCHORLESS, author_peer_id=500, author_username="user",
            author_name="Имя", author_is_bot=False, is_automatic_forward=False,
            reply_to_message_id=None, thread_id=None, bound=bound)
    await db.commit()

    ma = (await db.execute(
        select(Message).where(Message.channel_id == a_id))).scalar_one()
    mb = (await db.execute(
        select(Message).where(Message.channel_id == b_id))).scalar_one()
    assert ma.cascade_level == 1 and ma.cascade_passed is None, \
        "обходное сообщение обязано лечь «в пути»"
    assert mb.cascade_level == 1 and mb.cascade_passed is False, \
        "без флага приговор прежний — убит словарём"

    va = (await db.execute(
        select(WfVerdict).where(WfVerdict.workflow_id == wf_id,
                                WfVerdict.message_id == ma.id))).scalar_one()
    vb = (await db.execute(
        select(WfVerdict).where(WfVerdict.workflow_id == wf_id,
                                WfVerdict.message_id == mb.id))).scalar_one()
    assert va.level == 1 and va.passed is None, "флаг не доехал до wf_verdicts"
    assert vb.level == 1 and vb.passed is False, \
        "wf_verdicts разошлись со старыми колонками"


# ── T9: потолок вопросов откладывает, а не убивает ────────────────────────────

async def test_l3_limit_defers_unasked_bypassed_messages(db, monkeypatch):
    """Два обходных сообщения, один вопрос за прогон: второе остаётся «в пути»
    на L2 — его догонит следующий прогон, а не похоронит этот."""
    stub_stages(monkeypatch)
    channel = await seed_channel(db, peer_id=-1003, bypass=True)
    channel_id = channel.id
    for i in range(2):
        db.add(Message(channel_id=channel_id, tg_message_id=1000 + i, tg_date=NOW,
                       author_peer_id=500 + i, author_username=f"user{i}",
                       author_name="Имя", author_is_bot=False,
                       is_automatic_forward=False, text=LONG_ANCHORLESS,
                       processed_at=NOW))
    await db.commit()

    summary = await reclassify.run(db, l2_enabled=True, l3_enabled=True,
                                   l3_limit=1, scope="pending",
                                   channel_ids=[channel_id])
    rows = (await db.execute(select(Message))).scalars().all()
    assert summary["l3_questions"] == 1
    assert sum(1 for m in rows
               if m.cascade_level == 3 and m.cascade_passed is True) == 1, \
        "спрошенное сообщение обязано дойти до L3 и пройти"
    assert sum(1 for m in rows
               if m.cascade_level == 2 and m.cascade_passed is None) == 1, \
        "неспрошенное обязано отложиться «в пути», а не умереть"

    # Отложенное действительно догоняется: без потолка второй прогон его дособирает.
    await reclassify.run(db, l2_enabled=True, l3_enabled=True,
                         l3_limit=None, scope="pending", channel_ids=[channel_id])
    rows = (await db.execute(select(Message))).scalars().all()
    assert all(m.cascade_level == 3 and m.cascade_passed is True for m in rows), \
        "отложенное сообщение не догнано вторым прогоном"


# ── T14: прогон по списку каналов трогает только перечисленные ────────────────

async def test_channel_scoped_run_touches_only_listed_channels(db, monkeypatch):
    """Наверстание истории канала А не обязано перекрашивать канал Б: вердикт Б —
    прежний, `processed_at` — прежний."""
    stub_stages(monkeypatch)
    a = await seed_channel(db, peer_id=-1004, bypass=True)
    b = await seed_channel(db, peer_id=-1005, bypass=False)
    a_id, b_id = a.id, b.id
    for channel, peer in ((a, 500), (b, 501)):
        db.add(Message(channel_id=channel.id, tg_message_id=1000, tg_date=NOW,
                       author_peer_id=peer, author_username="user", author_name="Имя",
                       author_is_bot=False, is_automatic_forward=False,
                       text=LONG_ANCHORLESS, cascade_level=1, cascade_passed=False,
                       processed_at=NOW))
    await db.commit()

    summary = await reclassify.run(db, l2_enabled=True, l3_enabled=True, l3_limit=0,
                                   scope="all", channel_ids=[a_id])
    # Единственная проверка, которую ломает снятие фильтра: у B тот же приговор
    # и без фильтра, а processed_at прогон не пишет вовсе
    assert summary["messages"] == 1, "в прогон попал только перечисленный канал"

    ma = (await db.execute(
        select(Message).where(Message.channel_id == a_id))).scalar_one()
    mb = (await db.execute(
        select(Message).where(Message.channel_id == b_id))).scalar_one()
    assert ma.cascade_passed is None, \
        "убитое словарём у отмеченного канала обязано уйти «в путь»"
    assert mb.cascade_passed is False, "у неотмеченного канала приговор прежний"
    assert mb.processed_at == NOW, "неотмеченный канал тронут не был"
