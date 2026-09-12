"""Отмена прогона в стадии L3 — на настоящем Postgres.

Дефект 12.09: проверка `cancelled()` стояла в `_stage_l3` **до** семафора. `gather`
создаёт все корутины разом, каждая проходила проверку в первую же секунду — до своей
очереди у семафора — и дальше отмену не видела никогда: на 3 350 вопросов стадия
оставалась неотменяемой ~85 минут, прогресс рос, запросы к модели шли каждые ~1,4 с.

Здесь проверяется починка целиком, а не только место проверки:

* после отмены модель вызывается лишь теми, кто уже ушёл к ней (плюс те, кто успел
  получить слот до переворота флага) — новые вопросы не задаются;
* прогон завершается статусом `cancelled`, а не ошибкой;
* ответы на успевшие заданные вопросы не выбрасываются: трейсы `LlmTrace` и
  вердикты отвеченных сообщений записаны, как при штатном завершении;
* неспрошенные сообщения остаются «в пути» (`level=2, passed=NULL`), а не убиваются.

Отмену здесь решает настоящий `run` по настоящей базе: место проверки — это гонка
корутин у семафора, моками она не доказывается.

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db.models import Base, Channel, LlmTrace, Message
from app.services import embeddings, llm, reclassify

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

NOW = datetime(2026, 9, 12, 12, 0, tzinfo=timezone.utc)

N_QUESTIONS = 20

# Текст с якорем боли и признаком проблемы: проходит L1 (при включённом L2 словарь
# работает на полноту), а заглушка близости проводит его через L2 «в пути» —
# до L3 добираются все двадцать.
TEXT = "не могу оплатить инвойс, помогите разобраться"

# Картина близости от заглушки эмбеддера — та же, что в `test_l1_bypass_db.py`:
# верхний эталон «боль» 0.60 (выше порога 0.57), шум ниже.
RANKED_POS = [("pos", "банк не пропускает платёж", 0.60), ("neg", "офтоп", 0.40)]

# Ответ модели: «настоящая проблема автора» — вердикт L3 пропуск.
LLM_YES = {"real_problem": True, "is_seller": False,
           "answering_someone_else": False, "why": "…"}


def _trace() -> dict:
    """Трейс вызова, как его отдаёт настоящий клиент `llm.verdict`.

    Обязателен: по строкам `llm_traces` тест проверяет, что сделанные вызовы не
    выброшены при отмене. Пустой `trace=None` строки бы не породил — и проверять
    было бы нечего.
    """
    return {"stage": "l3", "model": "stub", "prompt_version": "l3-verdict-v4",
            "temperature": 0.0, "prompt": "вопрос", "response": "ответ",
            "tokens_in": 1, "tokens_out": 1, "latency_ms": 1, "cost_usd": 0}


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


def stub_stages(monkeypatch, calls: dict):
    """Заглушки ступеней — по образцу `test_l1_bypass_db.py`, но с отменой.

    `llm.verdict` отвечает с задержкой и ведёт счёт: `calls["started"]` — сколько
    раз модель позвали, `calls["finished"]` — сколько ответов уже вернулось.
    `cancelled()` из этого же счётчика: False до третьего ответа модели, True
    после. Флаг переворачивается по завершённым вызовам, а не по начатым — иначе
    проверка «над семафором» (прежний дефект) не отличалась бы от починенной:
    все двадцать корутин стартуют до первого ответа, и обе версии видели бы
    `cancelled() is False`.
    """

    async def fake_prototype_vectors():
        return []

    async def fake_embed(texts):
        return [[0.0] for _ in texts]

    async def fake_verdict(*, text, context, prompt_key):
        calls["started"] += 1
        await asyncio.sleep(0.05)
        calls["finished"] += 1
        return dict(LLM_YES), _trace()

    def cancelled():
        return calls["finished"] >= 3

    monkeypatch.setattr(embeddings, "enabled", lambda: True)
    monkeypatch.setattr(embeddings, "prototype_vectors", fake_prototype_vectors)
    monkeypatch.setattr(embeddings, "embed", fake_embed)
    monkeypatch.setattr(embeddings, "rank", lambda vector, protos: list(RANKED_POS))
    monkeypatch.setattr(llm, "verdict", fake_verdict)
    return cancelled


async def test_cancel_in_l3_stops_asking_and_keeps_answers(db, monkeypatch):
    """T-cancel-l3: отмена посреди L3 — вопросы прекращаются, ответы остаются."""
    channel = Channel(peer_id=-1001, username="ch", title="Канал")
    db.add(channel)
    await db.flush()
    for i in range(N_QUESTIONS):
        db.add(Message(channel_id=channel.id, tg_message_id=1000 + i,
                       tg_date=NOW - timedelta(minutes=i),
                       author_peer_id=500 + i, author_username=f"user{i}",
                       author_name="Имя", author_is_bot=False,
                       is_automatic_forward=False, text=TEXT, processed_at=NOW))
    await db.commit()

    calls = {"started": 0, "finished": 0}
    cancelled = stub_stages(monkeypatch, calls)

    summary = await reclassify.run(db, l2_enabled=True, l3_enabled=True,
                                   scope="all", cancelled=cancelled)

    assert calls["started"] >= 3, "отмена обязана случиться после настоящих вызовов"
    # Уже стоявшие у семафора не начинают запрос; в полёте — не больше слотов.
    assert calls["started"] <= 3 + reclassify.L3_CONCURRENCY, \
        (f"после отмены модель позвали {calls['started']} раз — "
         f"новые вопросы задаваться не должны")
    assert summary["cancelled"] is True, "остановка — это статус, а не ошибка"

    rows = (await db.execute(
        select(Message).order_by(Message.id))).scalars().all()
    asked = [m for m in rows if m.cascade_level == 3]
    waiting = [m for m in rows if m.cascade_level == 2 and m.cascade_passed is None]
    assert len(asked) == calls["started"], \
        "ответившим вопросам вердикт записан, как при штатном завершении"
    assert all(m.cascade_passed is True for m in asked)
    assert len(waiting) == N_QUESTIONS - calls["started"], \
        "неспрошенные обязаны остаться «в пути»"
    assert all(m.cascade_passed is not False for m in rows), \
        "отмена не убивает сообщения"

    traces = (await db.execute(select(func.count(LlmTrace.id)))).scalar_one()
    assert traces == calls["started"], "трейсы сделанных вызовов не выбрасываются"
    assert summary["created"] == calls["started"], \
        "посчитанные вердикты доезжают до лидов, а не теряются вместе с прогоном"
