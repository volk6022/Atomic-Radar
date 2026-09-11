"""Изоляция состояния между тестами набора Radar.

В наборе не было ни одного conftest'а. Каждое приложение, поднятое тестом
(`create_app()` + TestClient), при старте перечитывает базу в память процесса:
`lifespan` зовёт `cascade_registry.ensure_bootstrap` + `reload` (таксономия,
эталоны L2, промпты L3, реестр Engage), а правки, сделанные тестами через
`config_bundle.import_bundle` / `save_l3_prompt`, мутируют те же глобальные
словари напрямую. Всё это переживало конец теста — и следующий тест начинался
на правилах, оставшихся от предыдущего. Отсюда «борьба фикстур»: модули
`test_cascade`, `test_drafts`, `test_targeting_db`, `test_l3_prompts`,
`test_llm_grammar`, `test_llm_channel_fit` падают только в полном прогоне и
каждый зелёный в изоляции. Дефект — в отсутствующей изоляции, то есть в
фикстурах; сами тесты и приложение не тронуты.

Два доказанных механизма.

1. **JSONB переставляет ключи.** Postgres хранит ключи jsonb-объекта
   упорядоченными по (длина, байты), а не по порядку вставки. Первый же старт
   приложения на базе пишет `cascade.PAIN_ANCHORS` в `cascade_versions`
   (`ensure_bootstrap`) и читает обратно (`reload` → `apply_taxonomy`) — и
   порядок ключей в памяти процесса меняется: «не может оплатить за рубеж»
   (26 знаков) было первым в коде, а в базе первым становится «банк не
   пропускает платёж» (25). `cascade.level1` возвращает **первую** боль с
   совпавшим якорем, поэтому после любого теста с приложением на базе
   `classify("…банк отказал, платёж за рубеж не проходит…")` отдаёт
   «банк не пропускает платёж» вместо «не может оплатить за рубеж». Ломает
   `test_cascade::test_real_pain_passes` (детерминированно — достаточно пары
   с любым DB-тестом), а в полном прогоне ещё `test_drafts` и
   `test_targeting_db`: их сообщения «платёж за рубеж не проходит…» перестают
   матчиться на ту боль, под которую посажены шаблоны и проверки.

2. **Env-утечка.** Часть файлов ставит `os.environ["RADAR_DATABASE_URL"]` в
   фикстуре и не возвращает назад (`test_backfill_rules_db`, `test_backfill_drain_db`,
   `test_discussions_*_db`, `test_events_sse_db`, `test_ingest_queue_db`) — с
   середины прогона все настройки читаются уже с чужой переменной.

Что делает autouse-фикстура `_isolate_radar_state`:

* снимает и восстанавливает мутируемые реестры приложения (`PAIN_ANCHORS`,
  `DISQUALIFIERS`, `POSITIVE`/`NEGATIVE` эталоны, реестр промптов L3, кэш
  векторов эмбеддера, кэш эндпоинтов Engage) — мутацией на месте, чтобы
  `MappingProxyType`-витрины (`cascade.PAIN_ANCHORS` на профилях,
  `llm.PROMPTS`) остались теми же объектами;
* отменяет фонового наблюдателя реестра (`cascade_registry._watch_task`),
  если тест оставил его от своего цикла событий;
* сбрасывает lru-кеш движка, фабрик сессий и настроек
  (`get_engine`/`get_session_maker`/`get_settings`) — следующий тест получает
  свежие объекты на своём цикле. **Диспоузить закешированный движок отсюда
  нельзя**, хотя именно так чинилась «утечка соединений»: пул движка
  приложения принадлежит циклу портала ещё живого TestClient (lifespan и
  запросы исполняются там), а закрыть соединение с чужого цикла asyncpg не
  может — `await engine.dispose()` в `asyncio.run` падает с
  `RuntimeError: … Future … attached to a different loop`, SQLAlchemy
  выбрасывает соединение из пула, **не закрыв сокет**, — и при выходе
  TestClient цикл портала перестаёт закрываться: proactor на Windows не
  выходит из `loop.close()`, пока не опустеют незавершённые overlapped-операции,
  а главный поток вечно ждёт `thread.join()` портала. Так полный прогон
  вставал навсегда на teardown модульного `client` `test_ingest_queue_db`
  (47 % сбора, между ним и `test_jobs`). Сброс кеша только лишает движок
  последней ссылки; живые сокеты закрывает собственный цикл при выходе
  TestClient — с ним и Postgres отпускает соединения;
* возвращает назад `RADAR_*`-переменные окружения, изменённые тестом.

Плюс **выравнивание часов** (только когда задан `RADAR_TEST_DATABASE_URL`).
Ручки сравнивают `recorded_at`/`created_at` — серверный `now()` базы — с
`clock.utcnow()` процесса. В проде и на Linux-машине разработчика это одни и
те же часы (одно ядро: приложение и Postgres рядом). Под Docker Desktop на
Windows база живёт в VM, и её часы плывут относительно часов Windows на
секунды в обе стороны (замер на этой машине: +0.4…+1.1 с за минуты). Дрейф
больше, чем пауза «посев → запрос» (доли секунды), — и строка, записанная
перед запросом, выпадает из окна `sent_ts <= until`: мерцание на десятки
процентов прогонов (`test_wf_activity_db`, ручные записи без `sent_at`).
Фикстура измеряет сдвиг одним `SELECT clock_timestamp()` и подменяет
`clock.utcnow` на «часы процесса + сдвиг» — процесс начинает смотреть на те
же часы, что и база, то есть ровно на ту точку времени, которая в проде одна
на всё. Никакая проверка от этого не слабеет: тесты, замораживающие время
(`monkeypatch.setattr(clock, "utcnow", …)`), перекрывают подмену как и раньше.
Тесты, изменяющие реестры в своих фикстурах, тоже не задеты — восстановление
происходит после конца теста, а не до его тела.
"""
from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime, timedelta, timezone

import pytest

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

# RADAR_-переменные, которые тесты крутят руками; после теста возвращаем как было.
_MANAGED_ENV = ("RADAR_DATABASE_URL", "RADAR_TEST_DATABASE_URL", "RADAR_DEBUG",
                "RADAR_SECRET_KEY", "RADAR_DEFAULT_MODE")

# Сдвиг часов меньше этого не стоит подмены: это шум одного сетевого обхода.
_CLOCK_OFFSET_EPSILON = 0.05


def _snapshot_app_state() -> dict:
    from app.core import cascade, prototypes
    from app.services import embeddings, engage_registry, llm

    return {
        "pain": dict(cascade.PAIN_ANCHORS),
        "disq": dict(cascade.DISQUALIFIERS),
        "pos": dict(prototypes.POSITIVE),
        "neg": dict(prototypes.NEGATIVE),
        "prompts": dict(llm._PROMPTS),
        "proto_cache": embeddings._prototype_cache,
        "engage": dict(engage_registry._cache),
    }


def _restore_app_state(snapshot: dict) -> None:
    from app.core import cascade, prototypes
    from app.services import embeddings, engage_registry, llm

    # Мутация на месте, а не переприсваивание: на эти словари смотрят
    # MappingProxyType-витрины (`cascade.PAIN_ANCHORS` на профилях,
    # `llm.PROMPTS`) — им важно, чтобы объект остался тем же.
    cascade.PAIN_ANCHORS.clear()
    cascade.PAIN_ANCHORS.update(snapshot["pain"])
    cascade.DISQUALIFIERS.clear()
    cascade.DISQUALIFIERS.update(snapshot["disq"])
    prototypes.POSITIVE.clear()
    prototypes.POSITIVE.update(snapshot["pos"])
    prototypes.NEGATIVE.clear()
    prototypes.NEGATIVE.update(snapshot["neg"])
    llm._PROMPTS.clear()
    llm._PROMPTS.update(snapshot["prompts"])
    embeddings._prototype_cache = snapshot["proto_cache"]
    engage_registry._cache.clear()
    engage_registry._cache.update(snapshot["engage"])


def _stop_watch_task_and_caches() -> None:
    from app.core.config import get_settings
    from app.db.session import get_engine, get_session_maker
    from app.services import cascade_registry

    task, cascade_registry._watch_task = cascade_registry._watch_task, None
    if task is not None:
        # Цикл, в котором задача создана, скорее всего уже закрыт вместе с
        # TestClient; await здесь взять негде — отменяем без ожидания.
        task.cancel()

    # Соединения кешированного движка принадлежат циклу портала TestClient, и
    # закрыть их отсюда нельзя: dispose в `asyncio.run` падает с «Future
    # attached to a different loop», SQLAlchemy выбрасывает соединение из пула,
    # не закрыв сокет, — и цикл портала потом вечно не может закрыться
    # (proactor не выходит из loop.close() при незавершённых overlapped-операциях),
    # TestClient.__exit__ висит на thread.join(). Поэтому движок только
    # отвязывается: cache_clear роняет последнюю ссылку, а сокеты закрывает
    # собственный цикл при выходе TestClient — вместе с ними освобождается
    # и серверная сторона. Диспоуз «от утечки соединений» превращал полный
    # прогон в бесконечный висяк на границе test_ingest_queue_db → test_jobs.
    get_engine.cache_clear()
    get_session_maker.cache_clear()
    get_settings.cache_clear()


def _db_clock_offset() -> float:
    """Сдвиг часов Postgres относительно часов процесса, секунд (база − хост).

    Один `clock_timestamp()` с поправкой на половину времени обхода. При любом
    сбое измерения считаем сдвиг нулевым: отсутствие подмены возвращает тестам
    сегодняшнее поведение, а не ломает их.
    """
    async def _measure() -> float:
        import asyncpg

        dsn = DB_URL.replace("postgresql+asyncpg://", "postgresql://", 1)
        conn = await asyncpg.connect(dsn)
        try:
            host_before = time.time()
            db_now = await conn.fetchval("SELECT clock_timestamp()")
            host_after = time.time()
            return db_now.timestamp() - (host_before + host_after) / 2
        finally:
            await conn.close()

    try:
        return asyncio.run(_measure())
    except Exception:  # noqa: BLE001 — нет базы, нет и сдвига
        return 0.0


@pytest.fixture(autouse=True)
def _isolate_radar_state(monkeypatch):
    """Снимок до теста — восстановление после; выравнивание часов с базой."""
    snapshot = _snapshot_app_state()
    env_before = {key: os.environ.get(key) for key in _MANAGED_ENV}

    if DB_URL:
        offset = _db_clock_offset()
        if abs(offset) > _CLOCK_OFFSET_EPSILON:
            from app.core import clock as clock_mod

            monkeypatch.setattr(
                clock_mod, "utcnow",
                lambda: datetime.now(timezone.utc) + timedelta(seconds=offset))

    yield

    _restore_app_state(snapshot)
    _stop_watch_task_and_caches()
    for key, value in env_before.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
