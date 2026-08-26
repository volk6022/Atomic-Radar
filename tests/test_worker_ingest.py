"""Воркер приёма: что он делает с негодным событием и с ненастроенным окружением.

Ни Redis, ни Postgres здесь не нужны. Проверяется поведение самой задачи — то, чего
в ручке приёма не было и быть не могло: у HTTP-ручки отказ уходит кодом ответа, а у
задачи в очереди отказ — это либо ретрай, либо тишина, и путать их дорого.

Главное, что здесь закреплено: **ни один отказ не остаётся молчаливым**. Раньше
исключение долетало до Engage кодом ответа, тот повторял пять раз и сдавался — это
было видно хотя бы в его журнале. Теперь Engage получает `202` и уходит, и если
работа умрёт в очереди, о ней не узнает никто.
"""
from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")

from app.services import queue  # noqa: E402
from app.workers import ingest as worker  # noqa: E402

BODY = {"event": "task_complete", "result": {"posts": []}}
Q = {"kind": "history"}


def _session():
    """Сессия-двойник: воркер обязан её закрыть и откатить, что бы ни случилось."""
    db = AsyncMock()
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=db)
    ctx.__aexit__ = AsyncMock(return_value=False)
    maker = MagicMock(return_value=ctx)
    return maker, db


async def test_bad_event_is_recorded_not_retried():
    """`HTTPException` от разбора — это «Engage прислал негодное», а не сбой.

    Ретраить такое бессмысленно: тот же вход даст тот же отказ, и задача трижды
    прокрутится вхолостую, прежде чем осесть в неудачных. Поэтому она завершается
    успешно, а причина уезжает в результат работы.

    Но не молча. Событие, которого мы не понимаем, — это новость об Engage, и до
    самого Engage она уже не дойдёт: он получил `202` и ушёл.
    """
    maker, db = _session()
    with patch.object(worker, "get_session_maker", return_value=maker), \
         patch.object(worker, "_alert", AsyncMock()) as alert, \
         patch.object(worker, "process_event",
                      AsyncMock(side_effect=HTTPException(400, "нет peer_id"))):
        out = await worker.ingest_event({}, BODY, Q)

    assert out == {"accepted": 0, "rejected": "нет peer_id"}
    db.rollback.assert_awaited()
    alert.assert_awaited_once()
    assert alert.await_args.args[0] == "ingest_event_rejected"


async def test_transient_failure_is_raised_so_arq_retries():
    """Упавший Postgres — временное. Проглотить это значило бы потерять событие
    молча: очередь сочла бы работу сделанной.

    Тревоги на этой попытке быть не должно — попытки ещё есть, и будить оператора
    из-за перезапуска базы значит приучить его не смотреть на тревоги.
    """
    maker, db = _session()
    with patch.object(worker, "get_session_maker", return_value=maker), \
         patch.object(worker, "_alert", AsyncMock()) as alert, \
         patch.object(worker, "process_event",
                      AsyncMock(side_effect=ConnectionError("база перезапускается"))):
        with pytest.raises(ConnectionError):
            await worker.ingest_event({"job_try": 1}, BODY, Q)

    db.rollback.assert_awaited()
    alert.assert_not_awaited()


async def test_last_attempt_raises_an_alert_before_giving_up():
    """Единственный момент, когда об умирающей работе можно сказать вслух.

    Особенно это важно для бэкфилла: его строка в `runs` при обрыве цепочки не
    помечается упавшей ничем — `mark_interrupted` намеренно не трогает
    `kind="backfill"`, — и без тревоги прогон висел бы «выполняется» бесконечно.
    """
    maker, _ = _session()
    with patch.object(worker, "get_session_maker", return_value=maker), \
         patch.object(worker, "_alert", AsyncMock()) as alert, \
         patch.object(worker, "process_event",
                      AsyncMock(side_effect=ConnectionError("Engage не отвечает"))):
        with pytest.raises(ConnectionError):
            await worker.ingest_event({"job_try": worker.MAX_TRIES}, BODY, Q)

    alert.assert_awaited_once()
    key, text = alert.await_args.args
    assert key == "ingest_event_failed"
    assert str(worker.MAX_TRIES) in text
    assert "бэкфилл" in text, "тревога должна называть, что именно могло оборваться"


async def test_alert_failure_does_not_swallow_the_original_error():
    """Тревога — не главное в этой ветке. Упади она сама, наружу обязана уйти
    исходная ошибка: иначе очередь решит, что работа выполнена, и событие исчезнет
    ровно тем способом, от которого тревога и заводилась."""
    maker, _ = _session()
    broken = MagicMock(emit=AsyncMock(side_effect=RuntimeError("и тревога не легла")))
    with patch.object(worker, "get_session_maker", return_value=maker), \
         patch.object(worker, "alerts", broken), \
         patch.object(worker, "process_event",
                      AsyncMock(side_effect=ConnectionError("Engage не отвечает"))):
        with pytest.raises(ConnectionError):
            await worker.ingest_event({"job_try": worker.MAX_TRIES}, BODY, Q)


async def test_result_of_a_good_event_is_returned_verbatim():
    """Результат разбора — это результат работы arq. По нему потом видно, что
    именно случилось с конкретным вебхуком, без похода в логи."""
    maker, _ = _session()
    with patch.object(worker, "get_session_maker", return_value=maker), \
         patch.object(worker, "process_event",
                      AsyncMock(return_value={"accepted": 12, "created": 12})):
        assert await worker.ingest_event({}, BODY, Q) == {"accepted": 12, "created": 12}


async def test_worker_refuses_to_start_without_a_queue(monkeypatch):
    """Воркер, поднявшийся без адреса очереди, выглядит исправным и не работает.

    Импорт модуля намеренно безобиден (arq читает настройки при импорте), поэтому
    отказ обязан случиться здесь — на старте процесса, где его видит оператор.
    """
    monkeypatch.setattr(queue, "enabled", lambda: False)
    with pytest.raises(RuntimeError, match="RADAR_REDIS_URL"):
        await worker.startup({})
