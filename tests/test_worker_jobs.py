"""Воркер прогонов: что он обещает очереди и что делает со строкой в `runs`.

Ни Redis, ни Postgres здесь не нужны. Проверяется не умение arq разговаривать с
Redis — это забота его авторов, — а наши решения, каждое из которых меняет судьбу
часового прогона на видеокарте:

* повторов нет, и это выбор, а не недоделка;
* остановка воркера не оставляет строку висеть «выполняется»;
* пометка прерванных живёт у исполнителя и не трогает тех, кто ещё ждёт в Redis;
* постановщик и исполнитель называют работу одним и тем же именем.

Последнее звучит мелко, но опечатка в имени с одной стороны выглядит как молча
пропавшая работа: задача ставится, исполнителя для неё нет.
"""
from __future__ import annotations

import ast
import asyncio
import inspect
import os
from unittest.mock import AsyncMock, patch

import pytest

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")

from arq.worker import Worker, func, get_kwargs  # noqa: E402

from app.services import jobs, queue  # noqa: E402
from app.workers import ingest as ingest_worker  # noqa: E402
from app.workers import jobs as worker  # noqa: E402


# ── имя работы: постановщик и исполнитель обязаны совпасть ────────────────────

def test_the_enqueued_name_is_the_name_the_worker_answers_to():
    """Имя считает arq, а не мы: `coroutine.__qualname__`, если не задано своё.

    Разойдись оно с константой, которой пользуется `jobs.start`, — и постановка
    прошла бы успешно, а работу не взял бы никто. Снаружи это выглядит как задача,
    вечно висящая «в очереди», и причину пришлось бы искать в Redis руками.
    """
    assert func(worker.run_job).name == queue.RUN_JOB


def test_the_worker_actually_registers_that_function():
    assert worker.run_job in worker.WorkerSettings.functions


# ── настройки, которые arq молча теряет при опечатке ──────────────────────────

def test_settings_reach_arq_instead_of_being_silently_dropped():
    """`get_kwargs` отбирает из класса настроек только те имена, что есть у `Worker`.

    Опечатка (`job_timeout_s` вместо `job_timeout`) не роняет ничего: атрибут просто
    исчезает, и воркер работает с умолчанием — пять попыток и таймаут в пять минут.
    Часовой прогон при таких умолчаниях убивался бы на двенадцатой минуте и
    перезапускался бы четырежды. Поэтому проверяем не сам класс, а то, что из него
    доехало.
    """
    kwargs = get_kwargs(worker.WorkerSettings)
    assert kwargs["max_tries"] == worker.MAX_TRIES
    assert kwargs["job_timeout"] == worker.JOB_TIMEOUT
    assert kwargs["keep_result"] == 0
    assert kwargs["max_jobs"] == worker.MAX_JOBS
    # Всё перечисленное — настоящие параметры `Worker`, а не наши выдумки.
    assert set(kwargs) <= set(inspect.signature(Worker).parameters)


def test_a_run_is_never_retried_from_scratch():
    """Повтор здесь дороже потери, и это противоположность воркеру приёма.

    Разбор вебхука стоит миллисекунды — повторить его после упавшего Postgres дешевле,
    чем потерять событие. Прогон стоит час видеокарты, на которой работает живой
    каскад, и «начать заново» — решение человека, глядящего на экран, а не цикла
    повторов, пока никто не смотрит.

    Единица имеет в arq точный смысл: счётчик попыток растёт в начале работы
    (`arq/worker.py:482`) и смерть воркера переживает, поэтому подобранная заново
    работа падает на `job_try > max_tries` (`arq/worker.py:550`), не дойдя до нашей
    функции. Прогон с нуля не начинается.
    """
    assert worker.MAX_TRIES == 1
    assert ingest_worker.MAX_TRIES > 1, "у приёма повторы есть, и это разные решения"


def test_the_timeout_leaves_room_for_the_longest_real_run():
    """`reclassify --scope all` идёт до часа. Приёмных пяти минут мало на порядок.

    Верхняя граница тоже не произвольна: тем же числом задаётся TTL ключа «в работе»
    (`arq/worker.py:277`), то есть срок, на который работа мёртвого воркера остаётся
    никем не тронутой. Сутки — это когда протухает сама работа; таймаут обязан
    сработать заметно раньше.
    """
    hour = 60 * 60
    assert worker.JOB_TIMEOUT >= 4 * hour, "часовой прогон должен помещаться с запасом"
    assert worker.JOB_TIMEOUT <= 12 * hour, "зависший прогон обязан кончиться в тот же день"


def test_the_run_result_is_not_kept_in_redis_as_a_second_truth():
    """Правда о прогоне живёт в строке `runs` — со статусом, итогом и логом.

    Копия в Redis была бы вторым источником, а два источника расходятся всегда. У
    приёма результат хранится сутки по другой причине: он там работает ключом от
    повторной доставки, которой здесь нет.
    """
    assert worker.WorkerSettings.keep_result == 0
    assert ingest_worker.WorkerSettings.keep_result > 0


# ── старт процесса ────────────────────────────────────────────────────────────

async def test_worker_refuses_to_start_without_a_queue(monkeypatch):
    """Воркер, поднявшийся без адреса очереди, выглядит исправным и не работает.

    Импорт модуля намеренно безобиден (arq читает настройки при импорте), поэтому
    отказ обязан случиться здесь — на старте процесса, где его видит оператор.
    """
    monkeypatch.setattr(queue, "enabled", lambda: False)
    with pytest.raises(RuntimeError, match="RADAR_REDIS_URL"):
        await worker.startup({})


async def test_startup_marks_only_the_runs_that_died_with_the_process(monkeypatch):
    """Прерванным помечается «выполняется», но не «в очереди».

    Строка в «в очереди» смерти процесса не пережила: работа лежит в Redis
    (`appendonly`) и ждёт, когда её возьмут, — а взять её собирается тот же воркер
    секундой позже. Пометь её прерванной, и экран соврал бы ровно за миг до старта
    работы, оставив в строке `error` и `finished_at`, которых потом никто не сотрёт.
    """
    monkeypatch.setattr(queue, "enabled", lambda: True)
    # Перечитка таксономии каскада тоже трогает базу (`cascade_registry.reload`) —
    # мокаем её той же монетой, что и `mark_interrupted`: тест проверяет решение
    # про пометку прерванных, а не то, что воркер умеет говорить с Postgres.
    with patch.object(worker.cascade_registry, "reload", AsyncMock()), \
         patch.object(worker.jobs, "mark_interrupted",
                      AsyncMock(return_value=2)) as sweep:
        await worker.startup({})

    assert sweep.await_args.kwargs["statuses"] == ("running",)


def test_the_api_does_not_sweep_runs_it_no_longer_owns():
    """Зеркало предыдущего: с включённой очередью пометка на старте API — ложь.

    Рестарт API работу воркера не обрывает. Пометка оттуда не только соврала бы на
    экране, но и отпустила бы `active_run`: оператор запустил бы вторую
    переклассификацию поверх первой, обе на одной видеокарте.
    """
    from app import main
    src = inspect.getsource(main.lifespan)
    assert "if queue.enabled():" in src
    assert "jobs.mark_interrupted()" in src


def test_the_run_worker_does_not_reach_into_telegram():
    """Прогон ходит в базу, в эмбеддинги и в модель — и никуда больше.

    Отсюда и короткий `startup` (реестр инстансов Engage не поднимается), и то, что
    сервису воркера в `docker-compose.yml` не нужна сеть Engage. Появись здесь
    обращение к нему — обе эти вещи станут неправдой молча.

    Смотрим на импорты, а не на текст: слово «Engage» в докстроке — это объяснение,
    а зависимость заводит только `import`.
    """
    tree = ast.parse(inspect.getsource(worker))
    imported = {alias.name
                for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
                for alias in node.names}
    assert not {"engage", "engage_registry"} & imported, sorted(imported)


# ── одна работа ───────────────────────────────────────────────────────────────

async def test_a_finished_run_reports_itself_to_the_worker_log():
    """Ошибки прогона наружу не выходят: `jobs.execute` сам пишет статус, лог и
    тревогу. Бросать их дальше значило бы записать работу в неудачные вторым
    местом, ничего этим не изменив."""
    with patch.object(worker.jobs, "execute", AsyncMock()) as execute:
        out = await worker.run_job({}, 17, "reclassify", {"scope": "all"})

    execute.assert_awaited_once_with(17, "reclassify", {"scope": "all"})
    assert out["run"] == 17


async def test_an_unknown_kind_does_not_leave_the_row_queued_forever():
    """Постановщик этот список уже проверял — значит образы API и воркера разошлись.

    Молчаливый возврат оставил бы строку в «в очереди» навсегда: работы для неё в
    этой сборке нет вовсе, и ждать её появления бессмысленно.
    """
    with patch.object(worker.jobs, "finish", AsyncMock()) as finish, \
         patch.object(worker, "_alert", AsyncMock()) as alert, \
         patch.object(worker.jobs, "execute", AsyncMock()) as execute:
        out = await worker.run_job({}, 5, "export", {})

    execute.assert_not_awaited()
    assert finish.await_args.kwargs["status"] == "failed"
    assert alert.await_args.args[0] == "run_kind_unknown"
    assert "rejected" in out


async def test_stopping_the_worker_closes_the_row_instead_of_freezing_it():
    """Так выглядит выкатка: arq отменяет корутину работы по SIGTERM.

    Не закрой мы строку здесь — она осталась бы «выполняется» с замершим прогрессом
    до старта следующего воркера. Тот её домёл бы (`startup`), но между двумя
    моментами экран показывал бы идущую работу, которой уже нет.
    """
    with patch.object(worker.jobs, "execute",
                      AsyncMock(side_effect=asyncio.CancelledError())), \
         patch.object(worker.jobs, "finish", AsyncMock()) as finish:
        with pytest.raises(asyncio.CancelledError):
            await worker.run_job({}, 9, "reclassify", {})

    assert finish.await_args.kwargs["status"] == "interrupted"


async def test_a_cancelled_run_is_still_reported_as_cancelled_to_arq():
    """Отмену обязательно пробрасывать дальше.

    Проглоти мы её — arq счёл бы работу выполненной и снял бы её с очереди, а строка
    в базе к этому моменту уже помечена прерванной. Наружу это выглядело бы как
    успешно завершённая выкатка, тихо убившая часовой прогон.
    """
    with patch.object(worker.jobs, "execute",
                      AsyncMock(side_effect=asyncio.CancelledError())), \
         patch.object(worker.jobs, "finish", AsyncMock()):
        with pytest.raises(asyncio.CancelledError):
            await worker.run_job({}, 9, "reclassify", {})


async def test_a_failed_alert_does_not_hide_the_reason_it_was_raised_for():
    """Тревога — не главное в этой ветке. Упади она сама, строка всё равно обязана
    быть закрыта: иначе работа исчезнет ровно тем способом, от которого тревога и
    заводилась."""
    broken = AsyncMock(side_effect=RuntimeError("и тревога не легла"))
    with patch.object(worker.jobs, "finish", AsyncMock()) as finish, \
         patch.object(worker.alerts, "emit", broken):
        out = await worker.run_job({}, 5, "export", {})

    finish.assert_awaited_once()
    assert "rejected" in out


# ── обе ветки запуска ─────────────────────────────────────────────────────────

def test_both_launch_paths_run_the_same_code():
    """С очередью и без неё прогон обязан быть одним и тем же.

    Разойдись они — разница вылезла бы там, где второй ветки нет: на проде. Поэтому
    и ветка `create_task`, и воркер зовут `jobs.execute`, а не свои копии.
    """
    assert "execute(run.id, kind, params)" in inspect.getsource(jobs.start)
    assert "jobs.execute(run_id, kind, params)" in inspect.getsource(worker.run_job)
