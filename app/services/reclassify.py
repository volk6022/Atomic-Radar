"""Прогон сохранённых сообщений по каскаду заново.

Нужен каждый раз, когда меняются правила: сообщения уже лежат в базе с прежним
вердиктом, и без переклассификации экран потока продолжит объяснять решения, которых
код больше не принимает. И ещё чаще — просто чтобы разобрать накопившееся: ингест
кладёт новое сообщение со статусом «ждёт обработки», а досчитывает его вот это.

Ступени идут отдельными проходами, а не по одному сообщению до конца, и это не
стилистика. L2 считает эмбеддинги пачками — по одному через туннель было бы в разы
дольше. L3 ходит в модель на четыре слота параллельно. Оба режима возможны только
если между ступенями есть барьер.

Лиды, по которым человек уже принял решение, не трогаются: переписывать чужое решение
задним числом нельзя. Лиды в статусе `new`, переставшие проходить каскад, удаляются —
держать в очереди то, что система больше не считает лидом, значит тратить время
оператора на заведомый мусор. «Решение принято» означает и статус лида, и решение по
его черновику; подробности в `_reconcile_leads`.

Модуль — общее ядро для CLI (`scripts/reclassify.py`) и для задачи, запускаемой из
интерфейса. Отсюда `report` и `cancelled`: снаружи прогон обязан показывать, сколько
осталось, и уметь останавливаться, а из консоли это никому не нужно.
"""
from __future__ import annotations

import asyncio
import logging

from sqlalchemy import func, or_, select

from app.core import cascade
from app.db.models import Channel, Draft, Lead, LlmTrace, Message, WfVerdict
from app.services import drafting, embeddings, llm, targeting

log = logging.getLogger("radar.reclassify")

# Столько запросов к модели держим в воздухе одновременно. Ровно по числу слотов
# llama-server: больше — очередь на стороне сервера, меньше — простой карты.
L3_CONCURRENCY = 4

# Размер пачки эмбеддингов на один шаг прогресса. Сам клиент режет запросы мельче;
# здесь величина другая — на сколько частей делится ожидание, чтобы прогресс двигался
# и отмена срабатывала не через десять минут.
EMBED_CHUNK = 256

# Во сколько обходится каждая ступень в общей шкале прогресса. Числа взяты из
# наблюдаемого времени на 12 тысячах сообщений: L0/L1 — секунды, L2 — минуты,
# L3 — десятки минут.
WEIGHTS = {"l0l1": 5, "l2": 25, "l3": 65, "leads": 5}

SCOPES = ("all", "pending")


class Cancelled(Exception):
    """Прогон остановлен снаружи. Не ошибка: то, что успели, уже записано."""


async def _noop_report(pct: float, note: str) -> None:
    return None


def _never_cancelled() -> bool:
    return False


def _run_l0l1(m: Message, *, l2_enabled: bool, l3_enabled: bool,
              l1_bypass: bool = False) -> dict:
    return cascade.classify(
        text=m.text, is_automatic_forward=m.is_automatic_forward,
        author_is_bot=m.author_is_bot, author_peer_id=m.author_peer_id,
        author_username=m.author_username, tg_date=m.tg_date,
        l2_enabled=l2_enabled, l3_enabled=l3_enabled, l1_bypass=l1_bypass)


def _apply(m: Message, v: dict) -> None:
    m.cascade_level, m.cascade_passed = v["level"], v["passed"]
    m.cascade_detail = v["detail"]


# Промпт старых колонок. Они считаются профилем по умолчанию, значит и вопрос к
# модели у них его же — иначе `leads` перестал бы быть точной тенью сценария ЛС.
LEGACY_PROMPT = cascade.DM_V1.l3_prompt_key


def _wf_verdicts(bound: list[targeting.Bound], messages: list[Message], *,
                 l2_enabled: bool, l3_enabled: bool,
                 ranked: dict[int, list],
                 llm_answers: dict[int, dict[str, dict]],
                 channels: dict[int, Channel],
                 ) -> dict[tuple[int, int], dict]:
    """Вердикты всех сценариев по всем сообщениям — из того, что посчитано на сейчас.

    Пересчитывается целиком перед каждой дорогой ступенью, а не поддерживается
    приращением. `cascade.classify` — чистая функция от уже готовых входов, и стоит
    микросекунды; вести для каждого сценария своё изменяемое состояние параллельно
    старому значило бы завести второе место, где вердикты могут разойтись.

    Ответ модели берётся по ключу промпта самого профиля: чужой здесь означал бы
    отбор по вопросу, заданному не про этот контур.

    Флаг обхода L1 берётся у канала сообщения: второй конвейер обязан судить
    сообщение по тем же правилам, что и старые колонки, иначе `wf_verdicts` молча
    разошёлся бы с ними.
    """
    return {(b.workflow.id, m.id): targeting.verdict_for(
                b, m, l2_enabled=l2_enabled, l3_enabled=l3_enabled,
                ranked=ranked.get(m.id),
                llm=llm_answers.get(m.id, {}).get(b.profile.l3_prompt_key),
                l1_bypass=channels[m.channel_id].l1_bypass_enabled)
            for b in bound for m in messages}


def _waits(v: dict | None, level: int) -> bool:
    return v is not None and v["passed"] is None and v["level"] == level


def _waiting_at(level: int, messages: list[Message], verdicts: dict,
                wf_verdicts: dict, bound: list[targeting.Bound]) -> list[Message]:
    """Кому ещё нужна ступень `level` — с учётом всех сценариев, а не только старых колонок.

    Считать по одним `messages.cascade_*` было бы тихой поломкой второго конвейера:
    сообщение, отсеянное правилами ЛС на L1, для публичного ответа может быть законной
    целью, но вектор ему никто бы не посчитал — и оно навсегда осталось бы в «ожидает».

    Годится для L2, где результат один на сообщение. Для L3 нужен `_l3_jobs`: там у
    каждого контура свой вопрос, и «кто ждёт» — это пары, а не сообщения.
    """
    return [m for m in messages
            if _waits(verdicts[m.id], level)
            or any(_waits(wf_verdicts.get((b.workflow.id, m.id)), level) for b in bound)]


def _l3_jobs(messages: list[Message], verdicts: dict, wf_verdicts: dict,
             bound: list[targeting.Bound]) -> list[tuple[Message, str]]:
    """Пары «сообщение × вопрос» — по одному вызову модели на каждую.

    Здесь и живёт решение о независимости контуров: список строится не по сообщениям,
    а по вопросам к ним. Два сценария с разными промптами дают два обращения к модели
    по одному и тому же тексту, и это принято сознательно — время карты дешевле, чем
    отбор публичных ответов по вопросу про личные сообщения.

    Совпадение остаётся ровно одно: если у двух контуров **дословно один и тот же**
    ключ промпта, вызов будет один. Это не совмещение разных вопросов, а один и тот
    же вопрос — при `temperature=0` ответ на него побайтово тот же самый. Именно так
    старые колонки делят вызов со сценарием ЛС, и только благодаря этому `leads`
    остаётся точной тенью `wf_targets`, а не расходится с ним.
    """
    jobs: list[tuple[Message, str]] = []
    seen: set[tuple[int, str]] = set()
    for m in messages:
        keys = []
        if _waits(verdicts[m.id], 2):
            keys.append(LEGACY_PROMPT)
        keys += [b.profile.l3_prompt_key for b in bound
                 if _waits(wf_verdicts.get((b.workflow.id, m.id)), 2)]
        for key in keys:
            if (m.id, key) not in seen:
                seen.add((m.id, key))
                jobs.append((m, key))
    return jobs


async def _select_messages(db, scope: str, bound: list[targeting.Bound],
                           channel_ids: list[int] | None = None) -> list[Message]:
    stmt = select(Message)
    if channel_ids is not None:
        # Частичный прогон: только перечисленные каналы, для любого охвата.
        # Наверстание истории канала — это тот же прогон, суженный списком, а не
        # отдельный механизм (контракт §6).
        stmt = stmt.where(Message.channel_id.in_(channel_ids))
    if scope == "pending":
        # Только то, что ещё не досчитано: новое из ингеста и застрявшее на ступени,
        # которая в прошлый раз была недоступна. На двенадцати тысячах сообщений это
        # разница между десятками минут и парой секунд.
        #
        # Недосчитанность меряется по всем сценариям, а не по старым колонкам.
        # Иначе новый сценарий не догнал бы накопленное никогда: у двенадцати тысяч
        # сообщений старый вердикт давно проставлен, а его собственного — нет вовсе,
        # и дешёвый инкрементальный прогон обходил бы их стороной каждый раз.
        # Заметно это было бы только по вечно пустому разделу второго конвейера.
        pending = or_(Message.cascade_level.is_(None), Message.cascade_passed.is_(None))
        if bound:
            settled = (select(WfVerdict.message_id)
                       .where(WfVerdict.workflow_id.in_([b.workflow.id for b in bound]),
                              WfVerdict.passed.isnot(None))
                       .group_by(WfVerdict.message_id)
                       .having(func.count() == len(bound)))
            pending = or_(pending, Message.id.notin_(settled))
        stmt = stmt.where(pending)
    return list((await db.execute(stmt)).scalars().all())


async def _stage_l2(db, waiting, verdicts, *, l3_enabled, report, cancelled,
                    base: float, ranked_out: dict[int, list],
                    channels: dict[int, Channel]) -> int:
    """Досчитать L2 у всех, кто ждёт вектора.

    Список ожидающих приходит снаружи: он собран по всем сценариям сразу, потому что
    вектор один на сообщение и считать его по разу на конвейер незачем.
    """
    if not waiting:
        return 0

    protos = await embeddings.prototype_vectors()
    passed = 0
    for start in range(0, len(waiting), EMBED_CHUNK):
        if cancelled():
            raise Cancelled()
        chunk = waiting[start:start + EMBED_CHUNK]
        vectors = await embeddings.embed([(m.text or "")[:2000] for m in chunk])
        for m, vec in zip(chunk, vectors):
            ranked = embeddings.rank(vec, protos)
            ranked_out[m.id] = ranked
            # Старые колонки обновляются только у тех, кого ждала старая же логика.
            # Сообщение, попавшее в пачку ради второго сценария, для неё отсеяно на
            # L1 — дописать ему вердикт L2 значило бы сдвинуть прежнее поведение.
            if not (verdicts[m.id]["passed"] is None and verdicts[m.id]["level"] == 1):
                continue
            # Флаг обхода обязателен и здесь: обходное сообщение ждёт вектора
            # с вердиктом «уровень 1, в пути», и достройка без флага судила бы его
            # словарём заново — то есть убивала бы то, что канал решил пропустить.
            v = cascade.classify(
                text=m.text, is_automatic_forward=m.is_automatic_forward,
                author_is_bot=m.author_is_bot, author_peer_id=m.author_peer_id,
                author_username=m.author_username, tg_date=m.tg_date,
                l2_enabled=True, l3_enabled=l3_enabled, ranked=ranked,
                l1_bypass=channels[m.channel_id].l1_bypass_enabled)
            verdicts[m.id] = v
            _apply(m, v)
            passed += int(v["passed"] is not False)
        done = min(start + EMBED_CHUNK, len(waiting))
        await report(base + WEIGHTS["l2"] * done / len(waiting),
                     f"L2: посчитано векторов {done} из {len(waiting)}")
    return passed


# Сколько соседних сообщений показываем модели: столько же до цели и после неё.
# Раньше брались «первые шесть» из окна ±12 сообщений — то есть в живой ветке почти
# всегда шесть реплик ДО цели, и то, что ответили после, модель не видела никогда.
# Для вопроса «на это уже ответили по существу» такой контекст бесполезен, а для
# личных сообщений просто беднее, чем мог бы быть.
L3_CONTEXT_BEFORE = 3
L3_CONTEXT_AFTER = 3


async def _context_for(db, m: Message) -> list[str]:
    """Соседи цели по ветке, поровну с двух сторон, в хронологическом порядке."""
    rows = await drafting.thread_context(db, m, around=2)
    at = next((i for i, c in enumerate(rows) if c["target"]), len(rows))
    before = [c["text"] for c in rows[:at]][-L3_CONTEXT_BEFORE:]
    after = [c["text"] for c in rows[at + 1:]][:L3_CONTEXT_AFTER]
    return before + after


async def _stage_l3(db, jobs, *, limit, report, cancelled, base: float,
                    llm_out: dict[int, dict[str, dict]],
                    llm_errors: dict[int, dict[str, str]]) -> int:
    """Задать модели все накопившиеся вопросы.

    На вход приходят пары «сообщение × вопрос», а не сообщения: у каждого контура на
    L3 свой промпт, и обращений к модели столько, сколько разных вопросов. Это дороже
    по времени карты и принято сознательно — разбор в `_l3_jobs`.

    Ответы складываются в `llm_out[message_id][prompt_key]`, ошибки — отдельно в
    `llm_errors`. Порознь потому, что это разные факты: «модель сказала нет» и
    «модель не ответила» ведут к разным вердиктам, и свалив их в один словарь, мы
    получили бы отказ там, где была недоступность своей же машины.

    Отмена не рвёт начатое: вопросы, уже ушедшие к модели, дожидаются ответа, и всё,
    что успел ответить моделям до остановки, попадает в `llm_out`/`llm_errors` и в
    трейсы — после чего стадия поднимает `Cancelled`. Ответов, заданных после отмены,
    не бывает.

    Возвращает число заданных вопросов.
    """
    if limit is not None and len(jobs) > limit:
        log.warning("L3: вопросов %s, разбираем первые %s", len(jobs), limit)
        jobs = jobs[:limit]
    if not jobs:
        return 0

    # Контекст веток собираем ДО параллельной части и последовательно. `AsyncSession`
    # не рассчитан на одновременное использование из нескольких задач: пока одна
    # выбирает соединение, вторая получает «concurrent operations are not permitted».
    # Контекст зависит от сообщения, а не от вопроса, — считаем по одному разу.
    contexts: dict[int, list[str]] = {}
    for m, _ in jobs:
        if m.id not in contexts:
            contexts[m.id] = await _context_for(db, m)

    sem = asyncio.Semaphore(L3_CONCURRENCY)
    done = {"n": 0}
    total = len(jobs)

    async def one(m: Message, prompt_key: str):
        async with sem:
            # Проверка — здесь, под семафором, а не до него. `gather` создаёт все
            # корутины разом: проверка до семафора проходится всеми в первую же
            # секунду, до своей очереди, и дальше корутина отмены не видит никогда —
            # на трёх тысячах вопросов стадия становится неотменяемой до конца.
            # Под семафором проверку проходит тот, кто реально получил слот: уже
            # ушедший к модели вопрос дожидается ответа (рвать соединение незачем),
            # а новые вопросы после отмены не задаются.
            if cancelled():
                raise Cancelled()
            try:
                parsed, trace = await llm.verdict(text=(m.text or "")[:2000],
                                                  context=contexts[m.id],
                                                  prompt_key=prompt_key)
            except llm.LlmUnavailable as e:
                parsed, trace = {"error": str(e)}, None
        done["n"] += 1
        if done["n"] % 10 == 0 or done["n"] == total:
            await report(base + WEIGHTS["l3"] * done["n"] / total,
                         f"L3: задано вопросов {done['n']} из {total}")
        return m, prompt_key, parsed, trace

    # `return_exceptions=True` обязателен: с ним gather дожидается всех корутин, и
    # первая `Cancelled` не обрывает сбор ответов. Иначе исключение вылетело бы
    # сразу, а соседние задачи — и уже готовые ответы, и трейсы сделанных вызовов —
    # остались бы несобранными: до 85 минут карты в никуда. Отменённые корутины
    # (те, что встали у семафора после отмены) возвращаются как `Cancelled` и
    # просто пропускаются — вопрос им уже никто не задавал.
    results = await asyncio.gather(*(one(m, key) for m, key in jobs),
                                   return_exceptions=True)

    stop: Cancelled | None = None
    crash: BaseException | None = None
    for r in results:
        if isinstance(r, Cancelled):
            stop = stop or r
        elif isinstance(r, BaseException):
            crash = crash or r
        else:
            m, prompt_key, parsed, trace = r
            if parsed.get("error"):
                llm_errors.setdefault(m.id, {})[prompt_key] = parsed["error"]
            else:
                llm_out.setdefault(m.id, {})[prompt_key] = parsed
            if trace is not None:
                db.add(LlmTrace(**trace))

    # Чужая ошибка важнее отмены: она означает поломку, которую нельзя маскировать
    # штатной остановкой. Ответы, собранные до неё, записаны выше — но коммита не
    # будет, прогон падает целиком, как и раньше.
    if crash is not None:
        raise crash
    if stop is not None:
        raise Cancelled()

    return total


def _apply_legacy_l3(messages: list[Message], verdicts: dict,
                     llm_out: dict[int, dict[str, dict]],
                     llm_errors: dict[int, dict[str, str]]) -> None:
    """Дописать ответ модели в старые колонки.

    Отдельно от `_stage_l3` потому, что теперь ступень обслуживает несколько контуров,
    а старые колонки — только один из них. Смешивать «спросить у модели» и «записать
    ответ прежней логике» в одном цикле значило бы каждый раз выяснять заново, чей
    именно ответ сейчас в руках.
    """
    for m in messages:
        if not _waits(verdicts[m.id], 2):
            continue
        parsed = llm_out.get(m.id, {}).get(LEGACY_PROMPT)
        error = llm_errors.get(m.id, {}).get(LEGACY_PROMPT)
        if parsed is None and error is None:
            # Вопрос не задавали вовсе: упёрлись в `limit` или прогон остановили.
            continue

        # Достраиваем вердикт, посчитанный на L2, а не пересобираем его через
        # classify: скор, боль и дисквалификаторы там уже посчитаны, и повторный
        # прогон означал бы два места, где они могут разойтись.
        v = dict(verdicts[m.id])
        v["detail"] = dict(v["detail"])

        if error is not None:
            # Модель не ответила или ответила неразбираемым. Это «не досчитали», а не
            # «не прошло»: сообщение остаётся на L2 со статусом «ожидает» и будет
            # переспрошено следующим прогоном. Записать отказ значило бы потерять лид
            # из-за недоступности своей же машины.
            v["detail"]["l3"] = f"ожидает: {error}"
            v["passed"] = None
        else:
            ok3, why3 = cascade.level3(parsed)
            v["detail"]["l3"] = why3
            v["level"], v["passed"] = 3, ok3
            if not ok3:
                v["pain"] = None

        verdicts[m.id] = v
        _apply(m, v)


async def _reconcile_leads(db, messages, verdicts, *, create_only: bool = False,
                           skipped: dict[str, int] | None = None) -> tuple[int, int, int]:
    """Привести очередь лидов в соответствие со свежими вердиктами.

    Лид, переставший проходить каскад, из очереди убирается — держать в ней то, что
    система больше не считает лидом, значит тратить время оператора на заведомый мусор.
    Но убрать его можно **только пока за него никто не брался**, и «взялся» здесь
    означает две разные вещи.

    Первая — статус лида: всё, кроме `new`, поставил человек, и переписывать чужое
    решение задним числом нельзя.

    Вторая — черновик. `drafts.lead_id` объявлен `NOT NULL` и без `ondelete`, то есть
    база физически запрещает удалить лид, у которого есть черновик. Это не досадное
    ограничение, а ровно то поведение, которое здесь нужно: черновик означает, что лид
    дошёл до человека. Раньше про это не знали, и `reclassify --scope all` падал
    нарушением ключа на первом же таком лиде, откатывая весь прогон целиком.

    Поэтому: неразобранный черновик (`pending`) удаляется вместе с лидом — решения в
    нём нет, терять нечего; разобранный оставляет лид на месте.

    `create_only` — частичный прогон (задан список каналов, контракт §6.1): только
    создание. Перезапись оценок и удаление пропускаются — решение о них принимается
    по вердикту, который в частичном прогоне мог измениться из-за новых правил
    отмеченных каналов, а не из-за самого лида. Сколько действий пропущено, функция
    кладёт в переданный словарь `skipped`: ключи `"updates"` и `"removals"`.
    Лид, который и в обычном режиме остался бы на месте (человеческий статус,
    решение по черновику, «ещё в пути»), считается `kept` — его ничего не
    пропускает, он и так не трогался.
    """
    leads = {row.message_id: row
             for row in (await db.execute(select(Lead))).scalars().all()}
    drafts = {row.lead_id: row
              for row in (await db.execute(select(Draft))).scalars().all()}
    created = removed = kept = 0
    skipped = {} if skipped is None else skipped

    for m in messages:
        v = verdicts[m.id]
        lead = leads.get(m.id)

        if v["passed"] and lead is None:
            db.add(Lead(
                message_id=m.id, channel_id=m.channel_id,
                author_peer_id=m.author_peer_id, author_username=m.author_username,
                author_name=m.author_name, pain=v["pain"],
                quote=(m.text or "")[:500], score=v["score"],
                score_breakdown=v["breakdown"], disqualifiers=v["disqualifiers"],
                status="new"))
            created += 1
        elif v["passed"] and lead is not None:
            if create_only:
                skipped["updates"] = skipped.get("updates", 0) + 1
            else:
                lead.score, lead.score_breakdown = v["score"], v["breakdown"]
                lead.pain, lead.disqualifiers = v["pain"], v["disqualifiers"]
                kept += 1
        elif not v["passed"] and lead is not None:
            # `passed is None` — «ещё в пути», это не повод удалять лид: сообщение
            # просто не досчитали, а не признали мусором.
            if v["passed"] is None:
                kept += 1
            elif lead.status != "new":
                log.warning("лид %s больше не проходит каскад, но статус «%s» — "
                            "оставлен как есть", lead.id, lead.status)
                kept += 1
            else:
                draft = drafts.get(lead.id)
                if draft is not None and draft.state != "pending":
                    if not create_only:
                        log.warning("лид %s больше не проходит каскад, но по нему уже "
                                    "есть решение по черновику («%s») — оставлен как есть",
                                    lead.id, draft.state)
                    kept += 1
                elif create_only:
                    # Частичный прогон удаляет только «на бумаге»: сюда доходят ровно
                    # те, кого обычный прогон удалил бы (статус `new`, черновика нет
                    # или он неразобранный).
                    skipped["removals"] = skipped.get("removals", 0) + 1
                else:
                    # Порядок важен: сначала черновик, потом лид. Обратный порядок —
                    # это то самое нарушение внешнего ключа, ради которого всё писалось.
                    if draft is not None:
                        await db.delete(draft)
                    await db.delete(lead)
                    removed += 1

    return created, removed, kept


async def _reconcile_targets(db, bound, messages, *, l2_enabled: bool, l3_enabled: bool,
                             ranked: dict[int, list],
                             llm_answers: dict[int, dict],
                             channels: dict[int, Channel],
                             create_only: bool = False) -> dict:
    """Записать вердикты сценариев и привести их цели в соответствие.

    Отдельным проходом после `_reconcile_leads`, а не внутри него: очередь лидов и
    цели сценариев — две независимые витрины над одними и теми же вердиктами, и
    смешивать их правила в одном цикле значило бы получить место, где починка одной
    молча меняет другую.

    Словарь каналов приходит выбранным в `run` до цикла L0/L1 — тот же запрос,
    что даёт флаг обхода L1; второй выбор всех каналов за прогон был бы вторым
    местом той же правды.

    `create_only` — частичный прогон: цели только создаются, перезапись и удаление
    пропускаются (`kept`), по той же причине, что и у очереди лидов.
    """
    if not bound:
        return {}

    wf_summary: dict = {}
    for m in messages:
        channel = channels.get(m.channel_id)
        if channel is None:
            # Внешний ключ этого не допускает. Если всё же случилось — сообщение
            # пропускается, но громко: тихий `continue` спрятал бы дыру в данных.
            log.error("сообщение %s ссылается на несуществующий канал %s",
                      m.id, m.channel_id)
            continue
        await targeting.sync_message(
            db, bound, message=m, channel=channel, l2_enabled=l2_enabled,
            l3_enabled=l3_enabled, ranked=ranked.get(m.id),
            llm_by_prompt=llm_answers.get(m.id),
            l1_bypass=channel.l1_bypass_enabled, create_only=create_only,
            summary=wf_summary)
    return wf_summary


async def run(db, *, l2_enabled: bool, l3_enabled: bool, l3_limit: int | None = None,
              scope: str = "all", channel_ids: list[int] | None = None,
              report=None, cancelled=None) -> dict:
    """Прогнать каскад и вернуть сводку.

    `channel_ids` сужает прогон перечисленными каналами (для любого охвата) —
    так наверстывается история одного канала после включения ему обхода L1.
    Задан список — прогон идёт в режиме «только создавать»: существующие лиды
    и цели не перезаписываются и не удаляются, пропущенные действия считаются
    в `skipped_updates`/`skipped_removals` (контракт §6.1: частичный охват не
    даёт права выносить приговор тому, что прогон не пересчитывал). Связка
    «список задан ⇒ create-only» жёсткая, отдельным параметром не разносится.

    Коммит делается один раз в конце — включая случай отмены: то, что успели
    посчитать, терять незачем, а частично разобранный поток ничем не хуже
    неразобранного.
    """
    if scope not in SCOPES:
        raise ValueError(f"неизвестный охват «{scope}», ожидается один из {SCOPES}")
    report = report or _noop_report
    cancelled = cancelled or _never_cancelled
    create_only = channel_ids is not None

    # Профили ищутся до первой записи и до выборки: чужой ключ профиля в реестре
    # должен уронить прогон на старте, а не на середине, оставив половину сообщений
    # пересчитанной. Выборке же реестр нужен, чтобы понимать, кто ещё не досчитан.
    bound = await targeting.bind_active(db)

    messages = await _select_messages(db, scope, bound, channel_ids=channel_ids)
    summary = {"scope": scope, "messages": len(messages), "l2": l2_enabled,
               "l3": l3_enabled, "cancelled": False,
               "created": 0, "removed": 0, "kept": 0,
               "skipped_updates": 0, "skipped_removals": 0,
               "l3_questions": 0, "workflows": {}}
    if not messages:
        await report(100, "нечего пересчитывать")
        return summary

    # Каналы — одним запросом до цикла L0/L1: отсюда берётся флаг обхода L1 для
    # обеих ступеней и обоих конвейеров, а заодно и канал для целей сценариев.
    channels = {c.id: c for c in
                (await db.execute(select(Channel))).scalars().all()}

    was_cancelled = False
    verdicts: dict[int, dict] = {}
    ranked: dict[int, list] = {}
    # Ответы модели по сообщению и вопросу: у каждого контура на L3 свой промпт.
    llm_answers: dict[int, dict[str, dict]] = {}
    llm_errors: dict[int, dict[str, str]] = {}
    try:
        await report(0, f"сообщений в работе: {len(messages)}")
        for m in messages:
            v = _run_l0l1(m, l2_enabled=l2_enabled, l3_enabled=l3_enabled,
                          l1_bypass=channels[m.channel_id].l1_bypass_enabled)
            verdicts[m.id] = v
            _apply(m, v)
        alive = sum(1 for v in verdicts.values() if v["passed"] is not False)
        await report(WEIGHTS["l0l1"], f"после L0/L1 в живых: {alive}")

        base = WEIGHTS["l0l1"]
        if l2_enabled:
            wf_now = _wf_verdicts(bound, messages, l2_enabled=l2_enabled,
                                  l3_enabled=l3_enabled, ranked=ranked,
                                  llm_answers=llm_answers, channels=channels)
            await _stage_l2(db, _waiting_at(1, messages, verdicts, wf_now, bound),
                            verdicts, l3_enabled=l3_enabled, report=report,
                            cancelled=cancelled, base=base, ranked_out=ranked,
                            channels=channels)
        base += WEIGHTS["l2"]
        if l3_enabled:
            wf_now = _wf_verdicts(bound, messages, l2_enabled=l2_enabled,
                                  l3_enabled=l3_enabled, ranked=ranked,
                                  llm_answers=llm_answers, channels=channels)
            asked = await _stage_l3(
                db, _l3_jobs(messages, verdicts, wf_now, bound), limit=l3_limit,
                report=report, cancelled=cancelled, base=base,
                llm_out=llm_answers, llm_errors=llm_errors)
            summary["l3_questions"] = asked
            _apply_legacy_l3(messages, verdicts, llm_answers, llm_errors)
    except Cancelled:
        was_cancelled = True
        summary["cancelled"] = True
        # Ответы, успевшие прийти до отмены, обязаны стать вердиктами здесь же.
        # `_stage_l3` прерван и сам `_apply_legacy_l3` не вызывает: без этого
        # прохода отвеченные сообщения остались бы «в пути», и следующий прогон
        # переспросил бы модель то, за что уже заплачено. Для отмены до L3 функция
        # ничего не меняет: без ответов в `llm_answers` она никуда не пишет.
        _apply_legacy_l3(messages, verdicts, llm_answers, llm_errors)
        await report(None, "остановлено; посчитанное сохранено")

    skipped: dict[str, int] = {}
    created, removed, kept = await _reconcile_leads(db, messages, verdicts,
                                                    create_only=create_only,
                                                    skipped=skipped)
    summary.update(created=created, removed=removed, kept=kept)
    summary["skipped_updates"] = skipped.get("updates", 0)
    summary["skipped_removals"] = skipped.get("removals", 0)
    summary["workflows"] = await _reconcile_targets(
        db, bound, messages, l2_enabled=l2_enabled, l3_enabled=l3_enabled,
        ranked=ranked, llm_answers=llm_answers, channels=channels,
        create_only=create_only)

    # Счётчик лидов в канале — производная величина. При частичном охвате пересчитать
    # её по одним лишь тронутым сообщениям нельзя: получится «в канале два лида»
    # вместо сорока. Поэтому считаем по базе, а не по выборке прогона.
    counts = dict((await db.execute(
        select(Message.channel_id, func.count(Message.id))
        .where(Message.cascade_passed.is_(True))
        .group_by(Message.channel_id))).all())
    for channel in channels.values():
        channel.leads_total = counts.get(channel.id, 0)

    await db.commit()
    if not was_cancelled:
        await report(100, f"готово: лидов создано {created}, удалено {removed}, "
                          f"оставлено {kept}")
    return summary
