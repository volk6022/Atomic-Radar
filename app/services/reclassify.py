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
from app.db.models import Channel, Draft, Lead, LlmTrace, Message
from app.services import drafting, embeddings, llm

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


def _run_l0l1(m: Message, *, l2_enabled: bool, l3_enabled: bool) -> dict:
    return cascade.classify(
        text=m.text, is_automatic_forward=m.is_automatic_forward,
        author_is_bot=m.author_is_bot, author_peer_id=m.author_peer_id,
        author_username=m.author_username, tg_date=m.tg_date,
        l2_enabled=l2_enabled, l3_enabled=l3_enabled)


def _apply(m: Message, v: dict) -> None:
    m.cascade_level, m.cascade_passed = v["level"], v["passed"]
    m.cascade_detail = v["detail"]


async def _select_messages(db, scope: str) -> list[Message]:
    stmt = select(Message)
    if scope == "pending":
        # Только то, что ещё не досчитано: новое из ингеста и застрявшее на ступени,
        # которая в прошлый раз была недоступна. На двенадцати тысячах сообщений это
        # разница между десятками минут и парой секунд.
        stmt = stmt.where(or_(Message.cascade_level.is_(None),
                              Message.cascade_passed.is_(None)))
    return list((await db.execute(stmt)).scalars().all())


async def _stage_l2(db, messages, verdicts, *, l3_enabled, report, cancelled,
                    base: float) -> int:
    """Досчитать L2 у всех, кто ждёт вектора."""
    waiting = [m for m in messages if verdicts[m.id]["passed"] is None
               and verdicts[m.id]["level"] == 1]
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
            v = cascade.classify(
                text=m.text, is_automatic_forward=m.is_automatic_forward,
                author_is_bot=m.author_is_bot, author_peer_id=m.author_peer_id,
                author_username=m.author_username, tg_date=m.tg_date,
                l2_enabled=True, l3_enabled=l3_enabled, ranked=ranked)
            verdicts[m.id] = v
            _apply(m, v)
            passed += int(v["passed"] is not False)
        done = min(start + EMBED_CHUNK, len(waiting))
        await report(base + WEIGHTS["l2"] * done / len(waiting),
                     f"L2: посчитано векторов {done} из {len(waiting)}")
    return passed


async def _stage_l3(db, messages, verdicts, *, limit, report, cancelled,
                    base: float) -> int:
    """Спросить модель по всем, кто дошёл до L3."""
    waiting = [m for m in messages if verdicts[m.id]["passed"] is None
               and verdicts[m.id]["level"] == 2]
    if limit is not None and len(waiting) > limit:
        log.warning("L3: кандидатов %s, разбираем первые %s", len(waiting), limit)
        waiting = waiting[:limit]
    if not waiting:
        return 0

    # Контекст веток собираем ДО параллельной части и последовательно. `AsyncSession`
    # не рассчитан на одновременное использование из нескольких задач: пока одна
    # выбирает соединение, вторая получает «concurrent operations are not permitted».
    contexts: dict[int, list[str]] = {}
    for m in waiting:
        contexts[m.id] = [c["text"] for c in await drafting.thread_context(db, m, around=2)
                          if not c["target"]][:6]

    sem = asyncio.Semaphore(L3_CONCURRENCY)
    done = {"n": 0}
    total = len(waiting)

    async def one(m: Message):
        if cancelled():
            raise Cancelled()
        async with sem:
            try:
                parsed, trace = await llm.verdict(text=(m.text or "")[:2000],
                                                  context=contexts[m.id])
            except llm.LlmUnavailable as e:
                parsed, trace = {"error": str(e)}, None
        done["n"] += 1
        if done["n"] % 10 == 0 or done["n"] == total:
            await report(base + WEIGHTS["l3"] * done["n"] / total,
                         f"L3: разобрано моделью {done['n']} из {total}")
        return m, parsed, trace

    results = await asyncio.gather(*(one(m) for m in waiting))

    for m, parsed, trace in results:
        # Достраиваем вердикт, посчитанный на L2, а не пересобираем его через
        # classify: скор, боль и дисквалификаторы там уже посчитаны, и повторный
        # прогон означал бы два места, где они могут разойтись.
        v = dict(verdicts[m.id])
        v["detail"] = dict(v["detail"])

        if parsed.get("error"):
            # Модель не ответила или ответила неразбираемым. Это «не досчитали», а не
            # «не прошло»: сообщение остаётся на L2 со статусом «ожидает» и будет
            # переспрошено следующим прогоном. Записать отказ значило бы потерять лид
            # из-за недоступности своей же машины.
            v["detail"]["l3"] = f"ожидает: {parsed['error']}"
            v["passed"] = None
        else:
            ok3, why3 = cascade.level3(parsed)
            v["detail"]["l3"] = why3
            v["level"], v["passed"] = 3, ok3
            if not ok3:
                v["pain"] = None

        verdicts[m.id] = v
        _apply(m, v)
        if trace is not None:
            db.add(LlmTrace(**trace))

    return sum(1 for m in waiting if verdicts[m.id]["passed"])


async def _reconcile_leads(db, messages, verdicts) -> tuple[int, int, int]:
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
    """
    leads = {row.message_id: row
             for row in (await db.execute(select(Lead))).scalars().all()}
    drafts = {row.lead_id: row
              for row in (await db.execute(select(Draft))).scalars().all()}
    created = removed = kept = 0

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
                    log.warning("лид %s больше не проходит каскад, но по нему уже есть "
                                "решение по черновику («%s») — оставлен как есть",
                                lead.id, draft.state)
                    kept += 1
                else:
                    # Порядок важен: сначала черновик, потом лид. Обратный порядок —
                    # это то самое нарушение внешнего ключа, ради которого всё писалось.
                    if draft is not None:
                        await db.delete(draft)
                    await db.delete(lead)
                    removed += 1

    return created, removed, kept


async def run(db, *, l2_enabled: bool, l3_enabled: bool, l3_limit: int | None = None,
              scope: str = "all", report=None, cancelled=None) -> dict:
    """Прогнать каскад и вернуть сводку.

    Коммит делается один раз в конце — включая случай отмены: то, что успели
    посчитать, терять незачем, а частично разобранный поток ничем не хуже
    неразобранного.
    """
    if scope not in SCOPES:
        raise ValueError(f"неизвестный охват «{scope}», ожидается один из {SCOPES}")
    report = report or _noop_report
    cancelled = cancelled or _never_cancelled

    messages = await _select_messages(db, scope)
    summary = {"scope": scope, "messages": len(messages), "l2": l2_enabled,
               "l3": l3_enabled, "cancelled": False,
               "created": 0, "removed": 0, "kept": 0}
    if not messages:
        await report(100, "нечего пересчитывать")
        return summary

    was_cancelled = False
    verdicts: dict[int, dict] = {}
    try:
        await report(0, f"сообщений в работе: {len(messages)}")
        for m in messages:
            v = _run_l0l1(m, l2_enabled=l2_enabled, l3_enabled=l3_enabled)
            verdicts[m.id] = v
            _apply(m, v)
        alive = sum(1 for v in verdicts.values() if v["passed"] is not False)
        await report(WEIGHTS["l0l1"], f"после L0/L1 в живых: {alive}")

        base = WEIGHTS["l0l1"]
        if l2_enabled:
            await _stage_l2(db, messages, verdicts, l3_enabled=l3_enabled,
                            report=report, cancelled=cancelled, base=base)
        base += WEIGHTS["l2"]
        if l3_enabled:
            await _stage_l3(db, messages, verdicts, limit=l3_limit,
                            report=report, cancelled=cancelled, base=base)
    except Cancelled:
        was_cancelled = True
        summary["cancelled"] = True
        await report(None, "остановлено; посчитанное сохранено")

    created, removed, kept = await _reconcile_leads(db, messages, verdicts)
    summary.update(created=created, removed=removed, kept=kept)

    # Счётчик лидов в канале — производная величина. При частичном охвате пересчитать
    # её по одним лишь тронутым сообщениям нельзя: получится «в канале два лида»
    # вместо сорока. Поэтому считаем по базе, а не по выборке прогона.
    counts = dict((await db.execute(
        select(Message.channel_id, func.count(Message.id))
        .where(Message.cascade_passed.is_(True))
        .group_by(Message.channel_id))).all())
    for channel in (await db.execute(select(Channel))).scalars().all():
        channel.leads_total = counts.get(channel.id, 0)

    await db.commit()
    if not was_cancelled:
        await report(100, f"готово: лидов создано {created}, удалено {removed}, "
                          f"обновлено {kept}")
    return summary
