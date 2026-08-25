"""Вердикты и цели по каждому действующему сценарию.

Здесь живёт то, чего не хватало для второго конвейера. Приём и переклассификация
умеют ровно одно: прогнать сообщение по каскаду **один раз** и завести по нему лид.
Пока сценарий один, разницы нет; как только их два, «один раз» означает, что второй
сценарий не увидит ни одного сообщения — ни нового, ни из накопленных двенадцати
тысяч.

Модуль считает вердикт **на каждую пару (сообщение, сценарий)** и заводит цели в
`wf_targets`. Дорогие входы считаются заранее и передаются сюда готовыми, но делятся
между контурами по-разному, и разница принципиальная:

* **вектор — общий.** Эмбеддинг есть функция текста, от сценария он не зависит.
  Считать его по разу на конвейер значило бы платить за один и тот же результат.
* **ответ модели — свой у каждого контура.** Решение Ивана от 25.08: у сценариев
  свои промпты, и один ответ на всех не раздаётся, даже когда это дешевле. Мы на
  стадии разработки, поведение модели при таком совмещении не измерено, и вопрос,
  заданный про личное сообщение, не должен определять отбор для публичного ответа.
  Поэтому сюда приходит `llm_by_prompt` — ответы по ключу промпта, и каждый профиль
  берёт оттуда свой.

`cascade.classify` при этом вызывается по разу на профиль: он чистая функция, и на
готовых входах стоит микросекунды.

**Старые `messages.cascade_*` и `leads` этот модуль не трогает.** Они продолжают
писаться прежним кодом, слово в слово, и остаются страховкой на откат: пока экраны
не переехали на новые таблицы, отключать старую запись нечем. Разойтись с целями
сценария ЛС они не могут: `cascade.classify` — чистая функция, и на одних входах с
одним профилем она даёт один результат.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.core import cascade, clock
from app.db.models import Channel, Message, WfDraft, WfTarget, WfVerdict, Workflow
from app.services import workflows as workflow_service

logger = logging.getLogger("radar.targeting")


@dataclass(frozen=True)
class Bound:
    """Сценарий вместе с уже найденным профилем каскада.

    Пара держится вместе намеренно: профиль ищется по строковому ключу из базы и
    может не найтись (`UnknownProfileError`). Искать его один раз на прогон, а не на
    каждое сообщение, значит и падать один раз — на старте, а не на пятитысячном
    сообщении посреди записи.
    """

    workflow: Workflow
    profile: cascade.CascadeProfile


async def bind_active(db) -> list[Bound]:
    """Действующие сценарии с их профилями, в порядке меню."""
    return [Bound(wf, cascade.profile(wf.cascade_profile))
            for wf in await workflow_service.active(db)]


def verdict_for(bound: Bound, message: Message, *, l2_enabled: bool, l3_enabled: bool,
                ranked: list[tuple[str, str, float]] | None = None,
                llm: dict | None = None, now: datetime | None = None) -> dict:
    """Вердикт каскада по одному сообщению в правилах одного сценария."""
    return cascade.classify(
        text=message.text, is_automatic_forward=message.is_automatic_forward,
        author_is_bot=message.author_is_bot, author_peer_id=message.author_peer_id,
        author_username=message.author_username, tg_date=message.tg_date, now=now,
        l2_enabled=l2_enabled, l3_enabled=l3_enabled, ranked=ranked, llm=llm,
        profile=bound.profile)


async def save_verdict(db, *, workflow_id: int, message_id: int, verdict: dict) -> None:
    """Записать вердикт пары (сценарий, сообщение).

    Через `ON CONFLICT`, а не «выбрать и обновить»: приём идёт двумя путями сразу
    (бэкфилл и вотчер), и одно сообщение приходит с двух сторон одновременно.
    """
    values = {
        "workflow_id": workflow_id, "message_id": message_id,
        "level": verdict["level"], "passed": verdict["passed"],
        "detail": verdict["detail"], "pain": verdict["pain"],
        "score": verdict["score"], "score_breakdown": verdict["breakdown"],
        "disqualifiers": verdict["disqualifiers"],
        "computed_at": clock.utcnow(),
    }
    await db.execute(
        pg_insert(WfVerdict).values(**values)
        .on_conflict_do_update(index_elements=["workflow_id", "message_id"],
                               set_={k: v for k, v in values.items()
                                     if k not in ("workflow_id", "message_id")}))


def addressing(bound: Bound, message: Message, channel: Channel) -> dict | None:
    """Куда сценарий будет обращаться по этому сообщению.

    `None` означает «по этому сообщению обратиться нечем» — цель не заводится вовсе.
    Такое бывает у ЛС: пост от анонимного админа проходит отбор по тексту, но писать
    в личку некому. Завести цель без адреса нельзя — `ck_target_addressing` не даст,
    и правильно сделает: недозаполненная цель дожила бы до отправки и упала там.
    """
    if bound.workflow.target_kind == "user":
        if message.author_peer_id is None:
            return None
        return {"recipient_peer_id": message.author_peer_id}
    # Ответ в ветке уходит туда же, откуда пришло сообщение: строка `channels`
    # заводится по `chat_id` события, то есть это и есть группа обсуждения, а не
    # сам канал.
    return {"chat_peer_id": channel.peer_id,
            "reply_to_message_id": message.tg_message_id}


async def _existing_target(db, *, workflow_id: int, message_id: int) -> WfTarget | None:
    return (await db.execute(
        select(WfTarget).where(WfTarget.workflow_id == workflow_id,
                               WfTarget.message_id == message_id))).scalar_one_or_none()


async def _drop_target(db, target: WfTarget) -> bool:
    """Убрать цель, за которую никто не брался. Возвращает, убрали ли.

    Правила те же, что у очереди лидов, и по той же причине: решение человека нельзя
    переписывать задним числом. «Взялся» — это либо статус цели не `new`, либо
    разобранный черновик по ней.

    Порядок удаления обратный порядку ссылок: `wf_drafts.target_id` объявлен
    `NOT NULL`, поэтому сначала черновик, потом цель. Обратный порядок — нарушение
    внешнего ключа, откатывающее весь прогон.
    """
    if target.status != "new":
        logger.warning("цель %s больше не проходит отбор, но статус «%s» — оставлена",
                       target.id, target.status)
        return False

    draft = (await db.execute(
        select(WfDraft).where(WfDraft.target_id == target.id))).scalar_one_or_none()
    if draft is not None and draft.state != "pending":
        logger.warning("цель %s больше не проходит отбор, но по её черновику уже есть "
                       "решение («%s») — оставлена", target.id, draft.state)
        return False

    if draft is not None:
        await db.delete(draft)
    await db.delete(target)
    return True


async def sync_target(db, bound: Bound, *, message: Message, channel: Channel,
                      verdict: dict) -> str:
    """Привести цель сценария в соответствие со свежим вердиктом.

    Возвращает, что сделали: `created`, `updated`, `removed`, `unaddressable` или
    `kept`. Строкой, а не булевым: вызывающая сторона считает по ним сводку, и «цель
    не завелась, потому что писать некому» — совсем не то же самое, что «цель не
    завелась, потому что сообщение не прошло отбор».
    """
    target = await _existing_target(db, workflow_id=bound.workflow.id,
                                    message_id=message.id)

    if verdict["passed"] is None:
        # «Ещё в пути»: ступень включена, но её вход не посчитан. Это не повод ни
        # заводить цель, ни убирать заведённую.
        return "kept"

    if not verdict["passed"]:
        if target is None:
            return "kept"
        return "removed" if await _drop_target(db, target) else "kept"

    if target is not None:
        target.pain = verdict["pain"]
        target.score = verdict["score"]
        target.score_breakdown = verdict["breakdown"]
        target.disqualifiers = verdict["disqualifiers"]
        return "updated"

    address = addressing(bound, message, channel)
    if address is None:
        return "unaddressable"

    db.add(WfTarget(
        workflow_id=bound.workflow.id, target_kind=bound.workflow.target_kind,
        message_id=message.id, channel_id=channel.id,
        author_peer_id=message.author_peer_id,
        author_username=message.author_username, author_name=message.author_name,
        pain=verdict["pain"], quote=(message.text or "")[:500],
        score=verdict["score"], score_breakdown=verdict["breakdown"],
        disqualifiers=verdict["disqualifiers"], status="new", **address))
    await db.flush()
    return "created"


async def sync_message(db, bound_list: list[Bound], *, message: Message,
                       channel: Channel, l2_enabled: bool, l3_enabled: bool,
                       ranked: list[tuple[str, str, float]] | None = None,
                       llm_by_prompt: dict[str, dict] | None = None,
                       now: datetime | None = None,
                       summary: dict | None = None) -> dict:
    """Посчитать и записать вердикт и цель по одному сообщению во всех сценариях.

    `llm_by_prompt` — ответы модели по этому сообщению, по ключу промпта. Каждый
    профиль берёт оттуда свой и только свой: чужой ответ здесь означал бы отбор по
    вопросу, который задавали не про этот контур.

    Сводка накапливается по ключу сценария, а не суммой: «завели 40 целей» на двух
    конвейерах не говорит ничего — 40 и 0 и 20 и 20 выглядят одинаково.
    """
    summary = {} if summary is None else summary
    answers = llm_by_prompt or {}
    for bound in bound_list:
        verdict = verdict_for(bound, message, l2_enabled=l2_enabled,
                              l3_enabled=l3_enabled, ranked=ranked,
                              llm=answers.get(bound.profile.l3_prompt_key), now=now)
        await save_verdict(db, workflow_id=bound.workflow.id, message_id=message.id,
                           verdict=verdict)
        outcome = await sync_target(db, bound, message=message, channel=channel,
                                    verdict=verdict)
        per_wf = summary.setdefault(bound.workflow.key, {})
        per_wf[outcome] = per_wf.get(outcome, 0) + 1
    return summary
