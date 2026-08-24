"""Перенос накопленных данных в многопоточную схему.

Что переносится, и куда:

    messages.cascade_*   → wf_verdicts   (вердикт на пару сообщение+сценарий)
    leads                → wf_targets    (цель, обобщение лида)
    drafts               → wf_drafts
    outbound_attempts    → wf_outbound

Всё уходит в сценарий ЛС (`cold_dm`) — установка работает ровно по нему, других
сценариев до сих пор не существовало.

Три решения, которые стоит понимать перед запуском:

* **По умолчанию — сухой прогон.** Скрипт считает и показывает, что сделает, и
  ничего не пишет. Запись включается `--apply` явно. Схема Radar не знает Alembic:
  здесь нет отката одной командой, поэтому цена ошибки выше обычной.
* **Идемпотентно.** Уже перенесённые строки пропускаются, повторный запуск ничего
  не дублирует. Прерванный на середине прогон дозапускается тем же вызовом.
* **Ничего не удаляется.** Старые таблицы и колонки остаются нетронутыми — это
  единственная страховка на случай, если перенос окажется неверным. Их удаление —
  отдельный осознанный шаг после того, как новая схема поработает.

Запуск:

    docker exec api-radar python -m scripts.migrate_to_workflows            # посмотреть
    docker exec api-radar python -m scripts.migrate_to_workflows --apply    # перенести
"""
from __future__ import annotations

import argparse
import asyncio
import logging

from sqlalchemy import func, select

from app.db.models import (Draft, Lead, Message, OutboundAttempt, WfDraft,
                           WfOutbound, WfTarget, WfVerdict, Workflow)
from app.db.session import get_session_maker
from app.services import engage_registry, workflows

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("migrate")

BATCH = 1000


async def _target_workflow(db, apply: bool) -> Workflow:
    """Сценарий, в который всё переезжает. Заводим, если его ещё нет.

    В сухом прогоне не создаём ничего — даже реестр. Сухой прогон, который втихую
    пишет две строки, хуже отсутствующего: один раз обнаружив такое, ему перестают
    верить, а верить ему надо именно в тот момент, когда данных жалко.

    Тогда возвращается заготовка с несуществующим `id`: запросы по ней ничего не
    находят, и счётчики показывают ровно то, что будет перенесено на пустое место.
    """
    wf = await workflows.by_key(db, "cold_dm")
    if wf is not None:
        return wf

    if not apply:
        log.info("сценарий cold_dm будет создан (сухой прогон — не создаю)")
        return Workflow(id=-1, key="cold_dm", title="Личные сообщения",
                        target_kind="user", action="dm", visibility="private",
                        engage_instance_id=-1, engage_use_case="cold_dm",
                        cascade_profile="dm_v1")

    await engage_registry.ensure_bootstrap(db)
    await workflows.ensure_bootstrap(db)
    wf = await workflows.by_key(db, "cold_dm")
    if wf is None:
        raise SystemExit(
            "сценарий cold_dm не заведён и не создался автоматически — вероятно, пуст "
            "реестр инстансов Engage; проверьте RADAR_ENGAGE_BASE_URL"
        )
    return wf


async def migrate_verdicts(db, wf: Workflow, apply: bool) -> tuple[int, int]:
    """messages.cascade_* → wf_verdicts.

    Переносим только сообщения, которые каскад уже трогал: `cascade_level IS NOT NULL`.
    Нетронутые не переносим вовсе — «нет строки вердикта» и «вердикт есть, но пустой»
    означают разное, и второе соврало бы переклассификации, что считать нечего.
    """
    done = set((await db.execute(
        select(WfVerdict.message_id).where(WfVerdict.workflow_id == wf.id)
    )).scalars().all())

    total = (await db.execute(select(func.count(Message.id))
                              .where(Message.cascade_level.isnot(None)))).scalar_one()
    moved = 0
    last_id = 0
    while True:
        rows = (await db.execute(
            select(Message)
            .where(Message.cascade_level.isnot(None), Message.id > last_id)
            .order_by(Message.id).limit(BATCH))).scalars().all()
        if not rows:
            break
        last_id = rows[-1].id
        for m in rows:
            if m.id in done:
                continue
            moved += 1
            if apply:
                db.add(WfVerdict(
                    workflow_id=wf.id, message_id=m.id,
                    level=m.cascade_level, passed=m.cascade_passed,
                    detail=m.cascade_detail, computed_at=m.processed_at,
                ))
        if apply:
            await db.commit()
    return moved, total


async def migrate_targets(db, wf: Workflow, apply: bool) -> tuple[int, int]:
    """leads → wf_targets.

    `target_kind='user'`, адресат — автор лида: у ЛС цель это человек, и старая схема
    ровно его и хранила (`author_peer_id` там был NOT NULL).
    """
    done = set((await db.execute(
        select(WfTarget.message_id).where(WfTarget.workflow_id == wf.id)
    )).scalars().all())

    total = (await db.execute(select(func.count(Lead.id)))).scalar_one()
    moved = 0
    last_id = 0
    while True:
        rows = (await db.execute(
            select(Lead).where(Lead.id > last_id)
            .order_by(Lead.id).limit(BATCH))).scalars().all()
        if not rows:
            break
        last_id = rows[-1].id
        for lead in rows:
            if lead.message_id in done:
                continue
            moved += 1
            if apply:
                db.add(WfTarget(
                    workflow_id=wf.id, target_kind="user",
                    message_id=lead.message_id, channel_id=lead.channel_id,
                    recipient_peer_id=lead.author_peer_id,
                    author_peer_id=lead.author_peer_id,
                    author_username=lead.author_username,
                    author_name=lead.author_name,
                    pain=lead.pain, quote=lead.quote,
                    score=lead.score or 0,
                    score_breakdown=lead.score_breakdown,
                    disqualifiers=lead.disqualifiers,
                    status=lead.status, reject_reason=lead.reject_reason,
                    created_at=lead.created_at, updated_at=lead.updated_at,
                ))
        if apply:
            await db.commit()
    return moved, total


async def migrate_drafts(db, wf: Workflow, apply: bool) -> tuple[int, int, int]:
    """drafts → wf_drafts. Возвращает (перенесено, всего, осиротело).

    Связь `draft.lead_id → lead.message_id → wf_target` строится через сообщение:
    идентификаторы целей новые, и опираться на старые нельзя.

    Осиротевшие — черновики, чей лид не доехал (например, был удалён
    переклассификацией). Их не выдумываем и не переносим: черновик без цели не
    открывается ни на одном экране, и тихо создать его значило бы завести мусор,
    который потом кто-то будет отлаживать.
    """
    lead_to_message = dict((await db.execute(
        select(Lead.id, Lead.message_id))).all())
    message_to_target = dict((await db.execute(
        select(WfTarget.message_id, WfTarget.id)
        .where(WfTarget.workflow_id == wf.id))).all())
    done = set((await db.execute(
        select(WfDraft.target_id).where(WfDraft.workflow_id == wf.id)
    )).scalars().all())

    total = (await db.execute(select(func.count(Draft.id)))).scalar_one()
    moved = orphaned = 0
    last_id = 0
    while True:
        rows = (await db.execute(
            select(Draft).where(Draft.id > last_id)
            .order_by(Draft.id).limit(BATCH))).scalars().all()
        if not rows:
            break
        last_id = rows[-1].id
        for d in rows:
            message_id = lead_to_message.get(d.lead_id)
            target_id = message_to_target.get(message_id) if message_id else None
            if target_id is None:
                orphaned += 1
                continue
            if target_id in done:
                continue
            moved += 1
            if apply:
                db.add(WfDraft(
                    workflow_id=wf.id, target_id=target_id,
                    variants=d.variants, thread_context=d.thread_context,
                    chosen_variant=d.chosen_variant, final_text=d.final_text,
                    state=d.state, reject_reason=d.reject_reason,
                    decided_by=d.decided_by, decided_at=d.decided_at,
                    prompt_version=d.prompt_version,
                    source_message_link=d.source_message_link,
                    created_at=d.created_at, updated_at=d.updated_at,
                ))
                done.add(target_id)
        if apply:
            await db.commit()
    return moved, total, orphaned


async def migrate_outbound(db, wf: Workflow, apply: bool) -> tuple[int, int]:
    """outbound_attempts → wf_outbound.

    Таблица сегодня пуста — писателя у неё нет, отправки не существует. Перенос всё
    равно нужен: если он понадобится позже, делать его руками в спешке хуже, чем
    иметь готовым сейчас.
    """
    total = (await db.execute(select(func.count(OutboundAttempt.id)))).scalar_one()
    if total == 0:
        return 0, 0

    old_draft_to_new = {}
    lead_to_message = dict((await db.execute(select(Lead.id, Lead.message_id))).all())
    message_to_target = dict((await db.execute(
        select(WfTarget.message_id, WfTarget.id)
        .where(WfTarget.workflow_id == wf.id))).all())
    target_to_draft = dict((await db.execute(
        select(WfDraft.target_id, WfDraft.id)
        .where(WfDraft.workflow_id == wf.id))).all())
    for old_id, lead_id in (await db.execute(select(Draft.id, Draft.lead_id))).all():
        message_id = lead_to_message.get(lead_id)
        target_id = message_to_target.get(message_id) if message_id else None
        if target_id is not None:
            old_draft_to_new[old_id] = (target_id, target_to_draft.get(target_id))

    already = (await db.execute(select(func.count(WfOutbound.id))
                                .where(WfOutbound.workflow_id == wf.id))).scalar_one()
    if already:
        log.info("wf_outbound уже содержит %s строк — пропускаю", already)
        return 0, total

    moved = 0
    rows = (await db.execute(select(OutboundAttempt))).scalars().all()
    for a in rows:
        target_id, draft_id = old_draft_to_new.get(a.draft_id, (None, None))
        moved += 1
        if apply:
            db.add(WfOutbound(
                workflow_id=wf.id, target_id=target_id, draft_id=draft_id,
                conversation_id=a.conversation_id, account_id=a.account_id,
                allowed=a.allowed, reasons=a.reasons, mode=a.mode,
                delivered_message_id=a.delivered_message_id,
                text_snapshot=a.text_snapshot, created_at=a.created_at,
            ))
    if apply:
        await db.commit()
    return moved, total


async def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="действительно записать (по умолчанию — сухой прогон)")
    args = ap.parse_args()

    if not args.apply:
        log.info("СУХОЙ ПРОГОН — ничего не записывается. Для записи добавьте --apply")

    async with get_session_maker()() as db:
        wf = await _target_workflow(db, args.apply)
        log.info("сценарий назначения: %s (id=%s)", wf.key, wf.id)

        moved, total = await migrate_verdicts(db, wf, args.apply)
        log.info("вердикты:  %s из %s обработанных сообщений", moved, total)

        moved, total = await migrate_targets(db, wf, args.apply)
        log.info("цели:      %s из %s лидов", moved, total)

        if not args.apply and moved:
            # В сухом прогоне целей в базе нет, поэтому связать с ними черновики
            # нечем. Честно говорим об этом, а не показываем ноль как результат.
            log.info("черновики: подсчёт невозможен в сухом прогоне — цели ещё не "
                     "созданы, а связь строится через них")
            log.info("исходящие: то же самое")
        else:
            moved, total, orphaned = await migrate_drafts(db, wf, args.apply)
            log.info("черновики: %s из %s", moved, total)
            if orphaned:
                log.warning("осиротевших черновиков (лид не найден): %s — не перенесены",
                            orphaned)
            moved, total = await migrate_outbound(db, wf, args.apply)
            log.info("исходящие: %s из %s", moved, total)

    log.info("готово%s", "" if args.apply else " (ничего не записано)")


if __name__ == "__main__":
    asyncio.run(main())
