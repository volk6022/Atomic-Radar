"""Догнать решения из старого контура `drafts` в `wf_drafts` сценария `cold_dm`.

Зачем отдельный прогон, если черновики уже переносил `scripts/migrate_to_workflows.py`.
Миграция копировала состояние строк **на момент запуска**: тогда все 151 черновик ещё
жили в очереди, и в `wf_drafts` они легли `pending`. После переноса заказчик разобрал
очередь в СТАРОМ экране: 7 одобрений, 144 отказа — и эти решения остались в `drafts`,
а `wf_drafts` так и стоят `pending`. Заставлять человека ставить 144 отказа второй раз
в новом экране нельзя: решения уже приняты, и этот скрипт переносит их как есть.

Как находится пара «старый черновик → черновик сценария» — **та же дорога, что в
миграции** (`migrate_drafts`, scripts/migrate_to_workflows.py:173-180), своё
сопоставление не изобретается:

    drafts.lead_id → leads.message_id → wf_targets (по workflow_id=cold_dm)
                   → wf_drafts.target_id

Для каждой строки `drafts` со `state != 'pending'`:

* её `wf_drafts` в `pending` — получает `state`, `chosen_variant`, `final_text`,
  `reject_reason`, `decided_by`, `decided_at` исходной строки, а цель (`wf_targets`)
  — ровно то, что ставят ручки `approve_draft` / `reject_draft`
  (`app/api/v1/wf_queues.py`): одобрение — только `status='approved'`, отказ —
  `status='rejected'` **и** `reject_reason` на цели. Гейт отправки и HTTP здесь
  не воспроизводятся: ничего не отправляется, `wf_outbound` не трогается;
* её `wf_drafts` уже решён (не `pending`) — строка не трогается, случай считается
  конфликтом (в новом экране кто-то решил сам) и печатается по id исходного
  черновика;
* сопоставить не удалось (лид или цель не доехали до нового контура) — осиротевшее,
  печатается по id, не выдумывается;
* комментарии `draft_comments` с `contour='lead'` копируются в контур новой очереди
  с новым `draft_id` (сейчас их 0, но код должен пережить появление первых).

Три решения, тем же способом, что и в `migrate_to_workflows.py`:

* **По умолчанию — сухой прогон.** Скрипт считает и показывает, что сделает, и ничего
  не пишет. Запись включается `--apply` явно. Отката одной командой нет (Alembic в
  схеме Radar не заведён), поэтому цена ошибки выше обычной.
* **Идемпотентно.** Метка «решение уже перенесено» — запись `audit_log` с
  `action='wf_draft_decision_synced'` и `source_draft_id` в деталях: по ней повторный
  запуск пропускает сделанное и не дублирует ни решений, ни комментариев. Отличать
  «уже перенесено» от «конфликт» по состоянию `wf_drafts` нельзя — после первого же
  прогона перенесённые строки перестают быть `pending`, и второй прогон объявил бы
  конфликтом собственную работу.
* **Ничего не удаляется и не меняется в старом контуре.** `drafts`, `leads` и их
  комментарии остаются нетронутыми — это страховка на случай, если перенос окажется
  неверным. `wf_outbound` не создаётся вовсе: отправленного в старом контуре нет, и
  выдумывать его значило бы подделывать журнал.

Запуск:

    docker exec api-radar python -m scripts.sync_draft_decisions            # посмотреть
    docker exec api-radar python -m scripts.sync_draft_decisions --apply    # перенести
"""
from __future__ import annotations

import argparse
import asyncio
import logging
from dataclasses import dataclass, field

from sqlalchemy import select

from app.db.models import (AuditLog, Draft, DraftComment, Lead, WfDraft, WfTarget,
                           Workflow)
from app.db.session import get_session_maker
from app.services import workflows

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("sync-decisions")

WORKFLOW_KEY = "cold_dm"
AUDIT_ACTION = "wf_draft_decision_synced"
BATCH = 1000

# Контуры в `draft_comments`. В задаче новый назван «cold_dm», но ручки пишут ровно
# "wf" (`wf_queues.add_draft_comment`: contour="wf"), а списки читают
# `DraftComment.contour == "wf"` — ссылка полиморфная, и третья она была бы записью
# в никуда: экраны и лента отзывов её не увидели бы никогда.
CONTOUR_OLD = "lead"
CONTOUR_NEW = "wf"


class MissingWorkflowError(RuntimeError):
    """Сценария `cold_dm` в реестре нет — переносить решения некуда.

    Не создаём его сами (в отличие от миграции): существование `wf_targets` и
    `wf_drafts` — предусловие этого скрипта, и заводить пустой сценарий поверх
    его отсутствия значило бы дать прогону с виду успех на пустом месте.
    """


class AmbiguousMappingError(RuntimeError):
    """Связь «сообщение → цель» или «цель → черновик» не единственна.

    Схема запрещает обе (uq_target_wf_message, uq_draft_target), и нарушить их
    изнутри Radar нельзя — но скрипт идёт по чужим данным, чинить которые он не
    вправе молча: словарь съел бы дубль, и решение уехало бы в произвольную строку.
    """


@dataclass
class Summary:
    """Итог одного прогона — отдельно от печати, чтобы CLI и тесты не расходились
    в том, что было сделано."""

    candidates: int = 0
    approved: int = 0
    rejected: int = 0
    already_synced: int = 0
    conflicts: list[int] = field(default_factory=list)
    orphaned: list[int] = field(default_factory=list)
    comments_copied: int = 0
    comments_skipped: int = 0


async def _synced_source_ids(db) -> set[int]:
    """Исходные черновики, чьи решения уже переносились этим скриптом.

    Метка — собственная запись в `audit_log`: единственное, что скрипт пишет и
    чего не было в базе до него. По состоянию `wf_drafts` идемпотентность строить
    нельзя (см. докстринг модуля).
    """
    rows = (await db.execute(
        select(AuditLog.detail).where(AuditLog.action == AUDIT_ACTION))).all()
    out: set[int] = set()
    for (detail,) in rows:
        source_id = (detail or {}).get("source_draft_id")
        if source_id is not None:
            out.add(int(source_id))
    return out


async def _matching(db, wf: Workflow) -> tuple[dict[int, int], dict[int, int],
                                               dict[int, int]]:
    """Три словаря сопоставления — те же запросы, что в `migrate_drafts`
    (scripts/migrate_to_workflows.py:173-177), плюс проверка единственности.

    Идентификаторы целей новые, опираться на старые нельзя, поэтому дорога одна:
    `lead_id → message_id → target_id → wf_draft_id`.
    """
    lead_to_message = dict((await db.execute(
        select(Lead.id, Lead.message_id))).all())

    target_pairs = (await db.execute(
        select(WfTarget.message_id, WfTarget.id)
        .where(WfTarget.workflow_id == wf.id))).all()
    if len(target_pairs) != len({m for m, _ in target_pairs}):
        raise AmbiguousMappingError(
            "в wf_targets сценария cold_dm нашлись дубли message_id — "
            "сопоставление неоднозначно, ничего не переношу")
    message_to_target = dict(target_pairs)

    draft_pairs = (await db.execute(
        select(WfDraft.target_id, WfDraft.id)
        .where(WfDraft.workflow_id == wf.id))).all()
    if len(draft_pairs) != len({t for t, _ in draft_pairs}):
        raise AmbiguousMappingError(
            "в wf_drafts сценария cold_dm нашлись дубли target_id — "
            "сопоставление неоднозначно, ничего не переношу")
    target_to_draft = dict(draft_pairs)

    return lead_to_message, message_to_target, target_to_draft


async def sync_decisions(db, *, apply: bool) -> Summary:
    """Посчитать переносы и — если `apply=True` — сделать их.

    По умолчанию (без `apply`) только считает: ровно те же счётчики, что и при
    записи, — сухой прогон, который показывает не то, что сделал бы настоящий,
    хуже отсутствующего.
    """
    wf = await workflows.by_key(db, WORKFLOW_KEY)
    if wf is None:
        raise MissingWorkflowError(
            f"сценария {WORKFLOW_KEY!r} нет в реестре — сначала "
            "scripts/migrate_to_workflows.py --apply")

    summary = Summary()
    done = await _synced_source_ids(db)
    lead_to_message, message_to_target, target_to_draft = await _matching(db, wf)
    # Обратная ссылка для фазы комментариев: исходный черновик → черновик сценария.
    # Собирается для всех сопоставленных, а не только переносимых: отзыв о тексте
    # не зависит от того, в каком состоянии его черновик и кто и когда решил.
    link: dict[int, int] = {}

    last_id = 0
    while True:
        rows = (await db.execute(
            select(Draft)
            .where(Draft.state != "pending", Draft.id > last_id)
            .order_by(Draft.id).limit(BATCH))).scalars().all()
        if not rows:
            break
        last_id = rows[-1].id
        for d in rows:
            message_id = lead_to_message.get(d.lead_id)
            target_id = message_to_target.get(message_id) if message_id else None
            draft_id = target_to_draft.get(target_id) if target_id else None
            if draft_id is None:
                summary.orphaned.append(d.id)
                continue

            summary.candidates += 1
            link[d.id] = draft_id
            if d.id in done:
                summary.already_synced += 1
                continue

            wd = await db.get(WfDraft, draft_id)
            if wd.state != "pending":
                summary.conflicts.append(d.id)
                continue
            if d.state not in ("approved", "rejected"):
                # Других решённых состояний старый контур не знает; появись новое —
                # зеркалить его было бы не с чего, и молча переносить его нельзя.
                summary.conflicts.append(d.id)
                continue

            if apply:
                target = await db.get(WfTarget, target_id)
                previous = wd.state
                wd.state = d.state
                wd.chosen_variant = d.chosen_variant
                wd.final_text = d.final_text
                wd.reject_reason = d.reject_reason
                wd.decided_by = d.decided_by
                wd.decided_at = d.decided_at
                # Ровно то, что делают ручки wf_queues.approve_draft /
                # wf_queues.reject_draft со строкой цели (wf_queues.py:1168 и
                # wf_queues.py:1254-1255) — кроме гейта и HTTP, которых здесь нет:
                if d.state == "approved":
                    target.status = "approved"
                else:
                    target.status = "rejected"
                    target.reject_reason = d.reject_reason

                # user_id — None, как во всякой записи, которую сделал не залогиненный
                # человек: скрипт не имеет права выдавать себя за пользователя, а
                # автор исходного решения сохранён в user_email и в detail.
                db.add(AuditLog(
                    user_id=None, user_email=d.decided_by, action=AUDIT_ACTION,
                    detail={"workflow": wf.key, "draft_id": wd.id,
                            "source_draft_id": d.id, "from": previous, "to": wd.state,
                            "decided_by": d.decided_by}))

            if d.state == "approved":
                summary.approved += 1
            else:
                summary.rejected += 1
        if apply:
            await db.commit()

    summary.comments_copied, summary.comments_skipped = await _sync_comments(
        db, link, apply)
    return summary


async def _sync_comments(db, link: dict[int, int], apply: bool) -> tuple[int, int]:
    """Отзывы о старых черновиках — в контур новой очереди, с новым `draft_id`.

    Идемпотентность — по совпадению (автор, текст, время): копия неотличима от
    уже перенесённого отзыва, и второй прогон не должен плодить близнецов.
    Промпт-версия и номер варианта едут как есть — это атрибуты того же текста,
    который миграция уже скопировала в `wf_drafts.variants`.
    """
    copied = skipped = 0
    for source_id in sorted(link):
        draft_id = link[source_id]
        existing = set((await db.execute(
            select(DraftComment.author_email, DraftComment.text,
                   DraftComment.created_at)
            .where(DraftComment.contour == CONTOUR_NEW,
                   DraftComment.draft_id == draft_id))).all())
        rows = (await db.execute(
            select(DraftComment)
            .where(DraftComment.contour == CONTOUR_OLD,
                   DraftComment.draft_id == source_id)
            .order_by(DraftComment.id))).scalars().all()
        for c in rows:
            if (c.author_email, c.text, c.created_at) in existing:
                skipped += 1
                continue
            copied += 1
            if apply:
                db.add(DraftComment(
                    contour=CONTOUR_NEW, draft_id=draft_id,
                    variant_index=c.variant_index, prompt_version=c.prompt_version,
                    author_email=c.author_email, text=c.text,
                    created_at=c.created_at))
    if apply and copied:
        await db.commit()
    return copied, skipped


def _report(s: Summary, apply: bool) -> None:
    log.info("кандидатов (решённых в drafts и найденных в wf_drafts): %s", s.candidates)
    log.info("перенесено: одобренных %s, отклонённых %s", s.approved, s.rejected)
    log.info("пропущено как уже перенесённые (метка в audit_log): %s", s.already_synced)
    log.info("комментариев: перенесено %s, уже существовали %s",
             s.comments_copied, s.comments_skipped)
    if s.conflicts:
        log.warning("конфликты — в wf_drafts уже есть решение, не тронуты, "
                    "исходные черновики: %s", s.conflicts)
    if s.orphaned:
        log.warning("осиротевшие — сопоставить с wf_drafts не удалось: %s", s.orphaned)
    log.info("готово%s", "" if apply else " (ничего не записано)")


async def main(argv: list[str] | None = None) -> Summary:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true",
                    help="действительно записать (по умолчанию — сухой прогон)")
    args = ap.parse_args(argv)

    if not args.apply:
        log.info("СУХОЙ ПРОГОН — ничего не записывается. Для записи добавьте --apply")

    async with get_session_maker()() as db:
        try:
            summary = await sync_decisions(db, apply=args.apply)
        except (MissingWorkflowError, AmbiguousMappingError) as exc:
            raise SystemExit(f"отказ: {exc}") from exc

    _report(summary, args.apply)
    return summary


if __name__ == "__main__":
    asyncio.run(main())
