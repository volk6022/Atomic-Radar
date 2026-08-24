"""Форма ручной отправки — то, чем сегодня заменяется несуществующая автоотправка.

Андрей отправил сообщение из Telegram руками. Заходит сюда, выбирает наводку, которую
дал Radar, выбирает аккаунт, вставляет текст, сохраняет. Всё, что можно вывести на
сервере — что предлагал Radar, из какого сообщения наводка, какому каналу принадлежит —
выводится на сервере и от браузера не принимается.

Одно решение стоит назвать отдельно: **недоступность Engage не мешает записать**.
Список аккаунтов — удобство, а факт отправки уже произошёл; терять его из-за того, что
чужой сервис не отвечает, было бы прямым уроном. Поэтому экран флота на ту же ошибку
отвечает 503, а этот — пустым списком с указанием причины.
"""
from __future__ import annotations

import logging
from datetime import datetime

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select

from app.api.deps import GetDB, permits, requires
from app.core.access import Capability, Role, Section
from app.db.models import AuditLog, EngageInstance, ManualSend, Message, WfTarget, Workflow
from app.services import engage, manual_sends, workflows

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/manual-sends", tags=["manual-sends"])


async def _describe(db, entry: ManualSend) -> dict:
    """Ответ после записи и после правки — одной формы, что и строка в списке.

    Иначе экран после сохранения показывает не то, что покажет при следующем открытии,
    и разница вылезает в самый неудобный момент — когда человек проверяет, сохранилось
    ли то, что он вставил.
    """
    target = (await db.execute(
        select(WfTarget).where(WfTarget.id == entry.target_id))).scalar_one_or_none() \
        if entry.target_id else None
    message = (await db.execute(
        select(Message).where(Message.id == entry.message_id))).scalar_one_or_none() \
        if entry.message_id else None
    return manual_sends.describe(entry, target=target, message=message)


async def _workflow(db, workflow_id: int) -> Workflow:
    wf = (await db.execute(
        select(Workflow).where(Workflow.id == workflow_id))).scalar_one_or_none()
    if wf is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND,
                            f"workflow {workflow_id} не найден")
    return wf


@router.get("/form")
async def form(db: GetDB, user=requires(Section.MANUAL_SENDS)):
    """Всё, что нужно, чтобы нарисовать форму: по каким контурам можно записывать."""
    active = await workflows.active(db)
    return {
        "workflows": [{"id": wf.id, "key": wf.key, "title": wf.title,
                       "action": wf.action, "visibility": wf.visibility}
                      for wf in active],
        "default_workflow_id": active[0].id if active else None,
    }


@router.get("/accounts")
async def accounts(workflow_id: int, db: GetDB, user=requires(Section.MANUAL_SENDS)):
    """Аккаунты того инстанса Engage, к которому привязан контур.

    Ошибка Engage здесь не 503. Это подсказка для поля «с какого аккаунта», а запись
    факта от неё не зависит: без списка человек просто оставит поле пустым, и данные
    всё равно сохранятся.
    """
    wf = await _workflow(db, workflow_id)
    instance = (await db.execute(
        select(EngageInstance)
        .where(EngageInstance.id == wf.engage_instance_id))).scalar_one_or_none()

    try:
        raw = await engage.list_accounts(
            instance=instance.key if instance is not None else None)
    except engage.EngageUnavailable as e:
        logger.warning("manual_send_accounts_unavailable workflow=%s error=%s",
                       wf.key, e)
        return {"available": False, "reason": str(e), "rows": []}

    return {"available": True, "reason": None, "rows": [
        {"id": a.get("account_id"), "label": f"acc-{a.get('account_id')}",
         "status": a.get("status"), "use_case": a.get("use_case")}
        for a in raw]}


@router.get("/candidates")
async def candidates(workflow_id: int, db: GetDB, user=requires(Section.MANUAL_SENDS),
                     q: str | None = None, limit: int = 20):
    wf = await _workflow(db, workflow_id)
    return {"rows": await manual_sends.candidates(db, workflow=wf, q=q, limit=limit)}


@router.get("/list")
async def listing(db: GetDB, user=requires(Section.MANUAL_SENDS),
                  workflow_id: int | None = None, limit: int = 50, offset: int = 0):
    return await manual_sends.history(db, workflow_id=workflow_id,
                                      limit=limit, offset=offset)


class RecordRequest(BaseModel):
    # Лишние поля отвергаются, а не игнорируются. Ключевой случай — присланный
    # клиентом `suggested_text`: молча его проигнорировать значит оставить у автора
    # формы впечатление, что снимок он задал сам, а его на самом деле сделал сервер.
    model_config = ConfigDict(extra="forbid")

    workflow_id: int
    text: str = Field(min_length=1)
    # Наводки может не быть: написали тому, кого Radar не находил. Это тоже данные.
    target_id: int | None = None
    engage_account_id: int | None = None
    sent_at: datetime | None = None
    note: str | None = None


@router.post("", status_code=status.HTTP_201_CREATED)
async def create(body: RecordRequest, request: Request, db: GetDB,
                 user=permits(Section.MANUAL_SENDS, Capability.MANUAL_SEND_RECORD)):
    wf = await _workflow(db, body.workflow_id)
    try:
        entry = await manual_sends.record(
            db, workflow=wf, text=body.text, recorded_by=user.email,
            target_id=body.target_id, engage_account_id=body.engage_account_id,
            sent_at=body.sent_at, note=body.note)
    except manual_sends.ManualSendError as e:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(e)) from e

    db.add(AuditLog(
        user_id=user.id, user_email=user.email, action="manual_send_record",
        detail={"manual_send_id": entry.id, "workflow": wf.key,
                "target_id": entry.target_id, "chars": len(entry.text),
                "had_suggestion": entry.suggested_text is not None},
        ip=request.client.host if request.client else None))
    await db.commit()
    # `recorded_at` проставляет база. Без явного перечитывания экран получил бы либо
    # пустое поле, либо ленивую подгрузку в асинхронном контексте — то есть падение.
    await db.refresh(entry)
    return await _describe(db, entry)


class CorrectRequest(BaseModel):
    """Правка уже записанного. Всё необязательно — присылается только изменённое.

    Наводку сменить нельзя: это означало бы «на самом деле я отвечал не тому», то
    есть другую запись. Снимок предложенного тоже неизменяем — он и есть свидетельство.
    """
    model_config = ConfigDict(extra="forbid")

    text: str | None = Field(default=None, min_length=1)
    note: str | None = None
    sent_at: datetime | None = None
    engage_account_id: int | None = None


@router.patch("/{entry_id}")
async def correct(entry_id: int, body: CorrectRequest, request: Request, db: GetDB,
                  user=permits(Section.MANUAL_SENDS, Capability.MANUAL_SEND_RECORD)):
    """Поправить свою запись — опечатку в тексте, забытое время, не тот аккаунт.

    Чужую правит только владелец. Дело не в секретности: запись — это чей-то рассказ
    о том, что он сделал, и переписывать его за него нельзя. Без этой правки
    единственным способом исправить опечатку была бы вторая запись, и корпус
    наполнился бы дублями.
    """
    entry = (await db.execute(
        select(ManualSend).where(ManualSend.id == entry_id))).scalar_one_or_none()
    if entry is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"запись {entry_id} не найдена")
    if entry.recorded_by != user.email and user.role != Role.OWNER:
        raise HTTPException(status.HTTP_403_FORBIDDEN,
                            "правка чужой записи доступна только владельцу")

    fields = body.model_dump(exclude_unset=True)
    if not fields:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "нечего менять")

    try:
        changed = manual_sends.correct(entry, fields)
    except manual_sends.ManualSendError as e:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(e)) from e

    # Правка, которая ничего не изменила, в журнал не пишется: иначе журнал заполнится
    # событиями «поправил, ничего не поменяв» и перестанет быть читаемым.
    if changed:
        db.add(AuditLog(
            user_id=user.id, user_email=user.email, action="manual_send_edit",
            detail={"manual_send_id": entry.id, "changed": changed,
                    "own": entry.recorded_by == user.email},
            ip=request.client.host if request.client else None))
    await db.commit()
    return await _describe(db, entry)
