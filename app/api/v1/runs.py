"""Задачи: список, запуск, отмена.

Раздел видит весь штат — замершая очередь без видимой причины читается как поломка,
а причина обычно в идущем пересчёте. Запускать при этом может не каждый: тяжёлые
прогоны делят видеокарту с работающим каскадом, и решение «занять её на час» — не
дело наёмного разборщика.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field
from sqlalchemy import func, select

from app.api.deps import GetDB, permits, requires
from app.api.v1.listing import ListParams, apply_sort, list_params
from app.core.access import Capability, Role, Section
from app.db.models import AuditLog, Message, Run
from app.services import embeddings, jobs, llm

logger = logging.getLogger("radar")

router = APIRouter(prefix="/api/v1/runs", tags=["runs"])

RUN_SORTS = {"created": Run.created_at, "kind": Run.kind, "status": Run.status,
             "progress": Run.progress}

# Какое право нужно для запуска каждого вида. Пересчёт — рычаг качества и занимает
# карту, поэтому за владельцем; дочитать историю канала дорого по лимитам чтения,
# но не опасно, и заказчику это доступно.
KIND_CAPABILITY = {
    "reclassify": Capability.RUN_HEAVY,
    "backfill": Capability.RUN_BACKFILL,
    "export": Capability.RUN_EXPORT,
}

KIND_TITLE = {"reclassify": "Переклассификация", "backfill": "Дочитать историю",
              "export": "Выгрузка"}

# Где у вида задачи кнопка. Бэкфиллу нужен выбранный канал, поэтому он живёт в
# разделе Channels и оттуда же заводит строку в `runs`.
KIND_WHERE = {"reclassify": "runs", "backfill": "channels", "export": "nowhere"}


def _row(r: Run) -> dict:
    return {
        "id": r.id, "name": r.name, "kind": r.kind, "status": r.status,
        "progress": float(r.progress or 0), "params": r.params or {},
        "error": r.error, "result": r.result,
        "cancel_requested": bool(r.cancel_requested),
        "created_by": r.created_by,
        "created_at": r.created_at.isoformat() if r.created_at else None,
        "started_at": r.started_at.isoformat() if r.started_at else None,
        "finished_at": r.finished_at.isoformat() if r.finished_at else None,
    }


@router.get("")
async def list_runs(db: GetDB, user=requires(Section.RUNS),
                    p: ListParams = Depends(list_params)):
    total = (await db.execute(select(func.count(Run.id)))).scalar_one()
    q = apply_sort(select(Run), p, RUN_SORTS, default="created", tiebreak=Run.id)
    rows = (await db.execute(q.limit(p.limit).offset(p.offset))).scalars().all()

    # Сколько сообщений ждёт обработки — то самое число, ради которого пересчёт и
    # запускают. Показывать список задач без него значит заставлять человека
    # угадывать, нужен ли прогон вообще.
    pending = (await db.execute(select(func.count(Message.id)).where(
        Message.cascade_passed.is_(None)))).scalar_one()

    return {**p.page(total), "rows": [_row(r) for r in rows],
            "pending_messages": pending,
            # `where` важнее, чем `available`: бэкфилл запускается, просто не отсюда —
            # ему нужен выбранный канал, и дублировать этот выбор на двух экранах
            # значило бы разойтись между ними. «Недоступно» и «нажимается в другом
            # месте» — разные новости, и подпись обязана их различать.
            "kinds": [{"kind": k, "title": KIND_TITLE.get(k, k),
                       "available": k in jobs.RUNNERS,
                       "where": KIND_WHERE.get(k, "runs")}
                      for k in jobs.KINDS],
            "stages": {"l2": embeddings.enabled(), "l3": llm.enabled()}}


@router.get("/{run_id}")
async def one_run(run_id: int, db: GetDB, user=requires(Section.RUNS)):
    run = (await db.execute(select(Run).where(Run.id == run_id))).scalar_one_or_none()
    if run is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"задача {run_id} не найдена")
    return {**_row(run), "log": run.log or []}


class StartRequest(BaseModel):
    kind: str
    params: dict = Field(default_factory=dict)


@router.post("")
async def start_run(body: StartRequest, request: Request, db: GetDB,
                    user=requires(Section.RUNS)):
    """Запустить задачу.

    Право проверяется по виду задачи, а не одно на всю ручку: у «дочитать историю» и
    «пересчитать всё» разная цена и разные роли, и объединять их в одно разрешение
    значило бы выдать заказчику видеокарту вместе с бэкфиллом.

    Ответ приходит сразу и означает «задача заведена», а не «посчитано»: с включённой
    очередью прогон идёт в воркере, ход виден в этом же разделе. Недоступная очередь —
    `503`: строка при этом уже помечена упавшей, и повторить запуск можно кнопкой,
    когда очередь вернётся.
    """
    from app.core.access import allows  # локально: иначе цикл импорта через deps

    cap = KIND_CAPABILITY.get(body.kind)
    if cap is None:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            f"вид задачи «{body.kind}» неизвестен")
    if not allows(user.role, cap):
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            f"роль «{user.role}» не может запускать задачи вида «{body.kind}»")

    # Отдельный отказ для видов, у которых кнопка в другом месте: «неизвестный вид»
    # здесь было бы неправдой и отправило бы искать несуществующую поломку.
    where = KIND_WHERE.get(body.kind)
    if body.kind not in jobs.RUNNERS:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            f"«{KIND_TITLE.get(body.kind, body.kind)}» запускается в разделе Channels: "
            f"нужен выбранный канал" if where == "channels"
            else f"вид задачи «{body.kind}» пока не реализован")

    name = KIND_TITLE.get(body.kind, body.kind)
    if body.kind == "reclassify":
        scope = body.params.get("scope") or "pending"
        name += " · " + ("всё" if scope == "all" else "недосчитанное")

    try:
        run = await jobs.start(db, kind=body.kind, params=body.params, name=name,
                               user_email=user.email)
    except jobs.JobBusy as e:
        raise HTTPException(status.HTTP_409_CONFLICT, str(e)) from e
    except jobs.JobUnknown as e:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(e)) from e
    except jobs.JobQueueDown as e:
        # 503, а не 500: это не ошибка в запросе и не поломка в коде, а отсутствующая
        # сейчас ступень. Человеку нужно знать, что повторить попытку имеет смысл, —
        # `500` сказал бы ровно обратное и отправил бы искать несуществующий баг.
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, str(e)) from e

    db.add(AuditLog(
        user_id=user.id, user_email=user.email, action="run_start",
        detail={"run_id": run.id, "kind": body.kind, "params": body.params},
        ip=request.client.host if request.client else None))
    await db.commit()
    return _row(run)


@router.post("/{run_id}/cancel")
async def cancel_run(run_id: int, request: Request, db: GetDB,
                     user=permits(Section.RUNS, Capability.RUN_CANCEL)):
    """Попросить задачу остановиться.

    Останавливается не мгновенно: прогон дописывает посчитанное и выходит на
    ближайшей проверке. Обрывать посреди транзакции было бы хуже — часть сообщений
    осталась бы с новым вердиктом, часть со старым, и понять, где граница, стало бы
    нельзя.
    """
    run = (await db.execute(select(Run).where(Run.id == run_id))).scalar_one_or_none()
    if run is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"задача {run_id} не найдена")
    if run.status not in jobs.ACTIVE:
        raise HTTPException(status.HTTP_409_CONFLICT,
                            f"задача {run_id} уже не выполняется (статус «{run.status}»)")
    # Заказчик отменяет только своё: чужой прогон мог быть запущен ради вопроса,
    # ответ на который ему неизвестен.
    if user.role != Role.OWNER and run.created_by != user.email:
        raise HTTPException(status.HTTP_403_FORBIDDEN,
                            "отменить можно только собственную задачу")

    await jobs.request_cancel(db, run)
    db.add(AuditLog(
        user_id=user.id, user_email=user.email, action="run_cancel",
        detail={"run_id": run_id, "kind": run.kind, "progress": float(run.progress or 0)},
        ip=request.client.host if request.client else None))
    await db.commit()
    logger.warning("run_cancel_requested run=%s by=%s", run_id, user.email)
    return {"id": run_id, "cancel_requested": True, "status": run.status}
