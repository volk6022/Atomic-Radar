"""Реестр сценариев — то, из чего интерфейс строит боковое меню.

Сценарий описан тремя осями (`target_kind`, `action`, `visibility`), и состав его
разделов выводится из этих осей, а не перечисляется руками. Отсюда главное решение
этого модуля: **состав меню считает сервер**. Третий сценарий появляется строкой в
таблице, а не правкой оболочки — иначе список разделов пришлось бы держать в двух
местах, и он разошёлся бы ровно так же, как разошлась матрица прав.

Права. Эти ручки открыты **любому вошедшему**, а не разделу из матрицы. Причина в том,
что меню рисуется до входа в какой-либо раздел: потребуй ручка права на конкретный
раздел, гость получил бы 403 при отрисовке оболочки и не увидел бы даже дозволенного
ему. Секрета здесь нет — это названия сценариев, а не данные. Что человеку **можно**,
решается по-прежнему матрицей на каждой ручке с данными.
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

from app.api.deps import CurrentUser, GetDB
from app.services import workflows

router = APIRouter(prefix="/api/v1/workflows", tags=["workflows"])


@router.get("")
async def listing(db: GetDB, user: CurrentUser):
    """Действующие сценарии в порядке меню.

    Выключенные не отдаются: меню строится по этому ответу, и блок выключенного
    сценария, нарисованный «на всякий случай», вёл бы на экраны без данных.
    """
    rows = [workflows.describe(wf) for wf in await workflows.active(db)]
    return {"rows": rows, "total": len(rows)}


@router.get("/{key}")
async def one(key: str, db: GetDB, user: CurrentUser):
    wf = await workflows.by_key(db, key)
    if wf is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"сценарий {key!r} не найден")
    return workflows.describe(wf)


@router.get("/{key}/sections")
async def sections(key: str, db: GetDB, user: CurrentUser):
    """Разделы одного сценария — тот же список, что внутри `describe`.

    Отдельной ручкой, потому что оболочке при переходе внутри блока нужен только он,
    а тянуть ради этого описание сценария целиком значит гонять лишнее на каждом клике.
    """
    wf = await workflows.by_key(db, key)
    if wf is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"сценарий {key!r} не найден")
    return {
        "key": wf.key,
        "title": wf.title,
        "sections": [{"key": s, "title": workflows.SECTION_TITLES[s]}
                     for s in workflows.sections_for(wf)],
    }
