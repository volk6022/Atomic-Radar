"""Очередь дочитывания истории каналов — HTTP-поверхность над службой очереди.

Служба (`app/services/backfill_queue.py`) есть, а поверхности у неё не было никакой:
ни ручки, ни экрана, ни исполнителя. Здесь появляется ровно то, что нужно человеку
ДО исполнителя: посмотреть, что стоит, поставить каналы и снять ошибочно
поставленное. Ручки выдачи работы (`drain`) здесь намеренно нет — без исполнителя
она была бы заглушкой, а заглушка в очереди работы — ложное обещание, что кто-то
возьмёт. Появится исполнитель — появится и ручка.

Права. Смотреть очередь может любой вошедший, а не раздел из матрицы: замершая
очередь выглядит поломкой, и человек, которому её не показали, пойдёт искать
поломку вместо причины (тот же приём, что у реестра сценариев: меню рисуется до
входа в раздел). Ставить и снимать — право `run.backfill`: постановка тратит
дневной бюджет чтений аккаунта, и распоряжаться им может не всякий вошедший.

Отказы не топят пачку. Кнопка «дочитать всем» на реестре из шестидесяти каналов
не должна падать целиком из-за одной группы, в которую не вступили: пригодность
каналов проверяется ДО вызова службы, и ответ называет поимённо и поставленное,
и отвергнутое с причиной. Если не встал никто — это не 200: неверный запрос не
должен выглядеть исполненным.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, Query, Request, Response, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from app.api.deps import CurrentUser, GetDB, permits
from app.api.v1.listing import MAX_LIMIT
from app.core import clock
from app.core.access import Capability, Section
from app.db.models import AuditLog, BackfillItem, Channel
from app.services import backfill_queue

logger = logging.getLogger("radar")

router = APIRouter(prefix="/api/v1/backfill", tags=["backfill"])

# Потолки глубины равны умолчаниям службы и берутся из неё, а не дублируются
# числами: «до 2000 сообщений и до месяца» — одно правило постановки (Иван,
# 04.09.2026), и разъехаться потолок ручки с умолчанием службы могут только
# вместе с правкой службы.
MAX_TARGET = backfill_queue.DEFAULT_TARGET
MAX_DEPTH_DAYS = backfill_queue.DEFAULT_DEPTH.days


def _name(ch: Channel) -> str:
    """Имя канала в тексте отказа — тем же образом, каким его называет служба:
    отказ обязан читаться одинаково, вышел он из ручки или из службы."""
    return f"@{ch.username}" if ch.username else f"«{ch.title}»"


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt is not None else None


def _item(it: BackfillItem, ch: Channel | None) -> dict:
    """Элемент очереди для экрана: канал, состояние, глубина, ход, времена.

    Канал — вложенным объектом, а не плоскими полями: экран показывает его
    отдельной колонкой со ссылкой, и собирать её из трёх плоских полей пришлось
    бы каждому вызывающему заново.
    """
    return {
        "id": it.id,
        "channel": {"id": it.channel_id,
                    "username": ch.username if ch is not None else None,
                    "title": ch.title if ch is not None else None},
        "state": it.state,
        "account_id": it.account_id,
        "position": it.position,
        "target": it.target,
        "min_date": _iso(it.min_date),
        "read_total": it.read_total,
        "attempts": it.attempts,
        "error": it.error,
        "scheduled_for": _iso(it.scheduled_for),
        "requested_by": it.requested_by,
        "created_at": _iso(it.created_at),
        "started_at": _iso(it.started_at),
        "finished_at": _iso(it.finished_at),
    }


@router.get("/queue")
async def list_queue(db: GetDB, user: CurrentUser,
                     state: str | None = Query(None),
                     limit: int = Query(50, ge=1, le=MAX_LIMIT),
                     offset: int = Query(0, ge=0)):
    """Что стоит в очереди: страница элементов и сводка по всем состояниям.

    Сводка приходит всегда, включая пустую очередь, и не зависит от фильтра:
    экран не должен различать «пусто» и «не пришло», а фильтр по состоянию —
    способ найти свою строку, а не способ узнать, сколько всего работы.

    Порядок — `(position, id)`, тот же, в котором очередь выдаёт работу:
    экран обязан показывать живую очередь, а не произвольный порядок таблицы,
    иначе «следующий канал» на экране и «следующий канал» у воркера — разные.

    Постраничность — limit/offset, как у соседних списков: очередь мала
    (десятки строк на пачку), курсорная арифметика тут не окупилась бы.
    """
    state = (state or "").strip() or None
    if state is not None and state not in BackfillItem.STATES:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            f"состояние «{state}» неизвестно, возможные: "
            f"{', '.join(BackfillItem.STATES)}")

    filters = [BackfillItem.state == state] if state else []
    total = (await db.execute(
        select(func.count(BackfillItem.id)).where(*filters))).scalar_one()
    rows = (await db.execute(
        select(BackfillItem).where(*filters)
        .order_by(BackfillItem.position, BackfillItem.id)
        .limit(limit).offset(offset))).scalars().all()

    # Каналы читаются одной пачкой, а не по строке: N+1 запросов на страницу
    # очереди — это N+1 причин для экрана подтормаживать ровно там, где человек
    # ждёт «ну что там стоит».
    channels: dict[int, Channel] = {}
    if rows:
        channels = {c.id: c for c in (await db.execute(
            select(Channel).where(Channel.id.in_([i.channel_id for i in rows]))))
            .scalars().all()}

    return {"total": total, "limit": limit, "offset": offset,
            "items": [_item(i, channels.get(i.channel_id)) for i in rows],
            "summary": await backfill_queue.summary(db)}


class EnqueueRequest(BaseModel):
    """Пачка каналов на постановку: одна кнопка — один запрос — одно окно глубины."""
    model_config = ConfigDict(extra="forbid")
    channel_ids: list[int] = Field(default_factory=list)
    target: int | None = Field(None, ge=1)
    depth_days: int | None = Field(None, ge=1)
    # Отложенный запуск; отсутствует — выдавать сразу.
    scheduled_for: datetime | None = None


@router.post("/queue")
async def enqueue_channels(body: EnqueueRequest, request: Request, response: Response,
                           db: GetDB,
                           user=permits(Section.CHANNELS, Capability.RUN_BACKFILL)):
    """Поставить каналы в очередь. Ответ называет поимённо поставленное и
    отвергнутое с причиной по каждому.

    Зачем нужна: «дочитать всем» на реестре из шестидесяти каналов должно
    переживать одну группу, в которую не вступили, — иначе каналы пришлось бы
    перебирать по одному. Поэтому пригодность проверяется ДО вызова службы, а
    не ловится по ходу: `enqueue` бросает исключение на первом непригодном
    канале, и без предварительной проверки пачка падала бы целиком из-за
    шестидесятого.

    Решения:

    * **глубина считается здесь и только здесь.** `depth_days` переводится в
      `min_date` на момент запроса; потолки — отказ с текстом, а не молчаливое
      урезание: урезанный запрос выглядит исполненным. Умолчания глубины ручка
      не дублирует — их считает служба, и растить вторую копию правил здесь
      значило бы ждать, пока они разойдутся.
    * **несуществующий канал — отказ по имени**, а не молчаливый пропуск:
      «поставлено 0» без причины — приглашение гадать, какой канал и почему.
    * **ничего не встало — не 200.** Всё отвергнуто по состоянию (не вступили,
      уже стоит) — `409`; среди отвергнутых есть несуществующие — `404`: ссылка
      на отсутствующую сущность — ошибка самого запроса, а не конфликт состояния.
    * **группа обсуждения всегда идёт аккаунту, который в неё вступил.** Поле
      «каким аккаунтом читать» в запросе намеренно нет, поэтому `AccountMismatch`
      с этой поверхности недостижим: группам служба сама ставит
      `subscribed_account_id`, обычным каналам аккаунт не нужен вовсе.
    """
    if not body.channel_ids:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            "список каналов пуст — ставить нечего")
    if body.target is not None and body.target > MAX_TARGET:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            f"target {body.target} выше потолка {MAX_TARGET} сообщений "
            f"(правило постановки: до {MAX_TARGET}); молча урезать нельзя — "
            f"урезанный запрос выглядел бы исполненным")
    if body.depth_days is not None and body.depth_days > MAX_DEPTH_DAYS:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            f"depth_days {body.depth_days} больше {MAX_DEPTH_DAYS} — глубина "
            f"ограничена месяцем; молча урезать нельзя — урезанный запрос "
            f"выглядел бы исполненным")

    # Граница окна считается один раз на запрос: элементы одной кнопки обязаны
    # обещать одно и то же окно (то же правило, что у службы для умолчания).
    min_date = (clock.utcnow() - timedelta(days=body.depth_days)
                if body.depth_days is not None else None)

    # Дубли внутри пачки схлопываются с сохранением порядка: служба всё равно
    # дала бы одну строку на канал, и дважды назвать один канал поставленным
    # значило бы соврать в отчёте.
    ids = list(dict.fromkeys(body.channel_ids))

    channels = {c.id: c for c in (await db.execute(
        select(Channel).where(Channel.id.in_(ids)))).scalars().all()}
    standing = dict((await db.execute(
        select(BackfillItem.channel_id, BackfillItem.id).where(
            BackfillItem.channel_id.in_(ids),
            BackfillItem.state.in_(backfill_queue.ACTIVE)))).all())

    refused: list[dict] = []
    eligible: list[int] = []
    refers_to_missing = False
    for cid in ids:
        ch = channels.get(cid)
        if ch is None:
            refers_to_missing = True
            refused.append({"channel_id": cid, "username": None, "title": None,
                            "reason": f"канала #{cid} нет — проверьте список каналов"})
            continue
        if cid in standing:
            refused.append({"channel_id": cid, "username": ch.username,
                            "title": ch.title,
                            "reason": f"уже стоит в очереди "
                                      f"(элемент #{standing[cid]}) — повторная "
                                      f"постановка не нужна"})
            continue
        # Дальше те же правила, что у службы в `enqueue`, и теми же текстами:
        # отказ обязан читаться одинаково, из какой бы двери он ни вышел.
        if ch.chat_type in backfill_queue.GROUP_CHAT_TYPES:
            if ch.linked_joined_at is None:
                refused.append({"channel_id": cid, "username": ch.username,
                                "title": ch.title,
                                "reason": f"в группу {_name(ch)} аккаунты ещё не "
                                          f"вступали — сначала вступите, потом "
                                          f"ставьте историю на дочитывание"})
                continue
            if ch.subscribed_account_id is None:
                refused.append({"channel_id": cid, "username": ch.username,
                                "title": ch.title,
                                "reason": f"группа {_name(ch)} помечена вступившей, "
                                          f"но кто вступил — неизвестно; историю "
                                          f"группы читает вступивший"})
                continue
        eligible.append(cid)

    try:
        made = await backfill_queue.enqueue(
            db,
            items=[{"channel_id": cid} for cid in eligible],
            requested_by=user.email, scheduled_for=body.scheduled_for,
            target=body.target, min_date=min_date)
    except IntegrityError as e:
        # Параллельная постановка того же канала: наша проверка «уже стоит»
        # прочитала базу до чужого коммита, и дубль поймал частичный уникальный
        # индекс. Откат и 409: список в этот момент уже недостоверен, честнее
        # попросить обновить очередь, чем показать гипотезу.
        await db.rollback()
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            "один из каналов только что поставлен параллельным запросом — "
            "обновите очередь и повторите") from e

    if made:
        db.add(AuditLog(
            user_id=user.id, user_email=user.email, action="backfill_enqueue",
            detail={"queued": [m.channel_id for m in made],
                    "refused": [r["channel_id"] for r in refused],
                    "target": body.target, "depth_days": body.depth_days,
                    "scheduled_for": _iso(body.scheduled_for)},
            ip=request.client.host if request.client else None))
        await db.commit()
        logger.info("backfill_enqueued queued=%s refused=%s target=%s by=%s",
                    [m.channel_id for m in made],
                    [r["channel_id"] for r in refused], body.target, user.email)
    else:
        # Ничего не встало — код по худшей из причин: ссылка на несуществующий
        # канал важнее конфликта состояния. Журнал не пишется: отказа по сути
        # ещё нет, ответ с перечнем и есть весь итог.
        response.status_code = (status.HTTP_404_NOT_FOUND if refers_to_missing
                                else status.HTTP_409_CONFLICT)

    return {"queued": [_item(m, channels.get(m.channel_id)) for m in made],
            "refused": refused}


@router.delete("/queue/{item_id}")
async def cancel_queue_item(item_id: int, request: Request, db: GetDB,
                            user=permits(Section.CHANNELS, Capability.RUN_BACKFILL)):
    """Снять стоящий элемент с очереди.

    Снять можно только то, что ещё стоит: взятое уже уехало в Engage, и пометить
    начатое чтение «отменённым» значило бы соврать экрану. Отсюда `409` на
    повторном снятии и на попытке снять бегущий элемент — текст отказа отдаёт
    служба, в нём названо текущее состояние, то есть следующий шаг. Отсутствующий
    идентификатор — тоже `409` по той же причине: молчать в ответ на неверный id
    хуже, чем отказ (см. `ItemNotWaiting`).

    Право то же, что у постановки: снять поставленное — тоже распоряжение
    дневным бюджетом чтений.
    """
    try:
        item = await backfill_queue.cancel(db, item_id)
    except backfill_queue.ItemNotWaiting as e:
        raise HTTPException(status.HTTP_409_CONFLICT, str(e)) from e

    db.add(AuditLog(
        user_id=user.id, user_email=user.email, action="backfill_item_canceled",
        detail={"item_id": item.id, "channel_id": item.channel_id,
                "position": item.position},
        ip=request.client.host if request.client else None))
    await db.commit()
    logger.info("backfill_item_canceled item=%s channel=%s by=%s",
                item.id, item.channel_id, user.email)
    return {"id": item.id, "channel_id": item.channel_id, "state": item.state}
