"""Один сценарий: поток, цели, черновики — и решения по ним.

**Параллельный ресурс, а не параметр к старым ручкам.** Соблазн был добавить
`?workflow_id=` к `/api/v1/leads` и `/messages` — и это сломалось бы по построению.
У лидов есть массовые решения и правка: ручка читала бы `wf_targets`, а `POST /bulk`
и `PATCH /{id}` продолжали бы писать в `leads`. Список показывал бы одно, кнопка
меняла бы другое, и заметно это стало бы по «отклонил, а оно всё висит».

Старые ручки остаются как есть и продолжают обслуживать контур ЛС. Это та же
страховка на откат, что и везде в этой ветке: пока экраны не переехали, `leads` и
`drafts` — рабочая витрина, а не наследие.

**Права здесь настоящие, в отличие от `workflows.py`.** Тот реестр открыт любому
вошедшему намеренно: из него рисуется меню, и 403 при отрисовке оболочки лишил бы
гостя даже дозволенного. Здесь данные, поэтому каждая ручка спрашивает свой раздел
матрицы — а те, что меняют, ещё и своё разрешение (`permits`). Разводить их по разным
модулям пришлось именно поэтому: сложи я их вместе, докстринг про «открыто любому
вошедшему» стал бы ложью для половины ручек.

**Чтение и решения лежат вместе**, как в `leads.py` и `drafts.py`. Соблазн вынести
записи в отдельный модуль был, и отказался я от него по одной причине: массовое
решение обязано отбирать цели тем же кодом, что и список (`_filtered`). Через границу
модуля этот код пришлось бы либо импортировать приватным именем, либо повторить — а
повторённый отбор рано или поздно разъезжается, и человек отклоняет не то, что видел.

**Точка невозврата — доставленное сообщение, а не одобрение.** Пока система в сухом
прогоне, одобрение это запись в базе, и передумать человек вправе. Поэтому решения
здесь обратимы все до одного, а необратимость проверяется ровно по одному признаку:
есть ли в `wf_outbound` попытка с `delivered_message_id`.

Сценарий адресуется ключом (`cold_dm`), а не числовым id — так же, как в реестре, из
которого интерфейс строит меню. Ключ переживает пересев базы, id — нет.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field
from sqlalchemy import func, select

from app.api.deps import CurrentUser, GetDB, permits, requires
from app.api.v1.drafts import REASON_BY_N
from app.api.v1.leads import BULK_ACTIONS, BulkRequest
from app.api.v1.listing import ListParams, apply_search, apply_sort, list_params
from app.api.v1.system import current_mode
from app.core import cascade, clock
from app.core.access import BULK_LIMIT_REVIEWER, Capability, Role, Section
from app.core.outbound_gate import OutboundGate, SendRequest
from app.db.models import (AuditLog, Channel, Message, WfDraft, WfOutbound, WfTarget,
                           WfVerdict, Workflow)
from app.services import wf_drafting, workflows as workflow_service

logger = logging.getLogger("radar")

router = APIRouter(prefix="/api/v1/workflows/{key}", tags=["workflow-data"])

TARGET_STATUSES = ("new", "in_review", "approved", "rejected")
DRAFT_STATES = ("pending", "approved", "rejected", "edited")

# Пять положений фильтра потока, а не четыре: у сценария есть состояние, которого у
# общего потока нет вовсе, — «сценарий сюда ещё не доходил». См. `stream()`.
STREAM_FILTERS = ("true", "false", "pending", "uncomputed")

TARGET_SORTS = {"score": WfTarget.score, "created": WfTarget.created_at,
                "author": WfTarget.author_name, "channel": Channel.title,
                "status": WfTarget.status, "pain": WfTarget.pain}

STREAM_SORTS = {"date": Message.tg_date, "channel": Channel.title,
                "author": Message.author_name, "level": WfVerdict.level}

DRAFT_SORTS = {"created": WfDraft.created_at, "score": WfTarget.score,
               "state": WfDraft.state, "pain": WfTarget.pain}


async def _workflow(key: str, db: GetDB, user: CurrentUser) -> Workflow:
    """Сценарий по ключу — или 404.

    Зависимость, а не три одинаковых проверки в ручках: забыть её в четвёртой ручке
    значило бы отдать пустой список вместо «такого сценария нет», а пустой список
    читается как «данных пока нет» и никого не настораживает.

    `user` объявлен здесь не потому, что нужен телу, а ради порядка: без него поиск
    сценария выполнялся раньше проверки сессии, и анонимный запрос успевал сходить в
    базу и получить 404 вместо 401 — то есть узнать, заведён такой ключ или нет.
    Проверка раздела остаётся на ручках: у каждой она своя.
    """
    wf = await workflow_service.by_key(db, key)
    if wf is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"сценарий {key!r} не найден")
    return wf


GetWorkflow = Depends(_workflow)


def _check(value: str | None, allowed: tuple[str, ...], what: str) -> None:
    if value and value not in allowed:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            f"неизвестный {what} «{value}», ожидается один из {', '.join(allowed)}")


# Заголовок колонки адресации. Живёт здесь, а не в шаблоне экрана: экран целей один
# на все сценарии, и формулировка «Кому» против «Под каким сообщением» — следствие
# оси `target_kind`, то есть свойство сценария, а не оформление таблицы.
ADDRESSING_LABEL = {"user": "Кому", "message": "Под каким сообщением"}


def _addressing(target: WfTarget) -> dict:
    """Куда пойдёт действие — в форме, которую экран покажет как есть.

    По спецификации (§9.2) колонка адресации зависит от `target_kind`: «Кому» у ЛС,
    «Под каким сообщением» у публичного ответа. Считает это сервер, а не экран: иначе
    правило «у публичной цели автора может не быть вовсе» пришлось бы держать и в
    шаблоне тоже.
    """
    if target.target_kind == "user":
        return {"kind": "user", "label": ADDRESSING_LABEL["user"],
                "value": (("@" + target.author_username) if target.author_username
                          else (target.author_name or f"id{target.recipient_peer_id}")),
                "recipient_peer_id": target.recipient_peer_id}
    return {"kind": "message", "label": ADDRESSING_LABEL["message"],
            "value": f"сообщение {target.reply_to_message_id}",
            "chat_peer_id": target.chat_peer_id,
            "reply_to_message_id": target.reply_to_message_id}


# ── поток сценария ────────────────────────────────────────────────────────────

@router.get("/stream")
async def stream(db: GetDB, wf: Workflow = GetWorkflow,
                 user=requires(Section.STREAM),
                 p: ListParams = Depends(list_params),
                 channel_id: int | None = None,
                 passed: str | None = None):
    """Сообщения с вердиктом **этого** сценария, а не старых колонок.

    Разница не косметическая. `messages.cascade_*` — вердикт контура ЛС; сообщение,
    отсеянное его правилами на L1, для публичного ответа может быть законной целью.
    Показывать в потоке публичного сценария причины отбраковки по правилам личных
    сообщений значило бы отвечать не на тот вопрос, ради которого экран существует.

    Соединение внешнее: у сообщения может не быть вердикта вовсе — новое из ингеста
    или заведённый позже сценарий, который ещё не догнал накопленное. Это «не
    считалось», и выглядеть оно должно как пустые галочки, а не как «не прошло».
    """
    _check(passed, STREAM_FILTERS, "фильтр")

    join = (select(Message, Channel, WfVerdict)
            .join(Channel, Message.channel_id == Channel.id)
            .outerjoin(WfVerdict,
                       (WfVerdict.message_id == Message.id)
                       & (WfVerdict.workflow_id == wf.id)))
    count_q = (select(func.count(Message.id))
               .join(Channel, Message.channel_id == Channel.id)
               .outerjoin(WfVerdict,
                          (WfVerdict.message_id == Message.id)
                          & (WfVerdict.workflow_id == wf.id)))

    if channel_id is not None:
        join = join.where(Message.channel_id == channel_id)
        count_q = count_q.where(Message.channel_id == channel_id)

    # Четыре состояния, а не три: у сценария сообщение может быть ещё и «не
    # считалось» — строки вердикта нет вовсе.
    #
    # Тонкость, на которой это уже один раз сломалось. После внешнего соединения у
    # сообщения без вердикта все колонки `wf_verdicts` равны NULL, поэтому голое
    # `passed IS NULL` ловит **оба** состояния разом: и «модель ещё не ответила», и
    # «сценарий сюда не доходил». Фильтр «ждёт обработки» показывал очередь вместе с
    # нетронутым остатком, то есть ровно ту разницу, ради которой заведено четвёртое
    # состояние, и стирал. Отличаем по ключу вердикта, а не по `passed`.
    where = {"true": WfVerdict.passed.is_(True),
             "false": WfVerdict.passed.is_(False),
             "pending": WfVerdict.message_id.isnot(None) & WfVerdict.passed.is_(None),
             "uncomputed": WfVerdict.message_id.is_(None)}.get(passed or "")
    if where is not None:
        join = join.where(where)
        count_q = count_q.where(where)

    search = [Message.text, Message.author_name, Message.author_username]
    join, count_q = apply_search(join, p, search), apply_search(count_q, p, search)

    total = (await db.execute(count_q)).scalar_one()
    join = apply_sort(join, p, STREAM_SORTS, default="date", tiebreak=Message.id)
    rows = (await db.execute(join.limit(p.limit).offset(p.offset))).all()

    target_by_message = dict((await db.execute(
        select(WfTarget.message_id, WfTarget.id)
        .where(WfTarget.workflow_id == wf.id,
               WfTarget.message_id.in_([m.id for m, _, _ in rows])))).all()) if rows else {}

    out = []
    for m, c, v in rows:
        out.append({
            "id": m.tg_message_id, "message_id": m.id, "channel": c.title,
            "channel_id": m.channel_id,
            "author_name": m.author_name or "—",
            "author_username": ("@" + m.author_username) if m.author_username else None,
            "text": m.text or "",
            "tg_date": m.tg_date.isoformat(),
            "is_automatic_forward": m.is_automatic_forward,
            "cascade": cascade.stage_flags(v.level if v else None,
                                           v.passed if v else None),
            "cascade_notes": (v.detail if v else {}) or {},
            "score": v.score if v else None,
            "pain": v.pain if v else None,
            # Отличие от общего потока: «вердикта нет» — самостоятельное состояние.
            "computed": v is not None,
            "target_id": target_by_message.get(m.id),
        })
    return {**p.page(total), "workflow": wf.key, "rows": out}


# ── цели сценария ─────────────────────────────────────────────────────────────

def _filtered(stmt, *, target_status, channel_id, pain, min_score):
    """Отбор целей — одним куском кода для списка и для массового решения.

    Пока это была вложенная функция внутри списка, массовое решение «отклонить всё,
    что под фильтром» вынуждено было бы повторить те же четыре условия своими руками.
    Ровно так расходятся выборка на экране и выборка под кнопкой, а расхождение здесь
    означает, что человек отклонил не то, что видел.
    """
    if target_status:
        stmt = stmt.where(WfTarget.status == target_status)
    if channel_id is not None:
        stmt = stmt.where(WfTarget.channel_id == channel_id)
    if pain:
        stmt = stmt.where(WfTarget.pain == pain)
    if min_score:
        stmt = stmt.where(WfTarget.score >= min_score)
    return stmt


@router.get("/targets")
async def targets(db: GetDB, wf: Workflow = GetWorkflow,
                  user=requires(Section.LEADS),
                  p: ListParams = Depends(list_params),
                  status_filter: str | None = Query(None, alias="status"),
                  channel_id: int | None = None,
                  pain: str | None = None,
                  min_score: int | None = None):
    """Цели сценария — обобщение экрана лидов.

    Счётчики по статусам считаются **в пределах сценария**: общая сводка по
    `wf_targets` показывала бы сумму по всем конвейерам, и «двадцать новых» в блоке
    публичного ответа означало бы двадцать где-то ещё.
    """
    _check(status_filter, TARGET_STATUSES, "статус")

    q = (select(WfTarget, Channel)
         .join(Channel, WfTarget.channel_id == Channel.id)
         .where(WfTarget.workflow_id == wf.id))
    count_q = (select(func.count(WfTarget.id))
               .join(Channel, WfTarget.channel_id == Channel.id)
               .where(WfTarget.workflow_id == wf.id))

    keep = dict(target_status=status_filter, channel_id=channel_id, pain=pain,
                min_score=min_score)
    q, count_q = _filtered(q, **keep), _filtered(count_q, **keep)
    search = [WfTarget.author_name, WfTarget.author_username, WfTarget.quote,
              WfTarget.pain]
    q, count_q = apply_search(q, p, search), apply_search(count_q, p, search)

    total = (await db.execute(count_q)).scalar_one()
    q = apply_sort(q, p, TARGET_SORTS, default="score", tiebreak=WfTarget.id)
    rows = (await db.execute(q.limit(p.limit).offset(p.offset))).all()

    by_status = dict((await db.execute(
        select(WfTarget.status, func.count(WfTarget.id))
        .where(WfTarget.workflow_id == wf.id)
        .group_by(WfTarget.status))).all())

    out = [{
        "id": t.id, "target_kind": t.target_kind,
        "addressing": _addressing(t),
        "author_name": t.author_name or "—",
        "author_username": ("@" + t.author_username) if t.author_username else None,
        "channel": c.title, "channel_id": t.channel_id,
        "message_id": t.message_id,
        "pain": t.pain, "score": t.score, "status": t.status,
        "quote": t.quote, "reject_reason": t.reject_reason,
        "score_breakdown": t.score_breakdown or [],
        "disqualifiers": t.disqualifiers or [],
        "created_at": t.created_at.isoformat() if t.created_at else None,
    } for t, c in rows]

    return {**p.page(total), "workflow": wf.key,
            "target_kind": wf.target_kind,
            # Заголовок колонки — в конверте, а не только в строках: пустой список
            # тоже рисует шапку таблицы, и без этого поля она осталась бы без имени
            # ровно там, где человек и спрашивает «а что тут должно быть».
            "addressing_label": ADDRESSING_LABEL.get(wf.target_kind, "Кому"),
            "rows": out,
            "states": [{"key": k, "count": by_status.get(k, 0)}
                       for k in TARGET_STATUSES]}


@router.get("/pains")
async def pains(db: GetDB, wf: Workflow = GetWorkflow, user=requires(Section.LEADS)):
    """Боли, встречающиеся у целей этого сценария, — для выпадающего фильтра.

    Считается по своим целям, а не по справочнику каскада: показывать в фильтре боль,
    которой в этом конвейере ни разу не было, значит предлагать заведомо пустой отбор.
    """
    rows = (await db.execute(
        select(WfTarget.pain, func.count(WfTarget.id))
        .where(WfTarget.workflow_id == wf.id, WfTarget.pain.isnot(None))
        .group_by(WfTarget.pain).order_by(func.count(WfTarget.id).desc()))).all()
    return {"rows": [{"pain": p_, "count": n} for p_, n in rows]}


# ── черновики сценария ────────────────────────────────────────────────────────

@router.get("/drafts")
async def drafts(db: GetDB, wf: Workflow = GetWorkflow,
                 user=requires(Section.DRAFTS),
                 p: ListParams = Depends(list_params),
                 state: str | None = None):
    """Очередь заготовок сценария.

    **Ручка не только читает** — она достраивает очередь: целям без черновика заводит
    заготовку и переводит их в `in_review`. Так же устроен и старый экран черновиков,
    и по той же причине: генератор шаблонный и стоит микросекунды, а фоновый воркер
    ради него был бы лишним местом, где что-то молча не запустится.

    Сказано это вслух, потому что `GET`, меняющий данные, — то, что читатель кода
    вправе не ожидать. Когда появится генератор на модели, достройка уедет в воркер.
    """
    _check(state, DRAFT_STATES, "статус")

    created = await wf_drafting.ensure_queue(db, wf)
    if created:
        await db.commit()
        logger.info("wf_queue_filled workflow=%s created=%s", wf.key, created)

    q = (select(WfDraft, WfTarget, Channel)
         .join(WfTarget, WfDraft.target_id == WfTarget.id)
         .join(Channel, WfTarget.channel_id == Channel.id)
         .where(WfDraft.workflow_id == wf.id))
    count_q = (select(func.count(WfDraft.id))
               .join(WfTarget, WfDraft.target_id == WfTarget.id)
               .where(WfDraft.workflow_id == wf.id))
    if state:
        q = q.where(WfDraft.state == state)
        count_q = count_q.where(WfDraft.state == state)

    total = (await db.execute(count_q)).scalar_one()
    q = apply_sort(q, p, DRAFT_SORTS, default="created", tiebreak=WfDraft.id)
    rows = (await db.execute(q.limit(p.limit).offset(p.offset))).all()

    by_state = dict((await db.execute(
        select(WfDraft.state, func.count(WfDraft.id))
        .where(WfDraft.workflow_id == wf.id).group_by(WfDraft.state))).all())

    out = [{
        "id": d.id, "target_id": t.id, "state": d.state,
        "addressing": _addressing(t),
        "author_name": t.author_name or "—",
        "channel": c.title, "pain": t.pain, "score": t.score,
        "quote": t.quote,
        "variants": d.variants or [],
        "chosen_variant": d.chosen_variant, "final_text": d.final_text,
        "reject_reason": d.reject_reason,
        "prompt_version": d.prompt_version,
        "source_message_link": d.source_message_link,
        "created_at": d.created_at.isoformat() if d.created_at else None,
    } for d, t, c in rows]

    return {**p.page(total), "workflow": wf.key, "action": wf.action,
            "created_now": created, "rows": out,
            "states": [{"key": k, "count": by_state.get(k, 0)} for k in DRAFT_STATES]}


def _one(wf: Workflow, d: WfDraft, t: WfTarget, c: Channel) -> dict:
    """Черновик целиком — для карточки, а не для строки таблицы.

    Одна форма на курсорную выдачу и на прямую ссылку: экран у них общий, и разойдись
    эти два ответа хоть одним полем, карточка, открытая из таблицы, отличалась бы от
    той же карточки, до которой дошли стрелкой.
    """
    return {
        "id": d.id, "target_id": t.id, "state": d.state,
        "workflow": wf.key, "action": wf.action,
        "addressing": _addressing(t),
        "author_name": t.author_name or "—",
        "author_username": ("@" + t.author_username) if t.author_username else None,
        "channel": c.title, "pain": t.pain, "score": t.score, "quote": t.quote,
        "score_breakdown": t.score_breakdown or [],
        "disqualifiers": t.disqualifiers or [],
        "variants": d.variants or [],
        # Имя поля дословно как в `/api/v1/drafts` — `thread`, а не `thread_context`.
        # Карточку рисует один и тот же экран, и расхождение в одном ключе означало
        # бы, что ветку вокруг цели он показывает только в одном из двух контуров.
        "thread": d.thread_context or [],
        "chosen_variant": d.chosen_variant, "final_text": d.final_text,
        "reject_reason": d.reject_reason, "decided_by": d.decided_by,
        "decided_at": d.decided_at.isoformat() if d.decided_at else None,
        "prompt_version": d.prompt_version,
        "source_message_link": d.source_message_link,
    }


# `/drafts/next` объявлен раньше `/drafts/{draft_id}` — не для красоты. FastAPI
# сопоставляет маршруты в порядке объявления, и литеральный путь, оказавшийся после
# параметризованного, перехватывается им и начинает отвечать «422, это не число».
# В старой очереди черновиков так уже уезжал `/reasons`, и правка молча переставала
# открываться.

@router.get("/drafts/next")
async def next_draft(db: GetDB, wf: Workflow = GetWorkflow,
                     user=requires(Section.DRAFTS),
                     after: int | None = None, state: str = "pending"):
    """Следующий черновик сценария в выбранном срезе очереди.

    Курсор, а не страница списка: экран показывает по одному и двигается стрелками,
    и «дай следующий после этого» — единственный вопрос, который он задаёт.

    По умолчанию срез — неразобранные: ради них экран и существует. Но разобранный
    черновик обязан оставаться доступным для просмотра, иначе решение оператора
    исчезает с глаз сразу после того, как принято.

    Пусто — это `draft: null`, а не 404: разобранная очередь нормальное состояние
    экрана, а не ошибка запроса.
    """
    if state != "all":
        _check(state, DRAFT_STATES, "статус")

    created = await wf_drafting.ensure_queue(db, wf)
    if created:
        await db.commit()
        logger.info("wf_queue_filled workflow=%s created=%s", wf.key, created)

    base = (select(WfDraft, WfTarget, Channel)
            .join(WfTarget, WfDraft.target_id == WfTarget.id)
            .join(Channel, WfTarget.channel_id == Channel.id)
            .where(WfDraft.workflow_id == wf.id))
    count_q = select(func.count(WfDraft.id)).where(WfDraft.workflow_id == wf.id)
    if state != "all":
        base = base.where(WfDraft.state == state)
        count_q = count_q.where(WfDraft.state == state)

    remaining = (await db.execute(count_q)).scalar_one()

    row = None
    if after is not None:
        row = (await db.execute(
            base.where(WfDraft.id > after).order_by(WfDraft.id).limit(1))).first()
    if row is None:
        # Дойдя до конца, заворачиваем на начало — так же ведёт себя старая очередь.
        row = (await db.execute(base.order_by(WfDraft.id).limit(1))).first()

    return {"remaining": remaining, "state": state, "workflow": wf.key,
            "action": wf.action,
            "draft": _one(wf, *row) if row is not None else None}


@router.get("/drafts/{draft_id}")
async def draft(draft_id: int, db: GetDB, wf: Workflow = GetWorkflow,
                user=requires(Section.DRAFTS)):
    """Один черновик целиком — с веткой вокруг цели.

    Принадлежность сценарию проверяется в запросе, а не после выборки: иначе черновик
    чужого конвейера отдавался бы по прямой ссылке любому, кому открыт хоть один.

    Конверт тот же, что у курсора (`{remaining, state, draft}`), хотя для одной
    записи он и выглядит избыточным. Причина не в красоте: экран очереди один на оба
    контура и на оба входа — стрелкой и по прямой ссылке из таблицы. Отдай эта ручка
    голый объект, и экрану пришлось бы различать, откуда он открыт, — а это ровно то
    место, где потом обнаруживается, что счётчик «осталось» показывает ноль.
    """
    row = (await db.execute(
        select(WfDraft, WfTarget, Channel)
        .join(WfTarget, WfDraft.target_id == WfTarget.id)
        .join(Channel, WfTarget.channel_id == Channel.id)
        .where(WfDraft.id == draft_id, WfDraft.workflow_id == wf.id))).first()
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND,
                            f"черновик {draft_id} в сценарии {wf.key!r} не найден")
    d = row[0]
    return {"remaining": await _pending(db, wf), "state": d.state,
            "workflow": wf.key, "draft": _one(wf, *row)}


# ── решения по целям ──────────────────────────────────────────────────────────

async def _delivered_target_ids(db, wf: Workflow, target_ids: list[int]) -> set[int]:
    """Цели, по которым действие уже дошло до людей.

    Смотрим на `delivered_message_id`, а не на `allowed`: «гейт пропустил» и «Telegram
    принял» — разные события, и необратимо только второе. У публичного ответа правило
    то же, хоть отменить его и нельзя иначе, чем удалив сообщение: поменять статус
    цели, под которой уже висит наш ответ, значит соврать в отчётности.
    """
    if not target_ids:
        return set()
    rows = (await db.execute(
        select(WfOutbound.target_id)
        .where(WfOutbound.workflow_id == wf.id,
               WfOutbound.target_id.in_(target_ids),
               WfOutbound.delivered_message_id.isnot(None)))).all()
    return {r[0] for r in rows if r[0] is not None}


@router.post("/targets/bulk")
async def targets_bulk(body: BulkRequest, request: Request, db: GetDB,
                       wf: Workflow = GetWorkflow,
                       user=permits(Section.LEADS, Capability.BULK_DECIDE)):
    """Массовое решение по целям сценария.

    Тело запроса — тот же `BulkRequest`, что у `/api/v1/leads/bulk`, и импортирован
    он оттуда, а не переписан. Экран целей — один шаблон на все сценарии; разойдись
    формы тела, и шаблону пришлось бы знать, какому конвейеру что слать.

    Три предохранителя те же и по тем же причинам: отправленное не трогается,
    количество сверяется с тем, что видел человек, разборщику положен потолок.
    Добавился четвёртый, свой: **выборка ограничена сценарием**. Без него «отклонить
    всё под фильтром» в блоке публичных ответов выкосило бы и личные сообщения.
    """
    target = BULK_ACTIONS.get(body.action)
    if target is None:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            f"действие «{body.action}» неизвестно, ожидается одно из "
            f"{', '.join(BULK_ACTIONS)}")
    if body.action == "reject" and not (body.reason or "").strip():
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            "массовое отклонение требует причины")

    scoped = select(WfTarget.id).where(WfTarget.workflow_id == wf.id)
    if body.ids:
        stmt = scoped.where(WfTarget.id.in_(body.ids))
    elif body.filter is not None:
        f = body.filter
        _check(f.get("status"), TARGET_STATUSES, "статус")
        stmt = _filtered(scoped, target_status=f.get("status"),
                         channel_id=f.get("channel_id"), pain=f.get("pain"),
                         min_score=f.get("min_score"))
    else:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            "нужно передать либо ids, либо filter")

    matched = [r[0] for r in (await db.execute(stmt)).all()]

    if body.expect is not None and body.expect != len(matched):
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"выборка изменилась: экран показывал {body.expect}, сейчас под условие "
            f"подходит {len(matched)}. Обновите список и повторите")

    if user.role == Role.REVIEWER and len(matched) > BULK_LIMIT_REVIEWER:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            f"за раз можно решить не больше {BULK_LIMIT_REVIEWER} целей, "
            f"в выборке {len(matched)}")

    sent = await _delivered_target_ids(db, wf, matched)
    ids = [i for i in matched if i not in sent]
    if not ids:
        return {"changed": 0, "drafts_changed": 0, "skipped_sent": sorted(sent),
                "matched": len(matched)}

    rows = (await db.execute(
        select(WfTarget).where(WfTarget.id.in_(ids)))).scalars().all()
    for t in rows:
        t.status = target
        t.reject_reason = body.reason if body.action == "reject" else None

    # Черновики идут следом: отклонённая цель, оставшаяся в очереди на ревью, — это
    # та же цель, которую человек уже разобрал, показанная ему второй раз.
    drafts_changed = 0
    if body.action != "reset":
        pending = (await db.execute(
            select(WfDraft).where(WfDraft.workflow_id == wf.id,
                                  WfDraft.target_id.in_(ids),
                                  WfDraft.state == "pending"))).scalars().all()
        for d in pending:
            d.state = target
            d.reject_reason = body.reason if body.action == "reject" else None
            d.decided_by = user.email
            d.decided_at = clock.utcnow()
        drafts_changed = len(pending)

    db.add(AuditLog(
        user_id=user.id, user_email=user.email, action="wf_target_bulk",
        detail={"workflow": wf.key, "action": body.action, "reason": body.reason,
                "count": len(ids), "drafts": drafts_changed,
                "skipped_sent": sorted(sent), "by_filter": body.ids is None,
                "filter": body.filter},
        ip=request.client.host if request.client else None))
    await db.commit()
    logger.warning("wf_target_bulk workflow=%s %s count=%s by=%s reason=%s",
                   wf.key, body.action, len(ids), user.email, body.reason)

    return {"changed": len(ids), "drafts_changed": drafts_changed,
            "skipped_sent": sorted(sent), "matched": len(matched)}


# `/targets/bulk` объявлен раньше `/targets/{target_id}` намеренно — по той же
# причине, по какой такая же оговорка стоит в `leads.py`: литеральный путь, попавший
# после параметризованного, однажды перехватывается им и начинает отвечать «422, это
# не число». Методы здесь разные, и сегодня это не столкнулось бы, но порядок держим
# такой же, чтобы правило не пришлось вспоминать заново на третьей ручке.

class TargetPatch(BaseModel):
    status: str | None = None
    pain: str | None = None
    reject_reason: str | None = None


@router.patch("/targets/{target_id}")
async def update_target(target_id: int, body: TargetPatch, request: Request,
                        db: GetDB, wf: Workflow = GetWorkflow,
                        user=permits(Section.LEADS, Capability.LEAD_STATUS)):
    """Правка одной цели: статус и/или боль.

    Боль правится руками намеренно, как и у лидов: это разметка, и она же датасет, по
    которому меряется качество классификации.

    Принадлежность сценарию — в запросе, а не проверкой после выборки: иначе цель
    чужого конвейера правилась бы по прямой ссылке любому, кому открыт хоть один.
    """
    _check(body.status, TARGET_STATUSES, "статус")
    t = (await db.execute(
        select(WfTarget).where(WfTarget.id == target_id,
                               WfTarget.workflow_id == wf.id))).scalar_one_or_none()
    if t is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND,
                            f"цель {target_id} в сценарии {wf.key!r} не найдена")

    if await _delivered_target_ids(db, wf, [target_id]):
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"по цели {target_id} действие уже совершено — статус менять нельзя")

    before = {"status": t.status, "pain": t.pain}
    if body.status:
        t.status = body.status
        t.reject_reason = body.reject_reason if body.status == "rejected" else None
    if body.pain is not None:
        t.pain = body.pain

    db.add(AuditLog(
        user_id=user.id, user_email=user.email, action="wf_target_update",
        detail={"workflow": wf.key, "target_id": target_id, "from": before,
                "to": {"status": t.status, "pain": t.pain}},
        ip=request.client.host if request.client else None))
    await db.commit()
    logger.info("wf_target_updated workflow=%s target=%s by=%s status=%s",
                wf.key, target_id, user.email, t.status)
    return {"id": target_id, "workflow": wf.key, "status": t.status, "pain": t.pain}


# ── решения по черновикам ─────────────────────────────────────────────────────

async def _for_decision(db, wf: Workflow, draft_id: int) -> tuple[WfDraft, WfTarget]:
    """Черновик сценария, по которому ещё можно принять или изменить решение.

    Проверяется ровно одно — доставлено ли. Состояние черновика не проверяется
    намеренно: пока система в сухом прогоне, одобрение это запись в базе, и человек,
    ошибившийся в очереди из сотни, не должен идти за исправлением в psql.
    """
    row = (await db.execute(
        select(WfDraft, WfTarget)
        .join(WfTarget, WfDraft.target_id == WfTarget.id)
        .where(WfDraft.id == draft_id, WfDraft.workflow_id == wf.id))).first()
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND,
                            f"черновик {draft_id} в сценарии {wf.key!r} не найден")
    d, t = row
    delivered = (await db.execute(
        select(WfOutbound)
        .where(WfOutbound.workflow_id == wf.id, WfOutbound.draft_id == draft_id,
               WfOutbound.delivered_message_id.isnot(None))
        .limit(1))).scalar_one_or_none()
    if delivered is not None:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"по черновику {draft_id} действие уже совершено "
            f"({delivered.created_at:%d.%m.%Y %H:%M}) — решение изменить нельзя")
    return d, t


async def _pending(db, wf: Workflow) -> int:
    return (await db.execute(
        select(func.count(WfDraft.id))
        .where(WfDraft.workflow_id == wf.id,
               WfDraft.state == "pending"))).scalar_one()


async def _gate_verdict(db, wf: Workflow, draft: WfDraft, target: WfTarget,
                        text: str) -> dict:
    """Прогнать одобренный текст через гейт, ничего не отправляя.

    **Только для личных сообщений.** Проверки в `invariants.check_all` написаны про
    переписку с человеком: «уже писали этому», «получатель — админ», «тихие часы по
    его местному времени». У публичного ответа адресата-человека нет вовсе, и прогон
    выдал бы уверенный зелёный, посчитанный не про то. Зелёный, посчитанный не про то,
    хуже отсутствующего: на него смотрят как на разрешение.

    Поэтому у прочих контуров ответ честный — `checked: false` с причиной. Гейт
    публичных ответов появится вместе с их отправкой (SPEC §2.4, этап 4).
    """
    if wf.action != "dm":
        return {"checked": False, "allowed": None,
                "reasons": ["проверки исходящих написаны под личные сообщения; "
                            f"для действия «{wf.action}» гейт ещё не заведён"]}

    gate = OutboundGate(engage_client=None, mode_provider=lambda: current_mode(db),
                        journal=None)
    req = SendRequest(
        draft_id=draft.id, conversation_id=0, account_id=0,
        recipient_peer_id=target.recipient_peer_id or 0, text=text,
        draft_state="approved", is_first_message=True,
        # Те же заглушки, что в `drafts.py`: истории отправок по этому контуру пока
        # нет ни одной. Значения намеренно совпадают дословно — `wf_drafts` обязан
        # оставаться точной тенью `drafts`, пока экраны не переехали.
        sent_count=0, last_sent_at=None,
        recipient_local_hour=(clock.utcnow().hour + 3) % 24,
        recipient_is_admin=False, previously_contacted=False,
    )
    verdict = await gate.evaluate(req, clock.utcnow())
    return {"checked": True, "allowed": verdict.allowed, "reasons": verdict.reasons}


class WfApproveRequest(BaseModel):
    variant_index: int = Field(ge=0)
    text: str | None = None


@router.post("/drafts/{draft_id}/approve")
async def approve_draft(draft_id: int, body: WfApproveRequest, request: Request,
                        db: GetDB, wf: Workflow = GetWorkflow,
                        user=permits(Section.DRAFTS, Capability.DRAFT_DECIDE)):
    """Одобрить вариант — при необходимости с правкой текста.

    Правка и одобрение — одна ручка, потому что в интерфейсе это одно действие.
    Разделять их значило бы допустить состояние «текст поправлен, но не одобрен»,
    которого на экране не существует.
    """
    d, t = await _for_decision(db, wf, draft_id)
    variants = d.variants or []
    if body.variant_index >= len(variants):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            f"вариант {body.variant_index} не существует "
                            f"(их {len(variants)})")

    original = variants[body.variant_index]["text"]
    # Приоритет: присланный текст → ранее сохранённая правка → исходный вариант.
    # Иначе одобрение после «сохранить с пометкой» молча пустило бы в дело
    # генерацию, а не то, что человек написал руками.
    text = (body.text or d.final_text or original).strip()
    if not text:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            "пустой текст сообщения")
    edited = text != original
    send = await _gate_verdict(db, wf, d, t, text)

    previous = d.state
    d.state = "approved"
    d.chosen_variant = body.variant_index
    d.final_text = text
    d.decided_by = user.email
    d.decided_at = clock.utcnow()
    t.status = "approved"

    db.add(AuditLog(
        user_id=user.id, user_email=user.email, action="wf_draft_approve",
        detail={"workflow": wf.key, "draft_id": draft_id, "target_id": t.id,
                "from": previous, "variant_index": body.variant_index,
                "edited": edited, "send_checked": send["checked"],
                "send_allowed": send["allowed"], "send_reasons": send["reasons"]},
        ip=request.client.host if request.client else None))
    await db.commit()
    logger.info("wf_draft_approved workflow=%s draft=%s by=%s edited=%s allowed=%s",
                wf.key, draft_id, user.email, edited, send["allowed"])

    return {"draft_id": draft_id, "workflow": wf.key, "decision": "approved",
            "variant_index": body.variant_index, "edited": edited,
            "send": send, "remaining": await _pending(db, wf)}


class WfEditRequest(BaseModel):
    variant_index: int = Field(ge=0)
    text: str


@router.post("/drafts/{draft_id}/edit")
async def edit_draft(draft_id: int, body: WfEditRequest, request: Request,
                     db: GetDB, wf: Workflow = GetWorkflow,
                     user=permits(Section.DRAFTS, Capability.DRAFT_DECIDE)):
    """Сохранить правку, НЕ принимая решения.

    Отдельно от одобрения, потому что это разные действия: «текст поправлен, ещё
    думаю» — нормальное состояние работы, и заставлять человека одобрять только ради
    того, чтобы не потерять правку, значит подталкивать его к решению.
    """
    d, t = await _for_decision(db, wf, draft_id)
    variants = d.variants or []
    if body.variant_index >= len(variants):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            f"вариант {body.variant_index} не существует "
                            f"(их {len(variants)})")

    text = body.text.strip()
    if not text:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            "пустой текст сообщения")

    d.chosen_variant = body.variant_index
    d.final_text = text
    # Состояние намеренно не трогаем: черновик остаётся неразобранным.
    db.add(AuditLog(
        user_id=user.id, user_email=user.email, action="wf_draft_edit",
        detail={"workflow": wf.key, "draft_id": draft_id, "target_id": t.id,
                "variant_index": body.variant_index,
                "changed": text != variants[body.variant_index]["text"]},
        ip=request.client.host if request.client else None))
    await db.commit()
    logger.info("wf_draft_edited workflow=%s draft=%s by=%s",
                wf.key, draft_id, user.email)

    return {"draft_id": draft_id, "workflow": wf.key, "saved": True,
            "state": d.state, "edited": True, "text": text}


class WfRejectRequest(BaseModel):
    reason_n: int = Field(ge=1, le=9)


@router.post("/drafts/{draft_id}/reject")
async def reject_draft(draft_id: int, body: WfRejectRequest, request: Request,
                       db: GetDB, wf: Workflow = GetWorkflow,
                       user=permits(Section.DRAFTS, Capability.DRAFT_DECIDE)):
    """Отклонить с типизированной причиной из закрытого справочника.

    Справочник общий с контуром ЛС (`/api/v1/drafts/reasons`) и своей ручки здесь не
    получает: он не зависит от сценария, а вторая копия начала бы расходиться.
    """
    d, t = await _for_decision(db, wf, draft_id)
    label = REASON_BY_N.get(body.reason_n)
    if label is None:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            f"причина {body.reason_n} отсутствует в справочнике")

    previous = d.state
    d.state = "rejected"
    d.reject_reason = label
    d.decided_by = user.email
    d.decided_at = clock.utcnow()
    t.status = "rejected"
    t.reject_reason = label

    db.add(AuditLog(
        user_id=user.id, user_email=user.email, action="wf_draft_reject",
        detail={"workflow": wf.key, "draft_id": draft_id, "target_id": t.id,
                "from": previous, "reason_n": body.reason_n, "reason": label},
        ip=request.client.host if request.client else None))
    await db.commit()
    logger.info("wf_draft_rejected workflow=%s draft=%s by=%s reason=%s",
                wf.key, draft_id, user.email, label)

    return {"draft_id": draft_id, "workflow": wf.key, "decision": "rejected",
            "reason_n": body.reason_n, "reason": label,
            "remaining": await _pending(db, wf)}


@router.post("/drafts/{draft_id}/reopen")
async def reopen_draft(draft_id: int, request: Request, db: GetDB,
                       wf: Workflow = GetWorkflow,
                       user=permits(Section.DRAFTS, Capability.DRAFT_REOPEN)):
    """Вернуть разобранный черновик в очередь.

    Отдельно от смены решения: «передумал, посмотрю ещё раз» и «решил иначе» — разные
    действия, и второе не должно быть единственным способом выполнить первое.

    Цель возвращается в `in_review`, а не в `new`: черновик по ней уже существует, и
    «новая» означало бы, что до цели ещё не доходили руки.
    """
    d, t = await _for_decision(db, wf, draft_id)
    if d.state == "pending":
        raise HTTPException(status.HTTP_409_CONFLICT,
                            f"черновик {draft_id} и так в очереди")

    previous = d.state
    d.state = "pending"
    d.reject_reason = None
    d.decided_by = None
    d.decided_at = None
    t.status = "in_review"
    t.reject_reason = None

    db.add(AuditLog(
        user_id=user.id, user_email=user.email, action="wf_draft_reopen",
        detail={"workflow": wf.key, "draft_id": draft_id, "target_id": t.id,
                "from": previous},
        ip=request.client.host if request.client else None))
    await db.commit()
    logger.info("wf_draft_reopened workflow=%s draft=%s by=%s from=%s",
                wf.key, draft_id, user.email, previous)

    return {"draft_id": draft_id, "workflow": wf.key, "state": "pending",
            "previous": previous, "remaining": await _pending(db, wf)}
