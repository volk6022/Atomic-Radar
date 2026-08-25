"""Данные одного сценария: поток, цели, черновики.

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
матрицы. Разводить их по разным модулям пришлось именно поэтому: сложи я их вместе,
докстринг про «открыто любому вошедшему» стал бы ложью для половины ручек.

Сценарий адресуется ключом (`cold_dm`), а не числовым id — так же, как в реестре, из
которого интерфейс строит меню. Ключ переживает пересев базы, id — нет.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select

from app.api.deps import GetDB, requires
from app.api.v1.listing import ListParams, apply_search, apply_sort, list_params
from app.core import cascade
from app.core.access import Section
from app.db.models import Channel, Message, WfDraft, WfTarget, WfVerdict, Workflow
from app.services import wf_drafting, workflows as workflow_service

logger = logging.getLogger("radar")

router = APIRouter(prefix="/api/v1/workflows/{key}", tags=["workflow-data"])

TARGET_STATUSES = ("new", "in_review", "approved", "rejected")
DRAFT_STATES = ("pending", "approved", "rejected", "edited")

TARGET_SORTS = {"score": WfTarget.score, "created": WfTarget.created_at,
                "author": WfTarget.author_name, "channel": Channel.title,
                "status": WfTarget.status, "pain": WfTarget.pain}

STREAM_SORTS = {"date": Message.tg_date, "channel": Channel.title,
                "author": Message.author_name, "level": WfVerdict.level}

DRAFT_SORTS = {"created": WfDraft.created_at, "score": WfTarget.score,
               "state": WfDraft.state, "pain": WfTarget.pain}


async def _workflow(key: str, db: GetDB) -> Workflow:
    """Сценарий по ключу — или 404.

    Зависимость, а не три одинаковых проверки в ручках: забыть её в четвёртой ручке
    значило бы отдать пустой список вместо «такого сценария нет», а пустой список
    читается как «данных пока нет» и никого не настораживает.
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


def _addressing(target: WfTarget) -> dict:
    """Куда пойдёт действие — в форме, которую экран покажет как есть.

    По спецификации (§9.2) колонка адресации зависит от `target_kind`: «Кому» у ЛС,
    «Под каким сообщением» у публичного ответа. Считает это сервер, а не экран: иначе
    правило «у публичной цели автора может не быть вовсе» пришлось бы держать и в
    шаблоне тоже.
    """
    if target.target_kind == "user":
        return {"kind": "user", "label": "Кому",
                "value": (("@" + target.author_username) if target.author_username
                          else (target.author_name or f"id{target.recipient_peer_id}")),
                "recipient_peer_id": target.recipient_peer_id}
    return {"kind": "message", "label": "Под каким сообщением",
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
    _check(passed, ("true", "false", "pending"), "фильтр")

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
    # считалось» — строки вердикта нет. Сваливать его в «ожидает» значило бы скрыть
    # разницу между «модель не ответила» и «сценарий сюда не доходил».
    where = {"true": WfVerdict.passed.is_(True),
             "false": WfVerdict.passed.is_(False),
             "pending": WfVerdict.passed.is_(None)}.get(passed or "")
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

    def filtered(stmt):
        if status_filter:
            stmt = stmt.where(WfTarget.status == status_filter)
        if channel_id is not None:
            stmt = stmt.where(WfTarget.channel_id == channel_id)
        if pain:
            stmt = stmt.where(WfTarget.pain == pain)
        if min_score:
            stmt = stmt.where(WfTarget.score >= min_score)
        return stmt

    q, count_q = filtered(q), filtered(count_q)
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
            "target_kind": wf.target_kind, "rows": out,
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


@router.get("/drafts/{draft_id}")
async def draft(draft_id: int, db: GetDB, wf: Workflow = GetWorkflow,
                user=requires(Section.DRAFTS)):
    """Один черновик целиком — с веткой вокруг цели.

    Принадлежность сценарию проверяется в запросе, а не после выборки: иначе черновик
    чужого конвейера отдавался бы по прямой ссылке любому, кому открыт хоть один.
    """
    row = (await db.execute(
        select(WfDraft, WfTarget, Channel)
        .join(WfTarget, WfDraft.target_id == WfTarget.id)
        .join(Channel, WfTarget.channel_id == Channel.id)
        .where(WfDraft.id == draft_id, WfDraft.workflow_id == wf.id))).first()
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND,
                            f"черновик {draft_id} в сценарии {wf.key!r} не найден")
    d, t, c = row
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
        "thread_context": d.thread_context or [],
        "chosen_variant": d.chosen_variant, "final_text": d.final_text,
        "reject_reason": d.reject_reason, "decided_by": d.decided_by,
        "decided_at": d.decided_at.isoformat() if d.decided_at else None,
        "prompt_version": d.prompt_version,
        "source_message_link": d.source_message_link,
    }
