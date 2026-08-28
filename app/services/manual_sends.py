"""Запись того, что человек отправил руками.

Автоотправки в Radar нет и в ближайшее время не будет: Андрей пишет сам, из Telegram.
Значит единственное место, где живёт правда о том, как надо отвечать, — его голова, и
единственный способ её оттуда достать — попросить вставить отправленный текст.

Ценность не в самом тексте, а в **паре**: что предложил Radar → что человек написал на
самом деле. Каждая половина по отдельности почти бесполезна, вместе — это корпус, на
котором однажды можно будет померить качество генерации. `evaluations` пустая ровно
потому, что сравнивать пока не с чем.

Отсюда два решения, которые выглядят строгими без нужды, но обязательны:

* **Снимок предложенного делает сервер, а не клиент.** Форма присылает только выбор
  наводки и свой текст. Если бы «что предлагал Radar» приходило из браузера, пара
  перестала бы быть свидетельством: её половину писал бы тот же, кто пишет вторую.
* **Наводка обязана принадлежать тому же workflow.** Иначе запись свяжет ответ из
  одного контура с предложением из другого, и обнаружится это на этапе, когда данные
  уже начали считать.

Наводка при этом необязательна. Андрей мог написать тому, кого Radar не находил, — и
это тоже ценные данные: отказаться их принять значит потерять их совсем.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from sqlalchemy import func, or_, select

from app.core import clock
from app.db.models import Channel, ManualSend, Message, WfDraft, WfTarget, Workflow

logger = logging.getLogger(__name__)

# Запись — рассказ о прошлом. Небольшой запас вперёд оставлен на расхождение часов
# между машиной человека и сервером; всё, что дальше, — это опечатка в дате, и молча
# принять её значит получить отправку из будущего в любом отчёте.
FUTURE_TOLERANCE = timedelta(minutes=10)

CANDIDATE_LIMIT = 50
HISTORY_LIMIT = 200


class ManualSendError(ValueError):
    """Запись не может быть принята в том виде, в каком пришла."""


def check_sent_at(sent_at: datetime | None) -> None:
    """Время отправки, как его назвал человек.

    Проверка одна на запись и на правку: разъехавшись, они дали бы дыру, через
    которую в базу попадает то, что при создании не принимается.
    """
    if sent_at is None:
        return
    if sent_at.tzinfo is None:
        raise ManualSendError("время отправки без часового пояса — "
                              "непонятно, какой момент имеется в виду")
    if sent_at > clock.utcnow() + FUTURE_TOLERANCE:
        raise ManualSendError("время отправки в будущем")


def chosen_text(draft: WfDraft | None) -> str | None:
    """Текст, который человек видел перед глазами.

    Порядок именно такой: правка оператора важнее выбранного варианта, выбранный —
    важнее первого. Первый вариант в конце, потому что «ничего не выбрано» на экране
    выглядит как показанный первый.
    """
    if draft is None:
        return None
    if draft.final_text:
        return draft.final_text
    variants = draft.variants or []
    if not variants:
        return None
    index = draft.chosen_variant
    if index is not None and 0 <= index < len(variants):
        return variants[index].get("text")
    return variants[0].get("text")


async def candidates(db, *, workflow: Workflow, q: str | None = None,
                     limit: int = 20) -> list[dict]:
    """Наводки, из которых человек выбирает, кому он написал.

    Сортировка по свежести, а не по оценке: человек ищет то, что делал сегодня, и
    список «самых качественных за всё время» ему в этом не помощник.
    """
    limit = max(1, min(limit, CANDIDATE_LIMIT))
    stmt = (select(WfTarget, Channel, WfDraft)
            .join(Channel, WfTarget.channel_id == Channel.id)
            .outerjoin(WfDraft, WfDraft.target_id == WfTarget.id)
            .where(WfTarget.workflow_id == workflow.id))

    if q:
        like = f"%{q.lower()}%"
        stmt = stmt.where(or_(func.lower(WfTarget.author_name).like(like),
                              func.lower(WfTarget.author_username).like(like),
                              func.lower(WfTarget.quote).like(like)))

    rows = (await db.execute(
        stmt.order_by(WfTarget.created_at.desc(), WfTarget.id.desc())
        .limit(limit))).all()

    return [{
        "target_id": target.id,
        "author_name": target.author_name or "—",
        "author_username": ("@" + target.author_username) if target.author_username else None,
        "channel": channel.title,
        "pain": target.pain,
        "score": target.score,
        "quote": target.quote,
        "status": target.status,
        "suggested_text": chosen_text(draft),
        "draft_id": draft.id if draft is not None else None,
        "created_at": target.created_at.isoformat(),
    } for target, channel, draft in rows]


async def _resolve_target(db, *, workflow: Workflow,
                          target_id: int | None) -> tuple[WfTarget | None, WfDraft | None]:
    if target_id is None:
        return None, None

    target = (await db.execute(
        select(WfTarget).where(WfTarget.id == target_id))).scalar_one_or_none()
    if target is None:
        raise ManualSendError(f"наводка {target_id} не найдена")
    if target.workflow_id != workflow.id:
        raise ManualSendError(
            f"наводка {target_id} принадлежит другому workflow — связать ответ одного "
            f"контура с предложением другого нельзя")

    draft = (await db.execute(
        select(WfDraft).where(WfDraft.target_id == target.id))).scalar_one_or_none()
    return target, draft


async def record(db, *, workflow: Workflow, text: str, recorded_by: str,
                 target_id: int | None = None, engage_account_id: int | None = None,
                 sent_at: datetime | None = None,
                 note: str | None = None) -> ManualSend:
    """Записать факт отправки. Возвращает сохранённую запись (без коммита).

    Коммит оставлен вызывающему: рядом пишется запись в журнал действий, и обе должны
    лечь одной транзакцией — иначе в журнале появится отправка, которой нет в данных.
    """
    body = (text or "").strip()
    if not body:
        raise ManualSendError("текст пустой — записывать нечего")

    check_sent_at(sent_at)
    target, draft = await _resolve_target(db, workflow=workflow, target_id=target_id)

    entry = ManualSend(
        workflow_id=workflow.id,
        target_id=target.id if target is not None else None,
        draft_id=draft.id if draft is not None else None,
        message_id=target.message_id if target is not None else None,
        engage_account_id=engage_account_id,
        text=body,
        # Снимок делает сервер: пара «предложено → отправлено» перестала бы быть
        # свидетельством, если бы её первую половину присылал браузер.
        suggested_text=chosen_text(draft),
        sent_at=sent_at,
        recorded_by=recorded_by,
        note=(note or "").strip() or None,
    )
    db.add(entry)
    await db.flush()
    logger.info("manual_send_recorded id=%s workflow=%s target=%s by=%s chars=%s",
                entry.id, workflow.key, entry.target_id, recorded_by, len(body))
    return entry


CORRECTABLE = ("text", "note", "sent_at", "engage_account_id")


def correct(entry: ManualSend, fields: dict) -> list[str]:
    """Применить правку. Возвращает список изменённых полей.

    `fields` — только то, что действительно прислали: разница между «поле не пришло» и
    «поле пришло пустым» здесь содержательная. Первое значит «не трогай», второе —
    «сотри заметку», и схлопывать их нельзя.

    Наводки и снимка предложенного в списке правимого нет. Сменить наводку — это не
    правка, а другая запись («на самом деле я отвечал не тому»); снимок же и есть
    свидетельство, ради которого всё затевалось, и переписываемое свидетельство
    ничего не стоит.
    """
    unknown = set(fields) - set(CORRECTABLE)
    if unknown:
        raise ManualSendError(f"эти поля не правятся: {', '.join(sorted(unknown))}")

    changed = []
    if "text" in fields:
        text = (fields["text"] or "").strip()
        if not text:
            raise ManualSendError("текст пустой — записывать нечего")
        if text != entry.text:
            entry.text = text
            changed.append("text")
    if "note" in fields:
        note = (fields["note"] or "").strip() or None
        if note != entry.note:
            entry.note = note
            changed.append("note")
    if "engage_account_id" in fields:
        if fields["engage_account_id"] != entry.engage_account_id:
            entry.engage_account_id = fields["engage_account_id"]
            changed.append("engage_account_id")
    if "sent_at" in fields:
        check_sent_at(fields["sent_at"])
        if fields["sent_at"] != entry.sent_at:
            entry.sent_at = fields["sent_at"]
            changed.append("sent_at")
    return changed


def matches_suggestion(entry: ManualSend) -> bool:
    """Совпало ли отправленное с тем, что предлагал Radar.

    Отдельной функцией, а не выражением по месту: это же сравнение спрашивает лента
    активности сценария, и вторая копия начала бы расходиться в мелочи — например, в
    том, обрезаются ли пробелы. Расхождение здесь читалось бы как «одна и та же
    запись в двух таблицах то совпадает с подсказкой, то нет».

    Записи без снимка (написали тому, кого Radar не находил) — не совпадение, а
    отсутствие второй половины пары; отдельного состояния под это нет намеренно:
    считать «предложенного не было» совпадением значит завысить долю попаданий.
    """
    return (entry.suggested_text is not None
            and entry.text.strip() == entry.suggested_text.strip())


def describe(entry: ManualSend, *, target: WfTarget | None = None,
             message: Message | None = None, channel: Channel | None = None) -> dict:
    """Запись для экрана. Пара «предложено → отправлено» рядом, а не в разных местах —
    ради неё всё и затевалось."""
    return {
        "id": entry.id,
        "workflow_id": entry.workflow_id,
        "target_id": entry.target_id,
        "draft_id": entry.draft_id,
        "engage_account_id": entry.engage_account_id,
        "text": entry.text,
        "suggested_text": entry.suggested_text,
        "matches_suggestion": matches_suggestion(entry),
        "note": entry.note,
        "sent_at": entry.sent_at.isoformat() if entry.sent_at else None,
        "recorded_by": entry.recorded_by,
        "recorded_at": entry.recorded_at.isoformat() if entry.recorded_at else None,
        "author_name": (target.author_name if target is not None else None) or "—",
        "author_username": (("@" + target.author_username)
                            if target is not None and target.author_username else None),
        "pain": target.pain if target is not None else None,
        # Канал приходит из справочника, а не из наводки: у записи без наводки его нет
        # и быть не может, и подставлять сюда что-либо значило бы придумывать источник.
        "channel": channel.title if channel is not None else None,
        "quote": (target.quote if target is not None
                  else (message.text if message is not None else None)),
    }


async def history(db, *, workflow_id: int | None = None, limit: int = 50,
                  offset: int = 0) -> dict:
    # Канал подтягивается тем же запросом: экран показывает его колонкой, а добирать
    # его построчно значило бы N+1 обращений там, где хватает внешнего соединения.
    stmt = (select(ManualSend, WfTarget, Message, Channel)
            .outerjoin(WfTarget, ManualSend.target_id == WfTarget.id)
            .outerjoin(Message, ManualSend.message_id == Message.id)
            .outerjoin(Channel, WfTarget.channel_id == Channel.id))
    count_stmt = select(func.count(ManualSend.id))
    if workflow_id is not None:
        stmt = stmt.where(ManualSend.workflow_id == workflow_id)
        count_stmt = count_stmt.where(ManualSend.workflow_id == workflow_id)

    # Наружу отдаём применённые значения, а не присланные: страница, собранная по
    # чужому `limit`, ломает пролистывание молча — экран считает, что показал сто
    # строк, а показал двести.
    limit = max(1, min(limit, HISTORY_LIMIT))
    offset = max(0, offset)

    total = (await db.execute(count_stmt)).scalar_one()
    rows = (await db.execute(
        stmt.order_by(ManualSend.recorded_at.desc(), ManualSend.id.desc())
        .limit(limit).offset(offset))).all()

    return {
        "total": total, "limit": limit, "offset": offset,
        "rows": [describe(entry, target=target, message=message, channel=channel)
                 for entry, target, message, channel in rows],
    }
