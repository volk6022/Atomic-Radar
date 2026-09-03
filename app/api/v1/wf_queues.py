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
from datetime import date, datetime, time, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field
from sqlalchemy import Date, cast, func, literal_column, select

from app.api.deps import CurrentUser, GetDB, permits, requires
from app.api.v1.drafts import REASON_BY_N
from app.api.v1.leads import BULK_ACTIONS, BulkRequest
from app.api.v1.listing import ListParams, apply_search, apply_sort, list_params
from app.api.v1.system import current_mode
from app.core import cascade, clock
from app.core.access import BULK_LIMIT_REVIEWER, Capability, Role, Section
from app.core.outbound_gate import OutboundGate, SendRequest
from app.db.models import (Account, AuditLog, Channel, EngageInstance, ManualSend,
                           Message, MessageReader, WfDraft, WfOutbound, WfTarget,
                           WfVerdict, Workflow)
from app.services import (drafting, engage, manual_sends as manual_sends_service,
                          wf_drafting, workflows as workflow_service)

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

def _user_link(username: str | None) -> str | None:
    """Ссылка на адресата в Telegram — для кнопки «открыть диалог».

    Собирает сервер, а не экран: иначе форма ссылки разойдётся по экранам, и
    «у цели без юзернейма ссылки нет» один экран понял бы как `null`, а другой —
    как битый `t.me/None`. Публичной цели (`react`, `reply`) ссылки не нужно, и
    там честный `null` — не ошибка.
    """
    if not username:
        return None
    return f"https://t.me/{username.removeprefix('@')}"


async def _instance_key(db: GetDB, wf: Workflow) -> str:
    """Ключ инстанса Engage, из которого сценарий берёт аккаунты.

    Аккаунты адресуются парой (инстанс, engage_account_id) — один и тот же номер
    в двух инстансах это два разных аккаунта, поэтому фильтр и подписи читателей
    считаются по инстансу сценария, а не по всему зеркалу `accounts`.
    """
    return (await db.execute(
        select(EngageInstance.key)
        .where(EngageInstance.id == wf.engage_instance_id))).scalar_one()


async def _filter_accounts(db: GetDB, wf: Workflow,
                           instance_key: str) -> dict[int, dict]:
    """Аккаунты, по которым можно резать очередь этого сценария, и их вес.

    Источников ДВА, и это не избыточность.

    **Кто действительно читал** — `message_readers` по целям сценария. Это
    единственный источник, который на проде вообще есть: зеркало `accounts`
    заполняет только посев стенда, боевого пути записи в него нет ни одного
    (проверено 03.09: на проде 0 строк при 322 записях о прочтении). Строй список
    из одного зеркала — и фильтр, ради которого всё делалось, оказался бы пуст,
    а `_check_account` отвергал бы каждый аккаунт с 422.

    **Зеркало `accounts`** — за теми аккаунтами, что сегодня не прочитали ничего:
    пустой срез и отсутствующий пункт читаются одинаково, но первый хотя бы правда
    — человек видит, что вошёл в аккаунт, которому писать некому. Зеркало берётся
    по инстансу сценария: один и тот же номер в двух инстансах — два разных
    человека за клавиатурой.

    **Подпись — из ЖИВОГО флота Engage**, а не из зеркала: зеркало никто не
    наполняет, и подпись из него была бы вечным «аккаунт 3». Человеку предстоит
    войти в Telegram, а опознаёт он аккаунт по телефону — его и показываем,
    маскированным (`engage.mask_phone`), тем же способом, что экран флота.

    Читатель без подписи не выбрасывается, а называет себя номером: недоступность
    Engage и отставание справочника — не повод прятать аккаунт, которым сообщение
    заведомо получено, и не повод ронять фильтр целиком.
    """
    counts = dict((await db.execute(
        select(MessageReader.account_id, func.count(func.distinct(WfDraft.id)))
        .join(WfTarget, WfTarget.message_id == MessageReader.message_id)
        .join(WfDraft, WfDraft.target_id == WfTarget.id)
        .where(WfDraft.workflow_id == wf.id)
        .group_by(MessageReader.account_id))).all())

    labels = dict((await db.execute(
        select(Account.engage_account_id, Account.label)
        .where(Account.engage_instance == instance_key))).all())

    known = set(counts) | set(labels)

    # Третий источник — все, кто вообще что-то читал. Появился из-за перекоса во
    # времени: атрибуция приёма пишется с 02.09, а целями становились сообщения
    # СТАРОГО бэкфилла, прочитанные до неё, — 322 записи о прочтении и ровно ноль
    # пересечений с очередью, то есть пустой фильтр при пяти работающих аккаунтах.
    #
    # Сам перекос закрыт 03.09: атрибуция восстановлена задним числом из журнала
    # задач Engage, теперь читатель есть у всех сообщений
    # (`scripts/recover_message_readers.sql`). Источник тем не менее остаётся, и не
    # для истории: аккаунт, который сегодня ничего не прочитал, обязан быть в
    # списке и показать честный пустой срез. Человек, вошедший в свой аккаунт, не
    # должен гадать, почему его там нет.
    #
    # ⚠️ Берётся только когда инстанс в реестре ОДИН: `message_readers` хранит номер
    # аккаунта без инстанса, и при двух инстансах этот источник смешал бы чужие
    # аккаунты со своими. Тогда список сужается до первых двух — то есть до заведомо
    # своих. Ветка на два инстанса тестами не покрыта: в реестре его сегодня один.
    if (await db.execute(select(func.count(EngageInstance.id)))).scalar_one() == 1:
        known |= set((await db.execute(
            select(MessageReader.account_id).distinct())).scalars())

    # Живой флот — только за подписью. Отказ Engage гасится здесь, а не общим
    # обработчиком: список аккаунтов — условие работы (без него человек не отберёт
    # свои черновики), а подпись — удобство. Ронять первое ради второго нельзя.
    fleet: dict[int, str] = {}
    try:
        for a in await engage.list_accounts(instance=instance_key):
            acc_id = a.get("id")
            if acc_id is None:
                continue
            phone = engage.mask_phone(a.get("phone"))
            fleet[int(acc_id)] = (f"аккаунт {acc_id} · {phone}" if phone != "—"
                                  else f"аккаунт {acc_id}")
    except engage.EngageUnavailable as e:
        logger.info("drafts_accounts_fleet_unavailable instance=%s: %s", instance_key, e)

    return {acc_id: {"account_id": acc_id,
                     "label": (fleet.get(acc_id) or labels.get(acc_id)
                               or f"аккаунт {acc_id}"),
                     "drafts": counts.get(acc_id, 0)}
            for acc_id in known}


async def _check_account(db: GetDB, wf: Workflow, instance_key: str,
                         account_id: int | None) -> None:
    """Фильтр по аккаунту — отказ по закрытому списку, в стиле `_check`.

    Список тот же, что предлагает `/drafts/accounts`: выпадающий список, значение
    которого ручка отвергнет, хуже отсутствующего. Молча отдать весь список при
    опечатке тоже нельзя — человек решит, что видит свой срез.
    """
    if account_id is None:
        return
    known = await _filter_accounts(db, wf, instance_key)
    if account_id not in known:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            f"неизвестный аккаунт «{account_id}» в инстансе «{instance_key}», "
            f"ожидается один из {', '.join(str(a) for a in sorted(known)) or '—'}")


def _seen_by(account_id: int):
    """Условие «исходное сообщение этой цели видел данный аккаунт».

    EXISTS, а не JOIN: у сообщения может быть несколько читателей, и соединение
    умножило бы строку черновика на каждого — `total` молча поехал бы, а вместе с
    ним и сводка состояний.
    """
    return select(MessageReader.account_id).where(
        MessageReader.message_id == WfTarget.message_id,
        MessageReader.account_id == account_id).exists()


async def _readers_by_message(db: GetDB, instance_key: str,
                              message_ids: list[int]) -> dict[int, list[dict]]:
    """Кто из аккаунтов видел исходные сообщения — одним запросом на страницу.

    Запрос на строку здесь уже случался в соседних ручках и стоил полутора сотен
    запросов на страницу в пятьдесят строк; образец пакетной доборки — `readers_by_message`
    в `screens.messages()`. Подпись берётся внешним соединением к зеркалу `accounts`
    по паре (инстанс, engage_account_id) — тем же запросом, без второй ходки.

    Читатель, которого зеркало ещё не догнало, остаётся в выдаче с запасной
    подписью: исчезнувший читатель хуже безымянного, атрибуция приёма не должна
    пропадать из-за отставания кеша. Порядок — по номеру аккаунта: он одинаков
    для строки и карточки, иначе экраны разошлись бы порядком одного и того же
    списка.
    """
    if not message_ids:
        return {}
    out: dict[int, list[dict]] = {}
    for msg_id, acc_id, label in (await db.execute(
            select(MessageReader.message_id, MessageReader.account_id, Account.label)
            .outerjoin(Account, (Account.engage_instance == instance_key)
                       & (Account.engage_account_id == MessageReader.account_id))
            .where(MessageReader.message_id.in_(message_ids))
            .order_by(MessageReader.message_id, MessageReader.account_id))).all():
        out.setdefault(msg_id, []).append({
            "account_id": acc_id,
            "label": label or f"аккаунт {acc_id}",
        })
    return out


async def _source_by_message(db, channel_by_id: dict, targets: list) -> dict:
    """Ссылки на источник для страницы: по одному набору запросов, а не на строку.

    Возвращает словарь по `message_id`, потому что строка и карточка достают его
    по-разному, а форма ответа обязана быть одна: разойдись они полем, ссылка на пост
    была бы видна только в одном из двух мест.
    """
    if not targets:
        return {}
    messages = {m.id: m for m in (await db.execute(
        select(Message).where(Message.id.in_([t.message_id for t in targets])))
    ).scalars()}
    pairs, order = [], []
    for t in targets:
        message = messages.get(t.message_id)
        channel = channel_by_id.get(t.channel_id)
        if message is None or channel is None:
            continue
        pairs.append((channel, message))
        order.append(t.message_id)
    return dict(zip(order, await drafting.source_links_many(db, pairs)))


@router.get("/drafts")
async def drafts(db: GetDB, wf: Workflow = GetWorkflow,
                 user=requires(Section.DRAFTS),
                 p: ListParams = Depends(list_params),
                 state: str | None = None,
                 account_id: int | None = None):
    """Очередь заготовок сценария.

    **Ручка не только читает** — она достраивает очередь: целям без черновика заводит
    заготовку и переводит их в `in_review`. Так же устроен и старый экран черновиков,
    и по той же причине: генератор шаблонный и стоит микросекунды, а фоновый воркер
    ради него был бы лишним местом, где что-то молча не запустится.

    Сказано это вслух, потому что `GET`, меняющий данные, — то, что читатель кода
    вправе не ожидать. Когда появится генератор на модели, достройка уедет в воркер.

    **Строка называет аккаунт приёма.** Человек рассылает руками и с одного
    аккаунта; написать адресату с аккаунта, не читавшего группу, — прийти к нему
    «ниоткуда». Поэтому каждая строка несёт `readers` (кто видел исходное
    сообщение) и готовую ссылку `tg_link` на адресата.

    Фильтр `account_id` режет ту же очередь: он применяется **до пагинации** и
    одинаково к строкам, `total` и сводке `states`. Сводка «вообще», а выборка
    «в срезе» — дефект, уже пойманный на `/channels`: чипс говорил 1, фильтр
    отдавал 2.
    """
    _check(state, DRAFT_STATES, "статус")
    instance_key = await _instance_key(db, wf)
    await _check_account(db, wf, instance_key, account_id)

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
    states_q = (select(WfDraft.state, func.count(WfDraft.id))
                .join(WfTarget, WfDraft.target_id == WfTarget.id)
                .where(WfDraft.workflow_id == wf.id))
    if state:
        q = q.where(WfDraft.state == state)
        count_q = count_q.where(WfDraft.state == state)
    if account_id is not None:
        seen = _seen_by(account_id)
        q = q.where(seen)
        count_q = count_q.where(seen)
        states_q = states_q.where(seen)

    total = (await db.execute(count_q)).scalar_one()
    q = apply_sort(q, p, DRAFT_SORTS, default="created", tiebreak=WfDraft.id)
    rows = (await db.execute(q.limit(p.limit).offset(p.offset))).all()

    by_state = dict((await db.execute(
        states_q.group_by(WfDraft.state))).all())

    readers = await _readers_by_message(
        db, instance_key, [t.message_id for _, t, _ in rows])
    source = await _source_by_message(db, {c.id: c for _, _, c in rows},
                                      [t for _, t, _ in rows])

    out = [{
        "id": d.id, "target_id": t.id, "state": d.state,
        "addressing": _addressing(t),
        "author_name": t.author_name or "—",
        "author_username": ("@" + t.author_username) if t.author_username else None,
        "tg_link": _user_link(t.author_username),
        "channel": c.title, "pain": t.pain, "score": t.score,
        "quote": t.quote,
        "readers": readers.get(t.message_id, []),
        "source": source.get(t.message_id),
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


def _one(wf: Workflow, d: WfDraft, t: WfTarget, c: Channel,
         readers: list[dict], source: dict | None = None) -> dict:
    """Черновик целиком — для карточки, а не для строки таблицы.

    Одна форма на курсорную выдачу и на прямую ссылку: экран у них общий, и разойдись
    эти два ответа хоть одним полем, карточка, открытая из таблицы, отличалась бы от
    той же карточки, до которой дошли стрелкой.

    Атрибуция приёма (`readers`) и ссылка на адресата (`tg_link`) — те же поля, что
    у строки списка, по той же причине: карточка и строка это один экран, и аккаунт
    должен быть виден в обоих местах, а не только в одном из них.
    """
    return {
        "id": d.id, "target_id": t.id, "state": d.state,
        "workflow": wf.key, "action": wf.action,
        "addressing": _addressing(t),
        "author_name": t.author_name or "—",
        "author_username": ("@" + t.author_username) if t.author_username else None,
        "tg_link": _user_link(t.author_username),
        "readers": readers,
        "source": source,
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
                     after: int | None = None, state: str = "pending",
                     account_id: int | None = None):
    """Следующий черновик сценария в выбранном срезе очереди.

    Курсор, а не страница списка: экран показывает по одному и двигается стрелками,
    и «дай следующий после этого» — единственный вопрос, который он задаёт.

    По умолчанию срез — неразобранные: ради них экран и существует. Но разобранный
    черновик обязан оставаться доступным для просмотра, иначе решение оператора
    исчезает с глаз сразу после того, как принято.

    `account_id` сужает тот же срез, что и у списка: и обход (`after`), и счётчик
    `remaining` остаются внутри него. Курсор, который шагает сквозь фильтр,
    увозит из среза молча.

    Пусто — это `draft: null`, а не 404: разобранная очередь нормальное состояние
    экрана, а не ошибка запроса.
    """
    if state != "all":
        _check(state, DRAFT_STATES, "статус")
    instance_key = await _instance_key(db, wf)
    await _check_account(db, wf, instance_key, account_id)

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
    if account_id is not None:
        seen = _seen_by(account_id)
        base = base.where(seen)
        # Соединение с целями нужно только под фильтром: условие `_seen_by`
        # коррелирует по `WfTarget.message_id`.
        count_q = (count_q.join(WfTarget, WfDraft.target_id == WfTarget.id)
                   .where(seen))

    remaining = (await db.execute(count_q)).scalar_one()

    row = None
    if after is not None:
        row = (await db.execute(
            base.where(WfDraft.id > after).order_by(WfDraft.id).limit(1))).first()
    if row is None:
        # Дойдя до конца, заворачиваем на начало — так же ведёт себя старая очередь.
        row = (await db.execute(base.order_by(WfDraft.id).limit(1))).first()

    one = None
    if row is not None:
        readers = await _readers_by_message(db, instance_key, [row[1].message_id])
        source = await _source_by_message(db, {row[2].id: row[2]}, [row[1]])
        one = _one(wf, row[0], row[1], row[2], readers.get(row[1].message_id, []),
                   source.get(row[1].message_id))

    # `readers` и `tg_link` продублированы на верхний уровень конверта — как у
    # прямой ссылки на карточку: конверты у ручек обязаны совпадать, экран их не
    # различает. При пустом срезе честные пустые значения, а не отсутствующие ключи.
    return {"remaining": remaining, "state": state, "workflow": wf.key,
            "action": wf.action,
            "readers": one["readers"] if one else [],
            "tg_link": one["tg_link"] if one else None,
            "draft": one}


@router.get("/drafts/accounts")
async def draft_accounts(db: GetDB, wf: Workflow = GetWorkflow,
                         user=requires(Section.DRAFTS)):
    """Аккаунты, из которых строится фильтр очереди.

    Ровно тот же закрытый список, по которому `_check_account` выносит отказ, —
    см. `_filter_accounts` о том, почему источников два.

    **Не живой Engage.** Соседняя `/manual-sends/accounts` спрашивает флот по сети
    и при недоступности отдаёт пустой список — там это уместно, поле необязательное.
    Здесь фильтр — единственный способ разобрать очередь по-человечески, и он не
    должен исчезать вместе с сетью до Софии.

    Порядок — по номеру аккаунта: он одинаков для строки и для фильтра, иначе
    экраны разошлись бы порядком одного и того же списка.

    ⚠️ Объявлено ВЫШЕ `/drafts/{draft_id}`: маршруты разбираются по порядку, и иначе
    «accounts» уехало бы в номер черновика, а ручка отвечала бы 422.
    """
    instance_key = await _instance_key(db, wf)
    rows = await _filter_accounts(db, wf, instance_key)
    return {"rows": [rows[k] for k in sorted(rows)]}


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

    `readers` и `tg_link` лежат и на верхнем уровне конверта, и внутри `draft`:
    верхний уровень читает экран карточки, внутренний — общий код отрисовки, и
    держать их разными значило бы завести второе место, где атрибуция разъедется.
    """
    row = (await db.execute(
        select(WfDraft, WfTarget, Channel)
        .join(WfTarget, WfDraft.target_id == WfTarget.id)
        .join(Channel, WfTarget.channel_id == Channel.id)
        .where(WfDraft.id == draft_id, WfDraft.workflow_id == wf.id))).first()
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND,
                            f"черновик {draft_id} в сценарии {wf.key!r} не найден")
    d, t = row[0], row[1]
    readers = (await _readers_by_message(
        db, await _instance_key(db, wf), [t.message_id])).get(t.message_id, [])
    source = await _source_by_message(db, {row[2].id: row[2]}, [t])
    one = _one(wf, d, t, row[2], readers, source.get(t.message_id))
    return {"remaining": await _pending(db, wf), "state": d.state,
            "workflow": wf.key,
            "readers": one["readers"], "tg_link": one["tg_link"],
            "draft": one}


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


# ── активность сценария ───────────────────────────────────────────────────────

# Ручка отвечает на вопрос «что ушло», и половина её чисел сегодня — гарантированные
# нули. Сказано это здесь, а не оставлено читателю как упражнение: **автоматической
# отправки в контуре нет вовсе**. `wf_outbound` пуст, и писателя у него не существует —
# `OutboundGate` зовётся из `drafts.py` и отсюда с `journal=None` и ничего никуда не
# шлёт. Прежняя `outbound_attempts` пуста по той же причине.
#
# Поэтому `automatic` — константа, а не «есть ли строки в журнале». Пустой журнал и
# отсутствующий отправитель — разные утверждения: первое означает «пока ничего не
# отправляли», второе «отправлять некому», и на экране они читаются по-разному.
# Вычисляй мы флаг по данным, первая же строка в журнале объявила бы контур рабочим.
# Появится отправитель — правится одно место, здесь.
AUTOMATIC_SENDING = False
SENDING_NOTE = ("автоматической отправки в этом контуре ещё нет: журнал исходящих "
                "пуст, отправитель не заведён")

# Лента — обозримый хвост, а не выгрузка: сводки выше отвечают на «сколько», лента
# нужна только чтобы глазом узнать последнее.
RECENT_LIMIT = 30
RECENT_TEXT_CHARS = 200

# Заголовок строки без канала. Ровно тот же прочерк, что и в соседних ручках: строка
# «канал неизвестен» обязана выглядеть как данные, иначе экран нарисует пустую ячейку
# и человек решит, что таблица недогрузилась.
NO_CHANNEL_TITLE = "—"


# Пояс подставляется литералом в текст запроса, а не параметром, и это не
# микрооптимизация. То же выражение стоит и в `SELECT`, и в `GROUP BY`, а Postgres
# сличает их по разбору: параметр там и параметр тут — разные узлы, даже когда значение
# одно, и группировка отвергается с «column must appear in the GROUP BY clause».
_UTC = literal_column("'UTC'")


def _utc_day(expr):
    """Календарный день выражения со временем — в UTC, а не в поясе соединения.

    `date(timestamptz)` в Postgres приводит к дате по `TimeZone` сессии, а его задаёт
    не этот код: он приходит из настроек сервера. Ряд по дням, посчитанный в чужом
    поясе, сдвинулся бы ровно на сутки — и заметно это стало бы только на машине,
    настроенной иначе, чем та, на которой это писали.
    """
    return cast(func.timezone(_UTC, expr), Date)


def _window_dates(until: datetime, days: int) -> list[date]:
    """Даты ряда `daily`: ровно `days` штук подряд, последняя — сегодняшняя в UTC.

    Отдельной функцией, потому что это единственная часть ручки, которую можно
    проверить без базы, — а сломать её проще всего: ряд на день короче или на день
    сдвинутый выглядит на графике как нормальный график.
    """
    return [until.date() - timedelta(days=n) for n in range(days - 1, -1, -1)]


def _window_start(until: datetime, days: int) -> datetime:
    """Начало окна — полночь первой даты ряда, а не `until - days`.

    Окно нарезано по суткам намеренно, и это единственное место, где решение принято
    против очевидного. Скользящее окно (`now - days`) начиналось бы в середине суток,
    которых в ряду уже нет, и сумма по ряду оказывалась бы меньше `totals` — на одном
    экране два честных числа, не сходящихся друг с другом. Человек, сложивший столбики
    и не получивший плитку, справедливо перестаёт верить обоим.

    Плата названа: `days=1` означает «сегодня», а не «за последние 24 часа», и рано
    утром это короткий отрезок. Подпись на экране говорит «сегодня» именно поэтому.
    """
    return datetime.combine(_window_dates(until, days)[0], time.min,
                            tzinfo=timezone.utc)


def _outbound_status(row: WfOutbound) -> str:
    """Чем кончилась попытка — словами, а не набором флагов.

    Исходов три, а не два: «гейт не пустил», «гейт пустил, но доставки нет» и
    «доставлено». Средний легко принять за сбой записи, но он законный — так выглядит
    сухой прогон, и слить его с отказом гейта значило бы объявить заблокированным то,
    что никто не блокировал.
    """
    if row.delivered_message_id is not None:
        return "доставлено"
    return "заблокировано гейтом" if not row.allowed else "отправка не подтверждена"


@router.get("/activity")
async def activity(db: GetDB, wf: Workflow = GetWorkflow,
                   user=requires(Section.ACTIVITY),
                   days: int = Query(7, ge=1, le=90)):
    """Что по этому сценарию ушло к людям: руками и автоматически.

    **Отправленное, а не воронка.** Спецификация (§9.2) запрещает здесь доли и
    конверсию, и запрет этот содержательный: «одобрено → отправлено» на сегодняшних
    данных дало бы аккуратный процент, посчитанный из того, что автоотправки нет.
    Такое число хуже отсутствующего — на него смотрят как на качество работы.

    **Половина чисел — гарантированные нули, и это не поломка.** Автоматической
    отправки в контуре не существует: `wf_outbound` пуст, писателя у него нет (см.
    `AUTOMATIC_SENDING` выше), поэтому `delivered` и `blocked` сегодня всегда нули.
    Ручка сообщает об этом состоянием — `sending.automatic: false` и текст рядом, — а
    не молчаливым нулём, который экран нарисует как «за неделю не отправили ничего».
    Единственный настоящий источник отправленного — `manual_sends`, то есть форма, в
    которую человек вносит то, что послал из Telegram сам.

    **`awaiting` — не за период.** Это одобренные черновики без доставленной попытки,
    то есть состояние «сейчас». Класть его в окно бессмысленно: одобренное неделю
    назад и не отправленное — ровно та проблема, которую число обязано показать.
    Подпись на экране про «сейчас, не за период» — часть контракта, а не украшение.

    **`daily` отдаёт ровно `days` строк, включая нулевые,** и ряд покрывает то же
    время, что и сводки: окно начинается полночью первой его даты (`_window_start`).
    Дырка в ряду читается экраном как сдвинутый график, а не как пустой день, а
    несовпадение ряда с окном давало бы `sum(daily.manual) < totals.manual` — два
    честных числа на одном экране, не сходящихся между собой.

    Часовых поясов пользователя тут нет намеренно: соседние экраны их тоже не знают, и
    один экран, живущий в местном времени, разошёлся бы датами со всеми остальными.
    """
    until = clock.utcnow()
    since = _window_start(until, days)

    # «Когда отправил», а не «когда записал» — но `sent_at` необязателен: человек может
    # его не заполнить, и запись без времени всё равно ценна. Голое сравнение по
    # `sent_at` выкинуло бы такие строки из окна целиком (NULL не сравнивается ни с
    # чем), то есть тихо занизило бы отправленное. Подпираем моментом записи.
    sent_ts = func.coalesce(ManualSend.sent_at, ManualSend.recorded_at)
    manual_where = (ManualSend.workflow_id == wf.id,
                    sent_ts >= since, sent_ts <= until)
    outbound_window = (WfOutbound.workflow_id == wf.id,
                       WfOutbound.created_at >= since, WfOutbound.created_at <= until)
    delivered_where = (*outbound_window, WfOutbound.delivered_message_id.isnot(None))

    manual_total = (await db.execute(
        select(func.count(ManualSend.id)).where(*manual_where))).scalar_one()
    delivered_total = (await db.execute(
        select(func.count(WfOutbound.id)).where(*delivered_where))).scalar_one()
    blocked_total = (await db.execute(
        select(func.count(WfOutbound.id))
        .where(*outbound_window, WfOutbound.allowed.is_(False)))).scalar_one()

    # Подзапрос отсекает `draft_id IS NULL` не ради скорости: `NOT IN` со списком, в
    # котором есть NULL, не отбирает вообще ничего — счётчик молча стал бы нулём, и
    # выглядело бы это как «всё отправлено».
    delivered_drafts = (select(WfOutbound.draft_id)
                        .where(WfOutbound.workflow_id == wf.id,
                               WfOutbound.draft_id.isnot(None),
                               WfOutbound.delivered_message_id.isnot(None)))
    awaiting = (await db.execute(
        select(func.count(WfDraft.id))
        .where(WfDraft.workflow_id == wf.id, WfDraft.state == "approved",
               WfDraft.id.notin_(delivered_drafts)))).scalar_one()

    manual_by_day = dict((await db.execute(
        select(_utc_day(sent_ts), func.count(ManualSend.id))
        .where(*manual_where).group_by(_utc_day(sent_ts)))).all())
    delivered_by_day = dict((await db.execute(
        select(_utc_day(WfOutbound.created_at), func.count(WfOutbound.id))
        .where(*delivered_where)
        .group_by(_utc_day(WfOutbound.created_at)))).all())
    daily = [{"date": d.isoformat(),
              "manual": manual_by_day.get(d, 0),
              "delivered": delivered_by_day.get(d, 0)}
             for d in _window_dates(until, days)]

    # Канал у ручной записи — через снимок сообщения, у попытки — через цель. Пути
    # разные, потому что запись бывает и без наводки вовсе: человек написал тому, кого
    # Radar не находил. Соединения внешние по той же причине — такая строка обязана
    # дойти до экрана без канала, а не пропасть из сводки.
    by_channel: dict[int | None, dict] = {}

    def _channel_row(cid, title, username) -> dict:
        row = by_channel.get(cid)
        if row is None:
            row = by_channel[cid] = {"channel_id": cid,
                                     "title": title or NO_CHANNEL_TITLE,
                                     "username": username,
                                     "manual": 0, "delivered": 0}
        return row

    for cid, title, username, n in (await db.execute(
            select(Channel.id, Channel.title, Channel.username,
                   func.count(ManualSend.id))
            .select_from(ManualSend)
            .outerjoin(Message, ManualSend.message_id == Message.id)
            .outerjoin(Channel, Message.channel_id == Channel.id)
            .where(*manual_where)
            .group_by(Channel.id, Channel.title, Channel.username))).all():
        _channel_row(cid, title, username)["manual"] = n
    for cid, title, username, n in (await db.execute(
            select(Channel.id, Channel.title, Channel.username,
                   func.count(WfOutbound.id))
            .select_from(WfOutbound)
            .outerjoin(WfTarget, WfOutbound.target_id == WfTarget.id)
            .outerjoin(Channel, WfTarget.channel_id == Channel.id)
            .where(*delivered_where)
            .group_by(Channel.id, Channel.title, Channel.username))).all():
        _channel_row(cid, title, username)["delivered"] = n

    # Строка без канала — последней всегда, сколько бы записей в ней ни было: это не
    # самый тихий канал, это «неизвестно куда», и место ему в конце списка, а не в его
    # середине по величине.
    channels = sorted(by_channel.values(),
                      key=lambda r: (r["channel_id"] is None,
                                     -(r["manual"] + r["delivered"]), r["title"]))

    by_account: dict[int | None, dict] = {}

    def _account_row(aid, last_at) -> dict:
        row = by_account.get(aid)
        if row is None:
            row = by_account[aid] = {"engage_account_id": aid, "manual": 0,
                                     "delivered": 0, "last_at": None}
        if last_at is not None and (row["last_at"] is None or last_at > row["last_at"]):
            row["last_at"] = last_at
        return row

    for aid, n, last_at in (await db.execute(
            select(ManualSend.engage_account_id, func.count(ManualSend.id),
                   func.max(sent_ts))
            .where(*manual_where).group_by(ManualSend.engage_account_id))).all():
        _account_row(aid, last_at)["manual"] = n
    for aid, n, last_at in (await db.execute(
            select(WfOutbound.engage_account_id, func.count(WfOutbound.id),
                   func.max(WfOutbound.created_at))
            .where(*delivered_where)
            .group_by(WfOutbound.engage_account_id))).all():
        _account_row(aid, last_at)["delivered"] = n

    accounts = [{**r, "last_at": r["last_at"].isoformat() if r["last_at"] else None}
                for r in sorted(by_account.values(),
                                key=lambda r: (r["engage_account_id"] is None,
                                               -(r["manual"] + r["delivered"]),
                                               r["engage_account_id"] or 0))]

    # Слияние двух журналов идёт по объекту времени, а не по строке ISO: строки
    # сравнимы, только пока смещение у всех одинаковое, и первая же запись, пришедшая
    # не в UTC, перемешала бы ленту незаметно.
    merged: list[tuple] = []
    for entry, channel in (await db.execute(
            select(ManualSend, Channel)
            .outerjoin(Message, ManualSend.message_id == Message.id)
            .outerjoin(Channel, Message.channel_id == Channel.id)
            .where(*manual_where)
            .order_by(sent_ts.desc(), ManualSend.id.desc())
            .limit(RECENT_LIMIT))).all():
        at = entry.sent_at or entry.recorded_at
        merged.append((at, entry.id, {
            "kind": "manual", "id": entry.id, "at": at.isoformat(),
            "channel": channel.title if channel is not None else NO_CHANNEL_TITLE,
            "engage_account_id": entry.engage_account_id,
            "target_id": entry.target_id,
            "text": (entry.text or "")[:RECENT_TEXT_CHARS],
            "status": "записано руками",
            "matches_suggestion": manual_sends_service.matches_suggestion(entry),
        }))

    # Попытки берём все, а не только доставленные: заблокированная гейтом — это тоже
    # то, что происходило, и `reasons` рядом с ней и есть ответ на «почему не ушло».
    for row, channel in (await db.execute(
            select(WfOutbound, Channel)
            .outerjoin(WfTarget, WfOutbound.target_id == WfTarget.id)
            .outerjoin(Channel, WfTarget.channel_id == Channel.id)
            .where(*outbound_window)
            .order_by(WfOutbound.created_at.desc(), WfOutbound.id.desc())
            .limit(RECENT_LIMIT))).all():
        merged.append((row.created_at, row.id, {
            "kind": "outbound", "id": row.id, "at": row.created_at.isoformat(),
            "channel": channel.title if channel is not None else NO_CHANNEL_TITLE,
            "engage_account_id": row.engage_account_id,
            "target_id": row.target_id,
            "text": (row.text_snapshot or "")[:RECENT_TEXT_CHARS],
            "status": _outbound_status(row),
            "allowed": row.allowed,
            "reasons": row.reasons or [],
        }))
    merged.sort(key=lambda x: (x[0], x[1]), reverse=True)

    return {
        "workflow": {"key": wf.key, "title": wf.title, "action": wf.action,
                     "visibility": wf.visibility},
        "window": {"days": days, "since": since.isoformat(),
                   "until": until.isoformat()},
        # `sent` складывает сервер: та же сумма, посчитанная на экране, разъехалась бы
        # с этой в тот день, когда сюда добавится третий источник отправленного.
        "totals": {"sent": manual_total + delivered_total, "manual": manual_total,
                   "delivered": delivered_total, "blocked": blocked_total,
                   "awaiting": awaiting},
        "sending": {"automatic": AUTOMATIC_SENDING, "note": SENDING_NOTE},
        "daily": daily,
        "channels": channels,
        "accounts": accounts,
        "recent": [row for _, _, row in merged[:RECENT_LIMIT]],
    }
