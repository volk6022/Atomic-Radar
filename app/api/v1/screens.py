"""Ручки данных для экранов — пока на фиксированных примерах.

Форма ответов взята из `radar-api-contract.md`, с двумя отличиями от моков в вёрстке,
и оба намеренные:

* **числа отдаются числами.** В экранах всё числовое записано строками (`score:'87'`,
  `perDay:'145'`) — это артефакт вёрстки. Сортировка и фильтры на строках работают
  неверно, а шаблон одинаково подставит и то и другое.
* **цвета не отдаются вовсе.** Поля `c:'#156479'` в моках — цвет бейджа. Палитре не
  место в API и тем более в базе: иначе смена темы становится миграцией. Отдаём
  `status`, сопоставление «статус → цвет» остаётся во фронтенде.

Каждая ручка объявляет свой раздел через `requires(...)`: матрица прав в GUI только
прячет пункты меню, а решение принимается здесь.
"""
from __future__ import annotations

# `status` из fastapi здесь не импортируется намеренно: у ручки лидов есть параметр
# с таким же именем, и одноимённый модуль рядом читался бы как ошибка.
import asyncio

from fastapi import APIRouter, Depends, HTTPException

from datetime import datetime, timedelta, timezone

from sqlalchemy import and_, func, or_, select

from app.api.deps import GetDB, requires
from app.api.v1.listing import ListParams, apply_search, apply_sort, list_params
from app.core import cascade
from app.core import invariants as inv
from app.core import prototypes
from app.core.access import Section
from app.core.config import get_settings
from app.api.v1.system import get_state
from app.db.models import (Attribution, AuditLog, Channel, Conversation, Draft,
                           Lead, LlmTrace, Message, OutboundAttempt,
                           ProfileVersion, User)
from app.services import drafting, embeddings, engage, llm, queue

router = APIRouter(prefix="/api/v1", tags=["screens"])


# ── шапка и общее ─────────────────────────────────────────────────────────────

async def _counts(db) -> dict:
    """Одним местом на весь дашборд: числа обязаны сходиться между плитками,
    воронкой и бейджами меню. Считать их в трёх местах — гарантированно разойтись."""
    day_ago = datetime.now(timezone.utc) - timedelta(days=1)

    total_msgs = (await db.execute(select(func.count(Message.id)))).scalar_one()
    msgs_24h = (await db.execute(
        select(func.count(Message.id)).where(Message.tg_date >= day_ago))).scalar_one()

    # Воронка: сколько сообщений пережило каждую ступень. «Пережило ступень k» — это
    # `level > k` (дошло дальше) плюс те, кто на ней и остановился, но прошёл её
    # (`level == k and passed`). Считать по одному `cascade_passed` нельзя: он говорит
    # только про последнюю ступень, и сообщение, отсеянное на L2, тогда выглядело бы
    # так, будто оно не прошло и L0.
    async def survived(stage: int) -> int:
        return (await db.execute(select(func.count(Message.id)).where(
            or_(Message.cascade_level > stage,
                and_(Message.cascade_level == stage,
                     Message.cascade_passed.isnot(False)))))).scalar_one()

    l0_passed, l1_passed = await survived(0), await survived(1)
    l2_passed, l3_passed = await survived(2), await survived(3)
    leads = (await db.execute(select(func.count(Lead.id)))).scalar_one()
    leads_new = (await db.execute(
        select(func.count(Lead.id)).where(Lead.status == "new"))).scalar_one()
    channels = (await db.execute(select(func.count(Channel.id)))).scalar_one()
    drafts_pending = (await db.execute(
        select(func.count(Draft.id)).where(Draft.state == "pending"))).scalar_one()

    return {"total_msgs": total_msgs, "msgs_24h": msgs_24h,
            "l0": l0_passed, "l1": l1_passed, "l2": l2_passed, "l3": l3_passed,
            "leads": leads, "leads_new": leads_new,
            "channels": channels, "drafts": drafts_pending}


# Тревоги переехали в `app/api/v1/alerts.py`: у них появилась отметка «прочитано»,
# то есть побочный эффект, и настоящая таблица вместо трёх условий в коде.


@router.get("/counters")
async def counters(db: GetDB, user=requires(Section.DASHBOARD)):
    """Бейджи в меню. Берутся из тех же счётчиков, что и дашборд."""
    c = await _counts(db)
    conversations = (await db.execute(
        select(func.count(Conversation.id))
        .where(Conversation.state == "awaiting_reply"))).scalar_one()
    return {"drafts": c["drafts"], "conversations": conversations}


# ── Dashboard ─────────────────────────────────────────────────────────────────

@router.get("/dashboard")
async def dashboard(db: GetDB, user=requires(Section.DASHBOARD)):
    c = await _counts(db)
    state = await get_state(db)

    # Отправленным считается только то, у чего есть id доставленного сообщения:
    # `allowed` значит «гейт пропустил», а не «Telegram принял».
    sent = (await db.execute(
        select(func.count(OutboundAttempt.id))
        .where(OutboundAttempt.delivered_message_id.isnot(None)))).scalar_one()
    blocked = (await db.execute(select(func.count(OutboundAttempt.id))
                                .where(OutboundAttempt.allowed.is_(False)))).scalar_one()
    conversations = (await db.execute(select(func.count(Conversation.id)))).scalar_one()

    # Здоровье внешних сервисов спрашиваем у них самих, а не рисуем зелёный кружок по
    # вере. Три опроса параллельно: модели живут на машине Ивана за туннелем, и
    # последовательно это было бы полторы секунды на ровном месте.
    async def _engage_status() -> str:
        try:
            await engage.fleet_health()
            return "ok"
        except engage.EngageUnavailable:
            return "down"

    engage_status, embed_status, llm_status, queue_status = await asyncio.gather(
        _engage_status(), embeddings.ping(), llm.ping(), queue.ping())

    return {
        "tiles": [
            {"key": "messages", "label": "Сообщений всего", "value": c["total_msgs"],
             "go": "stream"},
            {"key": "messages_24h", "label": "Сообщений за 24ч", "value": c["msgs_24h"],
             "go": "stream"},
            {"key": "leads", "label": "Лидов найдено", "value": c["leads"], "go": "leads"},
            {"key": "drafts", "label": "Черновиков в очереди", "value": c["drafts"],
             "go": "drafts"},
            {"key": "conversations", "label": "Диалогов", "value": conversations,
             "go": "conversations"},
            {"key": "sent", "label": "Отправлено", "value": sent, "go": "conversations"},
            {"key": "blocked", "label": "Заблокировано гейтом", "value": blocked,
             "go": "safety"},
        ],
        "queues": [
            {"key": "drafts", "label": "Черновики", "count": c["drafts"], "go": "drafts"},
            {"key": "leads", "label": "Лиды без обработки", "count": c["leads_new"],
             "go": "leads"},
            {"key": "channels", "label": "Каналов в реестре", "count": c["channels"],
             "go": "channels"},
            {"key": "conversations", "label": "Диалогов", "count": conversations,
             "go": "conversations"},
        ],
        "errors": [],
        # Воронка — это те же сообщения, просто разрезанные по ступеням каскада.
        # Выключенная ступень идёт прочерком, а не нулём: ноль читался бы как
        # «всё отсеяно», хотя на самом деле её просто не запускали.
        "funnel": [
            {"step": "Сообщений", "count": c["total_msgs"], "go": "stream"},
            {"step": "L0 структура", "count": c["l0"], "go": "stream"},
            {"step": "L1 слова", "count": c["l1"], "go": "stream"},
            {"step": "L2 эмбеддинги",
             "count": c["l2"] if embeddings.enabled() else None, "go": "stream"},
            {"step": "L3 LLM",
             "count": c["l3"] if llm.enabled() else None, "go": "stream"},
            {"step": "Лиды", "count": c["leads"], "go": "leads"},
        ],
        "mode": "DRY_RUN" if state.killed else state.mode,
        "killed": state.killed,
        "services": [
            {"name": "engage", "status": engage_status},
            {"name": "postgres", "status": "ok"},
            {"name": "embeddings", "status": embed_status},
            {"name": "llm", "status": llm_status},
            {"name": "queue", "status": queue_status},
        ],
    }


# ── флот и источники ──────────────────────────────────────────────────────────

def _mask_phone(phone: str | None) -> str:
    """`+12159021784` → `+1215•••1784`.

    Аккаунт нужно опознавать, а полный номер для этого не требуется. Экран смотрят
    и с чужих экранов тоже; отдавать наружу то, что не нужно для работы, — лишний риск.
    """
    if not phone:
        return "—"
    digits = phone.lstrip("+")
    if len(digits) <= 8:
        return phone
    return f"+{digits[:4]}•••{digits[-4:]}"


@router.get("/accounts")
async def accounts(user=requires(Section.FLEET)):
    """Живой флот из Engage — единственная ручка, за которой уже стоят настоящие данные.

    Поля, которых Engage не знает (часовой пояс собеседника, израсходованные за сутки
    лимиты, аптайм вотчера), не выдумываются: экран показывает по ним прочерк. Пустая
    клетка честнее правдоподобного числа — по такому экрану принимают решение о паузе
    аккаунта.
    """
    try:
        raw = await engage.list_accounts()
        safety = await engage.safety_config()
    except engage.EngageUnavailable as e:
        # 503, а не пустой список: «аккаунтов нет» и «мы не смогли спросить» — разные
        # новости, и путать их на экране здоровья флота нельзя.
        raise HTTPException(503, str(e)) from e

    totals = safety.get("warmup_totals", {})
    rows = []
    for a in raw:
        proxy = a.get("proxy") or {}
        phone_country = a.get("phone_country")
        proxy_country = proxy.get("country")
        rows.append({
            "id": a.get("account_id"),
            "label": f"acc-{a.get('account_id')}",
            "phone_masked": _mask_phone(a.get("phone")),
            "status": a.get("status"),
            "phone_country": phone_country,
            "proxy_country": proxy_country,
            "proxy_type": proxy.get("type"),
            "proxy_healthy": proxy.get("is_healthy"),
            # Тот самый рассинхрон, из-за которого гейт Engage усыплял аккаунты и
            # пришлось заводить `geo_override`: страна номера против страны прокси.
            "geo_match": bool(phone_country and proxy_country
                              and phone_country == proxy_country),
            "use_case": a.get("use_case"),
            "warmup_tier": a.get("warmup_tier"),
            "warmup_day": a.get("warmup_day"),
            "warmup_total": totals.get(a.get("use_case")),
        })
    return rows


CHANNEL_SORTS = {"title": Channel.title, "members": Channel.members,
                 "leads_total": Channel.leads_total}


@router.get("/channels")
async def channels(db: GetDB, user=requires(Section.CHANNELS),
                   p: ListParams = Depends(list_params)):
    """Реестр групп. Заводятся сами при первом сообщении из группы — просить оператора
    зарегистрировать канал заранее значило бы терять то, про что он ещё не знает."""
    q = select(Channel)
    count_q = select(func.count(Channel.id))

    # Поиск по названию и username канала. У каждого канала одна строка в БД, поэтому
    # счётчик не нужен отдельно: JOIN есть только в запросе строк.
    search = [Channel.title, Channel.username]
    q = apply_search(q, p, search)
    count_q = apply_search(count_q, p, search)

    total = (await db.execute(count_q)).scalar_one()
    q = apply_sort(q, p, CHANNEL_SORTS, default="leads_total", tiebreak=Channel.id)
    rows = (await db.execute(q.limit(p.limit).offset(p.offset))).scalars().all()

    out = []
    for c in rows:
        msgs = (await db.execute(
            select(func.count(Message.id)).where(Message.channel_id == c.id)
        )).scalar_one()
        passed = (await db.execute(
            select(func.count(Message.id))
            .where(Message.channel_id == c.id, Message.cascade_passed.is_(True))
        )).scalar_one()
        out.append({
            "id": c.id, "peer_id": c.peer_id, "title": c.title, "username": c.username,
            "topic": c.topic, "members": c.members,
            "messages_total": msgs,
            # Доля прошедших каскад — главный показатель полезности группы: по нему
            # решают, читать её дальше или выключить.
            "prefilter_rate": round(passed / msgs, 4) if msgs else None,
            "leads_total": c.leads_total,
            "leads_per_1000": round(c.leads_total * 1000 / msgs, 2) if msgs else None,
            "ingest_enabled": c.ingest_enabled, "is_junk": c.is_junk,
            "linked_chat_username": c.linked_chat_username,
        })
    return {**p.page(total), "rows": out}


# Колонки, по которым разрешено сортировать поток. Белый список, а не «любое поле
# модели»: имя приходит из браузера и попадает в SQL.
MESSAGE_SORTS = {"date": Message.tg_date, "channel": Channel.title,
                 "author": Message.author_name, "level": Message.cascade_level}


@router.get("/messages")
async def messages(db: GetDB, user=requires(Section.STREAM),
                   p: ListParams = Depends(list_params),
                   channel_id: int | None = None, channel: str | None = None,
                   passed: str | None = None):
    """Поток сообщений с результатом каскада L0→L3.

    У каждой ступени рядом с вердиктом лежит причина. Экран потока существует ради
    вопроса «почему это не стало лидом», и голое `false` на него не отвечает.

    Пагинация, сортировка и фильтры считаются в SQL, а не в питоне: одна активная
    группа даёт около 9000 сообщений в сутки, и выбирать их целиком, чтобы показать
    пятьдесят, перестанет работать в первый же день реального ингеста.

    Канал выбирается по `channel_id`. Параметр `channel` (по заголовку) оставлен
    ради старых ссылок, но помечен устаревшим: заголовок в Telegram меняют, и
    фильтр по нему тихо перестаёт находить группу, которую до этого находил.
    """
    q = select(Message, Channel).join(Channel, Message.channel_id == Channel.id)
    count_q = select(func.count(Message.id)).join(Channel, Message.channel_id == Channel.id)

    if channel_id is not None:
        q = q.where(Message.channel_id == channel_id)
        count_q = count_q.where(Message.channel_id == channel_id)
    elif channel:
        q = q.where(Channel.title == channel)
        count_q = count_q.where(Channel.title == channel)
    # Три значения фильтра, потому что у сообщения три состояния. Сваливать «ещё в
    # пути» в «не прошло», как было до появления L2/L3, значило бы прятать очередь
    # необработанного: на экране всё выглядит разобранным, а половина ждёт модель.
    where = {"true": Message.cascade_passed.is_(True),
             "false": Message.cascade_passed.is_(False),
             "pending": Message.cascade_passed.is_(None)}.get(passed or "")
    if where is not None:
        q = q.where(where)
        count_q = count_q.where(where)

    search = [Message.text, Message.author_name, Message.author_username]
    q = apply_search(q, p, search)
    count_q = apply_search(count_q, p, search)

    total = (await db.execute(count_q)).scalar_one()
    q = apply_sort(q, p, MESSAGE_SORTS, default="date", tiebreak=Message.id)
    rows = (await db.execute(q.limit(p.limit).offset(p.offset))).all()

    lead_by_message = {}
    if rows:
        ids = [m.id for m, _ in rows]
        for lead_id, msg_id in (await db.execute(
                select(Lead.id, Lead.message_id).where(Lead.message_id.in_(ids)))).all():
            lead_by_message[msg_id] = lead_id

    out = []
    for m, c in rows:
        detail = m.cascade_detail or {}
        level, ok = m.cascade_level, m.cascade_passed
        out.append({
            "id": m.tg_message_id, "channel": c.title,
            "author_name": m.author_name or "—",
            "author_username": ("@" + m.author_username) if m.author_username else None,
            "text": m.text or "",
            "tg_date": m.tg_date.isoformat(),
            "is_automatic_forward": m.is_automatic_forward,
            # Три состояния, а не два: `null` значит «до ступени не дошло», и это
            # не то же самое, что «не прошло».
            "cascade": cascade.stage_flags(level, ok),
            "cascade_notes": detail,
            "lead_id": lead_by_message.get(m.id),
        })
    return {**p.page(total), "rows": out}


@router.get("/channels/options")
async def channel_options(db: GetDB, user=requires(Section.STREAM)):
    """Справочник каналов для выпадающих списков на табличных экранах.

    Отдельно от `/channels`: тот считает по каждому каналу количество сообщений и
    долю прошедших, и дёргать его ради заполнения выпадающего списка — лишние
    запросы на каждое открытие экрана.
    """
    rows = (await db.execute(
        select(Channel.id, Channel.title, Channel.username)
        .order_by(Channel.title))).all()
    return [{"id": cid, "title": title, "username": username}
            for cid, title, username in rows]


# ── конвейер лидов ────────────────────────────────────────────────────────────

# Лиды переехали в `app/api/v1/leads.py`: у них появились правка статуса и массовые
# решения, то есть побочные эффекты. Этот модуль остаётся чтением — свойство, которое
# удобно проверять взглядом на список ручек, а не тестом.


# Очередь черновиков живёт в `app/api/v1/drafts.py`: там появилось состояние
# (решения оператора) и вызов OutboundGate, а этот модуль намеренно остаётся
# набором чистых заглушек без побочных эффектов.


CONVERSATION_STATES = ("new", "awaiting_reply", "replied", "handed_off", "closed")

CONVERSATION_SORTS = {"created": Conversation.created_at, "sent": Conversation.sent_count,
                      "last": Conversation.last_sent_at, "state": Conversation.state}


@router.get("/conversations")
async def conversations(db: GetDB, user=requires(Section.CONVERSATIONS),
                        p: ListParams = Depends(list_params),
                        state: str | None = None):
    """Диалоги. Пока система в сухом прогоне, их не будет ни одного — и это
    не поломка экрана, а главное свойство режима.

    Фильтр по состоянию считается здесь: на клиенте он работал бы только по уже
    загруженной странице, а диалоги — единственная сущность, которая растёт
    без ограничений сверху.
    """
    if state and state not in CONVERSATION_STATES:
        raise HTTPException(422, f"неизвестное состояние «{state}», ожидается одно из "
                                 f"{', '.join(CONVERSATION_STATES)}")

    q = select(Conversation)
    count_q = select(func.count(Conversation.id))
    if state:
        q = q.where(Conversation.state == state)
        count_q = count_q.where(Conversation.state == state)

    total = (await db.execute(count_q)).scalar_one()
    q = apply_sort(q, p, CONVERSATION_SORTS, default="created", tiebreak=Conversation.id)
    rows = (await db.execute(q.limit(p.limit).offset(p.offset))).scalars().all()
    out = []
    for c in rows:
        lead = (await db.execute(
            select(Lead).where(Lead.id == c.lead_id))).scalar_one_or_none()
        out.append({
            "id": c.id, "lead_id": c.lead_id,
            "peer_name": lead.author_name if lead else None,
            "peer_username": ("@" + lead.author_username)
                             if lead and lead.author_username else None,
            "account": c.account_id, "state": c.state, "sent_count": c.sent_count,
            "last_sent_at": c.last_sent_at.isoformat() if c.last_sent_at else None,
        })
    by_state = dict((await db.execute(
        select(Conversation.state, func.count(Conversation.id))
        .group_by(Conversation.state))).all())

    return {**p.page(total), "rows": out, "state": state,
            "states": [{"key": k, "count": by_state.get(k, 0)}
                       for k in CONVERSATION_STATES],
            "note": None if out else
                    ("Диалогов в этом состоянии нет" if state else
                     "Диалогов нет: в сухом прогоне ни одно сообщение не отправляется")}


# ── настройка ─────────────────────────────────────────────────────────────────

@router.get("/profile")
async def profile(db: GetDB, user=requires(Section.PROFILE)):
    """Профиль заказчика и таксономия болей.

    Боли отдаются не из отдельной таблицы, а из самого каскада: это ровно тот список,
    по которому сейчас принимаются решения. Держать рядом «профиль для показа» и
    «правила для работы» — верный способ разойтись между экраном и поведением.

    Профиль каскада назван явно. Пока workflow один, и это `dm_v1`; когда их станет
    несколько, экран получит выбор, а не молча продолжит показывать правила первого.
    """
    rules = cascade.profile(cascade.DEFAULT_PROFILE)
    version = (await db.execute(
        select(ProfileVersion).where(ProfileVersion.is_active.is_(True))
        .order_by(ProfileVersion.id.desc()).limit(1))).scalar_one_or_none()

    # Рядом с каждой болью — не только слова для L1, но и эталонные фразы для L2:
    # это две разные механики отбора одной и той же боли, и человеку, который решает,
    # «почему система на это среагировала», нужны обе.
    pains = [{"key": pain, "label": pain, "anchors": list(anchors)[:6],
              "anchors_total": len(anchors),
              "prototypes": list(prototypes.POSITIVE.get(pain, ()))}
             for pain, anchors in rules.pain_anchors.items()]

    return {
        "version": version.version if version else "не сохранён",
        "is_active": bool(version),
        "business_description": (version.business_description if version else
                                 "Настройка и сопровождение VPN/VPS-инфраструктуры "
                                 "под ключ, перенос без простоя."),
        "pains": pains,
        "disqualifiers": [{"key": k, "markers": list(v)[:6]}
                          for k, v in rules.disqualifier_markers.items()],
        # Отрицательные эталоны L2 — половина работы ступени, и без них список
        # «на кого охотимся» выглядел бы так, будто система только соглашается.
        "noise_prototypes": [{"key": k, "examples": list(v)[:4]}
                             for k, v in prototypes.NEGATIVE.items()],
        "cascade": {
            "profile": rules.key,
            "profile_title": rules.title,
            "l2_enabled": embeddings.enabled(),
            "l2_model": get_settings().EMBED_MODEL if embeddings.enabled() else None,
            "l2_min_margin": rules.l2_min_margin,
            "l3_enabled": llm.enabled(),
            "l3_model": get_settings().LLM_MODEL if llm.enabled() else None,
            # Промпт берётся у профиля, а не из глобальной константы: с 25.08 у
            # каждого контура свой вопрос к модели, и показывать здесь «вопрос вообще»
            # значило бы показывать чужой ровно в тот момент, когда контуров станет два.
            "l3_prompt_key": rules.l3_prompt_key,
            "l3_prompt_version": llm.prompt(rules.l3_prompt_key).version,
            "l3_prompt": llm.prompt(rules.l3_prompt_key).system,
        },
        "generation": {
            "prompt_version": drafting.PROMPT_VERSION,
            "note": "Черновики собираются по шаблонам: текст ответа модель пока "
                    "не пишет — L3 только выносит вердикт по сообщению",
        },
    }


# Задачи переехали в `app/api/v1/runs.py`: у них появились запуск и отмена, то есть
# побочные эффекты. Этот модуль остаётся чтением.


@router.get("/evaluations")
async def evaluations(db: GetDB, user=requires(Section.EVALS)):
    """Качество генерации. Меряется по решениям оператора: одобрено против отклонено
    с разбивкой по причинам — это и есть eval-датасет, ради которого справочник
    причин сделан закрытым."""
    approved = (await db.execute(select(func.count(Draft.id))
                                 .where(Draft.state == "approved"))).scalar_one()
    rejected = (await db.execute(select(func.count(Draft.id))
                                 .where(Draft.state == "rejected"))).scalar_one()
    pending = (await db.execute(select(func.count(Draft.id))
                                .where(Draft.state == "pending"))).scalar_one()
    decided = approved + rejected

    by_reason = (await db.execute(
        select(Draft.reject_reason, func.count(Draft.id))
        .where(Draft.state == "rejected").group_by(Draft.reject_reason))).all()

    return {
        "prompt_version": drafting.PROMPT_VERSION,
        "approved": approved, "rejected": rejected, "pending": pending,
        "approval_rate": round(approved / decided, 3) if decided else None,
        "reject_reasons": [{"reason": r or "—", "count": n} for r, n in by_reason],
        "note": (None if decided else
                 "Оценивать пока нечего: ни один черновик не разобран человеком"),
    }


# ── бизнес и эксплуатация ─────────────────────────────────────────────────────

@router.get("/attribution")
async def attribution(db: GetDB, user=requires(Section.ATTRIBUTION)):
    """Атрибуция. Считать выручку не с чего, пока не отправлено ни одного сообщения;
    юнит-экономика по каналам — из реальных сообщений и лидов."""
    rows = (await db.execute(select(Attribution).limit(200))).scalars().all()

    channels = (await db.execute(select(Channel))).scalars().all()
    unit = []
    for c in channels:
        msgs = (await db.execute(select(func.count(Message.id))
                                 .where(Message.channel_id == c.id))).scalar_one()
        if not msgs:
            continue
        unit.append({"channel": c.title, "messages": msgs, "leads": c.leads_total,
                     "answered": 0, "converted": 0, "revenue": 0.0})

    return {
        "leads": [{"ref_token": a.ref_token, "lead_id": a.lead_id,
                   "converted_at": a.converted_at.isoformat() if a.converted_at else None,
                   "amount": float(a.amount) if a.amount else None} for a in rows],
        "unit_economics": unit,
        "note": "Отправок ещё не было — конверсии считать не с чего",
    }


TRACE_SORTS = {"created": LlmTrace.created_at, "latency": LlmTrace.latency_ms,
               "tokens": LlmTrace.tokens_out, "stage": LlmTrace.stage}


@router.get("/traces")
async def traces(db: GetDB, user=requires(Section.OBSERVABILITY),
                 p: ListParams = Depends(list_params)):
    """LLM-трейсы: по одному на каждый вопрос к модели на ступени L3.

    Сводка считается по всей таблице, а не по показанной странице: средняя задержка
    по последним пятидесяти строкам — это не средняя задержка, а средняя по тому, что
    влезло на экран.
    """
    q = select(LlmTrace)
    count_q = select(func.count(LlmTrace.id))

    # Поиск по этапу и названию модели: помогает отладить, какая версия модели попала
    # в конкретный трейс.
    search = [LlmTrace.stage, LlmTrace.model]
    q = apply_search(q, p, search)
    count_q = apply_search(count_q, p, search)

    total = (await db.execute(count_q)).scalar_one()
    agg = (await db.execute(select(
        func.avg(LlmTrace.latency_ms), func.max(LlmTrace.latency_ms),
        func.sum(LlmTrace.tokens_in), func.sum(LlmTrace.tokens_out)))).one()

    q = apply_sort(q, p, TRACE_SORTS, default="created", tiebreak=LlmTrace.id)
    rows = (await db.execute(q.limit(p.limit).offset(p.offset))).scalars().all()
    out = [{"id": t.id, "stage": t.stage, "model": t.model,
            "prompt_version": t.prompt_version,
            "tokens_in": t.tokens_in, "tokens_out": t.tokens_out,
            "latency_ms": t.latency_ms,
            "cost_usd": float(t.cost_usd) if t.cost_usd else 0.0,
            "response": (t.response or "")[:400],
            "created_at": t.created_at.isoformat() if t.created_at else None}
           for t in rows]

    return {
        **p.page(total), "rows": out,
        "summary": {
            "avg_latency_ms": int(agg[0]) if agg[0] is not None else None,
            "max_latency_ms": agg[1], "tokens_in": agg[2], "tokens_out": agg[3],
            # Модель своя, на своей карте. Ноль здесь — это факт, а не пропуск:
            # подставить прайс OpenAI значило бы испортить себестоимость лида.
            "cost_usd": 0.0,
            "model": get_settings().LLM_MODEL if llm.enabled() else None,
        },
        "note": None if out else (
            "Трейсов нет: L3 выключена (не задан RADAR_LLM_BASE_URL)" if not llm.enabled()
            else "Трейсов нет: модель включена, но её ещё ни разу не спрашивали — "
                 "запустите scripts/reclassify"),
    }


@router.get("/limits")
async def limits(db: GetDB, user=requires(Section.SAFETY)):
    """Пороги гардрейлов — прямо из `app.core.invariants`.

    Значения не дублируются: экран читает те самые константы, которые проверяет гейт.
    Скопировать их сюда числами значило бы однажды показать одно, а применить другое —
    и это ровно тот экран, где такое расхождение недопустимо.
    """
    q = inv.QUIET_HOURS
    limits_rows = [
        {"key": "max_messages_per_conversation", "label": "Сообщений в одном диалоге",
         "value": inv.MAX_MESSAGES_PER_CONVERSATION, "unit": "шт",
         "description": "больше — это преследование, а не диалог"},
        {"key": "min_gap_hours", "label": "Пауза между сообщениями",
         "value": int(inv.MIN_GAP_BETWEEN_MESSAGES.total_seconds() // 3600), "unit": "ч",
         "description": "два подряд выдают бота вернее любого текста"},
        {"key": "quiet_hours", "label": "Тихие часы (время собеседника)",
         "value": f"{q[0]:02d}:00–{q[1]:02d}:00", "unit": "",
         "description": "ночное сообщение читается как рассылка"},
        {"key": "max_links_first_message", "label": "Ссылок в первом сообщении",
         "value": inv.MAX_LINKS_IN_FIRST_MESSAGE, "unit": "шт",
         "description": "прямой путь в спам, причина отклонения №8"},
    ]

    attempts = (await db.execute(
        select(OutboundAttempt).order_by(OutboundAttempt.id.desc()).limit(20))).scalars().all()
    guard_log = [{
        "at": a.created_at.isoformat() if a.created_at else None,
        "draft_id": a.draft_id, "mode": a.mode, "allowed": a.allowed,
        "reasons": a.reasons or [],
    } for a in attempts]

    return {
        "limits": limits_rows,
        # Блок-листы ещё не построены. Пустые списки без пояснения читались бы как
        # «проверено, никого нет» — а это не так.
        "blocklists": [
            {"name": "Админы и модераторы каналов", "source": "не построен",
             "count": None},
            {"name": "Боты", "source": "определяется на L0 по флагу автора",
             "count": None},
            {"name": "Ранее написанные", "source": "не построен — появится с первой отправкой",
             "count": 0},
        ],
        "guard_log": guard_log,
    }


# ── админ ─────────────────────────────────────────────────────────────────────

@router.get("/users")
async def users(db: GetDB, user=requires(Section.ADMIN)):
    """Пользователи — из БД. Список из двух строк в коде выглядел бы так же, но
    молчал бы о заведённом третьем и о выключенном первом."""
    rows = (await db.execute(select(User).order_by(User.id))).scalars().all()
    return [{"id": u.id, "name": u.name, "email": u.email, "role": u.role,
             # У поля есть свой флаг: секрет заведён всем, а подтверждён тот, кто
             # хотя бы раз ввёл код. Выводить одно из другого — врать.
             "totp_confirmed": u.totp_confirmed, "is_active": u.is_active,
             "last_login_at": u.last_login_at.isoformat() if u.last_login_at else None}
            for u in rows]


AUDIT_SORTS = {"created": AuditLog.created_at, "user": AuditLog.user_email,
               "action": AuditLog.action}


@router.get("/audit")
async def audit(db: GetDB, user=requires(Section.ADMIN),
                p: ListParams = Depends(list_params)):
    """Журнал аудита. Отвечает на вопрос «кто это сделал» — сюда пишутся решения
    по черновикам, переключения режима и аварийные остановки."""
    q = select(AuditLog)
    count_q = select(func.count(AuditLog.id))

    # Поиск по email пользователя и типу действия: помогает найти конкретный акт
    # и все действия конкретного человека.
    search = [AuditLog.user_email, AuditLog.action]
    q = apply_search(q, p, search)
    count_q = apply_search(count_q, p, search)

    total = (await db.execute(count_q)).scalar_one()
    q = apply_sort(q, p, AUDIT_SORTS, default="created", tiebreak=AuditLog.id)
    rows = (await db.execute(q.limit(p.limit).offset(p.offset))).scalars().all()
    return {**p.page(total),
            "rows": [{"id": a.id, "user_email": a.user_email, "action": a.action,
                      "detail": a.detail, "ip": a.ip,
                      "created_at": a.created_at.isoformat() if a.created_at else None}
                     for a in rows]}
