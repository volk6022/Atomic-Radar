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
from fastapi import APIRouter, HTTPException, Query

from datetime import datetime, timedelta, timezone

from sqlalchemy import func, or_, select

from app.api.deps import GetDB, requires
from app.core import cascade
from app.core import invariants as inv
from app.core.access import Section
from app.api.v1.system import get_state
from app.db.models import (Attribution, AuditLog, Channel, Conversation, Draft,
                           Lead, LlmTrace, Message, OutboundAttempt,
                           ProfileVersion, Run, User)
from app.services import drafting, engage

router = APIRouter(prefix="/api/v1", tags=["screens"])


def _page(rows: list[dict], limit: int, offset: int) -> dict:
    """Единая обёртка для листингов.

    `total` отдаётся всегда: на реальных объёмах (одна активная группа даёт ~9000
    сообщений в сутки) экран обязан знать размер выборки, не получая её целиком.
    """
    return {"total": len(rows), "limit": limit, "offset": offset,
            "rows": rows[offset:offset + limit]}


# ── шапка и общее ─────────────────────────────────────────────────────────────

async def _counts(db) -> dict:
    """Одним местом на весь дашборд: числа обязаны сходиться между плитками,
    воронкой и бейджами меню. Считать их в трёх местах — гарантированно разойтись."""
    day_ago = datetime.now(timezone.utc) - timedelta(days=1)

    total_msgs = (await db.execute(select(func.count(Message.id)))).scalar_one()
    msgs_24h = (await db.execute(
        select(func.count(Message.id)).where(Message.tg_date >= day_ago))).scalar_one()
    l0_passed = (await db.execute(
        select(func.count(Message.id)).where(Message.cascade_level > 0))).scalar_one()
    l1_passed = (await db.execute(
        select(func.count(Message.id)).where(Message.cascade_passed.is_(True)))).scalar_one()
    leads = (await db.execute(select(func.count(Lead.id)))).scalar_one()
    leads_new = (await db.execute(
        select(func.count(Lead.id)).where(Lead.status == "new"))).scalar_one()
    channels = (await db.execute(select(func.count(Channel.id)))).scalar_one()
    drafts_pending = (await db.execute(
        select(func.count(Draft.id)).where(Draft.state == "pending"))).scalar_one()

    return {"total_msgs": total_msgs, "msgs_24h": msgs_24h, "l0": l0_passed,
            "l1": l1_passed, "leads": leads, "leads_new": leads_new,
            "channels": channels, "drafts": drafts_pending}


@router.get("/alerts")
async def alerts(db: GetDB, user=requires(Section.DASHBOARD)):
    """Тревоги. Пока система в сухом прогоне, самая важная новость — что она в нём
    и находится: оператор должен видеть это, не заходя в Safety."""
    state = await get_state(db)
    out = [{"id": 1, "severity": "info" if state.mode == "DRY_RUN" else "error",
            "text": ("Сухой прогон: ни одно сообщение не уходит наружу"
                     if state.mode == "DRY_RUN" else
                     "ВНИМАНИЕ: режим LIVE — сообщения уходят людям"),
            "created_at": None}]
    if state.killed:
        out.insert(0, {"id": 0, "severity": "error", "created_at": None,
                       "text": "Аварийная остановка: " + (state.killed_reason or "")})

    c = await _counts(db)
    if c["channels"] and not c["total_msgs"]:
        out.append({"id": 2, "severity": "warn", "created_at": None,
                    "text": "Каналы заведены, но сообщений нет — бэкфилл не запускался"})
    return out


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

    # Здоровье Engage спрашиваем у самого Engage, а не рисуем зелёный кружок по вере.
    try:
        await engage.fleet_health()
        engage_status = "ok"
    except engage.EngageUnavailable:
        engage_status = "down"

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
        # L2/L3 показываем прочерком: их ещё нет, и ноль читался бы как «всё отсеяно».
        "funnel": [
            {"step": "Сообщений", "count": c["total_msgs"], "go": "stream"},
            {"step": "L0 структура", "count": c["l0"], "go": "stream"},
            {"step": "L1 слова", "count": c["l1"], "go": "stream"},
            {"step": "L2 эмбеддинги", "count": None, "go": "stream"},
            {"step": "L3 LLM", "count": None, "go": "stream"},
            {"step": "Лиды", "count": c["leads"], "go": "leads"},
        ],
        "mode": "DRY_RUN" if state.killed else state.mode,
        "killed": state.killed,
        "services": [
            {"name": "engage", "status": engage_status},
            {"name": "postgres", "status": "ok"},
            {"name": "llm", "status": "not_connected"},
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


@router.get("/channels")
async def channels(db: GetDB, user=requires(Section.CHANNELS)):
    """Реестр групп. Заводятся сами при первом сообщении из группы — просить оператора
    зарегистрировать канал заранее значило бы терять то, про что он ещё не знает."""
    rows = (await db.execute(
        select(Channel).order_by(Channel.leads_total.desc(), Channel.id)
    )).scalars().all()

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
    return out


@router.get("/messages")
async def messages(db: GetDB, user=requires(Section.STREAM),
                   limit: int = Query(50, ge=1, le=500), offset: int = Query(0, ge=0),
                   channel: str | None = None, passed: str | None = None):
    """Поток сообщений с результатом каскада L0→L3.

    У каждой ступени рядом с вердиктом лежит причина. Экран потока существует ради
    вопроса «почему это не стало лидом», и голое `false` на него не отвечает.

    Пагинация и фильтры считаются в SQL, а не в питоне: одна активная группа даёт
    около 9000 сообщений в сутки, и выбирать их целиком, чтобы показать пятьдесят,
    перестанет работать в первый же день реального ингеста.
    """
    q = select(Message, Channel).join(Channel, Message.channel_id == Channel.id)
    count_q = select(func.count(Message.id)).join(Channel, Message.channel_id == Channel.id)

    if channel:
        q = q.where(Channel.title == channel)
        count_q = count_q.where(Channel.title == channel)
    if passed == "true":
        q = q.where(Message.cascade_passed.is_(True))
        count_q = count_q.where(Message.cascade_passed.is_(True))
    elif passed == "false":
        q = q.where(or_(Message.cascade_passed.is_(False), Message.cascade_passed.is_(None)))
        count_q = count_q.where(or_(Message.cascade_passed.is_(False),
                                    Message.cascade_passed.is_(None)))

    total = (await db.execute(count_q)).scalar_one()
    rows = (await db.execute(
        q.order_by(Message.tg_date.desc()).limit(limit).offset(offset))).all()

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
            "cascade": {
                "l0": None if level is None else (level > 0 or bool(ok)),
                "l1": None if level is None or level < 1 else bool(ok),
                "l2": None, "l3": None,
            },
            "cascade_notes": detail,
            "lead_id": lead_by_message.get(m.id),
        })
    return {"total": total, "limit": limit, "offset": offset, "rows": out}


# ── конвейер лидов ────────────────────────────────────────────────────────────

@router.get("/leads")
async def leads(db: GetDB, user=requires(Section.LEADS),
                limit: int = Query(50, ge=1, le=500), offset: int = Query(0, ge=0),
                status: str | None = None):
    q = select(Lead, Channel).join(Channel, Lead.channel_id == Channel.id)
    count_q = select(func.count(Lead.id))
    if status:
        q = q.where(Lead.status == status)
        count_q = count_q.where(Lead.status == status)

    total = (await db.execute(count_q)).scalar_one()
    rows = (await db.execute(
        q.order_by(Lead.score.desc(), Lead.id.desc()).limit(limit).offset(offset))).all()

    out = [{
        "id": lead.id, "author_name": lead.author_name or "—",
        "author_username": ("@" + lead.author_username) if lead.author_username else None,
        "channel": c.title, "pain": lead.pain, "score": lead.score,
        "status": lead.status, "quote": lead.quote,
        "score_breakdown": lead.score_breakdown or [],
        "disqualifiers": lead.disqualifiers or [],
    } for lead, c in rows]
    return {"total": total, "limit": limit, "offset": offset, "rows": out}


# Очередь черновиков живёт в `app/api/v1/drafts.py`: там появилось состояние
# (решения оператора) и вызов OutboundGate, а этот модуль намеренно остаётся
# набором чистых заглушек без побочных эффектов.


@router.get("/conversations")
async def conversations(db: GetDB, user=requires(Section.CONVERSATIONS)):
    """Диалоги. Пока система в сухом прогоне, их не будет ни одного — и это
    не поломка экрана, а главное свойство режима."""
    rows = (await db.execute(
        select(Conversation).order_by(Conversation.id.desc()))).scalars().all()
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
    return {"rows": out, "total": len(out),
            "note": None if out else
                    "Диалогов нет: в сухом прогоне ни одно сообщение не отправляется"}


# ── настройка ─────────────────────────────────────────────────────────────────

@router.get("/profile")
async def profile(db: GetDB, user=requires(Section.PROFILE)):
    """Профиль заказчика и таксономия болей.

    Боли отдаются не из отдельной таблицы, а из самого каскада: это ровно тот список,
    по которому сейчас принимаются решения. Держать рядом «профиль для показа» и
    «правила для работы» — верный способ разойтись между экраном и поведением.
    """
    version = (await db.execute(
        select(ProfileVersion).where(ProfileVersion.is_active.is_(True))
        .order_by(ProfileVersion.id.desc()).limit(1))).scalar_one_or_none()

    pains = [{"key": pain, "label": pain, "anchors": list(anchors)[:6],
              "anchors_total": len(anchors)}
             for pain, anchors in cascade.PAIN_ANCHORS.items()]

    return {
        "version": version.version if version else "не сохранён",
        "is_active": bool(version),
        "business_description": (version.business_description if version else
                                 "Настройка и сопровождение VPN/VPS-инфраструктуры "
                                 "под ключ, перенос без простоя."),
        "pains": pains,
        "disqualifiers": [{"key": k, "markers": list(v)[:6]}
                          for k, v in cascade.DISQUALIFIERS.items()],
        "generation": {
            "prompt_version": drafting.PROMPT_VERSION,
            "note": "Черновики собираются по шаблонам: модель ещё не подключена",
        },
    }


@router.get("/runs")
async def runs(db: GetDB, user=requires(Section.RUNS)):
    """Прогоны. Бэкфилл сейчас запускается точечно и в отдельный прогон не
    оформляется — поэтому список пуст, и об этом сказано прямо."""
    rows = (await db.execute(select(Run).order_by(Run.id.desc()).limit(50))).scalars().all()
    return {"rows": [{"id": r.id, "name": r.name, "kind": r.kind, "status": r.status,
                      "progress": float(r.progress or 0), "error": r.error,
                      "created_at": r.created_at.isoformat() if r.created_at else None}
                     for r in rows],
            "note": None if rows else
                    "Прогонов нет: бэкфилл запускается по кнопке в разделе Channels "
                    "и пока не оформляется отдельной задачей"}


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


@router.get("/traces")
async def traces(db: GetDB, user=requires(Section.OBSERVABILITY),
                 limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0)):
    """LLM-трейсы. Модель ещё не подключена, поэтому список пуст — и это
    единственное честное содержимое экрана наблюдаемости на текущем этапе."""
    total = (await db.execute(select(func.count(LlmTrace.id)))).scalar_one()
    rows = (await db.execute(select(LlmTrace).order_by(LlmTrace.id.desc())
                             .limit(limit).offset(offset))).scalars().all()
    out = [{"id": t.id, "stage": t.stage, "model": t.model,
            "prompt_version": t.prompt_version,
            "tokens_in": t.tokens_in, "tokens_out": t.tokens_out,
            "latency_ms": t.latency_ms,
            "cost_usd": float(t.cost_usd) if t.cost_usd else 0.0,
            "created_at": t.created_at.isoformat() if t.created_at else None}
           for t in rows]
    return {"total": total, "limit": limit, "offset": offset, "rows": out,
            "note": None if out else
                    "Трейсов нет: L2/L3 ещё не построены, генерация идёт по шаблонам"}


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


@router.get("/audit")
async def audit(db: GetDB, user=requires(Section.ADMIN),
                limit: int = Query(100, ge=1, le=500), offset: int = Query(0, ge=0)):
    """Журнал аудита. Отвечает на вопрос «кто это сделал» — сюда пишутся решения
    по черновикам, переключения режима и аварийные остановки."""
    total = (await db.execute(select(func.count(AuditLog.id)))).scalar_one()
    rows = (await db.execute(select(AuditLog).order_by(AuditLog.id.desc())
                             .limit(limit).offset(offset))).scalars().all()
    return {"total": total, "limit": limit, "offset": offset,
            "rows": [{"id": a.id, "user_email": a.user_email, "action": a.action,
                      "detail": a.detail, "ip": a.ip,
                      "created_at": a.created_at.isoformat() if a.created_at else None}
                     for a in rows]}
