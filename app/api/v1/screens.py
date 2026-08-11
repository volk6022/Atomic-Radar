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

from app.api.deps import requires
from app.api.v1 import drafts as drafts_api
from app.core.access import Section
from app.services import engage

router = APIRouter(prefix="/api/v1", tags=["screens"])


def _page(rows: list[dict], limit: int, offset: int) -> dict:
    """Единая обёртка для листингов.

    `total` отдаётся всегда: на реальных объёмах (одна активная группа даёт ~9000
    сообщений в сутки) экран обязан знать размер выборки, не получая её целиком.
    """
    return {"total": len(rows), "limit": limit, "offset": offset,
            "rows": rows[offset:offset + limit]}


# ── шапка и общее ─────────────────────────────────────────────────────────────

@router.get("/alerts")
async def alerts(user=requires(Section.DASHBOARD)):
    return [
        {"id": 1, "text": "vertsanov-03 ушёл в паузу: PeerFlood",
         "severity": "error", "created_at": "2026-08-04T09:12:00Z"},
        {"id": 2, "text": "run_20260805_regen упал: ConnectionError",
         "severity": "error", "created_at": "2026-08-05T11:04:00Z"},
        {"id": 3, "text": "Новый ответ в диалоге: Игорь С.",
         "severity": "info", "created_at": "2026-08-11T12:30:00Z"},
    ]


@router.get("/counters")
async def counters(user=requires(Section.DASHBOARD)):
    """Бейджи в меню.

    Счётчик черновиков берётся из самой очереди, а не из константы: оператор разобрал
    три штуки, а бейдж продолжал показывать 12 — расхождение мелкое, но именно такие
    приучают не верить цифрам на экране.
    """
    return {"drafts": len(drafts_api._pending()), "conversations": 3}


# ── Dashboard ─────────────────────────────────────────────────────────────────

@router.get("/dashboard")
async def dashboard(user=requires(Section.DASHBOARD)):
    return {
        "tiles": [
            {"key": "messages", "label": "Сообщений за 24ч", "value": 8712, "go": "stream"},
            {"key": "prefiltered", "label": "После префильтра", "value": 431, "go": "stream"},
            {"key": "leads", "label": "Лидов найдено", "value": 27, "go": "leads"},
            {"key": "drafts", "label": "Черновиков в очереди",
             "value": len(drafts_api._pending()), "go": "drafts"},
            {"key": "conversations", "label": "Диалогов активно", "value": 3, "go": "conversations"},
            {"key": "sent", "label": "Отправлено", "value": 0, "go": "conversations"},
            {"key": "revenue", "label": "Конверсий", "value": 0, "go": "attribution"},
        ],
        "queues": [
            {"key": "drafts", "label": "Черновики",
             "count": len(drafts_api._pending()), "go": "drafts"},
            {"key": "leads", "label": "Лиды на ревью", "count": 5, "go": "leads"},
            {"key": "runs", "label": "Прогоны", "count": 1, "go": "runs"},
            {"key": "conversations", "label": "Ждут ответа", "count": 3, "go": "conversations"},
        ],
        "errors": [
            {"id": 1, "title": "PeerFlood на vertsanov-03", "at": "2026-08-04T09:12:00Z",
             "account": "vertsanov-03", "count": 1},
            {"id": 2, "title": "ConnectionError в run_20260805_regen",
             "at": "2026-08-05T11:04:00Z", "account": None, "count": 3},
        ],
        "funnel": [
            {"step": "Сообщений", "count": 8712, "go": "stream"},
            {"step": "L0 регекс", "count": 2140, "go": "stream"},
            {"step": "L1 ключевые слова", "count": 890, "go": "stream"},
            {"step": "L2 эмбеддинги", "count": 431, "go": "stream"},
            {"step": "L3 LLM", "count": 27, "go": "leads"},
            {"step": "Черновики", "count": len(drafts_api._pending()), "go": "drafts"},
        ],
        # Режим показывается всегда: оператор должен видеть его, не заходя в Safety.
        "mode": "DRY_RUN",
        "services": [
            {"name": "engage", "status": "ok"},
            {"name": "postgres", "status": "ok"},
            {"name": "llm", "status": "ok"},
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
async def channels(user=requires(Section.CHANNELS)):
    return [
        {"id": 1, "title": "VPS & Hosting Talk", "username": "vps_hosting_talk",
         "topic": "хостинг", "members": 12400, "msgs_per_day": 145,
         "prefilter_rate": 0.052, "leads_total": 34, "leads_per_1000": 4.1,
         "ingest_enabled": True, "is_junk": False},
        {"id": 2, "title": "Информационная безопасность", "username": "infosec_ru",
         "topic": "безопасность", "members": 31000, "msgs_per_day": 310,
         "prefilter_rate": 0.031, "leads_total": 41, "leads_per_1000": 2.9,
         "ingest_enabled": True, "is_junk": False},
        {"id": 3, "title": "IT Стартапы РФ", "username": "it_startups",
         "topic": "стартапы", "members": 8900, "msgs_per_day": 210,
         "prefilter_rate": 0.018, "leads_total": 12, "leads_per_1000": 1.0,
         "ingest_enabled": False, "is_junk": False},
    ]


@router.get("/messages")
async def messages(user=requires(Section.STREAM),
                   limit: int = Query(50, ge=1, le=500), offset: int = Query(0, ge=0),
                   channel: str | None = None, passed: str | None = None):
    """Поток сообщений с результатом каскада L0→L3.

    У каждой ступени рядом с вердиктом лежит `notes` — человеческая причина. Экран
    потока существует ради вопроса «почему это не стало лидом», и голое `false`
    на него не отвечает: отладка префильтра иначе превращается в гадание.

    `l2`/`l3` равны `null`, когда до ступени не дошло. Это не то же самое, что «не
    прошло», и на экране рисуется третьим значком, а не крестом.
    """
    rows = [
        {"id": 88213, "channel": "VPS & Hosting Talk", "author_name": "Дмитрий К.",
         "author_username": "@dmitry_kzl",
         "text": "ребят, задолбался с текущим хостингом, тормозит жутко, "
                 "кто может посоветовать замену?",
         "tg_date": "2026-08-11T15:41:00Z", "is_automatic_forward": False,
         "cascade": {"l0": True, "l1": True, "l2": True, "l3": True},
         "cascade_notes": {
             "l0": "не пост канала, длина 96, автор не бот и не админ",
             "l1": "совпало «хостинг», «тормозит», «посоветовать»",
             "l2": "близость 0.79 к «жалоба на хостинг» (порог 0.62)",
             "l3": "боль подтверждена, интент явный, признаки ЛПР есть",
         },
         "lead_id": 4821},
        {"id": 88215, "channel": "VPS & Hosting Talk", "author_name": "сосед",
         "author_username": None, "text": "+1, тоже думаю",
         "tg_date": "2026-08-11T15:44:00Z", "is_automatic_forward": False,
         "cascade": {"l0": True, "l1": False, "l2": None, "l3": None},
         "cascade_notes": {
             "l0": "не пост канала, длина 13",
             "l1": "ни одного ключевого слова, длина ниже порога",
             "l2": "не запускался: отсеяно на L1",
             "l3": "не запускался: отсеяно на L1",
         },
         "lead_id": None},
        {"id": 44120, "channel": "Информационная безопасность", "author_name": "Игорь С.",
         "author_username": "@igor_secops",
         "text": "у меня VPN постоянно отваливается на удалёнке, задрало",
         "tg_date": "2026-08-11T14:02:00Z", "is_automatic_forward": False,
         "cascade": {"l0": True, "l1": True, "l2": True, "l3": True},
         "cascade_notes": {
             "l0": "не пост канала, длина 55, автор не бот и не админ",
             "l1": "совпало «VPN», «отваливается»",
             "l2": "близость 0.83 к «жалоба на VPN» (порог 0.62)",
             "l3": "боль подтверждена, срочность высокая",
         },
         "lead_id": 4822},
        {"id": 44118, "channel": "Информационная безопасность", "author_name": "Бот Новостей",
         "author_username": "@infosec_news_bot",
         "text": "Дайджест уязвимостей за неделю — читайте в канале",
         "tg_date": "2026-08-11T13:50:00Z", "is_automatic_forward": True,
         "cascade": {"l0": False, "l1": None, "l2": None, "l3": None},
         "cascade_notes": {
             "l0": "автопересылка поста канала — комментарии к ней не считаются репликой",
             "l1": "не запускался: отсеяно на L0",
             "l2": "не запускался: отсеяно на L0",
             "l3": "не запускался: отсеяно на L0",
         },
         "lead_id": None},
        {"id": 88377, "channel": "VPS & Hosting Talk", "author_name": "Марина Л.",
         "author_username": "@marina_l",
         "text": "кто-нибудь поднимал 3x-ui на дебиане? запутался в конфигах",
         "tg_date": "2026-08-11T15:21:00Z", "is_automatic_forward": False,
         "cascade": {"l0": True, "l1": True, "l2": True, "l3": True},
         "cascade_notes": {
             "l0": "не пост канала, длина 58, автор не бот и не админ",
             "l1": "совпало «3x-ui», «конфиг»",
             "l2": "близость 0.71 к «не может настроить сам» (порог 0.62)",
             "l3": "боль подтверждена, интент — просьба о помощи",
         },
         "lead_id": 4824},
        {"id": 88401, "channel": "IT Стартапы РФ", "author_name": "Сергей П.",
         "author_username": "@sergey_p",
         "text": "а вы какой стек берёте для MVP? думаю между next и remix",
         "tg_date": "2026-08-11T12:10:00Z", "is_automatic_forward": False,
         "cascade": {"l0": True, "l1": True, "l2": False, "l3": None},
         "cascade_notes": {
             "l0": "не пост канала, длина 61, автор не бот и не админ",
             "l1": "совпало «стек» — слово общее, само по себе боли не значит",
             "l2": "близость 0.34, порог 0.62 — тема не про инфраструктуру",
             "l3": "не запускался: отсеяно на L2",
         },
         "lead_id": None},
    ]

    if channel:
        rows = [r for r in rows if r["channel"] == channel]
    # Фильтр «дошло до конца каскада» — то, ради чего на экран заходят чаще всего.
    if passed == "true":
        rows = [r for r in rows if r["cascade"]["l3"] is True]
    elif passed == "false":
        rows = [r for r in rows if r["cascade"]["l3"] is not True]

    return _page(rows, limit, offset)


# ── конвейер лидов ────────────────────────────────────────────────────────────

@router.get("/leads")
async def leads(user=requires(Section.LEADS),
                limit: int = Query(50, ge=1, le=500), offset: int = Query(0, ge=0),
                status: str | None = None):
    rows = [
        {"id": 4821, "author_name": "Дмитрий К.", "author_username": "@dmitry_kzl",
         "channel": "VPS & Hosting Talk", "pain": "хостинг тормозит/дорог",
         "score": 87, "status": "new",
         "quote": "задолбался с текущим хостингом, тормозит жутко",
         "score_breakdown": [
             {"label": "совпадение с болью", "value": 32},
             {"label": "срочность/интент", "value": 24},
             {"label": "признаки ЛПР", "value": 15},
             {"label": "свежесть", "value": 10},
             {"label": "достижимость в ЛС", "value": 6},
         ],
         "disqualifiers": []},
        {"id": 4822, "author_name": "Игорь С.", "author_username": "@igor_secops",
         "channel": "Информационная безопасность", "pain": "VPN постоянно отваливается",
         "score": 92, "status": "in_review", "quote": "VPN постоянно отваливается",
         "score_breakdown": [], "disqualifiers": []},
        {"id": 4823, "author_name": "Анна В.", "author_username": "@anna_vv",
         "channel": "IT Стартапы РФ", "pain": "нужен self-hosted VPN",
         "score": 74, "status": "approved", "quote": "ищу self-hosted решение",
         "score_breakdown": [], "disqualifiers": []},
    ]
    if status:
        rows = [r for r in rows if r["status"] == status]
    return _page(rows, limit, offset)


# Очередь черновиков живёт в `app/api/v1/drafts.py`: там появилось состояние
# (решения оператора) и вызов OutboundGate, а этот модуль намеренно остаётся
# набором чистых заглушек без побочных эффектов.


@router.get("/conversations")
async def conversations(user=requires(Section.CONVERSATIONS)):
    return [
        {"id": 51, "lead_id": 4822, "peer_name": "Игорь С.", "peer_username": "@igor_secops",
         "account": "vertsanov-01", "state": "awaiting_reply", "sent_count": 1,
         "last_sent_at": "2026-08-10T12:00:00Z", "waiting_since": "2026-08-10T12:00:00Z"},
        {"id": 52, "lead_id": 4823, "peer_name": "Анна В.", "peer_username": "@anna_vv",
         "account": "vertsanov-02", "state": "replied", "sent_count": 2,
         "last_sent_at": "2026-08-09T10:30:00Z", "waiting_since": None},
    ]


# ── настройка ─────────────────────────────────────────────────────────────────

@router.get("/profile")
async def profile(user=requires(Section.PROFILE)):
    return {
        "version": "v3", "is_active": True,
        "business_description": "Настройка и сопровождение VPN/VPS-инфраструктуры "
                                "под ключ, перенос без простоя.",
        "pains": [
            {"key": "slow_hosting", "label": "хостинг тормозит или дорог", "weight": 32},
            {"key": "vpn_unstable", "label": "VPN отваливается", "weight": 30},
            {"key": "self_hosted", "label": "нужен self-hosted VPN", "weight": 25},
            {"key": "team_vpn", "label": "VPN для команды", "weight": 20},
        ],
    }


@router.get("/runs")
async def runs(user=requires(Section.RUNS)):
    return [
        {"id": 1, "name": "run_20260805_regen", "kind": "generation", "status": "failed",
         "progress": 42.0, "eta_seconds": None, "gpu_hours": 4.2,
         "error": "ConnectionError", "created_at": "2026-08-05T11:00:00Z"},
        {"id": 2, "name": "run_20260811_backfill", "kind": "backfill", "status": "running",
         "progress": 68.5, "eta_seconds": 1800, "gpu_hours": 0.0,
         "error": None, "created_at": "2026-08-11T08:00:00Z"},
    ]


@router.get("/evaluations")
async def evaluations(user=requires(Section.EVALS)):
    return [
        {"prompt_version": "v1", "dataset_size": 120, "precision": 0.61,
         "recall": 0.55, "f1": 0.58},
        {"prompt_version": "v2", "dataset_size": 180, "precision": 0.72,
         "recall": 0.66, "f1": 0.69},
        {"prompt_version": "v3", "dataset_size": 240, "precision": 0.78,
         "recall": 0.71, "f1": 0.74},
    ]


# ── бизнес и эксплуатация ─────────────────────────────────────────────────────

@router.get("/attribution")
async def attribution(user=requires(Section.ATTRIBUTION)):
    return {
        "leads": [
            {"ref_token": "rdr_4821", "lead_id": 4821, "clicked_at": None,
             "bot_started_at": None, "converted_at": None, "amount": None},
        ],
        "unit_economics": [
            {"channel": "VPS & Hosting Talk", "leads": 34, "answered": 0, "converted": 0,
             "revenue": 0.0, "gpu_cost": 12.40, "proxy_cost": 3.00},
            {"channel": "Информационная безопасность", "leads": 41, "answered": 0,
             "converted": 0, "revenue": 0.0, "gpu_cost": 15.10, "proxy_cost": 3.00},
        ],
    }


@router.get("/traces")
async def traces(user=requires(Section.OBSERVABILITY),
                 limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0)):
    rows = [
        {"id": 1, "stage": "classify", "model": "qwen3.5-9b", "prompt_version": "v3",
         "temperature": 0.2, "tokens_in": 820, "tokens_out": 64, "latency_ms": 1840,
         "cost_usd": 0.0, "created_at": "2026-08-11T15:42:00Z"},
        {"id": 2, "stage": "generate", "model": "qwen3.5-9b", "prompt_version": "v3",
         "temperature": 0.7, "tokens_in": 1240, "tokens_out": 210, "latency_ms": 5210,
         "cost_usd": 0.0, "created_at": "2026-08-11T15:43:00Z"},
    ]
    return _page(rows, limit, offset)


@router.get("/limits")
async def limits(user=requires(Section.SAFETY)):
    """Пороги гардрейлов. Значения — те же, что зашиты в `app.core.invariants`;
    экран Safety их показывает и правит, а проверяет всё равно код."""
    return {
        "limits": [
            {"key": "max_messages_per_conversation", "value": 4, "unit": "шт",
             "description": "больше — это преследование, а не диалог"},
            {"key": "min_gap_hours", "value": 20, "unit": "ч",
             "description": "пауза между сообщениями одному человеку"},
            {"key": "quiet_hours_start", "value": 0, "unit": "ч", "description": "тихие часы"},
            {"key": "quiet_hours_end", "value": 8, "unit": "ч", "description": "тихие часы"},
            {"key": "max_links_first_message", "value": 0, "unit": "шт",
             "description": "ссылка в первом сообщении — прямой путь в спам"},
        ],
        "blocklists": {
            "usernames": [], "domains": [], "keywords": [],
        },
        "guard_log": [
            {"at": "2026-08-11T12:00:00Z", "reason": "режим DRY_RUN: отправка запрещена",
             "draft_id": 901},
        ],
    }


# ── админ ─────────────────────────────────────────────────────────────────────

@router.get("/users")
async def users(user=requires(Section.ADMIN)):
    return [
        {"id": 1, "name": "Иван", "initials": "ИВ", "email": "ivan@atomic-automation.net",
         "role": "owner", "totp_confirmed": True, "is_active": True},
        {"id": 2, "name": "Андрей", "initials": "АВ", "email": "andrey@vertsanov.ru",
         "role": "customer", "totp_confirmed": False, "is_active": True},
    ]


@router.get("/audit")
async def audit(user=requires(Section.ADMIN),
                limit: int = Query(100, ge=1, le=500), offset: int = Query(0, ge=0)):
    rows = [
        {"id": 1, "user_email": "ivan@atomic-automation.net", "action": "login_ok",
         "ip": "85.140.12.7", "created_at": "2026-08-11T09:22:00Z"},
    ]
    return _page(rows, limit, offset)
