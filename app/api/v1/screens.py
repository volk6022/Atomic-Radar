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

from fastapi import APIRouter, Query

from app.api.deps import requires
from app.core.access import Section

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
    """Бейджи в меню. В вёрстке они захардкожены (`badge:'12'`, `badge:'3'`)."""
    return {"drafts": 12, "conversations": 3}


# ── Dashboard ─────────────────────────────────────────────────────────────────

@router.get("/dashboard")
async def dashboard(user=requires(Section.DASHBOARD)):
    return {
        "tiles": [
            {"key": "messages", "label": "Сообщений за 24ч", "value": 8712, "go": "stream"},
            {"key": "prefiltered", "label": "После префильтра", "value": 431, "go": "stream"},
            {"key": "leads", "label": "Лидов найдено", "value": 27, "go": "leads"},
            {"key": "drafts", "label": "Черновиков в очереди", "value": 12, "go": "drafts"},
            {"key": "conversations", "label": "Диалогов активно", "value": 3, "go": "conversations"},
            {"key": "sent", "label": "Отправлено", "value": 0, "go": "conversations"},
            {"key": "revenue", "label": "Конверсий", "value": 0, "go": "attribution"},
        ],
        "queues": [
            {"key": "drafts", "label": "Черновики", "count": 12, "go": "drafts"},
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
            {"step": "Черновики", "count": 12, "go": "drafts"},
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

@router.get("/accounts")
async def accounts(user=requires(Section.FLEET)):
    return [
        {"id": 1, "label": "vertsanov-01", "status": "active", "phone_country": "US",
         "proxy_country": "US", "tz_offset": -18000, "limit_day": 30, "limit_hour": 4,
         "last_action_at": "2026-08-11T09:41:00Z", "watcher_uptime": 99.2},
        {"id": 2, "label": "vertsanov-02", "status": "active", "phone_country": "FR",
         "proxy_country": "US", "tz_offset": -18000, "limit_day": 30, "limit_hour": 4,
         "last_action_at": "2026-08-11T09:38:00Z", "watcher_uptime": 98.7},
        {"id": 3, "label": "vertsanov-03", "status": "sleeping", "phone_country": "GB",
         "proxy_country": "US", "tz_offset": -18000, "limit_day": 30, "limit_hour": 4,
         "last_action_at": "2026-08-04T09:12:00Z", "watcher_uptime": 0.0},
    ]


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
                   limit: int = Query(50, ge=1, le=500), offset: int = Query(0, ge=0)):
    rows = [
        {"id": 1, "channel": "VPS & Hosting Talk", "author_name": "Дмитрий К.",
         "author_username": "@dmitry_kzl",
         "text": "ребят, задолбался с текущим хостингом, тормозит жутко",
         "tg_date": "2026-08-11T15:41:00Z", "is_automatic_forward": False,
         "cascade": {"l0": True, "l1": True, "l2": True, "l3": True}},
        {"id": 2, "channel": "VPS & Hosting Talk", "author_name": "сосед",
         "author_username": None, "text": "+1, тоже думаю",
         "tg_date": "2026-08-11T15:44:00Z", "is_automatic_forward": False,
         "cascade": {"l0": True, "l1": False, "l2": None, "l3": None}},
        {"id": 3, "channel": "Информационная безопасность", "author_name": "Игорь С.",
         "author_username": "@igor_secops", "text": "VPN постоянно отваливается",
         "tg_date": "2026-08-11T14:02:00Z", "is_automatic_forward": False,
         "cascade": {"l0": True, "l1": True, "l2": True, "l3": True}},
    ]
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


@router.get("/drafts/next")
async def next_draft(user=requires(Section.DRAFTS), after: int | None = None):
    """Экран черновиков курсорный: показывает один и двигается по очереди.

    Поэтому не список, а «следующий после». `remaining` нужен, чтобы оператор видел
    объём работы — это единственный экран, где он сидит подолгу.
    """
    return {
        "id": 901, "lead_id": 4821, "remaining": 12,
        "author_name": "Дмитрий К.", "author_username": "@dmitry_kzl",
        "channel": "VPS & Hosting Talk", "pain": "хостинг тормозит/дорого", "score": 87,
        "score_breakdown": [
            {"label": "совпадение с болью", "value": 32},
            {"label": "срочность/интент", "value": 24},
            {"label": "признаки ЛПР", "value": 15},
            {"label": "свежесть", "value": 10},
            {"label": "достижимость в ЛС", "value": 6},
        ],
        "thread": [
            {"meta": "15:02 · корневой пост",
             "text": "«Обзор провайдеров VPS 2026 — делимся опытом в комментах»", "target": False},
            {"meta": "15:15 · сосед", "text": "«а кто как считает?»", "target": False},
            {"meta": "15:41 · Дмитрий К. — кандидат",
             "text": "«ребят, задолбался с текущим хостингом, тормозит жутко»", "target": True},
        ],
        "variants": [
            {"text": "«Видел твой вопрос про хостинг — обратись к Андрею (@vertsanov_biz), "
                     "мне он за день поднял новый сервер, полёт нормальный»",
             "spam_score": 0.12, "prompt_version": "v3", "lint_ok": True,
             "critic_passed": True,
             "critic_text": "Звучит нативно, как реальная рекомендация, не как реклама"},
            {"text": "«Привет! По хостингу — знакомый Андрей (@vertsanov_biz) занимается "
                     "инфраструктурой, переносил меня без простоя»",
             "spam_score": 0.18, "prompt_version": "v3", "lint_ok": True,
             "critic_passed": True, "critic_text": "Чуть обобщённее первого, но без штампов"},
            {"text": "«Хостинг тормозит? Андрей решает под ключ за 1 день, "
                     "гарантия аптайма 99.9%, пиши прямо сейчас!»",
             "spam_score": 0.61, "prompt_version": "v3", "lint_ok": False,
             "critic_passed": False,
             "critic_text": "Читается как реклама: обещание гарантии и призыв к действию"},
        ],
        "source_message_link": "https://t.me/c/1923847561/88213",
        "state": "pending",
    }


@router.get("/drafts/reasons")
async def reject_reasons(user=requires(Section.DRAFTS)):
    """Справочник причин отклонения. Список закрытый: причина уходит в eval-датасет,
    на котором меряется качество генерации, поэтому свободный текст его размывает."""
    return [
        {"n": 1, "label": "Не та боль"},
        {"n": 2, "label": "Не тот человек"},
        {"n": 3, "label": "Звучит как реклама"},
        {"n": 4, "label": "Слишком длинно"},
        {"n": 5, "label": "Фактическая ошибка"},
        {"n": 6, "label": "Неверный тон"},
        {"n": 7, "label": "Дублирует отправленное"},
        {"n": 8, "label": "Ссылка в первом сообщении"},
        {"n": 9, "label": "Другое"},
    ]


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
