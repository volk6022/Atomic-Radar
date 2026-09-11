"""Служба discovery: пороги, три проверки кандидата и тик (сценарий B, B.3–B.5).

Проверки идут по возрастанию цены — карточка → живость → модель — и следующая
ступень зовётся только для прошедших предыдущую (B.3: «модель зовём последней»).
Это не экономия ради экономии: чтение карточки и страниц истории списывается с
дневного бюджета аккаунта Engage, а вызов модели занимает карту, на которой в это
же время работает живой каскад. Порядок — свойство сценария, а не украшение.

Отказ и откладывание здесь разные вещи, и граница проведена по вине канала:
приватный канал, мёртвая группа, «не та аудитория» — отказ (данные о решении,
B.2); недоступный Engage, исчерпанный дневной лимит, промолчавшая модель —
откладывание: кандидат остаётся `pending`, и следующий прогон (тик бьётся каждые
пять минут) доделает сам. Образцы разделения — `backfill_drain` (сеть не вина
канала) и `plan_joins` (пачка режется молча, остаток виден в отчёте).

Правило аккаунта (B.5): карточку и живость спрашивает тем аккаунтом, которым
искали (`found_by_account_id`) — иначе счётчики чтений размажутся по флоту, и
человек, запускавший поиск с конкретного аккаунта, не увидит подмены.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from app.core import clock
from app.db.models import Channel, ChannelCandidate, DiscoveryQuery, Limit, LlmTrace, Run
from app.db.session import get_session_maker
from app.services import cascade_registry, deferrals, engage, llm

logger = logging.getLogger(__name__)

# Окно живости — 7 суток (B.3). Считается на момент шага, а не прогона: кандидат,
# простоявший в очереди день, должен быть измерен по тем же суткам, что увидит.
LIVENESS_WINDOW = timedelta(days=7)
# Страница истории для живости: потолок Engage — 1000, но живости достаточно
# сотни (порог постов — 2), и каждая страница стоит чтения из дневного бюджета.
LIVENESS_PAGE = 100
# Сколько ждать ответа поиска: он дешевле цепочки страниц, живого потока нет.
SCAN_WAIT_SECONDS = 60.0

# Кто подписывает автоматические решения. Различимы в `decided_by` наравне с
# email оператора — по ним видно, что решение приняла ступень, а не человек.
CARD_ACTOR = "auto:card"
LIVENESS_ACTOR = "auto:liveness"
FIT_ACTOR = "auto:channel_fit_v1"

# §6 — пороги и лимиты сценария B. Числа из постановки (§5) менять нельзя;
# два последних — умолчания контракта: 100 покрывает кандидатов нескольких
# поисков (~10 рекомендаций на заказ; «пять поисков» — объём одного прогона,
# Решение 1), 3 — не выше потолка вступлений аккаунта, чтобы автовключения
# не съели дневной бюджет флота.
DEFAULTS = {
    "discovery_queries_per_scan": 5,     # Решение 1 — объём прогона, не суточный лимит
    "discovery_min_members": 500,        # §5.1 — условие задачи
    "discovery_min_posts_7d": 2,         # §5.1 — условие задачи
    "discovery_min_comments_7d": 50,     # §5.1 — условие задачи
    "discovery_llm_checks_per_day": 100, # умолчание, не из постановки (см. выше)
    "discovery_auto_joins_per_day": 3,   # умолчание, не из постановки (см. выше)
    "discovery_autoconnect_enabled": 0,  # выключатель, по умолчанию выключен (B.4)
}


# ── пороги (§6) ───────────────────────────────────────────────────────────────

# Старое имя ключа потолка поисков (Решение 1: «пять поисков» — объём прогона,
# а не суточный лимит). Строка с таким ключом в `limits` на проде возможна —
# пороги правятся без выкатки, — и терять выставленное число молча нельзя:
# отсюда fallback в `thresholds`. Сама старая строка не удаляется автоматически:
# её уважает fallback, пока владелец не уберёт руками.
_OLD_SEARCHES_KEY = "discovery_searches_per_day"

# Флаг подавления: переименование пишется в журнал один раз на процесс, иначе
# каждый прогон (а `thresholds` зовётся на каждом) спамил бы одной и той же
# строкой.
_KEY_RENAME_LOGGED = False


async def thresholds(db) -> dict:
    """Пороги discovery: строки `limits` поверх умолчаний, строка в БД побеждает.

    Таблица читается на каждом прогоне, а не кешируется на старте: правка порога
    обязана действовать без выкатки (§5.1 постановки — «правка конфига дешевле
    выкатки»). Пустая таблица — рабочее состояние, отсутствующие ключи берутся
    из кода.

    Fallback-миграция переименования `discovery_searches_per_day` →
    `discovery_queries_per_scan`: пока строки с новым именем нет, а со старым
    есть, значение старой становится значением нового ключа, и переименование
    один раз пишется в журнал. Перенести настройку в БД под новым именем —
    дело владельца, автоматического удаления старой строки нет.
    """
    rows = (await db.execute(
        select(Limit.key, Limit.value)
        .where(Limit.key.in_([*DEFAULTS, _OLD_SEARCHES_KEY])))).all()
    out = dict(DEFAULTS)
    seen: dict = {}
    for key, value in rows:
        seen[key] = value
        if key in out:  # старое имя в `out` не попадает — его место в fallback ниже
            out[key] = value
    global _KEY_RENAME_LOGGED
    if _OLD_SEARCHES_KEY in seen and "discovery_queries_per_scan" not in seen:
        out["discovery_queries_per_scan"] = seen[_OLD_SEARCHES_KEY]
        if not _KEY_RENAME_LOGGED:
            _KEY_RENAME_LOGGED = True
            logger.warning("limits_key_renamed old=%s new=%s",
                           _OLD_SEARCHES_KEY, "discovery_queries_per_scan")
    return out


def _utc_day_start(now: datetime | None = None) -> datetime:
    """Начало текущих UTC-суток. То же окно, что у уникальности `discovery_queries`
    (§1.2), дневного счётчика проверок моделью и автовключений — один день, одно
    число везде."""
    return (now or clock.utcnow()).replace(hour=0, minute=0, second=0, microsecond=0)


async def _llm_checks_today(db) -> int:
    """Сколько проверок моделью потрачено сегодня. Считается по трейсам с версией
    промпта channel-fit: трейс различим по `prompt_version`, поле `stage` у всех
    контуров общее `"l3"` (REVIEW.md §4)."""
    return (await db.execute(
        select(func.count(LlmTrace.id)).where(
            LlmTrace.prompt_version == llm.PROMPTS["channel_fit_v1"].version,
            LlmTrace.created_at >= _utc_day_start()))).scalar_one()


async def _auto_joins_today(db) -> int:
    """Сколько автовключений (fit → approved) уже произошло сегодня."""
    return (await db.execute(
        select(func.count(ChannelCandidate.id)).where(
            ChannelCandidate.decided_by == FIT_ACTOR,
            ChannelCandidate.decision.in_(("approved", "connected")),
            ChannelCandidate.decided_at >= _utc_day_start()))).scalar_one()


# ── шаг 1 — карточка (§4.1) ───────────────────────────────────────────────────

def _translate(reason: str | None) -> str:
    """Причина отказа Engage человеческим текстом — тем же словарём, что и приём
    (`app/api/v1.ingest`): текст отказа обязан читаться одинаково из любой двери.
    Импорт в момент вызова, а не наверху модуля: слой ручек тянет за собой всю
    пирамиду зависимостей, и разрывать её ради словаря строк незачем."""
    from app.api.v1.ingest import _translate_engage_reason
    return _translate_engage_reason(reason)


async def _reject(db, candidate, *, decided_by: str, reason: str) -> str:
    """Записать отказ и вернуть `"reject"`. Отказ — это данные (B.2): причина
    читается оператором в списке кандидатов, поэтому текст, а не код."""
    candidate.decision = "rejected"
    candidate.decided_by = decided_by
    candidate.decided_at = clock.utcnow()
    candidate.decision_reason = reason
    await db.commit()
    return "reject"


async def _defer(note: str, report) -> str:
    """Откладывание: кандидат остаётся `pending`, причина — только в лог прогона.
    В поля строки её писать нельзя — отложенное не решение, а его отсутствие."""
    if report is not None:
        await report(None, note)
    return "defer"


async def _fetch_info(account_id: int, username: str) -> dict:
    """Карточка чата — та же двухшаговая форма, что у разбора групп обсуждения:
    обратный адрес обязателен по контракту Engage, результат забираем опросом."""
    task = await engage.action(
        account_id=account_id, action="get_chat_info", payload={"username": username},
        webhook_url=engage.webhook_url(kind="polled"))
    return await engage.wait_for_task(task["task_id"])


async def check_card(db, candidate, *, account_id: int,
                     out: dict | None = None) -> str:
    """Шаг 1 — карточка (§4.1). Возвращает `"pass" | "reject" | "defer"`.

    `out` — детализация для прогона (как у `check_fit`): откладывание лимитом
    (`interpret` → deferred) дописывает сюда `(аккаунт, действие, код)` —
    прогон собирает такие тройки, чтобы один раз спросить окно возврата
    бюджета и назвать его в своём результате (R4).
    """
    if not candidate.username:
        # Без username `get_chat_info` не выполнить (нужен знакомый пир), а
        # подключить тем более: это серверная половина проверки B.7 «кандидат
        # без имени не доходит до вступления».
        return await _reject(db, candidate, decided_by=CARD_ACTOR,
                             reason="нет username — карточку не спросить и "
                                    "подключить нельзя")
    try:
        info = await _fetch_info(account_id, candidate.username)
    except (engage.EngageTaskFailed, engage.EngageUnavailable) as e:
        # `EngageTaskDeferred` — наследник `EngageTaskFailed`, так что одна строка
        # catch ловит все три вида; различает их общая трактовка (`deferrals`).
        interp = deferrals.interpret(e)
        if interp.kind == "failed":
            return await _reject(db, candidate, decided_by=CARD_ACTOR,
                                 reason=_translate(interp.code))
        if interp.kind == "unavailable":
            # Сбой сети — не вина канала: поля не трогаем, кандидат остаётся pending.
            return await _defer(f"@{candidate.username}: Engage недоступен — {e}", None)
        # Исчерпан дневной лимит чтений аккаунта — «можно, но не сегодня».
        if out is not None:
            out.setdefault("budget_deferrals", []).append(
                (account_id, "get_chat_info", interp.code))
        return await _defer(f"@{candidate.username}: карточка отложена — {interp.note}",
                            None)

    if not info.get("found", True):
        return await _reject(db, candidate, decided_by=CARD_ACTOR,
                             reason=_translate(info.get("reason")))

    if info.get("peer_id") is not None:
        candidate.peer_id = info["peer_id"]
    if info.get("title"):
        candidate.title = info["title"]
    if info.get("type"):
        candidate.chat_type = info["type"]
    candidate.linked_chat_username = info.get("linked_chat_username")
    if info.get("members_count") is not None:
        candidate.members = info["members_count"]
    await db.commit()

    lim = await thresholds(db)
    min_members = int(lim["discovery_min_members"])
    if candidate.members is None:
        # Карточка не отдала число участников: порог «500+» не проверить, а
        # пропустить непроверенное — молча принять канала без признака жизни.
        return await _reject(db, candidate, decided_by=CARD_ACTOR,
                             reason=f"число участников неизвестно — порог "
                                    f"{min_members} не проверить")
    if candidate.members < min_members:
        return await _reject(
            db, candidate, decided_by=CARD_ACTOR,
            reason=f"{candidate.members} участников — меньше порога {min_members}")
    if not candidate.linked_chat_username:
        # Без группы обсуждения комментарии не читаются — канал для сценария B
        # бесполезен, каким бы большим он ни был (отсев по B.3).
        return await _reject(db, candidate, decided_by=CARD_ACTOR,
                             reason="нет группы обсуждения — комментарии "
                                    "не читаются")
    return "pass"


# ── шаг 2 — живость (§4.2) ────────────────────────────────────────────────────

async def _history_page(account_id: int, username: str, *, min_date: str,
                        max_id: int = 0) -> list[dict]:
    """Страница истории за окно живости. `min_date` — параметр самого действия,
    а не постфильтр: страницы вне окна всё равно были бы прочитаны и списаны с
    дневного бюджета (тот же довод, что у цепочки бэкфилла)."""
    payload: dict = {"username": username, "limit": LIVENESS_PAGE, "min_date": min_date}
    if max_id:
        payload["max_id"] = max_id
    task = await engage.action(
        account_id=account_id, action="get_chat_history", payload=payload,
        webhook_url=engage.webhook_url(kind="polled"))
    result = await engage.wait_for_task(task["task_id"])
    return result.get("posts") or []


async def check_liveness(db, candidate, *, account_id: int,
                         now: datetime | None = None,
                         sample: list[str] | None = None, report=None,
                         out: dict | None = None) -> str:
    """Шаг 2 — живость (§4.2). Тем же аккаунтом, что и карточка (B.5).

    `sample` — сборник текстов для шага модели: выборку отдаёт живость, второй
    ходки в Telegram ради неё не делаем (§5). `now` вынесен параметром для
    проверяемости окна, как у `backfill_drain.tick`. `out` — как у `check_card`:
    откладывание лимитом записывает `(аккаунт, действие, код)` для окна прогона.
    """
    now = now or clock.utcnow()
    min_date = (now - LIVENESS_WINDOW).isoformat()
    lim = await thresholds(db)
    min_posts = int(lim["discovery_min_posts_7d"])
    min_comments = int(lim["discovery_min_comments_7d"])

    try:
        posts = await _history_page(account_id, candidate.username, min_date=min_date)
        if sample is not None:
            sample.extend(p.get("text") for p in posts if p.get("text"))
        # Полная страница (100) закрывает порог постов сама; листать дальше
        # ради точного числа незачем — лишние чтения из дневного бюджета.
        post_count = len(posts)

        comments = 0
        max_id = 0
        first_group_page = True
        while comments < min_comments:
            page = await _history_page(account_id, candidate.linked_chat_username,
                                       min_date=min_date, max_id=max_id)
            if not page:
                break
            comments += len(page)
            if first_group_page and sample is not None:
                sample.extend(p.get("text") for p in page if p.get("text"))
                first_group_page = False
            ids = [p["message_id"] for p in page if p.get("message_id")]
            if not ids:
                break
            oldest = min(ids)
            if max_id and oldest >= max_id:
                # Курсор не сдвинулся — история кончилась или Engage вернул ту же
                # страницу; крутить один вызов до конца бюджета нельзя.
                break
            max_id = oldest - 1
            if len(page) < LIVENESS_PAGE:
                break
    except (engage.EngageTaskFailed, engage.EngageUnavailable) as e:
        # `EngageTaskDeferred` внутри — общий предок ловит все три вида, различает
        # трактовка. Поля живости не трогаем: шаг не доделан, а не «мёртв».
        interp = deferrals.interpret(e)
        if interp.kind == "deferred":
            if out is not None:
                out.setdefault("budget_deferrals", []).append(
                    (account_id, "get_chat_history", interp.code))
            return await _defer(f"@{candidate.username}: живость отложена — {interp.note}",
                                report)
        return await _defer(f"@{candidate.username}: живость не посчитана — {interp.note}",
                            report)

    candidate.liveness_posts_7d = post_count
    candidate.liveness_comments_7d = comments
    candidate.liveness_checked_at = now
    await db.commit()

    if post_count < min_posts:
        return await _reject(
            db, candidate, decided_by=LIVENESS_ACTOR,
            reason=f"{post_count} постов за 7 суток — меньше порога {min_posts}")
    if comments < min_comments:
        return await _reject(
            db, candidate, decided_by=LIVENESS_ACTOR,
            reason=f"{comments} сообщений в группе за 7 суток — меньше порога "
                   f"{min_comments}")
    return "pass"


# ── шаг 3 — модель (§4.3) и вход (§5) ────────────────────────────────────────

def build_channel_fit_input(candidate, posts: list[str]) -> str:
    """Пользовательское сообщение для оценки канала (§5), по образцу
    `llm.build_prompt`: карточка и выборка текстов одной строкой, без формулировок
    «разбираемое сообщение» — они про сообщение из чата, а не про канал."""
    lines = ["Карточка канала:", f"Название: {candidate.title}"]
    if candidate.username:
        lines.append(f"Username: @{candidate.username}")
    if candidate.members is not None:
        lines.append(f"Участников: {candidate.members}")
    if candidate.chat_type:
        lines.append(f"Тип чата: {candidate.chat_type}")
    if candidate.linked_chat_username:
        lines.append(f"Группа обсуждения: @{candidate.linked_chat_username}")
    texts = [t for t in posts if t]
    if texts:
        lines.append("Последние сообщения (выборка):")
        lines += [f"- «{t[:200]}»" for t in texts[:10]]
    else:
        lines.append("Последние сообщения: (выборки нет)")
    return "\n".join(lines)


async def check_fit(db, candidate, *, report, posts: list[str] | None = None,
                    out: dict | None = None) -> str:
    """Шаг 3 — модель (§4.3). Зовётся последней и только для прошедших живость.

    `out` — детализация для счётчика прогона («спросили ли»): возвращаемое
    значение — только pass/reject/defer, а «отложено лимитом» и «модель молчала»
    для отчёта разные события.
    """
    lim = await thresholds(db)
    per_day = int(lim["discovery_llm_checks_per_day"])
    if await _llm_checks_today(db) >= per_day:
        # Лимит режет пачку на нашей стороне, а не отказом (B.7): остаток — не
        # отказ каналу, на следующие сутки тик подхватит сам.
        return await _defer(
            f"@{candidate.username}: дневной лимит проверок моделью ({per_day}) "
            f"исчерпан — осталось на следующие сутки", report)

    profile = await cascade_registry.active_profile_version(db)
    descr = (profile.business_description if profile is not None
             and profile.business_description else "(описание бизнеса не задано)")
    # `.replace`, а не `.format`: в шаблоне литеральные фигурные скобки JSON и
    # свободный текст оператора (REVIEW.md §1) — `.format` здесь падает.
    system = llm.CHANNEL_FIT_SYSTEM.replace("{business_description}", descr)
    user = build_channel_fit_input(candidate, posts or [])

    if out is not None:
        out["asked"] = True
    try:
        parsed, trace = await llm.verdict(text="", context=[], system=system,
                                          user=user, prompt_key="channel_fit_v1")
    except llm.LlmUnavailable as e:
        # «Не досчитали», а не «не прошло» — образец reclassify: недоступность
        # своей же машины не должна стоить кандидату вердикта.
        return await _defer(f"@{candidate.username}: модель недоступна — {e}", report)

    verdict = parsed.get("verdict")
    reason = (parsed.get("reason") or "").strip()
    if not reason or verdict not in ChannelCandidate.VERDICTS:
        # Вердикт без обоснования не сохраняется вовсе (B.7): ни llm_*, ни трейса —
        # модель не «ответила нет», а не ответила. Кандидат ждёт переспроса.
        return await _defer(
            f"@{candidate.username}: модель ответила без обоснования — вердикт "
            f"не сохранён", report)

    score = None
    try:
        value = int(parsed.get("score"))
    except (TypeError, ValueError):
        value = None
    # Диапазон проверяем кодом, а не ждём отказа схемы: CHECK в базе — последняя
    # линия обороны от других писателей, а не способ узнать об ошибке коммитом.
    if value is not None and 0 <= value <= 100:
        score = value

    candidate.llm_verdict = verdict
    candidate.llm_score = score
    candidate.llm_reason = reason
    candidate.llm_at = clock.utcnow()
    db.add(LlmTrace(**trace))

    if verdict == "unfit":
        # Отказ — данные (B.2): причина модели читается оператором в списке.
        candidate.decision = "rejected"
        candidate.decided_by = FIT_ACTOR
        candidate.decided_at = clock.utcnow()
        candidate.decision_reason = reason
        await db.commit()
        return "reject"

    if (verdict == "fit"
            and int(lim["discovery_autoconnect_enabled"]) == 1
            and await _auto_joins_today(db) < int(lim["discovery_auto_joins_per_day"])):
        # Автовыключатель (§4.3/§6): fit одобряется сам, но не больше
        # `discovery_auto_joins_per_day` за сутки — остаток подключается следующим
        # днём, чтобы автовключения не съели весь бюджет вступлений флота.
        candidate.decision = "approved"
        candidate.decided_by = FIT_ACTOR
        candidate.decided_at = clock.utcnow()
    await db.commit()
    # fit без выключателя и unclear остаются pending — смотрит человек.
    return "pass"


# ── исполнители прогонов (§4.4) ───────────────────────────────────────────────

async def _check_one(db, candidate, *, fleet: set[int], report) -> dict:
    """Полный конвейер одного кандидата: карточка → живость → модель (B.3).
    Возвращает ступени, заметку для лога и — при откладывании лимитом — тройки
    `(аккаунт, действие, код)` под ключом `budget_deferrals`; прогон считает по
    ним итог и окно возврата."""
    name = (f"@{candidate.username}" if candidate.username
            else f"кандидат #{candidate.id}")
    if candidate.found_by_account_id not in fleet:
        # §7: аккаунт, запускавший поиск, обязан оставаться активным. Молча
        # сменить его нельзя — человек, запускавший поиск, обязан увидеть смену.
        return {"card": "defer", "liveness": None, "fit": None, "asked": False,
                "note": f"{name}: аккаунт {candidate.found_by_account_id} не активен "
                        f"во флоте — проверки отложены"}

    # Детализация ступеней для прогона: «спросили ли модель» (check_fit) и
    # «кого отложило лимитом» (check_card/check_liveness) — из неё run_check
    # собирает исход прогона и окно возврата бюджета.
    detail: dict = {}
    card = await check_card(db, candidate, account_id=candidate.found_by_account_id,
                            out=detail)
    if card != "pass":
        why = candidate.decision_reason if card == "reject" else "отложено"
        return _outcome({"card": card, "liveness": None, "fit": None,
                         "asked": False,
                         "note": f"{name}: карточка — {why}"}, detail)

    # Выборка для модели собирается здесь же, второй ходки в Telegram нет (§5).
    sample: list[str] = []
    liveness = await check_liveness(
        db, candidate, account_id=candidate.found_by_account_id,
        sample=sample, report=report, out=detail)
    if liveness != "pass":
        why = candidate.decision_reason if liveness == "reject" else "отложено"
        return _outcome({"card": card, "liveness": liveness, "fit": None,
                         "asked": False,
                         "note": f"{name}: живость — {why}"}, detail)

    fit = await check_fit(db, candidate, report=report, posts=sample, out=detail)
    why = ("отклонена моделью" if fit == "reject"
           else "отложена" if fit == "defer" else f"вердикт {candidate.llm_verdict}")
    return _outcome({"card": card, "liveness": liveness, "fit": fit,
                     "asked": bool(detail.get("asked")),
                     "note": f"{name}: модель — {why}"}, detail)


def _outcome(outcome: dict, detail: dict) -> dict:
    """Итог одного кандидата; пары «кого отложило лимитом» прикладываются,
    только если они есть — статистика штатного прогона остаётся прежней."""
    if detail.get("budget_deferrals"):
        outcome["budget_deferrals"] = detail["budget_deferrals"]
    return outcome


async def run_check(*, report, cancelled) -> dict:
    """Прогон `discovery_check`: все `pending` по `found_at, id`, по одному.

    Один отказ не роняет прогон (образец `discussions.scan`): приватный канал или
    флуд-контроль на списке кандидатов — обычные события, а перезапуск прогона
    перечитал бы уже проверенное.

    Исход «отложено лимитом» (R4): проверки кандидатов, отложенные Engage
    (`interpret` → deferred), собираются за прогон в тройки (аккаунт, действие,
    код) — по ним прогон **один раз** спрашивает `engage.limits()` и кладёт в
    свой результат маркер `deferred: True` (по нему `jobs.execute` ставит строке
    статус `deferred`) и окно возврата `retry_after_s` (его читает тик через
    `_waiting_budget`, чтобы не заводить новый прогон до возврата бюджета).
    Недоступность Engage, откладывание моделью и неактивный аккаунт — не лимит:
    маркера они не ставят, прогон остаётся «готово». Счётчик отложенных
    кандидатов — отдельный ключ `deferred_candidates`, не маркер.
    """
    maker = get_session_maker()
    stats = {"checked": 0, "passed_card": 0, "passed_liveness": 0, "asked_llm": 0,
             "rejected": 0, "deferred_candidates": 0, "connected": 0}
    budget_deferrals: list[tuple[int, str, str | None]] = []

    # Флот спрашивается один раз за прогон (образец `_job_discussions`): список
    # аккаунтов за минуты прогона не меняется, а запросов к Engage и так хватает.
    fleet = {a["account_id"] for a in await engage.list_accounts()
             if a.get("status") == "active"}

    async with maker() as db:
        stats["connected"] = await _connect_approved(db)
        ids = (await db.execute(
            select(ChannelCandidate.id).where(ChannelCandidate.decision == "pending")
            .order_by(ChannelCandidate.found_at, ChannelCandidate.id))
        ).scalars().all()

    total = len(ids)
    await report(0, f"кандидатов к проверке {total}, активных аккаунтов {len(fleet)}")

    for n, candidate_id in enumerate(ids, start=1):
        if cancelled():
            break
        # Своя сессия на кандидата: чужой отказ не должен травить транзакцию соседа.
        async with maker() as db:
            candidate = await db.get(ChannelCandidate, candidate_id)
            if candidate is None or candidate.decision != "pending":
                # Параллельная ручка decide уже закрыла строку — молча пропускаем.
                continue
            outcome = await _check_one(db, candidate, fleet=fleet, report=report)

        stats["checked"] += 1
        if outcome["card"] == "pass":
            stats["passed_card"] += 1
            if outcome["liveness"] == "pass":
                stats["passed_liveness"] += 1
                if outcome["asked"]:
                    stats["asked_llm"] += 1
        if "reject" in (outcome["card"], outcome["liveness"], outcome["fit"]):
            stats["rejected"] += 1
        elif "defer" in (outcome["card"], outcome["liveness"], outcome["fit"]):
            stats["deferred_candidates"] += 1
        budget_deferrals.extend(outcome.get("budget_deferrals") or [])
        await report(100.0 * n / total if total else 100.0,
                     f"[{n}/{total}] {outcome['note']}")

    stats["cancelled"] = cancelled() and stats["checked"] < total
    if budget_deferrals and not stats["cancelled"]:
        # Отменённый прогон остаётся «отменён» даже с отложенными проверками:
        # статус строки — про то, чем кончился прогон, окно подождёт следующего.
        stats["deferred"] = True
        stats["code"] = budget_deferrals[0][2]
        retry_after_s = await _check_retry_after_seconds(budget_deferrals)
        if retry_after_s is not None:
            stats["retry_after_s"] = retry_after_s
        await report(100, "проверки отложены лимитом Engage "
                     f"({stats['code']}) — "
                     + (f"возврат через {retry_after_s} с"
                        if retry_after_s is not None
                        else "окно возврата неизвестно, прежний ритм тика"))
    return stats


def _found_items(result) -> list[dict]:
    """Список найденных каналов из ответа поиска. Форма ответа Engage не
    зафиксирована (RECON, п. 1: живым вызовом проверить не удалось), поэтому
    берётся первый список словарей из известных ключей. Элемент без title
    считается в `found_total`, но строкой не становится (§3.2)."""
    if isinstance(result, list):
        return [r for r in result if isinstance(r, dict)]
    for key in ("channels", "chats", "similar", "results", "items", "posts"):
        rows = result.get(key) if isinstance(result, dict) else None
        if isinstance(rows, list):
            return [r for r in rows if isinstance(r, dict)]
    return []


async def _upsert_candidate(db, item: dict, *, kind: str,
                            seed_channel_id: int | None, account_id: int) -> bool:
    """Идемпотентная вставка кандидата по `username` (§1.1): существующей строке
    обновляются только карточные поля — `decision` и `llm_*` не трогаются, отказ
    это данные (B.2/B.7). Возвращает True, если строка заведена новой."""
    if not item.get("title"):
        return False
    username = item.get("username") or None
    if username:
        existing = (await db.execute(
            select(ChannelCandidate).where(ChannelCandidate.username == username)
        )).scalar_one_or_none()
        if existing is not None:
            if item.get("peer_id") is not None:
                existing.peer_id = item["peer_id"]
            existing.title = item["title"]
            if item.get("members") is not None:
                existing.members = item["members"]
            if item.get("chat_type"):
                existing.chat_type = item["chat_type"]
            return False
    db.add(ChannelCandidate(username=username, title=item["title"],
                            peer_id=item.get("peer_id"), members=item.get("members"),
                            chat_type=item.get("chat_type"), source=kind,
                            seed_channel_id=seed_channel_id,
                            found_by_account_id=account_id))
    return True


# Действие, которым заказывается каждый вид поиска: `resets_in_seconds` в E1
# отдаётся на действие, а TTL у ключей одного окна одинаков — поэтому окно
# ожидания читается у действия, которым заказывали, а не «у любого» (R4).
_SEARCH_ACTION = {"similar": "get_similar_channels", "search": "search_public_chats"}


def _resets_from_answer(lim: dict, account_id: int, action: str | None) -> int | None:
    """`resets_in_seconds` действия у аккаунта из ответа E1: сначала per_account,
    при отсутствии — агрегат того же действия (TTL у ключей одного окна
    одинаков). Действия в ответе нет — None."""
    for acc in lim.get("accounts") or []:
        if not isinstance(acc, dict) or acc.get("account_id") != account_id:
            continue
        for act in acc.get("actions") or []:
            if not isinstance(act, dict) or act.get("action") != action:
                continue
            for bucket in ("per_account", "aggregate"):
                value = (act.get(bucket) or {}).get("resets_in_seconds")
                if value is not None:
                    return int(value)
    return None


async def _retry_after_seconds(kind: str, account_id: int) -> int | None:
    """Сколько ждать до возврата бюджета поиска: `resets_in_seconds` действия,
    которым заказывали поиск, из ответа `GET /v1/limits` (E1). Это один из двух
    точечных опросов остатка у discovery — в момент откладывания, не на тик
    (R2/R4): тик, спрашивай он остаток каждые пять минут, сам стал бы источником
    нагрузки.

    Не время до полуночи UTC и не `_utc_day_start`: окно бюджета Engage —
    скользящие сутки от первого расхода, и эти величины не складываются.

    Опрос не удался (Engage недоступен, старая версия без маршрута) — None:
    тик вернётся к прежнему пятиминутному ритму. Молча ждать «до никогда» нельзя.
    """
    try:
        lim = await engage.limits(account_ids=[account_id])
    except engage.EngageUnavailable:
        return None
    return _resets_from_answer(lim, account_id, _SEARCH_ACTION.get(kind))


async def _check_retry_after_seconds(
        deferred: list[tuple[int, str, str | None]]) -> int | None:
    """Окно возврата для отложенных проверок (R4): один опрос `engage.limits()`
    сразу по всем аккаунтам, чьи проверки отложены, и минимальное окно среди
    отложенных пар (аккаунт, действие) — у того действия, которым проверяли.

    Минимум, а не максимум: тик обязан вернуться, как только вернётся хоть один
    нужный бюджет. Ещё исчерпанный бюджет отложит свою проверку заново — и новый
    прогон назовёт свежее окно; прогон на окно — не шторм. Опрос не удался —
    None: прежний пятиминутный ритм, молча ждать «до никогда» нельзя.
    """
    if not deferred:
        return None
    account_ids = sorted({account_id for account_id, _act, _code in deferred})
    try:
        lim = await engage.limits(account_ids=account_ids)
    except engage.EngageUnavailable:
        return None
    windows = [w for w in (_resets_from_answer(lim, account_id, action)
                           for account_id, action, _code in deferred)
               if w is not None]
    return min(windows) if windows else None


async def run_scan(run_id: int, *, params: dict, report, cancelled) -> dict:
    """Прогон `discovery_scan`: шаги §3.2 — Engage → кандидаты → строка
    `discovery_queries` с `run_id`. Проверки кандидатов прогон не стартует:
    их ведёт тик, единственная точка запуска избавляет от гонки «скан против
    тика» (§4.4).

    Поисковых заказов у Engage — не больше `discovery_queries_per_scan`
    (Решение 1: «пять поисков» — объём прогона, не суточный лимит). Потолок
    живёт здесь, в службе, а не в ручке поиска: общая `POST /runs` заводит этот
    же прогон с произвольными params и мимо ручки потолок бы обошла. Сегодня
    поиск делает ровно один заказ; счётчик с жёсткой границей — рамка для
    будущих мульти-целевых прогонов.
    """
    kind = params.get("kind")
    if kind not in DiscoveryQuery.KINDS:
        raise RuntimeError(f"неизвестный вид поиска {kind!r}")
    account_id = int(params["account_id"])
    maker = get_session_maker()

    async with maker() as db:
        per_scan = int((await thresholds(db))["discovery_queries_per_scan"])
        orders = 0

        async def order_search(action: str, payload: dict) -> dict:
            """Один поисковый заказ под жёсткой границей потолка прогона."""
            nonlocal orders
            if orders >= per_scan:
                raise RuntimeError(
                    f"прогон исчерпал потолок поисковых заказов ({per_scan})")
            orders += 1
            task = await engage.action(
                account_id=account_id, action=action, payload=payload,
                webhook_url=engage.webhook_url(kind="polled"))
            return await engage.wait_for_task(task["task_id"],
                                              timeout=SCAN_WAIT_SECONDS)

        seed_channel_id: int | None = None
        query_text: str | None = None
        if kind == "similar":
            seed_channel_id = int(params.get("seed_channel_id") or 0)
            seed = await db.get(Channel, seed_channel_id) if seed_channel_id else None
            if seed is None or not seed.username:
                raise RuntimeError(f"канал-семя {seed_channel_id} не найден или "
                                   f"без username")
            action, payload = "get_similar_channels", {"username": seed.username}
            what = f"похожие к @{seed.username}"
        else:
            query_text = (params.get("query") or "").strip()
            if not query_text:
                raise RuntimeError("пустая строка поиска")
            action, payload = "search_public_chats", {"query": query_text}
            what = f"поиск «{query_text}»"

        if cancelled():
            return {"cancelled": True, "found_total": 0, "new_total": 0}
        await report(0, f"{what}, аккаунт {account_id}")
        try:
            result = await order_search(action, payload)
        except engage.EngageTaskDeferred as e:
            # Отложено лимитом — не падение: прогон завершается штатно, а статус
            # «deferred» строке ставит `execute` по ключу в результате. Кандидаты
            # и строка `discovery_queries` не пишутся: поиска не было.
            interp = deferrals.interpret(e)
            await report(0, f"{what}: {interp.note}")
            out: dict = {"deferred": True, "code": interp.code}
            retry_after_s = await _retry_after_seconds(kind, account_id)
            if retry_after_s is not None:
                out["retry_after_s"] = retry_after_s
            return out
        items = _found_items(result)
        await report(40, f"найдено {len(items)} каналов")

        new_total = 0
        for item in items:
            if await _upsert_candidate(db, item, kind=kind,
                                       seed_channel_id=seed_channel_id,
                                       account_id=account_id):
                new_total += 1
        db.add(DiscoveryQuery(kind=kind, seed_channel_id=seed_channel_id,
                              query=query_text, account_id=account_id,
                              run_id=run_id, found_total=len(items),
                              new_total=new_total))
        try:
            # Один коммит на кандидатов и строку поиска: отказ окна уникальности
            # не должен оставить полупустой итог (лимит не съеден отказом, B3).
            await db.commit()
        except IntegrityError as e:
            await db.rollback()
            raise RuntimeError("этот запрос уже искали в текущие UTC-сутки") from e
        await report(100, f"готово: найдено {len(items)}, новых кандидатов {new_total}")
        return {"found_total": len(items), "new_total": new_total}


# ── тик (§4.4) ────────────────────────────────────────────────────────────────

async def _connect_approved(db) -> int:
    """Перевести `approved → connected` по факту существования отслеживаемого
    канала с тем же username (§3.4). `decided_by` и `decision_reason` не
    трогаются: подключение — выведенный факт, а не новое решение."""
    rows = (await db.execute(
        select(ChannelCandidate).where(ChannelCandidate.decision == "approved",
                                       ChannelCandidate.username.isnot(None))
    )).scalars().all()
    if not rows:
        return 0
    tracked = {u.lower() for u in (await db.execute(
        select(Channel.username).where(Channel.ingest_enabled.is_(True),
                                       Channel.username.isnot(None))
    )).scalars().all()}
    connected = 0
    for candidate in rows:
        if (candidate.username or "").lower() in tracked:
            candidate.decision = "connected"
            connected += 1
    if connected:
        await db.commit()
    return connected


async def discovery_check_tick(ctx: dict) -> dict:
    """Один удар проверки кандидатов (§4.4): долечить отложенное и перевести
    `approved → connected`, а если есть `pending` и прогон не идёт — завести его.

    «Прогон не идёт» — не единственное условие заводить новый (R4): если последний
    завершённый прогон отложен лимитом и назвал окно возврата (`retry_after_s`),
    которое ещё не истекло, новый не заводится — иначе каждый удар заново заказывал
    бы карточку, которую Engage снова откладывает: так выглядит шторм повторов.
    Окно не названо — прежний пятиминутный ритм.

    Бьётся каждые пять минут круглые сутки и падать не имеет права на том, что
    буднями считается погодой: исключение на ровном месте залило бы журнал
    воркера ложными отказами (образец — `backfill_drain_tick`).
    """
    # Локальный импорт: `jobs` тянет этот модуль ради RUNNERS, и импорт наверху
    # замкнул бы круг на уровне модулей. К моменту удара оба давно загружены.
    from app.services import jobs

    try:
        maker = get_session_maker()
        async with maker() as db:
            connected = await _connect_approved(db)
            pending = (await db.execute(
                select(func.count(ChannelCandidate.id))
                .where(ChannelCandidate.decision == "pending"))).scalar_one()
            started = False
            waiting_budget = False
            if pending and await jobs.active_run(db, "discovery_check") is None:
                if await _waiting_budget(db):
                    waiting_budget = True
                else:
                    await jobs.start(db, kind="discovery_check", params={},
                                     name="Проверка кандидатов Discovery",
                                     user_email="auto:tick")
                    started = True
    except Exception as e:  # noqa: BLE001 — тик не вправе уронить воркера приёма
        logger.warning("discovery_check_tick_failed error=%s", e)
        return {"connected": 0, "pending": 0, "started": False,
                "waiting_budget": False}
    logger.info("discovery_check_tick connected=%s pending=%s started=%s "
                "waiting_budget=%s", connected, pending, started, waiting_budget)
    return {"connected": connected, "pending": pending, "started": started,
            "waiting_budget": waiting_budget}


async def _waiting_budget(db) -> bool:
    """Последний завершённый прогон проверок отложен лимитом и назвал окно
    (`retry_after_s`), которое ещё не истекло (R4).

    Читается последний завершённый run вида `discovery_check` — не «любой
    deferred»: более поздний обычный прогон отменяет старое окно само собой.
    Окна нет (Engage был недоступен в момент откладывания и опрос остатка не
    удался) — False: молча ждать «до никогда» нельзя, работает прежний ритм.
    """
    last = (await db.execute(
        select(Run).where(Run.kind == "discovery_check",
                          Run.finished_at.is_not(None))
        .order_by(Run.finished_at.desc(), Run.id.desc())
        .limit(1))).scalar_one_or_none()
    if last is None or last.status != "deferred":
        return False
    seconds = (last.result or {}).get("retry_after_s")
    try:
        seconds = int(seconds)
    except (TypeError, ValueError):
        return False
    return clock.utcnow() < last.finished_at + timedelta(seconds=seconds)
