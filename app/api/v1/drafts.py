"""Очередь черновиков — ключевой экран продукта.

Здесь человек одобряет или отклоняет каждое сообщение до отправки. Именно этот экран
делает сухой прогон осмысленным: без него «прогон» — это просто выключенная отправка.

Три вещи, ради которых ручки вынесены из `screens.py` в отдельный модуль:

1. **Одобрение обязано пройти через `OutboundGate`.** Не «мы потом подключим гейт», а
   прямо сейчас: одобрение вызывает `gate.evaluate()` и возвращает вердикт экрану.
   Оператор видит своими глазами, что после его «одобрить» отправка всё равно
   заблокирована режимом — и видит это на живом коде, а не с наших слов.
2. **Гейт собран без клиента Engage намеренно** (`engage_client=None`). `evaluate()`
   сети не касается, а если кто-нибудь однажды позовёт здесь `send()`, он получит
   падение вместо отправленного сообщения. Для мок-ручки это ровно та асимметрия,
   которая нужна.
3. **Очередь курсорная, а не списочная.** Экран показывает один черновик и двигается
   по очереди, поэтому ручка отвечает «следующий после указанного», а не выдачей.

Данные пока фиксированные (реальные появятся после ингеста Э5), а решения по ним
живут в памяти процесса: `_DECISIONS` сбрасывается при рестарте и не разделяется
между воркерами. Это осознанная времянка — таблица `drafts` уже есть в моделях,
писать в неё нечего, пока в ней нет строк из ингеста.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel, Field

from app.api.deps import GetDB, requires
from app.api.v1.system import current_mode
from app.core import clock
from app.core.access import Section
from app.core.outbound_gate import OutboundGate, SendRequest
from app.db.models import AuditLog

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/drafts", tags=["drafts"])


# ── справочник причин ─────────────────────────────────────────────────────────

REASONS = [
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
_REASON_BY_N = {r["n"]: r["label"] for r in REASONS}


# ── очередь ───────────────────────────────────────────────────────────────────

# `context` — факты о получателе, которые нужны гейту. В моке они заданы руками;
# после ингеста возьмутся из диалога и профиля канала.
QUEUE: list[dict] = [
    {
        "id": 901, "lead_id": 4821,
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
             "text": "«ребят, задолбался с текущим хостингом, тормозит жутко, "
                     "кто может посоветовать замену?»", "target": True},
            {"meta": "15:44 · сосед", "text": "«+1, тоже думаю»", "target": False},
        ],
        "variants": [
            {"text": "«Видел твой вопрос про хостинг в VPS & Hosting Talk — обратись к "
                     "Андрею (@vertsanov_biz), мне он за день поднял новый сервер на "
                     "Remnawave, полёт нормальный»",
             "spam_score": 0.12, "prompt_version": "v3", "lint_ok": True,
             "critic_passed": True,
             "critic_text": "Звучит нативно, как реальная рекомендация от знакомого, "
                            "не как реклама"},
            {"text": "«Привет! По хостингу — знакомый Андрей (@vertsanov_biz) занимается "
                     "инфраструктурой, переносил меня без простоя. Напиши ему, если "
                     "ищешь замену»",
             "spam_score": 0.18, "prompt_version": "v3", "lint_ok": True,
             "critic_passed": True,
             "critic_text": "Чуть более обобщённо, чем вариант 1, но без рекламных штампов"},
            {"text": "«Хостинг тормозит? Андрей (@vertsanov_biz) решает это под ключ за "
                     "1 день, гарантия аптайма 99.9%, пиши прямо сейчас!»",
             "spam_score": 0.61, "prompt_version": "v3", "lint_ok": False,
             "critic_passed": False,
             "critic_text": "Читается как реклама: обещание гарантии и призыв к действию"},
        ],
        "source_message_link": "https://t.me/c/1923847561/88213",
        "context": {"conversation_id": 0, "account_id": 1, "peer_id": 1923847561,
                    "sent_count": 0, "last_sent_at": None, "recipient_local_hour": 15,
                    "recipient_is_admin": False, "previously_contacted": False},
    },
    {
        "id": 902, "lead_id": 4822,
        "author_name": "Игорь С.", "author_username": "@igor_secops",
        "channel": "Информационная безопасность", "pain": "VPN постоянно отваливается",
        "score": 92,
        "score_breakdown": [
            {"label": "совпадение с болью", "value": 36},
            {"label": "срочность/интент", "value": 26},
            {"label": "признаки ЛПР", "value": 14},
            {"label": "свежесть", "value": 11},
            {"label": "достижимость в ЛС", "value": 5},
        ],
        "thread": [
            {"meta": "14:50 · корневой пост",
             "text": "«Удалёнка и корпоративный доступ — что используете?»", "target": False},
            {"meta": "15:33 · Игорь С. — кандидат",
             "text": "«у меня VPN постоянно отваливается на удалёнке, задрало»", "target": True},
            {"meta": "15:36 · сосед",
             "text": "«у нас та же беда, ждём решения от админов»", "target": False},
        ],
        "variants": [
            {"text": "«Видел твоё сообщение про VPN — у меня было то же самое, пока Андрей "
                     "(@vertsanov_biz) не поднял свой на Remnawave. С тех пор не отваливается»",
             "spam_score": 0.09, "prompt_version": "v3", "lint_ok": True,
             "critic_passed": True,
             "critic_text": "Личный опыт как основа рекомендации, без давления"},
            {"text": "«Если VPN рвётся — это обычно провайдер, а не клиент. Андрей "
                     "(@vertsanov_biz) настраивает self-hosted, могу передать контакт»",
             "spam_score": 0.14, "prompt_version": "v3", "lint_ok": True,
             "critic_passed": True,
             "critic_text": "Полезный контекст перед рекомендацией, тон нейтральный"},
            {"text": "«Приветствую! Предлагаем настройку корпоративного VPN под ключ. "
                     "Подробности по ссылке t.me/vertsanov_biz»",
             "spam_score": 0.74, "prompt_version": "v3", "lint_ok": False,
             "critic_passed": False,
             "critic_text": "Ссылка в первом сообщении и продающий тон"},
        ],
        "source_message_link": "https://t.me/c/1923847998/44120",
        "context": {"conversation_id": 0, "account_id": 2, "peer_id": 1923847998,
                    "sent_count": 0, "last_sent_at": None, "recipient_local_hour": 15,
                    "recipient_is_admin": False, "previously_contacted": False},
    },
    {
        "id": 903, "lead_id": 4824,
        "author_name": "Марина Л.", "author_username": "@marina_l",
        "channel": "VPS & Hosting Talk", "pain": "не умеет настроить 3x-ui", "score": 81,
        "score_breakdown": [
            {"label": "совпадение с болью", "value": 30},
            {"label": "срочность/интент", "value": 22},
            {"label": "признаки ЛПР", "value": 12},
            {"label": "свежесть", "value": 11},
            {"label": "достижимость в ЛС", "value": 6},
        ],
        "thread": [
            {"meta": "15:10 · корневой пост",
             "text": "«Конфиги и подводные камни self-hosted решений»", "target": False},
            {"meta": "15:21 · Марина Л. — кандидат",
             "text": "«кто-нибудь поднимал 3x-ui на дебиане? запутался в конфигах»",
             "target": True},
        ],
        "variants": [
            {"text": "«С 3x-ui на дебиане сам мучился — в итоге позвал Андрея "
                     "(@vertsanov_biz), он развернул за вечер и объяснил, что где лежит»",
             "spam_score": 0.11, "prompt_version": "v3", "lint_ok": True,
             "critic_passed": True,
             "critic_text": "Разговорный тон, конкретика без обещаний"},
            {"text": "«По 3x-ui: чаще всего дело в правах и портах. Если не хочется "
                     "копаться — Андрей (@vertsanov_biz) настраивает такое под ключ»",
             "spam_score": 0.16, "prompt_version": "v3", "lint_ok": True,
             "critic_passed": True,
             "critic_text": "Даёт пользу до рекомендации — хорошо"},
            {"text": "«ЗДРАВСТВУЙТЕ!!! Настроим 3X-UI за 1 ДЕНЬ, недорого, пишите!»",
             "spam_score": 0.88, "prompt_version": "v3", "lint_ok": False,
             "critic_passed": False,
             "critic_text": "Капс, восклицания, штампы — очевидная реклама"},
        ],
        "source_message_link": "https://t.me/c/1923847561/88377",
        "context": {"conversation_id": 0, "account_id": 1, "peer_id": 1923847561,
                    "sent_count": 0, "last_sent_at": None, "recipient_local_hour": 15,
                    "recipient_is_admin": False, "previously_contacted": False},
    },
]

_BY_ID = {d["id"]: d for d in QUEUE}

# draft_id → решение оператора. См. оговорку про времянку в шапке модуля.
_DECISIONS: dict[int, dict] = {}


def _public(draft: dict) -> dict:
    """Черновик без служебного контекста: экрану он не нужен, а в ответе только шумит."""
    return {k: v for k, v in draft.items() if k != "context"}


def _pending() -> list[dict]:
    return [d for d in QUEUE if d["id"] not in _DECISIONS]


def _pick(pending: list[dict], after: int | None) -> dict | None:
    """Курсор по очереди: первый неразобранный с id больше указанного.

    Дойдя до конца, заворачиваем на начало — это поведение клавиши J в интерфейсе:
    оператор жмёт её не останавливаясь, и упереться в невидимую стену там неоткуда.
    Функция чистая, чтобы курсор проверялся тестом без БД и без HTTP.
    """
    if not pending:
        return None
    if after is None:
        return pending[0]
    return next((d for d in pending if d["id"] > after), pending[0])


# ── чтение ────────────────────────────────────────────────────────────────────

@router.get("/next")
async def next_draft(after: int | None = None, user=requires(Section.DRAFTS)):
    """Следующий неразобранный черновик.

    `remaining` считается до выдачи и нужен оператору: это единственный экран, где он
    сидит подолгу, и объём оставшейся работы должен быть виден без перехода на дашборд.

    Когда очередь разобрана, отдаём `draft: null`, а не 404: пустая очередь — это
    нормальное состояние экрана, а не ошибка запроса.
    """
    pending = _pending()
    nxt = _pick(pending, after)
    return {"remaining": len(pending), "draft": _public(nxt) if nxt else None}


@router.get("/reasons")
async def reject_reasons(user=requires(Section.DRAFTS)):
    """Справочник причин отклонения. Список закрытый: причина уходит в eval-датасет,
    на котором меряется качество генерации, и свободный текст его размывает."""
    return REASONS


# ── решения ───────────────────────────────────────────────────────────────────

class ApproveRequest(BaseModel):
    variant_index: int = Field(ge=0)
    text: str | None = None  # правка оператора; None — берём вариант как есть


class RejectRequest(BaseModel):
    reason_n: int = Field(ge=1, le=9)


def _draft_or_404(draft_id: int) -> dict:
    draft = _BY_ID.get(draft_id)
    if draft is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"черновик {draft_id} не найден")
    return draft


def _not_decided_or_409(draft_id: int) -> None:
    prev = _DECISIONS.get(draft_id)
    if prev:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"по черновику {draft_id} уже принято решение «{prev['decision']}»",
        )


async def _evaluate(draft: dict, text: str, db) -> dict:
    """Прогнать одобренный текст через гейт, ничего не отправляя.

    Гейт спрашивает режим у БД в момент вызова, поэтому вердикт честный: если кто-то
    переключит систему в LIVE, здесь это станет видно сразу же.
    """
    ctx = draft["context"]
    gate = OutboundGate(engage_client=None, mode_provider=lambda: current_mode(db),
                        journal=None)
    req = SendRequest(
        draft_id=draft["id"], conversation_id=ctx["conversation_id"],
        account_id=ctx["account_id"], recipient_peer_id=ctx["peer_id"],
        text=text, draft_state="approved",
        is_first_message=ctx["sent_count"] == 0,
        sent_count=ctx["sent_count"], last_sent_at=ctx["last_sent_at"],
        recipient_local_hour=ctx["recipient_local_hour"],
        recipient_is_admin=ctx["recipient_is_admin"],
        previously_contacted=ctx["previously_contacted"],
    )
    verdict = await gate.evaluate(req, clock.utcnow())
    return {"allowed": verdict.allowed, "reasons": verdict.reasons}


@router.post("/{draft_id}/approve")
async def approve(draft_id: int, body: ApproveRequest, request: Request, db: GetDB,
                  user=requires(Section.DRAFTS)):
    """Одобрить вариант — при необходимости с правкой текста.

    Правка и одобрение — одна ручка, потому что в интерфейсе это одно действие:
    оператор правит текст и тем самым его одобряет. Разделять их значило бы позволить
    состояние «текст поправлен, но не одобрен», которого на экране не существует.
    """
    draft = _draft_or_404(draft_id)
    _not_decided_or_409(draft_id)

    if body.variant_index >= len(draft["variants"]):
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            f"вариант {body.variant_index} не существует "
            f"(их {len(draft['variants'])})",
        )

    original = draft["variants"][body.variant_index]["text"]
    edited = body.text is not None and body.text.strip() != original
    text = body.text.strip() if body.text is not None else original
    if not text:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "пустой текст сообщения")

    send = await _evaluate(draft, text, db)

    _DECISIONS[draft_id] = {"decision": "approved", "variant_index": body.variant_index,
                            "text": text, "edited": edited, "by": user.email}
    db.add(AuditLog(
        user_id=user.id, user_email=user.email, action="draft_approve",
        detail={"draft_id": draft_id, "variant_index": body.variant_index,
                "edited": edited, "send_allowed": send["allowed"],
                "send_reasons": send["reasons"]},
        ip=request.client.host if request.client else None,
    ))
    await db.commit()
    logger.info("draft_approved draft=%s by=%s edited=%s send_allowed=%s",
                draft_id, user.email, edited, send["allowed"])

    return {"draft_id": draft_id, "decision": "approved",
            "variant_index": body.variant_index, "edited": edited,
            "send": send, "remaining": len(_pending())}


@router.post("/{draft_id}/reject")
async def reject(draft_id: int, body: RejectRequest, request: Request, db: GetDB,
                 user=requires(Section.DRAFTS)):
    """Отклонить с типизированной причиной.

    Причина — только из справочника: она попадает в eval-датасет, по которому меряется
    качество генерации, и свободный текст сделал бы этот датасет неразбираемым.
    """
    draft = _draft_or_404(draft_id)
    _not_decided_or_409(draft_id)

    label = _REASON_BY_N.get(body.reason_n)
    if label is None:  # ge/le уже отсеяли диапазон, это страховка от расхождения справочника
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            f"причина {body.reason_n} отсутствует в справочнике")

    _DECISIONS[draft_id] = {"decision": "rejected", "reason_n": body.reason_n,
                            "reason": label, "by": user.email}
    db.add(AuditLog(
        user_id=user.id, user_email=user.email, action="draft_reject",
        detail={"draft_id": draft_id, "reason_n": body.reason_n, "reason": label},
        ip=request.client.host if request.client else None,
    ))
    await db.commit()
    logger.info("draft_rejected draft=%s by=%s reason=%s", draft_id, user.email, label)

    return {"draft_id": draft_id, "decision": "rejected", "reason_n": body.reason_n,
            "reason": label, "remaining": len(_pending())}
