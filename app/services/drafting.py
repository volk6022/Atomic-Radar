"""Сборка черновиков ответа по лиду.

**Здесь нет модели.** Варианты собираются по шаблонам, и это записано в самих данных:
`prompt_version = "template-v0"`, а в поле самокритики прямым текстом сказано, что
проверки моделью не было. Выдать шаблон за вывод LLM было бы худшим из возможных
решений: оператор ревьюит очередь как раз для того, чтобы поймать плохую генерацию,
и если он поверит, что тексты уже проверены критиком, ревью превратится в формальность.

Когда появится L3 и генератор на модели, поменяется способ получения `variants` —
всё остальное (очередь, ревью, гейт) уже рассчитано на итоговую форму.
"""
from __future__ import annotations

import logging

from sqlalchemy import select

from app.db.models import Channel, Draft, Lead, Message

logger = logging.getLogger(__name__)

PROMPT_VERSION = "template-v0"

# Заготовки под боли из каскада. Первое лицо и разговорный тон — по требованию
# заказчика: сообщение должно читаться как рекомендация знакомого, а не как реклама.
TEMPLATES: dict[str, tuple[str, ...]] = {
    "хостинг тормозит/дорог": (
        "Видел твоё сообщение про хостинг — у меня было похоже, пока не перенёс всё "
        "к Андрею (@vertsanov_biz). Развернул за день, с тех пор тихо.",
        "По хостингу: если не хочется копаться самому, Андрей (@vertsanov_biz) "
        "занимается переносами без простоя. Могу передать контакт.",
        "Сам недавно съезжал с такого же. Помог Андрей (@vertsanov_biz) — сделал "
        "и объяснил, что где лежит.",
    ),
    "VPN не работает": (
        "Видел про VPN — у меня было то же самое, пока Андрей (@vertsanov_biz) "
        "не поднял свой на Remnawave. С тех пор не отваливается.",
        "Если VPN рвётся — это чаще провайдер, а не клиент. Андрей (@vertsanov_biz) "
        "настраивает self-hosted, могу дать контакт.",
        "У нас была такая же беда, решилось своим сервером. Настраивал "
        "Андрей (@vertsanov_biz), если интересно.",
    ),
    "не может настроить сам": (
        "Сам мучился с этим же — в итоге позвал Андрея (@vertsanov_biz), он развернул "
        "за вечер и показал, что к чему.",
        "Тут обычно дело в правах и портах. Если не хочется разбираться, "
        "Андрей (@vertsanov_biz) настраивает такое под ключ.",
        "Могу подсказать, кто это быстро закрывает — Андрей (@vertsanov_biz), "
        "мне он похожую конфигурацию поднимал.",
    ),
    "нужен админ/подрядчик": (
        "По админу: Андрей (@vertsanov_biz) занимается серверами и настройкой, "
        "мне делал — вопросов не было.",
        "Если ещё ищешь — Андрей (@vertsanov_biz). Работал с ним по инфраструктуре, "
        "делает и объясняет.",
        "Могу порекомендовать Андрея (@vertsanov_biz), он как раз про сервера "
        "и панели.",
    ),
}

FALLBACK = (
    "Видел твоё сообщение — по такой задаче помогает Андрей (@vertsanov_biz), "
    "мне он поднимал похожее.",
)

CRITIC_NOTE = ("Проверки моделью не было: вариант собран по шаблону "
               "(prompt_version=template-v0). Читайте текст сами.")


def _spam_score(text: str) -> float:
    """Грубая оценка «похоже на рекламу». Считается по признакам, которые видно
    глазом, — иначе число выглядело бы как вывод модели, которой здесь нет."""
    score = 0.10
    lowered = text.lower()
    if any(w in lowered for w in ("гарантия", "под ключ", "недорого", "прямо сейчас")):
        score += 0.25
    if text.count("!") > 1:
        score += 0.20
    if any(c.isupper() for c in text) and sum(c.isupper() for c in text) > len(text) * 0.2:
        score += 0.25
    if "http" in lowered or "t.me/" in lowered:
        score += 0.30
    return round(min(score, 0.99), 2)


def build_variants(pain: str | None) -> list[dict]:
    texts = TEMPLATES.get(pain or "", FALLBACK)
    out = []
    for t in texts:
        spam = _spam_score(t)
        out.append({
            "text": t, "spam_score": spam, "prompt_version": PROMPT_VERSION,
            # Линтер политики — настоящая проверка, в отличие от «критика»:
            # ссылку в первом сообщении и капс он ловит по тексту.
            "lint_ok": spam < 0.4 and "t.me/" not in t.lower(),
            "critic_passed": None,
            "critic_text": CRITIC_NOTE,
        })
    return out


async def thread_context(db, message: Message, *, around: int = 2) -> list[dict]:
    """Несколько сообщений вокруг триггера — чтобы ревьюер понимал, кому пишем.

    Соседство определяется по id сообщения в чате: он монотонный, а `reply_to`
    заполнен далеко не у всех реплик.
    """
    rows = (await db.execute(
        select(Message)
        .where(Message.channel_id == message.channel_id,
               Message.tg_message_id.between(message.tg_message_id - around * 6,
                                             message.tg_message_id + around * 6))
        .order_by(Message.tg_message_id))).scalars().all()

    out = []
    for m in rows:
        stamp = m.tg_date.strftime("%d.%m %H:%M")
        who = m.author_name or (("@" + m.author_username) if m.author_username else "—")
        out.append({
            "meta": f"{stamp} · {who}" + (" — кандидат" if m.id == message.id else ""),
            "text": "«" + (m.text or "")[:300] + "»",
            "target": m.id == message.id,
        })
    return out


async def ensure_draft(db, lead: Lead) -> Draft:
    """Черновик по лиду — ровно один, создаётся при первом обращении к очереди."""
    draft = (await db.execute(
        select(Draft).where(Draft.lead_id == lead.id))).scalar_one_or_none()
    if draft is not None:
        return draft

    message = (await db.execute(
        select(Message).where(Message.id == lead.message_id))).scalar_one()
    channel = (await db.execute(
        select(Channel).where(Channel.id == lead.channel_id))).scalar_one()

    link = None
    if channel.username:
        link = f"https://t.me/{channel.username}/{message.tg_message_id}"
    elif channel.peer_id:
        # Внутренняя ссылка для приватных супергрупп: -100 в начале peer_id — префикс
        # канала, в t.me-ссылке его нет.
        link = f"https://t.me/c/{str(channel.peer_id).replace('-100', '', 1)}/{message.tg_message_id}"

    draft = Draft(
        lead_id=lead.id, variants=build_variants(lead.pain),
        thread_context=await thread_context(db, message),
        state="pending", prompt_version=PROMPT_VERSION, source_message_link=link,
    )
    db.add(draft)
    await db.flush()
    logger.info("draft_created draft=%s lead=%s pain=%s", draft.id, lead.id, lead.pain)
    return draft
