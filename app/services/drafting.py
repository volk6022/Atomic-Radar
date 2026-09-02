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
    "не может оплатить за рубеж": (
        "Видел твоё сообщение про оплату за рубеж — у нас была та же история, пока "
        "не начали проводить через Андрея (@vertsanov_biz). Платим рублями изнутри, "
        "документы для бухгалтерии обычные.",
        "По зарубежному счёту: если не хочется открывать валютный, Андрей "
        "(@vertsanov_biz) закрывает такие платежи. Могу передать контакт.",
        "Сам недавно упирался в то же самое. Помог Андрей (@vertsanov_biz) — "
        "и провёл, и объяснил, как это ложится в учёт.",
    ),
    "банк не пропускает платёж": (
        "Знакомая история с валютным контролем. У нас это закрылось тем, что платёж "
        "провели не через банк напрямую — Андрей (@vertsanov_biz) занимается этим.",
        "Если банк требует контракт на учёте ради разового платежа — обычно проще "
        "обойти этот путь целиком. Андрей (@vertsanov_biz) так делает, могу дать контакт.",
        "У нас платёж возвращали дважды, пока не сменили схему. Помогал "
        "Андрей (@vertsanov_biz), если интересно, как именно.",
    ),
    "выплаты людям за границей": (
        "По выплатам за границу: мы платим через Андрея (@vertsanov_biz) — деньги "
        "уходят человеку, а у компании остаются обычные рублёвые документы.",
        "Если исполнитель нерезидент, с расчётного счёта это боль. Андрей "
        "(@vertsanov_biz) закрывает такие выплаты, могу передать контакт.",
        "Была ровно такая задача с релокантами. Решилось через Андрея "
        "(@vertsanov_biz), бухгалтерия вопросов не задавала.",
    ),
    "оплата рекламы и западных сервисов": (
        "Видел про кабинет — мы пополняем через Андрея (@vertsanov_biz), от компании "
        "и с закрывающими. Кабинет не останавливается.",
        "По оплате западных сервисов: если карта не проходит, это решается "
        "посредником. Андрей (@vertsanov_biz) как раз этим занимается.",
        "У нас так же вставала реклама. Помог Андрей (@vertsanov_biz) — "
        "пополнение проходит, документы приходят.",
    ),
    "ищет, через кого платить": (
        "По твоему вопросу: Андрей (@vertsanov_biz) проводит платежи за рубеж, "
        "нам делал — вопросов не было.",
        "Если ещё ищешь — Андрей (@vertsanov_biz). Работали с ним по оплате "
        "иностранных счетов, всё с документами.",
        "Могу порекомендовать Андрея (@vertsanov_biz), он как раз про платежи "
        "за границу и закрывающие для бухгалтерии.",
    ),
    "нет валютного счёта или контракта": (
        "Открывать валютный счёт ради одного платежа — так себе затея. Мы платим "
        "через Андрея (@vertsanov_biz), рублями и без контракта с иностранцем.",
        "Если ВЭД разово, а не постоянно, обычно дешевле не заводить его вовсе. "
        "Андрей (@vertsanov_biz) закрывает такие платежи, могу дать контакт.",
        "Сами через это проходили. Помог Андрей (@vertsanov_biz) — платёж ушёл, "
        "а у нас остался обычный рублёвый пакет документов.",
    ),
}

FALLBACK = (
    "Видел твоё сообщение — по такой задаче помогает Андрей (@vertsanov_biz), "
    "мне он поднимал похожее.",
)

CRITIC_NOTE = ("Проверки моделью не было: вариант собран по шаблону "
               "(prompt_version=template-v0). Читайте текст сами.")


def spam_score(text: str) -> float:
    """Грубая оценка «похоже на рекламу». Считается по признакам, которые видно
    глазом, — иначе число выглядело бы как вывод модели, которой здесь нет.

    Имя публичное: той же оценкой пользуется генератор черновиков по целям сценариев
    (`wf_drafting`). Своя копия этой арифметики означала бы, что один и тот же текст
    получает разные оценки в двух очередях.
    """
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
        spam = spam_score(t)
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


def message_link(channel: Channel, message: Message) -> str | None:
    """Ссылка на исходное сообщение — чтобы ревьюер открыл ветку, а не верил цитате.

    Вынесено из `ensure_draft`, потому что тем же самым занят генератор черновиков
    по целям сценариев (`wf_drafting`). Две копии этой арифметики разошлись бы на
    первой же правке, и разошлись бы молча: ссылка ведёт «куда-то», и что она ведёт
    не туда, видно только человеку, который по ней пошёл.
    """
    if channel.username:
        return f"https://t.me/{channel.username}/{message.tg_message_id}"
    if channel.peer_id:
        # Внутренняя ссылка для приватных супергрупп: -100 в начале peer_id — префикс
        # канала, в t.me-ссылке его нет.
        return (f"https://t.me/c/{str(channel.peer_id).replace('-100', '', 1)}"
                f"/{message.tg_message_id}")
    return None


async def source_links(db, channel: Channel, message: Message) -> dict:
    """Две ссылки на источник и пометка «это комментарий»: где лежит сообщение и под
    каким постом канала.

    Лид в публичном сценарии — комментарий под постом канала. Ссылка на сам
    комментарий открывает ГРУППУ ОБСУЖДЕНИЯ, и человек, который в ней не состоит,
    видит чужой чат без контекста. Поэтому черновику нужны обе ссылки — на комментарий
    и на пост, — плюс `is_comment`, чтобы по одной записи отличить одно от другого.

    Комментарий опознаётся по корню ветки: у сообщения есть `thread_id`, и в том же
    канале лежит сообщение с `tg_message_id == thread_id` и
    `is_automatic_forward = True` — пост, отзеркаленный в обсуждение. Номер поста
    внутри канала берётся с ЭТОГО корня (`forward_from_message_id`), не с комментария.
    У сообщений, приехавших до правки Engage, номера нет — и тогда `post_link` остаётся
    None, а не собирается наугад: подстановка номера корня уводила бы на чужой пост,
    потому что нумерация в канале и в группе разная. Неверная ссылка хуже отсутствующей.
    """
    comment_link = message_link(channel, message)

    is_comment = False
    post_link = None
    post_channel = None

    if message.thread_id is not None:
        root = (await db.execute(
            select(Message).where(
                Message.channel_id == message.channel_id,
                Message.tg_message_id == message.thread_id,
                Message.is_automatic_forward.is_(True),
            ))).scalar_one_or_none()
        if root is not None:
            is_comment = True
            if root.forward_from_chat_id is not None:
                origin = (await db.execute(
                    select(Channel).where(
                        Channel.peer_id == root.forward_from_chat_id)
                )).scalar_one_or_none()
                if origin is not None:
                    post_channel = origin.title
                if root.forward_from_message_id is not None:
                    # Арифметика ссылки одна — та же, что для комментария: канал
                    # с юзернеймом получает t.me/<username>/<id>, без — внутреннюю
                    # t.me/c/<id>/<msg>. Канал-источник может и не быть в реестре
                    # (пост отзеркален из чужого канала), тогда имени нет и ссылка
                    # строится по peer_id, как для приватной группы.
                    origin_channel = Channel(
                        peer_id=root.forward_from_chat_id,
                        username=origin.username if origin else None,
                        title=post_channel or "",
                    )
                    origin_message = Message(
                        tg_message_id=root.forward_from_message_id)
                    post_link = message_link(origin_channel, origin_message)

    return {
        "comment_link": comment_link,
        "post_link": post_link,
        "is_comment": is_comment,
        "post_channel": post_channel,
    }


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

    link = message_link(channel, message)

    draft = Draft(
        lead_id=lead.id, variants=build_variants(lead.pain),
        thread_context=await thread_context(db, message),
        state="pending", prompt_version=PROMPT_VERSION, source_message_link=link,
    )
    db.add(draft)
    await db.flush()
    logger.info("draft_created draft=%s lead=%s pain=%s", draft.id, lead.id, lead.pain)
    return draft
