"""Правила, которые не должны нарушаться никогда — один модуль на тесты и рантайм.

Написан отдельно и без зависимостей от БД и FastAPI намеренно. Если инвариант живёт
только в тестах, продакшен его не соблюдает; если только в рантайме — его нельзя
проверить на наборе сценариев. Здесь он один и тот же объект, который зовут обе стороны.

Каждая функция возвращает `None`, когда всё хорошо, и строку с причиной, когда нет.
Причина попадает и в лог, и в отчёт сценария, поэтому формулируется по-человечески.
"""
from __future__ import annotations

from datetime import datetime, timedelta

# Из `conversation-scenarios.md`. Значения — стартовые, настраиваются в Safety.
MAX_MESSAGES_PER_CONVERSATION = 4
MIN_GAP_BETWEEN_MESSAGES = timedelta(hours=20)
MAX_LINKS_IN_FIRST_MESSAGE = 0
QUIET_HOURS = (0, 8)  # локальные часы собеседника, когда писать нельзя


def not_in_dry_run(mode: str) -> str | None:
    """Единственная проверка, ради которой существует OutboundGate."""
    if mode != "LIVE":
        return f"режим {mode}: отправка запрещена"
    return None


def draft_is_approved(state: str) -> str | None:
    """Человек одобрил именно этот текст. Без этого шага весь смысл сухого прогона теряется."""
    if state != "approved":
        return f"черновик в состоянии «{state}», а не «approved»"
    return None


def no_link_in_first_message(text: str, is_first: bool) -> str | None:
    """Ссылка в первом сообщении незнакомому человеку — самый быстрый способ получить
    репорт в спам. Это же причина отклонения №8 в очереди черновиков."""
    if not is_first:
        return None
    lowered = text.lower()
    if any(m in lowered for m in ("http://", "https://", "t.me/", "www.")):
        return "ссылка в первом сообщении"
    return None


def within_message_cap(sent_count: int) -> str | None:
    """Больше четырёх сообщений без ответа — это уже преследование, а не диалог."""
    if sent_count >= MAX_MESSAGES_PER_CONVERSATION:
        return f"исчерпан лимит сообщений в диалоге ({sent_count}/{MAX_MESSAGES_PER_CONVERSATION})"
    return None


def gap_respected(last_sent_at: datetime | None, now: datetime) -> str | None:
    """Два сообщения подряд в одну минуту выдают бота вернее любого текста."""
    if last_sent_at is None:
        return None
    waited = now - last_sent_at
    if waited < MIN_GAP_BETWEEN_MESSAGES:
        return f"с прошлого сообщения прошло {waited}, нужно ≥ {MIN_GAP_BETWEEN_MESSAGES}"
    return None


def outside_quiet_hours(local_hour: int) -> str | None:
    """Ночное сообщение читается как рассылка независимо от текста."""
    start, end = QUIET_HOURS
    if start <= local_hour < end:
        return f"тихие часы: {local_hour}:00 попадает в {start}:00-{end}:00"
    return None


def recipient_not_admin(is_admin: bool) -> str | None:
    """Админу группы писать нельзя: он забанит аккаунт и снимет весь канал разом."""
    if is_admin:
        return "получатель — админ или модератор канала"
    return None


def not_already_contacted(previously_contacted: bool) -> str | None:
    """Второй заход к тому же человеку от другого аккаунта — самое заметное, что можно сделать."""
    if previously_contacted:
        return "этому человеку уже писали"
    return None


def check_all(
    *,
    mode: str,
    draft_state: str,
    text: str,
    is_first: bool,
    sent_count: int,
    last_sent_at: datetime | None,
    now: datetime,
    local_hour: int,
    recipient_is_admin: bool,
    previously_contacted: bool,
) -> list[str]:
    """Все нарушения разом, а не первое попавшееся.

    Возвращать список, а не первую ошибку, — сознательно: оператору нужно увидеть
    все причины сразу, иначе он чинит их по одной и каждый раз получает новый отказ.
    """
    checks = (
        not_in_dry_run(mode),
        draft_is_approved(draft_state),
        no_link_in_first_message(text, is_first),
        within_message_cap(sent_count),
        gap_respected(last_sent_at, now),
        outside_quiet_hours(local_hour),
        recipient_not_admin(recipient_is_admin),
        not_already_contacted(previously_contacted),
    )
    return [c for c in checks if c is not None]
