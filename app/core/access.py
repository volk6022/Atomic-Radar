"""Матрица прав — серверная копия того, что оболочка GUI использует для меню.

В интерфейсе `ACCESS` только прячет пункты навигации. Это удобство, а не защита:
пункт можно не показать, но запрос-то отправить никто не мешает. Поэтому таблица
продублирована здесь и проверяется на каждой ручке — источник истины именно этот файл,
а не фронтенд.

Значения дословно совпадают с `ACCESS` в `Atomic Radar.dc.html` (см.
`contract/Atomic-Radar.md`). Если правишь тут — правь и там, иначе у пользователя
появится пункт меню, который отдаёт 403.
"""
from __future__ import annotations

from enum import StrEnum


class Role(StrEnum):
    OWNER = "owner"
    CUSTOMER = "customer"
    REVIEWER = "reviewer"
    VIEWER = "viewer"


class Section(StrEnum):
    DASHBOARD = "dashboard"
    FLEET = "fleet"
    CHANNELS = "channels"
    STREAM = "stream"
    LEADS = "leads"
    DRAFTS = "drafts"
    CONVERSATIONS = "conversations"
    PROFILE = "profile"
    RUNS = "runs"
    EVALS = "evals"
    ATTRIBUTION = "attribution"
    OBSERVABILITY = "observability"
    SAFETY = "safety"
    ADMIN = "admin"


_ALL = frozenset({Role.OWNER, Role.CUSTOMER, Role.REVIEWER, Role.VIEWER})
_STAFF = frozenset({Role.OWNER, Role.CUSTOMER, Role.REVIEWER})
_OWNER = frozenset({Role.OWNER})

ACCESS: dict[Section, frozenset[Role]] = {
    Section.DASHBOARD: _ALL,
    Section.FLEET: _OWNER,
    Section.CHANNELS: _STAFF,
    Section.STREAM: _STAFF,
    Section.LEADS: _STAFF,
    Section.DRAFTS: _STAFF,
    Section.CONVERSATIONS: _STAFF,
    Section.PROFILE: _STAFF,
    Section.RUNS: _OWNER,
    Section.EVALS: _STAFF,
    # Заказчик видит свою экономику, гость — только её (это витрина для инвестора).
    Section.ATTRIBUTION: frozenset({Role.OWNER, Role.CUSTOMER, Role.VIEWER}),
    Section.OBSERVABILITY: _OWNER,
    # Заказчик обязан видеть Safety: там переключатель DRY_RUN, а по договорённости
    # с ним ни одно сообщение не уходит без его ведома.
    Section.SAFETY: frozenset({Role.OWNER, Role.CUSTOMER}),
    Section.ADMIN: _OWNER,
}


def can(role: Role | str, section: Section | str) -> bool:
    """Разрешён ли раздел этой роли. Неизвестная роль или раздел → нет."""
    try:
        return Role(role) in ACCESS[Section(section)]
    except (ValueError, KeyError):
        return False


def sections_for(role: Role | str) -> list[str]:
    """Разделы, доступные роли. Оболочка получает этот список и строит по нему меню,
    вместо того чтобы вычислять права у себя."""
    return [s.value for s in Section if can(role, s)]
