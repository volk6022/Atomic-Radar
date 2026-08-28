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
    ACTIVITY = "activity"
    MANUAL_SENDS = "manual_sends"
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
    # Активность — то же самое, что «Переписки», но для сценариев, где переписки не
    # бывает: у публичного ответа и реакции есть только лента того, что ушло. Роли
    # поэтому те же, что у переписок, и по той же причине: это внутренняя кухня, а не
    # витрина. Гостю её видно быть не должно — иначе он через блок публичного сценария
    # увидел бы ровно то, что ему закрыто в блоке личных сообщений.
    Section.ACTIVITY: _STAFF,
    # Отправляет руками заказчик, а разборщик видит, что именно ушло, и с чем это
    # расходится с предложенным. Гостю здесь делать нечего: это внутренняя кухня.
    Section.MANUAL_SENDS: _STAFF,
    Section.PROFILE: _STAFF,
    # Разборщик должен видеть, что идёт пересчёт: иначе замершая очередь выглядит
    # поломкой. Запускать задачи он при этом не может — см. CAPABILITIES.
    Section.RUNS: _STAFF,
    Section.EVALS: _STAFF,
    # Заказчик видит свою экономику, гость — только её (это витрина для инвестора).
    Section.ATTRIBUTION: frozenset({Role.OWNER, Role.CUSTOMER, Role.VIEWER}),
    Section.OBSERVABILITY: _OWNER,
    # Заказчик обязан видеть Safety: там переключатель DRY_RUN, а по договорённости
    # с ним ни одно сообщение не уходит без его ведома.
    Section.SAFETY: frozenset({Role.OWNER, Role.CUSTOMER}),
    Section.ADMIN: _OWNER,
}


class Capability(StrEnum):
    """Что можно сделать, в отличие от `Section` — что можно увидеть.

    Раздела на это не хватает. Заказчик обязан видеть Safety: там переключатель
    сухого прогона, и по договорённости без его ведома наружу ничего не уходит.
    Включать LIVE он при этом не должен. Одна таблица «раздел → роли» такого
    различия не выражает, поэтому их две.

    Правило, по которому расставлены роли: **ужесточение доступно шире, чем
    ослабление**. Остановить, отклонить, выключить может любой из штата; запустить,
    одобрить, включить LIVE — только владелец. Цена ошибочной остановки — потерянный
    час, цена ошибочного запуска — сообщения живым людям от имени заказчика.
    """
    # конвейер лидов
    DRAFT_DECIDE = "draft.decide"
    DRAFT_REOPEN = "draft.reopen"
    LEAD_STATUS = "lead.status"
    BULK_DECIDE = "bulk.decide"
    MANUAL_SEND_RECORD = "manual_send.record"
    # конфигурация классификации
    CONFIG_EDIT = "config.edit"
    CONFIG_PROPOSE = "config.propose"
    CONFIG_PREVIEW = "config.preview"
    CONFIG_ACTIVATE = "config.activate"
    # тексты ответов — отдельная сущность с другими правами
    TEMPLATES_EDIT = "templates.edit"
    TEMPLATES_ACTIVATE = "templates.activate"
    # источники
    CHANNEL_ADD = "channel.add"
    CHANNEL_EDIT = "channel.edit"
    CHANNEL_ASSIGN = "channel.assign"
    CHANNEL_JOIN = "channel.join"
    CHANNEL_ARCHIVE = "channel.archive"
    ACCOUNT_MANAGE = "account.manage"
    # задачи
    RUN_BACKFILL = "run.backfill"
    RUN_HEAVY = "run.heavy"
    RUN_EVAL = "run.eval"
    RUN_EXPORT = "run.export"
    RUN_CANCEL = "run.cancel"
    # режим и безопасность
    MODE_LIVE = "mode.live"
    MODE_DRY_RUN = "mode.dry_run"
    SYSTEM_KILL = "system.kill"
    SYSTEM_RESUME = "system.resume"
    LIMITS_TIGHTEN = "limits.tighten"
    LIMITS_LOOSEN = "limits.loosen"
    BLOCKLIST_EDIT = "blocklist.edit"
    # администрирование
    USER_MANAGE = "user.manage"
    ALERT_ACK = "alert.ack"
    ALERT_RULES = "alert.rules"


_OWNER_CUSTOMER = frozenset({Role.OWNER, Role.CUSTOMER})

CAPABILITIES: dict[Capability, frozenset[Role]] = {
    Capability.DRAFT_DECIDE: _STAFF,
    Capability.DRAFT_REOPEN: _STAFF,
    Capability.LEAD_STATUS: _STAFF,
    # Наёмный разборщик решает по одному. Одна ошибка в фильтре — и триста лидов
    # отклонены одним нажатием; владельцу такой инструмент нужен, ему — нет.
    # Ограничение по количеству строк проверяется в ручке (см. BULK_LIMIT_REVIEWER).
    Capability.BULK_DECIDE: _STAFF,

    # Запись — рассказ о том, что уже произошло, наружу от неё ничего не уходит.
    # Правило «ужесточение шире ослабления» здесь ни при чём: запретить записывать
    # факт значит остаться без данных, ради которых форма и делается. Правка чужой
    # записи при этом закрыта — проверяется в ручке, а не таблицей: тут вопрос не
    # «кто вообще может», а «своя запись или нет».
    Capability.MANUAL_SEND_RECORD: _STAFF,

    Capability.CONFIG_EDIT: _OWNER,
    # Заказчик может предложить правку болей: черновик создаётся, но не включается.
    Capability.CONFIG_PROPOSE: _OWNER_CUSTOMER,
    Capability.CONFIG_PREVIEW: _OWNER,
    Capability.CONFIG_ACTIVATE: _OWNER,

    # Тексты правят трое — они уходят от имени заказчика, и ему виднее, как звучит.
    # Пускает в работу владелец: путь «написал → сразу в проде» тут неуместен.
    Capability.TEMPLATES_EDIT: _STAFF,
    Capability.TEMPLATES_ACTIVATE: _OWNER,

    Capability.CHANNEL_ADD: _OWNER_CUSTOMER,
    Capability.CHANNEL_EDIT: _OWNER_CUSTOMER,
    # Распоряжение флотом — только владелец: там номера, прокси и лимиты.
    Capability.CHANNEL_ASSIGN: _OWNER,
    Capability.CHANNEL_JOIN: _OWNER,
    Capability.CHANNEL_ARCHIVE: _OWNER,
    Capability.ACCOUNT_MANAGE: _OWNER,

    # Дочитать историю дорого по лимитам чтения, но не опасно.
    Capability.RUN_BACKFILL: _OWNER_CUSTOMER,
    # Переклассификация и примерка — рычаги качества, они за владельцем.
    Capability.RUN_HEAVY: _OWNER,
    Capability.RUN_EVAL: _OWNER_CUSTOMER,
    Capability.RUN_EXPORT: _STAFF,
    Capability.RUN_CANCEL: _OWNER_CUSTOMER,

    Capability.MODE_LIVE: _OWNER,
    Capability.MODE_DRY_RUN: _OWNER_CUSTOMER,
    Capability.SYSTEM_KILL: _OWNER_CUSTOMER,
    Capability.SYSTEM_RESUME: _OWNER,
    Capability.LIMITS_TIGHTEN: _OWNER_CUSTOMER,
    Capability.LIMITS_LOOSEN: _OWNER,
    Capability.BLOCKLIST_EDIT: _OWNER_CUSTOMER,

    Capability.USER_MANAGE: _OWNER,
    Capability.ALERT_ACK: _STAFF,
    Capability.ALERT_RULES: _OWNER,
}

# Сколько строк разборщик может решить одним действием.
BULK_LIMIT_REVIEWER = 25


def allows(role: Role | str, capability: Capability | str) -> bool:
    """Разрешено ли действие этой роли. Неизвестная роль или действие → нет."""
    try:
        return Role(role) in CAPABILITIES[Capability(capability)]
    except (ValueError, KeyError):
        return False


def capabilities_for(role: Role | str) -> list[str]:
    """Действия, доступные роли. Оболочка получает список вместе с разделами и
    прячет по нему кнопки — ровно с той же оговоркой, что и про меню: это
    удобство, а решение принимается на сервере."""
    return [c.value for c in Capability if allows(role, c)]


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
