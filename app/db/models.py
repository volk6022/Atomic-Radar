"""Схема Radar.

Спроектирована от `radar-api-contract.md`, а не от моков напрямую: в моках все числа
записаны строками, а цвета лежат рядом с данными. Здесь числа — числа, а цвет бейджа
не хранится вовсе, он выводится из статуса на фронтенде.

Отдельно про `server_default=func.now()`: в Engage тут была строка `"now()"`, которую
Postgres вычислил один раз при создании таблиц и заморозил, — все `created_at` во всех
таблицах хранили момент накатки схемы. Повторять не будем.
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger, Boolean, CheckConstraint, DateTime, ForeignKey, Index, Integer,
    Numeric, String, Text, UniqueConstraint, and_, func, or_,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    type_annotation_map = {dict: JSONB, list: JSONB}


def _created() -> Mapped[datetime]:
    return mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


def _updated() -> Mapped[datetime]:
    return mapped_column(DateTime(timezone=True), nullable=False,
                         server_default=func.now(), onupdate=func.now())


# ── доступ ────────────────────────────────────────────────────────────────────

class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    initials: Mapped[str] = mapped_column(String(8), nullable=False)
    role: Mapped[str] = mapped_column(String(20), nullable=False)

    password_hash: Mapped[str] = mapped_column(Text, nullable=False)
    # Секрет TOTP. Обязателен: без второго фактора эта админка одобряет отправку
    # сообщений живым людям, имея только пароль.
    totp_secret: Mapped[str] = mapped_column(String(64), nullable=False)
    totp_confirmed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    last_login_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_login_ip: Mapped[str | None] = mapped_column(String(45))
    created_at: Mapped[datetime] = _created()
    updated_at: Mapped[datetime] = _updated()


class AuditLog(Base):
    __tablename__ = "audit_log"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    user_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("users.id"))
    user_email: Mapped[str | None] = mapped_column(String(255))
    action: Mapped[str] = mapped_column(String(120), nullable=False)
    detail: Mapped[dict | None] = mapped_column(JSONB)
    ip: Mapped[str | None] = mapped_column(String(45))
    created_at: Mapped[datetime] = _created()

    __table_args__ = (Index("ix_audit_created", "created_at"),)


# ── состояние системы ─────────────────────────────────────────────────────────

class SystemState(Base):
    """Одна строка. Режим живёт здесь, а не в конфиге, потому что переключение
    DRY_RUN ⇄ LIVE и kill switch обязаны действовать немедленно и без рестарта."""
    __tablename__ = "system_state"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    mode: Mapped[str] = mapped_column(String(10), nullable=False, default="DRY_RUN")
    killed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    killed_reason: Mapped[str | None] = mapped_column(Text)
    changed_by: Mapped[str | None] = mapped_column(String(255))
    updated_at: Mapped[datetime] = _updated()


class Limit(Base):
    """Пары ключ-значение из раздела Safety. Значение числовое — пороги сравниваются,
    а не показываются."""
    __tablename__ = "limits"

    key: Mapped[str] = mapped_column(String(80), primary_key=True)
    value: Mapped[float] = mapped_column(Numeric(12, 4), nullable=False)
    unit: Mapped[str | None] = mapped_column(String(32))
    description: Mapped[str | None] = mapped_column(Text)
    updated_at: Mapped[datetime] = _updated()


class Alert(Base):
    """Событие, о котором оператор обязан узнать.

    Именно событие, а не состояние: сухой прогон видно и так, а «задача упала» или
    «включили LIVE» происходит один раз, и без записи исчезает бесследно.
    """
    __tablename__ = "alerts"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    # Ключ повторяемости: модель недоступна десять минут — это одна тревога, а не
    # двести. Повтор с тем же ключом обновляет непрочитанную запись.
    key: Mapped[str | None] = mapped_column(String(80))
    text: Mapped[str] = mapped_column(Text, nullable=False)
    severity: Mapped[str] = mapped_column(String(20), nullable=False, default="info")
    read_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = _created()

    __table_args__ = (Index("ix_alert_unread", "read_at", "created_at"),)


# ── флот и источники ──────────────────────────────────────────────────────────

class Account(Base):
    """Зеркало аккаунта из Engage. Radar не владеет сессиями — он их только показывает
    и просит Engage что-то с ними сделать, поэтому здесь кеш, а не источник истины."""
    __tablename__ = "accounts"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    engage_account_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    engage_instance: Mapped[str] = mapped_column(String(64), nullable=False)
    label: Mapped[str] = mapped_column(String(64), nullable=False)

    status: Mapped[str] = mapped_column(String(20), nullable=False)
    phone_country: Mapped[str | None] = mapped_column(String(2))
    proxy_country: Mapped[str | None] = mapped_column(String(2))
    tz_offset: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    limit_day: Mapped[int | None] = mapped_column(Integer)
    limit_hour: Mapped[int | None] = mapped_column(Integer)
    last_action_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    watcher_uptime: Mapped[float | None] = mapped_column(Numeric(6, 2))
    synced_at: Mapped[datetime] = _updated()

    __table_args__ = (UniqueConstraint("engage_instance", "engage_account_id",
                                       name="uq_account_engage"),)


class Channel(Base):
    __tablename__ = "channels"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    peer_id: Mapped[int] = mapped_column(BigInteger, unique=True, nullable=False)
    username: Mapped[str | None] = mapped_column(String(64))
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    topic: Mapped[str | None] = mapped_column(String(120))

    # Обсуждение канала. `get_chat_info` в Engage отдаёт linked_chat_username, поэтому
    # группу для чтения комментариев находим автоматически, а не спрашиваем руками.
    linked_chat_peer_id: Mapped[int | None] = mapped_column(BigInteger)
    linked_chat_username: Mapped[str | None] = mapped_column(String(64))

    # Что это за чат по версии Telegram: `channel` (вещательный) или
    # `supergroup`/`group`/`forum` (обсуждение). Группа обсуждения живёт отдельной
    # строкой канала — связи между «Островок Командировки» и «Островок Командировки
    # Chat» в данных нет никакой, кроме этих полей, и без `chat_type` отличить одно
    # от другого можно было только гадая по суффиксу имени.
    chat_type: Mapped[str | None] = mapped_column(String(20))

    # Когда последний раз спрашивали карточку канала у Telegram. Ровно для того,
    # чтобы пустой `linked_chat_username` перестал значить сразу две вещи: до этой
    # колонки «у канала нет обсуждения» и «мы ещё не спрашивали» были неразличимы,
    # и оба показывались как ноль сообщений (FIXES.md #3).
    linked_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    # Когда аккаунт вступил в группу обсуждения. Это НЕ то же, что «мы прочитали
    # её историю»: `get_chat_history` публичную супергруппу отдаёт и постороннему,
    # а живые апдейты Telegram шлёт только участнику. Пока здесь пусто, комментарии
    # приезжают разовой выгрузкой и дальше группа молчит — ровно то, на что 29.08
    # пожаловался Андрей.
    linked_joined_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    members: Mapped[int | None] = mapped_column(Integer)
    msgs_per_day: Mapped[int | None] = mapped_column(Integer)
    prefilter_rate: Mapped[float | None] = mapped_column(Numeric(6, 4))
    leads_total: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    leads_per_1000: Mapped[float | None] = mapped_column(Numeric(8, 3))

    ingest_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_junk: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    backfill_cursor: Mapped[int | None] = mapped_column(BigInteger)

    # Каким аккаунтом Engage канал был подключён (пункт 7 FIXES.md). Не связь
    # «канал ↔ аккаунт» — это m2m и id аккаунта Engage, локальной таблицы accounts
    # для него нет; полноценная схема на несколько подписчиков одного канала —
    # отдельная задача (пункт 9), здесь только «кто подписал последним».
    subscribed_account_id: Mapped[int | None] = mapped_column(BigInteger)
    subscribed_by: Mapped[str | None] = mapped_column(String(255))
    subscribed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    created_at: Mapped[datetime] = _created()
    updated_at: Mapped[datetime] = _updated()


# ── конвейер ──────────────────────────────────────────────────────────────────

class Message(Base):
    """Сырое сообщение из обсуждения плюс результат каскада L0-L3.

    Самая большая таблица системы: одна активная группа даёт ~9000 сообщений в сутки.
    Отсюда индексы по (channel_id, tg_date) и уникальность по (channel_id, tg_message_id) —
    ингест обязан быть идемпотентным, бэкфилл и реалтайм неизбежно пересекаются.
    """
    __tablename__ = "messages"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    channel_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("channels.id"), nullable=False)
    tg_message_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    tg_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    author_peer_id: Mapped[int | None] = mapped_column(BigInteger)
    author_username: Mapped[str | None] = mapped_column(String(64))
    author_name: Mapped[str | None] = mapped_column(String(160))
    author_is_bot: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # Пост канала, автоматически отзеркаленный в обсуждение. На такие не отвечаем:
    # это не человек. В Engage поле называется automatic_forward (без is_).
    is_automatic_forward: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    reply_to_message_id: Mapped[int | None] = mapped_column(BigInteger)
    thread_id: Mapped[int | None] = mapped_column(BigInteger)

    # Пост канала, из которого сделана автопересылка в группу обсуждения: id чата-
    # источника и номер поста внутри этого канала. Стоят на корне ветки (у самого
    # поста-зеркала), не у комментариев. Без них ссылку «под каким постом» собрать
    # не из чего — а номер корня в группе подставлять нельзя: нумерация канала и
    # группы разная, и ссылка уводила бы на чужой пост.
    forward_from_chat_id: Mapped[int | None] = mapped_column(BigInteger)
    forward_from_message_id: Mapped[int | None] = mapped_column(BigInteger)

    text: Mapped[str | None] = mapped_column(Text)

    # Каскад: на каком уровне сообщение отсеяно и почему. NULL — ещё не обрабатывалось.
    cascade_level: Mapped[int | None] = mapped_column(Integer)
    cascade_passed: Mapped[bool | None] = mapped_column(Boolean)
    cascade_detail: Mapped[dict | None] = mapped_column(JSONB)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = _created()

    __table_args__ = (
        UniqueConstraint("channel_id", "tg_message_id", name="uq_message_tg"),
        Index("ix_message_channel_date", "channel_id", "tg_date"),
        Index("ix_message_unprocessed", "cascade_level", "created_at"),
    )


class MessageReader(Base):
    """Аккаунт Engage, видевший сообщение.

    29.08, заказчик: «берем аккаунт который прочитал сообщение и я от его имени
    пишу» — атрибуция приёма, без которой выбор аккаунта для ответа брать неоткуда.

    Таблица, а не колонка в `messages`: приём идемпотентен по ключу
    `(channel_id, tg_message_id)`, и одно сообщение, увиденное двумя аккаунтами, —
    одна строка `messages`. Колонка «кто видел» хранила бы только последнего.

    `account_id` — идентификатор аккаунта в Engage, а не строка локальной `accounts`:
    та таблица — зеркало, Radar аккаунтами не владеет, и внешнего ключа тут быть
    не может (как у `engage_account_id` в `manual_sends`).
    """
    __tablename__ = "message_readers"

    message_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("messages.id", ondelete="CASCADE"), primary_key=True)
    account_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    # Время первого появления пары: вставка идемпотентна (`ON CONFLICT DO NOTHING`),
    # повторная доставка записи не обновляет, поэтому колонка хранит именно
    # первую встречу, а не последний вебхук.
    first_seen_at: Mapped[datetime] = _created()

    # По этому индексу выбирают «что видел этот аккаунт» — прямой запрос экрана.
    __table_args__ = (Index("ix_message_reader_account", "account_id"),)


class Lead(Base):
    __tablename__ = "leads"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    message_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("messages.id"), nullable=False)
    channel_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("channels.id"), nullable=False)

    author_peer_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    author_username: Mapped[str | None] = mapped_column(String(64))
    author_name: Mapped[str | None] = mapped_column(String(160))

    pain: Mapped[str | None] = mapped_column(String(255))
    quote: Mapped[str | None] = mapped_column(Text)
    score: Mapped[int] = mapped_column(Integer, nullable=False)
    # Разбор скора по слагаемым: [{label, value}]. Оператор обязан видеть, из чего
    # сложилась оценка, иначе доверять ей нельзя.
    score_breakdown: Mapped[list | None] = mapped_column(JSONB)
    disqualifiers: Mapped[list | None] = mapped_column(JSONB)

    status: Mapped[str] = mapped_column(String(20), nullable=False, default="new")
    reject_reason: Mapped[str | None] = mapped_column(String(120))
    created_at: Mapped[datetime] = _created()
    updated_at: Mapped[datetime] = _updated()

    __table_args__ = (
        Index("ix_lead_status_created", "status", "created_at"),
        Index("ix_lead_author", "author_peer_id"),
    )


class Draft(Base):
    """Черновик ответа на лид: три варианта, из которых человек выбирает и одобряет.

    `variants` — JSONB, а не отдельная таблица: они всегда читаются и пишутся вместе
    с черновиком, отдельно не запрашиваются и не переиспользуются.
    """
    __tablename__ = "drafts"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    lead_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("leads.id"), nullable=False)

    variants: Mapped[list] = mapped_column(JSONB, nullable=False)
    thread_context: Mapped[list | None] = mapped_column(JSONB)
    chosen_variant: Mapped[int | None] = mapped_column(Integer)
    final_text: Mapped[str | None] = mapped_column(Text)

    state: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    reject_reason: Mapped[str | None] = mapped_column(String(120))
    decided_by: Mapped[str | None] = mapped_column(String(255))
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    prompt_version: Mapped[str | None] = mapped_column(String(32))
    source_message_link: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = _created()
    updated_at: Mapped[datetime] = _updated()

    __table_args__ = (Index("ix_draft_state_created", "state", "created_at"),)


class Conversation(Base):
    __tablename__ = "conversations"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    lead_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("leads.id"), nullable=False)
    account_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("accounts.id"), nullable=False)
    peer_id: Mapped[int] = mapped_column(BigInteger, nullable=False)

    state: Mapped[str] = mapped_column(String(24), nullable=False, default="new")
    sent_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_inbound_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # Момент, когда человек последний раз прочитал нитку. Пусто — не читал вовсе.
    # Дозоздаётся на существующей базе из `app/db/migrate.py`.
    read_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    waiting_since: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    handed_off_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = _created()
    updated_at: Mapped[datetime] = _updated()

    events: Mapped[list["ConversationEvent"]] = relationship(back_populates="conversation")

    @hybrid_property
    def unread(self) -> bool:
        """Непрочитанность — свойство строки, и правило живёт здесь, в одном месте.

        Диалог непрочитан, если есть входящее, которого человек ещё не видел: либо
        он не читал вовсе (`read_at` пуст, а входящие были), либо читал, но после
        прочтения пришло новое (`last_inbound_at > read_at`). Входящих не было —
        читать нечего, диалог не непрочитан, каким бы ни был `read_at`.

        У гибрида две половины — питоновская для строк и SQL-выражение для
        запросов, — и они обязаны говорить одно и то же: иначе счётчик значка
        сойдётся со списком однажды и навсегда разъедется. Список, `total`,
        значок и флажок в строке — всё ходит через этот атрибут, копий условия
        в коде нет.
        """
        if self.last_inbound_at is None:
            return False
        return self.read_at is None or self.last_inbound_at > self.read_at

    @unread.expression
    @classmethod
    def unread(cls):
        return or_(and_(cls.read_at.is_(None), cls.last_inbound_at.isnot(None)),
                   cls.last_inbound_at > cls.read_at)

    __table_args__ = (UniqueConstraint("peer_id", name="uq_conversation_peer"),)


class ConversationEvent(Base):
    """Журнал диалога, только на добавление. Состояние диалога — свёртка этих событий.

    Так сделано, потому что «почему бот это написал» — вопрос, который зададут, и
    ответить на него по текущему состоянию невозможно: оно уже перезаписано.
    """
    __tablename__ = "conversation_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    conversation_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("conversations.id"), nullable=False)
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    payload: Mapped[dict | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = _created()

    conversation: Mapped[Conversation] = relationship(back_populates="events")

    __table_args__ = (Index("ix_convevent_conv", "conversation_id", "created_at"),)


class OutboundAttempt(Base):
    """Каждая попытка отправки, включая заблокированные, с причинами.

    Отдельно от событий диалога: это журнал гардрейла. Он отвечает на вопрос «почему
    не отправилось», который иначе превращается в раскопки по логам.
    """
    __tablename__ = "outbound_attempts"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    draft_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("drafts.id"))
    conversation_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("conversations.id"))
    account_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("accounts.id"))

    allowed: Mapped[bool] = mapped_column(Boolean, nullable=False)
    reasons: Mapped[list | None] = mapped_column(JSONB)
    mode: Mapped[str] = mapped_column(String(10), nullable=False)
    delivered_message_id: Mapped[int | None] = mapped_column(BigInteger)
    text_snapshot: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = _created()

    __table_args__ = (Index("ix_outbound_created", "created_at"),)


# ── настройка и наблюдение ────────────────────────────────────────────────────

class ProfileVersion(Base):
    """Версия профиля заказчика и таксономии болей. Версионируется, потому что от неё
    зависит качество классификации: без версии нельзя сказать, на чём мерили."""
    __tablename__ = "profile_versions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    version: Mapped[str] = mapped_column(String(32), nullable=False, unique=True)
    business_description: Mapped[str | None] = mapped_column(Text)
    pains: Mapped[list | None] = mapped_column(JSONB)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_by: Mapped[str | None] = mapped_column(String(255))
    created_at: Mapped[datetime] = _created()


class CascadeVersion(Base):
    """Версия таксономии L1: якоря боли и дисквалификаторы.

    До 30.08 это были константы в `app/core/cascade.py` — экран профиля читал их
    напрямую, а редактировать было негде. Версии здесь по образцу `ProfileVersion`:
    `is_active=False` — это не черновик, а предложенная заказчиком правка
    (`Capability.CONFIG_PROPOSE`), которая ждёт, когда владелец её включит
    (`CONFIG_ACTIVATE`). Хранить только активную строку значило бы потерять само
    предложение в момент, когда его отклонили или просто не успели посмотреть.

    Снимок полный, а не diff: строка целиком описывает состояние каскада на момент
    активации, и по одной строке видно, на каких правилах вынесен вердикт — без
    необходимости накатывать цепочку патчей.
    """
    __tablename__ = "cascade_versions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    version: Mapped[str] = mapped_column(String(32), nullable=False)
    pain_anchors: Mapped[dict] = mapped_column(JSONB, nullable=False)
    disqualifiers: Mapped[dict] = mapped_column(JSONB, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_by: Mapped[str | None] = mapped_column(String(255))
    created_at: Mapped[datetime] = _created()

    __table_args__ = (Index("ix_cascade_version_active", "is_active", "id"),)


class L2Prototype(Base):
    """Одна эталонная фраза L2 (положительная или шумовая) внутри версии таксономии.

    Привязана к `cascade_version_id`, а не к «текущему состоянию» без версии: когда
    владелец активирует другую версию `CascadeVersion`, набор эталонов обязан
    переключиться вместе с ней атомарно — иначе якоря L1 уже говорят про новую
    боль, а эмбеддинги L2 ещё сравнивают со старой.

    `vector` может быть NULL: строка заводится с текстом сразу, а посчитать
    эмбеддинг получится только когда ответит эмбеддер (см. `cascade_registry`).
    До этого момента L2 не должен тихо доверять пустому вектору — читающий код
    обязан считать NULL как «эталон ещё не участвует в сравнении».
    """
    __tablename__ = "l2_prototypes"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    cascade_version_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("cascade_versions.id"), nullable=False)
    kind: Mapped[str] = mapped_column(String(8), nullable=False)
    label: Mapped[str] = mapped_column(String(120), nullable=False)
    phrase: Mapped[str] = mapped_column(String(500), nullable=False)
    vector: Mapped[list | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = _created()

    __table_args__ = (Index("ix_l2_prototype_version", "cascade_version_id"),)


class L3Prompt(Base):
    """Версия системного промпта L3, по ключу контура (`dm_v1`, `public_v1`).

    Правка обязана поднимать версию, а не переписывать текст на месте: старые
    вердикты в `llm_traces.prompt_version` вынесены прежним вопросом к модели, и
    молча смешивать их с новыми нельзя — по версии видно, каким промптом получен
    конкретный ответ (см. докстринг `app/services/llm.Prompt`).
    """
    __tablename__ = "l3_prompts"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    prompt_key: Mapped[str] = mapped_column(String(32), nullable=False)
    version: Mapped[str] = mapped_column(String(32), nullable=False)
    system_prompt: Mapped[str] = mapped_column(Text, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_by: Mapped[str | None] = mapped_column(String(255))
    created_at: Mapped[datetime] = _created()

    __table_args__ = (Index("ix_l3_prompt_active", "prompt_key", "is_active"),)


class Run(Base):
    """Длинная операция: бэкфилл, переклассификация, выгрузка.

    Строка здесь — источник истины, а не отражение переменной в памяти. От этого
    зависит и отмена (флаг ставит ручка, читает исполнитель), и честность экрана
    после перезапуска контейнера: процесс умирает, строка остаётся, и на старте её
    помечают прерванной вместо вечного «выполняется, 43%».
    """
    __tablename__ = "runs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    params: Mapped[dict | None] = mapped_column(JSONB)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="queued")
    progress: Mapped[float] = mapped_column(Numeric(5, 2), nullable=False, default=0)
    eta_seconds: Mapped[int | None] = mapped_column(Integer)
    gpu_hours: Mapped[float | None] = mapped_column(Numeric(8, 3))
    error: Mapped[str | None] = mapped_column(Text)

    # Просьба остановиться. Отдельным полем, а не статусом: пока задача не увидела
    # флаг и не дописала посчитанное, она всё ещё выполняется, и статус обязан
    # говорить именно это.
    cancel_requested: Mapped[bool] = mapped_column(Boolean, nullable=False,
                                                   default=False)
    # Последние строки хода работы — чтобы понять, на чём задача стоит, не заходя
    # в логи контейнера. Полный вывод остаётся там.
    log: Mapped[list | None] = mapped_column(JSONB)
    # Сводка по завершении: сколько посчитано, сколько лидов создано и удалено.
    result: Mapped[dict | None] = mapped_column(JSONB)
    created_by: Mapped[str | None] = mapped_column(String(255))

    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = _created()

    __table_args__ = (Index("ix_run_kind_status", "kind", "status"),)


class LlmTrace(Base):
    """Трейс вызова модели. Нужен и для отладки, и для счёта себестоимости лида."""
    __tablename__ = "llm_traces"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    stage: Mapped[str] = mapped_column(String(32), nullable=False)
    model: Mapped[str] = mapped_column(String(80), nullable=False)
    prompt_version: Mapped[str | None] = mapped_column(String(32))
    temperature: Mapped[float | None] = mapped_column(Numeric(4, 2))
    prompt: Mapped[str | None] = mapped_column(Text)
    response: Mapped[str | None] = mapped_column(Text)
    tokens_in: Mapped[int | None] = mapped_column(Integer)
    tokens_out: Mapped[int | None] = mapped_column(Integer)
    latency_ms: Mapped[int | None] = mapped_column(Integer)
    cost_usd: Mapped[float | None] = mapped_column(Numeric(10, 6))
    lead_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("leads.id"))
    created_at: Mapped[datetime] = _created()

    __table_args__ = (Index("ix_trace_created", "created_at"),)


class Evaluation(Base):
    __tablename__ = "evaluations"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    prompt_version: Mapped[str] = mapped_column(String(32), nullable=False)
    dataset_size: Mapped[int | None] = mapped_column(Integer)
    precision: Mapped[float | None] = mapped_column(Numeric(5, 4))
    recall: Mapped[float | None] = mapped_column(Numeric(5, 4))
    f1: Mapped[float | None] = mapped_column(Numeric(5, 4))
    notes: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = _created()


class Attribution(Base):
    """Реф-токен связывает конкретного лида с его конверсией у заказчика."""
    __tablename__ = "attribution"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    ref_token: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    lead_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("leads.id"))
    channel_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("channels.id"))
    clicked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    bot_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    converted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    amount: Mapped[float | None] = mapped_column(Numeric(12, 2))
    created_at: Mapped[datetime] = _created()


# ── многопоточная архитектура (workflows) ─────────────────────────────────────
#
# Бэкенд был спроектирован под один сценарий, и на него навешана вся бизнес-логика
# отбора и черновиков. Ниже — сущности, разводящие эту логику по независимым
# конвейерам, чтобы третий и пятый сценарий добавлялись строкой в `workflows`,
# а не переделкой.
#
# Разделение сделано колонкой `workflow_id`, а не отдельными таблицами на сценарий:
# раздельность нужна в интерфейсе (у каждого workflow свои страницы), а в базе она
# только мешала бы — одна схема означает один набор запросов и один шаблон экрана.


class EngageInstance(Base):
    """Инстанс Engage, в который ходит Radar.

    Раньше адрес был один на весь сервис (`ENGAGE_BASE_URL`), и второй клиент со своим
    инстансом подключить было физически некуда. `accounts.engage_instance` при этом
    инстансы уже различал — не хватало только реестра.

    Ключ API здесь НЕ хранится: лежит имя переменной окружения, значение читается из
    окружения процесса. Иначе дамп базы становится связкой ключей от всех клиентов.
    """
    __tablename__ = "engage_instances"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    # Совпадает с `accounts.engage_instance` — по нему аккаунты сходятся с инстансом.
    key: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    # Метка для интерфейса, а НЕ тенант: изоляции данных по ней нет и не подразумевается.
    # Когда в Radar появятся пользователи со стороны клиента, понадобится настоящая
    # многотенантность, и вот тогда это станет внешним ключом.
    client_label: Mapped[str] = mapped_column(String(80), nullable=False)
    base_url: Mapped[str] = mapped_column(String(255), nullable=False)
    api_key_env: Mapped[str] = mapped_column(String(120), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    # Проставляется проверкой доступности. Нужно, чтобы «Engage недоступен» было видно
    # на экране флота, а не только в логах.
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = _created()
    updated_at: Mapped[datetime] = _updated()


class Workflow(Base):
    """Сценарий работы: кого ищем, что с ним делаем, откуда берём аккаунты.

    Описан не названием, а тремя осями — тогда новый сценарий не требует ветки в коде:

    * `target_kind` — что является целью: `user` (человек) или `message` (сообщение);
    * `action`      — что делаем: `dm`, `reply`, `react`;
    * `visibility`  — `private` или `public`, видит ли результат кто-то кроме адресата.

    Три известных сценария ложатся так::

        ЛС               user    / dm    / private
        публичный ответ  message / reply / public
        реакции          message / react / public

    Состав меню и форма экранов выводятся из осей, а не из `key`: `action='dm'` даёт
    раздел «Переписки», `visibility='public'` — «Активность» вместо него.
    """
    __tablename__ = "workflows"

    TARGET_KINDS = ("user", "message")
    ACTIONS = ("dm", "reply", "react")
    VISIBILITIES = ("private", "public")

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    key: Mapped[str] = mapped_column(String(32), unique=True, nullable=False)
    title: Mapped[str] = mapped_column(String(80), nullable=False)

    target_kind: Mapped[str] = mapped_column(String(16), nullable=False)
    action: Mapped[str] = mapped_column(String(16), nullable=False)
    visibility: Mapped[str] = mapped_column(String(16), nullable=False)

    engage_instance_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("engage_instances.id"), nullable=False)
    # Пул аккаунтов на стороне Engage. Поле там NOT NULL, ровно одно на аккаунт и
    # ручки смены нет — значит аккаунт закреплён за сценарием с заведения.
    engage_use_case: Mapped[str] = mapped_column(String(20), nullable=False)

    # Набор правил отбора: `dm_v1` пропускает только реплики людей, `public_v1` —
    # ещё и посты канала, потому что для публичного ответа пост и есть цель.
    cascade_profile: Mapped[str] = mapped_column(String(32), nullable=False)
    prompt_version: Mapped[str | None] = mapped_column(String(32))

    # Настройки сценария лежат при нём, а не в общем окне «настроек вообще».
    settings: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict,
                                           server_default="{}")

    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    description: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = _created()
    updated_at: Mapped[datetime] = _updated()

    __table_args__ = (Index("ix_workflow_active_order", "is_active", "sort_order"),)


class WfVerdict(Base):
    """Вердикт каскада для пары (сообщение, workflow).

    Раньше вердикт лежал прямо в `messages` одним набором колонок — то есть на
    сообщение приходился ровно один результат. Два конвейера с разными критериями
    затирали бы вердикты друг друга: одно и то же сообщение может не годиться для ЛС
    и отлично годиться для публичного ответа, и это два независимых факта.

    `passed` трёхзначно, и это существенно: `True` — прошло все включённые ступени,
    `False` — отсеяно, `NULL` — «ещё в пути»: ступень включена, но её вход (вектор,
    ответ модели) пока не посчитан.
    """
    __tablename__ = "wf_verdicts"

    workflow_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("workflows.id"), primary_key=True)
    message_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("messages.id", ondelete="CASCADE"), primary_key=True)

    level: Mapped[int | None] = mapped_column(Integer)
    passed: Mapped[bool | None] = mapped_column(Boolean)
    detail: Mapped[dict | None] = mapped_column(JSONB)

    pain: Mapped[str | None] = mapped_column(String(255))
    score: Mapped[int | None] = mapped_column(Integer)
    score_breakdown: Mapped[list | None] = mapped_column(JSONB)
    disqualifiers: Mapped[list | None] = mapped_column(JSONB)

    computed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = _created()
    updated_at: Mapped[datetime] = _updated()

    __table_args__ = (
        Index("ix_verdict_wf_passed", "workflow_id", "passed"),
        Index("ix_verdict_wf_level", "workflow_id", "level"),
    )


class WfTarget(Base):
    """Цель: повод сделать действие сценария. Обобщение прежнего `leads`.

    `leads` описывал ровно одну форму — «повод написать вот этому человеку в ЛС», и
    требовал `author_peer_id NOT NULL`. Публичному ответу адресат-человек не нужен:
    цель там — сообщение в треде, а автора может не быть вовсе (пост анонимного
    админа комментировать можно).

    Поэтому адресация разнесена по трём полям и заполняется по `target_kind`
    сценария. Проверка — в `__table_args__`, чтобы недозаполненная цель не доехала
    до отправки.
    """
    __tablename__ = "wf_targets"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    workflow_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("workflows.id"), nullable=False)
    # Денормализовано с `workflows.target_kind`: CHECK не умеет ходить в другую
    # таблицу, а проверка адресации нужна на уровне схемы.
    target_kind: Mapped[str] = mapped_column(String(16), nullable=False)

    message_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("messages.id"), nullable=False)
    channel_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("channels.id"), nullable=False)

    # ── адресация ─────────────────────────────────────────────────────────────
    # target_kind='user': кому пишем в ЛС.
    recipient_peer_id: Mapped[int | None] = mapped_column(BigInteger)
    # target_kind='message': куда отвечаем. Комментарии живут в группе обсуждения
    # канала, поэтому чат — это она, а не сам канал.
    chat_peer_id: Mapped[int | None] = mapped_column(BigInteger)
    reply_to_message_id: Mapped[int | None] = mapped_column(BigInteger)

    # ── витрина для оператора ─────────────────────────────────────────────────
    # Автор здесь необязателен: у публичной цели его может не быть.
    author_peer_id: Mapped[int | None] = mapped_column(BigInteger)
    author_username: Mapped[str | None] = mapped_column(String(64))
    author_name: Mapped[str | None] = mapped_column(String(160))

    pain: Mapped[str | None] = mapped_column(String(255))
    quote: Mapped[str | None] = mapped_column(Text)
    score: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    score_breakdown: Mapped[list | None] = mapped_column(JSONB)
    disqualifiers: Mapped[list | None] = mapped_column(JSONB)

    status: Mapped[str] = mapped_column(String(20), nullable=False, default="new")
    reject_reason: Mapped[str | None] = mapped_column(String(120))
    created_at: Mapped[datetime] = _created()
    updated_at: Mapped[datetime] = _updated()

    __table_args__ = (
        # Одно сообщение даёт максимум одну цель в каждом сценарии, но может дать
        # цель в каждом. Это и есть развязка, которой не хватало.
        UniqueConstraint("workflow_id", "message_id", name="uq_target_wf_message"),
        Index("ix_target_wf_status", "workflow_id", "status", "score"),
        Index("ix_target_wf_created", "workflow_id", "created_at"),
        Index("ix_target_recipient", "workflow_id", "recipient_peer_id"),
        CheckConstraint(
            "(target_kind = 'user' AND recipient_peer_id IS NOT NULL)"
            " OR (target_kind = 'message' AND chat_peer_id IS NOT NULL"
            "     AND reply_to_message_id IS NOT NULL)",
            name="ck_target_addressing",
        ),
    )


class WfDraft(Base):
    """Черновик действия по цели. Обобщение прежнего `drafts`.

    Для `action='react'` текста нет — в `final_text` лежит выбранное эмодзи. Отдельная
    сущность под реакции не заводится: решение оператора устроено одинаково («вот
    заготовка, одобри или отклони»), и разделять его значит дублировать весь экран.
    """
    __tablename__ = "wf_drafts"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    workflow_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("workflows.id"), nullable=False)
    target_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("wf_targets.id"), nullable=False)

    variants: Mapped[list] = mapped_column(JSONB, nullable=False)
    thread_context: Mapped[list | None] = mapped_column(JSONB)
    chosen_variant: Mapped[int | None] = mapped_column(Integer)
    final_text: Mapped[str | None] = mapped_column(Text)

    state: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    reject_reason: Mapped[str | None] = mapped_column(String(120))
    decided_by: Mapped[str | None] = mapped_column(String(255))
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    prompt_version: Mapped[str | None] = mapped_column(String(32))
    source_message_link: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = _created()
    updated_at: Mapped[datetime] = _updated()

    __table_args__ = (
        # Одна цель — один черновик. Раньше это подразумевалось молча.
        UniqueConstraint("target_id", name="uq_draft_target"),
        Index("ix_wfdraft_wf_state", "workflow_id", "state", "created_at"),
    )


class WfOutbound(Base):
    """Журнал исходящих попыток. Обобщение прежнего `outbound_attempts`.

    `conversation_id` необязателен: у публичного ответа переписки нет — есть
    сообщение в треде, на которое ответили. Адрес доставки продублирован здесь
    снимком, потому что цель со временем может быть переоценена, а журнал обязан
    показывать, что происходило на самом деле.
    """
    __tablename__ = "wf_outbound"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    workflow_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("workflows.id"), nullable=False)
    target_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("wf_targets.id"))
    draft_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("wf_drafts.id"))
    conversation_id: Mapped[int | None] = mapped_column(BigInteger)
    # Как и в `manual_sends` — id аккаунта в Engage. В старой `outbound_attempts`
    # здесь был внешний ключ на локальную `accounts`, но та таблица мертва, а
    # отправлять всё равно придётся через Engage, который знает только свои id.
    engage_account_id: Mapped[int | None] = mapped_column(BigInteger)

    # Куда фактически уходило.
    recipient_peer_id: Mapped[int | None] = mapped_column(BigInteger)
    chat_peer_id: Mapped[int | None] = mapped_column(BigInteger)
    reply_to_message_id: Mapped[int | None] = mapped_column(BigInteger)

    allowed: Mapped[bool] = mapped_column(Boolean, nullable=False)
    reasons: Mapped[list | None] = mapped_column(JSONB)
    mode: Mapped[str] = mapped_column(String(16), nullable=False)
    delivered_message_id: Mapped[int | None] = mapped_column(BigInteger)
    text_snapshot: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = _created()

    __table_args__ = (
        Index("ix_wfoutbound_wf_created", "workflow_id", "created_at"),
        Index("ix_wfoutbound_recipient", "workflow_id", "recipient_peer_id"),
        Index("ix_wfoutbound_reply", "workflow_id", "chat_peer_id", "reply_to_message_id"),
    )


class ManualSend(Base):
    """Что человек отправил руками.

    Автоотправки нет, и Андрей пишет сам — а значит единственное место, где живёт
    правда о том, как надо отвечать, это его голова. Форма переносит её в базу.

    Пара «что предложил Radar» → «что человек написал на самом деле» стоит дороже,
    чем каждая половина по отдельности: на ней потом можно будет что-то мерить.
    Сейчас `evaluations` пустая ровно потому, что сравнивать не с чем.
    """
    __tablename__ = "manual_sends"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    workflow_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("workflows.id"), nullable=False)
    # Необязательны: Андрей мог написать тому, кого Radar не находил, и это тоже
    # ценные данные — отказаться их принять значит потерять их совсем.
    target_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("wf_targets.id"))
    draft_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("wf_drafts.id"))
    message_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("messages.id"))
    # Именно id аккаунта в Engage, а не в локальной `accounts`: та таблица мертва, и
    # экран флота берёт список прямо из Engage. Одного числа хватает, потому что
    # инстанс задан сценарием — пара (workflow, аккаунт) однозначна.
    engage_account_id: Mapped[int | None] = mapped_column(BigInteger)

    text: Mapped[str] = mapped_column(Text, nullable=False)
    # Снимок того, что предлагал Radar на момент отправки. Черновик потом могут
    # переписать, а сравнивать надо с тем, что человек видел перед глазами.
    suggested_text: Mapped[str | None] = mapped_column(Text)

    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    recorded_by: Mapped[str] = mapped_column(String(255), nullable=False)
    recorded_at: Mapped[datetime] = _created()
    note: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        Index("ix_manual_wf_sent", "workflow_id", "sent_at"),
        Index("ix_manual_target", "target_id"),
    )
