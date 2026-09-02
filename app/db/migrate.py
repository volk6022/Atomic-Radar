"""Досоздание колонок, появившихся после того, как таблицы уже накатились.

`Base.metadata.create_all` умеет создавать таблицы, но не менять существующие: новая
колонка в модели просто не появляется в базе, и приложение падает на первом же
запросе с «column does not exist». Alembic для одного развёртывания с одной базой —
лишняя церемония, поэтому здесь список идемпотентных DDL.

Правила, чтобы это не превратилось в мину:

* только `ADD COLUMN IF NOT EXISTS` и `CREATE INDEX IF NOT EXISTS` — ничего, что
  теряет данные. Переименование или удаление колонки делается руками и осознанно;
* каждая новая строка сопровождается датой и причиной;
* порядок не важен: всё идемпотентно и переживает повторный запуск.
"""
from __future__ import annotations

import logging

from sqlalchemy import text

logger = logging.getLogger("radar")

STATEMENTS: list[str] = [
    # 2026-08-14, задачи с прогрессом и отменой.
    "ALTER TABLE runs ADD COLUMN IF NOT EXISTS cancel_requested BOOLEAN "
    "NOT NULL DEFAULT FALSE",
    "ALTER TABLE runs ADD COLUMN IF NOT EXISTS log JSONB",
    "ALTER TABLE runs ADD COLUMN IF NOT EXISTS result JSONB",
    "ALTER TABLE runs ADD COLUMN IF NOT EXISTS created_by VARCHAR(255)",
    "CREATE INDEX IF NOT EXISTS ix_run_kind_status ON runs (kind, status)",
    # 2026-08-14, тревоги стали настоящими: ключ нужен, чтобы одно и то же событие
    # не копилось сотнями строк.
    "ALTER TABLE alerts ADD COLUMN IF NOT EXISTS key VARCHAR(80)",
    "CREATE INDEX IF NOT EXISTS ix_alert_unread ON alerts (read_at, created_at)",

    # 2026-08-30, FIXES.md #5: таксономия каскада (боли, дисквалификаторы, эталоны
    # L2, промпт L3) переехала из констант в БД с версионированием, чтобы экран
    # Profile & Prompts мог писать в тот же источник, из которого читает каскад.
    "CREATE TABLE IF NOT EXISTS cascade_versions ("
    "id BIGSERIAL PRIMARY KEY, version VARCHAR(32) NOT NULL, "
    "pain_anchors JSONB NOT NULL, disqualifiers JSONB NOT NULL, "
    "is_active BOOLEAN NOT NULL DEFAULT FALSE, created_by VARCHAR(255), "
    "created_at TIMESTAMPTZ NOT NULL DEFAULT now())",
    "CREATE INDEX IF NOT EXISTS ix_cascade_version_active "
    "ON cascade_versions (is_active, id)",
    "CREATE TABLE IF NOT EXISTS l2_prototypes ("
    "id BIGSERIAL PRIMARY KEY, "
    "cascade_version_id BIGINT NOT NULL REFERENCES cascade_versions(id), "
    "kind VARCHAR(8) NOT NULL, label VARCHAR(120) NOT NULL, "
    "phrase VARCHAR(500) NOT NULL, vector JSONB, "
    "created_at TIMESTAMPTZ NOT NULL DEFAULT now())",
    "CREATE INDEX IF NOT EXISTS ix_l2_prototype_version "
    "ON l2_prototypes (cascade_version_id)",
    "CREATE TABLE IF NOT EXISTS l3_prompts ("
    "id BIGSERIAL PRIMARY KEY, prompt_key VARCHAR(32) NOT NULL, "
    "version VARCHAR(32) NOT NULL, system_prompt TEXT NOT NULL, "
    "is_active BOOLEAN NOT NULL DEFAULT FALSE, created_by VARCHAR(255), "
    "created_at TIMESTAMPTZ NOT NULL DEFAULT now())",
    "CREATE INDEX IF NOT EXISTS ix_l3_prompt_active ON l3_prompts (prompt_key, is_active)",

    # 2026-08-30, FIXES.md #7: канал заводится сам при первом сообщении, но теперь
    # его можно завести и руками — запоминаем, каким аккаунтом Engage подписались.
    "ALTER TABLE channels ADD COLUMN IF NOT EXISTS subscribed_account_id BIGINT",
    "ALTER TABLE channels ADD COLUMN IF NOT EXISTS subscribed_by VARCHAR(255)",
    "ALTER TABLE channels ADD COLUMN IF NOT EXISTS subscribed_at TIMESTAMPTZ",

    # 2026-08-31, FIXES.md #3: пустой `linked_chat_username` до сих пор означал сразу
    # две несовместимые вещи — «у канала нет группы обсуждения» и «мы про неё ещё не
    # спрашивали». На экране обе выглядели одинаково: ноль сообщений. `linked_checked_at`
    # разводит их, `linked_joined_at` отвечает на третий вопрос — идёт ли из группы
    # живой поток: историю публичной супергруппы Telegram отдаёт и без вступления, а
    # апдейты в реальном времени — только тем, кто в ней состоит. `chat_type` избавляет
    # от догадок по имени: группа обсуждения заводится отдельной строкой канала, и до
    # сих пор отличить её от самого канала можно было разве что по суффиксу `_chat`.
    "ALTER TABLE channels ADD COLUMN IF NOT EXISTS chat_type VARCHAR(20)",
    "ALTER TABLE channels ADD COLUMN IF NOT EXISTS linked_checked_at TIMESTAMPTZ",
    "ALTER TABLE channels ADD COLUMN IF NOT EXISTS linked_joined_at TIMESTAMPTZ",

    # 2026-09-02, экран «Переписки»: непрочитанные считаются в базе (фильтр списка,
    # total и значок в боковой панели), для этого нужен момент последнего прочтения
    # нитки. NULL — не прочитано. Индекс под условие не заводим: оно сравнивает две
    # колонки одной строки (`last_inbound_at > read_at`), обычный btree его не
    # покроет — частичный индекс по одной ветке планировщик в OR-предикате не
    # использует, и вышел бы индекс для галочки, а не для скорости.
    "ALTER TABLE conversations ADD COLUMN IF NOT EXISTS read_at TIMESTAMPTZ",

    # 2026-09-02, ссылки из черновика: у поста, отзеркаленного в группу обсуждения,
    # запоминаем канал-источник и номер поста внутри него — без них ссылку «под каким
    # постом» собрать не из чего. Таблица messages на проде уже существует, и
    # `create_all` новые колонки в неё не добавляет, поэтому досоздаются они только
    # здесь.
    "ALTER TABLE messages ADD COLUMN IF NOT EXISTS forward_from_chat_id BIGINT",
    "ALTER TABLE messages ADD COLUMN IF NOT EXISTS forward_from_message_id BIGINT",
]


async def apply(conn) -> int:
    """Выполнить всё по очереди. Возвращает число выполненных выражений.

    Ошибка одного не должна валить старт: база могла обогнать код (откатились на
    прошлую версию образа), и это не повод не подниматься.
    """
    done = 0
    for stmt in STATEMENTS:
        try:
            await conn.execute(text(stmt))
            done += 1
        except Exception as e:  # noqa: BLE001
            logger.warning("migrate_skipped: %s — %s", stmt[:60], e)
    return done
