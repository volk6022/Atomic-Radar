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
