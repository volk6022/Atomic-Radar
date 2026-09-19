"""Досоздание колонок, появившихся после того, как таблицы уже накатились.

`Base.metadata.create_all` умеет создавать таблицы, но не менять существующие: новая
колонка в модели просто не появляется в базе, и приложение падает на первом же
запросе с «column does not exist». Alembic для одного развёртывания с одной базой —
лишняя церемония, поэтому здесь список идемпотентных DDL.

Правила, чтобы это не превратилось в мину:

* только `ADD COLUMN IF NOT EXISTS` и `CREATE INDEX IF NOT EXISTS` — ничего, что
  теряет данные. Переименование или удаление колонки делается руками и осознанно.
  Снимать можно только отсутствие данных (NOT NULL, ограничение на пустую
  таблицу) — и каждый такой шаг сопровождается причиной; НАПОЛНЕННЫЕ колонки
  и строки этот список не трогает никогда;
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

    # 2026-09-05, автоматическое дочитывание: у элемента очереди появляется своя
    # глубина (не больше 2000 сообщений и не глубже месяца — правила Ивана) плюс
    # два счётчика, без которых итог работы виден только в логе прогона. Таблица
    # backfill_queue на проде уже создана `create_all`, новые колонки в неё
    # добавляются только отсюда.
    "ALTER TABLE backfill_queue ADD COLUMN IF NOT EXISTS target INTEGER",
    "ALTER TABLE backfill_queue ADD COLUMN IF NOT EXISTS min_date TIMESTAMPTZ",
    "ALTER TABLE backfill_queue ADD COLUMN IF NOT EXISTS read_total INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE backfill_queue ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0",

    # 2026-09-11, адресный обход словаря L1 (контракт каскада §1.1): решение
    # «якорей нет — сообщение идёт к L2, а не умирает» принимается на канале,
    # а не глобально. DEFAULT FALSE обязателен: выкатка не меняет поведение ни
    # одного канала, включение — отдельное действие владельца. Таблица channels
    # на проде уже создана create_all, колонку в неё досоздаёт только эта строка.
    "ALTER TABLE channels ADD COLUMN IF NOT EXISTS l1_bypass_enabled BOOLEAN "
    "NOT NULL DEFAULT FALSE",

    # 2026-09-12, автоматика подбора (план 13.3): доноры автоскана «похожих».
    # DEFAULT FALSE обязателен: выкатка не меняет отбор семян ни у одного канала;
    # включение — осознанное действие владельца (PATCH /channels/{id}). Таблица
    # channels на проде уже создана create_all, колонку досоздаёт только эта строка.
    "ALTER TABLE channels ADD COLUMN IF NOT EXISTS discovery_seed BOOLEAN "
    "NOT NULL DEFAULT FALSE",

    # 2026-09-12, комментарии к черновикам (просьба заказчика). Таблица новая,
    # но создаётся отсюда, а не только create_all: прод читает DDL исключительно
    # из этого списка, и типы обязаны совпадать с моделью колонка в колонку.
    # Внешнего ключа на черновики нет намеренно — ссылка полиморфная, см.
    # докстринг DraftComment.
    "CREATE TABLE IF NOT EXISTS draft_comments ("
    "id BIGSERIAL PRIMARY KEY, contour VARCHAR(8) NOT NULL, "
    "draft_id BIGINT NOT NULL, variant_index INTEGER, prompt_version VARCHAR(32), "
    "author_email VARCHAR(255) NOT NULL, text TEXT NOT NULL, "
    "created_at TIMESTAMPTZ NOT NULL DEFAULT now())",
    "CREATE INDEX IF NOT EXISTS ix_draft_comment_draft "
    "ON draft_comments (contour, draft_id, created_at)",

    # 2026-09-19, PLAN 16.1: схема диалогов под оба контура. `conversations` и
    # `conversation_events` пусты и на проде, и на стенде (писателей у них до
    # этого не существовало), поэтому NOT NULL на новые колонки ставится без
    # оговорок; воссоздать старые строки всё равно не из чего.
    #
    # `lead_id` теряет NOT NULL: нитка нового контура привязана к цели
    # (`target_id`), а «unsolicited» — ни к чему; «ровно одна привязка» держит
    # CHECK `ck_conversation_binding`, а не пара обязательных колонок.
    "ALTER TABLE conversations ALTER COLUMN lead_id DROP NOT NULL",
    # `account_id` — FK на мёртвое зеркало `accounts` (прод: 0 строк). Модель
    # колонку больше не знает; в базе она остаётся пустой — правила файла
    # (и здравый смысл) не дают терять данные, даже мёртвые. На базе, созданной
    # уже новой моделью, колонки нет, поэтому прямой ALTER завершился бы ошибкой:
    # guard по information_schema делает шаг тихим на обеих формах схемы.
    "DO $$ BEGIN "
    "IF EXISTS (SELECT 1 FROM information_schema.columns "
    "WHERE table_schema = current_schema() AND table_name = 'conversations' "
    "AND column_name = 'account_id') THEN "
    "ALTER TABLE conversations ALTER COLUMN account_id DROP NOT NULL; "
    "END IF; END $$",
    # Имя FK снято со стенда (`\d conversations` в radar-local-pg,
    # база radar_db): conversations_account_id_fkey.
    "ALTER TABLE conversations DROP CONSTRAINT IF EXISTS conversations_account_id_fkey",
    # Аккаунт — как в `wf_outbound`/`manual_sends`: id в Engage, без FK.
    "ALTER TABLE conversations ADD COLUMN IF NOT EXISTS engage_account_id BIGINT",
    "ALTER TABLE conversations ADD COLUMN IF NOT EXISTS target_id BIGINT",
    "ALTER TABLE conversations ADD COLUMN IF NOT EXISTS peer_username VARCHAR(64)",
    "ALTER TABLE conversations ADD COLUMN IF NOT EXISTS source VARCHAR(24) NOT NULL",
    # Журнал событий: источник, автор, текст, номер сообщения в Telegram,
    # сценарий и момент события — отдельно от момента записи (`created_at`).
    # `at` на старых строках (их нигде нет) честно дополняется моментом записи,
    # и только потом ставится NOT NULL.
    "ALTER TABLE conversation_events ADD COLUMN IF NOT EXISTS source VARCHAR(64)",
    "ALTER TABLE conversation_events ADD COLUMN IF NOT EXISTS actor VARCHAR(255)",
    "ALTER TABLE conversation_events ADD COLUMN IF NOT EXISTS text TEXT",
    "ALTER TABLE conversation_events ADD COLUMN IF NOT EXISTS tg_message_id BIGINT",
    "ALTER TABLE conversation_events ADD COLUMN IF NOT EXISTS workflow_id BIGINT",
    "ALTER TABLE conversation_events ADD COLUMN IF NOT EXISTS at TIMESTAMPTZ",
    "UPDATE conversation_events SET at = created_at WHERE at IS NULL",
    "ALTER TABLE conversation_events ALTER COLUMN at SET NOT NULL",
    # Ручные отправки получают ссылку на свою нитку (16.6а).
    "ALTER TABLE manual_sends ADD COLUMN IF NOT EXISTS conversation_id BIGINT",
    # CHECK и FK — через DO с проверкой pg_constraint: на базе, созданной
    # `create_all` уже новой модели, они существуют, и повторный ADD CONSTRAINT
    # упал бы «already exists». Имена FK — как их называет сам Postgres по умолчанию,
    # чтобы совпасть с тем, что ставит `create_all`.
    "DO $$ BEGIN "
    "IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_conversation_binding' "
    "AND conrelid = 'conversations'::regclass) THEN "
    "ALTER TABLE conversations ADD CONSTRAINT ck_conversation_binding "
    "CHECK (NOT (lead_id IS NOT NULL AND target_id IS NOT NULL)); "
    "END IF; END $$",
    "DO $$ BEGIN "
    "IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'conversations_target_id_fkey' "
    "AND conrelid = 'conversations'::regclass) THEN "
    "ALTER TABLE conversations ADD CONSTRAINT conversations_target_id_fkey "
    "FOREIGN KEY (target_id) REFERENCES wf_targets(id); "
    "END IF; END $$",
    # Бывший «ненастоящий» FK из журнала отправок (задача 16.1, §1).
    "DO $$ BEGIN "
    "IF NOT EXISTS (SELECT 1 FROM pg_constraint "
    "WHERE conname = 'wf_outbound_conversation_id_fkey' "
    "AND conrelid = 'wf_outbound'::regclass) THEN "
    "ALTER TABLE wf_outbound ADD CONSTRAINT wf_outbound_conversation_id_fkey "
    "FOREIGN KEY (conversation_id) REFERENCES conversations(id); "
    "END IF; END $$",
    "DO $$ BEGIN "
    "IF NOT EXISTS (SELECT 1 FROM pg_constraint "
    "WHERE conname = 'manual_sends_conversation_id_fkey' "
    "AND conrelid = 'manual_sends'::regclass) THEN "
    "ALTER TABLE manual_sends ADD CONSTRAINT manual_sends_conversation_id_fkey "
    "FOREIGN KEY (conversation_id) REFERENCES conversations(id); "
    "END IF; END $$",

    # 2026-09-19, PLAN 16.2: у журнала исходящих появилась жизнь между «заказано»
    # и «доставлено» — ручная отправка черновика ждёт вебхук Engage, и статус
    # ожидания с причиной обязан быть виден. Таблица пуста и на проде, и на
    # стенде (писателя до 16.2 не существовало), поэтому NOT NULL DEFAULT на
    # state ставится без оговорок: дополнять старые строки не из чего.
    "ALTER TABLE wf_outbound ADD COLUMN IF NOT EXISTS state VARCHAR(16) "
    "NOT NULL DEFAULT 'pending'",
    "ALTER TABLE wf_outbound ADD COLUMN IF NOT EXISTS engage_task_id VARCHAR(64)",
    "ALTER TABLE wf_outbound ADD COLUMN IF NOT EXISTS error TEXT",
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
