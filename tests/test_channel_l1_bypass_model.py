"""Колонка `channels.l1_bypass_enabled` — на объявлениях, база не обязательна.

Задача волны A «model» из CONTRACT-cascade.md §1.1: колонка обязана существовать
на `Channel`, по умолчанию быть FALSE (выкатка не меняет вердикт ни одного
канала — включение адресности отдельное действие владельца) и не ломать
фикстуры: схему набор создаёт `create_all`-ом и без сервера, и с ним.

Модель и DDL из `app/db/migrate.py::STATEMENTS` проверяются вместе: колонка,
добавленная миграцией, но не описанная в модели, не читается кодом — и наоборот
(по образцу `test_jobs`); DDL живёт здесь, потому что `create_all` колонки в
уже существующую таблицу не добавляет. Один смысл — одно имя: второго названия
этого выключателя быть не должно, дубли ловим проверкой на соседство.
"""
from __future__ import annotations

import os

import asyncpg
import pytest
from sqlalchemy import Boolean, create_engine, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db.models import Base, Channel

# Дословная строка из TESTS-cascade.md T11: формулировка DDL — часть приёмки.
L1_BYPASS_DDL = ("ALTER TABLE channels ADD COLUMN IF NOT EXISTS l1_bypass_enabled "
                 "BOOLEAN NOT NULL DEFAULT FALSE")

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")


# ── модель: колонка есть, паттерн тот же, что у ingest_enabled ────────────────

def test_channel_has_l1_bypass_enabled_like_ingest_enabled():
    """Колонка Boolean NOT NULL с Python-default FALSE — ровно паттерн
    `ingest_enabled` («осознанный выключатель на канале»). Default проверяется
    на колонке таблицы, а не на атрибуте объекта: он применяется при вставке,
    а не при конструировании (TESTS-cascade, T11)."""
    columns = Channel.__table__.c
    assert "l1_bypass_enabled" in columns, "колонки нет — код волны B не чтение"
    col = columns["l1_bypass_enabled"]
    ingest = columns["ingest_enabled"]
    assert type(col.type) is Boolean and type(ingest.type) is Boolean
    assert col.nullable is False
    assert col.default is not None and col.default.is_scalar and col.default.arg is False


def test_column_sits_next_to_ingest_enabled_and_is_the_only_one():
    """«Рядом с `ingest_enabled`» — не украшение: второе имя того же
    выключателя («открыть L1», «мягкий L1») разъелось бы с этим молча.
    Ловим и позицию, и отсутствие дублей по смыслу — среди булевых колонок
    канала имя должно быть ровно одно."""
    names = list(Channel.__table__.c.keys())
    assert names.index("l1_bypass_enabled") == names.index("ingest_enabled") + 1
    # Дублей по смыслу нет: среди колонок канала имя про обход L1 ровно одно.
    assert [n for n in names if "l1" in n or "bypass" in n] == ["l1_bypass_enabled"]


# ── миграция: DDL в STATEMENTS, путь на живую базу ────────────────────────────

def test_migration_adds_the_column_with_default_false():
    """Строка в `STATEMENTS` — единственный найденный механизм доведения колонок
    до прода (контракт §4): накатывается при каждом старте API. Формулировка
    дословно из приёмки; правила файла соблюдены — только добавление, ничего
    теряющего данные, идемпотентно."""
    from app.db.migrate import STATEMENTS

    assert L1_BYPASS_DDL in STATEMENTS
    upper = L1_BYPASS_DDL.upper()
    assert "DROP" not in upper and "DELETE" not in upper and "TRUNCATE" not in upper
    assert "IF NOT EXISTS" in upper and "NOT NULL DEFAULT FALSE" in upper


# ── фикстуры не ломаются: схема и вставка живут с новой колонкой ─────────────

def test_channels_table_creates_and_new_channel_lands_false():
    """Таблица `channels` поднимается на sqlite без сервера, и новый канал без
    явно заданного флага ложится FALSE: default применяется при вставке, что и
    видно по прочитанной строке. Всю metadata sqlite не компилирует (JSONB —
    диалект Postgres); полный `create_all` на настоящем Postgres накрывает
    DB-тест ниже — фикстуры с базой живут именно там."""
    engine = create_engine("sqlite://")
    try:
        Channel.__table__.create(engine)
        with engine.begin() as conn:
            # id задаётся руками: BigInteger-PK на sqlite не автоинкрементный.
            conn.execute(Channel.__table__.insert().values(
                id=1, peer_id=-100, title="Канал"))
            flag = conn.execute(
                select(Channel.l1_bypass_enabled).where(Channel.peer_id == -100)
            ).scalar_one()
        assert flag is False
    finally:
        engine.dispose()


# ── на настоящем Postgres: дефолт модели и путь DDL на живую базу ─────────────

@pytest.fixture
async def db():
    """Пересоздание схемы на чистой базе — по образцу `test_reclassify_leads_db`.
    Полный `create_all` здесь же и есть проверка «фикстуры не ломаются»: схема
    набора обязана подниматься целиком с новой колонкой."""
    engine = create_async_engine(DB_URL)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session:
        yield session
    await engine.dispose()


@pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — дефолт модели живёт при вставке")
async def test_channel_defaults_to_false_after_flush(db):
    """«Умолчание FALSE» уровня модели: Python-default применяется при вставке,
    а не при конструировании (TESTS-cascade, T11) — потому flush, а не объект."""
    session = db
    session.add(Channel(peer_id=-101, title="Канал"))
    await session.flush()
    channel = (await session.execute(
        select(Channel).where(Channel.peer_id == -101))).scalar_one()
    assert channel.l1_bypass_enabled is False


@pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — путь DDL живёт на Postgres")
async def test_ddl_rehearses_the_prod_path_on_a_pre_column_base():
    """Репетиция выкатки (контракт §4) на одноразовой базе-скелете старого
    образа: таблица `channels` ещё без колонки и с живой строкой — как прод
    перед стартом нового контейнера. DDL из STATEMENTS обязан добавить колонку
    NOT NULL DEFAULT FALSE, существующая строка — прочесть FALSE (выкатка не
    меняет поведение ни одного канала), вставка «старого кода», не знающего
    колонку, — получить FALSE от сервера, повторный накат — пройти молча
    («база обогнала код», см. докстринг migrate). Своя база — чтобы не трогать
    схему, которую пересоздают другие фикстуры набора."""
    probe_url = DB_URL.rsplit("/", 1)[0] + "/radar_l1bypass_ddl_probe"
    # CREATE/DROP DATABASE в транзакции нельзя — админ-команды идут через
    # asyncpg напрямую (autocommit), по образцу conftest::_db_clock_offset.
    dsn = DB_URL.replace("postgresql+asyncpg://", "postgresql://", 1)

    async def _admin(sql: str) -> None:
        conn = await asyncpg.connect(dsn)
        try:
            await conn.execute(sql)
        finally:
            await conn.close()

    await _admin("DROP DATABASE IF EXISTS radar_l1bypass_ddl_probe")
    await _admin("CREATE DATABASE radar_l1bypass_ddl_probe")

    engine = create_async_engine(probe_url)
    try:
        async with engine.begin() as conn:
            # Скелет прод-таблицы: не копия, но колонки NOT NULL без серверных
            # дефолтов — как их создал create_all старого образа.
            await conn.execute(text(
                "CREATE TABLE channels ("
                "id BIGSERIAL PRIMARY KEY, peer_id BIGINT, "
                "title VARCHAR(255) NOT NULL, "
                "leads_total INTEGER NOT NULL DEFAULT 0, "
                "ingest_enabled BOOLEAN NOT NULL)"))
            await conn.execute(text(
                "INSERT INTO channels (peer_id, title, ingest_enabled) "
                "VALUES (-1, 'Старый канал', TRUE)"))
            # Дважды: первый накат добавляет колонку, второй — «база обогнала код».
            await conn.execute(text(L1_BYPASS_DDL))
            await conn.execute(text(L1_BYPASS_DDL))
            info = (await conn.execute(text(
                "SELECT is_nullable, column_default FROM information_schema.columns "
                "WHERE table_name = 'channels' "
                "AND column_name = 'l1_bypass_enabled'"))).one()
            old_row = (await conn.execute(text(
                "SELECT l1_bypass_enabled FROM channels WHERE peer_id = -1"))).scalar_one()
            # «Старый код»: перечисляет только то, что знал до выкатки.
            await conn.execute(text(
                "INSERT INTO channels (peer_id, title, ingest_enabled) "
                "VALUES (-2, 'Новый канал', TRUE)"))
            new_row = (await conn.execute(text(
                "SELECT l1_bypass_enabled FROM channels WHERE peer_id = -2"))).scalar_one()
    finally:
        await engine.dispose()
        await _admin("DROP DATABASE IF EXISTS radar_l1bypass_ddl_probe")

    assert info.is_nullable == "NO"
    assert info.column_default == "false"
    assert old_row is False
    assert new_row is False
