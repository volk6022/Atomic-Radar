"""Декларации discovery-таблиц: без базы, на объявлениях моделей.

Смысл проверок один: на схему опирается сценарий B — уникальность кандидатов,
перечисления и окно повторов держит база, а не код. Код можно обойти новой
веткой, ограничение — нет; поэтому ловим на объявлениях.
"""
from __future__ import annotations

import re

import pytest
from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
)
from sqlalchemy.exc import IntegrityError

from app.db.models import ChannelCandidate, DiscoveryQuery


def _check(table, name: str) -> CheckConstraint:
    """CHECK-ограничение таблицы по имени. Отсутствие — падение с именем, а не
    тихий поиск среди безымянных."""
    for c in table.__table__.constraints:
        if c.__class__.__name__ == "CheckConstraint" and c.name == name:
            return c
    raise AssertionError(f"нет ограничения {name}")


def _in_values(expr: str) -> set[str]:
    """Значения, перечисленные в `IN (...)`: сверяем список целиком, чтобы
    лишнее значение не проскочило мимо."""
    found = re.search(r"IN \(([^()]*)\)", expr)
    assert found, expr
    return set(re.findall(r"'([^']*)'", found.group(1)))


def _engine_with(sql: str, *columns: Column):
    """Стенд: таблица с колонками модели и одним её ограничением. Ограничение
    обязано работать само, на любой базе, — sqlite без сервера это тоже база,
    и CHECK там enforced."""
    metadata = MetaData()
    table = Table("probe", metadata, *columns, CheckConstraint(sql, name="probe_check"))
    engine = create_engine("sqlite://")
    metadata.create_all(engine)
    return table, engine


# ── channel_candidates: поля ──────────────────────────────────────────────────

CANDIDATE_COLUMNS = {
    "id": (BigInteger, False),
    "username": (String, True),
    "title": (String, False),
    "peer_id": (BigInteger, True),
    "members": (Integer, True),
    "chat_type": (String, True),
    "source": (String, False),
    "seed_channel_id": (BigInteger, True),
    "found_by_account_id": (BigInteger, False),
    "found_at": (DateTime, False),
    "liveness_posts_7d": (Integer, True),
    "liveness_comments_7d": (Integer, True),
    "liveness_checked_at": (DateTime, True),
    "llm_verdict": (String, True),
    "llm_score": (Integer, True),
    "llm_reason": (Text, True),
    "llm_at": (DateTime, True),
    "decision": (String, False),
    "decided_by": (String, True),
    "decided_at": (DateTime, True),
    "decision_reason": (Text, True),
    "linked_chat_username": (String, True),
    "updated_at": (DateTime, False),
}


def test_candidate_has_all_contract_columns():
    """Поле в поле из §1.1: пропущенная колонка не читается кодом, лишняя —
    врёт экрану; тип и обязательность (nullable) — обязательная часть."""
    table = ChannelCandidate.__table__
    assert set(table.c.keys()) == set(CANDIDATE_COLUMNS)
    for name, (type_, nullable) in CANDIDATE_COLUMNS.items():
        col = table.c[name]
        assert isinstance(col.type, type_), name
        assert col.nullable is nullable, name


def test_candidate_string_lengths_match_the_contract():
    lengths = {"username": 64, "title": 255, "chat_type": 20, "source": 8,
               "llm_verdict": 8, "decision": 12, "decided_by": 255,
               "linked_chat_username": 64}
    for name, length in lengths.items():
        assert ChannelCandidate.__table__.c[name].type.length == length, name


def test_candidate_timestamps_are_tz_aware_with_defaults():
    """found_at — по образцу `_created()`: серверное значение по умолчанию
    обязательно, иначе вставка из кода без метки падает или пишет NULL."""
    table = ChannelCandidate.__table__
    for name in ("found_at", "liveness_checked_at", "llm_at", "decided_at",
                 "updated_at"):
        assert table.c[name].type.timezone is True, name
    assert table.c.found_at.server_default is not None
    assert table.c.updated_at.server_default is not None


def test_candidate_defaults_and_keys():
    table = ChannelCandidate.__table__
    assert table.c.id.primary_key
    # Новый кандидат ждёт человека, а не считается решённым молча.
    assert table.c.decision.default.arg == "pending"


def test_candidate_foreign_keys():
    """FK — только на реальные строки: семя обязано быть каналом. Аккаунт
    Engage — id без FK: локальная `accounts` — зеркало (как у BackfillItem)."""
    table = ChannelCandidate.__table__
    assert [fk.target_fullname for fk in table.c.seed_channel_id.foreign_keys] == ["channels.id"]
    assert table.c.found_by_account_id.foreign_keys == set()


def test_candidate_username_is_unique():
    uq = {c.name: tuple(col.name for col in c.columns)
          for c in ChannelCandidate.__table__.constraints
          if c.__class__.__name__ == "UniqueConstraint"}
    assert uq.get("uq_candidate_username") == ("username",)


# ── channel_candidates: перечисления и диапазон ───────────────────────────────

def test_candidate_enum_checks_list_exactly_the_contract_values():
    """Ровно значения контракта и ни одного лишнего: лишнее значение в CHECK —
    это молча разрешённое состояние, которого в постановке нет."""
    assert _in_values(str(_check(ChannelCandidate, "ck_candidate_source").sqltext)) \
        == {"similar", "search", "manual"}
    assert _in_values(str(_check(ChannelCandidate, "ck_candidate_verdict").sqltext)) \
        == {"fit", "unfit", "unclear"}
    assert _in_values(str(_check(ChannelCandidate, "ck_candidate_decision").sqltext)) \
        == {"pending", "approved", "rejected", "connected"}


@pytest.mark.parametrize("score,allowed", [
    (None, True), (0, True), (42, True), (100, True),
    (101, False), (-1, False),
])
def test_candidate_score_range_is_enforced_by_schema(score, allowed):
    """0..100 и NULL — мимо, 101 и -1 — отвергнуты базой: модель отдаёт строку,
    int() может дать что угодно, мусор ловит схема."""
    sql = str(_check(ChannelCandidate, "ck_candidate_score").sqltext)
    probe, engine = _engine_with(sql, Column("llm_score", Integer))
    with engine.begin() as conn:
        if allowed:
            conn.execute(probe.insert().values(llm_score=score))
        else:
            with pytest.raises(IntegrityError):
                conn.execute(probe.insert().values(llm_score=score))


# ── discovery_queries: поля ───────────────────────────────────────────────────

DISCOVERY_COLUMNS = {
    "id": (BigInteger, False),
    "kind": (String, False),
    "seed_channel_id": (BigInteger, True),
    "query": (String, True),
    "account_id": (BigInteger, False),
    "run_id": (BigInteger, True),
    "found_total": (Integer, False),
    "new_total": (Integer, False),
    "created_at": (DateTime, False),
}


def test_discovery_query_has_all_contract_columns():
    """Поле в поле из §1.2."""
    table = DiscoveryQuery.__table__
    assert set(table.c.keys()) == set(DISCOVERY_COLUMNS)
    for name, (type_, nullable) in DISCOVERY_COLUMNS.items():
        col = table.c[name]
        assert isinstance(col.type, type_), name
        assert col.nullable is nullable, name
    assert table.c.query.type.length == 255
    assert table.c.kind.type.length == 8
    assert table.c.created_at.type.timezone is True
    assert table.c.id.primary_key
    # Счётчики начинаются с нуля, а не с NULL: ноль найденных — рабочий итог.
    assert table.c.found_total.default.arg == 0
    assert table.c.new_total.default.arg == 0


def test_discovery_query_foreign_keys():
    table = DiscoveryQuery.__table__
    assert [fk.target_fullname for fk in table.c.seed_channel_id.foreign_keys] == ["channels.id"]
    assert [fk.target_fullname for fk in table.c.run_id.foreign_keys] == ["runs.id"]
    assert table.c.account_id.foreign_keys == set()


# ── discovery_queries: ограничения ────────────────────────────────────────────

def test_discovery_query_kind_lists_exactly_the_contract_values():
    assert _in_values(str(_check(DiscoveryQuery, "ck_discovery_query_kind").sqltext)) \
        == {"similar", "search"}


@pytest.mark.parametrize("kind,seed,query,allowed", [
    ("similar", 1, None, True),
    ("similar", 1, "бухгалтерия", False),
    ("search", None, "бухгалтерия", True),
    ("search", 1, "бухгалтерия", False),
])
def test_discovery_query_addressing_forbids_both_wrong_combinations(
        kind, seed, query, allowed):
    """`similar` с `query` и `search` с `seed_channel_id` запрещены схемой:
    недозаполненный или переополненный запрос не должен доезжать до поиска."""
    sql = str(_check(DiscoveryQuery, "ck_discovery_query_target").sqltext)
    probe, engine = _engine_with(
        sql,
        Column("kind", String(8)),
        Column("seed_channel_id", Integer),
        Column("query", String(255)),
    )
    with engine.begin() as conn:
        if allowed:
            conn.execute(probe.insert().values(
                kind=kind, seed_channel_id=seed, query=query))
        else:
            with pytest.raises(IntegrityError):
                conn.execute(probe.insert().values(
                    kind=kind, seed_channel_id=seed, query=query))


def test_discovery_query_indexes_are_named_and_shaped_as_in_the_contract():
    """Имена индексов — часть контракта: по ним код не ходит, но от них зависит
    DDL, а уникальность окна обязана быть именно частичной и именно по суткам
    UTC, иначе окно молча станет другим."""
    by_name = {ix.name: ix for ix in DiscoveryQuery.__table__.indexes}
    assert set(by_name) == {"uq_discovery_query_seed", "uq_discovery_query_text"}
    for name, key_column in (("uq_discovery_query_seed", "seed_channel_id"),
                             ("uq_discovery_query_text", "query")):
        ix = by_name[name]
        assert ix.unique, name
        assert [c.name for c in ix.columns][:2] == ["kind", key_column], name
        # Частичность и окно: предикат и date_trunc по UTC-суткам created_at.
        where = ix.dialect_options["postgresql"]["where"]
        assert str(where) == f"{key_column} IS NOT NULL", name
        window = str(ix.expressions[2])
        assert "date_trunc" in window, name
        assert "created_at AT TIME ZONE 'UTC'" in window, name


def test_candidate_index_is_named_as_in_the_contract():
    by_name = {ix.name: ix for ix in ChannelCandidate.__table__.indexes}
    assert set(by_name) == {"ix_candidate_decision_created"}
    assert [c.name for c in by_name["ix_candidate_decision_created"].columns] \
        == ["decision", "found_at"]
