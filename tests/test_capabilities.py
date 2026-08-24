"""Права на действия — вторая таблица рядом с матрицей разделов.

Проверяется не «работает ли словарь», а решения из согласованной матрицы
(`radar-admin-spec.md` → `radar-roles-matrix.md`). Каждый тест назван так, чтобы при
падении было видно, какое именно решение нарушено, — иначе через месяц никто не
вспомнит, почему заказчик может остановить систему, но не может её запустить.
"""
from __future__ import annotations

import pytest

from app.core.access import (BULK_LIMIT_REVIEWER, CAPABILITIES, Capability, Role,
                             allows, capabilities_for)


def test_every_capability_has_an_entry():
    """Забытое действие в словаре означает «нельзя никому» — это тихо ломает ручку."""
    assert set(CAPABILITIES) == set(Capability)


def test_no_capability_is_open_to_everyone():
    """Гость смотрит витрину и не делает ничего: у него нет ни одного действия."""
    assert capabilities_for(Role.VIEWER) == []


def test_owner_can_do_everything():
    assert set(capabilities_for(Role.OWNER)) == {c.value for c in Capability}


# ── асимметрия: ужесточение шире, чем ослабление ──────────────────────────────

def test_customer_may_stop_but_not_start():
    """Главное правило матрицы. Цена ошибочной остановки — потерянный час, цена
    ошибочного запуска — сообщения живым людям от имени заказчика."""
    assert allows(Role.CUSTOMER, Capability.SYSTEM_KILL)
    assert allows(Role.CUSTOMER, Capability.MODE_DRY_RUN)
    assert not allows(Role.CUSTOMER, Capability.MODE_LIVE)
    assert not allows(Role.CUSTOMER, Capability.SYSTEM_RESUME)


def test_limits_may_be_tightened_wider_than_loosened():
    assert allows(Role.CUSTOMER, Capability.LIMITS_TIGHTEN)
    assert not allows(Role.CUSTOMER, Capability.LIMITS_LOOSEN)


def test_reviewer_has_no_power_over_the_system():
    """Разборщик решает по черновикам и не трогает режим: решено 2026-08-13."""
    for cap in (Capability.SYSTEM_KILL, Capability.MODE_DRY_RUN,
                Capability.MODE_LIVE, Capability.SYSTEM_RESUME):
        assert not allows(Role.REVIEWER, cap), cap


# ── конфигурация против текстов ───────────────────────────────────────────────

def test_classification_config_is_owner_only():
    for role in (Role.CUSTOMER, Role.REVIEWER, Role.VIEWER):
        assert not allows(role, Capability.CONFIG_EDIT)
        assert not allows(role, Capability.CONFIG_ACTIVATE)
        assert not allows(role, Capability.CONFIG_PREVIEW)


def test_customer_may_propose_config_changes_but_not_apply_them():
    """Заказчик знает свою нишу и вправе предложить правку болей; включает владелец."""
    assert allows(Role.CUSTOMER, Capability.CONFIG_PROPOSE)
    assert not allows(Role.CUSTOMER, Capability.CONFIG_ACTIVATE)


def test_texts_are_written_by_three_and_released_by_one():
    """Тексты уходят живым людям от имени заказчика — писать их могут трое, но путь
    «написал и сразу в проде» здесь недопустим."""
    for role in (Role.OWNER, Role.CUSTOMER, Role.REVIEWER):
        assert allows(role, Capability.TEMPLATES_EDIT), role
    for role in (Role.CUSTOMER, Role.REVIEWER):
        assert not allows(role, Capability.TEMPLATES_ACTIVATE), role


# ── флот и задачи ─────────────────────────────────────────────────────────────

def test_fleet_belongs_to_the_owner():
    """Там номера, прокси и лимиты — распоряжаться ими может только владелец."""
    for cap in (Capability.ACCOUNT_MANAGE, Capability.CHANNEL_ASSIGN,
                Capability.CHANNEL_JOIN, Capability.CHANNEL_ARCHIVE):
        assert capabilities_for(Role.CUSTOMER).count(cap.value) == 0, cap


def test_customer_may_read_history_but_not_reclassify():
    """Дочитать канал дорого по лимитам чтения, но не опасно. Пересчёт — рычаг
    качества, он за владельцем."""
    assert allows(Role.CUSTOMER, Capability.RUN_BACKFILL)
    assert not allows(Role.CUSTOMER, Capability.RUN_HEAVY)


def test_export_is_open_to_all_staff():
    for role in (Role.OWNER, Role.CUSTOMER, Role.REVIEWER):
        assert allows(role, Capability.RUN_EXPORT), role
    assert not allows(Role.VIEWER, Capability.RUN_EXPORT)


# ── массовые действия ─────────────────────────────────────────────────────────

def test_bulk_is_allowed_to_staff_with_a_cap_for_the_reviewer():
    """Право есть у всех троих, но у разборщика ограничено по количеству строк:
    одна ошибка в фильтре — и сотни лидов отклонены одним нажатием."""
    assert allows(Role.REVIEWER, Capability.BULK_DECIDE)
    assert BULK_LIMIT_REVIEWER == 25


@pytest.mark.parametrize("role", ["", "admin", "root", None, "Owner "])
def test_unknown_roles_get_nothing(role):
    assert not allows(role, Capability.DRAFT_DECIDE)
