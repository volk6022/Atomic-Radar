"""Матрица прав на сервере обязана совпадать с той, что в оболочке GUI.

Расхождение проявляется одинаково неприятно в обе стороны: пункт меню, который отдаёт
403, либо раздел, доступный тому, кому не должен.
"""
from __future__ import annotations

import pytest

from app.core.access import ACCESS, Role, Section, can, sections_for

# Дословно из `ACCESS` в `Atomic Radar.dc.html` (см. contract/Atomic-Radar.md),
# сверено 2026-08-24 после подключения раздела ручных отправок.
#
# Одно расхождение остаётся намеренно: в оболочке есть маршрут `draftsTable` —
# второй вид того же раздела черновиков, у которого нет и не должно быть отдельного
# `Section` на сервере. Права он наследует от `drafts`.
#
# Этот список — третья копия матрицы, и сверяется он руками. Пока `/auth/me` не
# станет для оболочки источником прав (карточка T3 в `GUI-SPEC-agents.md`), тест
# ловит расхождение только тогда, когда кто-то не забыл его сюда перенести.
EXPECTED = {
    "dashboard": {"owner", "customer", "reviewer", "viewer"},
    "fleet": {"owner"},
    "channels": {"owner", "customer", "reviewer"},
    "stream": {"owner", "customer", "reviewer"},
    "leads": {"owner", "customer", "reviewer"},
    "drafts": {"owner", "customer", "reviewer"},
    "conversations": {"owner", "customer", "reviewer"},
    "manual_sends": {"owner", "customer", "reviewer"},
    "profile": {"owner", "customer", "reviewer"},
    "runs": {"owner", "customer", "reviewer"},
    "evals": {"owner", "customer", "reviewer"},
    "attribution": {"owner", "customer", "viewer"},
    "observability": {"owner"},
    "safety": {"owner", "customer"},
    "admin": {"owner"},
}


def test_matrix_matches_the_gui():
    actual = {s.value: {r.value for r in roles} for s, roles in ACCESS.items()}
    assert actual == EXPECTED


def test_every_section_is_covered():
    """Новый раздел без записи в матрице открылся бы всем — ловим это тестом."""
    assert {s.value for s in Section} == set(EXPECTED)


@pytest.mark.parametrize("section", ["fleet", "observability", "admin"])
def test_owner_only_sections(section):
    assert can(Role.OWNER, section)
    for role in (Role.CUSTOMER, Role.REVIEWER, Role.VIEWER):
        assert not can(role, section), f"{role} не должен видеть {section}"


def test_staff_sees_runs_but_cannot_start_them():
    """Раздел задач открыт штату: замершая очередь иначе читается как поломка, а
    причина — идущий пересчёт — видна только владельцу. Право на запуск при этом
    остаётся у владельца, оно проверяется отдельно (см. test_capabilities)."""
    for role in (Role.OWNER, Role.CUSTOMER, Role.REVIEWER):
        assert can(role, "runs"), role
    assert not can(Role.VIEWER, "runs")


def test_customer_sees_safety():
    """Заказчик обязан видеть переключатель DRY_RUN: по договорённости ни одно
    сообщение не уходит без его ведома."""
    assert can(Role.CUSTOMER, Section.SAFETY)


def test_viewer_sees_only_dashboard_and_attribution():
    assert sorted(sections_for(Role.VIEWER)) == ["attribution", "dashboard"]


def test_unknown_input_denies():
    assert not can("superuser", Section.ADMIN)
    assert not can(Role.OWNER, "nonexistent")
