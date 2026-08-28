"""Главный тест сервиса: в DRY_RUN сообщение не уходит ни при каких обстоятельствах.

Условие сделки с заказчиком — сначала сухой прогон без единой отправки. Если этот файл
краснеет, продукт нельзя показывать.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.core.outbound_gate import OutboundGate, SendRequest


class SpyEngage:
    """Считает, сколько раз его попросили отправить. В DRY_RUN должно быть ноль."""

    def __init__(self):
        self.calls: list[dict] = []

    async def send_message(self, account_id: int, peer_id: int, text: str) -> int:
        self.calls.append({"account_id": account_id, "peer_id": peer_id, "text": text})
        return 12345


class SpyJournal:
    def __init__(self):
        self.blocked: list[tuple] = []
        self.sent: list[tuple] = []

    async def record_blocked(self, req, reasons, now):
        self.blocked.append((req, reasons, now))

    async def record_sent(self, req, message_id, now):
        self.sent.append((req, message_id, now))


NOW = datetime(2026, 8, 11, 14, 0, tzinfo=timezone.utc)


def make_request(**over) -> SendRequest:
    """Заведомо валидная попытка: каждый тест ломает ровно одно поле."""
    base = dict(
        draft_id=1, conversation_id=1, account_id=1, recipient_peer_id=555,
        text="Видел твой вопрос про оплату за рубеж, могу посоветовать знакомого",
        draft_state="approved", is_first_message=True, sent_count=0,
        last_sent_at=None, recipient_local_hour=14,
        recipient_is_admin=False, previously_contacted=False,
    )
    base.update(over)
    return SendRequest(**base)


def gate(mode: str, engage=None, journal=None) -> tuple[OutboundGate, SpyEngage, SpyJournal]:
    engage = engage or SpyEngage()
    journal = journal or SpyJournal()

    async def mode_provider() -> str:
        return mode

    return OutboundGate(engage, mode_provider, journal), engage, journal


@pytest.mark.asyncio
async def test_dry_run_never_reaches_engage():
    g, engage, journal = gate("DRY_RUN")
    verdict = await g.send(make_request(), NOW)

    assert verdict.allowed is False
    assert engage.calls == [], "в DRY_RUN не должно быть ни одного вызова Engage"
    assert journal.blocked, "заблокированная попытка обязана попасть в журнал"


@pytest.mark.asyncio
async def test_live_with_clean_request_sends_once():
    g, engage, journal = gate("LIVE")
    verdict = await g.send(make_request(), NOW)

    assert verdict.allowed is True
    assert verdict.delivered_message_id == 12345
    assert len(engage.calls) == 1
    assert journal.sent and not journal.blocked


@pytest.mark.asyncio
async def test_unapproved_draft_is_blocked_even_in_live():
    """Человек не одобрял этот текст — значит он не уходит, какой бы ни был режим."""
    g, engage, _ = gate("LIVE")
    verdict = await g.send(make_request(draft_state="pending"), NOW)

    assert verdict.allowed is False
    assert engage.calls == []
    assert any("approved" in r for r in verdict.reasons)


@pytest.mark.asyncio
async def test_link_in_first_message_is_blocked():
    g, engage, _ = gate("LIVE")
    verdict = await g.send(
        make_request(text="глянь https://example.com там всё есть"), NOW)

    assert verdict.allowed is False
    assert engage.calls == []


@pytest.mark.asyncio
async def test_link_allowed_once_conversation_started():
    """Запрет касается только первого сообщения: в завязавшемся диалоге ссылка уместна."""
    g, engage, _ = gate("LIVE")
    verdict = await g.send(
        make_request(text="вот ссылка https://example.com", is_first_message=False,
                     sent_count=1, last_sent_at=NOW - timedelta(days=2)),
        NOW)

    assert verdict.allowed is True
    assert len(engage.calls) == 1


@pytest.mark.asyncio
async def test_message_cap_blocks_the_fifth():
    g, engage, _ = gate("LIVE")
    verdict = await g.send(
        make_request(sent_count=4, is_first_message=False,
                     last_sent_at=NOW - timedelta(days=3)), NOW)

    assert verdict.allowed is False
    assert engage.calls == []


@pytest.mark.asyncio
async def test_too_soon_after_previous_message():
    g, engage, _ = gate("LIVE")
    verdict = await g.send(
        make_request(is_first_message=False, sent_count=1,
                     last_sent_at=NOW - timedelta(hours=2)), NOW)

    assert verdict.allowed is False
    assert engage.calls == []


@pytest.mark.asyncio
async def test_quiet_hours_block_the_send():
    g, engage, _ = gate("LIVE")
    verdict = await g.send(make_request(recipient_local_hour=4), NOW)

    assert verdict.allowed is False
    assert engage.calls == []


@pytest.mark.asyncio
async def test_admin_recipient_is_blocked():
    """Забаненный админом аккаунт теряет не одного лида, а весь канал."""
    g, engage, _ = gate("LIVE")
    verdict = await g.send(make_request(recipient_is_admin=True), NOW)

    assert verdict.allowed is False
    assert engage.calls == []


@pytest.mark.asyncio
async def test_already_contacted_person_is_blocked():
    g, engage, _ = gate("LIVE")
    verdict = await g.send(make_request(previously_contacted=True), NOW)

    assert verdict.allowed is False
    assert engage.calls == []


@pytest.mark.asyncio
async def test_all_violations_reported_at_once():
    """Причины возвращаются списком, а не по одной: иначе оператор чинит их по кругу."""
    g, _, _ = gate("DRY_RUN")
    verdict = await g.send(
        make_request(draft_state="pending", recipient_is_admin=True,
                     recipient_local_hour=3, text="http://spam.example"),
        NOW)

    assert len(verdict.reasons) >= 4


@pytest.mark.asyncio
async def test_evaluate_does_not_send_even_when_allowed():
    """Сухой прогон обязан идти тем же кодом, что и боевая отправка, — но без сети."""
    g, engage, journal = gate("LIVE")
    verdict = await g.evaluate(make_request(), NOW)

    assert verdict.allowed is True
    assert engage.calls == []
    assert not journal.sent
