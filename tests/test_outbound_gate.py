"""Главный тест сервиса: в DRY_RUN сообщение не уходит ни при каких обстоятельствах.

Условие сделки с заказчиком — сначала сухой прогон без единой отправки. Если этот файл
краснеет, продукт нельзя показывать.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.core.outbound_gate import OutboundGate, SendRequest


class SpyEngage:
    """Считает, сколько раз его попросили отправить. В DRY_RUN должно быть ноль.

    С 16.2 заказ двухфазный: `send_message` только принимает задачу и отвечает
    её номером, доставку подтверждает вебхук `kind="send"` позже — поэтому
    подпись и ответ здесь формы контракта (`app/services/engage.send_message`),
    а не старой «отправил — вернул номер сообщения».
    """

    def __init__(self):
        self.calls: list[dict] = []

    async def send_message(self, *, account_id: int, recipient_peer_id: int | None,
                           recipient_username: str | None, text: str,
                           webhook_url: str, idempotency_key: str,
                           reply_to_message_id: int | None = None,
                           instance: str | None = None,
                           context_chat_id: int | None = None,
                           context_message_id: int | None = None) -> dict:
        self.calls.append({
            "account_id": account_id, "recipient_peer_id": recipient_peer_id,
            "recipient_username": recipient_username, "text": text,
            "webhook_url": webhook_url, "idempotency_key": idempotency_key,
            "instance": instance,
        })
        return {"task_id": f"task-{len(self.calls)}", "status": "queued"}


class SpyJournal:
    def __init__(self):
        self.blocked: list[tuple] = []
        self.sent: list[tuple] = []

    async def record_blocked(self, req, reasons, now):
        self.blocked.append((req, reasons, now))

    async def record_sent(self, req, message_id, now):
        self.sent.append((req, message_id, now))


NOW = datetime(2026, 8, 11, 14, 0, tzinfo=timezone.utc)

OUTBOUND_ID = 77


def fake_webhook(**params) -> str:
    """Двойник `engage.webhook_url`: та же форма вызова (kind и прочее — keyword)."""
    return "http://radar.test/ingest/tok?" + "&".join(
        f"{k}={v}" for k, v in sorted(params.items()))


def make_request(**over) -> SendRequest:
    """Заведомо валидная попытка: каждый тест ломает ровно одно поле.

    Боевые отправки несут `outbound_id` — номер строки `wf_outbound`, созданной
    до гейта: из него строятся ключ идемпотентности и адрес вебхука.
    """
    base = dict(
        draft_id=1, conversation_id=1, account_id=1, recipient_peer_id=555,
        text="Видел твой вопрос про оплату за рубеж, могу посоветовать знакомого",
        draft_state="approved", is_first_message=True, sent_count=0,
        last_sent_at=None, recipient_local_hour=14,
        recipient_is_admin=False, previously_contacted=False,
        outbound_id=OUTBOUND_ID,
    )
    base.update(over)
    return SendRequest(**base)


def gate(mode: str, engage=None, journal=None,
         webhook_builder=None) -> tuple[OutboundGate, SpyEngage, SpyJournal]:
    engage = engage or SpyEngage()
    journal = journal or SpyJournal()

    async def mode_provider() -> str:
        return mode

    return (OutboundGate(engage, mode_provider, journal,
                         webhook_builder=webhook_builder),
            engage, journal)


def live_gate(**kw) -> tuple[OutboundGate, SpyEngage, SpyJournal]:
    """Гейт готовый к боевой отправке: со строкой журнала и адресом возврата."""
    return gate("LIVE", webhook_builder=fake_webhook, **kw)


@pytest.mark.asyncio
async def test_dry_run_never_reaches_engage():
    g, engage, journal = gate("DRY_RUN")
    verdict = await g.send(make_request(), NOW)

    assert verdict.allowed is False
    assert engage.calls == [], "в DRY_RUN не должно быть ни одного вызова Engage"
    assert journal.blocked, "заблокированная попытка обязана попасть в журнал"


@pytest.mark.asyncio
async def test_live_with_clean_request_sends_once():
    g, engage, journal = live_gate()
    verdict = await g.send(make_request(), NOW)

    assert verdict.allowed is True
    # Двухфазный заказ: вердикт знает номер задачи, а не сообщения — доставка
    # приедет позже вебхуком kind="send".
    assert verdict.task_id == "task-1"
    assert verdict.delivered_message_id is None
    [call] = engage.calls  # «отправка ровно один раз» — и по форме контракта
    assert call["account_id"] == 1 and call["recipient_peer_id"] == 555
    assert call["idempotency_key"] == f"radar-wf-outbound-{OUTBOUND_ID}"
    assert "kind=send" in call["webhook_url"]
    assert f"outbound_id={OUTBOUND_ID}" in call["webhook_url"]
    assert journal.sent and not journal.blocked


@pytest.mark.asyncio
async def test_unapproved_draft_is_blocked_even_in_live():
    """Человек не одобрял этот текст — значит он не уходит, какой бы ни был режим."""
    g, engage, _ = live_gate()
    verdict = await g.send(make_request(draft_state="pending"), NOW)

    assert verdict.allowed is False
    assert engage.calls == []
    assert any("approved" in r for r in verdict.reasons)


@pytest.mark.asyncio
async def test_link_in_first_message_is_blocked():
    g, engage, _ = live_gate()
    verdict = await g.send(
        make_request(text="глянь https://example.com там всё есть"), NOW)

    assert verdict.allowed is False
    assert engage.calls == []


@pytest.mark.asyncio
async def test_link_allowed_once_conversation_started():
    """Запрет касается только первого сообщения: в завязавшемся диалоге ссылка уместна."""
    g, engage, _ = live_gate()
    verdict = await g.send(
        make_request(text="вот ссылка https://example.com", is_first_message=False,
                     sent_count=1, last_sent_at=NOW - timedelta(days=2)),
        NOW)

    assert verdict.allowed is True
    assert verdict.task_id == "task-1"
    assert len(engage.calls) == 1


@pytest.mark.asyncio
async def test_live_send_without_outbound_row_is_refused():
    """Боевая отправка без записи журнала невозможна: из её id строятся и ключ
    идемпотентности, и адрес вебхука — без них заказ был бы выстрелом в пустоту."""
    g, engage, journal = live_gate()

    with pytest.raises(ValueError, match="outbound_id"):
        await g.send(make_request(outbound_id=None), NOW)
    assert engage.calls == [] and journal.sent == []


@pytest.mark.asyncio
async def test_live_send_without_webhook_builder_is_refused():
    """Без адреса возврата Engage не смог бы сообщить о доставке."""
    g, engage, journal = gate("LIVE")  # builder не передан

    with pytest.raises(ValueError, match="webhook_builder"):
        await g.send(make_request(), NOW)
    assert engage.calls == [] and journal.sent == []


@pytest.mark.asyncio
async def test_manual_origin_sends_even_in_dry_run():
    """Решение владельца (PLAN 16.11): ручная отправка одобренного черновика идёт
    мимо проверки режима — предохранители другие (право DRAFT_SEND, подтверждение
    в интерфейсе, аудит). Остальные гардрейлы действуют и здесь."""
    g, engage, journal = gate("DRY_RUN", webhook_builder=fake_webhook)
    verdict = await g.send(make_request(origin="manual"), NOW)

    assert verdict.allowed is True
    assert len(engage.calls) == 1
    assert journal.sent


@pytest.mark.asyncio
async def test_manual_origin_does_not_cancel_other_guardrails():
    """«Мимо режима» не значит «мимо правил приличия»: этому человеку уже писали."""
    g, engage, _ = gate("DRY_RUN", webhook_builder=fake_webhook)
    verdict = await g.send(make_request(origin="manual", previously_contacted=True),
                           NOW)

    assert verdict.allowed is False
    assert any("уже писали" in r for r in verdict.reasons)
    assert engage.calls == []


@pytest.mark.asyncio
async def test_send_without_journal_still_reaches_the_task_id():
    """Регресс 16.2: заказчик, уже записавший строку журнала сам, создаёт гейт с
    `journal=None` — отправка не должна падать на `record_sent` ПОСЛЕ того, как
    Engage принял задачу."""
    g, engage, _ = live_gate(journal=None)
    verdict = await g.send(make_request(), NOW)

    assert verdict.allowed is True
    assert verdict.task_id == "task-1"
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
