"""Запись ручных отправок — правила, которые не требуют базы.

Здесь всё, что можно проверить чистыми функциями: выбор показанного текста, разбор
времени отправки и правка уже записанного. Работа с наводками и историей требует
настоящего Postgres и живёт в `test_manual_sends_db.py`.

Разделение не косметическое: эти проверки идут на любой машине, а те — только там, где
есть база. Смешав их, мы получили бы файл, который на половине машин пропускается
целиком, включая правила, которым база не нужна вовсе.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.core import clock
from app.db.models import ManualSend
from app.services import manual_sends


def draft(**kw):
    """Черновик как объект с нужными полями: `chosen_text` ходит только по атрибутам,
    и поднимать ради неё базу незачем."""
    return SimpleNamespace(**{"variants": [], "chosen_variant": None,
                              "final_text": None, **kw})


# ── что человек видел перед глазами ───────────────────────────────────────────

def test_operator_edit_wins_over_everything():
    """`final_text` — то, что оператор написал руками. Если показать вместо него
    заготовку, пара «предложено → отправлено» будет сравнивать не с тем."""
    d = draft(variants=[{"text": "заготовка"}], chosen_variant=0, final_text="правка")
    assert manual_sends.chosen_text(d) == "правка"


def test_chosen_variant_wins_over_the_first():
    d = draft(variants=[{"text": "первый"}, {"text": "второй"}], chosen_variant=1)
    assert manual_sends.chosen_text(d) == "второй"


def test_nothing_chosen_means_the_first_was_on_screen():
    d = draft(variants=[{"text": "первый"}, {"text": "второй"}])
    assert manual_sends.chosen_text(d) == "первый"


def test_no_draft_and_no_variants_give_nothing():
    assert manual_sends.chosen_text(None) is None
    assert manual_sends.chosen_text(draft()) is None


def test_index_out_of_range_falls_back_instead_of_crashing():
    """Черновик мог быть переписан короче, чем был в момент выбора. Падать здесь
    нельзя: форма перестала бы открываться из-за расхождения, которое ничему не мешает."""
    d = draft(variants=[{"text": "единственный"}], chosen_variant=5)
    assert manual_sends.chosen_text(d) == "единственный"


# ── время отправки ────────────────────────────────────────────────────────────

def test_naive_time_is_rejected():
    """Без часового пояса неизвестно, какой момент имеется в виду. Додумать его —
    значит записать отправку со сдвигом в три часа и не узнать об этом никогда."""
    with pytest.raises(manual_sends.ManualSendError, match="часовой пояс|часового пояса"):
        manual_sends.check_sent_at(datetime(2026, 8, 24, 12, 0))


def test_future_time_is_rejected():
    with pytest.raises(manual_sends.ManualSendError, match="будущем"):
        manual_sends.check_sent_at(clock.utcnow() + timedelta(days=1))


def test_small_clock_skew_is_tolerated():
    """Часы на машине человека идут вперёд на минуту — это не опечатка в дате."""
    manual_sends.check_sent_at(clock.utcnow() + timedelta(minutes=1))


def test_absent_time_is_fine():
    """Человек мог не помнить, когда именно отправил. Требовать точное время значит
    получить выдуманное."""
    manual_sends.check_sent_at(None)


# ── правка записанного ────────────────────────────────────────────────────────

def entry(**kw):
    return ManualSend(**{"id": 1, "workflow_id": 1, "text": "как есть",
                         "suggested_text": "как предлагали", "recorded_by": "a@b.c",
                         "note": None, "sent_at": None, "engage_account_id": None,
                         "target_id": 7, **kw})


def test_correcting_reports_only_what_actually_changed():
    """Список изменённого уходит в журнал действий. Если писать туда всё присланное,
    журнал заполнится правками, которые ничего не поменяли."""
    e = entry()
    assert manual_sends.correct(e, {"text": "как есть", "note": None}) == []
    assert manual_sends.correct(e, {"text": "иначе"}) == ["text"]
    assert e.text == "иначе"


def test_empty_note_clears_it_but_missing_note_does_not():
    """«Поле не пришло» и «поле пришло пустым» — разные намерения: не трогай против
    сотри. Схлопнув их, форма молча теряла бы заметки при каждой правке текста."""
    e = entry(note="первая заметка")
    assert manual_sends.correct(e, {"text": "иначе"}) == ["text"]
    assert e.note == "первая заметка"
    assert manual_sends.correct(e, {"note": "   "}) == ["note"]
    assert e.note is None


def test_text_cannot_be_erased():
    with pytest.raises(manual_sends.ManualSendError, match="пустой"):
        manual_sends.correct(entry(), {"text": "   "})


def test_the_snapshot_and_the_target_are_not_correctable():
    """Снимок предложенного — свидетельство, ради которого запись и существует.
    Наводка — это «кому отвечали»; сменить её значит записать другой факт."""
    for field, value in (("suggested_text", "подделка"), ("target_id", 42),
                         ("recorded_by", "другой@b.c"), ("workflow_id", 2)):
        with pytest.raises(manual_sends.ManualSendError, match="не правятся"):
            manual_sends.correct(entry(), {field: value})


def test_future_time_is_rejected_on_correction_too():
    """Дыра, которая появилась бы от двух отдельных проверок: при создании нельзя,
    а при правке — можно."""
    with pytest.raises(manual_sends.ManualSendError, match="будущем"):
        manual_sends.correct(entry(), {"sent_at": clock.utcnow() + timedelta(days=1)})


# ── витрина ───────────────────────────────────────────────────────────────────

def test_matching_the_suggestion_is_reported():
    """Совпало ли отправленное с предложенным — единственное, что по этим данным
    можно посчитать сегодня. Считается на сервере, чтобы экран и отчёт не разошлись."""
    same = manual_sends.describe(entry(text=" как предлагали "))
    assert same["matches_suggestion"] is True

    other = manual_sends.describe(entry(text="совсем другое"))
    assert other["matches_suggestion"] is False


def test_no_suggestion_is_not_a_match():
    """Наводки не было — сравнивать не с чем. `False` здесь значит «не совпало», а не
    «не с чем сравнивать», и путать это нельзя, поэтому проверяем явно."""
    row = manual_sends.describe(entry(suggested_text=None))
    assert row["matches_suggestion"] is False
    assert row["suggested_text"] is None


def test_describe_survives_a_record_without_a_target():
    row = manual_sends.describe(entry(target_id=None))
    assert row["author_name"] == "—"
    assert row["target_id"] is None


def test_describe_takes_the_quote_from_the_message_when_there_is_no_target():
    """Наводки нет, но сообщение известно — цитату всё равно есть откуда взять."""
    message = SimpleNamespace(text="впн не работает второй день")
    row = manual_sends.describe(entry(target_id=None), message=message)
    assert row["quote"] == "впн не работает второй день"


def test_recorded_time_is_serialised_not_dumped_raw():
    e = entry()
    e.recorded_at = datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)
    assert manual_sends.describe(e)["recorded_at"] == "2026-08-24T12:00:00+00:00"
