"""Формы сценариев: какие тройки осей осмысленны и что из них следует в интерфейсе.

Смысл проверок здесь один: сценарий описан осями, а не именем, и если осевая логика
поедет, то поедет молча — неверная тройка не падает сама по себе, она просто рисует
не тот раздел и адресует не туда. Ловим на входе.
"""
from __future__ import annotations

from app.db.models import WfTarget, Workflow
from app.services import workflows


def wf(**kw) -> Workflow:
    base = dict(key="x", title="X", target_kind="user", action="dm",
                visibility="private", engage_instance_id=1,
                engage_use_case="cold_dm", cascade_profile="dm_v1")
    return Workflow(**{**base, **kw})


# ── осевые сочетания ──────────────────────────────────────────────────────────

def test_dm_to_user_is_valid():
    assert workflows.validate(wf()) == []


PUBLIC_REPLY = dict(key="public_reply", target_kind="message", action="reply",
                    visibility="public")
REACTIONS = dict(key="reactions", target_kind="message", action="react",
                 visibility="public")


def axis_problems(w) -> list[str]:
    """Претензии к тройке осей — без претензий к профилю каскада.

    Разделено потому, что это два разных вопроса. «Можно ли отвечать в треде на
    сообщение» — свойство конструкции и от кода не зависит. «Годится ли для этого
    профиль `dm_v1`» — свойство сегодняшнего кода, в котором `public_v1` ещё нет.
    """
    return [p for p in workflows.validate(w) if not p.startswith("профиль")]


def test_public_reply_to_message_is_valid():
    assert axis_problems(wf(**PUBLIC_REPLY)) == []


def test_reaction_to_message_is_valid():
    """Третий сценарий из разговора — реакции. Конструкция обязана его принять
    без единой правки в коде, иначе расширяемость только на словах."""
    assert axis_problems(wf(**REACTIONS)) == []


# ── профиль обязан не противоречить осям ──────────────────────────────────────

def test_public_workflow_on_dm_profile_is_reported():
    """Сегодня в коде один профиль каскада — `dm_v1`, и он отсеивает на L0 всё, у чего
    нет автора-человека: пост канала и анонимного админа. Для публичного ответа это
    ровно те сообщения, ради которых сценарий и заводится.

    Такой сценарий работает молча и почти впустую: цели даёт, но только по репликам.
    Пустой раздел выглядит как «пока никого не нашли», и разница видна только тому,
    кто пойдёт читать профиль. Поэтому — названная проблема, а не тишина.
    """
    problems = workflows.validate(wf(**PUBLIC_REPLY))
    assert any("автора" in p for p in problems)
    assert any("автопересылку" in p for p in problems)


def test_reaction_workflow_on_dm_profile_is_reported():
    problems = workflows.validate(wf(**REACTIONS))
    assert any("автора" in p for p in problems)


def test_dm_workflow_on_dm_profile_has_no_profile_complaints():
    """Обратная проверка: жалоба не должна срабатывать на том, ради чего профиль писан."""
    assert workflows.validate(wf()) == []


def test_unknown_profile_stops_before_comparing_it_with_the_axes():
    """У ненайденного профиля нечего сравнивать с осями. Одна внятная претензия лучше,
    чем она же плюс две производные от неё."""
    problems = workflows.validate(wf(**PUBLIC_REPLY, cascade_profile="нет-такого"))
    assert len(problems) == 1 and "нет такого" in problems[0]


def test_dm_to_message_is_rejected():
    """В личку пишут человеку. Сообщение адресом ЛС быть не может."""
    problems = workflows.validate(wf(target_kind="message"))
    assert problems and any("target_kind='user'" in p for p in problems)


def test_reply_to_user_is_rejected():
    """Ответить в треде можно только на сообщение: у человека нет `reply_to`."""
    problems = workflows.validate(wf(action="reply", target_kind="user",
                                     visibility="public"))
    assert problems and any("target_kind='message'" in p for p in problems)


def test_dm_cannot_be_public():
    problems = workflows.validate(wf(visibility="public"))
    assert problems and any("публичным" in p for p in problems)


def test_unknown_axis_value_is_reported_not_ignored():
    problems = workflows.validate(wf(action="telepathy"))
    assert problems and any("telepathy" in p for p in problems)


# ── что из формы следует в меню ───────────────────────────────────────────────

def test_dm_has_conversations_and_no_activity():
    sections = workflows.sections_for(wf())
    assert "conversations" in sections
    assert "activity" not in sections


def test_public_reply_has_activity_and_no_conversations():
    """У публичного ответа переписки не существует: каждый комментарий сам по себе.
    Экран «переписок», собранный из одиночных комментариев, врал бы оператору."""
    sections = workflows.sections_for(wf(target_kind="message", action="reply",
                                         visibility="public"))
    assert "activity" in sections
    assert "conversations" not in sections


def test_reactions_have_no_text_drafts():
    """У реакции нет текста — раздел называется иначе, чем «Черновики»."""
    sections = workflows.sections_for(wf(target_kind="message", action="react",
                                         visibility="public"))
    assert "reactions" in sections
    assert "drafts" not in sections


def test_describe_gives_titles_for_every_section():
    """Раздел без названия — пустой пункт меню. Проверяем, что карта названий полна."""
    for action in Workflow.ACTIONS:
        kind = "user" if action == "dm" else "message"
        vis = "private" if action == "dm" else "public"
        shape = workflows.describe(wf(action=action, target_kind=kind, visibility=vis))
        assert shape["sections"]
        for section in shape["sections"]:
            assert section["title"], f"нет названия у раздела {section['key']}"


def test_every_action_has_sections_declared():
    assert set(workflows.SECTIONS_BY_ACTION) == set(Workflow.ACTIONS)


# ── схема ─────────────────────────────────────────────────────────────────────

def test_target_addressing_is_enforced_by_schema():
    """Недозаполненная цель не должна доезжать до отправки. Проверка адресации живёт
    в схеме, а не только в коде: код можно обойти новой веткой, ограничение — нет."""
    checks = [c for c in WfTarget.__table__.constraints
              if c.__class__.__name__ == "CheckConstraint"]
    assert any(c.name == "ck_target_addressing" for c in checks)
    expr = str(next(c for c in checks if c.name == "ck_target_addressing").sqltext)
    assert "recipient_peer_id" in expr
    assert "chat_peer_id" in expr and "reply_to_message_id" in expr


def test_one_message_can_yield_a_target_in_each_workflow():
    """Ключевая развязка: сообщение может не годиться для ЛС и годиться для
    публичного ответа. Уникальность — по паре, а не по сообщению."""
    uq = [c for c in WfTarget.__table__.constraints
          if c.__class__.__name__ == "UniqueConstraint"]
    cols = {tuple(sorted(col.name for col in c.columns)) for c in uq}
    assert ("message_id", "workflow_id") in cols
    assert ("message_id",) not in cols


def test_target_author_is_optional():
    """У публичной цели автора-человека может не быть вовсе (пост анонимного админа),
    а прежний `leads.author_peer_id` был NOT NULL — на этом и ломалось."""
    assert WfTarget.__table__.c.author_peer_id.nullable
