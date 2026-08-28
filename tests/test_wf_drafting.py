"""Заготовки черновиков по целям сценариев: комплект на действие и политика контакта.

Проверяется здесь то, что можно проверить без базы и без модели: **какой комплект
достаётся сценарию и что попадает в текст**. Сама запись в `wf_drafts` — в
`tests/test_wf_drafting_db.py`.

Смысловой центр файла — политика контакта. В личке упоминание Андрея и есть весь
смысл сообщения; под чужим публичным вопросом та же фраза читается как реклама, и
именно ею публичный ответ проваливается. Отбор целей для публичного контура уже
спрашивает у модели «можно ли ответить по существу коротко» — подсунуть после этого
оператору заготовку «напишите Андрею» значило бы обессмыслить отбор.
"""
from __future__ import annotations

import pytest

from app.db.models import Workflow
from app.services import drafting, wf_drafting


# ── реестр комплектов ─────────────────────────────────────────────────────────

def test_every_action_has_a_kit():
    """Оси сценария объявлены в модели; комплект обязан быть у каждого действия,
    иначе первый же сценарий с этой осью упадёт на заведении черновика."""
    for action in Workflow.ACTIONS:
        assert wf_drafting.kit(action) is not None


def test_unknown_action_raises_instead_of_falling_back():
    """Подстановка комплекта ЛС была бы худшим исходом: публичный сценарий писал бы
    в ветку текст, рассчитанный на личку, и увидели бы это по удалённым сообщениям."""
    with pytest.raises(wf_drafting.UnknownActionError) as e:
        wf_drafting.kit("телепатия")
    assert "телепатия" in str(e.value)


def test_dm_kit_reuses_the_old_templates_rather_than_copying_them():
    """Ключевая проверка отката. Пока экраны не переехали на новые таблицы, `drafts`
    остаётся страховкой, а `wf_drafts` контура ЛС — его точной тенью.

    Сверяется тождество объектов, а не равенство текстов: копия сравнялась бы сегодня
    и разошлась на первой же правке, причём молча.
    """
    assert wf_drafting.DM_KIT.templates is drafting.TEMPLATES
    assert wf_drafting.DM_KIT.fallback is drafting.FALLBACK
    assert wf_drafting.DM_KIT.version == drafting.PROMPT_VERSION


def test_dm_variants_match_the_old_generator_word_for_word():
    """То же самое, но через результат.

    Сверяются и тексты, и вердикт линтера: линтеры у двух модулей написаны по-разному
    (старый — одним выражением, новый — цепочкой с объяснением причины), и совпадение
    их решений это не самоочевидность, а то самое свойство тени, ради которого всё и
    делалось. Разойдись они — один и тот же вариант был бы красным в одной очереди и
    зелёным в другой.
    """
    for pain in list(drafting.TEMPLATES) + [None, "неизвестная боль"]:
        old = drafting.build_variants(pain)
        new = wf_drafting.build_variants(pain, chosen=wf_drafting.DM_KIT)
        assert [v["text"] for v in old] == [v["text"] for v in new], pain
        assert [v["lint_ok"] for v in old] == [v["lint_ok"] for v in new], pain
        assert [v["spam_score"] for v in old] == [v["spam_score"] for v in new], pain


def test_public_kit_is_not_the_dm_kit():
    """Решение №8: публичные заготовки пишем иначе. Если бы они совпали, весь
    публичный контур свёлся бы к отправке личного сообщения в общий чат."""
    assert wf_drafting.PUBLIC_KIT.templates is not drafting.TEMPLATES
    dm_texts = {t for texts in drafting.TEMPLATES.values() for t in texts}
    public_texts = {t for texts in wf_drafting.PUBLIC_TEMPLATES.values() for t in texts}
    assert not (dm_texts & public_texts)


# ── политика контакта ─────────────────────────────────────────────────────────

def test_contact_in_a_public_reply_is_rejected_when_nobody_asked():
    """Главное правило публичного контура: под вопросом «как починить» рекомендация
    подрядчика — реклама, даже если она правдива."""
    ok, note = wf_drafting.lint(
        f"Тут поможет Андрей ({wf_drafting.CONTACT}), он такие платежи проводит.",
        pain="не может оплатить за рубеж", chosen=wf_drafting.PUBLIC_KIT)
    assert ok is False
    assert "не спрашивали" in note


def test_the_same_text_is_fine_in_a_private_message():
    """Та же фраза в личке — не нарушение, а смысл сообщения. Одно правило на оба
    контура означало бы либо немой ЛС, либо рекламу в ветке."""
    ok, note = wf_drafting.lint(
        f"Тут поможет Андрей ({wf_drafting.CONTACT}), он такие платежи проводит.",
        pain="не может оплатить за рубеж", chosen=wf_drafting.DM_KIT)
    assert ok is True and note is None


def test_contact_is_allowed_publicly_when_the_question_is_who_to_hire():
    """Обратная сторона правила: умолчать о контакте там, где спросили «кого позвать»,
    это не осторожность, а бесполезность."""
    ok, note = wf_drafting.lint(
        f"Могу порекомендовать: Андрей ({wf_drafting.CONTACT}).",
        pain=wf_drafting.ASKS_FOR_CONTRACTOR, chosen=wf_drafting.PUBLIC_KIT)
    assert ok is True and note is None


def test_link_in_the_first_message_is_rejected_in_both_kits():
    for chosen in (wf_drafting.DM_KIT, wf_drafting.PUBLIC_KIT):
        ok, note = wf_drafting.lint("глянь t.me/somechannel", pain=None, chosen=chosen)
        assert ok is False and "Ссылка" in note.capitalize()


def test_rejected_variant_says_what_is_wrong_with_it():
    """`lint_ok: false` без причины заставляет читать код, чтобы понять отказ."""
    ok, note = wf_drafting.lint(
        f"Пиши Андрею {wf_drafting.CONTACT}", pain="не может оплатить за рубеж",
        chosen=wf_drafting.PUBLIC_KIT)
    assert ok is False and note


# ── сами заготовки соблюдают собственную политику ─────────────────────────────

def test_public_templates_pass_their_own_policy():
    """Самая полезная проверка файла: правило, которое нарушают собственные шаблоны,
    это не правило, а украшение. Оператор получил бы очередь, где каждый вариант
    помечен красным."""
    for pain, texts in wf_drafting.PUBLIC_TEMPLATES.items():
        for text in texts:
            ok, note = wf_drafting.lint(text, pain, wf_drafting.PUBLIC_KIT)
            assert ok, f"{pain}: {note}\n{text}"


def test_dm_templates_pass_their_own_policy():
    for pain, texts in drafting.TEMPLATES.items():
        for text in texts:
            ok, note = wf_drafting.lint(text, pain, wf_drafting.DM_KIT)
            assert ok, f"{pain}: {note}\n{text}"


def test_public_templates_name_the_contact_only_where_it_was_asked():
    """Проверка не политики, а самих текстов: правило можно соблюсти, просто вычеркнув
    контакт отовсюду, — и тогда сценарий «ищу подрядчика» стал бы бесполезен."""
    named = {pain for pain, texts in wf_drafting.PUBLIC_TEMPLATES.items()
             if any(wf_drafting.CONTACT in t for t in texts)}
    assert named == {wf_drafting.ASKS_FOR_CONTRACTOR}


def test_public_and_dm_cover_the_same_pains():
    """Боли приходят из одного каскада: эталоны у профилей общие. Разойдись наборы —
    часть публичных целей молча получала бы заглушку вместо заготовки."""
    assert set(wf_drafting.PUBLIC_TEMPLATES) == set(drafting.TEMPLATES)


# ── форма вариантов ───────────────────────────────────────────────────────────

def test_unknown_pain_falls_back_instead_of_producing_nothing():
    """Пустой список вариантов выглядел бы на экране как «черновик не создался»."""
    for chosen in (wf_drafting.DM_KIT, wf_drafting.PUBLIC_KIT):
        variants = wf_drafting.build_variants("боль, которой нет", chosen=chosen)
        assert variants
        assert [v["text"] for v in variants] == list(chosen.fallback)


def test_variant_carries_everything_the_review_screen_needs():
    for chosen in (wf_drafting.DM_KIT, wf_drafting.PUBLIC_KIT, wf_drafting.REACTION_KIT):
        for v in wf_drafting.build_variants("не может оплатить за рубеж", chosen=chosen):
            assert set(v) == {"text", "spam_score", "prompt_version", "lint_ok",
                              "lint_note", "critic_passed", "critic_text"}
            assert v["prompt_version"] == chosen.version
            # Критика не было, и вариант обязан говорить это прямо: оператор ревьюит
            # очередь как раз для того, чтобы поймать плохой текст.
            assert v["critic_passed"] is None
            assert v["critic_text"]


def test_reactions_ignore_the_pain_and_offer_emoji():
    """У реакции нет текста для разбора: список один на все случаи, и боль его не
    меняет. `final_text` повезёт выбранное эмодзи."""
    for pain in ("не может оплатить за рубеж", None, "боль, которой нет"):
        variants = wf_drafting.build_variants(pain, chosen=wf_drafting.REACTION_KIT)
        assert [v["text"] for v in variants] == list(wf_drafting.REACTION_VARIANTS)
        assert all(v["lint_ok"] for v in variants)


def test_reply_action_gets_the_public_kit():
    """Комплект выбирается по оси действия, а не по ключу сценария: новый публичный
    сценарий не должен требовать ветки в коде."""
    assert wf_drafting.kit("reply") is wf_drafting.PUBLIC_KIT
    assert wf_drafting.kit("dm") is wf_drafting.DM_KIT
    assert wf_drafting.kit("react") is wf_drafting.REACTION_KIT
