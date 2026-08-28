"""Профили каскада: реестр, закреплённое поведение `dm_v1` и работоспособность развилок.

Два разных дела в одном файле, и оба нужны.

Первое — **закрепить `dm_v1`**. Профили появились рефакторингом, который обязан был
ничего не изменить; равенство старого и нового кода проверено перебором один раз, но
такая проверка не живёт в репозитории. Здесь прибиты конкретные числа: если завтра
правка ради второго профиля сдвинет первый, это будет видно сразу, а не по странным
лидам через неделю.

Второе — **проверить сами развилки**. Поле в профиле, которое никто никогда не менял,
ничем не лучше константы: оно выглядит как настройка, а работает ли — неизвестно.
Поэтому каждая развилка проверяется на профиле, где она переставлена. Профиль
публичного контура (`public_v1`) появится позже; когда он появится, эти проверки уже
будут стоять и скажут, что именно он поменял.
"""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from app.core import cascade

NOW = datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)

# Живая жалоба, дошедшая бы до конца каскада: тема, признак поломки, просьба.
COMPLAINT = "платёж за рубеж не проходит второй день, подскажите что делать"

# Якорь, который L1 нашёл бы в COMPLAINT. Передаётся в `score` руками: ступень и
# оценка считаются раздельно, и подмешивать сюда весь каскад ради одного списка
# значило бы проверять два правила одним тестом.
COMPLAINT_ANCHORS = ["платеж за рубеж"]


def classify(text=COMPLAINT, **kw):
    base = dict(is_automatic_forward=False, author_is_bot=False, author_peer_id=500,
                author_username="ivan", tg_date=NOW - timedelta(hours=1), now=NOW)
    return cascade.classify(text=text, **{**base, **kw})


# ── реестр ────────────────────────────────────────────────────────────────────

def test_default_profile_resolves():
    assert cascade.profile(cascade.DEFAULT_PROFILE) is cascade.DM_V1


def test_unknown_profile_is_an_error_not_a_fallback():
    """Подставить `dm_v1` вместо неизвестного ключа значило бы отбирать цели
    публичного контура по правилам личных сообщений — и молча."""
    with pytest.raises(cascade.UnknownProfileError) as e:
        cascade.profile("public_v9000")
    assert "public_v9000" in str(e.value)
    assert "dm_v1" in str(e.value), "в ошибке должно быть видно, что вообще бывает"


def test_every_registered_profile_answers_by_its_own_key():
    for key, rules in cascade.PROFILES.items():
        assert rules.key == key, "ключ в реестре разошёлся с ключом внутри профиля"
        assert rules.title, "у профиля должно быть человеческое имя — оно уедет на экран"


def test_profile_cannot_be_edited_in_flight():
    """Профиль — часть кода, а не состояние. Правка на ходу означала бы, что два
    сообщения одного прогона разобраны по разным правилам."""
    with pytest.raises(Exception):
        cascade.DM_V1.l2_min_margin = 0.5
    with pytest.raises(TypeError):
        cascade.DM_V1.pain_anchors["новая боль"] = ("слово",)


def test_no_word_list_contains_yo():
    """Якорь через «ё» не сработает никогда, и не скажет об этом.

    `cascade._norm` складывает «ё» в «е» перед поиском подстроки, поэтому слово
    «платёж» в списке — мёртвый груз: в нормализованном тексте такой подстроки не
    бывает. Ошибка бесшумная — якорь выглядит рабочим и просто не находит ничего, —
    и заметить её можно только вот такой проверкой, а не по результатам отбора.

    Проверяются все списки всех профилей разом: правило про написание, а не про
    конкретную боль, и новый профиль обязан ему подчиняться с первого дня.
    """
    dead: list[str] = []
    for key, rules in cascade.PROFILES.items():
        lists = [(f"{key}.pain_anchors[{name}]", words)
                 for name, words in rules.pain_anchors.items()]
        lists += [(f"{key}.disqualifier_markers[{name}]", words)
                  for name, words in rules.disqualifier_markers.items()]
        lists += [(f"{key}.{name}", getattr(rules, name))
                  for name in ("problem_markers", "intent_markers", "urgency_markers",
                               "decision_maker_markers")]
        dead += [f"{where}: «{word}»" for where, words in lists for word in words
                 if "ё" in word]

    assert dead == [], ("эти слова не совпадут никогда — _norm заменяет «ё» на «е»: "
                       + "; ".join(dead))


# ── закреплённое поведение dm_v1 ──────────────────────────────────────────────

def test_dm_v1_score_components_are_pinned():
    """Числа проверены на живых данных и нарисованы человеку в разборе оценки.
    Меняться они могут, но только осознанно — вместе с этим тестом."""
    total, breakdown = cascade.score(
        text=COMPLAINT, anchors=COMPLAINT_ANCHORS, now=NOW,
        tg_date=NOW - timedelta(hours=1), has_username=True)
    assert {b["label"]: b["value"] for b in breakdown} == {
        "совпадение с болью": 22,
        "срочность/интент": 24,
        "признаки ЛПР": 0,
        "свежесть": 10,
        "достижимость в ЛС": 6,
    }
    assert total == 62


@pytest.mark.parametrize("age,expected", [
    (timedelta(hours=1), 10),
    (timedelta(hours=12), 7),
    (timedelta(days=3), 3),
    (timedelta(days=30), 0),
])
def test_dm_v1_freshness_ladder_is_pinned(age, expected):
    """Лесенка свежести считается долями от потолка. Доли подобраны так, чтобы при
    потолке 10 давать прежние 10/7/3/0 — округление не должно съесть ни одного балла."""
    _, breakdown = cascade.score(text=COMPLAINT, anchors=COMPLAINT_ANCHORS,
                                 tg_date=NOW - age, now=NOW, has_username=True)
    assert {b["label"]: b["value"] for b in breakdown}["свежесть"] == expected


def test_dm_v1_score_cannot_exceed_the_sum_of_its_caps():
    """Потолки — это и есть веса. Слагаемое, пробивающее свой потолок, сделало бы
    оценку несравнимой между сообщениями."""
    loud = ("платёж за рубеж не проходит не могу оплатить срочно горит подскажите "
            "помогите у нас в компании наш бухгалтер требует документы "
            "ищу платёжного агента")
    total, breakdown = cascade.score(text=loud, anchors=["a", "b", "c", "d", "e"],
                                     tg_date=NOW, now=NOW, has_username=True)
    weights = cascade.DM_V1.weights
    caps = [weights.pain, weights.intent, weights.lpr, weights.fresh, weights.reach]
    for b, cap in zip(breakdown, caps):
        assert b["value"] <= cap, f"слагаемое «{b['label']}» пробило потолок"
    assert total == sum(caps) == 87


def test_dm_v1_drops_a_channel_post_and_an_authorless_message():
    assert classify(is_automatic_forward=True)["passed"] is False
    assert classify(author_peer_id=None)["passed"] is False


# ── развилки профиля ──────────────────────────────────────────────────────────

def test_a_profile_can_keep_automatic_forwards():
    """Автопересылка поста канала — мусор для личного сообщения и корень ветки
    комментариев для публичного ответа. Одно и то же сообщение, разные решения."""
    public = replace(cascade.DM_V1, key="p", title="п", drop_automatic_forward=False)
    assert classify(is_automatic_forward=True)["level"] == 0
    kept = classify(is_automatic_forward=True, profile=public)
    assert kept["passed"] is True and kept["level"] == 1


def test_a_profile_can_keep_messages_without_an_author():
    """Пост анонимного админа некому написать в личку, но прокомментировать можно."""
    public = replace(cascade.DM_V1, key="p", title="п", require_author=False)
    assert classify(author_peer_id=None)["level"] == 0
    assert classify(author_peer_id=None, profile=public)["passed"] is True


def test_a_zero_weight_removes_a_score_component_but_keeps_the_line():
    """Ноль в потолке выключает слагаемое — но строка разбора остаётся: человек
    должен видеть, что достижимость учтена и оценена в ноль, а не что её забыли."""
    no_reach = replace(cascade.DM_V1, key="p", title="п",
                       weights=replace(cascade.DM_V1.weights, reach=0))
    _, breakdown = cascade.score(text=COMPLAINT, anchors=COMPLAINT_ANCHORS,
                                 tg_date=NOW, now=NOW, has_username=True,
                                 profile=no_reach)
    row = {b["label"]: b["value"] for b in breakdown}
    assert "достижимость в ЛС" in row
    assert row["достижимость в ЛС"] == 0


def test_a_profile_can_stop_rejecting_sellers_and_helpers():
    """Наблюдения модели те же, отказы разные: продавцу бессмысленно писать в личку,
    но публично ответить ему по существу — обычное дело."""
    seller = {"real_problem": True, "is_seller": True, "answering_someone_else": False}
    helper = {"real_problem": True, "is_seller": False, "answering_someone_else": True}
    lenient = replace(cascade.DM_V1, key="p", title="п",
                      l3_reject_seller=False, l3_reject_answering_someone_else=False)

    assert cascade.level3(seller)[0] is False
    assert cascade.level3(helper)[0] is False
    assert cascade.level3(seller, profile=lenient)[0] is True
    assert cascade.level3(helper, profile=lenient)[0] is True


def test_no_profile_can_pass_a_message_the_model_calls_a_non_problem():
    """Единственный отказ L3, который не отключается. «Проблемы нет» — это не про
    политику контура, а про то, что писать не о чем."""
    empty = {"real_problem": False, "is_seller": False, "answering_someone_else": False}
    for rules in (cascade.DM_V1, replace(cascade.DM_V1, key="p", title="п",
                                         l3_reject_seller=False,
                                         l3_reject_answering_someone_else=False)):
        assert cascade.level3(empty, profile=rules)[0] is False


def test_l2_margin_belongs_to_the_profile():
    ranked = [("pos", "банк не пропускает платёж", 0.815),
              ("neg", "болтовня по теме", 0.795)]
    assert cascade.level2(ranked)[0] is True
    strict = replace(cascade.DM_V1, key="p", title="п", l2_min_margin=0.05)
    ok, why, _, _ = cascade.level2(ranked, profile=strict)
    assert ok is False and "0.05" in why, "порог должен попадать в объяснение отказа"


# ── стык с реестром сценариев ─────────────────────────────────────────────────

def test_workflow_pointing_at_a_missing_profile_is_rejected():
    """Внешнего ключа на профиль в базе нет и быть не может — профили живут в коде.
    Значит опечатку обязана поймать проверка сценария, иначе она всплывёт нарушением
    в момент первого отбора, посреди прогона."""
    from app.db.models import Workflow
    from app.services import workflows

    def wf(profile_key):
        return Workflow(key="k", title="т", target_kind="user", action="dm",
                        visibility="private", engage_instance_id=1,
                        engage_use_case="cold_dm", cascade_profile=profile_key)

    assert workflows.validate(wf("dm_v1")) == []
    # Раньше здесь стоял `public_v1` как заведомо отсутствующий. С 25.08 он есть, и
    # пример пришлось заменить: тест на «профиля нет» обязан ссылаться на то, чего
    # нет, иначе он однажды начинает проверять обратное самому себе.
    problems = workflows.validate(wf("particle_v9"))
    assert len(problems) == 1 and "particle_v9" in problems[0]


def test_minimum_length_belongs_to_the_profile():
    short = "платёж завис"
    assert classify(text=short)["level"] == 0
    talkative = replace(cascade.DM_V1, key="p", title="п", min_text_length=5)
    assert classify(text=short, profile=talkative)["level"] == 1
