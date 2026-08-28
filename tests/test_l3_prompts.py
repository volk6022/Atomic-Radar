"""Независимость контуров на L3: свой промпт и свой вызов модели у каждого.

Решение Ивана от 25.08. До него вопрос к модели был один на всё, и ответ по
сообщению раздавался всем сценариям — то есть отбор для публичного ответа
определялся бы вопросом, заданным про личное сообщение. На стадии разработки
поведение модели при таком совмещении не измерено, и цена ошибки выше цены времени
карты: контуры разведены, лишние обращения приняты сознательно.

Проверять это на настоящей модели нельзя — она недетерминирована по доступности и
медленна. Поэтому здесь проверяется то единственное, что и должно быть проверено:
**какие вопросы и сколько раз мы собираемся задать**. Сам ответ модели к решению
отношения не имеет.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.core import cascade
from app.db.models import Message, Workflow
from app.services import llm, reclassify, targeting

NOW = datetime(2026, 8, 25, 12, 0, tzinfo=timezone.utc)

PUBLIC_V1 = cascade.PUBLIC_V1


def message(mid: int = 1) -> Message:
    m = Message(channel_id=1, tg_message_id=1000 + mid, tg_date=NOW,
                author_peer_id=500, author_username="user", author_name="Имя",
                author_is_bot=False, is_automatic_forward=False,
                text="платёж за рубеж не проходит, ищу через кого оплатить инвойс")
    m.id = mid
    return m


def bound(key: str, profile: cascade.CascadeProfile) -> targeting.Bound:
    wf = Workflow(key=key, title=key, target_kind="user", action="dm",
                  visibility="private", engage_instance_id=1,
                  engage_use_case=key, cascade_profile=profile.key)
    wf.id = abs(hash(key)) % 1000 + 1
    return targeting.Bound(wf, profile)


# Отранжированные эталоны, при которых L2 уверенно проходит: цель — довести каскад
# до L3, а не проверять здесь саму ступень векторов.
RANKED = [("pos", "не может оплатить за рубеж", 0.81), ("neg", "болтовня", 0.42)]

PENDING_L3 = {"level": 2, "passed": None, "detail": {},
              "pain": "не может оплатить за рубеж",
              "score": 50, "breakdown": [], "disqualifiers": []}
DONE = {**PENDING_L3, "level": 3, "passed": True}


# ── реестр промптов ───────────────────────────────────────────────────────────

def test_profile_names_its_prompt():
    """Ключ промпта живёт в профиле, а не в глобальной константе: иначе «вопрос
    к модели» снова стал бы один на всех, просто по умолчанию."""
    assert cascade.DM_V1.l3_prompt_key in llm.PROMPTS


def test_unknown_prompt_key_raises_instead_of_falling_back():
    """Подстановка вопроса по умолчанию была бы худшим исходом: контур спрашивал бы
    не о том, и заметили бы это по странному отбору через неделю."""
    with pytest.raises(llm.UnknownPromptError) as e:
        llm.prompt("нет-такого")
    assert "нет-такого" in str(e.value)


def test_prompt_carries_its_own_version():
    """Версия попадает в трейс. Без неё ответы, полученные разными вопросами,
    смешались бы в одной таблице и разобрать их задним числом было бы нечем."""
    for key, prompt in llm.PROMPTS.items():
        assert prompt.key == key
        assert prompt.version
        assert prompt.system.strip()


# ── что именно спрашиваем ─────────────────────────────────────────────────────

def test_two_contours_with_different_prompts_ask_twice():
    """Суть решения. Одно сообщение, два контура с разными вопросами — два обращения
    к модели, а не одно с раздачей ответа обоим."""
    m = message()
    dm, public = bound("cold_dm", cascade.DM_V1), bound("public_reply", PUBLIC_V1)
    wf_verdicts = {(dm.workflow.id, m.id): PENDING_L3,
                   (public.workflow.id, m.id): PENDING_L3}

    jobs = reclassify._l3_jobs([m], {m.id: DONE}, wf_verdicts, [dm, public])

    assert len(jobs) == 2
    assert {key for _, key in jobs} == {"dm_v1", "public_v1"}


def test_the_same_question_is_asked_once():
    """Единственное сохранившееся совмещение — дословно один и тот же вопрос.

    Это не раздача чужого ответа: при `temperature=0` ответ на тот же промпт
    побайтово тот же. Именно так старые колонки делят вызов со сценарием ЛС, и
    только поэтому `leads` остаётся точной тенью `wf_targets`.
    """
    m = message()
    dm = bound("cold_dm", cascade.DM_V1)
    jobs = reclassify._l3_jobs([m], {m.id: PENDING_L3},
                               {(dm.workflow.id, m.id): PENDING_L3}, [dm])

    assert jobs == [(m, "dm_v1")]


def test_a_contour_that_is_not_waiting_asks_nothing():
    """Досчитанный контур не должен тянуть за собой обращение к модели: иначе каждый
    повторный прогон стоил бы как первый."""
    m = message()
    dm, public = bound("cold_dm", cascade.DM_V1), bound("public_reply", PUBLIC_V1)
    wf_verdicts = {(dm.workflow.id, m.id): DONE,
                   (public.workflow.id, m.id): PENDING_L3}

    jobs = reclassify._l3_jobs([m], {m.id: DONE}, wf_verdicts, [dm, public])

    assert jobs == [(m, "public_v1")]


def test_legacy_columns_ask_their_own_question_when_no_workflow_does():
    """Старые колонки — тоже контур. Реестр может быть пуст, а поток разбирать надо."""
    m = message()
    jobs = reclassify._l3_jobs([m], {m.id: PENDING_L3}, {}, [])
    assert jobs == [(m, reclassify.LEGACY_PROMPT)]


# ── чей ответ попадает в вердикт ──────────────────────────────────────────────

def test_each_profile_reads_only_its_own_answer():
    """Ключевая проверка разделения: ответ, полученный чужим вопросом, не должен
    доехать до вердикта. Иначе разведение промптов осталось бы декорацией."""
    m = message()
    # Разные вопросы дают разные наблюдения по одному и тому же тексту. У вопроса ЛС
    # поля «на это уже ответили» нет вовсе, у публичного — есть, и оно решающее.
    answers = {"dm_v1": {"real_problem": True, "is_seller": False,
                         "answering_someone_else": False},
               "public_v1": {"real_problem": True, "is_seller": False,
                             "answering_someone_else": False,
                             "already_answered": True, "thread_is_a_fight": False,
                             "answerable_briefly": True}}

    dm = targeting.verdict_for(bound("cold_dm", cascade.DM_V1), m,
                               l2_enabled=True, l3_enabled=True, ranked=RANKED,
                               llm=answers["dm_v1"], now=NOW)
    public = targeting.verdict_for(bound("public_reply", PUBLIC_V1), m,
                                   l2_enabled=True, l3_enabled=True, ranked=RANKED,
                                   llm=answers["public_v1"], now=NOW)

    # Одно и то же сообщение, разошлись ровно потому, что вопросы были разные:
    # человеку написать по-прежнему есть о чём, а публично лезть уже незачем.
    assert dm["passed"] is True
    assert public["passed"] is False

    # И наоборот — подсунь публичному профилю ответ, полученный вопросом ЛС, и
    # решающего наблюдения в нём просто не окажется.
    blind = targeting.verdict_for(bound("public_reply", PUBLIC_V1), m,
                                  l2_enabled=True, l3_enabled=True, ranked=RANKED,
                                  llm=answers["dm_v1"], now=NOW)
    assert blind["passed"] is True


def test_missing_answer_for_a_prompt_is_pending_not_rejected():
    """Вопрос задали одному контуру, второму не успели. Второй обязан остаться в
    «ожидает»: записать ему отказ значило бы потерять цель из-за очереди к модели."""
    m = message()
    v = targeting.verdict_for(bound("public_reply", PUBLIC_V1), m,
                              l2_enabled=True, l3_enabled=True, ranked=RANKED,
                              llm=None, now=NOW)
    assert v["passed"] is None

# ── публичный контур: свои наблюдения и своя политика ─────────────────────────

DM_ANSWER = {"real_problem": True, "is_seller": False,
             "answering_someone_else": False}
PUBLIC_ANSWER = {**DM_ANSWER, "already_answered": False,
                 "thread_is_a_fight": False, "answerable_briefly": True}


def public_verdict(**overrides) -> tuple[bool, str]:
    return cascade.level3({**PUBLIC_ANSWER, **overrides}, profile=cascade.PUBLIC_V1)


def test_public_prompt_asks_only_what_someone_reads():
    """Промпт не должен спрашивать поля, которые никто не разбирает.

    В DM-промпте так осталось с `urgency` и `pain`: их спрашивают, они уходят в
    трейс, и на решение не влияют. Повторять это в новом промпте значило бы платить
    токенами за строку в логе.
    """
    asked = llm.PUBLIC_V1.system
    for field in ("real_problem", "is_seller", "answering_someone_else",
                  "already_answered", "thread_is_a_fight", "answerable_briefly", "why"):
        assert f'"{field}"' in asked, field
    assert '"urgency"' not in asked
    assert '"pain"' not in asked


def test_public_reply_accepts_a_seller_and_a_helper():
    """Публично поправить продавца по существу нормально, и реплика в адрес чужого
    ответа описывает половину полезных комментариев. В ЛС оба — отказ."""
    assert public_verdict(is_seller=True)[0] is True
    assert public_verdict(answering_someone_else=True)[0] is True

    assert cascade.level3({**DM_ANSWER, "is_seller": True},
                          profile=cascade.DM_V1)[0] is False
    assert cascade.level3({**DM_ANSWER, "answering_someone_else": True},
                          profile=cascade.DM_V1)[0] is False


def test_public_reply_stands_down_when_the_question_is_already_answered():
    """Влезть после верного ответа значит выглядеть рекламой, даже будучи правым.
    У личного сообщения такого способа промахнуться нет."""
    ok, why = public_verdict(already_answered=True)
    assert ok is False and "уже дан верный ответ" in why


def test_public_reply_stays_out_of_a_fight():
    ok, why = public_verdict(thread_is_a_fight=True)
    assert ok is False and "ссора" in why


def test_public_reply_needs_to_be_answerable_briefly():
    """«Напишите Андрею» под чужим вопросом — реклама. Если по существу коротко
    ответить нельзя, публичного ответа не получится в принципе."""
    ok, why = public_verdict(answerable_briefly=False)
    assert ok is False and "коротко" in why


def test_dm_answer_is_not_judged_by_observations_it_never_made():
    """Ключевая защита от тихой поломки: промпт ЛС этих полей не возвращает вовсе.

    Профиль не должен судить по наблюдению, которого его собственный вопрос не
    запрашивал. Иначе достаточно поменять значение по умолчанию — и отсутствующее
    поле начнёт что-то решать.
    """
    assert cascade.level3(DM_ANSWER, profile=cascade.DM_V1)[0] is True
    assert cascade.level3(DM_ANSWER, profile=cascade.PUBLIC_V1)[0] is True


def test_public_profile_takes_posts_without_an_author():
    """То, ради чего профиль и заводился: пост канала и анонимный админ проходят L0.

    В ЛС они отсеиваются правильно — писать в личку некому. Под ними отвечают.
    """
    post = dict(text="оплатили инвойс поставщику, банк вернул платёж, что делать",
                is_automatic_forward=True, author_is_bot=False, author_peer_id=None,
                author_username=None, tg_date=NOW, now=NOW)

    assert cascade.classify(**post, profile=cascade.DM_V1)["level"] == 0
    assert cascade.classify(**post, profile=cascade.PUBLIC_V1)["level"] > 0


def test_public_profile_keeps_the_shared_pains():
    """Боли и эталоны общие: бизнес один, ищем тех же людей. Разное — что мы делаем.
    Разойдись они молча, публичный контур искал бы других клиентов."""
    assert cascade.PUBLIC_V1.pain_anchors is cascade.DM_V1.pain_anchors
    assert cascade.PUBLIC_V1.l2_min_margin == cascade.DM_V1.l2_min_margin
