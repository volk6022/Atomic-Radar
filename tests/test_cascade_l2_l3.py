"""Ступени L2 (эмбеддинги) и L3 (LLM) — правила решения, без сети.

Числа и ответ модели сюда приезжают готовыми: косинусы считает
`services/embeddings.py`, в модель ходит `services/llm.py`. Здесь проверяется ровно
то, что решает `core/cascade.py`, — и это единственный способ покрыть правило тестом,
не поднимая ни сеть, ни видеокарту.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.core import cascade

NOW = datetime(2026, 8, 12, 18, 0, tzinfo=timezone.utc)


def classify(text, **kw):
    kw.setdefault("is_automatic_forward", False)
    kw.setdefault("author_is_bot", False)
    kw.setdefault("author_peer_id", 777)
    kw.setdefault("author_username", "someone")
    kw.setdefault("tg_date", NOW - timedelta(hours=1))
    return cascade.classify(text=text, now=NOW, **kw)


# Реплика по теме, но без слов «не работает» и без просьбы — ровно тот класс, ради
# которого L1 когда-то ужесточали и ради которого теперь существует L2.
CHATTER = "а вы через swift ещё платите или уже нет, интересно"


# ── L1 меняет строгость в зависимости от того, есть ли что дальше ──────────────

def test_l1_strict_when_nothing_follows():
    """Без L2 ступень L1 последняя, и тема без проблемы не должна становиться лидом."""
    v = classify(CHATTER, l2_enabled=False)
    assert v["passed"] is False and v["level"] == 1
    assert "нет ни признака проблемы" in v["detail"]["l1"]


def test_l1_lets_topic_through_when_l2_will_decide():
    """С включённым L2 та же реплика обязана дойти до ступени, которая понимает смысл.
    Иначе L2 разбирал бы только то, что и так нашли словами, и не добавлял ничего."""
    v = classify(CHATTER, l2_enabled=True)
    assert v["passed"] is None, "должно быть «ещё в пути», а не решение"
    assert v["level"] == 1


def test_awaiting_is_not_the_same_as_rejected():
    """Третье состояние `passed=None` — единственное, что отличает «не досчитали» от
    «не прошло». Без него недоступность эмбеддера навсегда убивала бы лиды."""
    v = classify("не могу оплатить инвойс, помогите пожалуйста", l2_enabled=True)
    assert v["passed"] is None
    assert "ожидает" in v["detail"]["l2"]
    assert "ожидает" in v["detail"]["l3"]


def test_disabled_stage_says_so_rather_than_staying_empty():
    v = classify("не могу оплатить инвойс, помогите пожалуйста", l2_enabled=False)
    assert v["passed"] is True
    assert "выключен" in v["detail"]["l2"]
    assert "выключен" in v["detail"]["l3"]


# ── L2: классификация по ближайшему эталону ───────────────────────────────────

def test_nearest_prototype_is_a_pain_so_it_passes():
    ranked = [("pos", "не может оплатить за рубеж", 0.81), ("neg", "офтоп", 0.70)]
    ok, why, pain, margin = cascade.level2(ranked)
    assert ok is True and pain == "не может оплатить за рубеж"
    assert round(margin, 3) == 0.11
    assert "0.81" in why and "отрыв" in why


def test_nearest_prototype_is_noise_so_it_is_dropped():
    ranked = [("neg", "болтовня по теме, проблемы нет", 0.79),
              ("pos", "не может оплатить за рубеж", 0.77)]
    ok, why, pain, _ = cascade.level2(ranked)
    assert ok is False and pain is None
    assert "болтовня" in why, "человек должен видеть, ЧЕМ это признано"


def test_tie_between_classes_is_refused_not_guessed():
    """Отрыв меньше порога — это «не знаю», и такое решение принимать нельзя:
    у bge-m3 сжатая шкала, и разница в тысячные не значит ничего."""
    ranked = [("pos", "нет валютного счёта или контракта", 0.8000),
              ("neg", "офтоп", 0.7995)]
    ok, why, pain, _ = cascade.level2(ranked)
    assert ok is False and pain is None
    assert "неуверенное" in why


def test_margin_is_measured_against_the_other_kind_not_the_next_row():
    """Второй в списке может быть такой же «болью» — сравнивать надо с ближайшим
    представителем ПРОТИВОПОЛОЖНОГО класса, иначе две похожие боли схлопнут отрыв
    в ноль и всё будет отбраковано."""
    ranked = [("pos", "не может оплатить за рубеж", 0.84),
              ("pos", "банк не пропускает платёж", 0.839),
              ("neg", "офтоп", 0.60)]
    ok, _, pain, margin = cascade.level2(ranked)
    assert ok is True and pain == "не может оплатить за рубеж"
    assert round(margin, 2) == 0.24


def test_no_prototypes_is_refusal_with_explanation():
    ok, why, pain, _ = cascade.level2([])
    assert ok is False and pain is None and why


def test_l2_verdict_flows_into_the_full_cascade():
    v = classify(CHATTER, l2_enabled=True,
                 ranked=[("neg", "болтовня по теме, проблемы нет", 0.80),
                         ("pos", "не может оплатить за рубеж", 0.70)])
    assert v["passed"] is False and v["level"] == 2
    assert "не запускался: отсеяно на L2" == v["detail"]["l3"]


# ── L3: вердикт модели ────────────────────────────────────────────────────────

def test_model_confirms_a_real_problem():
    ok, why = cascade.level3({"real_problem": True, "is_seller": False,
                              "answering_someone_else": False,
                              "why": "человек второй день не может провести платёж"})
    assert ok is True and "второй день" in why


def test_seller_is_refused_even_with_a_real_problem():
    """Исполнитель, у которого «болит» то же самое, — не покупатель. Правило живёт
    в коде, а не в промпте: словами в промпте его не покрыть тестом."""
    ok, why = cascade.level3({"real_problem": True, "is_seller": True,
                              "answering_someone_else": False,
                              "why": "сам проводит платежи за комиссию"})
    assert ok is False and "услуги" in why


def test_helper_of_someone_else_is_refused():
    ok, why = cascade.level3({"real_problem": True, "is_seller": False,
                              "answering_someone_else": True,
                              "why": "объясняет соседу порядок валютного контроля"})
    assert ok is False and "помогает другому" in why


def test_model_failure_is_a_refusal_with_the_reason_visible():
    ok, why = cascade.level3({"error": "в ответе нет JSON"})
    assert ok is False and "не ответила" in why


# ── L3: промпты v6+ (`is_target_lead` + `customer_type`) ──────────────────────
# Правило §3.5 разбора Gemini 14.09, по которому наборы v6–v8 измерялись офлайн:
# is_target_lead ∧ customer_type ∈ {company, unclear} ∧ ¬is_seller ∧ ¬answering.

V6 = {"is_target_lead": True, "customer_type": "company", "is_seller": False,
      "answering_someone_else": False, "why": "не проходит оплата поставщику"}


def test_v6_prompt_target_lead_passes_for_company_and_unclear():
    assert cascade.level3(V6)[0] is True
    assert cascade.level3({**V6, "customer_type": "unclear"})[0] is True


def test_v6_prompt_not_a_target_lead_is_no_problem():
    ok, why = cascade.level3({**V6, "is_target_lead": False})
    assert ok is False and "проблемы нет" in why


def test_v6_prompt_personal_purchase_is_refused():
    ok, why = cascade.level3({**V6, "customer_type": "personal"})
    assert ok is False and "физлица" in why


def test_v6_prompt_customer_type_is_required():
    """Тип клиента у промпта v6+ обязателен: без него правило не то, по которому
    набор измеряли, а значение вне перечня — сломанный ответ, не «unclear»."""
    without = {k: v for k, v in V6.items() if k != "customer_type"}
    assert cascade.level3(without)[0] is False
    assert cascade.level3({**V6, "customer_type": "business"})[0] is False


def test_v6_prompt_keeps_seller_and_helper_refusals():
    assert "услуги" in cascade.level3({**V6, "is_seller": True})[1]
    assert "помогает другому" in cascade.level3(
        {**V6, "answering_someone_else": True})[1]


def test_v6_key_decides_not_the_value():
    """`is_target_lead: false` рядом с `real_problem: true` — ответ «нет» от нового
    промпта, а не повод читать старое поле."""
    assert cascade.level3({**V6, "is_target_lead": False,
                           "real_problem": True})[0] is False


def test_v6_null_is_target_lead_does_not_fall_back_to_real_problem():
    """Сломанный ответ нового промпта (`is_target_lead: null`) — не «поле не
    задавали»: ключ на месте, и старое поле решать за него не вправе."""
    ok, why = cascade.level3({**V6, "is_target_lead": None, "real_problem": True})
    assert ok is False and "проблемы нет" in why


def test_v6_only_a_real_boolean_true_counts_as_yes():
    """Сравнение строго `is True`: строка «true» из неразобранного ответа — не «да»,
    иначе правило кода расходится с тем, чем набор измеряли офлайн."""
    ok, why = cascade.level3({**V6, "is_target_lead": "true"})
    assert ok is False and "проблемы нет" in why


def test_legacy_prompt_ignores_customer_type():
    """Старый промпт типа клиента не спрашивает — случайное поле решать не должно."""
    ok, _ = cascade.level3({"real_problem": True, "is_seller": False,
                            "answering_someone_else": False,
                            "customer_type": "personal"})
    assert ok is True


def test_public_profile_refuses_personal_purchase_too():
    """I8: отказ по типу клиента — не политика профиля, а правило промптов v6+.
    У публичного промпта этого поля нет, но если ответ v6+ когда-нибудь дойдёт до
    `PUBLIC_V1`, физлицо отсекается тем же словом, что и в личных сообщениях."""
    ok, why = cascade.level3({**V6, "customer_type": "personal"},
                             profile=cascade.PUBLIC_V1)
    assert ok is False and "физлица" in why


def test_public_profile_still_keeps_a_v6_seller():
    """Зеркальное I8: новое правило не сломало политику профиля — продавца
    публичный контур не отсекает и на ответах v6+, как и на старом промпте."""
    ok, why = cascade.level3({**V6, "is_seller": True}, profile=cascade.PUBLIC_V1)
    assert ok is True


def test_model_verdict_reaches_the_top_level_cascade():
    v = classify("не могу оплатить инвойс, второй день банк возвращает платёж",
                 l2_enabled=True, l3_enabled=True,
                 ranked=[("pos", "банк не пропускает платёж", 0.86),
                         ("neg", "офтоп", 0.60)],
                 llm={"real_problem": True, "is_seller": False,
                      "answering_someone_else": False, "why": "банк возвращает платёж"})
    assert v["passed"] is True and v["level"] == 3
    assert v["pain"] == "банк не пропускает платёж", "имя боли на выходе — от L2, а не от L1"
    assert v["score"] > 0 and v["breakdown"]
