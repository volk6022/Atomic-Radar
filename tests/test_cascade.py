"""Каскад L0→L1 на примерах, взятых из настоящих чатов.

Проверяется не «функция что-то вернула», а поведение, которое обсуждалось как
требование: пост канала не человек, бот не человек, «+1» не боль, а объяснение
причины есть у каждой ступени — экран потока без него бесполезен.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.core import cascade

NOW = datetime(2026, 8, 11, 18, 0, tzinfo=timezone.utc)


def classify(text, **kw):
    kw.setdefault("is_automatic_forward", False)
    kw.setdefault("author_is_bot", False)
    kw.setdefault("author_peer_id", 777)
    kw.setdefault("author_username", "someone")
    kw.setdefault("tg_date", NOW - timedelta(hours=1))
    return cascade.classify(text=text, now=NOW, **kw)


# ── L0 ────────────────────────────────────────────────────────────────────────

def test_channel_post_mirrored_into_discussion_is_dropped():
    """Автопересылка поста канала — корень ветки, а не реплика. Отвечать на неё
    значит писать в пустоту: адресата у такого сообщения нет."""
    v = classify("Как платить за рубеж в 2026 — разбираем в комментариях",
                 is_automatic_forward=True)
    assert v["passed"] is False and v["level"] == 0
    assert "автопересылка" in v["detail"]["l0"]


def test_bot_is_dropped():
    v = classify("Дайджест изменений валютного контроля — читайте в канале",
                 author_is_bot=True)
    assert v["passed"] is False and "бот" in v["detail"]["l0"]


def test_short_message_is_dropped():
    v = classify("+1, тоже думаю")
    assert v["passed"] is False and v["level"] == 0


def test_bare_link_is_dropped():
    v = classify("https://example.com/very/long/path/that/is/long")
    assert v["passed"] is False and "ссылка" in v["detail"]["l0"]


def test_anonymous_admin_has_no_author():
    v = classify("не могу оплатить инвойс поставщику, банк заворачивает",
                 author_peer_id=None)
    assert v["passed"] is False and "автор" in v["detail"]["l0"]


# ── L1 ────────────────────────────────────────────────────────────────────────

def test_topic_without_a_problem_is_not_a_lead():
    """Правило добыто первым живым прогоном: одного тематического слова хватало, чтобы
    лидом стала праздная реплика. Тема без проблемы — это разговор, а не боль, и смена
    предметной области здесь ничего не поменяла: «инвойс» и «swift» произносят в чате
    так же походя, как любое другое слово из отраслевого словаря."""
    for text in ("да, инвойс в евро прислали, обычная для нас история",
                 "а вы через swift платите или как-то иначе сейчас"):
        v = classify(text)
        assert v["passed"] is False, text
        assert "нет ни признака проблемы" in v["detail"]["l1"]


def test_age_does_not_decide_whether_it_is_a_lead():
    """Сухой прогон идёт по истории, и у любого сообщения из прошлого свежесть равна
    нулю. Если бы порог считался по полной сумме, история отбраковывалась бы просто
    за то, что она история."""
    text = "всем привет, подскажите как оплатить инвойс поставщику из китая?"
    fresh = classify(text, tg_date=NOW - timedelta(hours=2))
    old = classify(text, tg_date=NOW - timedelta(days=90))
    assert fresh["passed"] is True and old["passed"] is True
    assert old["score"] < fresh["score"], "но в очереди свежий должен стоять выше"


def test_offtopic_passes_l0_but_dies_on_l1():
    v = classify("а вы какой стек берёте для MVP? думаю между next и remix")
    assert v["level"] == 1 and v["passed"] is False
    assert v["detail"]["l0"].startswith("не пост канала")
    assert "якор" in v["detail"]["l1"]


def test_real_pain_passes():
    v = classify("ребят, задолбался: банк отказал, платёж за рубеж не проходит "
                 "второй раз, кто может посоветовать вариант?")
    assert v["passed"] is True
    assert v["pain"] == "не может оплатить за рубеж"
    assert v["score"] > 40, "явная боль + интент + срочность должны дать заметный скор"


def test_every_stage_says_what_happened():
    """Ни одна ступень не остаётся без объяснения — включая те, что не запускались."""
    for text, _ in (("+1", None),
                    ("нужен агент, у нас платёж за рубеж завис второй день", None)):
        v = classify(text)
        for stage in ("l0", "l1", "l2", "l3"):
            assert v["detail"][stage], f"ступень {stage} без объяснения для {text!r}"


def test_stages_that_did_not_run_are_not_silently_passed():
    """Выключенная ступень обязана сказать это словами, иначе пустая строка на экране
    прочитается как «ступень пройдена». Проверка пережила появление L2/L3: раньше
    ступеней не существовало, теперь они бывают выключены — требование то же."""
    v = classify("не могу оплатить инвойс поставщику, помогите разобраться")
    assert v["passed"] is True
    assert "не запускался" in v["detail"]["l2"]
    assert "не запускался" in v["detail"]["l3"]


def test_ved_anchor_does_not_swallow_okved():
    """Ведущий пробел в якоре `" вэд"` — не опечатка, а вся его работа.

    Без пробела якорь ловит «ОКВЭД», а код ОКВЭД в чате юрлиц называют постоянно и
    совершенно не по нашему поводу: на живом прогоне это дало три ложных попадания
    из семи. Пробел стоит того, чтобы его случайно не «починили».
    """
    okved = "основной оквэд у нас 62.01, никаких проблем"
    assert cascade.level1(okved, strict=False)[3] == [], "ОКВЭД — не наша тема"

    # И даже вместе с признаком проблемы: якорь не должен появляться от того, что
    # рядом стоит «не проходит».
    assert cascade.level1("оквэд поменяли, а платёж всё равно не проходит",
                          strict=False)[0] is False

    ved = "везём по тн вэд 8517, банк требует документы"
    ok, _, pain, hits = cascade.level1(ved, strict=False)
    assert ok is True and hits == [" вэд"]
    assert pain == "нет валютного счёта или контракта"


# ── скор и дисквалификаторы ───────────────────────────────────────────────────

def test_no_username_costs_reachability():
    """Без username в личку не написать — это должно быть видно в разборе оценки."""
    text = "ищу, через кого оплатить инвойс поставщику, нужен рабочий вариант"
    with_name = classify(text)
    without = classify(text, author_username=None)
    def reach(v):
        return next(b["value"] for b in v["breakdown"]
                    if b["label"] == "достижимость в ЛС")
    assert reach(with_name) == 6 and reach(without) == 0
    assert without["score"] < with_name["score"]


def test_stale_message_scores_lower():
    text = "задолбался с валютным контролем, посоветуйте через кого платить"
    fresh = classify(text, tg_date=NOW - timedelta(hours=1))
    old = classify(text, tg_date=NOW - timedelta(days=30))
    assert old["score"] < fresh["score"]


def test_seller_is_flagged_not_dropped():
    """Автор сам продаёт такие же услуги — писать ему предложение бессмысленно.
    Но это пометка для человека, а не автоматический отсев."""
    v = classify("Помогу с оплатой инвойсов за рубеж под ключ, недорого, пишите в лс")
    assert v["passed"] is True, "фраза проходит L1: тема + «помог»"
    assert "сам продаёт услугу" in v["disqualifiers"]


def test_breakdown_sums_to_score():
    v = classify("у нас в компании срочно нужен вариант, платёж за рубеж не проходит")
    assert sum(b["value"] for b in v["breakdown"]) == v["score"]
    assert v["score"] <= 100
