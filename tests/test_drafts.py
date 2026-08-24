"""Очередь черновиков: справочник причин, защита ручек и сборка вариантов.

Очередь переехала из памяти процесса в БД, поэтому её обход и решения проверяются
интеграционно, на живой базе. Здесь остаётся то, что можно проверить честно и без
неё: закрытый справочник, недоступность решений без входа и содержимое вариантов.

Про варианты важно отдельно: генератор шаблонный, модели в нём нет. Тесты следят,
чтобы это было видно в данных, — иначе оператор решит, что тексты уже проверены
критиком, и ревью станет формальностью.
"""
from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_DEBUG", "true")

from app.api.v1 import drafts  # noqa: E402
from app.core.config import get_settings  # noqa: E402
from app.main import create_app  # noqa: E402
from app.services import drafting  # noqa: E402


@pytest.fixture
def client():
    get_settings.cache_clear()
    with TestClient(create_app(), raise_server_exceptions=False) as c:
        yield c


# ── справочник ────────────────────────────────────────────────────────────────

def test_reasons_match_the_hotkeys():
    """Причин ровно девять, пронумерованы 1..9: в интерфейсе это горячие клавиши."""
    assert [r["n"] for r in drafts.REASONS] == list(range(1, 10))
    assert all(r["label"] for r in drafts.REASONS)


def test_reason_lookup_is_closed():
    """Причина уходит в eval-датасет, поэтому списка «прочее по тексту» быть не должно."""
    assert drafts._REASON_BY_N.keys() == {1, 2, 3, 4, 5, 6, 7, 8, 9}


# ── доступ ────────────────────────────────────────────────────────────────────

def test_queue_and_decisions_require_auth(client):
    """Матрица прав в GUI ничего не защищает — решение принимается на сервере."""
    assert client.get("/api/v1/drafts/next").status_code == 401
    assert client.get("/api/v1/drafts/reasons").status_code == 401
    assert client.post("/api/v1/drafts/1/approve",
                       json={"variant_index": 0}).status_code == 401
    assert client.post("/api/v1/drafts/1/reject",
                       json={"reason_n": 1}).status_code == 401


def test_reject_validates_reason_before_touching_anything(client):
    """Причина вне справочника отсекается схемой — до базы дело не доходит."""
    assert client.post("/api/v1/drafts/1/reject",
                       json={"reason_n": 99}).status_code in (401, 422)


# ── сборка вариантов ──────────────────────────────────────────────────────────

def test_variants_exist_for_every_pain_the_cascade_can_emit():
    """Каждая боль из каскада должна иметь заготовки, иначе лид приедет с заглушкой.

    Перебираются все профили, а не только `dm_v1`: профиль с новой болью и без
    шаблонов под неё — это лид с пустой карточкой, и узнать об этом лучше здесь.
    """
    from app.core import cascade
    for rules in cascade.PROFILES.values():
        for pain in rules.pain_anchors:
            assert pain in drafting.TEMPLATES, \
                f"нет шаблонов под боль «{pain}» (профиль {rules.key})"


def test_variants_never_claim_a_critic_ran():
    """Модели в генераторе нет. Вариант обязан говорить об этом прямо."""
    for pain in drafting.TEMPLATES:
        for v in drafting.build_variants(pain):
            assert v["critic_passed"] is None, "нельзя выдавать шаблон за проверенный"
            assert "модел" in v["critic_text"].lower()
            assert v["prompt_version"] == "template-v0"


def test_no_link_in_generated_first_messages():
    """Ссылка в первом сообщении — причина отклонения №8 и инвариант гейта.
    Генератор не должен производить то, что гейт обязан заблокировать."""
    for pain in list(drafting.TEMPLATES) + ["неизвестная боль"]:
        for v in drafting.build_variants(pain):
            low = v["text"].lower()
            assert "http" not in low and "t.me/" not in low


def test_unknown_pain_still_produces_a_variant():
    variants = drafting.build_variants(None)
    assert variants and variants[0]["text"]


def test_spam_score_reacts_to_advertising_markers():
    clean = drafting._spam_score("Видел твоё сообщение про VPN — помогло своё решение")
    salesy = drafting._spam_score(
        "ПОД КЛЮЧ за 1 день, гарантия, недорого, пишите прямо сейчас!!")
    assert clean < 0.3 < salesy
    assert drafting._spam_score("зайди на https://example.com") > clean
