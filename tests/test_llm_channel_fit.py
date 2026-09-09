"""Параметры `system=`/`user=` у `llm.verdict` и промпт channel_fit_v1.

Контур оценки канала (поиск похожих каналов) в существующий вызов не влезает:
его системный промпт собирается в момент вызова из описания бизнеса, а
пользовательское сообщение — карточка канала, а не «разбираемое сообщение».
Правка — ровно два необязательных параметра, поэтому главный тест здесь
закрепляет обратное: без новых параметров тело запроса обязано остаться
прежним до байта — от этой правки не должны сдвинуться два работающих контура.

Сеть в тестах не трогается: подменяется сам клиент `llm._client`, у вызова
один вход наружу, и честнее перехватить его, чем пересобирать запрос.
"""
from __future__ import annotations

import json

import httpx
import pytest

from app.core.config import get_settings  # noqa: E402
from app.services import llm  # noqa: E402
from app.services.llm_grammar import grammar_for  # noqa: E402

# Ответ модели, до которого проверкам тела запроса дела нет: перехватчик возвращает
# его на любой запрос, тест смотрит только на то, что ушло.
_RESPONSE = {"choices": [{"message": {"content": '{"verdict": "fit", "score": "80", '
                                      '"reason": "тематика совпадает"}'}}]}

TEXT = "Нужен сервис для оплаты зарубежного инвойса, банк не пропускает платёж"
CONTEXT = ["Кто-нибудь пользовался? Тоже нужна оплата счёта за границей"]


@pytest.fixture(autouse=True)
def _llm_settings(monkeypatch):
    """Детерминированные настройки LLM на время теста.

    `get_settings` кешируется на процесс, и соседние тесты могут оставить в кеше
    свои переменные окружения. Гасим кеш и до, и после: после — чтобы наши
    значения не доехали до соседних тестов.
    """
    monkeypatch.setenv("RADAR_LLM_BASE_URL", "http://llm.test")
    monkeypatch.setenv("RADAR_LLM_MODEL", "qwen-local")
    monkeypatch.setenv("RADAR_LLM_MAX_TOKENS", "1200")
    monkeypatch.setenv("RADAR_LLM_GRAMMAR", "true")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
async def sent(monkeypatch):
    """Перехват HTTP: запросы не уходят в сеть, тела копятся в возвращённый список."""
    bodies = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(json.loads(request.content))
        return httpx.Response(200, json=_RESPONSE)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler),
                               base_url="http://llm.test")
    monkeypatch.setattr(llm, "_client", client)
    yield bodies
    await client.aclose()


async def test_verdict_without_new_parameters_builds_the_old_request(sent):
    """Главный тест: вызов без новых параметров формирует тело запроса, равное
    прежнему целиком — промпт из реестра dm_v1, пользовательский текст от
    `build_prompt`, грамматика от того же текста. Один лишний или переставленный
    ключ в теле — и работающие контуры поехали бы по-другому."""
    await llm.verdict(text=TEXT, context=CONTEXT)

    [body] = sent
    assert body == {
        "model": get_settings().LLM_MODEL,
        "messages": [
            {"role": "system", "content": llm.prompt("dm_v1").system},
            {"role": "user",
             "content": llm.build_prompt(text=TEXT, context=CONTEXT)},
        ],
        "temperature": 0.0,
        "max_tokens": get_settings().LLM_MAX_TOKENS,
        "grammar": grammar_for(llm.prompt("dm_v1").system),
    }


async def test_system_overrides_the_message_and_the_grammar_source(sent):
    """`system=` подменяет и системное сообщение, и источник грамматики.

    Грамматика выводится из текста, который реально слышит модель; оставить
    источник в реестре значило бы требовать поля ответа на вопрос, которого
    не задавали."""
    custom = ('Ответь так: {"verdict": "fit"|"unfit"|"unclear", '
              '"reason": "<одно предложение по-русски>"}')
    await llm.verdict(text=TEXT, context=[], system=custom)

    [body] = sent
    assert body["messages"][0] == {"role": "system", "content": custom}
    assert body["grammar"] == grammar_for(custom)
    # Страховка от теста, который прошёл бы и со старой грамматикой: у dm_v1
    # набор полей другой, совпадать нечему.
    assert body["grammar"] != grammar_for(llm.prompt("dm_v1").system)


async def test_user_overrides_the_user_message_and_ignores_text_and_context(sent):
    """`user=` идёт в запрос как есть: карточка канала не должна обрамляться
    формулировками «Соседние сообщения» и «Разбираемое сообщение» — они про
    один сообщение из чата, а не про карточку."""
    card = "Канал: @bank, 2 700 000 участников, тематика — финансы"
    await llm.verdict(text=TEXT, context=CONTEXT, user=card)

    [body] = sent
    assert body["messages"][1] == {"role": "user", "content": card}
    assert "Соседние сообщения" not in body["messages"][1]["content"]
    assert "Разбираемое сообщение" not in body["messages"][1]["content"]


def test_channel_fit_v1_is_registered():
    asked = llm.prompt("channel_fit_v1")
    assert asked.version == "channel-fit-v1"
    assert asked.system == llm.CHANNEL_FIT_SYSTEM


def test_channel_fit_grammar_forces_all_three_fields():
    """Грамматика обязана требовать объект со всеми тремя полями, а перечисление
    вердикта — стоять в скобках: без скобок `|` разделяет альтернативы всего
    правила, и модель вправе ответить одним словом «fit» вместо объекта."""
    grammar = grammar_for(llm.CHANNEL_FIT_SYSTEM)
    assert grammar is not None
    root = grammar.splitlines()[0]
    for field in ("verdict", "score", "reason"):
        assert f'"\\"{field}\\":"' in root, field
    assert '( "\\"fit\\"" | "\\"unfit\\"" | "\\"unclear\\"" )' in root


def test_placeholder_survives_braces_in_the_business_description():
    """Описание бизнеса — свободный текст оператора, скобки в нём не редкость
    (примеры, шаблоны писем). Подстановка обязана их переживать.

    На `.format` тест падает намеренно: в шаблоне промпта литеральные скобки
    шаблона ответа, и `.format` на этом тексте падает с KeyError. Рецепт —
    только `.replace`."""
    descr = "продаём отчёты вида {выручка} и шаблоны } обратные {"

    built = llm.CHANNEL_FIT_SYSTEM.replace("{business_description}", descr)
    assert descr in built
    assert "{business_description}" not in built

    with pytest.raises((KeyError, IndexError, ValueError)):
        llm.CHANNEL_FIT_SYSTEM.format(business_description=descr)
