"""Грамматика ответа L3 выводится из промпта — и обязана совпадать с ним по полям.

Проверяется здесь главное свойство: грамматика не живёт отдельной жизнью от текста,
который читает модель. Промпт правится из интерфейса, наборы полей у контуров разные
(личный спрашивает шесть, публичный — семь), и разойдись они — модель послушно
ответила бы по грамматике на вопрос, которого ей не задавали, а в логах и вердиктах
это не оставило бы следа.

Второй смысловой центр — группировка перечисления. `|` в GBNF разделяет альтернативы
всего правила, поэтому `"low" | "medium" | "high"`, вставленное в `root` без скобок,
разрешает модели ответить одним словом «medium» вместо объекта.
"""
from __future__ import annotations

import json
import re

from app.services import llm
from app.services.llm_grammar import grammar_for

_TEMPLATE = re.compile(r"\{[^{}]*\}")
_KEY = re.compile(r'"([A-Za-z_][A-Za-z0-9_]*)"\s*:')


def _keys_asked_in(system: str) -> list[str]:
    for m in _TEMPLATE.finditer(system):
        keys = _KEY.findall(m.group(0))
        if len(keys) >= 2:
            return keys
    raise AssertionError("в промпте не нашёлся шаблон ответа")


def _keys_forced_by(grammar: str) -> list[str]:
    root = grammar.splitlines()[0]
    return re.findall(r'"\\"([A-Za-z_][A-Za-z0-9_]*)\\":"', root)


def test_every_prompt_yields_a_grammar():
    """Промпт без грамматики означает молчаливый возврат к медленному режиму: такой
    случай допустим для чужой правки, но не для промптов, лежащих в коде."""
    for asked in llm.PROMPTS.values():
        assert grammar_for(asked.system) is not None, asked.key


def test_grammar_forces_exactly_the_fields_the_prompt_asks_for():
    for asked in llm.PROMPTS.values():
        grammar = grammar_for(asked.system)
        assert _keys_forced_by(grammar) == _keys_asked_in(asked.system), asked.key


def test_field_sets_of_the_two_contours_really_differ():
    """Страховка от теста, который проходил бы и с одной грамматикой на всё."""
    dm = set(_keys_forced_by(grammar_for(llm.DM_V1.system)))
    public = set(_keys_forced_by(grammar_for(llm.PUBLIC_V1.system)))
    assert dm != public
    assert "answerable_briefly" in public and "answerable_briefly" not in dm


def test_enumeration_is_parenthesised():
    grammar = grammar_for(llm.DM_V1.system)
    root = grammar.splitlines()[0]
    assert '( "\\"low\\"" | "\\"medium\\"" | "\\"high\\"" )' in root
    assert root.count("(") == root.count(")")


def test_nullable_field_allows_null():
    grammar = grammar_for(llm.DM_V1.system)
    assert '"\\"pain\\":" ws strnull' in grammar
    assert 'strnull ::= str | "null"' in grammar


def test_enumeration_may_contain_null():
    """Форма боевого промпта `l3-verdict-v6`: перечисление вместе с `null`.

    Она не разбиралась, и одно такое поле отменяло грамматику всего промпта — на
    проде это давало 8 вердиктов L3 из 8 в обход быстрого пути, с единственным
    предупреждением в лог. Проверяется и то, что `null` идёт голым: закавычь его —
    и модель ответит строкой «null», на которой `parse_verdict` даст не то поле.
    """
    grammar = grammar_for(
        'Ответь так: {"real_problem": true|false, '
        '"disqualified": null|"vacancy"|"ad"|"spam"}')
    assert grammar is not None
    root = grammar.splitlines()[0]
    assert '( "null" | "\\"vacancy\\"" | "\\"ad\\"" | "\\"spam\\"" )' in root
    assert root.count("(") == root.count(")")


def test_null_without_any_literal_still_gives_no_grammar():
    """Страховка от того, чтобы разрешение `null` не превратилось в разрешение чего
    угодно: перечисление без единого значения — это не поле, а опечатка."""
    assert grammar_for('Ответь так: {"a": true|false, "b": null|null}') is None


def test_string_rule_forbids_raw_control_characters():
    """Грамматика, разрешающая сырой перевод строки внутри строки, пропускала бы
    ответы, на которых потом падает `json.loads`."""
    grammar = grammar_for(llm.DM_V1.system)
    assert r'char ::= [^"\\\n\r\t] | "\\" ["\\nrtbf/]' in grammar


def test_unused_rules_are_not_emitted():
    """У публичного контура нет полей с null — правила `strnull` быть не должно."""
    assert "strnull" not in grammar_for(llm.PUBLIC_V1.system)


def test_prompt_without_a_template_gives_no_grammar():
    assert grammar_for("Ответь словами, без всякого JSON.") is None


def test_unknown_value_spec_gives_no_grammar():
    """Непонятное описание значения обязано отключать грамматику целиком, а не
    выкидывать одно поле: объект без поля не разберётся у вызывающего."""
    assert grammar_for('Ответь так: {"a": true|false, "b": 0..10}') is None


def test_answer_under_grammar_is_read_from_reasoning_content():
    """Под грамматикой llama.cpp кладёт весь ответ в `reasoning_content`, а `content`
    оставляет пустым: шаблон Qwen открывает `<think>`, а закрыть его грамматика не
    даёт. Это не запасной путь, а единственный, и держаться он должен на проверке, а
    не на удаче — иначе первый же рефакторинг `_extract` обнулит весь L3.
    """
    answer = '{"real_problem": false, "is_seller": false, ' \
             '"answering_someone_else": false, "urgency": "low", ' \
             '"pain": null, "why": "объявление, а не вопрос"}'
    payload = {"choices": [{"message": {"content": "", "reasoning_content": answer}}]}
    parsed = llm.parse_verdict(llm._extract(payload))
    assert parsed["real_problem"] is False
    assert parsed["why"]


def test_answer_in_content_still_wins_over_reasoning():
    """Без грамматики ответ приходит в `content`, и рассуждение не должно его
    подменять — иначе выключатель `RADAR_LLM_GRAMMAR` менял бы не только скорость."""
    payload = {"choices": [{"message": {
        "content": '{"real_problem": true, "why": "прямой вопрос об инвойсе"}',
        "reasoning_content": "долгое рассуждение без JSON"}}]}
    assert llm.parse_verdict(llm._extract(payload))["real_problem"] is True


def test_verdict_shape_of_the_dm_contour_matches_what_cascade_reads():
    """Пример ответа, который грамматика разрешает, должен разбираться и содержать
    ровно те наблюдения, на которые смотрит `cascade.level3`."""
    sample = json.dumps({"real_problem": True, "is_seller": False,
                         "answering_someone_else": False, "urgency": "low",
                         "pain": None, "why": "видно прямой вопрос об оплате инвойса"},
                        ensure_ascii=False)
    parsed = llm.parse_verdict(sample)
    assert parsed["real_problem"] is True
    assert "error" not in parsed
