"""Грамматика GBNF для ответа L3, выведенная из текста самого промпта.

Зачем грамматика. Qwen3.5-9B — модель рассуждающая, и отключить рассуждение штатными
средствами нельзя: замер 07.09 на сорока настоящих запросах прода показал, что
`reasoning_budget: 0` и `chat_template_kwargs.enable_thinking` игнорируются молча
(счётчики токенов совпадают с базовыми побайтно), а `response_format` любого вида
даёт чистый JSON, но рассуждение оставляет на месте и скорости не прибавляет.
Грамматика — единственный рычаг, который сработал: она требует, чтобы первым же
токеном шла `{`, и рассуждать модели становится негде. Цена вопроса — 2,15 с против
6,03 с и 99 токенов против 434 при полном совпадении вердикта по `real_problem`
(39 из 39 пар).

Почему грамматика выводится из текста промпта, а не объявлена списком полей рядом.
Промпт правится из интерфейса (`cascade_registry.save_prompt`), и у контуров он
разный: личный спрашивает шесть полей, публичный — семь, причём наборы не совпадают.
Список полей, живущий отдельно от текста, разошёлся бы с ним на первой же правке, и
разошёлся бы молча: модель послушно ответила бы по грамматике на вопрос, которого ей
не задавали. Единственный источник истины здесь — тот же текст, который читает
модель, поэтому шаблон ответа разбирается прямо из него.

Если разобрать шаблон не удалось, грамматика не строится вовсе и запрос уходит как
раньше — со свободным текстом и разбором JSON из него. Это осознанная деградация до
прежнего поведения, а не отказ: промпт, написанный в непривычной форме, должен
работать хуже, а не переставать работать.
"""
from __future__ import annotations

import re
from functools import lru_cache

_TEMPLATE = re.compile(r"\{[^{}]*\}")
_KEY = re.compile(r'"([A-Za-z_][A-Za-z0-9_]*)"\s*:')

_BASE_RULES = {
    "bool": r'bool ::= "true" | "false"',
    "str": r'str ::= "\"" char* "\""',
    "strnull": r'strnull ::= str | "null"',
    # Управляющие символы исключены не для красоты: json.loads со значениями по
    # умолчанию не принимает сырой перевод строки внутри строки, и ответ, прошедший
    # грамматику, всё равно оказался бы неразбираемым.
    "char": r'char ::= [^"\\\n\r\t] | "\\" ["\\nrtbf/]',
    "ws": r"ws ::= [ \n]*",
}

_MIN_FIELDS = 2


class GrammarError(ValueError):
    """Шаблон ответа в промпте не разобрался. Ловится вызывающим: грамматики просто
    не будет."""


def _template_of(system: str) -> str:
    for m in _TEMPLATE.finditer(system):
        body = m.group(0)[1:-1]
        if len(_KEY.findall(body)) >= _MIN_FIELDS:
            return body
    raise GrammarError("в промпте нет шаблона ответа вида {\"поле\": ...}")


def _fields_of(body: str) -> list[tuple[str, str]]:
    marks = list(_KEY.finditer(body))
    fields: list[tuple[str, str]] = []
    for i, m in enumerate(marks):
        end = marks[i + 1].start() if i + 1 < len(marks) else len(body)
        spec = body[m.end():end].strip().rstrip(",").strip()
        fields.append((m.group(1), spec))
    return fields


def _rule_for(name: str, spec: str) -> str:
    alts = [a.strip() for a in spec.split("|")]
    if set(alts) == {"true", "false"}:
        return "bool"
    if len(alts) == 1 and alts[0].startswith('"<') and alts[0].endswith('>"'):
        return "strnull" if "null" in alts[0] else "str"
    if len(alts) > 1 and all(len(a) > 2 and a.startswith('"') and a.endswith('"')
                             and "<" not in a for a in alts):
        # Скобки обязательны: в GBNF `|` разделяет альтернативы всего правила, и
        # перечисление, вставленное в `root` без группировки, означало бы «либо всё
        # начало объекта, либо одно слово „medium“».
        return "( " + " | ".join('"\\"%s\\""' % a[1:-1] for a in alts) + " )"
    raise GrammarError(f"поле «{name}»: непонятное описание значения «{spec}»")


@lru_cache(maxsize=32)
def grammar_for(system: str) -> str | None:
    """GBNF по системному промпту либо `None`, если шаблон ответа не разобрался."""
    try:
        fields = _fields_of(_template_of(system))
        rules = [(name, _rule_for(name, spec)) for name, spec in fields]
    except GrammarError:
        return None

    parts = []
    for i, (name, rule) in enumerate(rules):
        sep = '"," ws ' if i else ""
        parts.append(f'{sep}"\\"{name}\\":" ws {rule} ')
    lines = ['root ::= "{" ws ' + "".join(parts) + 'ws "}"']

    used = {"ws"}
    for _, rule in rules:
        for base in ("strnull", "str", "bool"):
            if base in rule:
                used.add(base)
    if "str" in used or "strnull" in used:
        used.update({"str", "char"})
    lines.extend(_BASE_RULES[name] for name in _BASE_RULES if name in used)
    return "\n".join(lines)
