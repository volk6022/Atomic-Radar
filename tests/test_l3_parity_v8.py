"""Паритет I7: `level3` на проде решает ровно так, как харнесс `_v5replay --rule v6`.

Правку 10.0 принимали по замеру: на 291 разобранном ответе модели набора v8
(`replay_v8_parity`) вердикт кода совпал с правилом харнесса у всех. Здесь тот же
сравнение на срезе из 281 записи (`_REF-l3-parity-v8.jsonl` в корне репо: `id`,
`l3` — разобранный ответ модели, `passed_rule` — правило харнесса, `passed_code` —
вердикт кода на проде, `detail_l3` — строка причины). Файл под git не заведён
(тяжёлый слеп данных), поэтому в CI его не будет — тогда весь модуль честно
пропускается, а не падает.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.core import cascade

REF = Path(__file__).resolve().parents[1] / "_REF-l3-parity-v8.jsonl"
MISSING = (f"нет {REF.name} в корне репо — файл не под git, в CI его не будет; "
           "положи срез прогона `replay_v8_parity` рядом и перезапусти")

try:
    RECORDS = [json.loads(line) for line in REF.open(encoding="utf-8") if line.strip()]
except FileNotFoundError:
    RECORDS = None

if RECORDS is None:
    PARAMS = [pytest.param(None, None, id="reference-file-missing")]
else:
    PARAMS = [pytest.param(r["l3"], r["passed_rule"], id=str(r["id"]))
              for r in RECORDS]


@pytest.mark.parametrize("l3,passed_rule", PARAMS)
def test_verdict_matches_the_offline_rule(l3, passed_rule):
    """`level3(l3)[0] == passed_rule` на каждой записи среза — id в имени теста
    называет сообщение, на котором паритет разошёлся."""
    if RECORDS is None:
        pytest.skip(MISSING)
    assert cascade.level3(l3)[0] == passed_rule


@pytest.mark.skipif(RECORDS is None, reason=MISSING)
def test_seventeen_of_the_slice_pass():
    """«Прошло 17» — число лидов в срезе прибито: если правило кода и харнесса
    разошлись в сторону пропуска, сумма уедет незаметно для по-записного теста."""
    assert sum(1 for r in RECORDS if r["passed_rule"]) == 17


@pytest.mark.skipif(RECORDS is None, reason=MISSING)
def test_refusal_and_pass_reasons_keep_their_words():
    """Текст причины — часть контракта §2.2: отказ «не лид» у нового промпта
    объясняется «проблемы нет», проход — «настоящая проблема»."""
    for r in RECORDS:
        _, why = cascade.level3(r["l3"])
        if r["passed_rule"] is False and r["l3"].get("is_target_lead") is False:
            assert "проблемы нет" in why, f"запись {r['id']}: {why}"
        if r["passed_rule"] is True:
            assert "настоящая проблема" in why, f"запись {r['id']}: {why}"
