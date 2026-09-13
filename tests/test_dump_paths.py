"""Строки `paths()` дампера фикстур — без базы (T-34).

Смоук отказывается работать, если для пути экрана в `api-fixtures.json` нет
образца, а образцы появляются только через `paths()`. Забыть строку там —
значит сломать проверку нового экрана молча, уже после выката. Поэтому каждый
новый путь держится именно этим тестом.

`paths()` обязана оставаться чистой от БД — она вызывается и из теста, и из
`main()` дампера, но читать базу ей нечем: только форматирование строк из
переданных `ids`. Ключи в `ids` — все, которые функция читает.
"""
from __future__ import annotations

from scripts.dump_gui_fixtures import paths


def test_dump_paths_include_new_routes():
    ids = {"draft_id": 1, "target_id": 2, "wf_draft_id": 3,
           "conversation_id": 4, "workflow_id": 5, "channel_id": 6}
    wanted, _ = paths(ids)
    keys = [key for key, _path in wanted]
    for expected in ("/automation", "/backfill/queue", "/discovery/candidates",
                     "/discovery/queries", "/channels/{id}"):
        assert expected in keys, expected

    # Подставной сегмент карточки: снимается с канала-донора из посева,
    # а не с выдуманного нуля.
    body = dict(wanted)
    assert body["/channels/{id}"] == "/channels/6"
