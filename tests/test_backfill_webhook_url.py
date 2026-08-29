"""Адрес возврата обязан влезать в поле, куда его кладёт Engage.

29.08 бэкфилл @CentrVED встал на первой же странице: `tasks.webhook_url` у Engage —
`varchar(500)`, а мы клали в адрес название группы. У кириллицы в percent-encoding
каждый символ занимает шесть, и «ВЭД чат (таможенное оформление, сертификация,
грузоперевозки, экспорт, импорт)» одно только съело больше четырёхсот символов.
Engage ответил `500` на вставку задачи, цепочка оборвалась, а прогон завис
«выполняется» — то есть отказ выглядел как что угодно, только не как «адрес длинный».

Название в адресе и не нужно: строка канала уже заведена шагом `chat_info`, а
`get_or_create_channel` при пустом названии оставляет существующее.
"""
import os
from urllib.parse import parse_qs, urlparse

os.environ.setdefault("RADAR_SECRET_KEY", "x" * 32)
os.environ.setdefault("RADAR_DATABASE_URL", "postgresql+asyncpg://u:p@localhost/db")
os.environ.setdefault("RADAR_INGEST_TOKEN", "t" * 24)

from app.api.v1.ingest import _webhook_url  # noqa: E402

# Настоящее название той самой группы, на которой всё и сломалось.
LONG_TITLE = ("ВЭД чат (таможенное оформление, сертификация, грузоперевозки, "
              "экспорт, импорт)")

# Ограничение чужой схемы: `tasks.webhook_url VARCHAR(500)` в fleet_manager.
ENGAGE_WEBHOOK_URL_LIMIT = 500


def _history_url() -> str:
    return _webhook_url(kind="history", peer_id=-1002102849363,
                        username="CentrVED_chat", account_id=5, limit=500,
                        target=2000, prev_cursor=668759, run_id=4)


def test_the_callback_url_fits_the_column_engage_stores_it_in():
    assert len(_history_url()) < ENGAGE_WEBHOOK_URL_LIMIT


def test_the_callback_url_carries_no_channel_title():
    """Название — единственный параметр без потолка длины, и класть его туда нельзя."""
    params = parse_qs(urlparse(_history_url()).query)
    assert "title" not in params


def test_a_long_cyrillic_title_would_have_broken_the_limit():
    """Проверка самой причины, а не только следствия.

    Без неё правку легко откатить «за ненадобностью»: адрес и с названием выглядит
    коротким, пока название латинское и короткое.
    """
    with_title = _webhook_url(kind="history", peer_id=-1002102849363,
                              username="CentrVED_chat", title=LONG_TITLE,
                              account_id=5, limit=500, target=2000,
                              prev_cursor=668759, run_id=4)
    assert len(with_title) > ENGAGE_WEBHOOK_URL_LIMIT


def test_everything_needed_to_place_the_page_is_still_there():
    """Убрали лишнее, но не нужное: без этих полей страницу не к чему привязать."""
    params = parse_qs(urlparse(_history_url()).query)
    for key in ("kind", "peer_id", "account_id", "limit", "target",
                "prev_cursor", "run_id"):
        assert key in params, f"без «{key}» цепочку не продолжить"
