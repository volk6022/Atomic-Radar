"""Единая трактовка отказа и откладывания Engage — одно место решения (R3).

До этого модуля вопрос «что значит брошенное Engage исключение» решался в пяти
местах, и каждое — по-своему: разбор групп discussion ловил `EngageTaskDeferred`
руками, discovery-карточка — другими руками, а `run_scan` не ловил вовсе и ронял
прогон. Пять трактовок одного события означали пять поведений, и каждое правилось
отдельно — ровно та цена, которую платят за отсутствие одного словаря.

Классификация проведена по вине канала и по тому, вернётся ли работа сама:

* **deferred** — «лимит кончился, Engage вернётся сам». Это не отказ и не падение:
  перепланировка у Engage — каждые 30 с, повторный defer даёт повторное событие.
  Поля строк каналов/кандидатов не меняются, тревог не поднимается.
* **unavailable** — не вина канала, последствия те же, но кода лимита нет: ждать
  возврата нечего, пока сеть не поднимется.
* **failed** — отказ. Приватный канал, «слишком много каналов», флуд-контроль —
  данные о решении, их записывают.

`EngageTaskDeferred` — наследник `EngageTaskFailed`, поэтому проверка идёт от
частного к общему: сначала deferred, потом failed. Перепутать порядок значило бы
считать отложенное отказом — та самая ошибка, ради исчезновения которой модуль и
заведён.
"""
from __future__ import annotations

from dataclasses import dataclass

from app.services import engage

# Виды трактовки. Значение уходит в статистику прогона и в статус строки `runs`,
# поэтому список закрытый: новый вид — новое решение о последствиях, а не строка
# в логе.
KINDS = ("deferred", "unavailable", "failed")


@dataclass(frozen=True)
class Interpretation:
    """Чем является исключение Engage для вызвавшего.

    `kind` — вид (см. `KINDS`); `code` — код Engage (`error_code` задачи; для
    deferred обязателен — без него отчёт не отличит read-бюджет от бюджета
    вступлений); `note` — человекочитаемая строка для лога прогона: она едет в
    отчёт, где читает человек, а не код.
    """

    kind: str          # "deferred" | "unavailable" | "failed"
    code: str | None   # код Engage (error_code), для deferred обязателен
    note: str          # человекочитаемая строка для лога прогона


def interpret(exc: BaseException) -> Interpretation:
    """Трактовать исключение Engage по таблице из трёх строк.

    Прочее (не-Engage) исключение — отказ с именем класса вместо кода: у него
    нет кода Engage, но и «отложенным» назвать его нельзя — само оно не вернётся.
    """
    if isinstance(exc, engage.EngageTaskDeferred):
        code = exc.code
        why = f"кончился дневной лимит ({code})" if code else "кончился дневной лимит"
        return Interpretation("deferred", code, f"{why} — Engage вернётся сам")
    if isinstance(exc, engage.EngageUnavailable):
        return Interpretation("unavailable", None, f"Engage недоступен — {exc}")
    if isinstance(exc, engage.EngageTaskFailed):
        code = exc.code or "без кода"
        return Interpretation("failed", exc.code,
                              f"задача Engage не выполнена ({code}) — {exc}")
    return Interpretation("failed", type(exc).__name__,
                          f"{type(exc).__name__}: {exc}")
