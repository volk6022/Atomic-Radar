"""Добавление сценария `public_reply` в боевой реестр `workflows`.

Зачем скрипт, а не просто рестарт. `workflows.ensure_bootstrap` (`app/services/workflows.py`)
работает по правилу «первый запуск, и только он»: если в таблице `workflows` есть хоть одна
строка, функция сразу возвращает `False` и ничего не делает. Так и должно быть — иначе
сценарий, выключенный или переименованный человеком, воскресал бы при каждом рестарте.
Следствие: на установке, где `cold_dm` давно существует, второй сценарий (`public_reply`)
с рестартом никогда не появится — строка в таблице уже есть, и bootstrap останавливается на
первой же проверке, даже не взглянув, чего в реестре не хватает.

Alembic в этом репозитории не заведён, поэтому правка боевых данных — идемпотентный
скрипт в `scripts/`, а не миграция с откатом одной командой.

Три решения, тем же способом, что и в `scripts/migrate_to_workflows.py`:

* **По умолчанию — сухой прогон.** Без `--apply` скрипт только считает и печатает, что
  сделал бы; ничего не пишет.
* **Идемпотентно.** Если `public_reply` в реестре уже есть, второй запуск не создаёт
  вторую строку и не трогает найденную — он сообщает, что делать нечего.
* **Выключен по умолчанию.** Сценарий заводится с `is_active=False`, пока не передан
  `--enable`. Включение второго контура означает, что по каждому входящему сообщению
  начинает считаться второй вердикт и появляются дополнительные обращения к языковой
  модели — это решение владельца установки, а не побочный эффект запуска скрипта.

Поля сценария скопированы из `scripts/seed_stand.py` — там пара `cold_dm` + `public_reply`
уже была описана (для стенда), и выдумывать значения заново незачем. Расхождения с
`ensure_bootstrap`, если они есть, — в docstring `build()` ниже.

Перед записью сценарий проходит `workflows.validate()` — те же проверки совместимости
осей, наличия профиля каскада в коде и промпта L3, что видит интерфейс. Невалидный
сценарий не попадает в базу, даже если передан `--apply`.

Запуск:

    docker exec api-radar python -m scripts.add_workflow                     # посмотреть
    docker exec api-radar python -m scripts.add_workflow --apply             # завести (выключенным)
    docker exec api-radar python -m scripts.add_workflow --apply --enable    # завести и включить
"""
from __future__ import annotations

import argparse
import asyncio
import logging
from dataclasses import dataclass

from sqlalchemy import select

from app.db.models import EngageInstance, Workflow
from app.db.session import get_session_maker
from app.services import engage_registry, workflows

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("add_workflow")

KEY = "public_reply"

# Поля сценария — как в `scripts/seed_stand.py`. Вынесены отдельным словарём (а не
# литералами внутри `build()`), чтобы их было видно и проверять одним взглядом на
# `scripts/seed_stand.py`, не читая тело функции.
#
# `description` там не задавался (в отличие от `cold_dm` в `ensure_bootstrap`, у
# которого есть). Здесь оставлено пустым по той же причине, по которой скрипт вообще
# не выдумывает значения: придумывать описание, которого не было в образце, значило бы
# внести решение, которое никто не принимал.
SPEC: dict = {
    "title": "Публичные ответы",
    "target_kind": "message",
    "action": "reply",
    "visibility": "public",
    "engage_use_case": "public_reply",
    "cascade_profile": "public_v1",
    # Строго больше, чем у `cold_dm` (10) — из поля строится порядок блоков в
    # сайдбаре, и меньшее значение подняло бы контур, который ещё ни разу не
    # отправлял, над личным. Значение и комментарий скопированы из seed_stand.py.
    "sort_order": 20,
}


class NoEngageInstanceError(RuntimeError):
    """В реестре нет ни одного инстанса Engage — заводить сценарий не к чему.

    Не подставляем сюда `-1` и не создаём инстанс сами: выбор боевого адреса Engage —
    решение о инфраструктуре, а не то, что скрипт вправе домыслить за оператора.
    """

    def __init__(self) -> None:
        super().__init__(
            "в таблице engage_instances нет ни одной строки — привязать сценарий не к "
            "чему. Сначала должен отработать engage_registry.ensure_bootstrap (обычно "
            "это происходит на старте приложения) или инстанс должен быть заведён "
            "вручную, потом запускайте этот скрипт заново"
        )


@dataclass
class Result:
    """Итог одного прогона — отдельно от печати, чтобы CLI и тесты не расходились
    в том, что считать успехом."""

    workflow: Workflow
    already_existed: bool
    problems: list[str]
    written: bool


async def _resolve_engage_instance(db) -> EngageInstance | None:
    """Инстанс Engage для привязки — тот же порядок поиска, что у
    `engage_registry.ensure_bootstrap`: сначала bootstrap-ключ, потом первый попавшийся.

    Одинаковый порядок — не стиль, а необходимость: два независимых способа выбрать
    «первый» инстанс молча разошлись бы на установке с несколькими инстансами Engage,
    и сценарии оказались бы привязаны к разным адресам без единой причины почему.
    """
    instance = (await db.execute(
        select(EngageInstance).where(
            EngageInstance.key == engage_registry.BOOTSTRAP_KEY))).scalar_one_or_none()
    if instance is None:
        instance = (await db.execute(select(EngageInstance).limit(1))).scalar_one_or_none()
    return instance


async def build(db, *, is_active: bool) -> Workflow:
    """Собрать сценарий `public_reply`, не сохраняя его в сессии.

    Поднимает `NoEngageInstanceError`, если привязать сценарий не к чему — отказ,
    а не запись с чем попало (см. требование к скрипту про привязку к Engage).
    """
    instance = await _resolve_engage_instance(db)
    if instance is None:
        raise NoEngageInstanceError()
    return Workflow(key=KEY, engage_instance_id=instance.id, is_active=is_active, **SPEC)


async def apply_workflow(db, *, is_active: bool, apply: bool) -> Result:
    """Посчитать, что нужно сделать, и — если `apply=True` — записать.

    Порядок проверок такой: сначала идемпотентность (не трогаем то, что уже есть),
    потом валидность (не пишем то, что заведомо сломано), и только затем запись.
    Невалидный сценарий не должен попасть в базу даже при `apply=True` — иначе флаг
    применения был бы обходом собственной проверки скрипта, а не подтверждением записи.
    """
    existing = await workflows.by_key(db, KEY)
    if existing is not None:
        return Result(workflow=existing, already_existed=True, problems=[], written=False)

    wf = await build(db, is_active=is_active)
    problems = workflows.validate(wf)
    if problems:
        return Result(workflow=wf, already_existed=False, problems=problems, written=False)

    if apply:
        db.add(wf)
        await db.commit()

    return Result(workflow=wf, already_existed=False, problems=[], written=apply)


async def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true",
                    help="действительно записать (по умолчанию — сухой прогон)")
    ap.add_argument(
        "--enable", action="store_true",
        help="завести сценарий сразу включённым (is_active=true). Без этого флага "
             "сценарий заводится выключенным: включение второго контура значит, что по "
             "каждому входящему сообщению начнёт считаться ещё один вердикт и появятся "
             "дополнительные обращения к языковой модели — это решение владельца "
             "установки, включайте явно, когда готовы")
    args = ap.parse_args()

    if not args.apply:
        log.info("СУХОЙ ПРОГОН — ничего не записывается. Для записи добавьте --apply")

    async with get_session_maker()() as db:
        try:
            result = await apply_workflow(db, is_active=args.enable, apply=args.apply)
        except NoEngageInstanceError as exc:
            raise SystemExit(f"отказ: {exc}") from exc

        if result.already_existed:
            log.info("сценарий %s уже есть в реестре (id=%s, is_active=%s) — менять "
                     "нечего", KEY, result.workflow.id, result.workflow.is_active)
            return

        if result.problems:
            log.error("сценарий %s не прошёл проверку, не записан:", KEY)
            for p in result.problems:
                log.error("  - %s", p)
            raise SystemExit(1)

        wf = result.workflow
        if args.apply:
            log.info("сценарий %s создан: id=%s target_kind=%s action=%s visibility=%s "
                     "cascade_profile=%s is_active=%s engage_instance_id=%s",
                     KEY, wf.id, wf.target_kind, wf.action, wf.visibility,
                     wf.cascade_profile, wf.is_active, wf.engage_instance_id)
        else:
            log.info("сценарий %s будет создан: target_kind=%s action=%s visibility=%s "
                     "cascade_profile=%s is_active=%s engage_instance_id=%s",
                     KEY, wf.target_kind, wf.action, wf.visibility, wf.cascade_profile,
                     wf.is_active, wf.engage_instance_id)


if __name__ == "__main__":
    asyncio.run(main())
