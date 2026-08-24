"""Сценарии работы: чтение реестра и его первичное наполнение.

Сценарий описан не названием, а тремя осями (`target_kind`, `action`, `visibility`).
Смысл в том, чтобы код ветвился по форме, а не по имени: тогда третий и пятый сценарий
добавляются строкой в таблице, а не веткой в каждом `if`.

Здесь же — карта «форма → состав разделов интерфейса». Она живёт на бэкенде, а не во
фронтенде, по той же причине, по которой матрица прав живёт на бэкенде: интерфейс
только рисует то, что ему сказали, и его нельзя делать источником правды о том, что
существует.
"""
from __future__ import annotations

import logging

from sqlalchemy import select

from app.db.models import EngageInstance, Workflow
from app.services import engage_registry

logger = logging.getLogger(__name__)


# Разделы, которые есть у сценария каждой формы. Порядок — порядок в меню.
#
# «Переписки» есть только там, где переписка вообще существует: у публичного ответа
# её нет, там есть лента активности. Подменять одно другим нельзя — это разные вещи,
# и экран «переписок», собранный из одиночных комментариев, врал бы оператору.
SECTIONS_BY_ACTION = {
    "dm":    ("stream", "targets", "drafts", "conversations", "settings"),
    "reply": ("stream", "targets", "drafts", "activity", "settings"),
    "react": ("stream", "targets", "reactions", "activity", "settings"),
}

SECTION_TITLES = {
    "stream": "Поток",
    "targets": "Цели",
    "drafts": "Черновики",
    "reactions": "Реакции",
    "conversations": "Переписки",
    "activity": "Активность",
    "settings": "Настройки",
}


def sections_for(wf: Workflow) -> tuple[str, ...]:
    return SECTIONS_BY_ACTION.get(wf.action, ("stream", "targets", "settings"))


def describe(wf: Workflow) -> dict:
    """Форма сценария для интерфейса: по ней рисуется блок в боковом меню."""
    return {
        "id": wf.id,
        "key": wf.key,
        "title": wf.title,
        "target_kind": wf.target_kind,
        "action": wf.action,
        "visibility": wf.visibility,
        "is_active": wf.is_active,
        "sort_order": wf.sort_order,
        "sections": [{"key": s, "title": SECTION_TITLES[s]} for s in sections_for(wf)],
    }


def validate(wf: Workflow) -> list[str]:
    """Проверить осевые значения. Возвращает список проблем, пустой — значит всё цело.

    Проверка здесь, а не только в CHECK базы: ошибку в форме сценария надо показать
    человеку словами при сохранении, а не поймать нарушением ограничения на вставке
    первой цели через два часа.
    """
    problems = []
    if wf.target_kind not in Workflow.TARGET_KINDS:
        problems.append(f"target_kind={wf.target_kind!r} — ожидалось одно из "
                        f"{', '.join(Workflow.TARGET_KINDS)}")
    if wf.action not in Workflow.ACTIONS:
        problems.append(f"action={wf.action!r} — ожидалось одно из "
                        f"{', '.join(Workflow.ACTIONS)}")
    if wf.visibility not in Workflow.VISIBILITIES:
        problems.append(f"visibility={wf.visibility!r} — ожидалось одно из "
                        f"{', '.join(Workflow.VISIBILITIES)}")
    # Осей три, но не всякая тройка осмысленна: писать в личку можно только человеку,
    # а отвечать в треде и ставить реакцию — только сообщению.
    if wf.action == "dm" and wf.target_kind != "user":
        problems.append("action='dm' требует target_kind='user': в личку пишут человеку")
    if wf.action in ("reply", "react") and wf.target_kind != "message":
        problems.append(f"action={wf.action!r} требует target_kind='message': "
                        "цель такого действия — сообщение, а не человек")
    if wf.action == "dm" and wf.visibility != "private":
        problems.append("action='dm' не может быть публичным")
    return problems


async def active(db) -> list[Workflow]:
    return list((await db.execute(
        select(Workflow)
        .where(Workflow.is_active.is_(True))
        .order_by(Workflow.sort_order, Workflow.id))).scalars().all())


async def by_key(db, key: str) -> Workflow | None:
    return (await db.execute(
        select(Workflow).where(Workflow.key == key))).scalar_one_or_none()


async def ensure_bootstrap(db) -> bool:
    """Завести сценарий ЛС, если реестр пуст.

    Существующая установка работает ровно по нему, и до появления таблицы он
    подразумевался молча. Заводим его явно, чтобы данные, которые уже накоплены, было
    к чему привязать при переносе.

    Возвращает True, если строка была создана.
    """
    existing = (await db.execute(select(Workflow).limit(1))).scalar_one_or_none()
    if existing is not None:
        return False

    instance = (await db.execute(
        select(EngageInstance)
        .where(EngageInstance.key == engage_registry.BOOTSTRAP_KEY))).scalar_one_or_none()
    if instance is None:
        instance = (await db.execute(select(EngageInstance).limit(1))).scalar_one_or_none()
    if instance is None:
        # Реестр инстансов ещё не наполнен — значит и привязывать сценарий не к чему.
        # Не выдумываем: следующий старт сделает и то и другое по порядку.
        logger.warning("workflow_bootstrap_skipped reason=no_engage_instance")
        return False

    db.add(Workflow(
        key="cold_dm",
        title="Личные сообщения",
        target_kind="user",
        action="dm",
        visibility="private",
        engage_instance_id=instance.id,
        engage_use_case="cold_dm",
        cascade_profile="dm_v1",
        sort_order=10,
        is_active=True,
        description="Найти человека с болью в канале и написать ему в личные сообщения.",
    ))
    await db.commit()
    logger.info("workflow_bootstrapped key=cold_dm instance=%s", instance.key)
    return True
