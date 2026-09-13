"""Ручки автоматики (план 13.7, волна Е1): сводка сценариев и настройки.

Экран «Автоматика» читает один GET: сводка `autoflow.status()` — настройки и
четыре сценария сразу. Отдельной ручки на сценарий нет: экрану нужен весь кадр
(включатели рядом с последними прогонами), а выдавать его кусками значило бы
учить фронтенд склеивать четыре ответа в один и разъезжаться с сервисом в
порядке обновления.

Настройки пишет только владелец (`Capability.CONFIG_EDIT`): каждый включатель
включает траты — чтения аккаунтов, вопросы L3, вступления. Обновление
частичное: непереданные ключи не трогаются — экран шлёт только изменённое
поле, и «обнулить всё, что не пришло» превратило бы сохранение одной галочки
в выключение остальных семи.

Тело POST разбирается моделью внутри ручки, а не параметром FastAPI: чужой
ключ обязан вернуть 422 с перечнем известных (иначе владелец видит
«extra inputs are not permitted» без намёка, как правильно), а стандартный
текст ошибки pydantic этого перечня не содержит. Проверка значений — та же
`autoflow.validate_settings`, которой импорт набора настроек валидирует блок
`automation`: ручка и импорт обязаны отказывать по одним правилам.
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from app.api.deps import GetDB, permits, requires
from app.core.access import Capability, Section
from app.db.models import AuditLog
from app.services import autoflow

router = APIRouter(prefix="/api/v1/automation", tags=["automation"])


class _SettingsBody(BaseModel):
    """Плоский словарь «ключ → значение» из §4.2 контракта.

    Строгие поля: в lax-режиме pydantic молча превратил бы `true` в 1, и
    выключатель включился бы значением, которое владелец не вводил — bool
    не число. `extra="forbid"` ловит опечатку в имени ключа отказом, а не
    молчаливым игнорированием.
    """
    model_config = ConfigDict(extra="forbid")

    autoflow_join_backfill_enabled: int | None = Field(default=None, strict=True)
    autoflow_backfill_depth_days: int | None = Field(default=None, strict=True)
    autoflow_backfill_target: int | None = Field(default=None, strict=True)
    autoflow_reclassify_enabled: int | None = Field(default=None, strict=True)
    autoflow_reclassify_interval_min: int | None = Field(default=None, strict=True)
    autoflow_reclassify_l3_limit: int | None = Field(default=None, strict=True)
    autoflow_reclassify_batch_channels: int | None = Field(default=None, strict=True)
    discovery_autoscan_enabled: int | None = Field(default=None, strict=True)
    discovery_autoconnect_enabled: int | None = Field(default=None, strict=True)


def _settings_422_text(e: ValidationError) -> str:
    """Текст отказа по телу настроек: перечень известных ключей обязателен."""
    known = ", ".join(autoflow.AUTOMATION_LIMIT_KEYS)
    extra = [str(err["loc"][-1]) for err in e.errors()
             if err["type"] == "extra_forbidden" and err["loc"]]
    if extra:
        return (f"настройки автоматики: неизвестный ключ "
                f"{', '.join(extra)}; известны: {known}")
    err = e.errors()[0]
    field = ".".join(str(p) for p in err["loc"]) or "тело"
    if err["type"].startswith("int"):
        return (f"{field}: ожидалось целое число (bool — не число), "
                f"получено значение другого типа")
    return f"{field}: {err['msg']}"


@router.get("")
async def get_automation(db: GetDB, user=requires(Section.RUNS)):
    """Сводка «Автоматики»: настройки + четыре сценария (§4.1).

    Форму целиком собирает `autoflow.status()` — то же место, которым пользуется
    импорт набора настроек; второй сборщик сводки здесь разъехался бы с первым
    молча. Раздел `runs` — штатное чтение (замершая автоматика выглядит поломкой
    так же, как замершая очередь).
    """
    return await autoflow.status(db)


@router.post("/settings")
async def save_automation_settings(body: dict, request: Request, db: GetDB,
                                   user=permits(Section.RUNS,
                                                Capability.CONFIG_EDIT)):
    """Сохранить настройки автоматики (§4.2). Частичное обновление, ответ —
    действующие значения всех девяти ключей.

    Порядок фиксированный: разбор тела → «нечего менять» → границы → чтение
    прежних значений → запись → журнал. Часть проверок дублирует
    `save_settings` (она валидирует ещё раз перед записью) — но отказ обязан
    выйти 422 с человеческим текстом, а не 500 от непойманного ValueError.
    """
    try:
        parsed = _SettingsBody.model_validate(body)
    except ValidationError as e:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            _settings_422_text(e)) from e

    # None — «ключ не передан», а не «обнулить»: None у девяти необязательных
    # полей — способ отличить непереданное от переданного нуля.
    values = {k: v for k, v in parsed.model_dump().items() if v is not None}
    if not values:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            "настройки автоматики: не передано ни одного значения — "
            "нечего менять")

    try:
        autoflow.validate_settings(values)
    except ValueError as e:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            str(e)) from e

    # Прежние значения — до записи: журнал «из чего во что» иначе солжёт.
    before = await autoflow.thresholds(db)
    frm = {k: before[k] for k in values}
    saved = await autoflow.save_settings(db, values, actor=user.email)

    db.add(AuditLog(
        user_id=user.id, user_email=user.email,
        action="automation_settings_saved",
        detail={"from": frm, "to": values, "keys": saved},
        ip=request.client.host if request.client else None))
    await db.commit()

    # Все девять, а не только переданные: экран после сохранения перерисовывается
    # из ответа (решение ревью), и непереданные ключи должны приехать с сервера.
    return {"settings": await autoflow.thresholds(db)}
