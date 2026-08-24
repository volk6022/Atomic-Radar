"""Реестр инстансов Engage: какой ключ в какой адрес разворачивается.

Зачем отдельный модуль. Клиент (`services/engage`) не должен знать про базу — иначе
каждый вызов к флоту тянул бы за собой сессию, а тесты клиента требовали бы Postgres.
Поэтому разделение такое: реестр читает таблицу и отдаёт клиенту функцию
`ключ -> Endpoint`, клиент про источник этих данных ничего не знает.

Ключи API в базе не лежат. В таблице — **имя переменной окружения**, значение берётся
из окружения процесса. Иначе дамп базы становится связкой ключей от всех заказчиков,
а дампы ездят между машинами куда охотнее, чем хотелось бы.
"""
from __future__ import annotations

import logging
import os

from sqlalchemy import select

from app.core.config import get_settings
from app.db.models import EngageInstance
from app.services import engage

logger = logging.getLogger(__name__)

# Имя переменной окружения с ключом инстанса, заведённого при первом запуске.
BOOTSTRAP_KEY = engage.DEFAULT_INSTANCE
BOOTSTRAP_API_KEY_ENV = "RADAR_ENGAGE_API_KEY"

_cache: dict[str, engage.Endpoint] = {}


def _endpoint(row: EngageInstance) -> engage.Endpoint:
    api_key = os.environ.get(row.api_key_env, "")
    if not api_key:
        # Не падаем: инстанс без ключа отвечает 401, и это видно на экране флота как
        # «Engage ответил 401». Упасть на старте значило бы уронить весь Radar из-за
        # одного неверно названного секрета у одного заказчика.
        logger.warning("engage_instance_no_api_key instance=%s env=%s",
                       row.key, row.api_key_env)
    return engage.Endpoint(key=row.key, base_url=row.base_url, api_key=api_key)


def resolve(key: str) -> engage.Endpoint | None:
    return _cache.get(key)


async def reload(db) -> int:
    """Перечитать активные инстансы в кэш. Возвращает их число."""
    rows = (await db.execute(
        select(EngageInstance).where(EngageInstance.is_active.is_(True)))).scalars().all()
    _cache.clear()
    for row in rows:
        _cache[row.key] = _endpoint(row)
    logger.info("engage_registry_loaded count=%s keys=%s", len(rows), sorted(_cache))
    return len(rows)


async def ensure_bootstrap(db) -> bool:
    """Завести инстанс из настроек процесса, если реестр пуст.

    Путь первого запуска на уже работающей установке: адрес Engage лежит в окружении,
    таблицы реестра ещё нет. Заводим одну строку из того, что уже настроено, — так
    выкатка не требует ручного шага, и ничего не ломается в момент перезапуска.

    Возвращает True, если строка была создана.
    """
    existing = (await db.execute(select(EngageInstance).limit(1))).scalar_one_or_none()
    if existing is not None:
        return False

    s = get_settings()
    db.add(EngageInstance(
        key=BOOTSTRAP_KEY,
        client_label="Основной",
        base_url=s.ENGAGE_BASE_URL,
        api_key_env=BOOTSTRAP_API_KEY_ENV,
        is_active=True,
    ))
    await db.commit()
    logger.info("engage_registry_bootstrapped key=%s url=%s", BOOTSTRAP_KEY,
                s.ENGAGE_BASE_URL)
    return True


def install() -> None:
    """Подключить реестр к клиенту. Вызывается один раз на старте приложения."""
    engage.set_resolver(resolve)
