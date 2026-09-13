"""Настройки автоматики и сводка для экрана «Автоматика» (план 13.5, волна А).

Автоматика — четыре сценария, которые ставят работу без человека: дочитать после
вступления, доклассифицировать ждущее L2, уйти в ежесуточный автоскан подбора,
подключить одобренного кандидата. Все выключатели по умолчанию 0: выкатка не
меняет поведение ни одного сценария, включение — осознанное действие владельца
строкой `limits` (ручка настроек — волна Е). Поэтому модуль — прежде всего
читатель и писатель строк `limits` и только потом всё остальное.

Чтение — на каждом событии и каждом ударе тика, без кеша и без рестарта (образец
— `discovery.thresholds`): правка строки обязана действовать сразу во всех трёх
процессах (API, воркер приёма, воркер прогонов). «Перезапустить, чтобы
применилось» — ровно то, от чего строки `limits` заводились.

`LIMIT_SPECS` — единственное место, где живёт описание ключа: умолчание,
границы, единица, описание. По нему работают и проверка ввода, и сидирование
старта, и набор настроек; второй список тех же ключей рядом разъехался бы с
первым молча.

Швы сценариев дописывают свои волны, по одному: `after_join` и `reclassify_tick`
— волны Б и В (ниже), `scan_tick`, `approve_tick` — волны Г и Д. Заглушек и тел
«на вырост» нет: функция-заполнитель выглядела бы «сценарием, который ничего
не делает», и от ещё не написанного её было бы не отличить.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Mapping

from sqlalchemy import func, literal_column, select
from sqlalchemy.exc import IntegrityError

# Форма строки прогона — дословно `runs._row` (решение ревью): одна форма прогона
# на все экраны, а не вторая, которая разъедется с первой. Цикла импорта нет:
# `runs` тянет службы (jobs, discovery), но ни одна из них не тянет `autoflow`;
# `jobs` тоже чист — его собственные импорты (`discussions`, `discovery`,
# `reclassify`…) автотех не знают.
from app.api.v1.runs import _row
from app.core import clock
from app.db.models import (AuditLog, BackfillItem, Channel, ChannelCandidate,
                           DiscoveryQuery, Limit, Message, Run)
from app.db.session import get_session_maker
from app.services import backfill_queue, discovery, engage, jobs

logger = logging.getLogger(__name__)

# Умолчания восьми новых ключей (§1.1 контракта). Девятый ключ,
# `discovery_autoconnect_enabled`, — существующий: его умолчание берётся из
# `discovery.DEFAULTS` ниже и здесь не дублируется. Значения целые — в
# `Numeric(12, 4)` (`models.Limit.value`) ложатся без потерь.
DEFAULTS: dict[str, int] = {
    "autoflow_join_backfill_enabled": 0,  # сценарий 1: вступил → дочитал
    "autoflow_backfill_depth_days": 30,   # = backfill_queue.DEFAULT_DEPTH.days
    "autoflow_backfill_target": 2000,     # = backfill_queue.DEFAULT_TARGET
    "autoflow_reclassify_enabled": 0,     # сценарий 2: доклассификация ждущих
    "autoflow_reclassify_interval_min": 60,
    "autoflow_reclassify_l3_limit": 200,
    "autoflow_reclassify_batch_channels": 10,
    "discovery_autoscan_enabled": 0,      # сценарий 3: ежесуточный автоскан
}

# Все девять ключей автоматики: DEFAULTS + существующий выключатель
# автоодобрения (сценарий 4), который `discovery.check_fit` уже читает через
# `discovery.thresholds`. Границы §1.1: потолки глубины/цели равны константам
# постановки очереди (30 суток / 2000 сообщений), интервал тика не бывает короче
# пяти минут (меньше — бессмысленно: тик и так бьётся раз в пять минут).
AUTOMATION_LIMIT_KEYS: tuple[str, ...] = (
    "autoflow_join_backfill_enabled",
    "autoflow_backfill_depth_days",
    "autoflow_backfill_target",
    "autoflow_reclassify_enabled",
    "autoflow_reclassify_interval_min",
    "autoflow_reclassify_l3_limit",
    "autoflow_reclassify_batch_channels",
    "discovery_autoscan_enabled",
    "discovery_autoconnect_enabled",
)

LIMIT_SPECS: dict[str, dict] = {
    "autoflow_join_backfill_enabled": {
        "default": 0, "bounds": (0, 1), "unit": "вкл/выкл",
        "description": "автопостановка канала и его группы в очередь "
                       "дочитывания после вступления"},
    "autoflow_backfill_depth_days": {
        "default": 30, "bounds": (1, 30), "unit": "сутки",
        "description": "глубина автодочитывания, не больше потолка "
                       "постановки (30)"},
    "autoflow_backfill_target": {
        "default": 2000, "bounds": (1, 2000), "unit": "сообщения",
        "description": "потолок сообщений на канал при автопостановке (2000)"},
    "autoflow_reclassify_enabled": {
        "default": 0, "bounds": (0, 1), "unit": "вкл/выкл",
        "description": "автопрогоны доклассификации ждущих L2 по расписанию"},
    "autoflow_reclassify_interval_min": {
        "default": 60, "bounds": (5, 1440), "unit": "минуты",
        "description": "не чаще, чем N минут после последнего завершённого "
                       "прогона переклассификации"},
    "autoflow_reclassify_l3_limit": {
        "default": 200, "bounds": (0, 10000), "unit": "вопросы",
        "description": "потолок вопросов L3 одного автопрогона"},
    "autoflow_reclassify_batch_channels": {
        "default": 10, "bounds": (1, 100), "unit": "каналы",
        "description": "сколько каналов берёт один автопрогон"},
    "discovery_autoscan_enabled": {
        "default": 0, "bounds": (0, 1), "unit": "вкл/выкл",
        "description": "ежесуточный автоскан подбора по каналам-донорам"},
    "discovery_autoconnect_enabled": {
        "default": discovery.DEFAULTS["discovery_autoconnect_enabled"],
        "bounds": (0, 1), "unit": "вкл/выкл",
        "description": "автоодобрение и автоподключение кандидатов с вердиктом "
                       "fit"},
}


def validate_settings(values: Mapping[str, int | float]) -> None:
    """Проверить ключи и границы §1.1; кривое падает ValueError (без БД).

    Единственная дверь для ручки настроек и импорта набора — обе обязаны
    отказывать по одним правилам, иначе «ручка пропустила, импорт нет» стало бы
    загадкой на полдня. Зовётся до первой записи: файл применяется целиком или
    никак. bool — не число, хотя Python считает иначе: `true` в JSON молча
    превратился бы в 1, и выключатель включился бы «пустым» значением.
    """
    unknown = [key for key in values if key not in LIMIT_SPECS]
    if unknown:
        raise ValueError(f"настройки автоматики: неизвестный ключ "
                         f"{', '.join(sorted(unknown))}; известны: "
                         f"{', '.join(AUTOMATION_LIMIT_KEYS)}")
    for key, value in values.items():
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{key}: ожидалось число, получено {value!r}")
        if isinstance(value, float) and not value.is_integer():
            raise ValueError(f"{key}: ожидалось целое, получено {value!r}")
        lo, hi = LIMIT_SPECS[key]["bounds"]
        if not lo <= value <= hi:
            raise ValueError(f"{key}: ожидалось целое из диапазона {lo}..{hi}, "
                             f"получено {value!r}")


async def thresholds(db) -> dict[str, int]:
    """Действующие значения всех ключей автоматики: строки `limits` поверх
    умолчаний, строка в БД побеждает, пустая таблица — рабочее состояние.

    Семантика и запрос — по образцу `discovery.thresholds`; умолчание
    `discovery_autoconnect_enabled` берётся из `discovery.DEFAULTS`, чтобы у
    одного ключа не появилось двух источников правды. Читается на каждом
    событии и ударе тика — кеш означал бы «правка действует после рестарта».
    """
    rows = (await db.execute(
        select(Limit.key, Limit.value)
        .where(Limit.key.in_(AUTOMATION_LIMIT_KEYS)))).all()
    out = {**DEFAULTS,
           "discovery_autoconnect_enabled":
               int(discovery.DEFAULTS["discovery_autoconnect_enabled"])}
    for key, value in rows:
        out[key] = int(value)
    return out


async def save_settings(db, values: Mapping[str, int | float], *,
                        actor: str) -> list[str]:
    """Upsert строк `limits` (validate_settings + запись); возвращает записанные
    ключи в порядке передачи.

    Та же дорога, которой правит порог владелец (`cascade_registry.
    save_thresholds`): существующая строка обновляется, отсутствующая заводится
    с `unit`/`description` из LIMIT_SPECS. Аудит пишет вызывающая ручка — у неё
    есть пользователь и ip; служба знает только строку-автора.
    """
    validate_settings(values)
    rows = (await db.execute(
        select(Limit).where(Limit.key.in_(list(values))))).scalars().all()
    by_key = {row.key: row for row in rows}
    for key, value in values.items():
        if key in by_key:
            by_key[key].value = float(value)
        else:
            spec = LIMIT_SPECS[key]
            db.add(Limit(key=key, value=float(value), unit=spec["unit"],
                         description=spec["description"]))
    await db.commit()
    logger.info("autoflow_settings_saved keys=%s by=%s", ",".join(values), actor)
    return list(values)


# ── сценарий 1: вступил → дочитал ─────────────────────────────────────────────

async def after_join(db, *, channel_id: int, source: str) -> dict:
    """Поставить канал и его группу обсуждения в очередь дочитывания.

    Зовут два шва — финал вступления в группу (`discussions._join_one`, автор
    `auto:join`) и финал подключения канала вместе с группой (стадия `linked`
    в `ingest._handle_chat_info_join`, автор `auto:channel_add`). Прогонов
    сценарий не заводит — он растит очередь дочитывания (план 13.1). Выключенный
    выключатель — пустой итог и ни одного запроса дальше чтения настроек: событие
    случается на каждом подключении канала, и в горячем пути приёма лишних
    вопросов базе задавать незачем.

    Партнёр ищется по `linked_chat_username`, а поле двунаправленное: у канала
    оно ведёт к группе, у группы — к каналу. Поэтому одна функция обслуживает
    оба шва: кто бы ни приехал в `channel_id`, канал или группа, — в очередь
    встают оба. Группу очередь читает только вступившим аккаунтом, и чужая
    постановка получила бы `NotJoined`, поэтому не вступившая группа фильтруется
    превентивно (по образцу ручки `backfill.py`) и попадает в `skipped`, а не
    роняет шов. Уже стоящий канал очередь пропускает молча сама; гонку
    одновременной постановки ловит частичный уникальный индекс
    `uq_backfill_active_channel` — пойманный IntegrityError значит «кто-то успел
    раньше», это откат и warning, а не ошибка наверх.
    """
    lim = await thresholds(db)
    if int(lim["autoflow_join_backfill_enabled"]) != 1:
        return {"enabled": False, "queued": [], "skipped": []}

    row = await db.get(Channel, channel_id)
    if row is None:
        logger.warning("autoflow_after_join_unknown_channel channel=%s source=%s",
                       channel_id, source)
        return {"enabled": True, "queued": [], "skipped": []}

    # Список постановки: сам канал и партнёр по имени обсуждения (не сам row —
    # поле может случайно указывать на собственную строку).
    wanted: list[Channel] = [row]
    username = (row.linked_chat_username or "").strip().lower()
    if username:
        partner = (await db.execute(
            select(Channel).where(Channel.id != row.id,
                                  func.lower(Channel.username) == username)
            .limit(1))).scalars().first()
        if partner is not None:
            wanted.append(partner)

    # Группа ставится только вступившим: превентивная проверка тех же двух полей,
    # которые проверяет `enqueue`, иначе постановка упала бы NotJoined посреди
    # шва. Каналу проверять нечего — его историю Telegram отдаёт любому.
    items: list[dict] = []
    skipped: list[int] = []
    for ch in wanted:
        if (ch.chat_type in backfill_queue.GROUP_CHAT_TYPES
                and (ch.linked_joined_at is None
                     or ch.subscribed_account_id is None)):
            skipped.append(ch.id)
            continue
        items.append({"channel_id": ch.id})

    made: list[BackfillItem] = []
    try:
        made = await backfill_queue.enqueue(
            db, items=items, requested_by=source,
            target=int(lim["autoflow_backfill_target"]),
            min_date=clock.utcnow()
            - timedelta(days=int(lim["autoflow_backfill_depth_days"])))
    except IntegrityError as e:
        # Параллельная постановка того же канала: наш «уже стоит» прочитал базу
        # до чужого коммита, и дубль поймал уникальный индекс. Откат: сделка в
        # этот момент недостоверна, а «поставлено раньше» — не происшествие.
        await db.rollback()
        logger.warning("autoflow_after_join_race channel=%s source=%s error=%s",
                       channel_id, source, e)

    queued = [m.channel_id for m in made]
    if queued:
        db.add(AuditLog(
            user_id=None, user_email=source, action="backfill_enqueue",
            detail={"queued": queued,
                    "target": int(lim["autoflow_backfill_target"]),
                    "depth_days": int(lim["autoflow_backfill_depth_days"])},
            ip=None))
        await db.commit()
    return {"enabled": True, "queued": queued, "skipped": skipped}


# ── сценарий 2: дочитал/принял → доклассифицировал ────────────────────────────

async def reclassify_tick(ctx: dict) -> dict:
    """Один удар сценария 2 (§3.2): поставить автопрогон доклассификации ждущих L2.

    Бьётся каждые пять минут, а интервал может быть длиннее: тик сам сверяет окно
    с последним завершённым прогоном вида — любого автора (ручной прогон разобрал
    тех же ждущих и карту бережёт так же, как авто). Холостой удар стоит один
    SELECT — нулевая работа против второй дороги-расписания.

    Порция — топ каналов по числу ждущих (`_waiting_channels`): потолок вопросов
    L3 конечен. Прогон с `channel_ids` жёстко create-only: автопрогон никогда не
    перезаписывает и не удаляет чужое, недосчитанное дожмёт следующий удар.

    Падать не имеет права (образец — `discovery_check_tick`): исключение на ровном
    месте залило бы журнал воркера приёма ложными отказами. `JobBusy` — не ошибка
    и не `last_error`, а штатное «занято»: тихий пропуск, лог уровня info.
    """
    try:
        maker = get_session_maker()
        async with maker() as db:
            lim = await thresholds(db)
            # Ждущие считаются всегда, даже при выключенном сценарии: возврат тика
            # — единственное место, где arq-задача отчитывается, чем дышит очередь.
            waiting_channels, waiting_messages = await _waiting_stats(db)
            started = busy = interval_wait = False
            if int(lim["autoflow_reclassify_enabled"]) == 1 and waiting_channels:
                try:
                    if await jobs.active_run(db, "reclassify") is not None:
                        busy = True
                    else:
                        last = await _last_finished_at(db, kind="reclassify")
                        interval = timedelta(
                            minutes=int(lim["autoflow_reclassify_interval_min"]))
                        if last is not None and clock.utcnow() < last + interval:
                            interval_wait = True
                        else:
                            ids = await _waiting_channels(
                                db, int(lim["autoflow_reclassify_batch_channels"]))
                            params = {"scope": "pending", "channel_ids": ids,
                                      "l3_limit":
                                          int(lim["autoflow_reclassify_l3_limit"])}
                            run = await jobs.start(
                                db, kind="reclassify", params=params,
                                name=f"Переклассификация · недосчитанное · каналы "
                                     f"{', '.join(str(i) for i in ids)}",
                                user_email="auto:reclassify")
                            # Тот же аудит, что у ручки запуска (`runs.py`), но от
                            # авто: автор строки — единственная метка источника.
                            db.add(AuditLog(
                                user_id=None, user_email="auto:reclassify",
                                action="run_start",
                                detail={"run_id": run.id, "kind": "reclassify",
                                        "params": params}, ip=None))
                            await db.commit()
                            started = True
                except jobs.JobBusy as e:
                    # Гонка с чужим запуском: «занято» проходит само, наружу —
                    # не ошибка (§0.4), состояние и так видно по строке runs.
                    logger.info("autoflow_reclassify_tick_busy error=%s", e)
                    busy = True
    except Exception as e:  # noqa: BLE001 — тик не вправе уронить воркера приёма
        logger.warning("autoflow_reclassify_tick_failed error=%s", e)
        return {"started": False, "busy": False, "interval_wait": False,
                "waiting_channels": 0, "waiting_messages": 0}
    logger.info("autoflow_reclassify_tick started=%s busy=%s interval_wait=%s "
                "waiting=%s/%s", started, busy, interval_wait, waiting_channels,
                waiting_messages)
    return {"started": started, "busy": busy, "interval_wait": interval_wait,
            "waiting_channels": waiting_channels,
            "waiting_messages": waiting_messages}


# ── сценарий 3: ежесуточный автоскан подбора ──────────────────────────────────

async def _scan_donors(db) -> list[int]:
    """id доноров автоскана в порядке отбора семян: флаг `discovery_seed`,
    отслеживание и имя (симметрично требованиям ручного скана), сортировка
    `members DESC NULLS LAST, id` — та же, что у авто-выбора семени в ручке
    поиска. Одна выборка для тика и сводки: число доноров на «Автоматике» и
    порция тика не должны уметь расходиться."""
    rows = (await db.execute(
        select(Channel.id).where(
            Channel.discovery_seed.is_(True),
            Channel.ingest_enabled.is_(True),
            Channel.username.isnot(None))
        .order_by(Channel.members.desc().nulls_last(), Channel.id)))
    return list(rows.scalars().all())


async def _searched_today(db, donor_ids: list[int]) -> set[int]:
    """Какие доноры уже исканы в текущие UTC-сутки (`kind='similar'`): то же
    окно `_utc_day_start`, что у уникальности суток. Строки по чужим семенам и
    поиски по строке фильтр не трогают — сценарий отвечает только за доноров."""
    if not donor_ids:
        return set()
    rows = (await db.execute(
        select(DiscoveryQuery.seed_channel_id).where(
            DiscoveryQuery.kind == "similar",
            DiscoveryQuery.created_at >= discovery._utc_day_start(),
            DiscoveryQuery.seed_channel_id.in_(donor_ids)))).scalars().all()
    return set(rows)


async def _scan_window_wait(db) -> bool:
    """Последний завершённый прогон скана отложен лимитом Engage и назвал окно
    возврата (`retry_after_s`), которое ещё не истекло (§3.3). Читается как
    `discovery._waiting_budget`: более поздний обычный прогон отменяет старое
    окно сам собой. Окна нет — False: прежний часовой ритм, молча ждать «до
    никогда» нельзя."""
    last = (await db.execute(
        select(Run).where(Run.kind == "discovery_scan",
                          Run.finished_at.is_not(None))
        .order_by(Run.finished_at.desc(), Run.id.desc())
        .limit(1))).scalar_one_or_none()
    if last is None or last.status != "deferred":
        return False
    seconds = (last.result or {}).get("retry_after_s")
    try:
        seconds = int(seconds)
    except (TypeError, ValueError):
        return False
    return clock.utcnow() < last.finished_at + timedelta(seconds=seconds)


async def scan_tick(ctx: dict) -> dict:
    """Один удар сценария 3 (§3.3): поставить ежесуточный автоскан по донорам.

    Бьётся раз в час (`minute={0}`), а сутки следят за собой: есть по хоть
    одному донору строка `discovery_queries` в текущие UTC-сутки — удар
    холостой. Все семена едут одним прогоном (`seed_channel_ids`): заказов у
    Engage всё равно не больше `discovery_queries_per_scan`, потолок живёт
    в прогоне. Окно отложенного возврата (`retry_after_s`) бережёт бюджет:
    отложенный ночью скан не перезаказывается каждый час до возврата лимита.

    Падать не имеет права (образец — `reclassify_tick`): `JobBusy` — штатное
    «занято», а пустой флот — состояние Engage, не происшествие; у тика нет
    права на 503, поэтому нет активных аккаунтов — пустой итог с логом.
    """
    try:
        maker = get_session_maker()
        async with maker() as db:
            # Доноры считаются всегда, даже при выключенном сценарии: возврат
            # тика — единственный отчёт arq-задачи, и T-19 ждёт доноров при
            # выключенном выключателе.
            donor_ids = await _scan_donors(db)
            donors = len(donor_ids)
            lim = await thresholds(db)
            started = busy = window_wait = False
            seeds: list[int] = []
            if int(lim["discovery_autoscan_enabled"]) == 1 and donors:
                searched = await _searched_today(db, donor_ids)
                if not searched:
                    try:
                        if await jobs.active_run(db, "discovery_scan") is not None:
                            busy = True
                        elif await _scan_window_wait(db):
                            window_wait = True
                        else:
                            dlim = await discovery.thresholds(db)
                            seeds = donor_ids[:min(donors, int(
                                dlim["discovery_queries_per_scan"]))]
                            fleet = await engage.list_accounts()
                            active = [a["account_id"] for a in fleet
                                      if a.get("status") == "active"]
                            if not active:
                                # Флот пуст — состояние, а не происшествие.
                                logger.warning(
                                    "autoflow_scan_tick_no_accounts donors=%s",
                                    donors)
                                seeds = []
                            else:
                                params = {"kind": "similar",
                                          "seed_channel_ids": seeds,
                                          "account_id": active[0]}
                                run = await jobs.start(
                                    db, kind="discovery_scan", params=params,
                                    name=f"Поиск похожих каналов · "
                                         f"доноры × {len(seeds)}",
                                    user_email="auto:scan")
                                # Тот же аудит, что у ручки поиска, но от
                                # авто: автор строки — метка источника.
                                db.add(AuditLog(
                                    user_id=None, user_email="auto:scan",
                                    action="discovery_scan_started",
                                    detail={"kind": "similar",
                                            "seed_channel_ids": seeds,
                                            "account_id": active[0],
                                            "run_id": run.id}, ip=None))
                                await db.commit()
                                started = True
                    except jobs.JobBusy as e:
                        # Гонка с чужим запуском: «занято» проходит само (§0.4),
                        # наружу — не ошибка, состояние видно по строке runs.
                        logger.info("autoflow_scan_tick_busy error=%s", e)
                        busy = True
    except Exception as e:  # noqa: BLE001 — тик не вправе уронить воркера приёма
        logger.warning("autoflow_scan_tick_failed error=%s", e)
        return {"started": False, "busy": False, "window_wait": False,
                "seeds": 0, "donors": 0}
    logger.info("autoflow_scan_tick started=%s busy=%s window_wait=%s "
                "seeds=%s donors=%s", started, busy, window_wait, len(seeds),
                donors)
    return {"started": started, "busy": busy, "window_wait": window_wait,
            "seeds": len(seeds), "donors": donors}


# ── сводка для экрана ─────────────────────────────────────────────────────────

# Сценарии, у которых бывают прогоны: вид задачи и автор строки (§0.2). Сценарий 1
# прогонов не заводит — он растит очередь дочитывания (план 13.1), поэтому его
# здесь нет.
_RUN_SCENARIOS = {
    "reclassify": ("reclassify", "auto:reclassify"),
    "autoscan": ("discovery_scan", "auto:scan"),
    "autoapprove": ("channel_add", "auto:approve"),
}


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt is not None else None


async def _waiting_stats(db) -> tuple[int, int]:
    """`(каналы, сообщения)` ждущих доклассификации: `cascade_level = 2` без
    вердикта каскада. Единая выборка для экрана и тика сценария 2: число на
    «Автоматике» и порция автопрогона не должны уметь расходиться."""
    waiting = (Message.cascade_level == 2, Message.cascade_passed.is_(None))
    channels = (await db.execute(
        select(func.count(func.distinct(Message.channel_id))).where(*waiting))
        ).scalar_one()
    messages = (await db.execute(
        select(func.count(Message.id)).where(*waiting))).scalar_one()
    return channels, messages


async def _waiting_channels(db, batch: int) -> list[int]:
    """Порция каналов под автопрогон: топ по числу ждущих убыванием, при равенстве
    — меньший id первым (SQL из §3.2 контракта дословно по смыслу), не больше
    `batch`. Прогону не хватает `l3_limit` вопросов на всех: польза максимальна
    там, где ждущих больше, а давность всё равно догонит следующий удар — интервал
    не длиннее часа."""
    rows = (await db.execute(
        select(Message.channel_id, func.count(Message.id).label("waiting"))
        .where(Message.cascade_level == 2, Message.cascade_passed.is_(None))
        .group_by(Message.channel_id)
        .order_by(literal_column("waiting").desc(), Message.channel_id.asc())
        .limit(batch))).all()
    return [row.channel_id for row in rows]


async def _last_finished_at(db, *, kind: str) -> datetime | None:
    """Момент последнего завершённого прогона вида — любого автора (`discovery.
    _waiting_budget`). Ручной прогон и авто читают одну и ту же выборку, и окно
    от последнего из них бережёт карту одинаково."""
    return (await db.execute(
        select(Run.finished_at).where(Run.kind == kind,
                                      Run.finished_at.is_not(None))
        .order_by(Run.finished_at.desc(), Run.id.desc()).limit(1))).scalar_one_or_none()


async def _last_run(db, *, kind: str, author: str) -> Run | None:
    """Последний прогон вида с автором `auto:*`, по `(finished_at, id)` — как
    читает окно сценария 2 (`discovery._waiting_budget`). Идущий прогон
    (finished_at NULL) — самый свежий и показывается: «что делала автоматика»
    включает «что делает прямо сейчас»."""
    return (await db.execute(
        select(Run).where(Run.kind == kind, Run.created_by == author)
        .order_by(Run.finished_at.desc().nullsfirst(), Run.id.desc())
        .limit(1))).scalar_one_or_none()


async def _last_error(db, *, kind: str, author: str) -> str | None:
    """Текст последнего упавшего прогона вида с автором `auto:*`. Отдельно от
    `last_run`: экран показывает и последний исход, и последнюю ошибку — они
    часто про разные прогоны (один упал, следующий прошёл успешно)."""
    return (await db.execute(
        select(Run.error).where(Run.kind == kind, Run.created_by == author,
                                Run.status == "failed")
        .order_by(Run.finished_at.desc().nullsfirst(), Run.id.desc())
        .limit(1))).scalar_one_or_none()


async def _autoscan_state(db) -> tuple[int, bool]:
    """`(donors, scanned_today)` для сценария 3 — та же выборка, что у тика:
    доноры `_scan_donors`, «сканировали сегодня» — есть ли в текущие UTC-сутки
    строка `similar` по донору (`_searched_today`). Ноль доноров — честный ноль:
    с волны Г флаг колонки существует, и «неизвестно» (прочерк волны А) больше
    не бывает.
    """
    donor_ids = await _scan_donors(db)
    if not donor_ids:
        return 0, False
    return len(donor_ids), bool(await _searched_today(db, donor_ids))


async def status(db) -> dict:
    """Сводка для GET /api/v1/automation: настройки + 4 сценария (§4.1).

    Всё считается по таблицам, которые уже есть (`limits`, `runs`,
    `backfill_queue`, `discovery_queries`, `channel_candidates`) — второго места
    правды не заводится: «занято» читается из `runs`, окно интервала — из
    последнего завершённого прогона, остаток вступлений — тот же счётчик, которым
    `check_fit` режет автоодобрения. `next_at` — расчёт, а не обещание воркера:
    тик вправе молча пропустить удар (JobBusy), и экран не должен выдавать план
    за гарантию.
    """
    lim = await thresholds(db)
    dlim = await discovery.thresholds(db)

    # Сценарий 1 — очередь, а не прогоны: сколько стоит от автоматики.
    queue_standing_auto = (await db.execute(
        select(func.count(BackfillItem.id)).where(
            BackfillItem.state.in_(backfill_queue.ACTIVE),
            BackfillItem.requested_by.like("auto:%")))).scalar_one()

    # Сценарий 2 — ждущие L2: та же выборка, что у тика (§3.2 контракта), чтобы
    # число на экране и порция тика не могли разойтись.
    waiting_channels, waiting_messages = await _waiting_stats(db)

    # Окно сценария 2 отсчитывается от последнего завершённого прогона вида
    # ЛЮБОГО автора: ручной прогон только что разобрал тех же ждущих, и бережёт
    # карту он так же, как авто.
    last_finished = await _last_finished_at(db, kind="reclassify")
    reclassify_next_at = None
    if last_finished is not None:
        reclassify_next_at = _iso(last_finished + timedelta(
            minutes=int(lim["autoflow_reclassify_interval_min"])))

    # Сценарий 3 — ритм: скан по донорам уже был сегодня → следующий в полночь
    # UTC; иначе — ближайший следующий целый час (тик бьётся раз в час).
    donors, scanned_today = await _autoscan_state(db)
    if scanned_today:
        autoscan_next_at = _iso(discovery._utc_day_start() + timedelta(hours=24))
    else:
        next_hour = (clock.utcnow() + timedelta(hours=1)).replace(
            minute=0, second=0, microsecond=0)
        autoscan_next_at = _iso(next_hour)

    # Сценарий 4 — одобренные с именем: ровно те, кого берёт approve_tick.
    approved_waiting = (await db.execute(
        select(func.count(ChannelCandidate.id)).where(
            ChannelCandidate.decision == "approved",
            ChannelCandidate.username.isnot(None)))).scalar_one()

    last = {name: await _last_run(db, kind=kind, author=author)
            for name, (kind, author) in _RUN_SCENARIOS.items()}
    errors = {name: await _last_error(db, kind=kind, author=author)
              for name, (kind, author) in _RUN_SCENARIOS.items()}

    return {
        "settings": lim,
        "scenarios": {
            "join_backfill": {
                "enabled": lim["autoflow_join_backfill_enabled"] == 1,
                "params": {
                    "autoflow_backfill_depth_days":
                        int(lim["autoflow_backfill_depth_days"]),
                    "autoflow_backfill_target":
                        int(lim["autoflow_backfill_target"]),
                },
                # Прогонов нет и не будет: событийный сценарий растит очередь.
                "last_run": None,
                "next_at": None,
                "last_error": None,
                "queue_standing_auto": queue_standing_auto,
            },
            "reclassify": {
                "enabled": lim["autoflow_reclassify_enabled"] == 1,
                "params": {
                    "autoflow_reclassify_interval_min":
                        int(lim["autoflow_reclassify_interval_min"]),
                    "autoflow_reclassify_l3_limit":
                        int(lim["autoflow_reclassify_l3_limit"]),
                    "autoflow_reclassify_batch_channels":
                        int(lim["autoflow_reclassify_batch_channels"]),
                },
                "last_run": (_row(last["reclassify"])
                             if last["reclassify"] is not None else None),
                "next_at": reclassify_next_at,
                "last_error": errors["reclassify"],
                "waiting_channels": waiting_channels,
                "waiting_messages": waiting_messages,
            },
            "autoscan": {
                "enabled": lim["discovery_autoscan_enabled"] == 1,
                "params": {
                    "discovery_queries_per_scan":
                        int(dlim["discovery_queries_per_scan"]),
                    "discovery_llm_checks_per_day":
                        int(dlim["discovery_llm_checks_per_day"]),
                },
                "last_run": (_row(last["autoscan"])
                             if last["autoscan"] is not None else None),
                "next_at": autoscan_next_at,
                "last_error": errors["autoscan"],
                "donors": donors,
                "scanned_today": scanned_today,
            },
            "autoapprove": {
                "enabled": lim["discovery_autoconnect_enabled"] == 1,
                "params": {
                    "discovery_auto_joins_per_day":
                        int(dlim["discovery_auto_joins_per_day"]),
                },
                "last_run": (_row(last["autoapprove"])
                             if last["autoapprove"] is not None else None),
                # Общий тик проверки кандидатов (каждые 5 минут) — расписания у
                # сценария своего нет.
                "next_at": None,
                "last_error": errors["autoapprove"],
                "approved_waiting": approved_waiting,
                "auto_joins_today": await discovery._auto_joins_today(db),
            },
        },
    }
