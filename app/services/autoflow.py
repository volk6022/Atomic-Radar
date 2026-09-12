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

Формы швов сценариев (`after_join`, `reclassify_tick`, `scan_tick`,
`approve_tick`) здесь сознательно нет — их дописывают волны Б, В, Г и Д. Ни тел,
ни заглушек: функция-заполнитель выглядела бы «сценарием, который ничего не
делает», и от ещё не написанного её было бы не отличить.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Mapping

from sqlalchemy import func, select

# Форма строки прогона — дословно `runs._row` (решение ревью): одна форма прогона
# на все экраны, а не вторая, которая разъедется с первой. Цикла импорта нет:
# `runs` тянет службы (jobs, discovery), но ни одна из них не тянет `autoflow`.
from app.api.v1.runs import _row
from app.core import clock
from app.db.models import BackfillItem, ChannelCandidate, Limit, Message, Run
from app.services import backfill_queue, discovery

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


async def _autoscan_state(db) -> tuple[int | None, bool]:
    """`(donors, scanned_today)` для сценария 3.

    До волны Г доноров знать никто не может: флаг `channels.discovery_seed`
    появится только там (§5 контракта), а «сканировали сегодня» считается по
    семенам-донорам. Пока честные значения — `None` и `False`: ноль экран
    прочитал бы как «доноров нет», а это не так («неизвестно» ≠ «нет»).
    """
    return None, False


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
    waiting = (Message.cascade_level == 2, Message.cascade_passed.is_(None))
    waiting_channels = (await db.execute(
        select(func.count(func.distinct(Message.channel_id))).where(*waiting))
        ).scalar_one()
    waiting_messages = (await db.execute(
        select(func.count(Message.id)).where(*waiting))).scalar_one()

    # Окно сценария 2 отсчитывается от последнего завершённого прогона вида
    # ЛЮБОГО автора: ручной прогон только что разобрал тех же ждущих, и бережёт
    # карту он так же, как авто.
    last_finished = (await db.execute(
        select(Run.finished_at).where(Run.kind == "reclassify",
                                      Run.finished_at.is_not(None))
        .order_by(Run.finished_at.desc(), Run.id.desc()).limit(1))).scalar_one_or_none()
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
