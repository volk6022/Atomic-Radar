"""Раздел Discovery (сценарий B): кандидаты, запуск поиска, решения оператора.

HTTP-поверхность над службой (`app/services/discovery.py`), а не вторая её копия:
три проверки кандидатов ведёт тик и прогоны, экрану же нужны четыре вещи —
смотреть список, запустить поиск, вынести решение, увидеть объём прогона.
Всё, что не это, живёт в службе.

Ручки `connect` здесь НЕТ (§3.4): подключение зовёт существующий
`POST /api/v1/channels`, а статус `connected` кандидату проставляет тик —
по факту существования отслеживаемого канала с тем же username. Вторая дорога
«нашёл → вступил» не заводится намеренно: состояние выводится из факта, и его
нельзя проехать по кнопке в обход существующей ручки подключения.

Имена фильтров списка — `state` и `verdict`, а не имена колонок (`decision`,
`llm_verdict`): оболочка никогда не шлёт имён с префиксами колонок, а `state` —
единственное имя фильтра состояния, которое она уже использует (§3.1).
Соответствие `state → decision`, `verdict → llm_verdict` зафиксировано контрактом.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select

from app.api.deps import GetDB, permits, requires
from app.api.v1.listing import ListParams, apply_search, apply_sort, list_params
from app.core import clock
from app.core.access import Capability, Section
from app.db.models import AuditLog, Channel, ChannelCandidate, DiscoveryQuery
from app.services import engage, jobs
# Служба и этот модуль называются одинаково; псевдоним — по той же причине, что и
# в `app/main.py` (`events_api`): без него читатель модуля `discovery` видит имя
# `discovery` и не может сказать, не глядя в импорт, чей это `run_scan`.
from app.services import discovery as discovery_service

logger = logging.getLogger("radar")

router = APIRouter(prefix="/api/v1/discovery", tags=["discovery"])

# Белый список сортировок. `created` — это колонка `found_at`: колонки `created`
# у кандидата нет (REVIEW.md §2), а имя экрану нужно то же, что у остальных
# списков. Сортировка по умолчанию — `created desc`: свежие находки сверху,
# потому что очередь проверок ест очередь по `found_at` и хвост списка — это то,
# что ещё не дошло до проверок.
CANDIDATE_SORTS = {"created": ChannelCandidate.found_at,
                   "members": ChannelCandidate.members,
                   "title": ChannelCandidate.title}


def _check(value: str | None, allowed: tuple[str, ...], what: str) -> None:
    """Отвергнуть чужое значение фильтра с перечнем допустимых — тем же текстом,
    что и у сценариев (`wf_queues._check`): отказ обязан читаться одинаково, из
    какой бы таблицы он ни вышел."""
    if value and value not in allowed:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            f"неизвестный {what} «{value}», ожидается один из {', '.join(allowed)}")


def _iso(dt) -> str | None:
    return dt.isoformat() if dt is not None else None


def _candidate_row(c: ChannelCandidate) -> dict:
    """Строка кандидата для экрана. Одна функция на список и на решение:
    после `decide` экрану надо перерисовать строку на месте, и собирать её
    второй рукой значило бы однажды показать два разных кандидата."""
    return {
        "id": c.id, "username": c.username, "title": c.title,
        "peer_id": c.peer_id, "members": c.members, "chat_type": c.chat_type,
        "source": c.source, "seed_channel_id": c.seed_channel_id,
        "linked_chat_username": c.linked_chat_username,
        "found_by_account_id": c.found_by_account_id,
        "found_at": _iso(c.found_at),
        "liveness_posts_7d": c.liveness_posts_7d,
        "liveness_comments_7d": c.liveness_comments_7d,
        "liveness_checked_at": _iso(c.liveness_checked_at),
        "llm_verdict": c.llm_verdict, "llm_score": c.llm_score,
        "llm_reason": c.llm_reason, "llm_at": _iso(c.llm_at),
        "decision": c.decision, "decided_by": c.decided_by,
        "decided_at": _iso(c.decided_at), "decision_reason": c.decision_reason,
    }


def _query_row(q: DiscoveryQuery) -> dict:
    return {"id": q.id, "kind": q.kind, "seed_channel_id": q.seed_channel_id,
            "query": q.query, "account_id": q.account_id, "run_id": q.run_id,
            "found_total": q.found_total, "new_total": q.new_total,
            "created_at": _iso(q.created_at)}


# ── §3.1 — список кандидатов ──────────────────────────────────────────────────

@router.get("/candidates")
async def list_candidates(db: GetDB, user=requires(Section.CHANNELS),
                          p: ListParams = Depends(list_params),
                          state: str | None = Query(None),
                          source: str | None = Query(None),
                          verdict: str | None = Query(None)):
    """Кандидаты в каналы: страница, фильтры, сводка состояний.

    Право — раздел `channels`: список смотрит штат (образец — реестр каналов
    `/channels`), решений он сам по себе не принимает.

    Сводка `states` считается по всей таблице без фильтров и приходит всегда,
    даже пустая: чипсы состояний на экране — способ узнать, сколько работы в
    каждом бакете, а фильтр по состоянию не должен эту сводку прятать (тот же
    приём, что у сводки очереди дочитывания и у черновиков сценариев).
    """
    _check(state, ChannelCandidate.DECISIONS, "состояние")
    _check(source, ChannelCandidate.SOURCES, "источник")
    _check(verdict, ChannelCandidate.VERDICTS, "вердикт")

    filters = []
    if state:
        filters.append(ChannelCandidate.decision == state)
    if source:
        filters.append(ChannelCandidate.source == source)
    if verdict:
        filters.append(ChannelCandidate.llm_verdict == verdict)

    q = select(ChannelCandidate)
    count_q = select(func.count(ChannelCandidate.id))
    if filters:
        q = q.where(*filters)
        count_q = count_q.where(*filters)

    # Поиск по названию и username — как в реестре каналов: оператор ищет
    # кандидата по тому имени, которое видел в выдаче поиска.
    search = [ChannelCandidate.title, ChannelCandidate.username]
    q = apply_search(q, p, search)
    count_q = apply_search(count_q, p, search)

    total = (await db.execute(count_q)).scalar_one()
    q = apply_sort(q, p, CANDIDATE_SORTS, default="created",
                   tiebreak=ChannelCandidate.id)
    rows = (await db.execute(q.limit(p.limit).offset(p.offset))).scalars().all()

    by_state = dict((await db.execute(
        select(ChannelCandidate.decision, func.count(ChannelCandidate.id))
        .group_by(ChannelCandidate.decision))).all())
    return {**p.page(total),
            "rows": [_candidate_row(c) for c in rows],
            "states": [{"key": key, "count": by_state.get(key, 0)}
                       for key in ChannelCandidate.DECISIONS],
            # Перечень сортировок — из CANDIDATE_SORTS, а не своя копия: экран
            # рисует контролы без пробного запроса, и разъехаться с сервером им
            # нельзя (образец — `/channels`).
            "sorts": sorted(CANDIDATE_SORTS)}


# ── §3.2 — запуск поиска ──────────────────────────────────────────────────────

class ScanRequest(BaseModel):
    """Тело запуска поиска. Адресация — ровно одна на запрос: у «похожих»
    семя, у поиска строка; скрещивать их не даёт и схема (`ck_discovery_query_target`),
    и проверка ниже — только рано, на нажатии, а не после прогона."""
    model_config = ConfigDict(extra="forbid")
    kind: str
    seed_channel_id: int | None = Field(None, ge=1)
    query: str | None = None
    account_id: int | None = Field(None, ge=1)


@router.post("/scan", status_code=202)
async def scan(body: ScanRequest, request: Request, db: GetDB,
               user=permits(Section.CHANNELS, Capability.RUN_BACKFILL)):
    """Запустить поиск кандидатов: «похожие» к семени или поиск по строке.

    Право — то же, что у постановки в очередь дочитывания: поиск тратит
    дневной бюджет чтений аккаунта Engage, и распоряжаться им может не всякий
    вошедший.

    Ответ 202 означает «прогон заведён», а не «найдено»: сам поиск ведёт прогон
    `discovery_scan`, проверки найденного — тик (§4.4). Суточного лимита поисков
    нет (Решение 1: «пять поисков» — объём одного прогона, и потолок этот живёт
    в службе, куда общей ручке прогонов хода нет): сверху стоят один прогон за
    раз (`JobBusy`), уникальность цели в UTC-сутки ниже и read-бюджеты самого
    Engage.
    """
    _check(body.kind, DiscoveryQuery.KINDS, "вид поиска")

    seed_channel_id: int | None = None
    query_text: str | None = None
    seed_name = ""
    if body.kind == "similar":
        if (body.query or "").strip():
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_ENTITY,
                "у поиска «похожих» не бывает строки поиска — уберите query "
                "или смените kind на search")
        if body.seed_channel_id is not None:
            seed = await db.get(Channel, body.seed_channel_id)
            # Семя обязано быть отслеживаемым: неискомый канал без подписки
            # карточкой отвечать не обязан, и прогон зашёл бы в пустоту.
            if seed is None or not seed.ingest_enabled:
                raise HTTPException(
                    status.HTTP_422_UNPROCESSABLE_ENTITY,
                    f"канала #{body.seed_channel_id} нет среди отслеживаемых — "
                    f"выберите семя из реестра каналов")
            seed_channel_id = seed.id
            seed_name = f" · @{seed.username}" if seed.username else ""
        else:
            # Семя выбирается само: первый отслеживаемый канал, не исканный
            # в текущем окне, по убыванию участников — та же сортировка, что у
            # реестра и у выбора групп для вступления. Порядок жёсткий, кнопки
            # «какой канал» в теле нет: выбор семени — не решение оператора,
            # а обход очереди.
            day_start = discovery_service._utc_day_start()
            searched = (select(DiscoveryQuery.seed_channel_id)
                        .where(DiscoveryQuery.kind == "similar",
                               DiscoveryQuery.created_at >= day_start))
            seed = (await db.execute(
                select(Channel)
                .where(Channel.ingest_enabled.is_(True),
                       Channel.id.not_in(searched))
                .order_by(Channel.members.desc().nulls_last(), Channel.id)
                .limit(1))).scalar_one_or_none()
            if seed is None:
                raise HTTPException(
                    status.HTTP_409_CONFLICT,
                    "все отслеживаемые семена в этом окне уже искали — "
                    "продолжайте завтра или выберите семя вручную")
            seed_channel_id = seed.id
            seed_name = f" · @{seed.username}" if seed.username else ""
    else:
        if body.seed_channel_id is not None:
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_ENTITY,
                "у поиска по строке не бывает семени — уберите seed_channel_id "
                "или смените kind на similar")
        query_text = (body.query or "").strip()
        if not query_text:
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_ENTITY,
                "пустая строка поиска — назовите, что искать")
        if len(query_text) > 255:
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_ENTITY,
                "строка поиска длиннее 255 символов — сократите её")

    # Окно уникальности проверяем и здесь, best effort: гонку всё равно ловит
    # индекс в прогоне (образец гонки — постановка очереди), но нажатие второй
    # кнопки по невнимательности честнее ответить сразу, чем красной строкой
    # в Runs через минуту.
    day_start = discovery_service._utc_day_start()
    dup_q = (select(func.count(DiscoveryQuery.id))
             .where(DiscoveryQuery.kind == body.kind,
                    DiscoveryQuery.created_at >= day_start,
                    DiscoveryQuery.seed_channel_id == seed_channel_id
                    if body.kind == "similar"
                    else DiscoveryQuery.query == query_text))
    if (await db.execute(dup_q)).scalar_one():
        raise HTTPException(
            status.HTTP_409_CONFLICT, "этот семен/запрос уже искали сегодня")

    # Аккаунт выбирается здесь, а не в прогоне: человек тратит бюджет
    # конкретного аккаунта, и какой именно — решается на нажатии. Нет активных —
    # 503, а не 409: это состояние флота, а не конфликт запроса.
    account_id = body.account_id
    if account_id is None:
        try:
            fleet = await engage.list_accounts()
        except engage.EngageUnavailable as e:
            raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE,
                                str(e)) from e
        active = [a["account_id"] for a in fleet if a.get("status") == "active"]
        if not active:
            raise HTTPException(
                status.HTTP_503_SERVICE_UNAVAILABLE,
                "во флоте Engage нет активных аккаунтов — поиск некому выполнить")
        account_id = active[0]

    name = (f"Поиск похожих каналов{seed_name}" if body.kind == "similar"
            else f"Поиск публичных чатов · «{query_text}»")
    try:
        run = await jobs.start(
            db, kind="discovery_scan",
            params={"kind": body.kind, "seed_channel_id": seed_channel_id,
                    "query": query_text, "account_id": account_id},
            name=name, user_email=user.email)
    except jobs.JobBusy as e:
        raise HTTPException(status.HTTP_409_CONFLICT, str(e)) from e
    except jobs.JobQueueDown as e:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, str(e)) from e

    db.add(AuditLog(
        user_id=user.id, user_email=user.email, action="discovery_scan_started",
        detail={"kind": body.kind, "seed_channel_id": seed_channel_id,
                "query": query_text, "account_id": account_id, "run_id": run.id},
        ip=request.client.host if request.client else None))
    await db.commit()
    logger.info("discovery_scan_started kind=%s seed=%s query=%r account=%s "
                "run=%s by=%s", body.kind, seed_channel_id, query_text,
                account_id, run.id, user.email)
    return {"started": True, "run_id": run.id,
            "note": "поиск поставлен в очередь; кандидаты появятся в разделе "
                    "Discovery, ход — в Runs"}


# ── §3.3 — решение оператора ──────────────────────────────────────────────────

class DecideRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    decision: str
    reason: str | None = None


@router.post("/candidates/{candidate_id}/decide")
async def decide(candidate_id: int, body: DecideRequest, request: Request,
                 db: GetDB,
                 user=permits(Section.CHANNELS, Capability.CHANNEL_EDIT)):
    """Одобрить или отклонить кандидата.

    Право — `channel.edit`: одобрение — то же распоряжение будущим подключением,
    что и правка реестра каналов, и гость его не получает.

    Переходы: `pending → approved|rejected` — всегда; `approved ↔ rejected` —
    пока не `connected`; `connected` — точка невозврата (фактическое
    подключение, тот же принцип, что у сценариев: невозвратна доставка, а не
    одобрение). Отказ без причины не принимается: причина — данные, по которым
    оператор потом видит, почему кандидат отвергнут.
    """
    _check(body.decision, ("approved", "rejected"), "решение")
    reason = (body.reason or "").strip() or None
    if body.decision == "rejected" and not reason:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            "отказ без причины не принимается — напишите, чем канал не подошёл")

    candidate = await db.get(ChannelCandidate, candidate_id)
    if candidate is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND,
                            f"кандидат {candidate_id} не найден")
    if candidate.decision == "connected":
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"кандидат #{candidate_id} уже подключён — состояние «connected» "
            f"точки невозврата, решение по нему больше не меняется")

    candidate.decision = body.decision
    candidate.decided_by = user.email
    candidate.decided_at = clock.utcnow()
    candidate.decision_reason = reason
    db.add(AuditLog(
        user_id=user.id, user_email=user.email,
        action="discovery_candidate_decided",
        detail={"candidate_id": candidate.id, "decision": body.decision,
                "username": candidate.username, "reason": reason},
        ip=request.client.host if request.client else None))
    await db.commit()
    logger.info("discovery_candidate_decided candidate=%s decision=%s by=%s",
                candidate.id, body.decision, user.email)
    return _candidate_row(candidate)


# ── §3.5 — история поисков ────────────────────────────────────────────────────

@router.get("/queries")
async def list_queries(db: GetDB, user=requires(Section.CHANNELS),
                       limit: int = Query(50, ge=1, le=500),
                       offset: int = Query(0, ge=0)):
    """История поисков и объём одного прогона.

    Без сортировки и фильтров намеренно: история одна, и порядок у неё жёсткий —
    свежие поиски сверху (`created_at DESC, id DESC`, образец «без сортировки» —
    очередь дочитывания). Параметров сортировки у ручки нет вовсе, чтобы экран
    не мог попросить порядок, которым история не читается.

    `per_run` — объём одного прогона (`discovery_queries_per_scan`): сколько
    поисковых заказов Engage сделает один запуск поиска. Это собственный лимит
    Radar, НЕ остаток Engage. Суточного счётчика поисков больше не существует
    (Решение 1), и остатка «на сегодня» ручка не показывает — остаток поисков
    как понятие удалено.
    """
    total = (await db.execute(select(func.count(DiscoveryQuery.id)))).scalar_one()
    rows = (await db.execute(
        select(DiscoveryQuery)
        .order_by(DiscoveryQuery.created_at.desc(), DiscoveryQuery.id.desc())
        .limit(limit).offset(offset))).scalars().all()
    per_run = int((await discovery_service.thresholds(db))
                  ["discovery_queries_per_scan"])
    return {"total": total, "limit": limit, "offset": offset,
            "rows": [_query_row(q) for q in rows],
            "per_run": {"queries": per_run}}
