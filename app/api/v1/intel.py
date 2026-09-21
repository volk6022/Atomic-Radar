"""Раздел Intel (SPEC-intel-screens, контракт `_TASK-intel-screens.md` §4): ключ,
проверка файла, пачки ресёрчей, строки с ревью, выгрузка.

Значение ключа Intel через API не проходит никогда — только имя переменной
окружения: секрет живёт в `.env` инстанса, а экран показывает четыре последних
символа. Пачка — одна за раз (`JobBusy` прогона), запрос собирается из шаблона
`{{col}}` на сервере: клиентский предпросмотр — удобство, серверная сборка — истина.
"""
from __future__ import annotations

import csv
import io
import json
import logging
import math
import os
import re
from copy import deepcopy
from urllib.parse import quote

import jsonschema
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select

from app.api.deps import GetDB, permits, requires
from app.api.v1.listing import ListParams, apply_sort, list_params
from app.core import clock
from app.core.access import Capability, Section
from app.db.models import AuditLog, IntelKey, ResearchBatch, ResearchItem, Run
from app.services import intel_client, jobs

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/intel", tags=["intel"])

MAX_ROWS = 1_000
DEFAULT_KEY = "default"
PLACEHOLDER = re.compile(r"\{\{\s*(\w+)\s*\}\}")
BATCH_SORTS = {"created": ResearchBatch.created_at, "status": ResearchBatch.status,
               "total": ResearchBatch.total}
ITEM_SORTS = {"row_no": ResearchItem.row_no, "critic_score": ResearchItem.critic_score,
              "updated_at": ResearchItem.updated_at, "status": ResearchItem.status}
REVIEW_STATES = ("new", "accepted", "rejected")
SOURCE_KINDS = ("json", "csv")
MODES = ("speed", "balanced", "quality")


def _iso(dt) -> str | None:
    return dt.isoformat() if dt is not None else None


def _num(v):
    return float(v) if v is not None else None


async def _key(db) -> IntelKey | None:
    return (await db.execute(
        select(IntelKey).where(IntelKey.key == DEFAULT_KEY))).scalar_one_or_none()


def _key_view(k: IntelKey | None) -> dict:
    if k is None:
        return {"configured": False, "base_url": None, "api_key_env": "RADAR_INTEL_API_KEY",
                "masked": None, "concurrency": None, "quota_per_hour": None,
                "last_ok_at": None, "last_error": None,
                "ratelimit": {"limit": None, "remaining": None}}
    value = os.environ.get(k.api_key_env) or ""
    return {"configured": bool(value), "base_url": k.base_url, "api_key_env": k.api_key_env,
            "masked": ("…" + value[-4:]) if value else None,
            "concurrency": k.concurrency, "quota_per_hour": k.quota_per_hour,
            "last_ok_at": _iso(k.last_ok_at), "last_error": k.last_error,
            "ratelimit": {"limit": k.last_ratelimit_limit,
                          "remaining": k.last_ratelimit_remaining}}


async def _probe(db, k: IntelKey) -> None:
    """`GET /intel/healthz` — единственный сетевой ход экрана ключа."""
    try:
        ok = await intel_client.healthz(await intel_client.endpoint(db))
    except intel_client.IntelNotConfigured as e:
        k.last_error = str(e)
    else:
        if ok:
            k.last_ok_at, k.last_error = clock.utcnow(), None
        else:
            k.last_error = "healthz не ответил 200"
    await db.commit()


def render(template: str, row: dict) -> str:
    """Подстановка `{{col}}` значениями строки; чужая колонка — ошибка, а не пустота:
    запрос с дырой уйдёт в Intel и сожжёт квоту на бессмыслицу."""
    def sub(m):
        col = m.group(1)
        if col not in row or row[col] is None:
            raise KeyError(col)
        return str(row[col])
    return PLACEHOLDER.sub(sub, template)


def validate(rows: list[dict], template: str, schema: dict | None,
             quota_per_hour: int | None) -> dict:
    """Проверки до постановки (§4 п.3). Строки нумеруются с 1 — как в файле."""
    errors: list[dict] = []
    warnings: list[dict] = []
    total = len(rows)
    if total == 0:
        errors.append({"row": 0, "field": "rows", "message": "в файле нет строк"})
    if total > MAX_ROWS:
        errors.append({"row": 0, "field": "rows",
                       "message": f"строк {total}, потолок пачки {MAX_ROWS}"})
    if not template.strip():
        errors.append({"row": 0, "field": "prompt_template", "message": "пустой шаблон запроса"})
    if schema is not None:
        try:
            jsonschema.validators.validator_for(schema).check_schema(schema)
        except jsonschema.exceptions.SchemaError as e:
            errors.append({"row": 0, "field": "schema_json",
                           "message": f"это не JSON Schema: {e.message}"})
    seen: dict[str, int] = {}
    if not errors:
        for i, row in enumerate(rows, 1):
            if not isinstance(row, dict):
                errors.append({"row": i, "field": "rows", "message": "строка — не объект"})
                continue
            try:
                q = render(template, row)
            except KeyError as e:
                errors.append({"row": i, "field": "prompt_template",
                               "message": f"в строке нет колонки {e.args[0]}"})
                continue
            if not 3 <= len(q) <= 8000:
                errors.append({"row": i, "field": "query",
                               "message": f"длина запроса {len(q)}, допустимо 3..8000"})
                continue
            if q in seen:
                warnings.append({"row": i, "field": "query",
                                 "message": f"дубликат строки {seen[q]}"})
            else:
                seen[q] = i
    quota = quota_per_hour or 500
    return {"ok": not errors, "errors": errors, "warnings": warnings, "total": total,
            "estimated_quota_hours": math.ceil(total / quota) if total else 0}


class KeyBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    base_url: str = Field(min_length=8, max_length=255)
    api_key_env: str = Field(default="RADAR_INTEL_API_KEY", pattern=r"^[A-Z][A-Z0-9_]{2,63}$")
    concurrency: int = Field(default=2, ge=1, le=10)
    quota_per_hour: int = Field(default=500, ge=1, le=100_000)


class ValidateBody(BaseModel):
    # `schema_json` — имя поля контракта; в Pydantic так зовётся метод BaseModel,
    # поэтому внутри поле живёт как `output_schema`, а снаружи принимается по алиасу.
    model_config = ConfigDict(extra="forbid", populate_by_name=True)
    rows: list[dict]
    prompt_template: str
    output_schema: dict | None = Field(default=None, alias="schema_json")
    source_kind: str = "json"
    source_name: str | None = None
    # Экран шлёт на проверку ту же форму, что и на запуск, — лишние поля не ошибка.
    name: str | None = Field(default=None, max_length=255)
    mode: str = "quality"
    language: str = Field(default="ru", min_length=2, max_length=8)


class BatchBody(ValidateBody):
    name: str = Field(min_length=1, max_length=255)


class ItemPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    review_status: str | None = None
    notes: str | None = None
    edited: dict | None = None


@router.get("/key")
async def get_key(db: GetDB, probe: int = Query(0, ge=0, le=1),
                  user=requires(Section.INTEL)):
    k = await _key(db)
    if k is not None and probe:
        await _probe(db, k)
    return _key_view(k)


@router.put("/key")
async def put_key(body: KeyBody, request: Request, db: GetDB,
                  user=permits(Section.INTEL, Capability.INTEL_KEY_EDIT)):
    """Адрес, имя переменной, потолки — и пробный `healthz`. Ключ — только в `.env`."""
    k = await _key(db)
    if k is None:
        k = IntelKey(key=DEFAULT_KEY, base_url=body.base_url)
        db.add(k)
    k.base_url = body.base_url.rstrip("/")
    k.api_key_env = body.api_key_env
    k.concurrency = body.concurrency
    k.quota_per_hour = body.quota_per_hour
    k.is_active = True
    await db.commit()
    await _probe(db, k)
    db.add(AuditLog(user_id=user.id, user_email=user.email, action="intel_key_update",
                    detail={"base_url": k.base_url, "api_key_env": k.api_key_env,
                            "concurrency": k.concurrency, "quota_per_hour": k.quota_per_hour},
                    ip=request.client.host if request.client else None))
    await db.commit()
    return _key_view(k)


def _check_enums(body: ValidateBody) -> None:
    if body.source_kind not in SOURCE_KINDS:
        raise HTTPException(422, f"source_kind: ожидается {SOURCE_KINDS}")
    if body.mode not in MODES:
        raise HTTPException(422, f"mode: ожидается {MODES}")


@router.post("/batches/validate")
async def validate_batch(body: ValidateBody, db: GetDB,
                         user=permits(Section.INTEL, Capability.INTEL_RUN)):
    _check_enums(body)
    k = await _key(db)
    return validate(body.rows, body.prompt_template, body.output_schema,
                    k.quota_per_hour if k else None)


@router.post("/batches", status_code=status.HTTP_202_ACCEPTED)
async def create_batch(body: BatchBody, request: Request, db: GetDB,
                       user=permits(Section.INTEL, Capability.INTEL_RUN)):
    """Пачка + строка на запрос + прогон. 422 — тот же список ошибок, что у validate;
    409 — пачка уже идёт (одна на инстанс, решение по умолчанию §11)."""
    _check_enums(body)
    k = await _key(db)
    report = validate(body.rows, body.prompt_template, body.output_schema,
                      k.quota_per_hour if k else None)
    if not report["ok"]:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, report)
    busy = await jobs.active_run(db, "intel_research")
    if busy is not None:
        raise HTTPException(status.HTTP_409_CONFLICT,
                            f"уже идёт пачка (прогон #{busy.id}) — дождитесь или отмените")

    batch = ResearchBatch(
        name=body.name, status="queued", prompt_template=body.prompt_template,
        schema_json=body.output_schema, source_kind=body.source_kind,
        source_name=body.source_name, mode=body.mode, language=body.language,
        total=len(body.rows), created_by=user.email)
    db.add(batch)
    await db.flush()
    db.add_all([ResearchItem(batch_id=batch.id, row_no=i, input=row,
                             query=render(body.prompt_template, row))
                for i, row in enumerate(body.rows, 1)])
    await db.commit()
    try:
        run = await jobs.start(db, kind="intel_research", params={"batch_id": batch.id},
                               name=f"Intel · {body.name}", user_email=user.email)
    except jobs.JobBusy as e:
        raise HTTPException(status.HTTP_409_CONFLICT, str(e)) from e
    except jobs.JobQueueDown as e:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, str(e)) from e
    batch.run_id = run.id
    db.add(AuditLog(user_id=user.id, user_email=user.email, action="intel_batch_start",
                    detail={"batch_id": batch.id, "run_id": run.id, "total": batch.total,
                            "mode": batch.mode, "source": body.source_name},
                    ip=request.client.host if request.client else None))
    await db.commit()
    logger.info("intel_batch_started batch=%s run=%s total=%s by=%s",
                batch.id, run.id, batch.total, user.email)
    return {"batch_id": batch.id, "run_id": run.id, "total": batch.total}


def _batch_row(b: ResearchBatch) -> dict:
    return {"id": b.id, "name": b.name, "status": b.status, "total": b.total,
            "done": b.done, "failed": b.failed, "mode": b.mode, "language": b.language,
            "source_kind": b.source_kind, "source_name": b.source_name,
            "created_by": b.created_by, "created_at": _iso(b.created_at),
            "started_at": _iso(b.started_at), "finished_at": _iso(b.finished_at),
            "run_id": b.run_id}


@router.get("/batches")
async def list_batches(db: GetDB, p: ListParams = Depends(list_params),
                       user=requires(Section.INTEL)):
    q = select(ResearchBatch)
    q = apply_sort(q, p, BATCH_SORTS, default="created", tiebreak=ResearchBatch.id)
    rows = (await db.execute(q.limit(p.limit).offset(p.offset))).scalars().all()
    total = (await db.execute(select(func.count()).select_from(ResearchBatch))).scalar_one()
    return {**p.page(total), "rows": [_batch_row(b) for b in rows]}


async def _batch_or_404(db, batch_id: int) -> ResearchBatch:
    b = await db.get(ResearchBatch, batch_id)
    if b is None:
        raise HTTPException(404, f"пачка {batch_id} не найдена")
    return b


@router.get("/batches/{batch_id}")
async def get_batch(batch_id: int, db: GetDB, user=requires(Section.INTEL)):
    b = await _batch_or_404(db, batch_id)
    counts = dict((await db.execute(
        select(ResearchItem.status, func.count()).where(ResearchItem.batch_id == b.id)
        .group_by(ResearchItem.status))).all())
    log: list = []
    if b.run_id:
        run = await db.get(Run, b.run_id)
        log = (run.log or [])[-50:] if run is not None else []
    return {**_batch_row(b), "counts": counts,
            "waiting_llm": counts.get("waiting_llm", 0),
            "running": counts.get("running", 0) + counts.get("submitted", 0),
            "log": log}


@router.post("/batches/{batch_id}/cancel")
async def cancel_batch(batch_id: int, request: Request, db: GetDB,
                       user=permits(Section.INTEL, Capability.INTEL_RUN)):
    """Флаг отмены прогону; `pending` сразу в `cancelled`, поставленные дорабатывают."""
    b = await _batch_or_404(db, batch_id)
    run = await db.get(Run, b.run_id) if b.run_id else None
    if run is not None and run.status in jobs.ACTIVE:
        await jobs.request_cancel(db, run)
    for item in (await db.execute(
            select(ResearchItem).where(ResearchItem.batch_id == b.id,
                                       ResearchItem.status == "pending"))).scalars().all():
        item.status = "cancelled"
        item.finished_at = clock.utcnow()
    if run is None or run.status not in jobs.ACTIVE:
        b.status = "cancelled"
        b.finished_at = b.finished_at or clock.utcnow()
    db.add(AuditLog(user_id=user.id, user_email=user.email, action="intel_batch_cancel",
                    detail={"batch_id": b.id, "run_id": b.run_id},
                    ip=request.client.host if request.client else None))
    await db.commit()
    return {"id": b.id, "status": b.status}


@router.get("/batches/{batch_id}/items")
async def list_items(batch_id: int, db: GetDB, p: ListParams = Depends(list_params),
                     status_: str | None = Query(None, alias="status"),
                     review_status: str | None = Query(None),
                     user=requires(Section.INTEL)):
    await _batch_or_404(db, batch_id)
    q = select(ResearchItem).where(ResearchItem.batch_id == batch_id)
    if status_:
        q = q.where(ResearchItem.status == status_)
    if review_status:
        q = q.where(ResearchItem.review_status == review_status)
    if p.q:
        q = q.where(ResearchItem.query.ilike(f"%{p.q}%"))
    total = (await db.execute(
        select(func.count()).select_from(q.subquery()))).scalar_one()
    q = apply_sort(q, p, ITEM_SORTS, default="row_no", tiebreak=ResearchItem.id)
    rows = (await db.execute(q.limit(p.limit).offset(p.offset))).scalars().all()
    return {**p.page(total), "rows": [
        {"id": i.id, "row_no": i.row_no, "query": i.query[:160], "status": i.status,
         "critic_score": _num(i.critic_score), "tokens": i.tokens,
         "elapsed_seconds": _num(i.elapsed_seconds), "review_status": i.review_status,
         "updated_at": _iso(i.updated_at), "has_result": i.result is not None,
         "error": i.error} for i in rows]}


async def _item_or_404(db, item_id: int) -> ResearchItem:
    i = await db.get(ResearchItem, item_id)
    if i is None:
        raise HTTPException(404, f"строка {item_id} не найдена")
    return i


@router.get("/items/{item_id}")
async def get_item(item_id: int, db: GetDB, user=requires(Section.INTEL)):
    i = await _item_or_404(db, item_id)
    return {"id": i.id, "batch_id": i.batch_id, "row_no": i.row_no, "input": i.input,
            "query": i.query, "status": i.status, "result": i.result, "edited": i.edited,
            "review_status": i.review_status, "notes": i.notes, "error": i.error,
            "critic_score": _num(i.critic_score), "output": output_of(i)}


def apply_edits(output, edited: dict | None):
    """Наложить правки «путь.через.точки → значение» на копию структурированного ответа."""
    if not isinstance(output, dict) or not edited:
        return output
    out = deepcopy(output)
    for path, value in edited.items():
        parts = [p for p in str(path).split(".") if p]
        if not parts:
            continue
        cur = out
        for part in parts[:-1]:
            nxt = cur.get(part)
            if not isinstance(nxt, dict):
                nxt = {}
                cur[part] = nxt
            cur = nxt
        cur[parts[-1]] = value
    return out


def output_of(i: ResearchItem):
    """Что показывать и выгружать: структурный ответ с правками, иначе markdown."""
    result = i.result or {}
    if result.get("structured_output") is not None:
        return apply_edits(result["structured_output"], i.edited)
    return result.get("answer_markdown")


@router.patch("/items/{item_id}")
async def patch_item(item_id: int, body: ItemPatch, request: Request, db: GetDB,
                     user=permits(Section.INTEL, Capability.INTEL_REVIEW)):
    """Решение ревью, заметка, правки полей — правки проверяются схемой пачки."""
    i = await _item_or_404(db, item_id)
    if body.review_status is not None:
        if body.review_status not in REVIEW_STATES:
            raise HTTPException(422, f"review_status: ожидается {REVIEW_STATES}")
        i.review_status = body.review_status
    if body.notes is not None:
        i.notes = body.notes
    if body.edited is not None:
        base = (i.result or {}).get("structured_output")
        if base is None and body.edited:
            raise HTTPException(422, "у строки нет структурного ответа — править нечего")
        batch = await db.get(ResearchBatch, i.batch_id)
        if batch is not None and batch.schema_json and body.edited:
            try:
                jsonschema.validate(apply_edits(base, body.edited), batch.schema_json)
            except jsonschema.exceptions.ValidationError as e:
                raise HTTPException(422, f"правка не проходит схему: {e.message}") from e
        i.edited = body.edited or None
    db.add(AuditLog(user_id=user.id, user_email=user.email, action="intel_item_review",
                    detail={"item_id": i.id, "batch_id": i.batch_id,
                            "review_status": i.review_status,
                            "edited_keys": sorted((body.edited or {}).keys())},
                    ip=request.client.host if request.client else None))
    await db.commit()
    return {"id": i.id, "review_status": i.review_status, "notes": i.notes,
            "edited": i.edited, "output": output_of(i)}


def flatten(value, prefix: str = "") -> dict:
    """Вложенные объекты → `a.b`, массивы → склейка через `;`, None → пусто."""
    if isinstance(value, dict):
        out: dict = {}
        for k, v in value.items():
            out.update(flatten(v, f"{prefix}.{k}" if prefix else str(k)))
        return out
    if isinstance(value, list):
        return {prefix: ";".join("" if x is None else
                                 (json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list))
                                  else str(x)) for x in value)}
    return {prefix: "" if value is None else value}


def export_rows(items: list[ResearchItem]) -> list[dict]:
    return [{"row_no": i.row_no, "input": i.input, "query": i.query, "status": i.status,
             "output": output_of(i), "critic_score": _num(i.critic_score),
             "review_status": i.review_status, "notes": i.notes,
             "sources": (i.result or {}).get("sources")} for i in items]


def to_csv(rows: list[dict]) -> str:
    """UTF-8 с BOM и `;` — так файл открывается в русском Excel без мастера импорта."""
    flat = [flatten(r) for r in rows]
    headers: list[str] = []
    for f in flat:
        for k in f:
            if k not in headers:
                headers.append(k)
    buf = io.StringIO()
    buf.write("﻿")
    w = csv.writer(buf, delimiter=";", quoting=csv.QUOTE_MINIMAL, lineterminator="\r\n")
    w.writerow(headers)
    for f in flat:
        w.writerow([f.get(h, "") for h in headers])
    return buf.getvalue()


@router.get("/batches/{batch_id}/export")
async def export_batch(batch_id: int, db: GetDB, format: str = Query("json"),
                       review_status: str | None = Query(None),
                       user=permits(Section.INTEL, Capability.INTEL_EXPORT)):
    if format not in ("json", "csv"):
        raise HTTPException(422, "format: json или csv")
    b = await _batch_or_404(db, batch_id)
    q = select(ResearchItem).where(ResearchItem.batch_id == b.id).order_by(ResearchItem.row_no)
    if review_status:
        q = q.where(ResearchItem.review_status == review_status)
    rows = export_rows((await db.execute(q)).scalars().all())
    stem = re.sub(r"[^\w.-]+", "_", b.name)[:60] or f"batch-{b.id}"
    if format == "json":
        body = json.dumps(rows, ensure_ascii=False, indent=2)
        media, ext = "application/json; charset=utf-8", "json"
    else:
        body = to_csv(rows)
        media, ext = "text/csv; charset=utf-8", "csv"
    # HTTP-заголовки — только latin-1: имя пачки с кириллицей в `filename="…"`
    # роняет ответ с UnicodeEncodeError уже на кодировании заголовка. ASCII-заглушка
    # для старых клиентов, настоящее имя — по RFC 5987: filename*=UTF-8''…
    fallback = (re.sub(r"[^\w.-]+", "_", b.name, flags=re.ASCII)[:60].strip("._-")
                or f"batch-{b.id}")
    disposition = (f"attachment; filename=\"intel-{b.id}-{fallback}.{ext}\"; "
                   f"filename*=UTF-8''{quote(f'intel-{b.id}-{stem}.{ext}')}")
    return StreamingResponse(iter([body.encode("utf-8")]), media_type=media, headers={
        "Content-Disposition": disposition})
