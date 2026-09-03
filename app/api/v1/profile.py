"""Запись профиля и таксономии каскада (FIXES.md #5).

`app/api/v1/screens.py` описывает себя как раздел на чтение — `GET /profile`
живёт там и остаётся неизменным по форме ответа. Запись вынесена отдельным
модулем: у неё другая забота (валидация, версии, эмбеддинги), и совмещать её с
чтением значило бы дать модулю два разных повода меняться.

**Владелец пишет и активирует сразу** (`Capability.CONFIG_EDIT` включает и
`CONFIG_PROPOSE` — он же выше в матрице), **заказчик только предлагает**
(`CONFIG_PROPOSE`): новая версия заводится, но `is_active` остаётся `False`, пока
владелец её не включит (`CONFIG_ACTIVATE`, `POST .../activate`). Так работа с
конфигурацией отбора не отличается от `profile_versions` до этой правки — разница
только в том, что запись теперь возможна вообще.

Активация версии таксономии или промпта L3 меняет то, чем каскад руководствуется
уже для следующего сообщения (перечитка — `cascade_registry.reload`, без
рестарта), но не для уже посчитанных вердиктов: их каскад пересчитывает только по
команде («Переклассификация» в разделе Runs). Отсюда `reclassify_suggested` в
ответе — подсказка интерфейсу предложить прогон, а не тихая автозапускалка: сам
прогон занимает видеокарту на десятки минут, и решать, когда его платить, должен
человек.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select

from app.api.deps import GetDB, permits, requires
from app.core import cascade
from app.core.access import Capability, Section, allows
from app.db.models import (AuditLog, CascadeVersion, ConfigFile, L3Prompt,
                           LlmTrace, ProfileVersion)
from app.services import cascade_registry, config_bundle

logger = logging.getLogger("radar.profile_api")
router = APIRouter(prefix="/api/v1/profile", tags=["profile"])


# ── история версий ───────────────────────────────────────────────────────────────

@router.get("/versions")
async def list_versions(db: GetDB, user=requires(Section.PROFILE)):
    """Все версии трёх редактируемых частей профиля — включая неактивированные
    предложения заказчика: без них «предложил и никто не увидел» неотличимо от
    «предложение потерялось»."""
    cascade_rows = (await db.execute(
        select(CascadeVersion).order_by(CascadeVersion.id.desc()).limit(20))).scalars().all()
    prompt_rows = (await db.execute(
        select(L3Prompt).order_by(L3Prompt.id.desc()).limit(20))).scalars().all()
    profile_rows = (await db.execute(
        select(ProfileVersion).order_by(ProfileVersion.id.desc()).limit(20))).scalars().all()

    return {"cascade": [
        {"id": r.id, "version": r.version, "is_active": r.is_active,
         "pains": len(r.pain_anchors or {}), "created_by": r.created_by,
         "created_at": r.created_at.isoformat()} for r in cascade_rows],
        "l3_prompts": [
        {"id": r.id, "prompt_key": r.prompt_key, "version": r.version,
         "is_active": r.is_active, "created_by": r.created_by,
         "created_at": r.created_at.isoformat()} for r in prompt_rows],
        "business": [
        {"id": r.id, "version": r.version, "is_active": r.is_active,
         "created_by": r.created_by, "created_at": r.created_at.isoformat()}
        for r in profile_rows]}


VERSION_KINDS = {"cascade", "prompt", "business"}


@router.post("/versions/{kind}/{version_id}/activate")
async def activate_version(kind: str, version_id: int, request: Request, db: GetDB,
                           user=permits(Section.PROFILE, Capability.CONFIG_ACTIVATE)):
    """Включить предложенную версию — то, чего у заказчика нет права сделать самому."""
    if kind not in VERSION_KINDS:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            f"неизвестный вид версии «{kind}», ожидается один из "
                            f"{', '.join(sorted(VERSION_KINDS))}")
    try:
        if kind == "cascade":
            row = await cascade_registry.activate_cascade_version(db, version_id)
            detail = {"kind": kind, "version": row.version}
        elif kind == "prompt":
            row = await cascade_registry.activate_l3_prompt(db, version_id)
            detail = {"kind": kind, "prompt_key": row.prompt_key, "version": row.version}
        else:
            row = await cascade_registry.activate_profile_version(db, version_id)
            detail = {"kind": kind, "version": row.version}
    except cascade_registry.TaxonomyValidationError as e:
        raise HTTPException(status.HTTP_404_NOT_FOUND, str(e)) from e

    await cascade_registry.reload(db)
    db.add(AuditLog(user_id=user.id, user_email=user.email, action="profile_version_activate",
                    detail=detail, ip=request.client.host if request.client else None))
    await db.commit()
    logger.info("profile_version_activated kind=%s id=%s by=%s", kind, version_id, user.email)
    return {"activated": True, **detail,
           "reclassify_suggested": kind in ("cascade", "prompt")}


# ── предложение правки ───────────────────────────────────────────────────────────

class PainInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    # Якоря L1 — обязательны и хотя бы один: боль без единого слова, по которому
    # её ищут, никогда не сработает и будет выглядеть решённой, а не забытой.
    anchors: list[str] = Field(min_length=1, max_length=40)
    # `None` — «эталоны L2 этой боли не трогаем», `[]` — «эталонов больше нет».
    # Разница у Pydantic по умолчанию не потерялась бы и без специальной пометки,
    # но важна она ровно ради того, чтобы правка одних якорей не обнулила чужие
    # эталонные фразы молча.
    prototypes: list[str] | None = Field(default=None, max_length=20)


class ProposalRequest(BaseModel):
    """Всё, что можно предложить одним запросом. Поля независимы: пришло —
    меняем, не пришло — не трогаем. Так правка одного промпта не требует
    пересылать все шесть болей заново."""
    model_config = ConfigDict(extra="forbid")

    business_description: str | None = Field(default=None, min_length=1, max_length=4000)
    pains: dict[str, PainInput] | None = None
    disqualifiers: dict[str, list[str]] | None = None
    noise_prototypes: dict[str, list[str]] | None = None
    l3_system_prompt: str | None = Field(default=None, min_length=1, max_length=8000)


@router.post("/proposals", status_code=status.HTTP_201_CREATED)
async def create_proposal(body: ProposalRequest, request: Request, db: GetDB,
                          user=permits(Section.PROFILE, Capability.CONFIG_PROPOSE)):
    """Сохранить правку. Владелец видит её включённой сразу, заказчик — как
    предложение, ждущее активации (см. докстринг модуля)."""
    if not any((body.business_description, body.pains, body.disqualifiers,
               body.noise_prototypes, body.l3_system_prompt)):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            "нечего сохранять: тело запроса пустое")

    activate = allows(user.role, Capability.CONFIG_EDIT)
    result: dict = {"activated": activate}
    audit_detail: dict = {}

    try:
        if body.business_description is not None:
            row = await cascade_registry.save_business_description(
                db, business_description=body.business_description,
                actor=user.email, activate=activate)
            result["business"] = {"id": row.id, "version": row.version}
            audit_detail["business_version"] = row.version

        if body.pains is not None or body.disqualifiers is not None \
                or body.noise_prototypes is not None:
            pains = ({label: (p.anchors, p.prototypes) for label, p in body.pains.items()}
                    if body.pains is not None else None)
            row = await cascade_registry.save_taxonomy(
                db, pains=pains, disqualifiers=body.disqualifiers,
                noise_prototypes=body.noise_prototypes,
                actor=user.email, activate=activate)
            result["cascade"] = {"id": row.id, "version": row.version}
            audit_detail["cascade_version"] = row.version

        if body.l3_system_prompt is not None:
            row = await cascade_registry.save_l3_prompt(
                db, prompt_key=cascade.profile(cascade.DEFAULT_PROFILE).l3_prompt_key,
                system_prompt=body.l3_system_prompt, actor=user.email, activate=activate)
            result["l3_prompt"] = {"id": row.id, "prompt_key": row.prompt_key,
                                   "version": row.version}
            audit_detail["l3_prompt_version"] = row.version
    except cascade_registry.TaxonomyValidationError as e:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(e)) from e

    if activate:
        await cascade_registry.reload(db)

    db.add(AuditLog(user_id=user.id, user_email=user.email,
                    action="profile_proposal" if not activate else "profile_edit",
                    detail=audit_detail, ip=request.client.host if request.client else None))
    await db.commit()
    logger.info("profile_proposal_saved by=%s activate=%s detail=%s",
               user.email, activate, audit_detail)

    result["reclassify_suggested"] = activate and bool(
        result.get("cascade") or result.get("l3_prompt"))
    return result


# ── подсказка «есть, что пересчитать» ─────────────────────────────────────────────

async def stale_l3_verdict_count(db) -> int:
    """Сколько трейсов L3 вынесено промптом, который больше не активен.

    Не количество лидов и не сообщений — трейс уже несёт версию промпта, которым
    получен, и это единственное место, где различие «старый вопрос / новый
    вопрос» видно без похода в код. Используется на `GET /screens/profile`, чтобы
    экран показывал не только текущий промпт, но и то, что часть решений вынесена
    другим.
    """
    active = await cascade_registry.active_l3_prompt(
        db, cascade.profile(cascade.DEFAULT_PROFILE).l3_prompt_key)
    if active is None:
        return 0
    return (await db.execute(
        select(func.count(LlmTrace.id))
        .where(LlmTrace.stage == "l3", LlmTrace.prompt_version.isnot(None),
              LlmTrace.prompt_version != active.version))).scalar_one()


# ── настройки одним файлом ────────────────────────────────────────────────────
#
# Внешняя единица работы — файл, а не версия. Версии остались историей под капотом
# (см. `app/services/config_bundle.py`), но человек забирает настройки целиком,
# правит их в редакторе и заливает обратно; отдельного шага «включить» нет, потому
# что именно он и превращал экран в лесенку кнопок.

def _file_row(row: ConfigFile, current_id: int | None) -> dict:
    return {"id": row.id, "name": row.name,
            "created_at": row.created_at.isoformat(),
            "created_by": row.created_by,
            "applied_at": row.applied_at.isoformat() if row.applied_at else None,
            "is_current": row.id == current_id,
            **config_bundle.summarize(row.body)}


async def _current_file_id(db) -> int | None:
    """Применённый последним — он и «текущий».

    Совпадение с живыми настройками отсюда не следует и следовать не может: их
    можно поправить и мимо файлов (`POST /proposals`). Поэтому пометка честно
    означает «этим файлом настройки приводили в порядок последний раз», а не
    «настройки равны файлу».
    """
    return (await db.execute(
        select(ConfigFile.id).where(ConfigFile.applied_at.isnot(None))
        .order_by(ConfigFile.applied_at.desc(), ConfigFile.id.desc()).limit(1))
    ).scalar_one_or_none()


@router.get("/config")
async def export_config(db: GetDB, user=requires(Section.PROFILE)):
    """Текущие настройки в том виде, в каком их принимает загрузка."""
    return await config_bundle.export_bundle(db)


@router.get("/config/files")
async def list_config_files(db: GetDB, user=requires(Section.PROFILE)):
    rows = (await db.execute(
        select(ConfigFile).order_by(ConfigFile.id.desc()))).scalars().all()
    current = await _current_file_id(db)
    return {"files": [_file_row(r, current) for r in rows]}


@router.get("/config/files/{file_id}")
async def read_config_file(file_id: int, db: GetDB, user=requires(Section.PROFILE)):
    """Отдаётся ровно то, что загрузили: файл правят снаружи, и возвращать надо то
    же самое тело, а не пересобранное из таблиц."""
    row = await db.get(ConfigFile, file_id)
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND,
                            f"набор настроек {file_id} не найден")
    return row.body


@router.post("/config/files", status_code=status.HTTP_201_CREATED)
async def upload_config_file(body: dict, request: Request, db: GetDB,
                             user=permits(Section.PROFILE, Capability.CONFIG_EDIT)):
    """Загрузить набор и применить его целиком и сразу.

    Право то же, что на включение версии, и это не строгость ради строгости:
    загрузка меняет правила отбора для следующего же сообщения.
    """
    try:
        row = await config_bundle.store(db, body, actor=user.email)
    except config_bundle.BundleError as e:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(e)) from e

    try:
        applied = await config_bundle.apply_stored(db, row.id, actor=user.email)
    except config_bundle.BundleError as e:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            f"файл сохранён, но применить его не удалось: {e}") from e

    db.add(AuditLog(user_id=user.id, user_email=user.email, action="config_file_apply",
                    detail={"file_id": row.id, "name": row.name, **applied},
                    ip=request.client.host if request.client else None))
    await db.commit()
    logger.info("config_file_applied id=%s name=%s by=%s", row.id, row.name, user.email)
    return {"file_id": row.id, "name": row.name, "applied": applied,
            "reclassify_suggested": True}


@router.post("/config/files/{file_id}/apply")
async def apply_config_file(file_id: int, request: Request, db: GetDB,
                            user=permits(Section.PROFILE, Capability.CONFIG_EDIT)):
    """Переключиться на ранее загруженный набор."""
    try:
        applied = await config_bundle.apply_stored(db, file_id, actor=user.email)
    except LookupError as e:
        raise HTTPException(status.HTTP_404_NOT_FOUND, str(e)) from e
    except config_bundle.BundleError as e:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(e)) from e

    db.add(AuditLog(user_id=user.id, user_email=user.email, action="config_file_apply",
                    detail={"file_id": file_id, **applied},
                    ip=request.client.host if request.client else None))
    await db.commit()
    logger.info("config_file_applied id=%s by=%s", file_id, user.email)
    return {"file_id": file_id, "applied": applied, "reclassify_suggested": True}


@router.delete("/config/files/{file_id}")
async def delete_config_file(file_id: int, request: Request, db: GetDB,
                             user=permits(Section.PROFILE, Capability.CONFIG_EDIT)):
    """Убрать набор из списка. Живых настроек это не касается: файл — снимок, а не
    источник работы, и удаление снимка не должно останавливать классификацию."""
    row = await db.get(ConfigFile, file_id)
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND,
                            f"набор настроек {file_id} не найден")
    name = row.name
    await db.delete(row)
    db.add(AuditLog(user_id=user.id, user_email=user.email, action="config_file_delete",
                    detail={"file_id": file_id, "name": name},
                    ip=request.client.host if request.client else None))
    await db.commit()
    logger.info("config_file_deleted id=%s name=%s by=%s", file_id, name, user.email)
    return {"deleted": True, "file_id": file_id}
