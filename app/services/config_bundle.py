"""Настройки отбора целиком, одним файлом: выгрузить, поправить снаружи, залить назад.

Единица обмена здесь — весь набор: описание бизнеса, боли с якорями L1 и эталонными
фразами L2, шум, дисквалификаторы, системные промпты L3. Не отдельная боль и не
отдельный промпт.

Почему так, а не версиями по сущностям. Прежний порядок правки был: предложить
(`CONFIG_PROPOSE`) — включить (`CONFIG_ACTIVATE`), у каждой сущности своя лесенка
версий (`v1`, `v2`, `l3-verdict-v4`). Со стороны человека, который хочет поменять
формулировки, это не работает: непонятно, что включено сейчас, нельзя поменять всё
разом, и нельзя открыть настройки в привычном редакторе. Версии при этом никуда не
делись — они по-прежнему пишутся под капотом и остаются историей, — но снаружи
единица работы теперь одна: файл.

⚠️ Загрузка целая или никакая. Половина применённого — это не частичный успех, а
отбор по правилам, которых никто не задавал: якоря уже новые, эталоны ещё старые, и
система при этом выглядит работающей. Поэтому сначала проверяется весь файл, потом
пишется самое рискованное (эталоны L2, которым нужен эмбеддер), и только если оно
прошло — остальное.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy import select

from app.db.models import L2Prototype, L3Prompt
from app.services import cascade_registry, llm

logger = logging.getLogger(__name__)

FORMAT = "atomic-radar-config"
VERSION = 1


class BundleError(ValueError):
    """Файл настроек не годится, и ни одна его часть не применена."""


def _clean_phrases(value, *, where: str) -> list[str]:
    if not isinstance(value, list):
        raise BundleError(f"{where}: ожидался список строк")
    out = [str(p).strip() for p in value if str(p).strip()]
    if not out:
        raise BundleError(f"{where}: список пуст")
    return out


def validate(bundle) -> None:
    """Всё, что можно проверить, не трогая базу. Зовётся до первой записи."""
    if not isinstance(bundle, dict):
        raise BundleError("файл настроек должен быть объектом JSON")
    if bundle.get("format") != FORMAT:
        raise BundleError(
            f"чужой формат: ожидался «{FORMAT}», в файле «{bundle.get('format')}»")
    if bundle.get("version") != VERSION:
        raise BundleError(
            f"версия формата {bundle.get('version')} не поддерживается, нужна {VERSION}")

    business = bundle.get("business") or {}
    if not str(business.get("description") or "").strip():
        raise BundleError("описание бизнеса пустое — по нему модель понимает, "
                          "чьи задачи искать")

    pains = bundle.get("pains") or {}
    if not isinstance(pains, dict) or not pains:
        raise BundleError("в файле нет ни одной боли — отбирать будет нечем")
    for label, body in pains.items():
        if not isinstance(body, dict):
            raise BundleError(f"боль «{label}»: ожидался объект с anchors и prototypes")
        _clean_phrases(body.get("anchors"), where=f"боль «{label}», якоря L1")
        _clean_phrases(body.get("prototypes"), where=f"боль «{label}», эталоны L2")

    for label, items in (bundle.get("noise") or {}).items():
        _clean_phrases(items, where=f"шум «{label}»")
    for label, words in (bundle.get("disqualifiers") or {}).items():
        _clean_phrases(words, where=f"дисквалификатор «{label}»")

    prompts = bundle.get("l3_prompts") or {}
    if not isinstance(prompts, dict):
        raise BundleError("l3_prompts: ожидался объект «ключ → текст промпта»")
    for key, body in prompts.items():
        if key not in llm.PROMPTS:
            raise BundleError(
                f"промпт «{key}» неизвестен; известны: {', '.join(sorted(llm.PROMPTS))}")
        if not str(body or "").strip():
            raise BundleError(f"промпт «{key}»: пустой текст")


async def export_bundle(db, *, name: str | None = None) -> dict:
    """Текущие активные настройки в том же виде, в каком их принимает загрузка.

    Круг обязан замыкаться: выгруженный файл грузится обратно без правок руками.
    Иначе «поправить снаружи» превращается в «собрать заново».
    """
    profile = await cascade_registry.active_profile_version(db)
    cascade = await cascade_registry.active_cascade_version(db)

    pains: dict[str, dict] = {}
    noise: dict[str, list[str]] = {}
    if cascade is not None:
        rows = (await db.execute(select(L2Prototype).where(
            L2Prototype.cascade_version_id == cascade.id).order_by(L2Prototype.id))
        ).scalars().all()
        positive: dict[str, list[str]] = {}
        for r in rows:
            (positive if r.kind == "pos" else noise).setdefault(r.label, []).append(
                r.phrase)
        for label, anchors in cascade.pain_anchors.items():
            pains[label] = {"anchors": list(anchors),
                            "prototypes": positive.get(label, [])}

    prompts: dict[str, str] = {}
    for key in sorted(llm.PROMPTS):
        row = (await db.execute(select(L3Prompt).where(
            L3Prompt.prompt_key == key, L3Prompt.is_active.is_(True)))
        ).scalar_one_or_none()
        if row is not None:
            prompts[key] = row.system_prompt

    return {
        "format": FORMAT,
        "version": VERSION,
        "name": name or (profile.version if profile else "текущие настройки"),
        "exported_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "business": {"description": profile.business_description if profile else None},
        "pains": pains,
        "noise": noise,
        "disqualifiers": {k: list(v)
                          for k, v in (cascade.disqualifiers if cascade else {}).items()},
        "l3_prompts": prompts,
    }


async def import_bundle(db, bundle, *, actor: str) -> dict:
    """Применить файл настроек целиком и сразу. Отдельного «включить» нет.

    Порядок не случаен: таксономия идёт первой, потому что только она может
    отказать по внешней причине — новым эталонным фразам нужен эмбеддер. Пройдёт
    она — остальное уже не зависит ни от чего снаружи.
    """
    validate(bundle)

    pains_in = bundle["pains"]
    pains = {label: ([a.strip() for a in body["anchors"] if a.strip()],
                     [p.strip() for p in body["prototypes"] if p.strip()])
             for label, body in pains_in.items()}
    noise = {label: [p.strip() for p in items if p.strip()]
             for label, items in (bundle.get("noise") or {}).items()}
    disq = {label: [w.strip() for w in words if w.strip()]
            for label, words in (bundle.get("disqualifiers") or {}).items()}

    try:
        await cascade_registry.save_taxonomy(
            db, pains=pains, disqualifiers=disq, noise_prototypes=noise,
            actor=actor, activate=True, replace=True)
    except cascade_registry.TaxonomyValidationError as e:
        # `save_taxonomy` откатывает свою транзакцию сама, и до сюда мы доходим,
        # не записав ничего. Тип меняется, чтобы вызывающему не пришлось знать про
        # внутренние исключения реестра.
        raise BundleError(str(e)) from e

    await cascade_registry.save_business_description(
        db, business_description=bundle["business"]["description"],
        actor=actor, activate=True)

    applied_prompts = []
    for key, text in (bundle.get("l3_prompts") or {}).items():
        await cascade_registry.save_l3_prompt(db, prompt_key=key, system_prompt=text,
                                              actor=actor, activate=True)
        applied_prompts.append(key)

    await cascade_registry.reload(db)

    out = {"name": bundle.get("name"), "pains": len(pains), "noise": len(noise),
           "disqualifiers": len(disq), "prompts": sorted(applied_prompts),
           "prototypes": sum(len(p) for _, p in pains.values())
           + sum(len(v) for v in noise.values())}
    logger.info("config_bundle_imported name=%s pains=%s prompts=%s by=%s",
                out["name"], out["pains"], ",".join(out["prompts"]), actor)
    return out
