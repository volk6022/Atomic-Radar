"""Реестр таксономии каскада: версии из БД → правила в памяти процесса.

До 30.08 боли, дисквалификаторы, эталоны L2 и промпт L3 жили константами в
`app/core/cascade.py`, `app/core/prototypes.py` и `app/services/llm.py`. Экран
Profile & Prompts (`GET /screens/profile`) читал их напрямую и был честен: то, что
показано, — ровно то, чем каскад руководствуется. FIXES.md #5 требует сделать это
редактируемым, не сломав это свойство: редактор обязан писать в тот же источник, из
которого каскад читает правила.

Источник — таблицы `cascade_versions` / `l2_prototypes` / `l3_prompts`. Каскад
по-прежнему не знает про базу (`cascade.py` — «модуль намеренно без БД и без сети»):
он читает module-level словари (`cascade.PAIN_ANCHORS`, `prototypes.POSITIVE`, …),
а этот модуль — единственное место, которое эти словари подменяет, прочитав
активную строку. `apply_taxonomy`/`apply_prototypes`/`apply_prompt` мутируют
существующие объекты на месте: `CascadeProfile.pain_anchors` — это
`MappingProxyType`, вычисленный один раз при импорте и указывающий на тот же
словарь, поэтому мутация видна каскаду без пересоздания профиля и без рестарта
контейнера — то самое требование «перечитка без перезапуска».

Версионирование — по образцу `ProfileVersion`, с той же причиной: без версии нельзя
сказать, на каких правилах вынесен вердикт. `is_active=False` — не черновик и не
брак, а предложение (`Capability.CONFIG_PROPOSE` — заказчик может предложить правку
болей, но не включить её), которое владелец либо активирует
(`Capability.CONFIG_ACTIVATE`), либо оставит лежать непросмотренным.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import re
from typing import Mapping, Sequence

from sqlalchemy import select, update

from app.core import cascade, prototypes
from app.db.models import CascadeVersion, L2Prototype, L3Prompt, ProfileVersion
from app.db.session import get_session_maker
from app.services import embeddings, llm

logger = logging.getLogger("radar.cascade_registry")

# Кто расставил начальные строки при первом запуске — по аналогии с тем, как
# `engage_registry.ensure_bootstrap` подписывает свою строку.
BOOTSTRAP_ACTOR = "bootstrap"


class TaxonomyValidationError(ValueError):
    """Правка таксономии не прошла проверку. Текст уходит оператору как есть —
    ручка отвечает 422, а не молча принимает то, что каскад не сможет применить."""


def _bump_version(existing: Sequence[str], *, prefix: str = "v") -> str:
    """Следующая версия вида `v3`: на единицу больше наибольшего номера среди уже
    занятых строк с этим префиксом. Не просто «счётчик строк» — версии не
    удаляются, но порядок сохранения не гарантирует возрастания id при параллельной
    записи, а вот текстовый номер обязан расти монотонно, иначе на экране версии
    шли бы не по порядку."""
    nums = []
    for v in existing:
        m = re.fullmatch(re.escape(prefix) + r"(\d+)", v or "")
        if m:
            nums.append(int(m.group(1)))
    return f"{prefix}{(max(nums) + 1) if nums else 1}"


def _bump_prompt_version(current: str, *, prompt_key: str) -> str:
    """`l3-verdict-v4` → `l3-verdict-v5`. Формат заимствован у `llm.Prompt.version`
    напрямую: старые трейсы уже содержат такие строки, и новая версия обязана
    остаться узнаваемой рядом с ними, а не начать новую систему нумерации."""
    m = re.search(r"-v(\d+)$", current or "")
    if m:
        return current[: m.start()] + f"-v{int(m.group(1)) + 1}"
    return f"{prompt_key}-v2"


def normalize_words(words: Sequence[str], *, field: str) -> tuple[str, ...]:
    """Привести список якорей/маркеров к форме, в которой их ищет L1.

    `cascade._norm` сворачивает «ё» в «е» перед сравнением с текстом сообщения —
    якорь, сохранённый с «ё», не совпадёт никогда, и тишина будет выглядеть как
    баг, а не как опечатка. Нормализуем молча (не отклоняем: «счёт» — валидное
    русское слово, а не ошибка ввода), но пустых значений после normalize не
    прощаем — пустая строка входит в `_norm(text)` любого сообщения.
    """
    out: list[str] = []
    seen: set[str] = set()
    for raw in words:
        norm = cascade.normalize_word(raw)
        # Пустоту проверяем по обрезанной копии, а храним — необрезанную: у
        # `cascade.normalize_word` внешние пробелы сохраняются намеренно (см. его
        # докстринг про «вэд»), и обрезать их здесь означало бы то же самое
        # молчаливое повреждение якоря с другой стороны функции.
        if not norm.strip():
            raise TaxonomyValidationError(
                f"{field}: пустое значение (после нормализации «{raw!r}» — пусто)")
        if norm not in seen:
            seen.add(norm)
            out.append(norm)
    if not out:
        raise TaxonomyValidationError(f"{field}: нужен хотя бы один якорь")
    return tuple(out)


# ── чтение активных строк ───────────────────────────────────────────────────────

async def active_cascade_version(db) -> CascadeVersion | None:
    return (await db.execute(
        select(CascadeVersion).where(CascadeVersion.is_active.is_(True))
        .order_by(CascadeVersion.id.desc()).limit(1))).scalar_one_or_none()


async def active_l3_prompt(db, prompt_key: str) -> L3Prompt | None:
    return (await db.execute(
        select(L3Prompt).where(L3Prompt.prompt_key == prompt_key,
                               L3Prompt.is_active.is_(True))
        .order_by(L3Prompt.id.desc()).limit(1))).scalar_one_or_none()


async def active_profile_version(db) -> ProfileVersion | None:
    return (await db.execute(
        select(ProfileVersion).where(ProfileVersion.is_active.is_(True))
        .order_by(ProfileVersion.id.desc()).limit(1))).scalar_one_or_none()


# ── старт процесса ───────────────────────────────────────────────────────────────

async def ensure_bootstrap(db) -> bool:
    """Завести первую версию из констант кода, если таблицы пусты.

    Путь первого запуска на уже работающей установке, тот же приём, что у
    `engage_registry.ensure_bootstrap`: правила и так уже действуют (они лежат в
    коде), таблица версий просто ещё не знает об этом. Без этого шага редактор
    писал бы новую версию поверх пустоты, и первая правка обнулила бы всю
    таксономию вместо одной боли.
    """
    created = False

    if (await db.execute(select(CascadeVersion.id).limit(1))).first() is None:
        version = CascadeVersion(
            version="v1",
            pain_anchors={k: list(v) for k, v in cascade.PAIN_ANCHORS.items()},
            disqualifiers={k: list(v) for k, v in cascade.DISQUALIFIERS.items()},
            is_active=True, created_by=BOOTSTRAP_ACTOR)
        db.add(version)
        await db.flush()

        rows = [L2Prototype(cascade_version_id=version.id, kind="pos", label=label,
                            phrase=text)
                for label, texts in prototypes.POSITIVE.items() for text in texts]
        rows += [L2Prototype(cascade_version_id=version.id, kind="neg", label=label,
                             phrase=text)
                 for label, texts in prototypes.NEGATIVE.items() for text in texts]
        db.add_all(rows)
        created = True
        logger.info("cascade_version_bootstrapped version=v1 prototypes=%s", len(rows))

    for key, p in llm.PROMPTS.items():
        if await active_l3_prompt(db, key) is None:
            db.add(L3Prompt(prompt_key=key, version=p.version, system_prompt=p.system,
                            is_active=True, created_by=BOOTSTRAP_ACTOR))
            created = True
            logger.info("l3_prompt_bootstrapped key=%s version=%s", key, p.version)

    if created:
        await db.commit()
    return created


async def reload(db) -> None:
    """Перечитать активные строки в модули каскада. Идемпотентно — можно звать и
    на старте, и сразу после сохранения правки в том же процессе."""
    version = await active_cascade_version(db)
    if version is not None:
        cascade.apply_taxonomy(
            pain_anchors={k: tuple(v) for k, v in version.pain_anchors.items()},
            disqualifiers={k: tuple(v) for k, v in version.disqualifiers.items()})
        await _reload_prototypes(db, version)

    for key in list(llm.PROMPTS):
        row = await active_l3_prompt(db, key)
        if row is not None:
            llm.apply_prompt(key, llm.Prompt(key=key, version=row.version,
                                             system=row.system_prompt))

    logger.info("cascade_registry_reloaded cascade_version=%s",
               version.version if version else None)


async def _reload_prototypes(db, version: CascadeVersion) -> None:
    rows = (await db.execute(select(L2Prototype)
                             .where(L2Prototype.cascade_version_id == version.id))
            ).scalars().all()
    if not rows:
        return

    positive: dict[str, list[str]] = {}
    negative: dict[str, list[str]] = {}
    vectors: list[tuple[str, str, list[float]]] = []
    all_vectorized = True
    for r in rows:
        (positive if r.kind == "pos" else negative).setdefault(r.label, []).append(r.phrase)
        if r.vector is not None:
            vectors.append((r.kind, r.label, r.vector))
        else:
            all_vectorized = False

    prototypes.apply_prototypes(positive={k: tuple(v) for k, v in positive.items()},
                                negative={k: tuple(v) for k, v in negative.items()})

    # Полный набор векторов уже посчитан и сохранён (правка через редактор) —
    # подставляем его и не ходим к эмбеддеру заново. Не полный (например, версия
    # из bootstrap: заведена из констант без похода к сети) — сбрасываем кэш, и
    # первый вызов L2 посчитает эталоны как раньше, лениво.
    if all_vectorized:
        embeddings.set_prototype_cache(vectors)
    else:
        embeddings.reset_prototype_cache()


# ── запись ────────────────────────────────────────────────────────────────────

async def save_business_description(db, *, business_description: str, actor: str,
                                     activate: bool) -> ProfileVersion:
    """Новая версия `business_description` — единственное поле профиля, у которого
    версионирование уже было (`ProfileVersion`); эта функция просто даёт ему точку
    записи, которой раньше не существовало."""
    text = business_description.strip()
    if not text:
        raise TaxonomyValidationError("описание бизнеса не может быть пустым")

    existing_versions = (await db.execute(select(ProfileVersion.version))).scalars().all()
    version = _bump_version(existing_versions)

    if activate:
        await db.execute(update(ProfileVersion).where(ProfileVersion.is_active.is_(True))
                         .values(is_active=False))

    row = ProfileVersion(version=version, business_description=text,
                         is_active=activate, created_by=actor)
    db.add(row)
    await db.commit()
    await db.refresh(row)
    logger.info("profile_version_saved version=%s active=%s by=%s", version, activate, actor)
    return row


async def save_taxonomy(db, *, pains: Mapping[str, tuple[Sequence[str], list[str] | None]]
                        | None,
                        disqualifiers: Mapping[str, Sequence[str]] | None,
                        noise_prototypes: Mapping[str, Sequence[str]] | None,
                        actor: str, activate: bool,
                        replace: bool = False) -> CascadeVersion:
    """Новая версия таксономии: якоря L1, дисквалификаторы и эталоны L2 — боли и шум.

    `pains[label] = (anchors, prototypes_or_none)`, `anchors` — как их ввёл человек,
    нормализацию (регистр, «ё») делает эта функция, а не вызывающий код. `prototypes_or_none is None`
    значит «эту боль не трогаем на уровне L2» — переносим её прежние эталонные
    фразы вперёд без изменений и без похода к эмбеддеру; пустой список — осознанное
    «эталонов для этой боли больше нет».

    `replace=True` — переданное считается ПОЛНЫМ набором: боли, шум и
    дисквалификаторы, которых в нём нет, из новой версии исчезают. Нужно для
    загрузки настроек целиком одним файлом: без этого прежние ярлыки остаются
    рядом с новыми, и отбор идёт по смеси двух наборов — состояние, которое
    выглядит рабочим и не является им. По умолчанию False: точечная правка одной
    боли не должна сносить остальные.

    Возвращает новую строку. Активация (или её отсутствие для предложения
    заказчика) решена заранее вызывающим кодом по `Capability.CONFIG_EDIT` /
    `CONFIG_PROPOSE` — здесь только формирование и запись снимка.
    """
    if pains is None and disqualifiers is None and noise_prototypes is None:
        raise TaxonomyValidationError("нечего сохранять: не передано ни pains, "
                                      "ни disqualifiers, ни noise_prototypes")

    current = await active_cascade_version(db)
    prev_rows = (await db.execute(select(L2Prototype).where(
        L2Prototype.cascade_version_id == current.id))).scalars().all() \
        if current is not None else []
    prev_positive: dict[str, list[str]] = {}
    prev_negative: dict[str, list[str]] = {}
    # Ключ по фразе целиком, а не по (kind, label): у одной боли несколько
    # эталонных фраз, и у каждой свой, непохожий на соседей, вектор — хранить один
    # вектор на весь ярлык значило бы подсунуть чужой эмбеддинг всем фразам, кроме
    # последней увиденной.
    prev_vectors: dict[tuple[str, str, str], list[float] | None] = {}
    for r in prev_rows:
        bucket = prev_positive if r.kind == "pos" else prev_negative
        bucket.setdefault(r.label, []).append(r.phrase)
        prev_vectors[(r.kind, r.label, r.phrase)] = r.vector

    if replace:
        # Полная замена: начинаем с пустого, а не с прежнего снимка. Всё, чего нет
        # в переданном наборе, в новую версию не попадает.
        pain_anchors, disq = {}, {}
        positive, negative = {}, {}
    else:
        pain_anchors = {k: list(v)
                        for k, v in (current.pain_anchors if current else {}).items()}
        disq = {k: list(v) for k, v in (current.disqualifiers if current else {}).items()}
        positive = dict(prev_positive)
        negative = dict(prev_negative)
    changed_labels: set[tuple[str, str]] = set()  # (kind, label) с новым текстом фраз

    if pains is not None:
        for label, (anchors, protos) in pains.items():
            pain_anchors[label] = list(
                normalize_words(anchors, field=f"pains.{label}.anchors"))
            if protos is not None:
                normalized = [p.strip() for p in protos if p.strip()]
                positive[label] = normalized
                # «Изменившимся» ярлык считается только если список фраз ДРУГОЙ.
                # Иначе загрузка того же набора целиком гоняла бы эмбеддер по всем
                # фразам заново — минуты GPU за ноль изменений.
                if normalized != prev_positive.get(label):
                    changed_labels.add(("pos", label))

    if disqualifiers is not None:
        for label, markers in disqualifiers.items():
            disq[label] = list(normalize_words(markers, field=f"disqualifiers.{label}"))

    if noise_prototypes is not None:
        for label, phrases in noise_prototypes.items():
            normalized = [p.strip() for p in phrases if p.strip()]
            if not normalized:
                raise TaxonomyValidationError(
                    f"noise_prototypes.{label}: нужна хотя бы одна фраза")
            negative[label] = normalized
            if normalized != prev_negative.get(label):
                changed_labels.add(("neg", label))

    if not pain_anchors:
        raise TaxonomyValidationError("таксономия не может остаться без единой боли")

    version_strs = (await db.execute(select(CascadeVersion.version))).scalars().all()
    version = CascadeVersion(version=_bump_version(version_strs),
                             pain_anchors=pain_anchors, disqualifiers=disq,
                             is_active=activate, created_by=actor)
    db.add(version)
    await db.flush()

    rows_to_embed: list[tuple[str, str, str]] = []  # (kind, label, phrase) без вектора
    new_rows: list[L2Prototype] = []
    for kind, bucket in (("pos", positive), ("neg", negative)):
        for label, phrases in bucket.items():
            changed = (kind, label) in changed_labels
            for phrase in phrases:
                # Не тронутый лейбл переносится с тем вектором, какой уже был —
                # даже если это `None` (эталон не успели посчитать раньше по
                # своей причине, например эмбеддер тогда не был настроен). Это
                # не долг ЭТОЙ правки: заставлять эмбеддер отвечать ради вообще
                # не менявшихся фраз значило бы требовать сеть там, где правки
                # не было. Пересчёта требует только то, что реально изменилось.
                vector = None if changed else prev_vectors.get((kind, label, phrase))
                row = L2Prototype(cascade_version_id=version.id, kind=kind, label=label,
                                  phrase=phrase, vector=vector)
                new_rows.append(row)
                if changed:
                    rows_to_embed.append((kind, label, phrase))
    db.add_all(new_rows)

    if rows_to_embed:
        if not embeddings.enabled():
            await db.rollback()
            raise TaxonomyValidationError(
                "эмбеддер недоступен (RADAR_EMBED_BASE_URL не задан) — новые "
                "эталонные фразы не будут участвовать в L2, пока не пересчитаны; "
                "сохранение отменено, а не принято наполовину")
        try:
            vectors = await embeddings.embed([p for _, _, p in rows_to_embed])
        except embeddings.EmbeddingsUnavailable as e:
            await db.rollback()
            raise TaxonomyValidationError(f"эмбеддер не ответил: {e}") from e
        by_key: dict[tuple[str, str, str], list[float]] = {}
        for (kind, label, phrase), vec in zip(rows_to_embed, vectors):
            by_key[(kind, label, phrase)] = vec
        for row in new_rows:
            if row.vector is None:
                row.vector = by_key.get((row.kind, row.label, row.phrase))

    if activate:
        await db.execute(update(CascadeVersion).where(CascadeVersion.id != version.id,
                                                       CascadeVersion.is_active.is_(True))
                         .values(is_active=False))

    await db.commit()
    await db.refresh(version)
    logger.info("cascade_version_saved version=%s active=%s by=%s embedded=%s",
               version.version, activate, actor, len(rows_to_embed))
    return version


async def save_l3_prompt(db, *, prompt_key: str, system_prompt: str, actor: str,
                         activate: bool) -> L3Prompt:
    """Новая версия системного промпта L3 одного контура. Версия обязана
    подняться на любую правку текста — даже правку опечатки: `llm_traces` уже
    хранит `prompt_version` по каждому вердикту, и подменить текст версии на
    месте значило бы задним числом переписать, каким вопросом эти вердикты
    получены."""
    text = system_prompt.strip()
    if not text:
        raise TaxonomyValidationError("системный промпт не может быть пустым")
    if prompt_key not in llm.PROMPTS:
        raise TaxonomyValidationError(
            f"промпт «{prompt_key}» неизвестен; известны: {', '.join(sorted(llm.PROMPTS))}")

    current = await active_l3_prompt(db, prompt_key)
    next_version = _bump_prompt_version(current.version if current else "", prompt_key=prompt_key)

    if activate:
        await db.execute(update(L3Prompt).where(L3Prompt.prompt_key == prompt_key,
                                                 L3Prompt.is_active.is_(True))
                         .values(is_active=False))

    row = L3Prompt(prompt_key=prompt_key, version=next_version, system_prompt=text,
                   is_active=activate, created_by=actor)
    db.add(row)
    await db.commit()
    await db.refresh(row)
    logger.info("l3_prompt_saved key=%s version=%s active=%s by=%s",
               prompt_key, next_version, activate, actor)
    return row


async def activate_cascade_version(db, version_id: int) -> CascadeVersion:
    version = (await db.execute(select(CascadeVersion)
                                .where(CascadeVersion.id == version_id))
              ).scalar_one_or_none()
    if version is None:
        raise TaxonomyValidationError(f"версия таксономии #{version_id} не найдена")
    await db.execute(update(CascadeVersion).where(CascadeVersion.id != version.id,
                                                   CascadeVersion.is_active.is_(True))
                     .values(is_active=False))
    version.is_active = True
    await db.commit()
    await db.refresh(version)
    return version


async def activate_l3_prompt(db, prompt_id: int) -> L3Prompt:
    row = (await db.execute(select(L3Prompt).where(L3Prompt.id == prompt_id))
          ).scalar_one_or_none()
    if row is None:
        raise TaxonomyValidationError(f"версия промпта #{prompt_id} не найдена")
    await db.execute(update(L3Prompt).where(L3Prompt.prompt_key == row.prompt_key,
                                            L3Prompt.id != row.id,
                                            L3Prompt.is_active.is_(True))
                     .values(is_active=False))
    row.is_active = True
    await db.commit()
    await db.refresh(row)
    return row


# ── перечитка без перезапуска ────────────────────────────────────────────────────

# API, воркер приёма и воркер прогонов — три разных процесса с тремя разными
# копиями module-level словарей каскада. Правка через API меняет только его
# собственную копию; `events.py` уже решает ровно эту же задачу для интерфейса тем
# же приёмом («опрос базы не имеет ни одной из бед публикации из места изменения»,
# см. докстринг модуля) — здесь тот же приём для правил, а не для чисел на экране.
_WATCH_INTERVAL = 30.0
_watch_task: asyncio.Task | None = None


async def _watch_loop(interval: float) -> None:
    while True:
        await asyncio.sleep(interval)
        try:
            async with get_session_maker()() as db:
                await reload(db)
        except Exception:  # noqa: BLE001 — сбой опроса не должен ронять процесс
            logger.exception("cascade_registry_watch_failed")


def start_watch(interval: float = _WATCH_INTERVAL) -> None:
    """Запустить фоновую перечитку. Без аргумента — раз в 30 секунд: активная
    правка ждёт этого дольше, чем взгляд на экран, но короче, чем воркер прогонов
    успевает уйти в реклассификацию на старых правилах."""
    global _watch_task
    if _watch_task is None:
        _watch_task = asyncio.create_task(_watch_loop(interval))


async def stop_watch() -> None:
    global _watch_task
    if _watch_task is not None:
        _watch_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await _watch_task
        _watch_task = None


async def activate_profile_version(db, version_id: int) -> ProfileVersion:
    row = (await db.execute(select(ProfileVersion).where(ProfileVersion.id == version_id))
          ).scalar_one_or_none()
    if row is None:
        raise TaxonomyValidationError(f"версия профиля #{version_id} не найдена")
    await db.execute(update(ProfileVersion).where(ProfileVersion.id != row.id,
                                                   ProfileVersion.is_active.is_(True))
                     .values(is_active=False))
    row.is_active = True
    await db.commit()
    await db.refresh(row)
    return row
