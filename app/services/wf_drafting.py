"""Черновики действий по целям сценариев (`wf_targets` → `wf_drafts`).

Второй конвейер копил цели и не заводил ни одной заготовки: `drafting.ensure_draft`
умеет ровно одну форму — `Draft` от `Lead`, то есть «что написать вот этому человеку
в личку». Раздел «Черновики» у публичного сценария оставался пуст, и пустым он
выглядел бы как «целей нет», а не как «генератора нет».

**Комплект заготовок выбирается по оси `action`, а не по ключу сценария.** Это тот же
принцип, на котором в спецификации построены меню и экраны: новый сценарий не должен
требовать ветки в коде. Что мы пишем, зависит от того, что мы делаем, — и ровно от
этого, а не от того, как сценарий назвали.

Главное содержательное отличие — публичный ответ. В личке рекомендация знакомого
уместна по определению: человека выбрали именно потому, что ему есть что предложить.
Под чужим вопросом та же фраза читается как реклама, её удаляют, а автора запоминают.
Ровно об этом спрашивает `llm.PUBLIC_SYSTEM` (`answerable_briefly`, `already_answered`),
и было бы странно отбирать цели по «можно ли ответить по существу», а потом
подсовывать оператору заготовку «напишите Андрею».

Поэтому публичные заготовки — это короткие ответы по делу, а контакт в них появляется
только там, где его спросили. Решение №8 говорит прямо: **Андрей всё равно переписывает
руками**, и заготовка честно называет себя заходом, а не готовым ответом.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping

from sqlalchemy import select

from app.db.models import Channel, Message, WfDraft, WfTarget, Workflow
from app.services import drafting

logger = logging.getLogger(__name__)

# Контакт, ради которого всё и делается. Вынесен в константу не для удобства правки, а
# потому что политика ниже спрашивает «упомянут ли он», и искать подстроку по трём
# файлам значило бы завести три места, где эта проверка может разойтись.
CONTACT = "@vertsanov_biz"

# Боль, при которой прямая рекомендация уместна даже публично: человек сам спрашивает,
# кого позвать. Отвечать ему «смотря что нужно» и умалчивать контакт — это не
# осторожность, а бесполезность.
ASKS_FOR_CONTRACTOR = "нужен админ/подрядчик"


# ── публичные заготовки ───────────────────────────────────────────────────────
#
# Пишутся иначе, чем для ЛС, и иначе по существу, а не по тону. Публичный ответ
# обязан быть полезен сам по себе: его читают не только автор вопроса, но и все
# остальные, и именно по нему решают, кто мы такие.
#
# Отсюда форма: первый шаг диагностики или конкретная развилка, по которой спрашивающий
# может двинуться сам. Это не готовый ответ — угадать чужую конфигурацию шаблон не
# может, — но это и не пустая любезность.
PUBLIC_TEMPLATES: Mapping[str, tuple[str, ...]] = MappingProxyType({
    "хостинг тормозит/дорог": (
        "Если тормозит именно отдача, стоит сначала понять, где узкое место: сервер "
        "или сеть до него. TTFB по одному и тому же адресу с двух разных точек обычно "
        "сразу показывает, на чьей стороне беда.",
        "По цене чаще всего переплачивают за то, что не используется. Если нагрузка "
        "стабильно низкая и память не выбирается — тариф режется без потерь, это "
        "видно по недельному графику.",
        "Смотря что именно медленно: генерация страницы, отдача статики или база. "
        "Это три разных диагноза, и лечатся они по-разному.",
    ),
    "VPN не работает": (
        "Если рвётся периодически, а не сразу — это чаще блокировка по протоколу, чем "
        "сервер. Быстрая проверка: поднять то же соединение на 443 через TLS. Держится "
        "там, где раньше рвалось, — вопрос закрыт.",
        "Стоит уточнить, что именно отваливается: не проходит handshake или соединение "
        "встаёт через несколько минут работы. Это два разных диагноза.",
        "Со стороны клиента проверяется за минуту: то же самое с мобильного интернета. "
        "Если там стабильно, дело не в конфиге, а в провайдере.",
    ),
    "не может настроить сам": (
        "Очень часто дело в том, что сервис слушает localhost вместо 0.0.0.0 — снаружи "
        "тогда всё выглядит как закрытый порт. `ss -tlnp` покажет, на каком адресе он "
        "реально висит.",
        "Firewall стоит проверять отдельно от приложения: порт бывает открыт в ufw и "
        "при этом закрыт у провайдера, и признаки у этого одинаковые.",
        "Если в логах есть ошибка, но непонятная — приведите её текстом. По ней обычно "
        "видно сразу, а без неё это гадание.",
    ),
    # Единственная боль, где контакт уместен публично: его и спросили.
    ASKS_FOR_CONTRACTOR: (
        "Смотря что нужно: разовая настройка или сопровождение. Под разовую задачу "
        "обычно дешевле человек под задачу, под постоянную — кто-то на part-time.",
        "Стоит описать стек и что должно работать в итоге — от этого сильно зависит, "
        "кого искать и сколько это стоит.",
        f"Могу порекомендовать: Андрей ({CONTACT}), делал мне инфраструктуру — "
        "и сделал, и объяснил, что где лежит.",
    ),
})

PUBLIC_FALLBACK = (
    "Опишите чуть подробнее, что уже пробовали и на чём остановилось — по такой "
    "вводной обычно видно, куда копать.",
)

PUBLIC_CRITIC_NOTE = (
    "Проверки моделью не было: это заход по шаблону "
    "(prompt_version=template-public-v0), а не готовый ответ. Публичный ответ обязан "
    "быть полезен сам по себе — перепишите под конкретный вопрос перед отправкой."
)

# ── реакции ───────────────────────────────────────────────────────────────────
#
# У реакции нет текста: в `final_text` едет выбранное эмодзи. Отдельной сущности под
# них не заводили намеренно (см. `WfDraft`), и отдельного комплекта правил тоже не
# нужно — выбор из короткого списка это и есть весь черновик.
REACTION_VARIANTS = ("👍", "🔥", "❤️", "🤔", "👀")

REACTION_CRITIC_NOTE = (
    "Реакция выбирается из списка, модель в этом не участвует. Смотрите, подходит ли "
    "она тону ветки: неуместная реакция заметнее неуместного текста."
)


@dataclass(frozen=True)
class DraftKit:
    """Комплект заготовок под одно действие сценария.

    `public` — не косметика, а признак, по которому работает политика контакта:
    в личке упоминание Андрея и есть смысл сообщения, публично оно же — тот самый
    способ превратить полезный ответ в рекламу.
    """

    key: str
    version: str
    templates: Mapping[str, tuple[str, ...]]
    fallback: tuple[str, ...]
    critic_note: str
    public: bool
    # Текстовый черновик или выбор реакции. У реакций нет ни боли, ни спам-оценки.
    kind: str = "text"


# Комплект ЛС переиспользует заготовки `drafting` дословно, а не копирует их.
#
# Причина та же, по которой старые колонки каскада делят промпт со сценарием ЛС:
# пока экраны не переехали на новые таблицы, `drafts` остаётся страховкой на откат,
# и `wf_drafts` контура ЛС обязан быть его точной тенью. Своя копия текстов разошлась
# бы с оригиналом на первой же правке — и разошлась бы молча.
DM_KIT = DraftKit(
    key="dm", version=drafting.PROMPT_VERSION,
    templates=drafting.TEMPLATES, fallback=drafting.FALLBACK,
    critic_note=drafting.CRITIC_NOTE, public=False)

PUBLIC_KIT = DraftKit(
    key="public_reply", version="template-public-v0",
    templates=PUBLIC_TEMPLATES, fallback=PUBLIC_FALLBACK,
    critic_note=PUBLIC_CRITIC_NOTE, public=True)

REACTION_KIT = DraftKit(
    key="reaction", version="template-react-v0",
    templates=MappingProxyType({}), fallback=REACTION_VARIANTS,
    critic_note=REACTION_CRITIC_NOTE, public=True, kind="reaction")

KITS: Mapping[str, DraftKit] = MappingProxyType({
    "dm": DM_KIT, "reply": PUBLIC_KIT, "react": REACTION_KIT})


class UnknownActionError(ValueError):
    """У сценария действие, под которое заготовок в коде нет."""

    def __init__(self, action: str) -> None:
        super().__init__(f"действие «{action}» — заготовок под него нет; известны: "
                         f"{', '.join(sorted(KITS))}")
        self.action = action


def kit(action: str) -> DraftKit:
    """Комплект заготовок по действию сценария.

    Падаем, а не подставляем комплект ЛС: сценарий с чужими заготовками писал бы
    в публичную ветку текст, рассчитанный на личку, и заметили бы это по удалённым
    сообщениям, а не по ошибке при запуске.
    """
    try:
        return KITS[action]
    except KeyError:
        raise UnknownActionError(action) from None


def lint(text: str, pain: str | None, chosen: DraftKit) -> tuple[bool, str | None]:
    """Проверка политики по тексту. Возвращает (прошло, чем не понравилось).

    Это настоящая проверка, в отличие от «критика»: она смотрит на текст, а не
    изображает мнение модели, которой здесь нет.

    Публичный случай отличается одним правилом, и оно содержательное: **упоминание
    контакта уместно только там, где о нём спросили.** Под вопросом «как починить»
    рекомендация подрядчика — реклама, даже если она правдива; под вопросом «кого
    позвать» умолчать о нём — бесполезность. Отличает их боль из каскада.
    """
    # Порядок проверок — от точной причины к общей, и это не косметика. Ссылка сама
    # по себе добавляет 0.30 к спам-оценке, а база — 0.10; то есть любой текст со
    # ссылкой набирает ровно порог и был бы отклонён общим правилом. Оператор при этом
    # прочитал бы «похоже на рекламу» там, где на самом деле сработало конкретное
    # «ссылка в первом сообщении», и пошёл бы искать в тексте несуществующий капс.
    if "t.me/" in text.lower() or "http" in text.lower():
        return False, "ссылка в первом сообщении"
    if chosen.public and CONTACT in text and pain != ASKS_FOR_CONTRACTOR:
        return False, ("контакт назван публично там, где о нём не спрашивали — "
                       "это читается как реклама")
    spam = drafting.spam_score(text)
    if spam >= 0.4:
        return False, f"похоже на рекламу (оценка {spam})"
    return True, None


def build_variants(pain: str | None, *, chosen: DraftKit) -> list[dict]:
    """Варианты черновика под одну цель.

    Форма записи та же, что у `drafting.build_variants`, плюс `lint_note`: экран
    оператора должен показывать не только «вариант не прошёл», но и чем именно, иначе
    единственный способ понять причину — читать код.
    """
    if chosen.kind == "reaction":
        # У реакции нет ни боли, ни текста для разбора: список один на все случаи.
        return [{"text": e, "spam_score": 0.0, "prompt_version": chosen.version,
                 "lint_ok": True, "lint_note": None,
                 "critic_passed": None, "critic_text": chosen.critic_note}
                for e in chosen.fallback]

    texts = chosen.templates.get(pain or "", chosen.fallback)
    out = []
    for text in texts:
        ok, note = lint(text, pain, chosen)
        out.append({
            "text": text, "spam_score": drafting.spam_score(text),
            "prompt_version": chosen.version,
            "lint_ok": ok, "lint_note": note,
            "critic_passed": None, "critic_text": chosen.critic_note,
        })
    return out


async def ensure_wf_draft(db, workflow: Workflow, target: WfTarget) -> WfDraft:
    """Черновик по цели — ровно один, заводится при первом обращении к очереди.

    Лениво, как и у лидов: генератор шаблонный и стоит микросекунды, а фоновый воркер
    ради него был бы лишним местом, где что-то молча не запустится.
    """
    draft = (await db.execute(
        select(WfDraft).where(WfDraft.target_id == target.id))).scalar_one_or_none()
    if draft is not None:
        return draft

    message = (await db.execute(
        select(Message).where(Message.id == target.message_id))).scalar_one()
    channel = (await db.execute(
        select(Channel).where(Channel.id == target.channel_id))).scalar_one()

    chosen = kit(workflow.action)
    draft = WfDraft(
        workflow_id=workflow.id, target_id=target.id,
        variants=build_variants(target.pain, chosen=chosen),
        thread_context=await drafting.thread_context(db, message),
        state="pending", prompt_version=chosen.version,
        source_message_link=drafting.message_link(channel, message),
    )
    db.add(draft)
    await db.flush()
    logger.info("wf_draft_created draft=%s target=%s workflow=%s action=%s pain=%s",
                draft.id, target.id, workflow.key, workflow.action, target.pain)
    return draft


async def ensure_queue(db, workflow: Workflow) -> int:
    """Завести черновики всем целям сценария, у которых их ещё нет.

    Возвращает, сколько завели. Цели переводятся в `in_review` тем же движением, что
    и лиды: заготовка есть — значит цель дошла до человека, и повторный проход не
    должен считать её новой.

    **Не коммитит.** Заведение заготовок и пометка целей — одно изменение, и решать,
    где кончается транзакция, должен вызывающий: у ручки чтения это конец запроса, а
    у будущего воркера — его собственный шаг. Коммит внутри означал бы, что половина
    работы уже записана, когда следующая строка упала.
    """
    chosen = kit(workflow.action)  # до первой записи: чужое действие — падение на старте
    logger.debug("wf_queue workflow=%s kit=%s", workflow.key, chosen.key)

    targets = (await db.execute(
        select(WfTarget)
        .where(WfTarget.workflow_id == workflow.id,
               WfTarget.status.in_(("new", "in_review")),
               WfTarget.id.notin_(select(WfDraft.target_id)))
        .order_by(WfTarget.score.desc(), WfTarget.id))).scalars().all()

    for target in targets:
        await ensure_wf_draft(db, workflow, target)
        target.status = "in_review"
    return len(targets)
