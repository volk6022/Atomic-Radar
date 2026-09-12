"""Комментарии-отзывы к черновикам — общий код обоих контуров.

Черновики живут в двух таблицах (`drafts` и `wf_drafts`), а отзывы о них — в одной:
`draft_comments` с полиморфной ссылкой (contour, draft_id). И раз таблица одна, код
тоже один: правила «какой текст принимается, какая версия промпта снимается, кто
может удалить» обязаны быть одинаковыми в обоих контурах, потому что экран формы
один. Размножить эти проверки по ручкам значило бы однажды получить 422 в одном
контуре и молчаливую запись в другом — на глаз неотличимо от «комментарий принят».

Коммит здесь не делается: вместе с комментарием пишется запись журнала действий,
и обе строки обязаны лечь одной транзакцией, которую открывает ручка.
"""
from __future__ import annotations

from fastapi import HTTPException, status
from sqlalchemy import func, select

from app.core.access import Role
from app.db.models import AuditLog, DraftComment

# Ограничение формы, а не базы: колонка TEXT длиннее. Развёрнутый отзыв о том,
# как звучит черновик, — пара абзацев; всё, что заметно длиннее, Иван читать
# не станет, а нечитаемый отзыв — это место, где заказчик считает, что его
# услышали, хотя править промпт по нему никто не будет.
MAX_LENGTH = 4000


def as_dict(c: DraftComment) -> dict:
    """Форма комментария, одна на всё: карточка черновика и общая лента."""
    return {
        "id": c.id, "contour": c.contour, "draft_id": c.draft_id,
        "variant_index": c.variant_index, "prompt_version": c.prompt_version,
        "author": c.author_email, "text": c.text,
        "created_at": c.created_at.isoformat() if c.created_at else None,
    }


async def list_for(db, contour: str, draft_id: int) -> list[dict]:
    """Комментарии черновика, старые раньше: карточка читает их как переписку."""
    rows = (await db.execute(
        select(DraftComment)
        .where(DraftComment.contour == contour, DraftComment.draft_id == draft_id)
        .order_by(DraftComment.created_at, DraftComment.id))).scalars().all()
    return [as_dict(c) for c in rows]


async def counts_for(db, contour: str, draft_ids: list[int]) -> dict[int, int]:
    """Счётчик комментариев для строк списка — один GROUP BY на страницу.

    Запрос на строку здесь уже случался в соседних экранах и стоил полутора сотен
    обращений к базе; на пустой странице возвращается пустой словарь, а не поход
    в базу ни за чем.
    """
    if not draft_ids:
        return {}
    counts = (await db.execute(
        select(DraftComment.draft_id, func.count(DraftComment.id))
        .where(DraftComment.contour == contour,
               DraftComment.draft_id.in_(draft_ids))
        .group_by(DraftComment.draft_id))).all()
    return dict(counts)


async def add(db, *, contour: str, draft_id: int, variants: list,
              draft_prompt_version, user, text: str,
              variant_index: int | None) -> DraftComment:
    """Новый отзыв. Коммит остаётся ручке — см. докстринг модуля.

    Снимок версии промпта делает сервер, а не клиент: отзыв существует ради
    правки промпта, и «какая это была версия» — вопрос к данным на момент
    отзыва, а не к тому, что браузер вспомнил. Про конкретный вариант — его
    собственная версия, черновик целиком — версия черновика.

    Проверки текста и индекса дублируют pydantic-схему ручки не случайно: сервис
    один на две ручки, и схемы могут разъехаться, а вот эта точка — одна.
    """
    body = (text or "").strip()
    if not body:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            "пустой текст комментария")
    if len(body) > MAX_LENGTH:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            f"текст комментария длиннее {MAX_LENGTH} знаков")
    if variant_index is not None and not 0 <= variant_index < len(variants):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            f"вариант {variant_index} не существует "
                            f"(их {len(variants)})")

    if variant_index is not None:
        prompt_version = variants[variant_index].get("prompt_version")
    else:
        prompt_version = draft_prompt_version

    comment = DraftComment(contour=contour, draft_id=draft_id,
                           variant_index=variant_index, prompt_version=prompt_version,
                           author_email=user.email, text=body)
    db.add(comment)
    # id комментария рождается в базе, а журнал без него слеп: запись «оставлен
    # отзыв» без ссылки на отзыв не отвечает на вопрос «какой именно».
    await db.flush()
    db.add(AuditLog(
        user_id=user.id, user_email=user.email, action="draft_comment",
        detail={"draft_id": draft_id, "contour": contour,
                "comment_id": comment.id, "variant_index": variant_index}))
    return comment


async def delete(db, *, contour: str, draft_id: int, comment_id: int, user) -> None:
    """Удалить отзыв: свой — любому, кому открыт экран; чужой — только владельцу.

    Отзыв — мнение о тексте, наружу от него ничего не уходит, поэтому право
    мягче, чем у одобрения черновика: отдельного разрешения не спрашивается.
    Но стирать чужое мнение значило бы переписывать историю того, что человек
    видел и думал, — это оставлено владельцу, как и все необратимые жесты.
    """
    comment = (await db.execute(
        select(DraftComment).where(DraftComment.id == comment_id,
                                   DraftComment.contour == contour,
                                   DraftComment.draft_id == draft_id)
    )).scalar_one_or_none()
    if comment is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND,
                            f"комментарий {comment_id} у черновика {draft_id} "
                            f"не найден")
    if comment.author_email != user.email and user.role != Role.OWNER:
        raise HTTPException(status.HTTP_403_FORBIDDEN,
                            "чужой комментарий может удалить только владелец")
    await db.delete(comment)
    db.add(AuditLog(
        user_id=user.id, user_email=user.email, action="draft_comment_delete",
        detail={"draft_id": draft_id, "contour": contour, "comment_id": comment_id}))
