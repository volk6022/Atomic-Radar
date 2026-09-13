"""`scripts/sync_draft_decisions.py` — перенос решений из `drafts` в `wf_drafts` на настоящем Postgres.

Почему не на моках. Скрипт правит боевые данные, отката одной командой у Radar нет —
половина риска в том, что он действительно переписывает ровно три пары полей у
черновика и ровно то, что ставят ручки `approve_draft`/`reject_draft` на цели, и что
сухой прогон в самом деле ничего не пишет. Подделками это не проверить.

Три посева, по одному на исход из задачи:

1. лид + старый черновик `approved` (с `final_text`) + цель + `wf_draft pending`;
2. то же со старым `rejected` и причиной (на проде их 144, все «Не та боль»);
3. конфликт: `wf_draft` уже `rejected` — в новом экране решили без переноса.

Сухой прогон ничего не меняет; `--apply` переносит первые два, третий не тронут,
`wf_targets.status` совпадает с тем, что ставят ручки; повторный `--apply` — без
изменений; в журнале ровно две записи. Отдельные проверки у комментариев (копия
один раз, с новым `draft_id`) и у осиротевшего черновика (сосчитан, не тронут).

Тест конфликта написан так, чтобы его покрасила мутация скрипта «перезаписывать и
не-pending»: проверяется каждое поле строки, а не только счётчик.

Все чтения и запуски скрипта — каждое со своим короткоживущим движком и своим
циклом (`asyncio.run`), как в `test_wf_decisions_db.py`: проверяется то, что
действительно легло в таблицу, а не состояние объектов в чужой сессии.

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db.models import (AuditLog, Base, Channel, Draft, DraftComment, EngageInstance,
                           Lead, Message, WfDraft, WfTarget, Workflow)
from scripts import sync_draft_decisions as sdd

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

T0 = datetime(2026, 9, 10, 9, 0, tzinfo=timezone.utc)
DECIDER = "andrey@vertsanov.ru"      # кто разбирал очередь в СТАРОМ экране
NEW_DECIDER = "ivan@local"           # кто решил конфликтный черновик в НОВОМ экране
REASON = "Не та боль"
WF_REASON = "Звучит как реклама"     # причина из нового экрана — её перезаписывать нельзя
FINAL = "Здравствуйте! Похоже, дело в валютном контроле — подскажу, как провести платёж."
COMMENT_TEXT = "Второй вариант точнее называет боль"
VARIANTS = [{"text": "Здравствуйте! Судя по описанию, дело в валютном контроле.",
             "kind": "template"},
            {"text": "Могу подсказать, как провести платёж за рубеж.",
             "kind": "template"}]


async def _seed() -> dict:
    """Установка «после миграции, до синхронизации»: `cold_dm` есть, цели перенесены,
    черновики сценария все `pending`, решения остались в старом контуре.

    Статусы целей — `in_review`, как у лидов с черновиком в очереди на момент
    миграции; у конфликтного — то, что оставила ручка нового экрана.
    """
    engine = create_async_engine(DB_URL, poolclass=None)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)

    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        instance = EngageInstance(key="default", client_label="Основной",
                                  base_url="http://engage:8103",
                                  api_key_env="RADAR_ENGAGE_API_KEY")
        db.add(instance)
        await db.flush()
        dm = Workflow(key="cold_dm", title="Личные сообщения", target_kind="user",
                      action="dm", visibility="private", engage_instance_id=instance.id,
                      engage_use_case="cold_dm", cascade_profile="dm_v1", sort_order=10,
                      is_active=True)
        channel = Channel(peer_id=-1001, username="chat", title="Обсуждение")
        db.add_all([dm, channel])
        await db.flush()

        ids: dict = {"workflow": dm.id, "old": {}, "wf": {}, "target": {}, "src": {},
                     "wf_decided_at": {}}

        async def case(tag, *, old_state, old_reason=None, chosen=None, final=None,
                       with_target=True, wf_state="pending", wf_decided_by=None,
                       wf_reason=None, target_status="in_review",
                       target_reason=None):
            n = len(ids["old"])
            message = Message(channel_id=channel.id, tg_message_id=1000 + n,
                              tg_date=T0, author_peer_id=500 + n,
                              author_username=f"user{n}", author_name=f"Имя {n}",
                              author_is_bot=False, is_automatic_forward=False,
                              text="платёж за рубеж не проходит, ищу через кого оплатить",
                              processed_at=T0)
            db.add(message)
            await db.flush()
            lead = Lead(message_id=message.id, channel_id=channel.id,
                        author_peer_id=500 + n, author_username=f"user{n}",
                        author_name=f"Имя {n}", pain="не может оплатить за рубеж",
                        quote=message.text, score=60, score_breakdown=[],
                        disqualifiers=[], status="in_review")
            db.add(lead)
            await db.flush()
            decided_at = T0 + timedelta(days=n + 1)
            draft = Draft(lead_id=lead.id, variants=VARIANTS, thread_context=[],
                          chosen_variant=chosen, final_text=final, state=old_state,
                          reject_reason=old_reason, decided_by=DECIDER,
                          decided_at=decided_at, prompt_version="template-v0")
            db.add(draft)
            await db.flush()
            ids["old"][tag] = draft.id
            ids["src"][tag] = decided_at
            if with_target:
                target = WfTarget(workflow_id=dm.id, target_kind="user",
                                  message_id=message.id, channel_id=channel.id,
                                  recipient_peer_id=500 + n, author_peer_id=500 + n,
                                  author_username=f"user{n}", author_name=f"Имя {n}",
                                  pain="не может оплатить за рубеж",
                                  quote=message.text, score=60, score_breakdown=[],
                                  disqualifiers=[], status=target_status,
                                  reject_reason=target_reason)
                db.add(target)
                await db.flush()
                wf_at = T0 + timedelta(days=40) if wf_decided_by else None
                wd = WfDraft(workflow_id=dm.id, target_id=target.id, variants=VARIANTS,
                             thread_context=[], state=wf_state, reject_reason=wf_reason,
                             decided_by=wf_decided_by, decided_at=wf_at,
                             prompt_version="template-v0",
                             source_message_link="https://t.me/chat/1000")
                db.add(wd)
                await db.flush()
                ids["target"][tag] = target.id
                ids["wf"][tag] = wd.id
                if wf_at is not None:
                    ids["wf_decided_at"][tag] = wf_at

        # 1. Одобрен в старом экране ПОСЛЕ переноса: 5 из 7 таких на проде несут текст.
        await case("approved", old_state="approved", chosen=0, final=FINAL)
        # 2. Отклонён с типизированной причиной — как 144 из 151 на проде.
        await case("rejected", old_state="rejected", old_reason=REASON)
        # 3. Конфликт: в новом экране уже решили, и иначе, чем в старом, — чтобы
        #    любая перезапись (причины, автора) была заметна.
        await case("conflict", old_state="rejected", old_reason=REASON,
                   wf_state="rejected", wf_decided_by=NEW_DECIDER, wf_reason=WF_REASON,
                   target_status="rejected", target_reason=WF_REASON)
        # 4. Осиротевший: решение есть, сопоставить с новым контуром нечего.
        await case("orphan", old_state="rejected", old_reason=REASON,
                   with_target=False)

        db.add(DraftComment(contour="lead", draft_id=ids["old"]["approved"],
                            variant_index=None, prompt_version="template-v0",
                            author_email=DECIDER, text=COMMENT_TEXT,
                            created_at=T0 + timedelta(hours=1)))
        ids["comment_at"] = T0 + timedelta(hours=1)
        await db.commit()

    await engine.dispose()
    return ids


@pytest.fixture
def seeded():
    """Посев в собственном цикле событий, полностью закрытый за собой."""
    return asyncio.run(_seed())


def _in_session(work):
    """Запустить работу со своей сессией и своим циклом: соединение asyncpg
    привязано к тому циклу, где создано, — жить дольше `asyncio.run` оно не может."""
    async def go():
        engine = create_async_engine(DB_URL, poolclass=None)
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with maker() as db:
            out = await work(db)
        await engine.dispose()
        return out
    return asyncio.run(go())


def _run(seeded, *, apply: bool):
    """Один прогон скрипта — как его сделала бы `main()`, только сессию даёт тест."""

    async def work(db):
        return await sdd.sync_decisions(db, apply=apply)

    return _in_session(work)


def _read(query, *, scalars: bool = False):
    """Прочитать строки: `scalars=True` — для целых сущностей, иначе — для наборов
    колонок (Row с несколькими элементами развертывать скалярами нельзя)."""
    async def work(db):
        result = await db.execute(query)
        return (result.scalars() if scalars else result).all()

    return _in_session(work)


def _wf_row(seeded, tag):
    return _read(select(WfDraft).where(WfDraft.id == seeded["wf"][tag]),
                 scalars=True)[0]


def _target_row(seeded, tag):
    return _read(select(WfTarget).where(WfTarget.id == seeded["target"][tag]),
                 scalars=True)[0]


def _audit():
    return _read(select(AuditLog).where(AuditLog.action == sdd.AUDIT_ACTION),
                 scalars=True)


def _wf_comments(draft_id):
    return _read(select(DraftComment).where(DraftComment.contour == "wf",
                                            DraftComment.draft_id == draft_id),
                 scalars=True)


def _snapshot(seeded):
    """Полный снимок переносимого: строки сценариев, цели, отзывы, журнал."""
    wf = _read(select(WfDraft.id, WfDraft.state, WfDraft.chosen_variant,
                      WfDraft.final_text, WfDraft.reject_reason, WfDraft.decided_by,
                      WfDraft.decided_at)
               .where(WfDraft.workflow_id == seeded["workflow"]))
    targets = _read(select(WfTarget.id, WfTarget.status, WfTarget.reject_reason)
                    .where(WfTarget.workflow_id == seeded["workflow"]))
    comments = _read(select(DraftComment.contour, DraftComment.draft_id,
                            DraftComment.author_email, DraftComment.text,
                            DraftComment.created_at))
    audit = _read(select(AuditLog.id, AuditLog.user_id, AuditLog.user_email,
                         AuditLog.detail))
    return (sorted(wf), sorted(targets), sorted(comments), sorted(audit))


# ── сухой прогон ─────────────────────────────────────────────────────────────────

def test_dry_run_counts_but_writes_nothing(seeded):
    s = _run(seeded, apply=False)

    assert (s.candidates, s.approved, s.rejected) == (3, 1, 1)
    assert s.already_synced == 0
    assert s.conflicts == [seeded["old"]["conflict"]]
    assert s.orphaned == [seeded["old"]["orphan"]]
    assert (s.comments_copied, s.comments_skipped) == (1, 0)

    # База в точности исходная: оба переноса всё ещё в очереди, аудит пуст,
    # отзыва в новом контуре нет.
    for tag in ("approved", "rejected"):
        wd = _wf_row(seeded, tag)
        assert (wd.state, wd.chosen_variant, wd.final_text, wd.reject_reason,
                wd.decided_by, wd.decided_at) == ("pending", None, None, None,
                                                  None, None)
        assert _target_row(seeded, tag).status == "in_review"
    assert _audit() == []
    assert _wf_comments(seeded["wf"]["approved"]) == []


# ── перенос одобрения ────────────────────────────────────────────────────────────

def test_apply_transfers_the_approval(seeded):
    s = _run(seeded, apply=True)
    assert (s.candidates, s.approved, s.rejected) == (3, 1, 1)

    wd = _wf_row(seeded, "approved")
    assert wd.state == "approved"
    assert wd.chosen_variant == 0
    assert wd.final_text == FINAL
    assert wd.decided_by == DECIDER
    assert wd.decided_at == seeded["src"]["approved"]
    # Ровно `approve_draft` (wf_queues.py:1168): у цели меняется только статус,
    # `reject_reason` ручка одобрения не трогает.
    target = _target_row(seeded, "approved")
    assert target.status == "approved"
    assert target.reject_reason is None


# ── перенос отказа ───────────────────────────────────────────────────────────────

def test_apply_transfers_the_rejection_with_reason(seeded):
    s = _run(seeded, apply=True)

    wd = _wf_row(seeded, "rejected")
    assert wd.state == "rejected"
    assert wd.reject_reason == REASON
    assert wd.decided_by == DECIDER
    assert wd.decided_at == seeded["src"]["rejected"]
    assert wd.chosen_variant is None and wd.final_text is None
    # Ровно `reject_draft` (wf_queues.py:1254-1255): статус И причина на цели.
    target = _target_row(seeded, "rejected")
    assert target.status == "rejected"
    assert target.reject_reason == REASON


# ── конфликт ─────────────────────────────────────────────────────────────────────

def test_conflicted_wfdraft_is_never_rewritten(seeded):
    """Решение, принятое в новом экране, новее переносимого — трогать его нельзя.

    Проверяется каждое поле, а не только счётчик: мутация скрипта «перезаписывать
    и не-pending» обязана упасть именно здесь, пусть она меняет что-то одно.
    """
    s = _run(seeded, apply=True)
    assert s.conflicts == [seeded["old"]["conflict"]]

    wd = _wf_row(seeded, "conflict")
    assert wd.state == "rejected"
    assert wd.reject_reason == WF_REASON
    assert wd.decided_by == NEW_DECIDER
    assert wd.decided_at == seeded["wf_decided_at"]["conflict"]
    assert wd.chosen_variant is None and wd.final_text is None
    target = _target_row(seeded, "conflict")
    assert target.status == "rejected"
    assert target.reject_reason == WF_REASON

    # И в журнале от него ничего: конфликт — это «не тронуто», а не «перенесено».
    audit = _audit()
    assert len(audit) == 2
    assert seeded["old"]["conflict"] not in {a.detail["source_draft_id"] for a in audit}


# ── журнал ───────────────────────────────────────────────────────────────────────

def test_audit_names_exactly_two_transfers_with_the_right_actor(seeded):
    _run(seeded, apply=True)

    audit = _audit()
    assert len(audit) == 2
    by_source = {a.detail["source_draft_id"]: a for a in audit}
    assert set(by_source) == {seeded["old"]["approved"], seeded["old"]["rejected"]}

    a = by_source[seeded["old"]["approved"]]
    assert a.user_id is None                      # запись системы, не залогиненного
    assert a.user_email == DECIDER                # автор исходного решения
    assert a.detail["workflow"] == "cold_dm"
    assert a.detail["draft_id"] == seeded["wf"]["approved"]
    assert (a.detail["from"], a.detail["to"]) == ("pending", "approved")
    assert a.detail["decided_by"] == DECIDER

    r = by_source[seeded["old"]["rejected"]]
    assert (r.detail["from"], r.detail["to"]) == ("pending", "rejected")


# ── идемпотентность ──────────────────────────────────────────────────────────────

def test_rerun_transfers_nothing_and_the_database_does_not_move(seeded):
    _run(seeded, apply=True)
    before = _snapshot(seeded)

    s2 = _run(seeded, apply=True)

    assert (s2.approved, s2.rejected) == (0, 0)
    assert s2.already_synced == 2                 # оба найдены по метке в audit_log
    assert s2.conflicts == [seeded["old"]["conflict"]]
    assert (s2.comments_copied, s2.comments_skipped) == (0, 1)
    assert _snapshot(seeded) == before            # ни одна строка не изменилась


# ── комментарии ──────────────────────────────────────────────────────────────────

def test_comment_is_copied_once_under_the_new_draft_id(seeded):
    _run(seeded, apply=True)

    rows = _wf_comments(seeded["wf"]["approved"])
    assert len(rows) == 1
    c = rows[0]
    assert (c.contour, c.draft_id) == ("wf", seeded["wf"]["approved"])
    assert (c.author_email, c.text) == (DECIDER, COMMENT_TEXT)
    assert c.created_at == seeded["comment_at"]
    assert c.variant_index is None and c.prompt_version == "template-v0"

    # Старый контур не тронут: исходный отзыв остаётся при своём черновике.
    old = _read(select(DraftComment).where(DraftComment.contour == "lead",
                                           DraftComment.draft_id
                                           == seeded["old"]["approved"]))
    assert len(old) == 1

    s2 = _run(seeded, apply=True)
    assert (s2.comments_copied, s2.comments_skipped) == (0, 1)
    assert len(_wf_comments(seeded["wf"]["approved"])) == 1


# ── осиротевшие ──────────────────────────────────────────────────────────────────

def test_orphaned_decision_is_counted_and_left_alone(seeded):
    s = _run(seeded, apply=True)
    assert s.orphaned == [seeded["old"]["orphan"]]
    assert s.candidates == 3                      # осиротевший не считается кандидатом
    assert seeded["old"]["orphan"] not in {a.detail["source_draft_id"]
                                           for a in _audit()}
