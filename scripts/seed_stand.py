"""Наполнение тестового стенда правдоподобным объёмом данных.

Создаёт:
- EngageInstance (1)
- User (4 с разными ролями)
- Channel (5-7 с русскими названиями)
- Message (200-300 разложены по каналам и датам за ~14 дней)
- Workflow (2: cold_dm и public_reply)
- WfVerdict (все четыре состояния)
- WfTarget (только для passed=True)
- WfDraft (через wf_drafting.ensure_queue)
- Lead + Draft (старая витрина для cold_dm)

Запуск:
    $env:RADAR_STAND_DATABASE_URL='postgresql+asyncpg://...'
    uv run python -m scripts.seed_stand
"""

from __future__ import annotations

import asyncio
import os
import random
from datetime import datetime, timedelta, timezone

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db.models import (
    Base, Channel, EngageInstance, Lead, Message, User, Workflow, WfTarget,
    WfVerdict
)
from app.services import drafting, wf_drafting

RNG = random.Random(42)
NOW = datetime.now(timezone.utc).replace(microsecond=0)

PAINS = [
    "хостинг тормозит/дорог",
    "VPN не работает",
    "не может настроить сам",
    "нужен админ/подрядчик",
]

# На стенде обязаны встречаться все состояния обсуждения, иначе экран Channels
# выглядит одинаково для случаев, ради различения которых он и переделан
# (FIXES.md #3): «канал → группа читается», «группа известна, но не читана»,
# «обсуждения нет» и «не спрашивали». `linked_*` расставлены руками именно за этим.
CHANNEL_NAMES = [
    # Канал и его группа обсуждения — две строки, как в проде. Сообщения на стенде
    # раскиданы по всем каналам, поэтому у группы они есть: состояние «история
    # прочитана, живого потока нет» — то самое, на которое пожаловался Андрей.
    {"title": "VPS Club", "username": "vpsclub", "topic": "Обсуждение хостинга и VPS",
     "chat_type": "channel", "linked": "devops_chat", "checked": True},
    {"title": "DevOps Chat", "username": "devops_chat", "topic": "Админство и DevOps",
     "chat_type": "supergroup", "linked": "vpsclub", "checked": True},
    {"title": "Мастерская инженера", "username": None, "topic": "Техническое обсуждение"},
    # Группа известна, а строки для неё нет вовсе — так выглядело 61 из 71 обсуждения.
    {"title": "Инфра для стартапов", "username": "infra_startups",
     "topic": "Облако и боль", "chat_type": "channel",
     "linked": "infra_startups_chat", "checked": True},
    {"title": "VPN и Обход", "username": None, "topic": "Технологии маршрутизации"},
    # Спросили — обсуждения у канала нет. Это не поломка: таких 149 из 220.
    {"title": "Linux Админы", "username": "linux_admins",
     "topic": "Системное администрирование", "chat_type": "channel", "checked": True},
]

MESSAGE_TEMPLATES = [
    ("Хостинг в последние дни стал тормозить нещадно, все пик-часы уходят в том, "
     "что пока страница грузится. Ищу куда переехать, посоветуйте вариант подешевле",
     "хостинг тормозит/дорог"),
    ("Сидим на VPS уже год, но последнее время нагрузка растёт, и пришлось выбирать "
     "между расширением или поиском чего-то более производительного",
     "хостинг тормозит/дорог"),
    ("Переехать со своего хостинга на облако, кажется, давно пора. Кто-нибудь уже "
     "делал? Остаётся ещё неделя до того как текущий контракт истечёт",
     "хостинг тормозит/дорог"),
    ("ВПН у меня уже второй день отваливается — дождись полчаса работы и вот "
     "порвалось. Не понимаю, где копать. 3x-ui стоит на сервере, конфиг вроде "
     "правильный", "VPN не работает"),
    ("Мой WireGuard туннель постоянно падает на определённый провайдер, а на другом "
     "всё работает. Дело в том, что его блокируют по DPI, или это конфиг?",
     "VPN не работает"),
    ("Прокси то отваливается, то работает, слова не найду. Может ли быть это из-за "
     "того что я на VPS с плохим каналом?", "VPN не работает"),
    ("Я не могу настроить самостоятельно Marzban на своём сервере — везде ошибки "
     "какие-то выскакивают, не знаю что делать. Может кто подсказать?",
     "не может настроить сам"),
    ("Пытаюсь поднять 3x-ui, но что-то не получается — где-то в конфиге ошибка, "
     "я новичок в этом, помогите", "не может настроить сам"),
    ("Remnawave я поднял, но он не работает, не запускается. Документация не помогает, "
     "в логах ничего понятного", "не может настроить сам"),
    ("Нужен хороший админ, который может настроить нам инфраструктуру. Может кто "
     "порекомендовать?", "нужен админ/подрядчик"),
    ("Ищу DevOps инженера, который понимает Kubernetes и может помочь с миграцией. "
     "Кто-нибудь есть в контактах?", "нужен админ/подрядчик"),
    ("Нужна помощь с переездом на новый сервер — весь процесс, с сохранением данных. "
     "Посоветуйте, где такое заказать", "нужен админ/подрядчик"),
    ("Спасибо за помощь, всё работает!", None),
    ("LOL это была смешная история с сервером", None),
    ("Кто ещё тут занимается хостингом? Просто любопытно", None),
    ("Хорошая погода сегодня, не так ли?", None),
    ("У кого есть рекомендация хорошего ВПН?", None),
    ("Я продаю услугу настройки серверов, недорого, пишите в ЛС", None),
    ("Вакансия: DevOps инженер зарплата 150к", None),
    ("Мой новый проект на Golang уже готов!", None),
]

AUTHOR_NAMES = [
    "Ивал М.", "Пётр К.", "Андрей В.", "Сергей Л.", "Алексей Н.",
    "Виктор Б.", "Денис Р.", "Игорь С.", "Константин Ф.", "Максим Г.",
]

DB_URL = os.environ.get("RADAR_STAND_DATABASE_URL")


async def seed() -> None:
    """Заполнить базу тестовыми данными."""
    if not DB_URL:
        raise SystemExit(
            "нужна переменная RADAR_STAND_DATABASE_URL (тестовая база — схема будет "
            "удалена и создана заново)"
        )
    if "test" not in DB_URL.lower():
        raise SystemExit(
            f"база должна содержать 'test' в имени для защиты от потери данных. "
            f"Получено: {DB_URL}"
        )

    engine = create_async_engine(DB_URL)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)

    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        # EngageInstance
        instance = EngageInstance(
            key="default",
            client_label="Основной",
            base_url="http://engage:8103",
            api_key_env="RADAR_ENGAGE_API_KEY",
        )
        db.add(instance)
        await db.flush()

        # User
        roles_and_names = [
            ("owner", "Иван В.", "ИВ"),
            ("customer", "Клиент А.", "КА"),
            ("reviewer", "Ревьюер Р.", "РР"),
            ("viewer", "Зритель З.", "ЗЗ"),
        ]
        users = {}
        for role, name, initials in roles_and_names:
            user = User(
                email=f"{role}@local",
                name=name,
                initials=initials,
                role=role,
                password_hash="!невозможно-войти",
                totp_secret="X" * 32,
                totp_confirmed=True,
                is_active=True,
            )
            db.add(user)
            users[role] = user
        await db.flush()

        # Channel
        channels = []
        for i, ch_data in enumerate(CHANNEL_NAMES):
            peer_id = -1001 - i * 100
            channel = Channel(
                peer_id=peer_id,
                username=ch_data["username"],
                title=ch_data["title"],
                topic=ch_data["topic"],
                chat_type=ch_data.get("chat_type"),
                linked_chat_username=ch_data.get("linked"),
                linked_checked_at=NOW if ch_data.get("checked") else None,
            )
            db.add(channel)
            channels.append(channel)
        await db.flush()

        # Message
        messages = []
        msg_id_counter = 1000
        msg_date = NOW - timedelta(days=13)

        while msg_date <= NOW:
            channel = RNG.choice(channels)

            if RNG.random() < 0.7:
                msg_text, pain = RNG.choice(MESSAGE_TEMPLATES[:12])
            else:
                msg_text, pain = RNG.choice(MESSAGE_TEMPLATES[12:])

            if RNG.random() < 0.1:
                msg_text = RNG.choice(
                    ["помогите", "не работает", "что делать?", "спасибо"]
                )
            elif RNG.random() < 0.05:
                msg_text = msg_text + " Добавлю деталей: " + msg_text

            is_bot = RNG.random() < 0.05
            is_auto_forward = RNG.random() < 0.1

            if is_auto_forward:
                author_peer_id = None
                author_username = None
                author_name = None
            elif is_bot:
                author_peer_id = 1000000 + RNG.randint(1, 100)
                author_username = f"bot_{RNG.randint(1, 50)}"
                author_name = f"Bot {RNG.randint(1, 50)}"
            else:
                author_peer_id = 500000 + RNG.randint(1, 1000)
                author_username = f"user_{RNG.randint(1, 500)}"
                author_name = RNG.choice(AUTHOR_NAMES)

            message = Message(
                channel_id=channel.id,
                tg_message_id=msg_id_counter,
                tg_date=msg_date,
                author_peer_id=author_peer_id,
                author_username=author_username,
                author_name=author_name,
                author_is_bot=is_bot,
                is_automatic_forward=is_auto_forward,
                text=msg_text,
            )
            db.add(message)
            messages.append(message)
            msg_id_counter += 1

            msg_date += timedelta(minutes=RNG.randint(5, 60))

        await db.flush()
        print(f"Создано сообщений: {len(messages)}")

        # Workflow
        wf_cold_dm = Workflow(
            key="cold_dm",
            title="Личные сообщения",
            target_kind="user",
            action="dm",
            visibility="private",
            engage_instance_id=instance.id,
            engage_use_case="cold_dm",
            cascade_profile="dm_v1",
            sort_order=10,
            is_active=True,
        )
        db.add(wf_cold_dm)

        wf_public_reply = Workflow(
            key="public_reply",
            title="Публичные ответы",
            target_kind="message",
            action="reply",
            visibility="public",
            engage_instance_id=instance.id,
            engage_use_case="public_reply",
            cascade_profile="public_v1",
            # Строго больше, чем у `cold_dm`. Из этого поля строится порядок блоков
            # в сайдбаре (SPEC §9.1), и меньшее значение поднимало публичный контур
            # над личным — контур, который ещё ни разу не отправлял, оказывался
            # первым, что человек видит при входе.
            sort_order=20,
            is_active=True,
        )
        db.add(wf_public_reply)
        await db.flush()

        # WfVerdict
        for workflow in [wf_cold_dm, wf_public_reply]:
            # Доля от ВСЕХ сообщений, а не фиксированные 200. Твёрдое число ломает
            # пропорцию, как только сообщений становится больше: при 559 оно
            # оставляло без вердикта две трети потока, и экран сценария выглядел
            # застрявшим конвейером, а не работающим.
            #
            # Остаток (~15%) вердикта не получает вовсе — это четвёртое состояние,
            # «сценарий сюда ещё не доходил», и оно обязано быть видно.
            processed = RNG.sample(messages, int(len(messages) * 0.85))
            n = len(processed)

            passed_msgs = processed[:int(n * 0.30)]
            rejected_msgs = processed[int(n * 0.30):int(n * 0.85)]
            pending_msgs = processed[int(n * 0.85):]

            for msg in passed_msgs:
                if msg.is_automatic_forward and workflow.key == "cold_dm":
                    continue

                pain = RNG.choice([p for p in PAINS if p is not None])
                verdict = WfVerdict(
                    workflow_id=workflow.id,
                    message_id=msg.id,
                    passed=True,
                    level=3,
                    pain=pain,
                    score=RNG.randint(40, 95),
                    score_breakdown=[
                        {"label": "есть боль", "points": RNG.randint(15, 32)},
                        {"label": "намерение", "points": RNG.randint(10, 24)},
                        {"label": "свежесть", "points": RNG.randint(5, 10)},
                    ],
                    detail={"l0": "структура в порядке", "l1": "есть боль",
                            "l2": "похоже на " + pain, "l3": "живая проблема"},
                )
                db.add(verdict)

            for msg in rejected_msgs:
                level = RNG.randint(0, 3)
                reasons = {
                    0: "автопересылка поста или бот",
                    1: "нет якорей боли",
                    2: "похоже на шум (L2)",
                    3: "не проходит L3",
                }
                verdict = WfVerdict(
                    workflow_id=workflow.id,
                    message_id=msg.id,
                    passed=False,
                    level=level,
                    detail={f"l{level}": reasons[level]},
                )
                db.add(verdict)

            for msg in pending_msgs:
                verdict = WfVerdict(
                    workflow_id=workflow.id,
                    message_id=msg.id,
                    passed=None,
                    level=None,
                    detail={"status": "в очереди на обработку"},
                )
                db.add(verdict)

        await db.flush()

        # Старые колонки `messages.cascade_*` — тень контура ЛС.
        #
        # Без этого шага общий экран потока и воронка на дашборде показывают нули:
        # они читают `messages.cascade_level/passed`, а не `wf_verdicts`. Пока экраны
        # не переехали на новые таблицы, именно эти колонки и смотрит человек, так что
        # незаполненными они делают стенд пустым ровно там, где на него глядят.
        #
        # Копируются из вердикта `cold_dm`, а не считаются заново: по конструкции
        # ветки старые колонки и есть сценарий ЛС, и два независимых расчёта разошлись
        # бы молча.
        await db.execute(
            text("UPDATE messages m SET cascade_level = v.level, "
                 "cascade_passed = v.passed, cascade_detail = v.detail "
                 "FROM wf_verdicts v "
                 "WHERE v.message_id = m.id AND v.workflow_id = :wf_id")
            .bindparams(wf_id=wf_cold_dm.id))
        await db.flush()

        # WfTarget
        targets_by_wf = {}
        for workflow in [wf_cold_dm, wf_public_reply]:
            targets = []
            verdicts = (
                await db.execute(
                    text(
                        "SELECT wv.message_id FROM wf_verdicts wv "
                        "WHERE wv.workflow_id = :wf_id AND wv.passed = true"
                    ).bindparams(wf_id=workflow.id)
                )
            ).scalars().all()

            for msg_id in verdicts[:min(len(verdicts), 50)]:
                msg = next(m for m in messages if m.id == msg_id)

                if workflow.key == "cold_dm" and msg.author_peer_id is None:
                    continue

                pain = (
                    await db.execute(
                        text("SELECT pain FROM wf_verdicts WHERE message_id = :msg_id "
                             "AND workflow_id = :wf_id")
                        .bindparams(msg_id=msg_id, wf_id=workflow.id)
                    )
                ).scalar()

                if workflow.target_kind == "user":
                    target = WfTarget(
                        workflow_id=workflow.id,
                        target_kind="user",
                        message_id=msg.id,
                        channel_id=msg.channel_id,
                        recipient_peer_id=msg.author_peer_id,
                        author_peer_id=msg.author_peer_id,
                        author_username=msg.author_username,
                        author_name=msg.author_name,
                        pain=pain,
                        quote=msg.text[:200] if msg.text else None,
                        score=RNG.randint(40, 95),
                        score_breakdown=[
                            {"label": "есть боль", "points": RNG.randint(15, 32)},
                            {"label": "намерение", "points": RNG.randint(10, 24)},
                        ],
                        disqualifiers=[],
                        status=RNG.choice(
                            ["new", "new", "in_review", "approved", "rejected"]
                        ),
                    )
                else:
                    target = WfTarget(
                        workflow_id=workflow.id,
                        target_kind="message",
                        message_id=msg.id,
                        channel_id=msg.channel_id,
                        # peer_id канала, а НЕ id его строки в базе. Ошибиться здесь
                        # легко (оба целые, оба «про канал»), а расплата — ответ,
                        # адресованный в никуда. Так же считает `targeting.addressing`.
                        chat_peer_id=next(
                            c.peer_id for c in channels if c.id == msg.channel_id),
                        reply_to_message_id=msg.tg_message_id,
                        author_peer_id=msg.author_peer_id,
                        author_username=msg.author_username,
                        author_name=msg.author_name,
                        pain=pain,
                        quote=msg.text[:200] if msg.text else None,
                        score=RNG.randint(40, 95),
                        score_breakdown=[
                            {"label": "есть боль", "points": RNG.randint(15, 32)},
                            {"label": "намерение", "points": RNG.randint(10, 24)},
                        ],
                        disqualifiers=[],
                        status=RNG.choice(
                            ["new", "new", "in_review", "approved", "rejected"]
                        ),
                    )

                if target.status == "rejected":
                    target.reject_reason = RNG.choice(
                        ["не наша тема", "уже клиент", "слишком старое"]
                    )

                db.add(target)
                targets.append(target)

            targets_by_wf[workflow.id] = targets
            await db.flush()
            print(f"Создано целей для {workflow.key}: {len(targets)}")

        # WfDraft
        for workflow in [wf_cold_dm, wf_public_reply]:
            count = await wf_drafting.ensure_queue(db, workflow)
            print(f"Создано черновиков для {workflow.key}: {count}")

            drafts = (
                await db.execute(
                    text("SELECT id FROM wf_drafts WHERE workflow_id = :wf_id")
                    .bindparams(wf_id=workflow.id)
                )
            ).scalars().all()

            for draft_id in RNG.sample(drafts, min(len(drafts), len(drafts) // 3)):
                state = RNG.choice(
                    ["approved", "rejected", "edited", "pending", "pending"]
                )
                await db.execute(
                    text("UPDATE wf_drafts SET state = :state WHERE id = :draft_id")
                    .bindparams(state=state, draft_id=draft_id)
                )
                if state in ["approved", "rejected", "edited"]:
                    decided_by = RNG.choice([u.email for u in users.values()])
                    await db.execute(
                        text(
                            "UPDATE wf_drafts SET decided_by = :decided_by, "
                            "decided_at = :decided_at WHERE id = :draft_id"
                        ).bindparams(
                            decided_by=decided_by,
                            decided_at=NOW - timedelta(hours=RNG.randint(1, 72)),
                            draft_id=draft_id,
                        )
                    )

        await db.flush()

        # Свежие цели — уже ПОСЛЕ того, как очередь разобрана.
        #
        # Без этого шага на стенде не остаётся ни одной цели в статусе «новая»:
        # `ensure_queue` переводит в `in_review` всё, до чего дотянулась, а это
        # основное рабочее состояние экрана — то, что оператор видит, открывая раздел.
        #
        # Заводятся они последними и черновиков не имеют намеренно: именно так
        # выглядит цель, появившаяся после последнего открытия очереди. Цель со
        # статусом «новая», но с готовым черновиком — состояние, которого рабочий
        # конвейер не порождает, и сеять его значило бы показывать небылицу.
        for workflow in [wf_cold_dm, wf_public_reply]:
            taken = set((await db.execute(
                text("SELECT message_id FROM wf_targets WHERE workflow_id = :wf_id")
                .bindparams(wf_id=workflow.id))).scalars().all())
            fresh = (await db.execute(
                text("SELECT message_id, pain, score FROM wf_verdicts "
                     "WHERE workflow_id = :wf_id AND passed = true")
                .bindparams(wf_id=workflow.id))).all()

            added = 0
            for msg_id, pain, score in fresh:
                if added >= 30 or msg_id in taken:
                    continue
                msg = next(m for m in messages if m.id == msg_id)
                if workflow.target_kind == "user" and msg.author_peer_id is None:
                    continue

                address = ({"recipient_peer_id": msg.author_peer_id}
                           if workflow.target_kind == "user"
                           else {"chat_peer_id": next(c.peer_id for c in channels
                                                      if c.id == msg.channel_id),
                                 "reply_to_message_id": msg.tg_message_id})
                db.add(WfTarget(
                    workflow_id=workflow.id, target_kind=workflow.target_kind,
                    message_id=msg.id, channel_id=msg.channel_id,
                    author_peer_id=msg.author_peer_id,
                    author_username=msg.author_username, author_name=msg.author_name,
                    pain=pain, quote=msg.text[:200] if msg.text else None,
                    score=score or RNG.randint(40, 95),
                    score_breakdown=[
                        {"label": "есть боль", "points": RNG.randint(15, 32)},
                        {"label": "намерение", "points": RNG.randint(10, 24)},
                    ],
                    disqualifiers=[], status="new", **address))
                added += 1
            print(f"Свежих целей без черновика у {workflow.key}: {added}")

        await db.flush()

        # Lead + Draft (старая витрина)
        leads = []
        for target in targets_by_wf.get(wf_cold_dm.id, []):
            msg = next(m for m in messages if m.id == target.message_id)
            if msg.author_peer_id is None:
                continue

            lead = Lead(
                message_id=msg.id,
                channel_id=msg.channel_id,
                author_peer_id=msg.author_peer_id,
                author_username=msg.author_username,
                author_name=msg.author_name,
                pain=target.pain,
                quote=msg.text[:200] if msg.text else None,
                score=target.score,
                score_breakdown=target.score_breakdown,
                disqualifiers=target.disqualifiers,
                status=target.status,
                reject_reason=target.reject_reason,
            )
            db.add(lead)
            leads.append(lead)

        await db.flush()
        print(f"Создано лидов: {len(leads)}")

        # Свежие лиды — по тем же сообщениям, что дали цели в статусе «новая».
        #
        # Ровно та же беда, что и с целями: `ensure_draft` ниже разбирает очередь
        # целиком, и без этого шага на экране лидов не остаётся ни одного нового —
        # а это основное состояние экрана, который сейчас и открывают. Черновиков у
        # них нет намеренно: так выглядит лид, доехавший после последнего разбора.
        fresh_dm = (await db.execute(
            text("SELECT t.message_id, t.pain, t.score FROM wf_targets t "
                 "WHERE t.workflow_id = :wf_id AND t.status = 'new'")
            .bindparams(wf_id=wf_cold_dm.id))).all()
        seen_leads = {lead.message_id for lead in leads}
        fresh_count = 0
        for msg_id, pain, score in fresh_dm:
            if msg_id in seen_leads:
                continue
            msg = next(m for m in messages if m.id == msg_id)
            if msg.author_peer_id is None:
                continue
            db.add(Lead(
                message_id=msg.id, channel_id=msg.channel_id,
                author_peer_id=msg.author_peer_id,
                author_username=msg.author_username, author_name=msg.author_name,
                pain=pain, quote=msg.text[:200] if msg.text else None,
                score=score, score_breakdown=[{"label": "есть боль", "points": 25}],
                disqualifiers=[], status="new"))
            fresh_count += 1
        await db.flush()
        print(f"Свежих лидов без черновика: {fresh_count}")

        for lead in leads:
            draft = await drafting.ensure_draft(db, lead)
            if RNG.random() < 0.3:
                draft.state = RNG.choice(
                    ["approved", "rejected", "edited", "pending"]
                )
                if draft.state in ["approved", "rejected", "edited"]:
                    draft.decided_by = RNG.choice([u.email for u in users.values()])
                    draft.decided_at = NOW - timedelta(hours=RNG.randint(1, 72))

        await db.commit()
        print("Коммит выполнен успешно!")

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(seed())
