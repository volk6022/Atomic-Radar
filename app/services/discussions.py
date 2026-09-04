"""Разбор групп обсуждения: где живут комментарии и читаем ли мы их (FIXES.md #3).

Лиды живут не в канале, а в его группе обсуждения: под постом канала комментируют
люди, и именно их реплики проходят каскад. Telegram связывает две сущности полем
`linked_chat_username`, а в Радаре канал и его группа — две независимые строки
`channels`, между которыми до этого модуля не было связи вовсе.

Отсюда три вещи, которые здесь и делаются.

**Связь заполняется.** `get_chat_info` у канала отдаёт имя его группы, у группы —
имя канала. Оба направления записываются в `linked_chat_username` /
`linked_chat_peer_id`, а факт самого опроса — в `linked_checked_at`. Без последнего
пустое поле значило сразу две несовместимые вещи: «группы нет» (таких 149 из 220
опрошенных 28.08) и «мы не спрашивали», и на экране обе выглядели одинаково — ноль
сообщений.

**История читается пачкой.** Разбор шестидесяти групп — это одна задача оператора,
а не шестьдесят: у неё один прогресс, одна кнопка отмены и один итог. Собрать такое
на вебхуках можно только счётчиком в общей строке, который правят несколько
процессов сразу, поэтому здесь результат Engage забирается опросом
(`engage.wait_for_task`), а сообщения кладутся тем же `ingest_service.ingest_history`,
что и при обычном бэкфилле. Никакой второй дороги для данных не заводится.

**Живой поток — отдельный вопрос, и он не решается чтением.** Историю публичной
супергруппы Telegram отдаёт кому угодно, а апдейты в реальном времени шлёт только
участнику. Пока аккаунт не вступил в группу (`linked_joined_at` пуст), разбор даёт
разовую выгрузку — ровно то, на что 29.08 пожаловался Андрей: комментарии под постом
он видит, а в Радаре их нет. Вступление живёт в подключении канала (FIXES.md #7) и
упирается в профиль безопасности аккаунтов в Engage, а не в этот код.
"""
from __future__ import annotations

import asyncio
import logging

from sqlalchemy import func, select

from app.core import clock
from app.db.models import Channel, Message, MessageReader
from app.db.session import get_session_maker
from app.services import engage
from app.services import ingest as ingest_service

logger = logging.getLogger("radar.discussions")

# Сколько сообщений просить за один вызов истории. То же значение, что у бэкфилла
# (`app/api/v1/ingest.py`, PAGE_LIMIT): потолок Engage — 1000, но каждый ответ едет
# целиком в одном JSON, и на тысяче это уже мегабайт.
PAGE_LIMIT = 500

# Типы чатов, которые Telegram считает обсуждением. `forum` — та же супергруппа, но
# с включёнными темами; без неё @amnezia_vpn молча не дал ни одного сообщения.
GROUP_TYPES = ("supergroup", "group", "forum")


# Состояния канала по его обсуждению. Пять, а не три, потому что каждое требует
# своего следующего шага, и сваливать их в «ноль сообщений» — это ровно та ошибка,
# из-за которой пункт 3 полгода выглядел как «не прошёл backfill».
#
#   unknown — карточку канала ни разу не спрашивали. Шаг: спросить.
#   none    — спрашивали, обсуждения у канала нет. Шага нет, это не поломка (таких
#             было 149 из 220 опрошенных 28.08).
#   unread  — группа известна, но её история не прочитана. Шаг: разбор.
#   history — история прочитана, но аккаунт в группе не состоит: живых комментариев
#             не будет. Шаг: вступить.
#   live    — аккаунт в группе, комментарии приезжают сами.
STATES = ("unknown", "none", "unread", "history", "live")


def discussion_state(channel, by_username: dict, message_counts: dict,
                     last_message: dict | None = None) -> dict:
    """Что мы знаем про обсуждение этого канала и что с этим делать.

    Группа ищется по имени, а не по `linked_chat_peer_id`: peer группы известен
    только после того, как её карточку спросили, а имя приезжает уже в карточке
    самого канала — то есть на шаг раньше.
    """
    linked = channel.linked_chat_username
    if not linked:
        return {"state": "none" if channel.linked_checked_at else "unknown",
                "username": None, "channel_id": None, "messages": 0,
                "last_message_at": None}

    group = by_username.get(linked.lower())
    messages = message_counts.get(group.id, 0) if group is not None else 0
    last = (last_message or {}).get(group.id) if group is not None else None
    if messages == 0:
        state = "unread"
    else:
        state = "live" if (group is not None and group.linked_joined_at) else "history"
    return {"state": state, "username": linked,
            "channel_id": group.id if group is not None else None,
            "messages": messages,
            "last_message_at": last.isoformat() if last else None}


async def select_channels(db, *, scope: str, channel_ids: list[int] | None) -> list[int]:
    """Кого разбираем. Возвращает id строк `channels` в порядке убывания аудитории —
    у крупного канала комментариев больше, и если прогон прервут на середине, ценное
    окажется уже прочитанным.

    * `ids` — ровно перечисленные строки, без домысливания;
    * `pending` — не разобранные: карточку не спрашивали ИЛИ группа известна и не
      прочитана. Значение по умолчанию, потому что именно это человек имеет в виду,
      нажимая «разобрать». Порознь эти два множества почти бесполезны: на 31.08 в
      проде «не спрашивали» — 141 канал, а «известна и не прочитана» — всего 2, и
      прогон по второму выглядел бы как «нечего делать» при 61 молчащей группе;
    * `unknown` — только те, у кого мы ни разу не спрашивали карточку;
    * `unread` — только те, где группа известна, но её строки в Радаре нет или она
      пустая;
    * `all` — всё, что отслеживается. Дороже, но переспрашивает и связи, которые
      могли устареть: канал заводит обсуждение или закрывает его когда захочет.
    """
    if scope == "ids":
        return list(channel_ids or [])

    q = select(Channel.id).where(Channel.ingest_enabled.is_(True))
    if scope == "unknown":
        q = q.where(Channel.linked_checked_at.is_(None))
    elif scope == "unread":
        q = q.where(Channel.linked_chat_username.isnot(None))

    rows = (await db.execute(q.order_by(
        Channel.members.desc().nullslast(), Channel.id))).scalars().all()

    if scope in ("all", "unknown"):
        return list(rows)

    # «Группу не читаем» — это сопоставление двух строк одной и той же таблицы по
    # имени (`channels.linked_chat_username` → `channels.username`). Выражать его
    # самосоединением с подзапросом ради одного round-trip — плохой размен: каналов
    # сотни, всё умещается в память, а условие должно читаться глазами.
    counts = dict((await db.execute(
        select(Message.channel_id, func.count(Message.id)).group_by(Message.channel_id)
    )).all())
    all_channels = (await db.execute(select(Channel))).scalars().all()
    by_id = {c.id: c for c in all_channels}
    by_name = {c.username.lower(): c for c in all_channels if c.username}

    out = []
    for cid in rows:
        channel = by_id[cid]
        if scope == "pending" and channel.linked_checked_at is None:
            out.append(cid)               # не спрашивали — разбирать в любом случае
            continue
        if scope == "pending" and not channel.linked_chat_username:
            continue                      # спрашивали, обсуждения нет — делать нечего
        group = by_name.get((channel.linked_chat_username or "").lower())
        if group is None or counts.get(group.id, 0) == 0:
            out.append(cid)
    return out


async def _fetch_info(account_id: int, username: str) -> dict:
    """Карточка чата. Обратный адрес обязателен по контракту Engage, поэтому едет
    настоящий — с `kind=polled`, который приёмник осознанно игнорирует: результат мы
    забираем опросом, и разбирать его вторым путём значило бы положить сообщения
    дважды."""
    task = await engage.action(
        account_id=account_id, action="get_chat_info", payload={"username": username},
        webhook_url=engage.webhook_url(kind="polled"))
    return await engage.wait_for_task(task["task_id"])


async def _fetch_page(account_id: int, *, username: str | None, peer_id: int | None,
                      limit: int, max_id: int) -> dict:
    payload: dict = {"limit": limit}
    if username:
        payload["username"] = username
    else:
        payload["peer_id"] = peer_id
    if max_id:
        payload["max_id"] = max_id
    task = await engage.action(
        account_id=account_id, action="get_chat_history", payload=payload,
        webhook_url=engage.webhook_url(kind="polled"))
    return await engage.wait_for_task(task["task_id"])


async def _read_history(db, account_id: int, *, peer_id: int, username: str | None,
                        title: str | None, target: int, cancelled) -> int:
    """Дочитать историю группы до `target` сообщений. Возвращает, сколько приняли.

    Листаем назад по `max_id`: `offset_id` в kurigram помечен устаревшим и молча
    возвращает одну и ту же страницу — это уже проверено живым прогоном на пяти
    страницах подряд (`app/api/v1/ingest.py`, тот же приём в цепочке вебхуков).
    """
    accepted = 0
    max_id = 0
    while accepted < target and not cancelled():
        result = await _fetch_page(account_id, username=username, peer_id=peer_id,
                                   limit=min(PAGE_LIMIT, target - accepted),
                                   max_id=max_id)
        posts = result.get("posts") or []
        if not posts:
            break
        out = await ingest_service.ingest_history(
            db, chat_id=peer_id, chat_username=username, chat_title=title,
            posts=posts, account_id=account_id)
        await db.commit()
        accepted += out.get("accepted", 0)

        ids = [p["message_id"] for p in posts if p.get("message_id")]
        if not ids:
            break
        oldest = min(ids)
        if max_id and oldest >= max_id:
            # Курсор не сдвинулся — история кончилась или Engage вернул то же самое.
            # Продолжать значило бы крутить один и тот же вызов до конца бюджета.
            logger.warning("history_cursor_stuck peer=%s max_id=%s", peer_id, max_id)
            break
        max_id = oldest - 1
        if len(posts) < PAGE_LIMIT:
            break
    return accepted


async def _scan_one(db, channel_id: int, account_id: int, *,
                    target: int, cancelled, check_only: bool = False) -> dict:
    """Разобрать один канал: карточка → связь → (если это группа) история.

    Граница «проверить / дочитать» проходит по вызову `_read_history`: всё до него —
    опрос карточек и запись связи (`linked_checked_at`, строка группы), сам же вызов
    и только он читает историю (`get_chat_history`). При `check_only=True` на этой
    границе разбор заканчивается: в ответе стоит `checked_only`, а `read` остаётся
    нулём не потому, что группа пуста, а потому что её не читали.

    Ветка по типу чата, а не по имени строки: в `channels` лежат вперемешку каналы
    и их группы обсуждения (`corpostrovokru` и `corpostrovokru_chat` — две
    независимые записи), и до `chat_type` отличить одно от другого можно было
    только гадая по суффиксу.
    """
    channel = await db.get(Channel, channel_id)
    if channel is None or not channel.username:
        # Без username карточку не спросить: `get_chat_info` по peer_id требует,
        # чтобы аккаунт уже знал этот пир, а у чужого канала он его не знает.
        return {"skipped": "нет username"}

    info = await _fetch_info(account_id, channel.username)
    if not info.get("found", True):
        return {"failed": info.get("reason") or "не найден"}

    chat_type = info.get("type")
    linked = info.get("linked_chat_username")
    channel.chat_type = chat_type
    channel.linked_chat_username = linked
    channel.linked_checked_at = clock.utcnow()
    if info.get("members_count") is not None:
        channel.members = info["members_count"]
    if info.get("title"):
        channel.title = info["title"]
    await db.commit()

    if chat_type in GROUP_TYPES:
        # Строка сама и есть группа обсуждения. Читаем её напрямую — искать «её
        # группу» некуда, `linked` здесь указывает обратно на канал.
        if check_only:
            return {"group_id": channel.id, "own_group": True, "checked_only": True}
        read = await _read_history(db, account_id, peer_id=channel.peer_id,
                                   username=channel.username, title=channel.title,
                                   target=target, cancelled=cancelled)
        return {"group_id": channel.id, "read": read, "own_group": True}

    if not linked:
        return {"no_group": True}

    ginfo = await _fetch_info(account_id, linked)
    if not ginfo.get("found", True):
        return {"failed_group": ginfo.get("reason") or "группа не найдена",
                "linked": linked}

    group = await ingest_service.get_or_create_channel(
        db, peer_id=ginfo["peer_id"], username=ginfo.get("username") or linked,
        title=ginfo.get("title"))
    group.chat_type = ginfo.get("type")
    group.linked_chat_username = ginfo.get("linked_chat_username") or channel.username
    group.linked_chat_peer_id = channel.peer_id
    group.linked_checked_at = clock.utcnow()
    if ginfo.get("members_count") is not None:
        group.members = ginfo["members_count"]
    channel.linked_chat_peer_id = group.peer_id
    await db.commit()

    have = (await db.execute(select(func.count(Message.id))
                             .where(Message.channel_id == group.id))).scalar_one()
    read = 0
    if not check_only and have < target:
        read = await _read_history(db, account_id, peer_id=group.peer_id,
                                   username=group.username, title=group.title,
                                   target=target - have, cancelled=cancelled)
    out = {"group_id": group.id, "linked": group.username, "read": read,
           "already_had": have}
    if check_only:
        out["checked_only"] = True
    return out


async def scan(*, channel_ids: list[int], account_ids: list[int], target: int,
               report, cancelled, check_only: bool = False) -> dict:
    """Пройти список каналов, разложив их по аккаунтам.

    Параллелизм ровно по числу аккаунтов: очередь задач у Engage поаккаунтная, и
    два одновременных чтения одним аккаунтом встанут друг за другом, зато потратят
    дневной бюджет вдвое быстрее без выигрыша по времени.

    При `check_only=True` история не читается вовсе: прогон только спрашивает
    карточки, записывает связь и заводит строки групп.

    Отказ на одном канале не отменяет остальные. Приватная группа, опечатка в имени,
    флуд-контроль на одном аккаунте — обычные события на списке из шестидесяти
    штук; прогон, падающий целиком из-за одного из них, пришлось бы запускать
    заново, перечитывая уже прочитанное.
    """
    queue: list[int] = list(channel_ids)
    total = len(queue)
    lock = asyncio.Lock()
    stats = {"total": total, "done": 0, "no_group": 0, "groups_linked": 0,
             "messages": 0, "failed": 0, "skipped": 0}

    async def worker(account_id: int) -> None:
        maker = get_session_maker()
        while not cancelled():
            async with lock:
                if not queue:
                    return
                channel_id = queue.pop(0)
            try:
                async with maker() as db:
                    out = await _scan_one(db, channel_id, account_id,
                                          target=target, cancelled=cancelled,
                                          check_only=check_only)
            except Exception as e:  # noqa: BLE001 — один канал не роняет прогон
                logger.warning("discussion_scan_failed channel=%s account=%s error=%s",
                               channel_id, account_id, e)
                out = {"failed": f"{type(e).__name__}: {e}"}

            async with lock:
                stats["done"] += 1
                if out.get("no_group"):
                    stats["no_group"] += 1
                if out.get("group_id"):
                    stats["groups_linked"] += 1
                stats["messages"] += out.get("read", 0)
                if out.get("failed") or out.get("failed_group"):
                    stats["failed"] += 1
                if out.get("skipped"):
                    stats["skipped"] += 1
                done, note = stats["done"], _note(channel_id, out)
            await report(100.0 * done / total if total else 100.0,
                         f"[{done}/{total}] {note}")

    await asyncio.gather(*(worker(a) for a in account_ids))

    stats["cancelled"] = cancelled() and stats["done"] < total
    return stats


def _note(channel_id: int, out: dict) -> str:
    if out.get("skipped"):
        return f"канал {channel_id} пропущен: {out['skipped']}"
    if out.get("failed"):
        return f"канал {channel_id}: {out['failed']}"
    if out.get("failed_group"):
        return (f"канал {channel_id}: группа @{out.get('linked')} — "
                f"{out['failed_group']}")
    if out.get("no_group"):
        return f"канал {channel_id}: группы обсуждения нет"
    if out.get("own_group"):
        if out.get("checked_only"):
            return f"группа {channel_id}: проверена, история не читалась"
        return f"группа {channel_id}: прочитано {out.get('read', 0)}"
    if out.get("checked_only"):
        return (f"канал {channel_id} → @{out.get('linked')}: группа найдена, "
                f"история не читалась, уже было {out.get('already_had', 0)}")
    return (f"канал {channel_id} → @{out.get('linked')}: прочитано "
            f"{out.get('read', 0)}, уже было {out.get('already_had', 0)}")


# ── вступление в группы (план 1.6, шаг 7) ─────────────────────────────────────

# Сколько вступлений в сутки разрешено одному аккаунту. Значение из
# `fleet_manager/config/safety.yaml`, профиль `public_reply`: `joins_per_day: 3`.
# Продублировано здесь осознанно и как потолок, а не запрошено у Engage: Engage при
# исчерпанном бюджете задачу не отвергает, а ОТКЛАДЫВАЕТ на час и переносит, пока не
# сменятся сутки. Пачка из сорока вступлений, заказанная разом, безопаснее от этого
# не станет — она превратится в сорок задач, которые сутками стучатся в планировщик.
# Резать пачку здесь дешевле, и остаток виден в отчёте прогона.
JOINS_PER_ACCOUNT_PER_DAY = 3

# Сколько ждать результата вступления. Пауза хьюманайзера у `join_group` — 60–300 с
# (у чтений её нет вовсе), и перед ней задача ещё стоит в поаккаунтной очереди Engage
# за предыдущей. Штатных 300 с `wait_for_task` на это не хватает.
JOIN_WAIT_SECONDS = 900.0


async def select_groups_to_join(db, *, scope: str, channel_ids: list[int] | None
                                ) -> list[int]:
    """Строки групп, в которые стоит вступить. Порядок — по убыванию аудитории:
    прерванный прогон оставит недоделанным мелкое.

    Отбор намеренно узкий, и каждое условие отсекает свой класс ошибок:

    * `chat_type` из `GROUP_TYPES` — вступают в группу обсуждения, а не в канал: на
      канал аккаунт подписывается при подключении, и «вступление» в него потратило бы
      лимит на уже сделанное;
    * `username is not null` — **только открытые группы**. Вступление идёт по имени
      (`join_chat("@name")`); у закрытой группы имени нет, и попасть туда можно только
      заявкой по ссылке-приглашению. Заявка — это след, который видит администратор,
      и решение, которое принимает человек, а не прогон;
    * `linked_joined_at is null` — уже вступили, второй раз лимит тратить не на что;
    * `ingest_enabled` — снятый с отслеживания чат оператор выключил сам.

    `scope="ids"` фильтруется теми же правилами, а не доверяет списку с экрана:
    кнопка «вступить» на строке приватной группы — ошибка интерфейса, и отработать её
    отказом здесь дешевле, чем потом объяснять заявку в чужой чат.
    """
    q = (select(Channel.id)
         .where(Channel.ingest_enabled.is_(True),
                Channel.chat_type.in_(GROUP_TYPES),
                Channel.username.isnot(None),
                Channel.linked_joined_at.is_(None)))
    if scope == "ids":
        q = q.where(Channel.id.in_(list(channel_ids or []) or [0]))
    return list((await db.execute(
        q.order_by(Channel.members.desc().nullslast(), Channel.id))).scalars().all())


async def _readers_of(db, group_ids: list[int]) -> dict[int, int]:
    """Кто из аккаунтов прочитал в группе больше всего — по одному на группу.

    Живой поток пойдёт через того, кто вступил, а Андрей отвечает из одного аккаунта.
    Если группу читал третий, а вступит первый, наводка и ответ уедут на разные
    аккаунты, и адресат получит сообщение «ниоткуда» — ровно та причина, по которой в
    очереди черновиков появилась колонка аккаунта приёма.
    """
    if not group_ids:
        return {}
    seen = func.count(MessageReader.message_id)
    rows = (await db.execute(
        select(Message.channel_id, MessageReader.account_id, seen)
        .join(MessageReader, MessageReader.message_id == Message.id)
        .where(Message.channel_id.in_(group_ids))
        .group_by(Message.channel_id, MessageReader.account_id)
        .order_by(Message.channel_id, seen.desc(), MessageReader.account_id))).all()
    best: dict[int, int] = {}
    for channel_id, account_id, _count in rows:
        best.setdefault(channel_id, account_id)
    return best


def plan_joins(group_ids: list[int], account_ids: list[int], *, per_account: int,
               preferred: dict[int, int]) -> dict[int, list[int]]:
    """Разложить группы по аккаунтам, не превышая суточный потолок ни у кого.

    Сначала группе предлагается тот аккаунт, который её читал; если у него на сегодня
    места нет, группа уходит к самому свободному. Что не поместилось — не уходит
    никуда: остаток честно виден в отчёте как «осталось на следующий раз».
    """
    cap = max(0, min(per_account, JOINS_PER_ACCOUNT_PER_DAY))
    plan: dict[int, list[int]] = {a: [] for a in account_ids}
    if not account_ids or cap == 0:
        return plan
    for group_id in group_ids:
        want = preferred.get(group_id)
        if want in plan and len(plan[want]) < cap:
            plan[want].append(group_id)
            continue
        free = min(plan, key=lambda a: (len(plan[a]), a))
        if len(plan[free]) >= cap:
            break
        plan[free].append(group_id)
    return plan


async def _join_one(db, group_id: int, account_id: int, *, subscribed_by: str) -> dict:
    """Вступить в одну группу. Историю здесь не читаем ни при каких условиях:
    «вступить» и «дочитать» — два разных решения оператора и два разных бюджета.

    Отметка ставится в строку САМОЙ ГРУППЫ, а не канала, которому она принадлежит:
    именно её читает `discussion_state`, и именно про эту строку правда «мы в этом
    чате состоим». Живой поток Telegram шлёт участнику того чата, в который вступили.
    """
    group = await db.get(Channel, group_id)
    if group is None or not group.username:
        return {"skipped": "нет username — закрытая группа"}
    if group.linked_joined_at is not None:
        return {"skipped": "уже вступали"}

    task = await engage.action(
        account_id=account_id, action="join_group",
        payload={"target": group.username},
        webhook_url=engage.webhook_url(kind="polled"))
    await engage.wait_for_task(task["task_id"], timeout=JOIN_WAIT_SECONDS)

    group.linked_joined_at = clock.utcnow()
    group.subscribed_account_id = account_id
    group.subscribed_by = subscribed_by
    group.subscribed_at = clock.utcnow()
    await db.commit()
    logger.info("group_joined group=%s username=%s account=%s by=%s",
                group.id, group.username, account_id, subscribed_by)
    return {"joined": True, "username": group.username, "account_id": account_id}


async def join_groups(*, group_ids: list[int], account_ids: list[int],
                      per_account: int, subscribed_by: str, report, cancelled) -> dict:
    """Вступить списком, по потоку на аккаунт.

    Параллелизм ровно по числу аккаунтов: очередь у Engage поаккаунтная, и два
    вступления одним аккаунтом всё равно встанут друг за другом.

    Отказ на одной группе не отменяет остальные — приватность, флуд-контроль и
    «слишком много каналов» на списке из сорока штук встречаются каждый раз.
    """
    async with get_session_maker()() as db:
        preferred = await _readers_of(db, group_ids)
    plan = plan_joins(group_ids, account_ids, per_account=per_account,
                      preferred=preferred)
    planned = sum(len(v) for v in plan.values())
    stats = {"total": len(group_ids), "planned": planned,
             "left": len(group_ids) - planned, "done": 0, "joined": 0,
             "failed": 0, "deferred": 0, "skipped": 0}
    lock = asyncio.Lock()

    async def worker(account_id: int) -> None:
        maker = get_session_maker()
        for group_id in plan[account_id]:
            if cancelled():
                return
            try:
                async with maker() as db:
                    out = await _join_one(db, group_id, account_id,
                                          subscribed_by=subscribed_by)
            except engage.EngageTaskDeferred as e:
                logger.warning("group_join_deferred group=%s account=%s error=%s",
                               group_id, account_id, e)
                out = {"deferred": str(e)}
            except Exception as e:  # noqa: BLE001 — одна группа не роняет прогон
                logger.warning("group_join_failed group=%s account=%s error=%s",
                               group_id, account_id, e)
                out = {"failed": f"{type(e).__name__}: {e}"}

            async with lock:
                stats["done"] += 1
                for key in ("joined", "failed", "deferred", "skipped"):
                    if out.get(key):
                        stats[key] += 1
                done, note = stats["done"], _join_note(group_id, account_id, out)
            await report(100.0 * done / planned if planned else 100.0,
                         f"[{done}/{planned}] {note}")

    await asyncio.gather(*(worker(a) for a in account_ids))
    stats["cancelled"] = cancelled() and stats["done"] < planned
    return stats


def _join_note(group_id: int, account_id: int, out: dict) -> str:
    if out.get("joined"):
        return f"аккаунт {account_id} вступил в @{out.get('username')}"
    if out.get("deferred"):
        return (f"группа {group_id}: у аккаунта {account_id} кончился дневной лимит "
                f"вступлений — {out['deferred']}")
    if out.get("skipped"):
        return f"группа {group_id} пропущена: {out['skipped']}"
    return f"группа {group_id}: {out.get('failed')}"
