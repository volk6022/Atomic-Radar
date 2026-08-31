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
from app.db.models import Channel, Message
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
            db, chat_id=peer_id, chat_username=username, chat_title=title, posts=posts)
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
                    target: int, cancelled) -> dict:
    """Разобрать один канал: карточка → связь → (если это группа) история.

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
    if have < target:
        read = await _read_history(db, account_id, peer_id=group.peer_id,
                                   username=group.username, title=group.title,
                                   target=target - have, cancelled=cancelled)
    return {"group_id": group.id, "linked": group.username, "read": read,
            "already_had": have}


async def scan(*, channel_ids: list[int], account_ids: list[int], target: int,
               report, cancelled) -> dict:
    """Пройти список каналов, разложив их по аккаунтам.

    Параллелизм ровно по числу аккаунтов: очередь задач у Engage поаккаунтная, и
    два одновременных чтения одним аккаунтом встанут друг за другом, зато потратят
    дневной бюджет вдвое быстрее без выигрыша по времени.

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
                                          target=target, cancelled=cancelled)
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
        return f"группа {channel_id}: прочитано {out.get('read', 0)}"
    return (f"канал {channel_id} → @{out.get('linked')}: прочитано "
            f"{out.get('read', 0)}, уже было {out.get('already_had', 0)}")
