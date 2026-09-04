"""Очередь дочитывания истории каналов.

До неё «дочитать всем» не уходило дальше первого канала: `jobs.create_external`
заведёт строку прогона под один запуск и отвергает второй `JobBusy`. Здесь —
таблица `backfill_queue`, по строке на канал: её можно пополнять, пока прогон
идёт, она переживает перезапуск процесса и видна снаружи — что стоит, что
делается, что упало.

**Выдача воркеру атомарна** — это главное свойство модуля. Пул задуман как в
`discussions.scan`: параллелизм ровно по числу аккаунтов, потому что очередь
чтений у Engage поаккаунтная и два одновременных чтения одним аккаунтом встанут
друг за другом, потратив дневной бюджет вдвое быстрее без выигрыша по времени.
Два воркера, взявшие один и тот же элемент, прочитали бы канал дважды и списали
двойной бюджет — молча. Поэтому `take_next` забирает строку одним запросом
`SELECT ... ORDER BY ... LIMIT 1 FOR UPDATE SKIP LOCKED`: строку, заблокированную
другим воркером, Postgres не ждёт, а пропускает и отдаёт следующую свободную.
Проверка «состояние = queued» здесь — часть WHERE того же запроса, а не отдельный
шаг в Python: между двумя отдельными запросами успеет влезть конкурент.

Сессию после `take_next` вызывающий коммитит сразу: блокировка строки живёт до
конца транзакции, и держать её на время чтения истории — запирать очередь. После
коммита элемент уже защищён состоянием `running`, и второй воркер его не увидит.

Два правила из постановки Ивана от 04.09.2026 тоже живут здесь, а не в прогоне:

* **глубина считается на постановке**: элемент несёт собственный потолок
  сообщений и дату-границу, а не наследует настройки на момент выдачи, —
  простоявший неделю элемент обязан дочитать ровно то окно, которое человеку
  показали при нажатии кнопки;
* **группу обсуждения читает вступивший**: историю публичной группы Telegram
  отдаст и постороннему, но живые апдейты — только участнику, поэтому элемент
  группы привязывается к `channels.subscribed_account_id` на постановке и
  перепроверяется по нему при выдаче.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import func, or_, select

from app.core import clock
from app.db.models import BackfillItem, Channel

# Работа, которая ещё не закончилась. Элемент в этих состояниях занимает канал:
# повторная постановка молча пропускается, финиширование разрешено.
ACTIVE = ("queued", "running")

# Из чего состоит элемент очереди: что допустимо в `enqueue`.
_ITEM_FIELDS = ("channel_id", "account_id", "target", "min_date")

# Умолчания глубины из постановки Ивана от 04.09.2026: «до 1 месяца истории +
# до 2000 исторических сообщений». Именованные константы, а не числа в коде:
# это условия задачи, и подбирать их по вкусу реализации нельзя.
DEFAULT_TARGET = 2000
DEFAULT_DEPTH = timedelta(days=30)

# Типы чатов Telegram, в которые вступают, а не подписываются: живой поток
# летит только участнику, поэтому историю такой группы обязан читать аккаунт,
# который в ней состоит, — привязка в `enqueue` и `take_next` стоит ровно на этом.
GROUP_CHAT_TYPES = ("supergroup", "group", "forum")


class ItemNotWaiting(Exception):
    """Элемент не в состоянии, допускающем операцию.

    Снять с очереди можно только то, что ещё стоит: взятая работа уже уехала в
    Engage, и пометить её «отменённой» здесь — соврать экрану. Закрыть можно
    только незакрытое: терминальное состояние — итог, а не редактируемое поле.
    Отсутствующий элемент попадает сюда же: отдельного «не найдено» очередь не
    заслужила, а молчать в ответ на неверный id — хуже.
    """


class NotJoined(Exception):
    """Группа обсуждения, в которую ни один аккаунт не вступил.

    Отказ на постановке, а не элемент, который молча упадёт у воркера через
    сутки: «в группу @x ещё не вступали» — для человека следующий шаг, а
    `failed` в очереди — разбирательство.
    """


class AccountMismatch(Exception):
    """Вызывающий просит читать группу аккаунтом, который в неё не вступал.

    Молча подменить аккаунт нельзя: тот, кто просил читать пятым, обязан узнать,
    что группу держит третий, — иначе приём и ответ тихо разъедутся по разным
    аккаунтам.
    """


async def enqueue(db, *, items: list[dict], requested_by: str | None = None,
                  scheduled_for: datetime | None = None, run_id: int | None = None,
                  target: int | None = None,
                  min_date: datetime | None = None) -> list[BackfillItem]:
    """Поставить каналы в очередь; вернуть созданные строки в порядке передачи.

    Канал, который ещё стоит или уже читается, пропускается молча: кнопка
    «дочитать всем», нажатая дважды, не должна удваивать работу. Законченный
    (успешно или нет) канал ставится снова — «дочитать ещё раз позже» — законная
    просьба. Пачка с повторами того же канала внутри даёт одну строку.

    Глубина считается здесь и только здесь: `target` и `min_date` берутся из
    элемента, из параметров пачки или из умолчаний — и замораживаются в строке.
    Считать границу на выдаче значило бы обещать окно, которое растёт вместе
    со временем простоя очереди.

    Группа обсуждения ставится только на аккаунт, который в неё вступил
    (`subscribed_account_id`): без вступления — `NotJoined`, с просьбой о чужом
    аккаунте — `AccountMismatch`. Обычный канал привязки не имеет: его историю
    Telegram отдаёт любому, и `account_id` остаётся как передали.

    Коммита внутри нет: транзакцией владеет вызывающий. Flush есть — чтобы id
    строк были доступны сразу, а не после чужого коммита.
    """
    if not items:
        return []

    unknown = {k for it in items for k in it} - set(_ITEM_FIELDS)
    if unknown:
        raise ValueError("неизвестные поля элемента очереди: "
                         f"{', '.join(sorted(unknown))}; допустимы: "
                         f"{', '.join(_ITEM_FIELDS)}")
    for it in items:
        if not isinstance(it.get("channel_id"), int):
            raise TypeError(f"у элемента очереди нет целого channel_id: {it!r}")
        if "target" in it and not isinstance(it["target"], int):
            raise TypeError(f"у элемента очереди нецелый target: {it!r}")
        if "min_date" in it and not isinstance(it["min_date"], datetime):
            raise TypeError(f"у элемента очереди min_date не дата: {it!r}")

    channel_ids = [it["channel_id"] for it in items]

    # Уже стоящие или читаемые — и в базе, и в самой пачке. Схема (частичный
    # уникальный индекс) поймает то, что проскочит мимо этой проверки при
    # одновременной постановке из двух сессий.
    standing = set((await db.execute(
        select(BackfillItem.channel_id).where(
            BackfillItem.channel_id.in_(channel_ids),
            BackfillItem.state.in_(ACTIVE)))
    ).scalars().all())

    # Порядок — максимум плюс один, а не отдельный генератор: постановка —
    # редкое действие по кнопке, и отдельная последовательность ради неё была
    # бы лишней сущностью. Коллизии одновременных постановок разводит тай-брейк
    # по id в сортировке выдачи.
    base = (await db.execute(select(func.max(BackfillItem.position)))).scalar() or 0

    channels = {c.id: c for c in (await db.execute(
        select(Channel).where(Channel.id.in_(channel_ids)))).scalars().all()}

    # Граница по умолчанию считается один раз на пачку: элементы одной кнопки
    # «дочитать всем» обязаны обещать одно и то же окно, а расползаться даже
    # на секунды между каналами одной пачки ей незачем.
    horizon = min_date or clock.utcnow() - DEFAULT_DEPTH

    made: list[BackfillItem] = []
    for it in items:
        if it["channel_id"] in standing:
            continue
        standing.add(it["channel_id"])
        account_id = it.get("account_id")
        ch = channels.get(it["channel_id"])
        if ch is not None and ch.chat_type in GROUP_CHAT_TYPES:
            # Группу обсуждения читает вступивший: живые апдейты Telegram шлёт
            # только участнику, и читать историю другим — развести приём и
            # ответ по разным аккаунтам.
            name = f"@{ch.username}" if ch.username else f"«{ch.title}»"
            if ch.linked_joined_at is None:
                raise NotJoined(
                    f"в группу {name} аккаунты ещё не вступали — сначала "
                    "вступите, потом ставьте историю на дочитывание")
            if ch.subscribed_account_id is None:
                raise NotJoined(
                    f"группа {name} помечена вступившей, но кто вступил — "
                    "неизвестно; историю группы читает вступивший")
            if account_id is not None and account_id != ch.subscribed_account_id:
                raise AccountMismatch(
                    f"группу {name} держит аккаунт {ch.subscribed_account_id}, "
                    f"а просили {account_id}")
            account_id = ch.subscribed_account_id
        base += 1
        made.append(BackfillItem(
            channel_id=it["channel_id"],
            account_id=account_id,
            position=base, state="queued",
            scheduled_for=scheduled_for, run_id=run_id,
            requested_by=requested_by,
            target=it.get("target", target if target is not None else DEFAULT_TARGET),
            min_date=it.get("min_date", horizon)))
    if made:
        db.add_all(made)
        await db.flush()
    return made


async def due(db, *, now: datetime | None = None) -> list[BackfillItem]:
    """Что назрело: стоящие, у которых наступило расписание, в порядке очереди.

    Читается, а не выдаётся: показать человеку, что очередь собирается делать.
    Брать работу отсюда нельзя — без блокировки это гонка; выдаёт `take_next`.
    """
    now = now or clock.utcnow()
    stmt = select(BackfillItem).where(
        BackfillItem.state == "queued",
        or_(BackfillItem.scheduled_for.is_(None),
            BackfillItem.scheduled_for <= now),
    ).order_by(BackfillItem.position, BackfillItem.id)
    return list((await db.execute(stmt)).scalars().all())


async def take_next(db, *, account_id: int,
                    now: datetime | None = None) -> BackfillItem | None:
    """Атомарно выдать следующий элемент этому аккаунту; нет работы — None.

    Один запрос к базе, и в нём всё: и отбор (стоит, назрело, доступно этому
    аккаунту), и захват. Привязка — фильтром того же запроса: канал, заведённый
    под конкретный аккаунт, обязан читаться именно им — другой может не
    состоять в группе обсуждения; непривязанный достаётся любому.

    Привязка группы перепроверяется и здесь: между постановкой и выдачей
    проходят сутки, и за это время в группу мог вступить другой аккаунт.
    Расхождённый элемент отдавать нельзя — он закрывается `failed` с номером
    нового владельца в тексте ошибки, а поиск продолжается со следующего
    элемента: один уехавший канал не должен останавливать очередь. Поэтому
    цикл, а не единственный запрос: закрытая здесь же строка выпадает из
    условия `state = 'queued'`, и повтор того же запроса берёт следующую.

    Выдача считает попытку в `attempts`: это единственная точка, через которую
    элемент проходит всякий раз, — больше считать попытки негде.

    Коммитит вызывающий, и сразу: пока транзакция открыта, строка заперта, и
    другой воркер её молча обходит. После коммита защиту принимает на себя
    состояние `running`.
    """
    now = now or clock.utcnow()
    stmt = (
        select(BackfillItem)
        .where(
            BackfillItem.state == "queued",
            or_(BackfillItem.scheduled_for.is_(None),
                BackfillItem.scheduled_for <= now),
            or_(BackfillItem.account_id.is_(None),
                BackfillItem.account_id == account_id),
        )
        .order_by(BackfillItem.position, BackfillItem.id)
        .limit(1)
        .with_for_update(skip_locked=True)
    )
    while True:
        item = (await db.execute(stmt)).scalar_one_or_none()
        if item is None:
            return None
        # Канал для проверки привязки читается отдельно, а не JOIN-ом в запросе
        # выдачи: FOR UPDATE по JOIN запер бы и строку канала, сериализуя
        # чужие постановки и отмены на том же канале.
        channel = await db.get(Channel, item.channel_id)
        if (channel is not None
                and channel.chat_type in GROUP_CHAT_TYPES
                and item.account_id is not None
                and channel.subscribed_account_id != item.account_id):
            holder = channel.subscribed_account_id
            item.state = "failed"
            item.error = (
                f"привязка расползлась: группу держит аккаунт {holder}, "
                f"а элемент поставлен на {item.account_id}" if holder is not None
                else f"аккаунт {item.account_id} вышел из группы — читать "
                     "её историю некому")
            item.finished_at = clock.utcnow()
            continue
        item.state = "running"
        item.account_id = account_id
        item.attempts += 1
        item.started_at = now
        return item


async def cancel(db, item_id: int) -> BackfillItem:
    """Снять элемент с очереди. Только стоящий: взятый уже уехал в Engage.

    Строка читается и запирается одним запросом `FOR UPDATE`: между отдельными
    чтением и записью состояния воркер успел бы взять элемент, и отмена легла
    бы поверх начатого чтения. Взявший блокировку конкурент, наоборот, заставит
    эту отмену подождать и увидеть честное `running`.
    """
    item = (await db.execute(
        select(BackfillItem).where(BackfillItem.id == item_id).with_for_update()
    )).scalar_one_or_none()
    if item is None:
        raise ItemNotWaiting(f"элемента #{item_id} нет")
    if item.state != "queued":
        raise ItemNotWaiting(f"элемент #{item_id} уже {item.state} — снять нельзя")
    item.state = "canceled"
    item.finished_at = clock.utcnow()
    return item


async def finish_item(db, item_id: int, *, state: str,
                      error: str | None = None,
                      read_total: int | None = None) -> BackfillItem:
    """Закрыть элемент: `done` или `failed`, с текстом отказа при неудаче.

    Разрешено и со стоящего, и с читаемого: канал может упасть до выдачи
    (воркер снял с себя работу), но обычный путь — воркер закрывает взятое им
    самим. Закрытое закрыть снова нельзя: терминальное состояние — итог работы,
    а не поле для правок.

    `read_total` фиксирует, сколько сообщений реально принято: «прочитано 0 из
    2000» и «прочитано 2000» — разные итоги, и оба обязаны пережить ротацию
    логов, а не жить только в логе прогона.

    Один отказ не останавливает очередь: упавший элемент просто перестаёт
    подходить под `queued`, и следующий `take_next` видит следующий канал.
    """
    if state not in ("done", "failed"):
        raise ValueError(f"закрыть элемент можно в done или failed, не в «{state}»")
    item = (await db.execute(
        select(BackfillItem).where(BackfillItem.id == item_id).with_for_update()
    )).scalar_one_or_none()
    if item is None:
        raise ItemNotWaiting(f"элемента #{item_id} нет")
    if item.state not in ACTIVE:
        raise ItemNotWaiting(f"элемент #{item_id} уже {item.state}")
    item.state = state
    item.error = error
    if read_total is not None:
        item.read_total = read_total
    item.finished_at = clock.utcnow()
    return item


async def summary(db) -> dict:
    """Срез очереди для экрана: сколько чего стоит и чем занят каждый аккаунт.

    Отсутствующие состояния входят нулями: экрану не приходится различать
    «пусто» и «не пришло». Непривязанные элементы в разбивке по аккаунтам не
    участвуют — их ещё никто не брал, и приписывать их какому-то аккаунту
    значило бы соврать о занятости.
    """
    rows = (await db.execute(
        select(BackfillItem.state, BackfillItem.account_id, func.count())
        .group_by(BackfillItem.state, BackfillItem.account_id)
    )).all()

    states = {s: 0 for s in BackfillItem.STATES}
    by_account: dict[int, dict[str, int]] = {}
    for state, account_id, n in rows:
        states[state] = states.get(state, 0) + n
        if account_id is not None:
            bucket = by_account.setdefault(
                account_id, {s: 0 for s in BackfillItem.STATES})
            bucket[state] = bucket.get(state, 0) + n
    return {"states": states, "by_account": by_account}
