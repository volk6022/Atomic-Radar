"""Исполнитель очереди дочитывания: тик, выдающий по одному каналу на аккаунт.

**Почему тик, а не пул с ожиданием.** Цепочку страниц двигает не наш процесс:
Радар просит у Engage одну страницу, Engage возвращает её вебхуком, и приёмная
ручка просит следующую. Воркеру нечего ждать — ждать пришлось бы опросом
состояния строки, а это и таймауты, и живой процесс, держащий очередь. Тик
поступает иначе: отдаёт просроченное, смотрит, у кого из аккаунтов нет начатого
чтения, выдаёт такому ровно один канал и завершается. Дальше цепочку ведут
вебхуки, а следующий тик по расписанию подберёт освободившихся.

**Один канал на аккаунт за раз** — свойство очереди Engage: она поаккаунтная и
строго FIFO, два одновременных чтения одним аккаунтом встанут друг за другом,
потратив дневной бюджет вдвое быстрее без выигрыша по времени.

**Оборванная цепочка обязана возвращаться.** Вебхук может не прийти вовсе
(задача Engage убита таймаутом, доставка исчерпала попытки) — тогда элемент
навсегда остаётся `running`, и этот аккаунт больше никогда не получит работы.
Поэтому тик первым делом отбирает просроченные: `STALE_AFTER` без движения —
назад в очередь, а после `MAX_ATTEMPTS` попыток — `failed`. Это и есть порог,
которого у колонки `attempts` не было: без него канал, падающий каждый раз,
крутится в очереди бесконечно и занимает аккаунт.

**Недоступный Engage — не вина канала.** Если первую страницу не удалось
заказать, элемент возвращается в очередь, а не закрывается `failed`: закрыть
его значило бы вычеркнуть канал из-за сетевого сбоя. Попытка при этом уже
посчитана в `take_next` и не сбрасывается — три сетевых сбоя подряд элемент
всё-таки закроют, и это честный предел.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from sqlalchemy import func, select

from app.core import clock
from app.db.models import BackfillItem, Channel, Message
from app.db.session import get_session_maker
from app.services import backfill_chain, backfill_queue, engage

logger = logging.getLogger(__name__)

# Сколько цепочка может молчать. Страница истории приезжает вебхуком за десятки
# секунд даже с ретраями доставки; час тишины означает, что цепочки больше нет —
# задача убита или доставка исчерпала попытки. Короче — отрывать живую цепочку
# ради перестраховки, дольше — держать аккаунт бесполезно запертым.
STALE_AFTER = timedelta(minutes=60)

# После скольких попыток элемент закрывается failed. Попытку считает `take_next`
# — единственная точка, через которую элемент проходит всякий раз. Трёх хватает,
# чтобы пережить один-два сетевых сбоя, и мало для вечного каруселирования.
MAX_ATTEMPTS = 3


async def tick(*, account_ids: list[int] | None = None,
               now: datetime | None = None) -> dict:
    """Один удар исполнителя: вернуть просроченное, выдать свободным по каналу.

    Возвращает итог с ключами `started`, `requeued`, `failed`, `busy` — по нему
    видно, чем закончился удар, не открывая журнал: расписание бьётся каждые
    несколько минут круглые сутки, и шаг удара обязан быть виден из результата
    задачи arq.

    `account_ids` — чей флот разбирать в этом ударе. По умолчанию спрашивается
    у Engage: список, записанный в настройках, увёл бы чтение на аккаунт,
    который давно заблокирован.
    """
    now = now or clock.utcnow()
    stats = {"started": 0, "requeued": 0, "failed": 0, "busy": 0}
    maker = get_session_maker()

    # Просроченные разбираются ДО выдачи и в той же транзакции, что и возврат:
    # к моменту отбора свободных аккаунтов возвращённый элемент уже снова стоит
    # в очереди, и освободившийся аккаунт тут же получит работу — вместо часа
    # простоя до следующего удара.
    async with maker() as db:
        stale = (await db.execute(
            select(BackfillItem)
            .where(BackfillItem.state == "running",
                   BackfillItem.started_at <= now - STALE_AFTER)
            .order_by(BackfillItem.position, BackfillItem.id))).scalars().all()
        for row in stale:
            if row.attempts >= MAX_ATTEMPTS:
                await backfill_queue.finish_item(
                    db, row.id, state="failed",
                    error=f"цепочка молчала дольше {int(STALE_AFTER.total_seconds() // 60)}"
                          f" минут и исчерпала все {MAX_ATTEMPTS} попытки — вебхуки"
                          " не приходили, чтение не двигалось")
                stats["failed"] += 1
                logger.warning("backfill_item_failed_stale item=%s attempts=%s",
                               row.id, row.attempts)
            else:
                # Попытку не сбрасываем: она уже посчитана в take_next, и именно
                # по счётчику элемент однажды закроется, а не крутится вечно.
                row.state = "queued"
                row.started_at = None
                stats["requeued"] += 1
                logger.info("backfill_item_requeued item=%s attempts=%s",
                            row.id, row.attempts)
        await db.commit()

    if account_ids is None:
        try:
            fleet = await engage.list_accounts()
        except engage.EngageUnavailable as e:
            # Флот недоступен — удар пуст, а не сломан: очерёдность элементов не
            # меняется, следующий удар договорится. Элементы при этом не
            # трогаются вовсе, ни одна попытка не списывается.
            logger.warning("backfill_drain_fleet_unavailable error=%s", e)
            return stats
        # Только активные: заблокированный или спящий аккаунт не выполнит задачу,
        # и элемент простоял бы свой час впустую.
        account_ids = [a["account_id"] for a in fleet
                       if a.get("status") == "active"]

    async with maker() as db:
        busy = set((await db.execute(
            select(BackfillItem.account_id).where(
                BackfillItem.state == "running",
                BackfillItem.account_id.is_not(None)))).scalars().all())

    for account_id in account_ids:
        if account_id in busy:
            stats["busy"] += 1
            continue
        async with maker() as db:
            item = await backfill_queue.take_next(db, account_id=account_id, now=now)
            # Коммит сразу после выдачи: блокировка строки живёт до конца
            # транзакции, и держать её на время запроса к Engage — запирать
            # очередь. После коммита элемент защищён состоянием `running`.
            await db.commit()
            if item is None:
                continue
            # Канал уже заведён, его peer_id и username лежат в строке: карточка
            # через get_chat_info стоила бы чтение из дневного бюджета на каждый
            # элемент очереди. Курсор и стартовый счётчик читаются после коммита:
            # это обычные чтения, блокировать выдачу ради них незачем.
            channel = await db.get(Channel, item.channel_id)
            if channel is None:
                await backfill_queue.finish_item(
                    db, item.id, state="failed",
                    error=f"канал {item.channel_id} исчез из базы — читать нечего")
                await db.commit()
                stats["failed"] += 1
                logger.error("backfill_channel_gone item=%s channel=%s",
                             item.id, item.channel_id)
                continue
            read0 = (await db.execute(
                select(func.count(Message.id))
                .where(Message.channel_id == item.channel_id))).scalar_one()
            # Поля канала снимаются в локальные переменные ЗДЕСЬ, пока сессия
            # жива: за границей блока объект отцеплен, и первый же коммит,
            # добавленный сюда позже, превратил бы обращение к нему в
            # `DetachedInstanceError` — отказ, который проявится не в тестах, а
            # на проде под нагрузкой.
            peer_id, username = channel.peer_id, channel.username
            resume = channel.backfill_cursor or 0

        # Продолжаем с того места, где остановились в прошлый раз: повторный
        # проход с начала перечитал бы уже лежащие страницы — идемпотентно, но
        # это выброшенные вызовы в дневной бюджет чтений.
        try:
            await backfill_chain.request_page(
                peer_id=peer_id, username=username,
                account_id=account_id,
                limit=backfill_chain.PAGE_LIMIT,
                target=item.target or backfill_queue.DEFAULT_TARGET,
                max_id=(resume - 1) if resume else 0, cursor=resume,
                run_id=item.run_id or 0,
                min_date=item.min_date.isoformat() if item.min_date else None,
                item_id=item.id, read0=read0)
        except engage.EngageUnavailable as e:
            # Не вина канала: вернуть в очередь и не закрывать. Проверка
            # состояния — защита от гонки с вебхуком, закрывшим элемент за время
            # нашего обращения к Engage: терминальный итог важнее нашего возврата.
            async with maker() as db:
                fresh = (await db.execute(
                    select(BackfillItem).where(BackfillItem.id == item.id)
                    .with_for_update())).scalar_one_or_none()
                if fresh is not None and fresh.state == "running":
                    fresh.state = "queued"
                    fresh.started_at = None
                    await db.commit()
            logger.warning("backfill_start_failed item=%s account=%s error=%s",
                           item.id, account_id, e)
            continue
        stats["started"] += 1
        logger.info("backfill_item_started item=%s channel=%s account=%s "
                    "target=%s read0=%s cursor=%s",
                    item.id, item.channel_id, account_id,
                    item.target, read0, resume)
    return stats


async def close_chain(db, *, item_id: int, ok: bool, reason: str,
                      read_total: int) -> None:
    """Закрыть элемент очереди по итогам цепочки страниц.

    Зовётся приёмом на остановке цепочки (`_continue_backfill`), а не самим
    тиком: только приём знает, чем закончилось чтение. Уже закрытый элемент
    (`ItemNotWaiting`) — не ошибка: повторная доставка вебхука закрывает
    цепочку второй раз, и молча проглотить её ровно то, что нужно.
    """
    try:
        await backfill_queue.finish_item(
            db, item_id, state="done" if ok else "failed",
            error=None if ok else reason, read_total=read_total)
    except backfill_queue.ItemNotWaiting as e:
        logger.info("backfill_chain_close_skipped item=%s why=%s", item_id, e)
