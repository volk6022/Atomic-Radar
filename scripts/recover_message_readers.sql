-- Восстановление атрибуции приёма за период до 02.09.2026.
--
-- ЗАЧЕМ. `message_readers` («какой аккаунт видел это сообщение») появилась
-- 02.09 вместе с требованием заказчика от 29.08: «берем аккаунт который прочитал
-- сообщение и я от его имени пишу». Всё, что приехало раньше, атрибуции не имело
-- — а целями сценариев к 03.09 становились именно старые сообщения бэкфилла.
-- В итоге у всех 102 черновиков аккаунт был пуст, и фильтр «показать черновики
-- аккаунта, под которым я сейчас сижу» показывал пустоту при исправном коде.
--
-- ОТКУДА БЕРЁТСЯ ИСТИНА. Не из Радара. В Радаре есть только `runs.params`
-- (кого попросили прочитать) — этого мало: прогоны заведены с 28.08, покрывают
-- 85 % сообщений и 66 черновиков из 102, а первые выгрузки 11-12.08 и массовый
-- разбор 28.08 прогонов не оставили вовсе.
--
-- Истина лежит в Engage, в его же журнале задач: `tasks` с `task_type =
-- 'get_chat_history'` хранит `account_id`, имя группы в `payload.username` и —
-- главное — `result.posts[]` с `message_id` каждого реально прочитанного
-- сообщения. Это не догадка по времени, а список, который аккаунт привёз.
--
-- ⚠️ НЕ ПОДБИРАТЬ АККАУНТ ПО ТЕКУЩЕМУ ЧИТАТЕЛЮ КАНАЛА. Соблазн есть: сейчас у
-- каждого канала ровно один читатель. Но раздача аккаунтов по каналам не
-- закреплена — сверка 03.09 дала совпадение с августовскими прогонами лишь в
-- 25 случаях из 106, а @kvtchannel в разные дни читали аккаунты 5 и 1.
--
-- ЖИВОЙ ПОТОК — ВО ВТОРОМ ЖУРНАЛЕ. Под пуш-апдейты вотчера задача не заводится,
-- и в `tasks` их нет. Но есть `webhook_deliveries`: у каждой доставки на ручку
-- приёма Радара в `payload` лежат `account_id`, `chat_id` и `message_id`. Ключ
-- там другой — не имя группы, а `chat_id` против `channels.peer_id`. Запрос
-- в разделе 5 ниже; он добирает остаток и закрывает покрытие до 100 %.
--
-- ПРОВЕРКА КЛЮЧА (делать до вставки, не после). Стыковка идёт по паре
-- (имя группы, номер сообщения в Telegram). На 3000 строках выборки текст
-- сошёлся во всех 3000 (2094 из них непустые), автор — во всех 3000.
--
-- ОТКАТ. Восстановленные строки отделяются от живых по времени без всякой
-- пометки: у восстановленных `first_seen_at` — момент чтения по журналам Engage,
-- у живых — не раньше 2026-09-02 15:18:14Z (см. раздел 5).
--     delete from message_readers where first_seen_at < '2026-09-02 15:18:14+00';
--
-- КАК ЗАПУСКАТЬ (обе базы — контейнеры на одном хосте; сначала pg_dump Радара):
--
--   docker compose exec -T postgres psql -U radar_user -d radar_db -f - <<'SQL'
--   <первая половина этого файла: создание _recover_readers_stage>
--   SQL
--
--   docker exec postgres-vertsanov psql -U client_vertsanov_user \
--     -d client_vertsanov_db -t -A -F',' -c "$(sed -n '/^-- ЗАПРОС К ENGAGE/,/^-- КОНЕЦ/p' ...)" \
--   | docker exec -i postgres-radar psql -U radar_user -d radar_db \
--       -c "\copy _recover_readers_stage from stdin with (format csv)"
--
--   <вторая половина: вставка и уборка>


-- ── 1. приёмник на стороне Радара ────────────────────────────────────────────

drop table if exists _recover_readers_stage;
create table _recover_readers_stage(
    account_id    bigint,
    grp           text,      -- имя группы обсуждения, как его знает Engage
    tg_message_id bigint,
    seen_at       timestamptz
);


-- ── 2. ЗАПРОС К ENGAGE (выполняется в базе клиента, выгрузка в CSV) ──────────
--
-- `status='complete'` обязателен: у оборванной задачи `result` пуст или содержит
-- часть страницы, и такие строки утверждали бы, что аккаунт видел то, чего не
-- привозил. `min(...)` — потому что одну и ту же страницу могли перечитать, а
-- колонка называется «первый раз увидел».
--
--   select account_id, lower(payload->>'username'), (p->>'message_id')::bigint,
--          min(coalesce(started_at, created_at))
--   from tasks, jsonb_array_elements(coalesce(result->'posts','[]'::jsonb)) p
--   where task_type = 'get_chat_history'
--     and payload ? 'username'
--     and status = 'complete'
--     and p ? 'message_id'
--   group by 1, 2, 3;
--
-- КОНЕЦ ЗАПРОСА К ENGAGE


-- ── 3. вставка ───────────────────────────────────────────────────────────────

create index on _recover_readers_stage(grp, tg_message_id);
analyze _recover_readers_stage;

-- `on conflict do nothing`, а не upsert: живые строки, записанные приёмом с
-- 02.09, — источник более точный, чем журнал, и перетирать их временем из
-- задачи нельзя.
insert into message_readers (message_id, account_id, first_seen_at)
select m.id, s.account_id, min(s.seen_at)
from _recover_readers_stage s
join channels c on lower(c.username) = s.grp
join messages m on m.channel_id = c.id and m.tg_message_id = s.tg_message_id
group by 1, 2
on conflict do nothing;

drop table _recover_readers_stage;


-- ── 4. что получилось 03.09.2026 ─────────────────────────────────────────────
--
--   вставлено строк                    102 086
--   сообщений с атрибуцией             102 132 из 103 839  (98,4 %)
--   черновиков с атрибуцией                102 из 102      (100 %)
--   из них с двумя читателями                3   — группу читали два аккаунта,
--                                                 и список это честно показывает


-- ── 5. добор живого потока из журнала доставок ───────────────────────────────
--
-- Выгрузка со стороны Engage (только события вотчера — у них нет строки запроса):
--
--   select (payload->>'account_id')::bigint, (payload->>'chat_id')::bigint,
--          (payload->>'message_id')::bigint, min(created_at)
--   from webhook_deliveries
--   where url like '%api-radar%' and url not like '%?%'
--     and payload->>'event' = 'incoming_message'
--     and payload ? 'account_id' and payload ? 'message_id'
--   group by 1, 2, 3;
--
-- Приёмник и вставка на стороне Радара — то же самое, но ключ по `peer_id`:
--
--   create table _recover_rt_stage(account_id bigint, chat_id bigint,
--                                  tg_message_id bigint, seen_at timestamptz);
--   ...
--   insert into message_readers (message_id, account_id, first_seen_at)
--   select m.id, s.account_id, min(s.seen_at)
--   from _recover_rt_stage s
--   join channels c on c.peer_id = s.chat_id
--   join messages m on m.channel_id = c.id and m.tg_message_id = s.tg_message_id
--   group by 1, 2
--   on conflict do nothing;
--
-- Результат второго захода 03.09:
--
--   стыковок                             2 125 из 2 150 выгруженных
--   текст сошёлся                        2 125 из 2 125  (1 594 непустых)
--   вставлено строк                      1 707
--   сообщений с атрибуцией             103 851 из 103 851  (100 %)
--
-- Провал закрылся ровно там, где включился боевой путь: последняя
-- восстановленная запись — 02.09 15:15:37Z, первая живая — 02.09 15:18:14Z.
-- Отсюда и точная граница отката для ОБОИХ заходов:
--
--   delete from message_readers where first_seen_at < '2026-09-02 15:18:14+00';
