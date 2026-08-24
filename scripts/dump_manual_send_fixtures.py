"""Снять образцы ответов ручек ручной отправки — для заглушек GUI.

Зачем это отдельный скрипт, а не файл, написанный руками. В `.dc`-фреймворке дырка
`{{ foo }}`, которой нет в результате `renderVals()`, не даёт никакой ошибки: ячейка
просто остаётся пустой. `smoke-dc.js` ловит это, сверяя разметку с логикой — но только
если заглушка повторяет **настоящий** ответ. Заглушка, написанная по памяти, делает
проверку бессмысленной: она подтверждает, что экран согласован с выдумкой.

Поэтому образцы снимаются прогоном настоящего кода сервиса по настоящей схеме. Формат
ответа меняется — перезапустили скрипт, и расхождение видно сразу.

Запуск (нужен Postgres, СХЕМА В НЁМ БУДЕТ УДАЛЕНА И СОЗДАНА ЗАНОВО — только тестовая
база, никогда не боевая):

    RADAR_TEST_DATABASE_URL=postgresql+asyncpg://... \\
        python -m scripts.dump_manual_send_fixtures out.json
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import datetime, timedelta, timezone

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db.models import (Base, Channel, EngageInstance, Message, WfDraft,
                           WfTarget, Workflow)
from app.services import manual_sends, workflows

# Отсчёт от настоящего «сейчас», а не от зашитой даты: запись отправки в будущем
# сервис не принимает, и зашитая дата ломала бы скрипт ровно до полудня.
NOW = datetime.now(timezone.utc).replace(microsecond=0)

# Боевой адрес сюда попасть не должен ни при каких обстоятельствах: скрипт удаляет
# схему целиком. Пускаем только по явной тестовой переменной.
DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")


async def seed(db) -> dict:
    instance = EngageInstance(key="default", client_label="Основной",
                              base_url="http://engage:8103",
                              api_key_env="RADAR_ENGAGE_API_KEY")
    db.add(instance)
    channel = Channel(peer_id=-1001, username="vpsclub", title="VPS Club")
    db.add(channel)
    await db.flush()

    wf = Workflow(key="cold_dm", title="Личные сообщения", target_kind="user",
                  action="dm", visibility="private", engage_instance_id=instance.id,
                  engage_use_case="cold_dm", cascade_profile="dm_v1", sort_order=10,
                  description="Найти человека с болью и написать ему в личные сообщения.")
    db.add(wf)

    texts = [
        ("впн отвалился второй день, не могу настроить 3x-ui, помогите", "VPN не работает"),
        ("хостинг тормозит, ищу куда переехать, посоветуйте vps", "хостинг тормозит/дорог"),
    ]
    targets = []
    for i, (body, pain) in enumerate(texts):
        m = Message(channel_id=channel.id, tg_message_id=1000 + i,
                    tg_date=NOW - timedelta(hours=i + 1), author_peer_id=500 + i,
                    author_username=f"user{i}", author_name=["Игорь С.", "Пётр К."][i],
                    author_is_bot=False, is_automatic_forward=False, text=body)
        db.add(m)
        await db.flush()
        t = WfTarget(workflow_id=wf.id, target_kind="user", message_id=m.id,
                     channel_id=channel.id, recipient_peer_id=m.author_peer_id,
                     author_peer_id=m.author_peer_id, author_username=m.author_username,
                     author_name=m.author_name, pain=pain, quote=body,
                     score=[62, 48][i],
                     score_breakdown=[{"label": "совпадение с болью", "value": 22},
                                      {"label": "срочность/интент", "value": 24},
                                      {"label": "признаки ЛПР", "value": 0},
                                      {"label": "свежесть", "value": 10},
                                      {"label": "достижимость в ЛС", "value": 6}],
                     status="new")
        db.add(t)
        targets.append(t)
    await db.flush()

    db.add(WfDraft(workflow_id=wf.id, target_id=targets[0].id,
                   variants=[{"text": "Привет! Видел твой вопрос про 3x-ui."},
                             {"text": "Привет. Судя по описанию, дело в конфиге."}],
                   chosen_variant=1, state="pending", prompt_version="template-v0"))
    await db.commit()
    return {"wf": wf, "targets": targets}


async def main() -> None:
    if not DB_URL:
        raise SystemExit("нужен RADAR_TEST_DATABASE_URL (тестовая база — схема будет "
                         "удалена и создана заново)")
    out_path = sys.argv[1] if len(sys.argv) > 1 else "manual-sends-fixtures.json"

    engine = create_async_engine(DB_URL)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)

    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        seeded = await seed(db)
        wf, targets = seeded["wf"], seeded["targets"]

        active = await workflows.active(db)
        form = {"workflows": [{"id": w.id, "key": w.key, "title": w.title,
                               "action": w.action, "visibility": w.visibility}
                              for w in active],
                "default_workflow_id": active[0].id if active else None}

        candidates = {"rows": await manual_sends.candidates(db, workflow=wf)}

        # Две записи: одна повторяет предложенное дословно, вторая переписана руками —
        # экран обязан различать эти случаи, и в заглушке должны быть обе.
        await manual_sends.record(
            db, workflow=wf, recorded_by="andrey@vertsanov.ru",
            target_id=targets[0].id, engage_account_id=12, sent_at=NOW,
            text="Привет. Судя по описанию, дело в конфиге.")
        await manual_sends.record(
            db, workflow=wf, recorded_by="andrey@vertsanov.ru",
            target_id=targets[1].id, engage_account_id=12,
            sent_at=NOW - timedelta(minutes=40),
            text="Привет! Могу помочь с переездом, у меня свои сервера.",
            note="написал по-своему, шаблон слишком формальный")
        await manual_sends.record(
            db, workflow=wf, recorded_by="andrey@vertsanov.ru",
            text="Ответил человеку из чата, которого Radar не находил")
        await db.commit()

        history = await manual_sends.history(db, workflow_id=wf.id)

    await engine.dispose()

    fixtures = {
        "_note": ("Снято прогоном настоящего кода сервиса "
                  "(scripts/dump_manual_send_fixtures.py). Не редактировать руками — "
                  "перезапустить скрипт."),
        "GET /api/v1/manual-sends/form": form,
        "GET /api/v1/manual-sends/candidates?workflow_id=1": candidates,
        "GET /api/v1/manual-sends/list?workflow_id=1": history,
        # Единственный ответ, который снять прогоном нельзя: он приходит из Engage.
        # Форма его вида зафиксирована в ручке; отмечено явно, чтобы никто не считал
        # эти строки снятыми с живого сервиса.
        "GET /api/v1/manual-sends/accounts?workflow_id=1 [СОБРАНО ПО КОДУ РУЧКИ]": {
            "available": True, "reason": None,
            "rows": [{"id": 12, "label": "acc-12", "status": "active",
                      "use_case": "cold_dm"},
                     {"id": 13, "label": "acc-13", "status": "warmup",
                      "use_case": "cold_dm"}],
        },
        "GET /api/v1/manual-sends/accounts — Engage недоступен": {
            "available": False,
            "reason": "engage unreachable: connection refused", "rows": []},
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(fixtures, f, ensure_ascii=False, indent=2)
    print(f"записано: {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
