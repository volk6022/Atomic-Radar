"""Снять образцы ответов всех ручек, которые дёргают экраны GUI.

Зачем это существует. В `.dc`-фреймворке дырка `{{ foo }}`, которой нет в результате
`renderVals()`, не даёт никакой ошибки: ячейка просто остаётся пустой, а если не
хватило `rows` или `cols` — пустой остаётся вся таблица. Ловит это `smoke-dc.js`,
подставляя ответы API и сверяя разметку с логикой. Но проверка стоит ровно столько,
сколько стоит подставленный ответ: заглушка, написанная по памяти, подтверждает
согласованность экрана с выдумкой, а не с сервером.

Раньше такие заглушки лежали прямо в `smoke-dc.js` — тридцать с лишним ответов,
набранных руками. Один из них уже успел устареть молча: `/channels` получил
пагинацию, а в заглушке остался массив, и проверка пропускала экран, который падал
на `.filter is not a function`. Теперь ответы снимаются прогоном настоящего
приложения, а `smoke-dc.js` читает этот файл и **отказывается работать**, если для
запрошенного пути образца нет: новая ручка обязана появиться здесь, а не получить
молча чужой ответ.

Данные берутся из `scripts/seed_stand.py` — того же посева, которым наполняется
стенд, — плюс небольшой блок «остальное»: записи, которых стенду не нужно, а экранам
нужно (диалоги, ручные отправки, задачи, трейсы, тревоги, профиль). Всё через
настоящие модели и настоящие ручки.

Запуск (нужен Postgres; СХЕМА В УКАЗАННОЙ БАЗЕ БУДЕТ УДАЛЕНА И СОЗДАНА ЗАНОВО —
только тестовая база, никогда не боевая и не база стенда):

    $env:RADAR_FIXTURES_DATABASE_URL='postgresql+asyncpg://...@127.0.0.1:15434/radar_fixtures_test'
    uv run python -m scripts.dump_gui_fixtures ../brand-site/radar/api-fixtures.json
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

DB_URL = os.environ.get("RADAR_FIXTURES_DATABASE_URL")

# Посев читает свою переменную на уровне модуля, поэтому её надо выставить до
# импорта. Отдельное имя переменной, а не общий `RADAR_TEST_DATABASE_URL`, —
# защита от столкновения: полный прогон тестов идёт в своей базе и роняет схему,
# и запусти мы это одновременно в одной базе, оба получили бы мусор.
if DB_URL:
    os.environ["RADAR_STAND_DATABASE_URL"] = DB_URL
    os.environ["RADAR_DATABASE_URL"] = DB_URL
os.environ.setdefault("RADAR_SECRET_KEY", "fixtures-secret-not-for-production")

from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import select  # noqa: E402
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine  # noqa: E402

from app.core.config import get_settings  # noqa: E402
from app.core.security import SessionSigner  # noqa: E402
from app.db.models import (Account, Alert, Attribution, Conversation,  # noqa: E402
                           ConversationEvent, Draft, EngageInstance, Evaluation,
                           Lead, LlmTrace, OutboundAttempt, ProfileVersion, Run,
                           User, WfTarget, Workflow)
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.services import manual_sends  # noqa: E402

import scripts.seed_stand as seed_stand  # noqa: E402

NOW = seed_stand.NOW

# Ответ Engage снять прогоном нельзя: локального Engage нет и быть не должно.
# Поэтому подменяется **вход** — сырой ответ чужого сервиса, — а форма, которую
# увидит экран, всё равно считается настоящей ручкой. Это не то же самое, что
# написать ответ ручки руками: поменяется сериализация — образец поменяется сам.
FAKE_ENGAGE_ACCOUNTS = [
    {"account_id": 12, "phone": "+79001234455", "status": "active",
     "phone_country": "RU", "use_case": "cold_dm", "warmup_tier": 2,
     "warmup_day": 5,
     # Страна номера против страны прокси — тот самый рассинхрон, из-за которого
     # гейт Engage усыплял аккаунты. В образце он обязан быть: экран флота ради
     # него и красит клетку.
     "proxy": {"country": "DE", "type": "socks5", "is_healthy": True}},
    {"account_id": 13, "phone": "+79007654433", "status": "warmup",
     "phone_country": "RU", "use_case": "cold_dm", "warmup_tier": 1,
     "warmup_day": 2,
     "proxy": {"country": "RU", "type": "socks5", "is_healthy": True}},
]

FAKE_ENGAGE_SAFETY = {"warmup_totals": {"cold_dm": 20, "public_reply": 20}}


async def extras(db) -> None:
    """То, чего нет в посеве стенда, а экранам нужно.

    Стенд наполняется под конвейер лидов, и диалогов, задач или трейсов ему не
    нужно. Экраны при этом есть, и пустой ответ проверил бы у них только оболочку:
    ветка «есть строки» так и осталась бы ни разу не исполненной.
    """
    instance = (await db.execute(select(EngageInstance))).scalars().first()
    owner = (await db.execute(
        select(User).where(User.role == "owner"))).scalars().first()
    lead = (await db.execute(
        select(Lead).order_by(Lead.id))).scalars().first()
    draft = (await db.execute(
        select(Draft).where(Draft.lead_id == lead.id))).scalars().first()
    target = (await db.execute(
        select(WfTarget).order_by(WfTarget.id))).scalars().first()
    wf = (await db.execute(
        select(Workflow).where(Workflow.key == "cold_dm"))).scalar_one()

    account = Account(engage_account_id=12, engage_instance=instance.key,
                      label="acc-12", status="active", phone_country="RU",
                      proxy_country="DE", tz_offset=3, limit_day=20,
                      limit_hour=4, last_action_at=NOW - timedelta(hours=2),
                      watcher_uptime=99.4)
    db.add(account)
    await db.flush()

    # Диалог ссылается на строку `accounts`, а не на id аккаунта в Engage. Числа
    # разные, оба целые, и перепутать их легко — как и `chat_peer_id` в посеве.
    conversation = Conversation(lead_id=lead.id, account_id=account.id,
                                peer_id=lead.author_peer_id or 500,
                                state="awaiting_reply", sent_count=1,
                                last_sent_at=NOW - timedelta(hours=4),
                                last_inbound_at=NOW - timedelta(hours=3),
                                waiting_since=NOW - timedelta(hours=3))
    db.add(conversation)
    await db.flush()
    db.add_all([
        ConversationEvent(conversation_id=conversation.id, kind="outbound",
                          payload={"text": "Привет. Судя по описанию, дело в конфиге."},
                          created_at=NOW - timedelta(hours=4)),
        ConversationEvent(conversation_id=conversation.id, kind="inbound",
                          payload={"text": "да, спасибо, попробую"},
                          created_at=NOW - timedelta(hours=3)),
    ])

    # Попытка отправки без доставки: режим сухой, наружу ничего не ушло. Именно это
    # состояние экран и обязан показывать честно — «одобрено, но не отправлено».
    db.add(OutboundAttempt(draft_id=draft.id, conversation_id=conversation.id,
                           account_id=account.id, allowed=False, mode="DRY_RUN",
                           reasons=["сухой прогон"], delivered_message_id=None,
                           text_snapshot=(draft.variants or [{}])[0].get("text")))

    db.add_all([
        Alert(key="run_failed", severity="error", text="задача упала",
              created_at=NOW - timedelta(hours=1)),
        Alert(key="proxy_degraded", severity="warn", text="прокси отвечает медленно",
              read_at=NOW - timedelta(minutes=30),
              created_at=NOW - timedelta(hours=5)),
    ])

    db.add(Run(name="Переклассификация · недосчитанное", kind="reclassify",
               status="running", progress=30, params={"scope": "pending"},
               cancel_requested=False, created_by=owner.email,
               started_at=NOW - timedelta(minutes=19)))

    db.add(LlmTrace(stage="l3", model="qwen3.5-9b", prompt_version="v1",
                    temperature=0, prompt="Ты оцениваешь сообщение из чата.",
                    response="да, похоже на живую проблему",
                    tokens_in=800, tokens_out=120, latency_ms=900, cost_usd=0,
                    lead_id=lead.id, created_at=NOW - timedelta(minutes=10)))

    db.add(Evaluation(prompt_version="template-v0", dataset_size=40,
                      precision=0.72, recall=0.65, f1=0.68,
                      notes="первая прикидка по разобранной очереди",
                      created_at=NOW - timedelta(hours=2)))

    db.add(Attribution(ref_token="rdr-0001", lead_id=lead.id,
                       channel_id=lead.channel_id,
                       clicked_at=NOW - timedelta(days=1),
                       bot_started_at=NOW - timedelta(days=1, minutes=-4)))

    db.add(ProfileVersion(version="v1", is_active=True, created_by=owner.email,
                          business_description="Настройка VPS, VPN и прокси.",
                          pains=[{"key": "vpn", "label": "VPN не работает",
                                  "anchors": ["впн отвалился"],
                                  "prototypes": ["не могу настроить 3x-ui"]}]))

    await db.commit()

    # Ручные отправки заводятся настоящим сервисом: пара «что предложили → что
    # человек написал» и есть то, ради чего форма существует, и подделывать её
    # записью в таблицу значит проверять не тот путь.
    await manual_sends.record(db, workflow=wf, recorded_by=owner.email,
                              target_id=target.id, engage_account_id=12,
                              sent_at=NOW - timedelta(minutes=30),
                              text="Привет. Судя по описанию, дело в конфиге.")
    await manual_sends.record(db, workflow=wf, recorded_by=owner.email,
                              text="Ответил человеку из чата, которого Radar не находил")
    await db.commit()


def paths(ids: dict) -> list[tuple[str, str]]:
    """Что снимаем. Ключ — путь без параметров, он же ключ поиска в `smoke-dc.js`.

    Пути с подставным сегментом записываются с фигурными скобками (`/drafts/{id}`):
    экран подставляет туда настоящий id, а проверке важна форма ответа, а не номер.
    """
    d, t, wf_d = ids["draft_id"], ids["target_id"], ids["wf_draft_id"]
    q = f"?workflow_id={ids['workflow_id']}"
    return [
        ("/auth/me", "/auth/me"),
        ("/dashboard", "/dashboard"),
        ("/counters", "/counters"),
        ("/alerts", "/alerts"),
        ("/audit", "/audit"),
        ("/users", "/users"),
        ("/channels", "/channels"),
        ("/channels/options", "/channels/options"),
        ("/messages", "/messages?limit=5"),
        ("/leads", "/leads?limit=5"),
        ("/leads/pains", "/leads/pains"),
        ("/drafts/next", "/drafts/next"),
        ("/drafts/list", "/drafts/list?limit=5"),
        ("/drafts/reasons", "/drafts/reasons"),
        ("/drafts/{id}", f"/drafts/{d}"),
        ("/conversations", "/conversations"),
        ("/profile", "/profile"),
        ("/limits", "/limits"),
        ("/system/mode", "/system/mode"),
        ("/evaluations", "/evaluations"),
        ("/attribution", "/attribution"),
        ("/traces", "/traces?limit=5"),
        ("/runs", "/runs"),
        ("/manual-sends/form", "/manual-sends/form"),
        ("/manual-sends/candidates", f"/manual-sends/candidates{q}"),
        ("/manual-sends/list", f"/manual-sends/list{q}&limit=5"),
        ("/workflows", "/workflows"),
        ("/workflows/{key}", "/workflows/cold_dm"),
        ("/workflows/{key}/sections", "/workflows/cold_dm/sections"),
        ("/workflows/{key}/stream", "/workflows/public_reply/stream?limit=5"),
        ("/workflows/{key}/targets", "/workflows/public_reply/targets?limit=5"),
        ("/workflows/{key}/pains", "/workflows/public_reply/pains"),
        ("/workflows/{key}/drafts", "/workflows/public_reply/drafts?limit=5"),
        ("/workflows/{key}/drafts/next", "/workflows/public_reply/drafts/next"),
        ("/workflows/{key}/drafts/{id}", f"/workflows/public_reply/drafts/{wf_d}"),
    ], t


async def main() -> None:
    if not DB_URL:
        raise SystemExit(
            "нужна переменная RADAR_FIXTURES_DATABASE_URL (тестовая база — схема "
            "будет удалена и создана заново)")
    if "test" not in DB_URL.lower():
        raise SystemExit(f"в имени базы обязано быть 'test'. Получено: {DB_URL}")
    out_path = sys.argv[1] if len(sys.argv) > 1 else "api-fixtures.json"

    await seed_stand.seed()

    engine = create_async_engine(DB_URL)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        await extras(db)
        owner = (await db.execute(
            select(User).where(User.role == "owner"))).scalars().first()
        lead = (await db.execute(select(Lead).order_by(Lead.id))).scalars().first()
        draft = (await db.execute(
            select(Draft).where(Draft.lead_id == lead.id))).scalars().first()
        target = (await db.execute(
            select(WfTarget).order_by(WfTarget.id))).scalars().first()
        wf = (await db.execute(
            select(Workflow).where(Workflow.key == "cold_dm"))).scalar_one()
        ids = {"draft_id": draft.id, "target_id": target.id,
               "workflow_id": wf.id, "owner_id": owner.id, "wf_draft_id": None}
    await engine.dispose()

    get_settings.cache_clear()
    get_engine.cache_clear()
    get_session_maker.cache_clear()

    from app.api.v1 import manual_sends as manual_sends_api
    from app.main import create_app

    app = create_app()
    fixtures: dict[str, object] = {
        "_note": ("Снято прогоном настоящего приложения "
                  "(scripts/dump_gui_fixtures.py). Руками не редактировать — "
                  "перезапустить скрипт."),
    }
    failures: list[str] = []

    with TestClient(app, raise_server_exceptions=False) as c:
        token = SessionSigner(get_settings().SECRET_KEY).dumps(
            {"uid": ids["owner_id"], "totp_ok": True})
        c.cookies.set(get_settings().SESSION_COOKIE, token)

        # Очередь сценария достраивается при первом чтении — иначе следом за ней
        # нечего снимать по прямой ссылке.
        queue = c.get("/api/v1/workflows/public_reply/drafts?limit=5").json()
        ids["wf_draft_id"] = queue["rows"][0]["id"] if queue.get("rows") else 0

        wanted, _ = paths(ids)
        for key, path in wanted:
            r = c.get("/api/v1" + path)
            if r.status_code != 200:
                failures.append(f"{key}: HTTP {r.status_code} {r.text[:160]}")
                continue
            fixtures[key] = r.json()

        # Ручки, за которыми стоит Engage. Локального Engage нет и быть не должно,
        # поэтому подменяется его ответ — вход, — а форма считается настоящим кодом
        # ручки. Модуль `engage` один на всё приложение, так что подмена накрывает
        # и флот, и форму ручной отправки разом.
        engage = manual_sends_api.engage
        real_list, real_safety = engage.list_accounts, engage.safety_config
        try:
            async def fake_list(instance=None):
                return FAKE_ENGAGE_ACCOUNTS

            async def fake_safety(instance=None):
                return FAKE_ENGAGE_SAFETY

            engage.list_accounts, engage.safety_config = fake_list, fake_safety
            for key, path in (("/accounts", "/accounts"),
                              ("/manual-sends/accounts",
                               f"/manual-sends/accounts?workflow_id={ids['workflow_id']}")):
                r = c.get("/api/v1" + path)
                if r.status_code == 200:
                    fixtures[key] = r.json()
                else:
                    failures.append(f"{key}: HTTP {r.status_code} {r.text[:160]}")

            # Второй снимок той же ручки: Engage молчит. Экран обязан пережить оба
            # состояния, и «мы не смогли спросить» не то же самое, что «их нет».
            async def broken(instance=None):
                raise engage.EngageUnavailable(
                    "engage unreachable: connection refused")

            engage.list_accounts = broken
            r = c.get(f"/api/v1/manual-sends/accounts?workflow_id={ids['workflow_id']}")
            if r.status_code == 200:
                fixtures["/manual-sends/accounts [ENGAGE НЕДОСТУПЕН]"] = r.json()
        finally:
            engage.list_accounts, engage.safety_config = real_list, real_safety

    if failures:
        for f in failures:
            print("НЕ СНЯТО ->", f)
        raise SystemExit(f"не снято образцов: {len(failures)}")

    Path(out_path).write_text(
        json.dumps(fixtures, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8")
    print(f"snyato obraztsov: {len(fixtures) - 1}")
    print(f"zapisano: {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
