"""Экран «Профиль → Каскад» показывает всё, что реально исполняется: ключ
`cascade.under_the_hood` в ответе `GET /api/v1/screens/profile`.

Ключ только читает: ступени с местом в коде, все промпты реестра `llm` (не только
активный `dm_v1`) с грамматикой GBNF и шаблоном пользовательского сообщения,
зашитые в код шаблоны черновиков и текущий набор настроек (`config_files`).

Проверки ровно под требования экрана:

* `prompts` — все контуры реестра (≥ 3), у каждого непустые `system` и `grammar`;
* `draft_templates.contact` — та же константа, что проверяет политика
  (`wf_drafting.CONTACT`), а не своя копия;
* `bundle` — честное «нет набора» (`null`) либо объект с именем;
* ступени идут по порядку исполнения L0→L3, у каждой есть `where` и `text`,
  у считающих — действующие пороги и размеры словарей.

База берётся из `RADAR_TEST_DATABASE_URL`; без переменной тесты пропускаются —
ручке профиля нужна база и без набора настроек.
"""
from __future__ import annotations

import asyncio
import os

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("RADAR_SECRET_KEY", "test-secret-key-not-for-production")
os.environ.setdefault("RADAR_DEBUG", "true")

from app.core.config import get_settings  # noqa: E402
from app.core.security import SessionSigner  # noqa: E402
from app.db.models import Base, User  # noqa: E402
from app.db.session import get_engine, get_session_maker  # noqa: E402
from app.main import create_app  # noqa: E402
from app.services import drafting, llm, wf_drafting  # noqa: E402

DB_URL = os.environ.get("RADAR_TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DB_URL, reason="нет RADAR_TEST_DATABASE_URL — этим тестам нужен Postgres")

BASE = "/api/v1"


async def _seed() -> None:
    """Чистая схема и один владелец для входа. Остальное (активные версии
    таксономии, промпты, пороги по умолчанию) пишет старт приложения —
    `cascade_registry.ensure_bootstrap`, ровно как вне тестов."""
    engine = create_async_engine(DB_URL, poolclass=None)
    async with engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        await conn.run_sync(Base.metadata.create_all)

    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        db.add(User(email="owner@local", name="owner", initials="OW",
                    role="owner", password_hash="!нельзя-войти",
                    totp_secret="X" * 32, totp_confirmed=True, is_active=True))
        await db.commit()
    await engine.dispose()


@pytest.fixture
def client():
    asyncio.run(_seed())
    previous = os.environ.get("RADAR_DATABASE_URL")
    os.environ["RADAR_DATABASE_URL"] = DB_URL
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_session_maker.cache_clear()

    app = create_app()
    with TestClient(app, raise_server_exceptions=False) as c:
        c.login = lambda email: c.cookies.set(  # type: ignore[attr-defined]
            get_settings().SESSION_COOKIE,
            SessionSigner(get_settings().SECRET_KEY).dumps(
                {"uid": 1, "totp_ok": True}))
        c.login("owner@local")  # type: ignore[attr-defined]
        yield c

    if previous is None:
        os.environ.pop("RADAR_DATABASE_URL", None)
    else:
        os.environ["RADAR_DATABASE_URL"] = previous
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_session_maker.cache_clear()


def _hood(client) -> dict:
    body = client.get(f"{BASE}/profile").json()
    assert body["cascade"].get("under_the_hood"), "ключа cascade.under_the_hood нет в ответе профиля"
    return body["cascade"]["under_the_hood"]


def test_under_the_hood_prompts_all_keys_with_system_and_grammar(client):
    """Все промпты реестра `llm`, а не только активный `dm_v1`: у каждого —
    полный текст вопроса и рабочая грамматика ответа."""
    prompts = _hood(client)["prompts"]

    keys = [p["key"] for p in prompts]
    registry = llm.prompt_keys()
    assert len(keys) >= 3, f"в реестре {registry}, а в ответе {keys}"
    assert {"dm_v1", "public_v1", "channel_fit_v1"} <= set(keys)
    for p in prompts:
        assert p["system"], f"у {p['key']} пустой system"
        assert p["grammar"], f"у {p['key']} не построилась грамматика GBNF"
        assert p["version"], f"у {p['key']} нет версии"
        assert p["used_for"], f"у {p['key']} не указано, для чего он"
        assert p["user_template"], f"у {p['key']} не показан шаблон сообщения"


def test_under_the_hood_draft_templates_use_wf_drafting_contact(client):
    """Контакт в ответе — та же константа, по которой `wf_drafting.lint` решает,
    уместен ли он публично; обе своя копия завела бы — и разъехались бы молча."""
    drafts = _hood(client)["draft_templates"]

    assert drafts["contact"] == wf_drafting.CONTACT
    assert drafts["contact_allowed_pain"] == wf_drafting.ASKS_FOR_CONTRACTOR
    assert drafts["prompt_version"] == drafting.PROMPT_VERSION
    # Зашитость — прямым текстом, чтобы экран не изображал генерацию моделью.
    assert "не настраиваются" in drafts["note"]
    # Каждая боль с шаблонами и запасной путь на месте, тексты не пустые.
    assert drafts["dm"]["_fallback"] == list(drafting.FALLBACK)
    assert drafts["public"]["_fallback"] == list(wf_drafting.PUBLIC_FALLBACK)
    for group in (drafts["dm"], drafts["public"]):
        for pain, texts in group.items():
            if pain == "_fallback":
                continue
            assert texts, f"в группе без текстов: боль «{pain}»"


def test_under_the_hood_bundle_is_null_or_named(client):
    """Набора может не быть — тогда честный `null`; если есть, у него есть имя."""
    bundle = _hood(client)["bundle"]
    assert bundle is None or bundle.get("name"), \
        "bundle без имени не отвечает на вопрос «чем настраивали»"


def test_under_the_hood_stages_follow_execution_order(client):
    """Ступени L0→L3 по порядку исполнения, у каждой — место в коде и описание;
    у считающих ступеней — действующие пороги (строки `limits` поверх кода) и
    непустые словари, у L3 — те же параметры, с которыми зовут модель."""
    stages = _hood(client)["stages"]

    assert [s["key"] for s in stages] == ["l0", "l1", "l2", "l3"]
    for s in stages:
        assert s["title"], f"у {s['key']} нет названия"
        assert s["where"] and ":" in s["where"], f"у {s['key']} нет where"
        assert s["text"], f"у {s['key']} пустое описание"

    l1 = stages[1]["params"]
    assert 0 < l1["l1_bypass_pos_min"] < 1
    assert l1["anchors_total"] > 0
    assert l1["disqualifiers"] > 0

    l2 = stages[2]["params"]
    assert 0 < l2["l2_min_margin"] < 1
    assert l2["positive_prototypes"] > 0
    assert l2["negative_prototypes"] > 0

    l3 = stages[3]["params"]
    assert l3["temperature"] == 0.0
    assert l3["concurrency"] >= 1, "эффективное число слотов модели"
    assert l3["max_tokens"] > 0
