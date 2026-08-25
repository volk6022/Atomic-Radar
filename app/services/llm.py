"""Клиент домашней LLM (Qwen3.5-9B) для ступени L3.

Модель рассуждающая: перед ответом она пишет размышление, и это меняет две вещи по
сравнению с обычным chat-эндпоинтом.

* **Ответ приходит не только в `content`.** llama.cpp кладёт размышление либо в
  `reasoning_content`, либо прямо в `content` внутри `<think>…</think>`. Если считать
  ответом только `content` и не резать теги, на выходе регулярно оказывается пустая
  строка — на этом уже обжигались в соседнем сервисе.
* **Лимит токенов должен быть щедрым.** Размышление съедает бюджет первым, и при
  тесном лимите модель обрывается ровно там, где собиралась ответить.

JSON вытаскивается из текста, а не ожидается в чистом виде: модель любит обрамить его
пояснением или ```json-блоком, и требовать идеального форматирования от локальной
9B — способ терять ответы на пустом месте.
"""
from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping

import httpx

from app.core.config import get_settings

logger = logging.getLogger(__name__)

_client: httpx.AsyncClient | None = None

_THINK = re.compile(r"<think>.*?</think>", re.S)
_JSON_BLOCK = re.compile(r"\{.*\}", re.S)


class LlmUnavailable(RuntimeError):
    """Модель не ответила. Текст уходит в объяснение ступени как есть."""


def enabled() -> bool:
    return bool(get_settings().LLM_BASE_URL)


def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        s = get_settings()
        if not s.LLM_BASE_URL:
            raise LlmUnavailable("адрес LLM не задан (RADAR_LLM_BASE_URL)")
        _client = httpx.AsyncClient(base_url=s.LLM_BASE_URL.rstrip("/"),
                                    timeout=httpx.Timeout(s.LLM_TIMEOUT, connect=10.0))
    return _client


async def close() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


async def ping() -> str:
    """Статус для плитки сервисов: `ok` | `off` | текст ошибки. Таймаут короткий —
    дашборд не должен ждать столько же, сколько ждёт рассуждающая модель."""
    if not enabled():
        return "off"
    try:
        r = await _get_client().get("/health", timeout=httpx.Timeout(4.0, connect=3.0))
    except httpx.HTTPError as e:
        return f"нет связи: {type(e).__name__}"
    return "ok" if r.status_code < 400 else f"ответ {r.status_code}"


@dataclass(frozen=True)
class Prompt:
    """Вопрос к модели на L3 — свой у каждого контура.

    Раньше промпт был один на всё (`SYSTEM`), и ответ по одному сообщению годился
    для любого сценария. Это решение отменено осознанно, 25.08: контуры должны быть
    независимы в своих шагах, и в первую очередь именно здесь. Мы на стадии
    разработки, и поведение модели при таком совмещении не измерено — а цена ошибки
    в том, что вопрос, заданный про личное сообщение, молча определял бы отбор для
    публичного ответа. Лишнее время карты дешевле.

    `version` попадает в `llm_traces.prompt_version`: по нему видно, каким вопросом
    получен конкретный вердикт, и правка текста не смешивается со старыми ответами.
    """

    key: str
    version: str
    system: str


DM_SYSTEM = (
    "Ты помогаешь отбирать людей, которым нужна помощь с хостингом, VPS, VPN и "
    "настройкой серверов. Тебе дают одно сообщение из публичного чата Telegram и "
    "соседние сообщения для контекста.\n\n"
    "Твоя работа — наблюдение, а не решение. Ты НЕ решаешь, писать человеку или нет; "
    "ты отвечаешь, что видно в тексте.\n\n"
    "Ответь ТОЛЬКО объектом JSON, без пояснений вокруг:\n"
    '{"real_problem": true|false, "is_seller": true|false, '
    '"answering_someone_else": true|false, "urgency": "low"|"medium"|"high", '
    '"pain": "<короткое имя проблемы или null>", "why": "<одно предложение по-русски>"}\n\n'
    "real_problem — у автора СЕЙЧАС есть неудобство, поломка или задача, которую он не "
    "может решить сам. Праздный вопрос, спор о технологиях, шутка — это false.\n"
    "is_seller — автор сам предлагает такие услуги или рекламирует свой продукт.\n"
    "answering_someone_else — автор отвечает на чужой вопрос, помогает другому; "
    "тогда проблема не его.\n"
    "why — почему ты так решил, одним предложением, по-русски."
)

DM_V1 = Prompt(key="dm_v1", version="l3-verdict-v1", system=DM_SYSTEM)

PROMPTS: Mapping[str, Prompt] = MappingProxyType({DM_V1.key: DM_V1})

DEFAULT_PROMPT = DM_V1.key


class UnknownPromptError(ValueError):
    """В профиле каскада стоит ключ промпта, которого в коде нет."""

    def __init__(self, key: str) -> None:
        super().__init__(f"промпт L3 «{key}» не найден; известны: "
                         f"{', '.join(sorted(PROMPTS))}")
        self.key = key


def prompt(key: str) -> Prompt:
    """Промпт по ключу из профиля каскада.

    Падаем, а не подставляем вопрос по умолчанию: контур с чужим промптом спрашивал
    бы модель не о том, и заметили бы это по странному отбору через неделю, а не по
    ошибке при запуске.
    """
    try:
        return PROMPTS[key]
    except KeyError:
        raise UnknownPromptError(key) from None


def build_prompt(*, text: str, context: list[str]) -> str:
    ctx = "\n".join(f"- {c}" for c in context) if context else "(контекста нет)"
    return (f"Соседние сообщения в чате:\n{ctx}\n\n"
            f"Разбираемое сообщение:\n«{text}»")


def _extract(payload: dict) -> str:
    """Достать содержательную часть ответа, чем бы её ни назвала модель."""
    choice = (payload.get("choices") or [{}])[0]
    msg = choice.get("message") or {}
    content = (msg.get("content") or "").strip()
    if not content:
        # Целиком ушло в размышление — значит, ответ надо искать там.
        content = (msg.get("reasoning_content") or "").strip()
    return _THINK.sub("", content).strip()


def parse_verdict(raw: str) -> dict:
    """Вытащить объект JSON из свободного текста модели."""
    m = _JSON_BLOCK.search(raw)
    if not m:
        return {"error": "в ответе нет JSON", "raw": raw[:400]}
    try:
        data = json.loads(m.group(0))
    except json.JSONDecodeError as e:
        return {"error": f"JSON не разобрался: {e}", "raw": raw[:400]}
    if not isinstance(data, dict):
        return {"error": "JSON есть, но это не объект", "raw": raw[:400]}
    return data


async def verdict(*, text: str, context: list[str],
                  prompt_key: str = DEFAULT_PROMPT) -> tuple[dict, dict]:
    """Вердикт модели по одному сообщению в вопросе одного контура.

    Возвращает (разобранный ответ, трейс).
    """
    s = get_settings()
    asked = prompt(prompt_key)
    user_text = build_prompt(text=text, context=context)
    body = {
        "model": s.LLM_MODEL,
        "messages": [{"role": "system", "content": asked.system},
                     {"role": "user", "content": user_text}],
        # Ноль, а не «поменьше»: одно и то же сообщение обязано получать один и тот же
        # вердикт, иначе перезапуск разметки меняет выборку и калибровать нечего.
        "temperature": 0.0,
        "max_tokens": s.LLM_MAX_TOKENS,
    }

    started = time.monotonic()
    try:
        r = await _get_client().post("/v1/chat/completions", json=body)
    except httpx.HTTPError as e:
        raise LlmUnavailable(f"LLM недоступна: {type(e).__name__}") from e
    latency_ms = int((time.monotonic() - started) * 1000)

    if r.status_code >= 400:
        raise LlmUnavailable(f"LLM ответила {r.status_code}: {r.text[:200]}")

    payload = r.json()
    raw = _extract(payload)
    parsed = parse_verdict(raw)
    usage = payload.get("usage") or {}

    trace = {
        "stage": "l3", "model": s.LLM_MODEL, "prompt_version": asked.version,
        "temperature": 0.0, "prompt": user_text, "response": raw[:4000],
        "tokens_in": usage.get("prompt_tokens"), "tokens_out": usage.get("completion_tokens"),
        "latency_ms": latency_ms,
        # Своя модель на своей карте: денежной стоимости у вызова нет, и писать сюда
        # выдуманный прайс OpenAI значило бы испортить отчёт по себестоимости лида.
        "cost_usd": 0,
    }
    return parsed, trace
