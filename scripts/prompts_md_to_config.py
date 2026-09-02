#!/usr/bin/env python3
"""Разбор рабочего Markdown с промптами в JSON-конфигурацию Радара.

Иван держит все настройки отбора в одном Markdown-файле и правит их там. Формат
обмена с Радаром — JSON: его же можно выгрузить, отредактировать снаружи и залить
обратно. Этот скрипт — мост между тем и другим.

    python scripts/prompts_md_to_config.py <input.md> [-o config.json] [--name имя]

Разбирается ровно та структура, которая в файле есть, и ничего сверх неё:

    # Бизнес
    ## Описание бизнеса заказчика        -> business.description (первый блок кода)
    # Боли и шум
    ## <название боли>
    ### L1 · слова-якоря (N)             -> pains[боль].anchors   (строка через запятую)
    ### L2 · эталонные фразы (N)         -> pains[боль].prototypes (строки с «—»)
    ## Что система считает шумом
    ### <категория шума>                 -> noise[категория]      (строки с «—»)
    ### Дисквалификаторы (...)           -> disqualifiers[метка]  («метка: слова»)
    # Каскад
    <блок кода после «Системный промпт L3»> -> l3_prompts.dm_v1

⚠️ Разбор строгий и шумный намеренно. Молча потерянная боль или молча пустой список
эталонов — это не сломанный импорт, а тихо испортившийся отбор: система продолжит
работать и будет находить не то. Поэтому скрипт всегда печатает, что именно нашёл, и
падает, если не нашёл ничего.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

# Заголовки внутри «шума», которые категориями НЕ являются.
NOISE_SKIP = re.compile(r"^L2 сравнивает|^Дисквалификаторы")
DISQ_HEADING = re.compile(r"^Дисквалификаторы")
BULLET = re.compile(r"^[—–-]\s*(.+)$")
L1_HEADING = re.compile(r"^L1\b")
L2_HEADING = re.compile(r"^L2\b")
PROMPT_MARKER = re.compile(r"Системный промпт\s+L3", re.IGNORECASE)


class ParseError(RuntimeError):
    pass


def _blocks(text: str) -> list[tuple[int, str, list[str]]]:
    """Файл как список (уровень, заголовок, строки до следующего заголовка)."""
    out: list[tuple[int, str, list[str]]] = []
    current: tuple[int, str, list[str]] | None = None
    for line in text.splitlines():
        m = re.match(r"^(#{1,6})\s+(.*)$", line)
        if m:
            if current:
                out.append(current)
            current = (len(m.group(1)), m.group(2).strip(), [])
        elif current:
            current[2].append(line)
    if current:
        out.append(current)
    return out


def _bullets(lines: list[str]) -> list[str]:
    return [m.group(1).strip() for ln in lines if (m := BULLET.match(ln.strip()))]


def _fenced(lines: list[str]) -> list[str]:
    """Содержимое ```-блоков, каждый одной строкой."""
    out, buf, inside = [], [], False
    for ln in lines:
        if ln.strip().startswith("```"):
            if inside:
                out.append("\n".join(buf).strip())
                buf = []
            inside = not inside
            continue
        if inside:
            buf.append(ln)
    if inside and buf:
        out.append("\n".join(buf).strip())
    return out


def _backticked(lines: list[str]) -> str | None:
    """Однострочный `текст в обратных кавычках` — так записано описание бизнеса."""
    joined = "\n".join(lines).strip()
    m = re.search(r"`([^`]{20,})`", joined, re.DOTALL)
    return m.group(1).strip() if m else None


def parse(text: str) -> dict:
    blocks = _blocks(text)
    if not blocks:
        raise ParseError("в файле нет ни одного заголовка — это не тот формат")

    business: str | None = None
    pains: dict[str, dict] = {}
    noise: dict[str, list[str]] = {}
    disqualifiers: dict[str, list[str]] = {}
    l3_prompt: str | None = None

    section = None       # текущий раздел первого уровня
    pain_label = None    # текущая боль (## внутри «Боли и шум»)
    in_noise = False
    saw_prompt_marker = False

    for level, title, lines in blocks:
        if level == 1:
            section = title
            in_noise = False
            pain_label = None
            if PROMPT_MARKER.search("\n".join(lines)):
                saw_prompt_marker = True
                fenced = _fenced(lines)
                if fenced:
                    l3_prompt = fenced[-1]
            continue

        if level == 2:
            if "Описание бизнеса" in title:
                business = _backticked(lines) or "\n".join(lines).strip() or None
                continue
            if "шум" in title.lower():
                in_noise = True
                pain_label = None
                continue
            if section and "Бол" in section:
                in_noise = False
                pain_label = title
                pains.setdefault(pain_label, {"anchors": [], "prototypes": []})
            continue

        if level == 3:
            if in_noise:
                if DISQ_HEADING.match(title):
                    for item in _bullets(lines):
                        label, _, words = item.partition(":")
                        if not words.strip():
                            raise ParseError(
                                f"дисквалификатор без списка слов: «{item[:60]}»")
                        disqualifiers[label.strip()] = [
                            w.strip() for w in words.split(",") if w.strip()]
                    continue
                if NOISE_SKIP.match(title):
                    continue
                got = _bullets(lines)
                if got:
                    noise[title] = got
                continue

            if pain_label is None:
                continue
            if L1_HEADING.match(title):
                joined = " ".join(ln.strip() for ln in lines if ln.strip())
                pains[pain_label]["anchors"] = [
                    w.strip() for w in joined.split(",") if w.strip()]
            elif L2_HEADING.match(title):
                pains[pain_label]["prototypes"] = _bullets(lines)

    if saw_prompt_marker and not l3_prompt:
        raise ParseError("заголовок системного промпта L3 есть, а ```-блока под ним нет")

    return {"business": {"description": business},
            "pains": pains, "noise": noise, "disqualifiers": disqualifiers,
            "l3_prompts": {"dm_v1": l3_prompt} if l3_prompt else {}}


def check(cfg: dict) -> list[str]:
    """Претензии к разобранному. Пустой список — можно грузить."""
    bad: list[str] = []
    if not cfg["business"]["description"]:
        bad.append("описание бизнеса не найдено")
    if not cfg["pains"]:
        bad.append("не найдено ни одной боли")
    for label, body in cfg["pains"].items():
        if not body["anchors"]:
            bad.append(f"боль «{label}»: нет якорей L1")
        if not body["prototypes"]:
            bad.append(f"боль «{label}»: нет эталонных фраз L2")
    if not cfg["noise"]:
        bad.append("не найдено ни одной категории шума")
    if not cfg["l3_prompts"]:
        bad.append("не найден системный промпт L3")
    return bad


def warnings(cfg: dict) -> list[str]:
    """То, что не ломает загрузку, но почти наверняка не сработает как задумано."""
    out: list[str] = []
    # Каскад сворачивает «ё» в «е» при сравнении, поэтому якорь с «ё» не совпадёт
    # никогда: человек напишет «счёт», а сравнение идёт по «счет».
    for label, body in cfg["pains"].items():
        with_yo = [w for w in body["anchors"] if "ё" in w or "Ё" in w]
        if with_yo:
            out.append(f"боль «{label}»: якоря с «ё» не сработают никогда "
                       f"(сравнение сворачивает ё→е): {', '.join(with_yo)}")
    return out


def main() -> int:
    # Кодировка вывода задаётся явно. На этой машине системная ANSI — cp1252, и
    # print с кириллицей падает с UnicodeEncodeError, стоит запустить скрипт не из
    # той оболочки. Отчёт о разборе — единственное, по чему видно, что разобралось
    # верно, и терять его из-за кодировки нельзя.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")

    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("input", type=Path)
    ap.add_argument("-o", "--output", type=Path)
    ap.add_argument("--name", default=None, help="имя набора настроек")
    args = ap.parse_args()

    text = args.input.read_text(encoding="utf-8")
    try:
        cfg = parse(text)
    except ParseError as e:
        print(f"РАЗБОР НЕ УДАЛСЯ: {e}", file=sys.stderr)
        return 2

    cfg = {"format": "atomic-radar-config", "version": 1,
           "name": args.name or args.input.stem,
           "exported_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           **cfg}

    print(f"описание бизнеса: {len(cfg['business']['description'] or '')} символов")
    print(f"боли: {len(cfg['pains'])}")
    for label, body in cfg["pains"].items():
        print(f"  · {label}: якорей {len(body['anchors'])}, "
              f"эталонов {len(body['prototypes'])}")
    print(f"категории шума: {len(cfg['noise'])}")
    for label, items in cfg["noise"].items():
        print(f"  · {label}: {len(items)}")
    print(f"дисквалификаторы: {len(cfg['disqualifiers'])}")
    for label, words in cfg["disqualifiers"].items():
        print(f"  · {label}: {len(words)}")
    for key, body in cfg["l3_prompts"].items():
        print(f"промпт L3 «{key}»: {len(body)} символов")

    for w in warnings(cfg):
        print(f"ВНИМАНИЕ: {w}", file=sys.stderr)

    bad = check(cfg)
    if bad:
        print("\nНЕ ГОДИТСЯ ДЛЯ ЗАГРУЗКИ:", file=sys.stderr)
        for b in bad:
            print(f"  - {b}", file=sys.stderr)
        return 1

    if args.output:
        args.output.write_text(json.dumps(cfg, ensure_ascii=False, indent=2),
                               encoding="utf-8")
        print(f"\nзаписано: {args.output}")
    else:
        print(json.dumps(cfg, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
