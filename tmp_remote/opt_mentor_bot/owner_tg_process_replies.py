from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import os
import time
import re
import sys
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.tg_proxy import build_client_kwargs

ACCOUNT_TYPE_STANDARD = "standard"
ACCOUNT_TYPE_POLYGON = "polygon"
SITE_URL = "https://leonovcare.ru/index.html"
BOT_HANDLE = "@Leonov_Care_bot"
MENTOR_CHANNEL_URL = "https://t.me/olegleonoff"
PRIMARY_DIRECTIONS = {"java", "frontend", "golang", "python"}


def _session_locked(exc: Exception) -> bool:
    return "database is locked" in str(exc or "").lower()


def _cleanup_session_locks(session_file: str) -> None:
    base = Path(str(session_file)).expanduser().resolve()
    for suffix in ("-journal", "-wal", "-shm"):
        p = Path(str(base) + suffix)
        try:
            if p.exists():
                p.unlink()
        except Exception:
            pass


def _load_json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _norm_text(text: str | None) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip().lower())


def _strip_emoji(text: str) -> str:
    # Убираем emoji/symbol pictographs, чтобы ответы выглядели делово и нейтрально.
    s = str(text or "")
    s = re.sub(r"[\U0001F300-\U0001FAFF]", "", s)
    s = re.sub(r"[\u2600-\u27BF]", "", s)
    return re.sub(r"[ \t]{2,}", " ", s).strip()


def _humanize_reply_text(text: str, inbound_text: str = "") -> str:
    msg = str(text or "").strip()
    if not msg:
        return ""
    msg = re.sub(r"\bнаписал по делу\b[.!]?", "написал", msg, flags=re.IGNORECASE)
    msg = re.sub(r"\bпишу по делу\b[.!]?", "пишу", msg, flags=re.IGNORECASE)
    # Убираем "поддержечные" формулировки. Отвечаем от лица уверенного ментора.
    msg = re.sub(
        r"\bя\s+просто\s+хочу\s+быстро\s+понять[,:\s]*",
        "",
        msg,
        flags=re.IGNORECASE,
    )
    msg = re.sub(
        r"\bчем\s+(именно\s+)?тебе\s+сейчас\s+помочь[^.?!]*[.?!]?",
        "Сфокусируемся на результате по поиску: роль, вилка и план выхода на офферы.",
        msg,
        flags=re.IGNORECASE,
    )
    msg = re.sub(
        r"\bчем\s+(именно\s+)?тебе\s+помочь[^.?!]*[.?!]?",
        "Сфокусируемся на результате по поиску: роль, вилка и план выхода на офферы.",
        msg,
        flags=re.IGNORECASE,
    )
    msg = re.sub(
        r"\bчем\s+могу\s+(быть\s+полезен|помочь)[^.?!]*[.?!]?",
        "Сфокусируемся на результате по поиску: роль, вилка и план выхода на офферы.",
        msg,
        flags=re.IGNORECASE,
    )
    msg = re.sub(
        r"\bу\s+тебя\s+уже\s+довольно\s+четк[а-я]+\s+цель\b[.:]?\s*",
        "",
        msg,
        flags=re.IGNORECASE,
    )
    # Не предлагаем «разобрать без созвона»: цель — довести до записи через бота.
    msg = re.sub(r"\bбез\s+созвон[а-я]*\b", "через диагностический созвон в боте", msg, flags=re.IGNORECASE)
    # Если собеседник уже подтвердил актуальность, не пишем условные конструкции.
    inb = _norm_text(inbound_text)
    if "актуал" in inb:
        msg = re.sub(
            r"если\s+тема\s+актуаль[а-я]*[^.?!]*[.?!]\s*",
            "",
            msg,
            flags=re.IGNORECASE,
        )
        msg = re.sub(
            r"если\s+это\s+актуаль[а-я]*[^.?!]*[.?!]\s*",
            "",
            msg,
            flags=re.IGNORECASE,
        )
    msg = re.sub(r"\n{3,}", "\n\n", msg).strip()
    if len(_norm_text(msg)) < 3:
        msg = "Ок. Тогда фиксируем цель и собираем короткий план выхода на офферы."
    return msg


def _services_overview(direction: str | None = None) -> str:
    d = str(direction or "").strip().lower()
    if d in PRIMARY_DIRECTIONS:
        spec = f"по направлению {d}"
    else:
        spec = "по разным направлениям программирования и смежным задачам карьерного роста"
    return (
        "Я оказываю услуги менторства: дообучаю, готовлю к собеседованиям, провожу mock-интервью, "
        f"помогаю с поиском работы и корректировкой резюме {spec}."
    )


def _price_overview() -> str:
    return (
        "По стоимости есть разные форматы: 1 mock — 4 990 ₽, диагностика/аудит — 8 900 ₽, "
        "интенсивы 16 900–27 900 ₽, длительное сопровождение до оффера — 60 000 ₽ + % от оффера "
        "(или 30 000 ₽ + 150% от оффера). "
        "Точный формат подбираю после мини-диагностики."
    )


def _is_price_question(text: str | None) -> bool:
    t = _norm_text(text)
    if not t:
        return False
    keys = (
        "сколько сто",
        "стоимост",
        "цена",
        "прайс",
        "по деньгам",
        "руб",
    )
    return any(k in t for k in keys)


def _is_uncertain_need(text: str | None) -> bool:
    t = _norm_text(text)
    if not t:
        return False
    patterns = (
        r"\bне\s+знаю\b",
        r"\bхз\b",
        r"\bзатрудняюсь\b",
        r"\bне\s+уверен\b",
        r"\bне\s+понял\b",
        r"\bне\s+понимаю\b",
        r"\bа\?\b",
        r"^\?$",
        r"^а$",
    )
    return any(re.search(p, t) for p in patterns)


def _inject_links(text: str, ai_step: int, force_booking_cta: bool = False) -> str:
    msg = str(text or "").strip()
    norm = _norm_text(msg)
    lines: list[str] = []
    # Второе сообщение в диалоге: обязательно даем сайт и бота.
    # ai_step=0 означает первый AI-ответ после первого исходящего сообщения от нас.
    if int(ai_step or 0) == 0:
        if "leonovcare.ru" not in norm:
            lines.append(f"Сайт: {SITE_URL}")
        if _norm_text(BOT_HANDLE) not in norm:
            lines.append(f"Бот: {BOT_HANDLE}")
    # Если лид сам идет в созвон — явно даем точку записи.
    if force_booking_cta and _norm_text(BOT_HANDLE) not in norm:
        lines.append(f"Можешь записаться на диагностический созвон в боте {BOT_HANDLE}.")
        lines.append("Созвон бесплатный, в боте перед записью попросят PDF-резюме.")
    if lines:
        msg = (msg + "\n\n" + "\n".join(lines)).strip() if msg else "\n".join(lines).strip()
    return _strip_emoji(msg)


def _is_negative_intent(text: str | None) -> bool:
    t = _norm_text(text)
    if not t:
        return False
    patterns = (
        r"\bне\s+интерес",
        r"\bне\s+акту",
        r"\bнеакту",
        r"\bне\s+сейчас\b",
        r"\bпока\s+не\b",
        r"\bпока\s+нет\b",
        r"\bне\s+нужно\b",
        r"\bне\s+надо\b",
        r"\bне\s+пиши(те)?\b",
        r"\bотстан(ьте)?\b",
        r"\bстоп\b",
        r"\bхватит\b",
        r"\bне\s+беспокой",
        r"\bне\s+звон",
        r"\bне\s+хочу\b",
    )
    if any(re.search(p, t) for p in patterns):
        return True
    if re.fullmatch(r"(нет|не|неа|нет спасибо|неа спасибо|нет, спасибо|не, спасибо|пожалуй нет)[.!]?", t):
        return True
    # Короткие отказы без уточнений.
    if len(t) <= 20 and t.startswith(("нет", "неа", "пока нет", "не сейчас")) and "?" not in t:
        return True
    return False


def _should_answer_replied(text: str | None) -> bool:
    t = _norm_text(text)
    if not t:
        return False
    if _is_negative_intent(t):
        return False
    # Короткий ответ неопределенности — это тоже повод продолжить диалог.
    if any(x in t for x in ("не знаю", "хз", "затрудняюсь", "не уверен", "не понял")):
        return True
    # Прямой позитивный маркер даже без вопросительного знака.
    if "актуал" in t:
        return True
    if any(x in t for x in ("интересно", "интересует", "давай")):
        return True
    if "?" in str(text or ""):
        return True
    # Лид просто поздоровался — считаем это поводом ответить, если диалог уже начат.
    if re.fullmatch(r"(привет|здравствуй(те)?|добрый\s+день|добрый\s+вечер|hi|hello)[.!]?", t):
        return True
    question_words = ("как", "что", "какой", "какая", "какие", "почему", "зачем", "сколько", "когда", "где")
    if any(re.search(rf"\b{w}\b", t) for w in question_words):
        return True
    semantic_keys = (
        "подробнее",
        "формат",
        "условия",
        "стоимость",
        "цена",
        "признател",
        "резюме",
        "созвон",
        "собесед",
        "поиск",
        "работ",
        "оффер",
        "ваканси",
        "hh",
        "актуал",
    )
    return any(k in t for k in semantic_keys)


def _classify(text: str) -> str:
    t = _norm_text(text)
    if not t:
        return "no_reply"
    if any(x in t for x in ("в чс", "заблок", "блокнул", "blacklist")):
        return "blocked"
    if _is_negative_intent(t):
        return "not_interested"
    if any(x in t for x in ("созвон провели", "созвонились", "провели звонок", "звонок был")):
        return "call_done"
    if any(x in t for x in ("созвон", "созвониться", "позвон", "когда удобно", "давай завтра", "давай сегодня")):
        return "call_booked"
    if re.search(r"\bда\b", t) or any(x in t for x in ("актуал", "интересно", "можно подробнее", "расскажи", "окей", "хорошо")):
        return "interested"
    return "replied"


def _looks_like_lead_message(text: str) -> bool:
    t = _norm_text(text)
    if not t:
        return False
    if "?" in str(text or ""):
        return True
    markers = (
        "ментор",
        "менторство",
        "собес",
        "работ",
        "резюме",
        "опыт",
        "стек",
        "вилка",
        "цель",
        "стопор",
        "ваканси",
        "hh",
        "карьер",
        "созвон",
        "кто ты",
        "кто вы",
        "что предлага",
        "какие услуги",
        "цена",
        "стоимост",
        "интересно",
        "не знаю",
        "хз",
    )
    if re.fullmatch(r"(привет|здравствуй(те)?|добрый\s+день|добрый\s+вечер|hello|hi)[.!]?", t):
        return True
    return any(m in t for m in markers)


def _next_step_text(script: dict[str, list[str]], direction: str, ai_step: int) -> str:
    d = str(direction or "").strip().lower()
    steps = script.get(d) if isinstance(script, dict) else None
    if not isinstance(steps, list):
        steps = script.get("python") if isinstance(script, dict) else None
    if not isinstance(steps, list):
        return ""
    idx = max(int(ai_step or 0), 0)
    if idx < len(steps):
        return str(steps[idx] or "").strip()
    return ""


def _load_examples(path: str | None) -> list[dict[str, str]]:
    p = Path(str(path or "").strip()) if path else None
    if not p or not p.exists() or not p.is_file():
        return []
    raw = _load_json(p, [])
    out: list[dict[str, str]] = []
    if not isinstance(raw, list):
        return out
    for item in raw:
        if not isinstance(item, dict):
            continue
        q = str(item.get("question") or "").strip()
        a = str(item.get("answer") or "").strip()
        if not q or not a:
            continue
        out.append({"question": q, "answer": a})
    return out


def _examples_prompt(examples: list[dict[str, str]], limit: int = 6) -> str:
    rows: list[str] = []
    for item in examples[: max(0, int(limit or 0))]:
        q = str(item.get("question") or "").strip()
        a = str(item.get("answer") or "").strip()
        if not q or not a:
            continue
        rows.append(f"- Вопрос: {q}\n  Ответ: {a}")
    if not rows:
        return ""
    return "Примеры ответов из реальных чатов:\n" + "\n".join(rows) + "\n\n"


def _typical_reply(inbound_text: str, direction: str = "") -> str:
    t = _norm_text(inbound_text)
    if not t:
        return ""
    if _is_uncertain_need(t):
        return (
            "Понял. Тогда доберём базу коротко:\n"
            "1) сколько у тебя коммерческого опыта,\n"
            "2) с каким стеком/ролью выходишь,\n"
            "3) какая целевая вилка и срок."
        )
    if "какой профиль" in t and ("нашел" in t or "нашёл" in t):
        return "Профиль на hh."
    if any(x in t for x in ("как именно", "каким образом", "чем именно", "как можете помочь", "что конкретно", "в каком формате")):
        return (
            f"{_services_overview(direction)}\n\n"
            "Для начала бесплатно провожу диагностический созвон: разберем резюме и найдем стопперы. "
            "Если все подойдет, обсудим дальнейшее сотрудничество и ценность."
        )
    if _is_price_question(t):
        return (
            f"{_services_overview(direction)}\n\n"
            f"{_price_overview()}\n\n"
            "Могу бесплатно провести диагностический созвон: разберу резюме и покажу, где основные стопперы. "
            "Если зайдет формат — обсудим дальнейшее сотрудничество."
        )
    if "что у вас за аккаунт hh" in t or ("аккаунт hh" in t and "что" in t):
        return "У нас бизнес-аккаунт на hh, с которого можем просматривать кандидатов."
    if "а что про меня было написано" in t or "ищу работу?" in t:
        return "Мы ищем по критериям «активно ищет работу» и «рассматривает предложения»."
    if any(x in t for x in ("кто ты", "о тебе", "где посмотреть", "канал", "кейс", "отзывы")):
        return (
            "Я ментор по IT-карьере и подготовке к собеседованиям, более 6 лет в коммерческой разработке.\n"
            f"Канал: {MENTOR_CHANNEL_URL}"
        )
    if "можно попробовать" in t:
        return (
            "Отлично. Тогда идем по делу: запишись на диагностический созвон в боте @Leonov_Care_bot.\n"
            "В процессе бот попросит PDF-резюме и передаст его ментору, чтобы разбор был предметным."
        )
    return ""


def _default_offline_reply(inbound_text: str, direction: str) -> str:
    t = _norm_text(inbound_text)
    if _is_uncertain_need(t):
        return (
            "Понял. Тогда коротко доберём мини-диагностику:\n"
            "сколько у тебя коммерческого опыта, с каким стеком выходишь, какая целевая роль/вилка и срок."
        )
    if _is_negative_intent(t):
        return "Понял, спасибо за ответ. Если что-то изменится, можешь написать в любой момент."
    if _is_price_question(t):
        return (
            f"{_services_overview(direction)}\n\n"
            f"{_price_overview()}\n\n"
            "Для старта бесплатно проведу диагностический созвон с разбором резюме. "
            "Если подойдет, обсудим удобный формат сотрудничества."
        )
    if any(x in t for x in ("как именно", "каким образом", "чем именно", "как поможете", "что конкретно")):
        return (
            f"{_services_overview(direction)}\n\n"
            "Если кратко по ценности: усиливаем резюме и стратегию поиска, прокачиваем собеседования и "
            "закрываем технические пробелы под цель."
        )
    if any(x in t for x in ("интерес", "актуаль", "ментор", "созвон", "подроб", "формат")):
        return (
            "Отлично. Тогда фиксируем цель и текущие стопперы в поиске:\n"
            "сколько у тебя опыта, какая целевая роль/вилка и что сейчас режет результат — резюме, отклики, интервью или техчасть?"
        )
    if "резюме" in t:
        return (
            "Супер. Запишись на диагностический созвон через @Leonov_Care_bot — "
            "внутри бот запросит PDF-резюме и отправит его ментору перед звонком."
        )
    if any(x in t for x in ("поиск работы", "собесед", "оффер", "резюме")):
        return (
            "Принял. Тогда уточню 3 вещи, чтобы дать полезный план:\n"
            "1) сколько сейчас опыта,\n2) какой стек,\n3) в чем основной стопор по поиску."
        )
    return (
        f"{_services_overview(direction)}\n\n"
        "Давай коротко уточню контекст и предложу конкретный план действий."
    )


def _msg_id(m: Any) -> int:
    try:
        return int(getattr(m, "id", 0) or 0)
    except Exception:
        return 0


def _is_voice_message(m: Any) -> bool:
    # Voice / video note shortcut flags (Telethon custom Message API).
    if bool(getattr(m, "voice", False)) or bool(getattr(m, "video_note", False)):
        return True
    doc = getattr(m, "document", None)
    if doc is None:
        return False
    mime = str(getattr(doc, "mime_type", "") or "").strip().lower()
    if mime.startswith("audio/"):
        return True
    attrs = list(getattr(doc, "attributes", []) or [])
    for a in attrs:
        if "documentattributeaudio" in str(a.__class__.__name__).lower():
            return True
    return False


def _msg_text(
    m: Any,
    transcribed_texts: dict[int, str] | None = None,
    *,
    history_mode: bool = False,
) -> str:
    txt = str(getattr(m, "raw_text", "") or "").strip()
    if txt:
        return txt
    mid = _msg_id(m)
    if mid <= 0:
        return ""
    t = str((transcribed_texts or {}).get(mid) or "").strip()
    if not t:
        return ""
    return f"[голосовое] {t}" if history_mode else t


async def _transcribe_audio_message(
    client: Any,
    peer: Any,
    msg_id: int,
    *,
    poll_attempts: int = 3,
    poll_delay_sec: float = 1.2,
) -> tuple[str, str]:
    if int(msg_id or 0) <= 0:
        return "", "invalid_msg_id"
    try:
        from telethon import functions
    except Exception as exc:
        return "", f"telethon_functions_import:{type(exc).__name__}:{exc}"
    try:
        res = await client(functions.messages.TranscribeAudioRequest(peer=peer, msg_id=int(msg_id)))
        attempts = max(0, int(poll_attempts or 0))
        while bool(getattr(res, "pending", False)) and attempts > 0:
            await asyncio.sleep(max(float(poll_delay_sec or 1.2), 0.2))
            res = await client(functions.messages.TranscribeAudioRequest(peer=peer, msg_id=int(msg_id)))
            attempts -= 1
        text = str(getattr(res, "text", "") or "").strip()
        if not text:
            return "", "empty_transcription"
        return text, ""
    except Exception as exc:
        return "", f"{type(exc).__name__}:{exc}"


async def _resolve_transcribed_texts(
    client: Any,
    peer: Any,
    messages: list[Any],
    *,
    enabled: bool,
    max_per_dialog: int,
    poll_attempts: int,
    poll_delay_ms: int,
) -> tuple[dict[int, str], int, int]:
    if not enabled:
        return {}, 0, 0
    out: dict[int, str] = {}
    ok = 0
    failed = 0
    budget = max(0, int(max_per_dialog or 0))
    if budget <= 0:
        return out, 0, 0
    for m in list(messages or []):
        if budget <= 0:
            break
        txt = str(getattr(m, "raw_text", "") or "").strip()
        if txt:
            continue
        if not _is_voice_message(m):
            continue
        mid = _msg_id(m)
        if mid <= 0:
            continue
        t, _ = await _transcribe_audio_message(
            client,
            peer,
            mid,
            poll_attempts=max(0, int(poll_attempts or 0)),
            poll_delay_sec=max(int(poll_delay_ms or 1200), 200) / 1000.0,
        )
        budget -= 1
        if t:
            out[mid] = t
            ok += 1
        else:
            failed += 1
    return out, ok, failed


def _build_history(
    messages: list[Any],
    max_lines: int = 80,
    transcribed_texts: dict[int, str] | None = None,
) -> str:
    rows: list[str] = []
    # Telethon возвращает чаще всего от новых к старым; разворачиваем в хронологию.
    for m in reversed(list(messages or [])):
        txt = _msg_text(m, transcribed_texts, history_mode=True)
        if not txt:
            continue
        role = "Ментор" if bool(getattr(m, "out", False)) else "Лид"
        rows.append(f"{role}: {txt}")
    if len(rows) > max_lines:
        rows = rows[-max_lines:]
    return "\n".join(rows)


def _default_guardrails() -> dict[str, Any]:
    return {
        "forbidden_substrings": [
            "пишу по делу",
            "написал по делу",
            "получилось посмотреть",
            "без созвона",
            "чем тебе помочь",
            "чем именно тебе сейчас помочь",
            "чем могу быть полезен",
            "я просто хочу быстро понять",
            "у тебя уже довольно четкая цель",
        ],
        "forbidden_regex": [
            r"\bесли\s+тема\s+актуаль",
            r"\bесли\s+это\s+актуаль",
            r"\bбез\s+созвон",
            r"\bчем\s+(именно\s+)?тебе\s+(сейчас\s+)?помочь",
            r"\bчем\s+могу\s+(быть\s+полезен|помочь)",
            r"\bя\s+просто\s+хочу\s+быстро\s+понять",
            r"\bу\s+тебя\s+уже\s+довольно\s+четк[а-я]+\s+цель",
        ],
        "max_message_chars": 900,
    }


def _normalize_guardrails(payload: dict[str, Any] | None) -> dict[str, Any]:
    src = payload or {}
    base = _default_guardrails()
    out: dict[str, Any] = {
        "forbidden_substrings": list(base["forbidden_substrings"]),
        "forbidden_regex": list(base["forbidden_regex"]),
        "max_message_chars": int(base["max_message_chars"]),
    }
    raw_sub = src.get("forbidden_substrings")
    seen: set[str] = set(str(x or "").strip().lower() for x in out["forbidden_substrings"])
    if isinstance(raw_sub, list):
        for item in raw_sub:
            s = str(item or "").strip().lower()
            if not s or s in seen:
                continue
            seen.add(s)
            out["forbidden_substrings"].append(s)

    raw_re = src.get("forbidden_regex")
    seen_re: set[str] = set(str(x or "").strip() for x in out["forbidden_regex"])
    if isinstance(raw_re, list):
        for item in raw_re:
            s = str(item or "").strip()
            if not s or s in seen_re:
                continue
            seen_re.add(s)
            out["forbidden_regex"].append(s)

    try:
        max_chars = int(src.get("max_message_chars") or base["max_message_chars"])
    except Exception:
        max_chars = int(base["max_message_chars"])
    out["max_message_chars"] = max(120, min(max_chars, 3000))
    return out


def _load_guardrails(path: str | None) -> dict[str, Any]:
    p = Path(str(path or "").strip()) if path else None
    if not p or not p.exists() or not p.is_file():
        return _normalize_guardrails({})
    raw = _load_json(p, {})
    if not isinstance(raw, dict):
        raw = {}
    return _normalize_guardrails(raw)


def _guardrail_violations(text: str, inbound_text: str, rules: dict[str, Any]) -> list[str]:
    msg = str(text or "").strip()
    if not msg:
        return ["empty_message"]
    lower = msg.lower()
    violations: list[str] = []
    for s in list(rules.get("forbidden_substrings") or []):
        needle = str(s or "").strip().lower()
        if needle and needle in lower:
            violations.append(f"forbidden_substring:{needle}")
    for pattern in list(rules.get("forbidden_regex") or []):
        p = str(pattern or "").strip()
        if not p:
            continue
        try:
            if re.search(p, lower, flags=re.IGNORECASE):
                violations.append(f"forbidden_regex:{p}")
        except re.error:
            continue
    max_chars = int(rules.get("max_message_chars") or 900)
    if len(msg) > max_chars:
        violations.append(f"too_long:{len(msg)}>{max_chars}")
    # Если лид уже сказал "актуально", не допускаем условных "если тема актуальна...".
    inb = _norm_text(inbound_text)
    if "актуал" in inb and re.search(r"\bесли\s+(тема|это)\s+актуаль", lower):
        violations.append("conditional_on_actual_topic")
    if re.search(r"\bбез\s+созвон", lower):
        violations.append("without_call_phrase")
    return violations


def _snippet(text: str | None, limit: int = 180) -> str:
    raw = " ".join(str(text or "").replace("\n", " ").split()).strip()
    if len(raw) <= limit:
        return raw
    return raw[: max(limit - 1, 1)].rstrip() + "…"


def _lead_history_lines(chat_history: str) -> list[str]:
    out: list[str] = []
    for line in str(chat_history or "").splitlines():
        row = str(line or "").strip()
        if not row:
            continue
        low = row.lower()
        if low.startswith("лид:"):
            out.append(row.split(":", 1)[1].strip())
    return out


def _conversation_state_hint(chat_history: str) -> str:
    lead_rows = _lead_history_lines(chat_history)
    if not lead_rows:
        return "Подтвержденных фактов из истории чата нет."
    src = "\n".join(lead_rows)
    low = src.lower()
    lines: list[str] = []

    m_exp = re.search(r"(\d{1,2}\+?)\s*(лет|года|год|месяц|месяцев)", low)
    if m_exp:
        lines.append(f"- Опыт уже известен: {m_exp.group(1)} {m_exp.group(2)}.")

    stack_map = [
        ("java", "Java"),
        ("python", "Python"),
        ("golang", "Golang"),
        ("go", "Go"),
        ("php", "PHP"),
        ("javascript", "JavaScript"),
        ("typescript", "TypeScript"),
        ("frontend", "Frontend"),
        ("backend", "Backend"),
        ("fullstack", "Fullstack"),
        ("c++", "C++"),
        ("react", "React"),
    ]
    stack: list[str] = []
    seen_stack: set[str] = set()
    for key, label in stack_map:
        if re.search(rf"\b{re.escape(key)}\b", low) and label not in seen_stack:
            seen_stack.add(label)
            stack.append(label)
    if stack:
        lines.append(f"- Стек уже известен: {', '.join(stack[:8])}.")

    goal_line = ""
    for row in reversed(lead_rows):
        r = row.lower()
        if any(k in r for k in ("цель", "хочу", "ищу", "тк", "net", "зарплат", "доход", "стабильн", "проект")):
            goal_line = row
            break
    if goal_line:
        lines.append(f"- Цель уже известна: {_snippet(goal_line, 170)}")

    blocker_line = ""
    for row in reversed(lead_rows):
        r = row.lower()
        if any(k in r for k in ("затык", "не хватает", "автоотказ", "не зовут", "сложн", "проблем")):
            blocker_line = row
            break
    if blocker_line:
        lines.append(f"- Стопор уже известен: {_snippet(blocker_line, 170)}")

    timeline_line = ""
    for row in reversed(lead_rows):
        r = row.lower()
        if any(k in r for k in ("срок", "сроки", "месяц", "недел", "квартал", "до конца")):
            timeline_line = row
            break
    if timeline_line:
        lines.append(f"- Срок уже известен: {_snippet(timeline_line, 140)}")

    if not lines:
        return "Подтвержденных фактов из истории чата мало — задавай только один следующий вопрос по делу."

    covered = set()
    if any("Опыт уже известен" in x for x in lines):
        covered.add("опыт")
    if any("Стек уже известен" in x for x in lines):
        covered.add("стек")
    if any("Цель уже известна" in x for x in lines):
        covered.add("цель")
    if any("Стопор уже известен" in x for x in lines):
        covered.add("стопор")
    missing = [x for x in ("опыт", "стек", "цель", "стопор") if x not in covered]
    lines.append("- Уже закрытые вопросы повторно НЕ задавай.")
    if missing:
        lines.append(f"- Ещё не уточнено: {', '.join(missing)}.")
    else:
        lines.append("- Базовая мини-диагностика собрана: переходи к записи на диагностический созвон через @Leonov_Care_bot.")
    return "\n".join(lines)


def _conversation_known_fields(chat_history: str) -> set[str]:
    lead_rows = _lead_history_lines(chat_history)
    if not lead_rows:
        return set()
    src = "\n".join(lead_rows)
    low = src.lower()
    known: set[str] = set()

    if re.search(r"(\d{1,2}\+?)\s*(лет|года|год|месяц|месяцев)", low):
        known.add("опыт")

    if any(re.search(rf"\b{re.escape(k)}\b", low) for k in ("java", "python", "golang", "go", "php", "javascript", "typescript", "frontend", "backend", "fullstack", "c++", "react")):
        known.add("стек")

    if any(k in low for k in ("цель", "хочу", "ищу", "тк", "net", "зарплат", "доход", "стабильн", "проект")):
        known.add("цель")

    if any(k in low for k in ("затык", "не хватает", "автоотказ", "не зовут", "сложн", "проблем", "стопор")):
        known.add("стопор")

    if any(k in low for k in ("срок", "сроки", "недел", "месяц", "квартал", "до конца", "в течение")):
        known.add("срок")

    return known


def _uncertain_followup_from_history(chat_history: str, inbound_text: str) -> str:
    known = _conversation_known_fields(chat_history)
    order = ["опыт", "стек", "цель", "стопор", "срок"]
    missing = [x for x in order if x not in known]
    if not missing:
        return (
            "Я могу помочь с этим через бесплатный диагностический созвон: "
            "разберу, как лучше упаковать опыт под целевую роль и где резюме сейчас проседает.\n"
            f"Запишись в боте {BOT_HANDLE}. После созвона обсудим детали дальнейшего сотрудничества."
        )

    # Если человек пишет, что не знает стопперы — сначала пытаемся уточнить через этапы.
    # Если это повторяется, переводим в бесплатную диагностику, чтобы не зацикливаться.
    if missing == ["стопор"] and _is_uncertain_need(inbound_text):
        uncertain_hits = 0
        for row in _lead_history_lines(chat_history):
            if _is_uncertain_need(row):
                uncertain_hits += 1
        if uncertain_hits >= 2:
            return (
                "Ок, тогда не будем гадать по переписке. "
                "Давай сделаем бесплатный диагностический созвон: там быстро выявим стопперы и соберем рабочий план.\n"
                f"Запись через {BOT_HANDLE}. После созвона обсудим детали дальнейшего сотрудничества."
            )
        return (
            "Ок, если стопор пока сложно назвать, давай через факты:\n"
            "на каком этапе чаще всего срезается воронка — отклики, тестовые, HR-этап или техсобеседования?"
        )

    questions = {
        "опыт": "сколько у тебя коммерческого опыта?",
        "стек": "с каким стеком и ролью сейчас хочешь выходить на рынок?",
        "цель": "какая целевая роль и вилка по доходу для тебя приоритетны?",
        "стопор": "где сейчас главный стопор: отклики, интервью, техчасть или упаковка резюме?",
        "срок": "в какие сроки хочешь выйти на этот результат?",
    }
    ask = [questions[k] for k in missing if k in questions]
    return "Понял. Тогда доберём оставшиеся пункты мини-диагностики:\n- " + "\n- ".join(ask)


def _extract_latest_messages(
    messages: list[Any],
    transcribed_texts: dict[int, str] | None = None,
) -> tuple[str, Any, int, str, Any, int]:
    inbound_text = ""
    inbound_dt = None
    inbound_id = 0
    outbound_text = ""
    outbound_dt = None
    outbound_id = 0
    for m in list(messages or []):
        txt = _msg_text(m, transcribed_texts, history_mode=False)
        if not txt:
            continue
        if bool(getattr(m, "out", False)):
            if not outbound_text:
                outbound_text = txt
                outbound_dt = getattr(m, "date", None)
                outbound_id = _msg_id(m)
        else:
            if not inbound_text:
                inbound_text = txt
                inbound_dt = getattr(m, "date", None)
                inbound_id = _msg_id(m)
        if inbound_text and outbound_text:
            break
    return inbound_text, inbound_dt, inbound_id, outbound_text, outbound_dt, outbound_id


def _is_duplicate_inbound(
    prev_inbound_text: str | None,
    current_inbound_text: str | None,
    has_prev_outbound: bool,
    inbound_dt: Any,
    latest_outbound_dt: Any,
    inbound_msg_id: int | None = None,
    latest_outbound_msg_id: int | None = None,
) -> tuple[bool, str]:
    try:
        in_id = int(inbound_msg_id or 0)
    except Exception:
        in_id = 0
    try:
        out_id = int(latest_outbound_msg_id or 0)
    except Exception:
        out_id = 0

    # Основная защита от дублей: если уже есть исходящее с id >= входящего, значит на это входящее ответ уже ушел.
    if out_id > 0 and in_id > 0 and out_id >= in_id:
        return True, "outbound_id_is_newer_or_equal"

    # Fallback по времени: только строгое превосходство (>= давало ложные блокировки при одном timestamp).
    if latest_outbound_dt is not None and inbound_dt is not None and latest_outbound_dt > inbound_dt:
        return True, "outbound_is_newer"
    # Не блокируем повтор только по совпадению текста: лид может повторить короткий ответ позже.
    return False, ""


def _llm_chat_completion(
    provider: str,
    base_url: str,
    proxy_auth: str,
    api_key: str,
    timeout_sec: int,
    body: dict[str, Any],
    http_proxy: str = "",
) -> tuple[bool, int, str, str]:
    def _post(url: str, req_headers: dict[str, str]) -> tuple[bool, int, str, str]:
        hp_local = str(http_proxy or "").strip()
        proxies_local = {"http": hp_local, "https": hp_local} if hp_local else None
        try:
            resp = requests.post(
                url,
                headers=req_headers,
                json=body,
                timeout=max(int(timeout_sec or 30), 5),
                proxies=proxies_local,
            )
        except Exception as exc:
            return False, 0, "", f"http_error:{type(exc).__name__}:{exc}"
        text_local = str(resp.text or "")
        if resp.status_code >= 300:
            return False, int(resp.status_code or 0), text_local[:500], "http_non_2xx"
        try:
            data = resp.json()
        except Exception:
            return False, int(resp.status_code or 0), text_local[:500], "invalid_json"
        content = (
            data.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "")
        )
        msg_local = str(content or "").strip()
        if not msg_local:
            return False, int(resp.status_code or 0), text_local[:500], "empty_llm_message"
        return True, int(resp.status_code or 0), msg_local, ""

    p = str(provider or "openai").strip().lower()
    p = p if p in {"openai", "proxy"} else "openai"
    endpoint = (
        "https://api.openai.com/v1/chat/completions"
        if p == "openai"
        else f"{str(base_url or '').rstrip('/')}/v1/chat/completions"
    )
    headers = {"Content-Type": "application/json"}
    if p == "openai":
        key = str(api_key or "").strip()
        if not key:
            return False, 0, "", "openai_api_key_missing"
        headers["Authorization"] = f"Bearer {key}"
    else:
        token = str(proxy_auth or "").strip()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        if not str(base_url or "").strip():
            return False, 0, "", "proxy_base_url_missing"
    ok, code, payload, err = _post(endpoint, headers)
    if ok:
        return True, code, payload, ""

    # Защита от "молчания": если прокси недоступен, пробуем прямой OpenAI.
    if p == "proxy":
        key = str(api_key or "").strip()
        if key:
            direct_headers = {"Content-Type": "application/json", "Authorization": f"Bearer {key}"}
            ok2, code2, payload2, err2 = _post("https://api.openai.com/v1/chat/completions", direct_headers)
            if ok2:
                return True, code2, payload2, "proxy_fallback_openai"
            return False, code2, payload2, f"proxy_failed:{err};openai_failed:{err2}"
    return False, code, payload, err


def _openai_reply(
    provider: str,
    base_url: str,
    proxy_auth: str,
    api_key: str,
    model: str,
    script_step_text: str,
    inbound_text: str,
    direction: str,
    ai_step: int = 0,
    previous_outbound_text: str = "",
    chat_history: str = "",
    force_booking_cta: bool = False,
    examples: list[dict[str, str]] | None = None,
    guardrails: dict[str, Any] | None = None,
    strict_mode: bool = True,
    timeout_sec: int = 30,
    http_proxy: str = "",
) -> dict[str, Any]:
    script_hint = str(script_step_text or "").strip()
    convo_hint = _conversation_state_hint(chat_history)
    d_norm = str(direction or "").strip().lower()
    direction_prompt = d_norm if d_norm in PRIMARY_DIRECTIONS else "не задано"
    is_strict = bool(strict_mode)
    rules = _normalize_guardrails(guardrails or {})
    if not str(provider or "").strip():
        provider = "openai"
    if str(provider).strip().lower() == "openai" and not str(api_key or "").strip():
        if is_strict:
            return {
                "text": "",
                "needs_review": True,
                "decision_source": "llm_error",
                "llm_status": "failed",
                "llm_code": 0,
                "llm_error": "openai_api_key_missing",
            }
        base = _typical_reply(inbound_text, direction) or _default_offline_reply(inbound_text, direction) or script_hint
        base = _humanize_reply_text(base, inbound_text=inbound_text)
        msg = _inject_links(base, ai_step=ai_step, force_booking_cta=force_booking_cta)
        violations = _guardrail_violations(msg, inbound_text, rules)
        if violations:
            return {
                "text": "",
                "needs_review": True,
                "decision_source": "guardrail_blocked",
                "llm_status": "failed",
                "llm_code": 0,
                "llm_error": "guardrail_violation:" + ";".join(violations[:3]),
            }
        return {
            "text": msg,
            "needs_review": False,
            "decision_source": "rule_fallback",
            "llm_status": "failed",
            "llm_code": 0,
            "llm_error": "openai_api_key_missing",
        }
    prev_outbound = str(previous_outbound_text or "").strip()
    typical = _typical_reply(inbound_text, direction)
    booking_rule = (
        "Если лид сам согласился на созвон или попросил перейти к созвону, "
        "только тогда добавь: "
        "\"Можешь записаться на диагностический созвон в нашем боте @Leonov_Care_bot\". "
        "Уточни, что PDF-резюме бот запросит сам при записи."
    )
    prompt = (
        "Ты отвечаешь от лица Олега Леонова, ментора по IT-карьере и разработке. "
        "Пиши строго от первого лица (я/мне), как живой человек в Telegram. "
        "Сформируй короткое следующее сообщение в Telegram по-русски, как живой человек. "
        "Сохрани дружелюбный тон, без давления, без спама и без обещаний гарантий. "
        "Мы играем в несколько касаний, а не пытаемся продать в одно сообщение.\n"
        "Не используй эмодзи.\n"
        "Перед ответом обязательно проанализируй весь диалог (историю чата ниже), а не только последнее сообщение.\n"
        "Если лид задал вопрос — сначала ответь по сути вопроса.\n"
        "Никогда не используй мягкие/поддержечные формулировки: "
        "«чем тебе помочь», «чем могу помочь», «чем могу быть полезен», "
        "«я просто хочу быстро понять». Пиши как уверенный ментор, ведущий к результату.\n"
        "Если лид написал «не знаю / хз / затрудняюсь» — не задавай повторный вопрос «с чем помочь»: "
        "сначала добери недостающие пункты мини-диагностики, и только после этого предлагай запись через @Leonov_Care_bot.\n"
        "Если лид написал «ищу работу» — сразу веди к конкретному следующему шагу мини-диагностики.\n"
        "Если лид пишет, что неактуально/неинтересно/стоп — не дожимай, ответь корректно и мягко заверши диалог.\n"
        "Не повторяй один и тот же вопрос, если лидер уже дал на него ответ в истории.\n"
        "Не пересказывай дословно то, что только что написал лид. Максимум 1 короткая фиксация сути без канцелярита.\n"
        "Не используй фразы вроде «у тебя уже довольно четкая цель».\n"
        "Сначала двигайся по мини-диагностике, но если опыт+цель+стопор уже понятны из истории — "
        "не затягивай и переводи к записи на диагностический созвон через бота.\n"
        "Никогда не используй формулировки типа «пишу по делу» или «написал по делу».\n"
        "Никогда не предлагай формат «без созвона».\n"
        "Не проси прислать резюме в этот чат: резюме запрашивает бот в процессе записи на созвон.\n"
        "Если человек уже подтвердил, что тема актуальна — не пиши «если тема актуальна...», переходи сразу к сути.\n"
        "Базово следуй скрипту холодных продаж и веди лид по его шагам.\n"
        "Если в сообщении лида есть вопрос — сначала ответь на вопрос, затем продолжай ближайший уместный шаг скрипта.\n"
        "Если конкретный вопрос скрипта противоречит уже полученным данным от лида — адаптируй формулировку под этого человека и не повторяй уже закрытый вопрос.\n\n"
        "Что именно я предлагаю:\n"
        "- менторство по основным направлениям Java/Frontend/Golang/Python и смежным карьерным задачам;\n"
        "- дообучение, подготовка к собеседованиям, mock-интервью;\n"
        "- усиление резюме и стратегии поиска работы;\n"
        "- сопровождение до оффера.\n"
        f"- сайт: {SITE_URL}\n"
        f"- бот: {BOT_HANDLE}\n"
        f"- канал ментора: {MENTOR_CHANNEL_URL}\n\n"
        "Ориентир по ценам (если спросили про стоимость):\n"
        "- 1 mock: 4 990 ₽\n"
        "- диагностика/аудит: 8 900 ₽\n"
        "- интенсивы: 16 900–27 900 ₽\n"
        "- сопровождение: 60 000 ₽ + % от оффера (или 30 000 ₽ + 150%)\n"
        "Если спрашивают цену, сначала ответь по диапазону/формату и объясни, что точный формат выбирается после мини-диагностики.\n\n"
        "При приглашении на созвон обязательно уточняй, что он бесплатный, "
        "и что после него можно обсудить детали дальнейшего сотрудничества.\n\n"
        "Типовые ответы (используй, когда вопрос совпадает):\n"
        "- «Привет, а какой профиль ты нашел?» -> «Профиль на hh.»\n"
        "- «Тема актуальна, что конкретно можешь предложить? в каком формате?» -> ответь, какие услуги и в чем ценность.\n"
        "- «Сколько стоят услуги?» -> сначала дай ориентиры по форматам, потом предложи бесплатную диагностику.\n"
        "- «а что у вас за аккаунт hh» -> «У нас бизнес-аккаунт на hh, с которого мы можем просматривать кандидатов.»\n"
        "- «а что про меня было написано? ищу работу?» -> "
        "«Мы ищем по критериям: активно ищет работу, рассматривает предложения.»\n"
        "- Если лид пишет «можно попробовать» -> начни мини-диагностику.\n"
        f"{booking_rule}\n\n"
        f"{_examples_prompt(examples or [])}"
        f"Направление: {direction_prompt}\n"
        f"Номер AI-шага: {int(ai_step or 0)}\n"
        f"Что уже известно из переписки:\n{convo_hint}\n\n"
        f"История чата:\n{chat_history or 'нет истории'}\n\n"
        f"Предыдущее исходящее сообщение: {prev_outbound or 'нет'}\n"
        f"Ориентир по скрипту: {script_hint or 'нет'}\n"
        f"Последнее сообщение лида: {inbound_text}\n"
        f"Типовой черновик (если подходит по вопросу): {typical or 'не задан'}\n"
        f"Форсировать CTA на запись через @Leonov_Care_bot: {'да' if force_booking_cta else 'нет'}\n"
    )
    body = {
        "model": model or "gpt-5.4-mini",
        "messages": [
            {"role": "system", "content": "Отвечай только текстом одного сообщения, без markdown и без эмодзи."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.4,
    }
    ok, code, payload, err = _llm_chat_completion(
        provider=provider,
        base_url=base_url,
        proxy_auth=proxy_auth,
        api_key=api_key,
        http_proxy=http_proxy,
        timeout_sec=timeout_sec,
        body=body,
    )
    if not ok:
        if is_strict:
            return {
                "text": "",
                "needs_review": True,
                "decision_source": "llm_error",
                "llm_status": "failed",
                "llm_code": int(code or 0),
                "llm_error": f"{err}:{payload[:160]}".strip(":"),
            }
        base = _typical_reply(inbound_text, direction) or _default_offline_reply(inbound_text, direction) or script_hint
        base = _humanize_reply_text(base, inbound_text=inbound_text)
        msg = _inject_links(base, ai_step=ai_step, force_booking_cta=force_booking_cta)
        violations = _guardrail_violations(msg, inbound_text, rules)
        if violations:
            return {
                "text": "",
                "needs_review": True,
                "decision_source": "guardrail_blocked",
                "llm_status": "failed",
                "llm_code": int(code or 0),
                "llm_error": "guardrail_violation:" + ";".join(violations[:3]),
            }
        return {
            "text": msg,
            "needs_review": False,
            "decision_source": "rule_fallback",
            "llm_status": "failed",
            "llm_code": int(code or 0),
            "llm_error": f"{err}:{payload[:160]}".strip(":"),
        }
    msg = _strip_emoji(str(payload or "").strip())
    if not msg:
        if is_strict:
            return {
                "text": "",
                "needs_review": True,
                "decision_source": "llm_error",
                "llm_status": "failed",
                "llm_code": int(code or 0),
                "llm_error": "empty_llm_message",
            }
        msg = _typical_reply(inbound_text, direction) or _default_offline_reply(inbound_text, direction) or script_hint
    msg = _humanize_reply_text(msg, inbound_text=inbound_text)
    final_text = _inject_links(msg, ai_step=ai_step, force_booking_cta=force_booking_cta)
    if _is_uncertain_need(inbound_text):
        final_text = _uncertain_followup_from_history(chat_history, inbound_text)
    violations = _guardrail_violations(final_text, inbound_text, rules)
    if violations:
        return {
            "text": "",
            "needs_review": True,
            "decision_source": "guardrail_blocked",
            "llm_status": "failed",
            "llm_code": int(code or 0),
            "llm_error": "guardrail_violation:" + ";".join(violations[:3]),
        }
    return {
        "text": final_text,
        "needs_review": False,
        "decision_source": "llm",
        "llm_status": "ok",
        "llm_code": int(code or 0),
        "llm_error": "",
    }


async def _run(args: argparse.Namespace) -> int:
    try:
        from telethon import TelegramClient
    except Exception as exc:
        Path(args.report_file).write_text(
            json.dumps({"status": "failed", "error": f"telethon import error: {exc}"}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return 1

    leads = _load_json(Path(args.leads_file), [])
    script = _load_json(Path(args.script_file), {})
    ai_enabled = int(args.ai_enabled or 0) == 1
    account_type = str(getattr(args, "account_type", ACCOUNT_TYPE_STANDARD) or "").strip().lower()
    if account_type not in {ACCOUNT_TYPE_STANDARD, ACCOUNT_TYPE_POLYGON}:
        account_type = ACCOUNT_TYPE_STANDARD
    try:
        send_ai_requested = int(args.send_ai) == 1
    except Exception:
        send_ai_requested = True
    # Жесткий предохранитель: AI-отправка разрешена только для полигон-аккаунта.
    send_ai_enabled = bool(send_ai_requested and account_type == ACCOUNT_TYPE_POLYGON)
    openai_key = str(args.openai_api_key or os.getenv("OWNER_OPENAI_API_KEY", "")).strip()
    openai_model = str(args.openai_model or "gpt-5.4-mini").strip()
    ai_provider = str(
        getattr(args, "ai_provider", "") or os.getenv("OWNER_AI_PROVIDER", "openai")
    ).strip().lower()
    if ai_provider not in {"openai", "proxy"}:
        ai_provider = "openai"
    ai_base_url = str(
        getattr(args, "ai_base_url", "") or os.getenv("OWNER_AI_BASE_URL", "")
    ).strip().rstrip("/")
    ai_proxy_auth = str(
        getattr(args, "ai_proxy_auth", "") or os.getenv("OWNER_AI_PROXY_AUTH", "")
    ).strip()
    ai_http_proxy = str(
        getattr(args, "ai_http_proxy", "") or os.getenv("OWNER_AI_HTTP_PROXY", "")
    ).strip()
    ai_timeout_sec = max(5, int(getattr(args, "ai_timeout_sec", 30) or 30))
    ai_strict_mode = int(getattr(args, "ai_strict_mode", 1) or 1) == 1
    transcribe_audio = int(getattr(args, "transcribe_audio", 1) or 1) == 1
    transcribe_max_per_dialog = max(0, int(getattr(args, "transcribe_max_per_dialog", 4) or 4))
    transcribe_poll_attempts = max(0, int(getattr(args, "transcribe_poll_attempts", 3) or 3))
    transcribe_poll_delay_ms = max(200, int(getattr(args, "transcribe_poll_delay_ms", 1200) or 1200))
    examples = _load_examples(args.examples_file)
    guardrails = _load_guardrails(getattr(args, "guardrails_file", ""))

    report: dict[str, Any] = {
        "status": "done",
        "processed": 0,
        "updated": 0,
        "ai_sent": 0,
        "needs_review": 0,
        "duplicate_blocked": 0,
        "ai_blocked_non_polygon": 0,
        "llm_ok": 0,
        "llm_failed": 0,
        "llm_provider": ai_provider,
        "guardrails": guardrails,
        "account_type": account_type,
        "send_ai_requested": send_ai_requested,
        "send_ai_enabled": send_ai_enabled,
        "transcribe_audio": transcribe_audio,
        "transcribe_ok": 0,
        "transcribe_failed": 0,
        "learn_pairs": [],
        "details": [],
    }
    if send_ai_requested and not send_ai_enabled:
        report["send_guard"] = "ai_send_forbidden_for_non_polygon"
    if not isinstance(leads, list):
        leads = []
    known_handles: set[str] = set()
    fallback_max_dialogs = max(0, int(getattr(args, "fallback_max_dialogs", 3) or 3))
    fallback_max_seconds = max(5, int(getattr(args, "fallback_max_seconds", 20) or 20))
    fallback_max_scan = max(20, fallback_max_dialogs * 10)
    proxy_raw = (
        str(getattr(args, "proxy", "") or "").strip()
        or str(os.getenv("OWNER_TG_PROXY") or "").strip()
        or str(os.getenv("OWNER_TG_MTPROXY") or "").strip()
    )
    client_kwargs, proxy_resolved = build_client_kwargs(proxy_raw)
    report["proxy_requested"] = bool(proxy_resolved)
    report["proxy_used"] = False
    report["proxy_fallback_to_direct"] = False
    report["proxy"] = str(proxy_resolved[2]) if proxy_resolved else ""
    report["proxy_type"] = str(proxy_resolved[0]) if proxy_resolved else ""
    report["connection_mode"] = "unknown"

    session_file = str(Path(args.session_file).expanduser().resolve())
    client = None
    last_exc: Exception | None = None
    connect_variants: list[tuple[str, dict[str, Any], Any]] = []
    if proxy_resolved:
        connect_variants.append(("proxy", dict(client_kwargs), proxy_resolved))
    connect_variants.append(("direct", {}, None))

    used_mode = "unknown"
    for mode, kwargs, resolved in connect_variants:
        last_exc = None
        for attempt in range(2):
            try:
                client = TelegramClient(session_file, int(args.api_id), str(args.api_hash), **kwargs)
                await client.connect()
                used_mode = mode
                report["proxy_used"] = bool(mode == "proxy")
                report["proxy_fallback_to_direct"] = bool(proxy_resolved and mode == "direct")
                report["connection_mode"] = mode
                break
            except Exception as exc:
                last_exc = exc
                if client is not None:
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
                    client = None
                if attempt == 0 and _session_locked(exc):
                    _cleanup_session_locks(session_file)
                    await asyncio.sleep(0.3)
                    continue
                # Для прокси-варианта не падаем сразу — пробуем direct.
                break
        if client is not None:
            break
    if client is None:
        report["connection_mode"] = used_mode
        raise RuntimeError(str(last_exc or "failed to initialize Telegram client"))
    try:
        if not await client.is_user_authorized():
            report["status"] = "failed"
            report["error"] = "Session not authorized"
            Path(args.report_file).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
            return 2

        for item in leads:
            if not isinstance(item, dict):
                continue
            lead_id = int(item.get("lead_id") or 0)
            username = str(item.get("telegram") or "").strip()
            direction = str(item.get("direction") or "").strip().lower()
            if direction not in PRIMARY_DIRECTIONS:
                direction = ""
            ai_step = int(item.get("ai_step") or 0)
            current_status = str(item.get("status") or "").strip().lower()
            if not username:
                continue
            if not username.startswith("@"):
                username = "@" + username
            known_handles.add(username.strip().lower())
            result = {
                "lead_id": lead_id,
                "telegram": username,
                "status": "no_reply",
                "inbound_text": "",
                "outbound_text": "",
                "ai_step": ai_step,
                "error": "",
                "note": "",
                "decision_source": "",
                "llm_status": "",
                "llm_error": "",
                "llm_code": 0,
                "duplicate_reason": "",
            }
            report["processed"] += 1
            try:
                entity = await client.get_entity(username)
                if bool(getattr(entity, "broadcast", False)) or bool(getattr(entity, "megagroup", False)) or bool(getattr(entity, "gigagroup", False)):
                    result["status"] = "failed"
                    result["note"] = "non_user_entity"
                    report["details"].append(result)
                    continue
                messages = await client.get_messages(entity, limit=60)
                transcribed_texts, t_ok, t_failed = await _resolve_transcribed_texts(
                    client,
                    entity,
                    messages,
                    enabled=transcribe_audio,
                    max_per_dialog=transcribe_max_per_dialog,
                    poll_attempts=transcribe_poll_attempts,
                    poll_delay_ms=transcribe_poll_delay_ms,
                )
                report["transcribe_ok"] = int(report.get("transcribe_ok") or 0) + int(t_ok or 0)
                report["transcribe_failed"] = int(report.get("transcribe_failed") or 0) + int(t_failed or 0)
                chat_history = _build_history(messages, max_lines=80, transcribed_texts=transcribed_texts)
                inbound, inbound_dt, inbound_id, latest_outbound, latest_outbound_dt, latest_outbound_id = _extract_latest_messages(
                    messages,
                    transcribed_texts=transcribed_texts,
                )
                if not inbound:
                    # Жесткое правило: если лид молчит, повторно не пишем.
                    result["status"] = "no_reply"
                    result["note"] = "no_inbound_hard_stop"
                    report["details"].append(result)
                    continue

                result["inbound_text"] = inbound
                try:
                    await client.send_read_acknowledge(entity)
                except Exception:
                    pass
                if latest_outbound:
                    norm_q = _norm_text(inbound)
                    norm_a = _norm_text(latest_outbound)
                    if norm_q and norm_a and norm_q != norm_a:
                        report["learn_pairs"].append(
                            {
                                "question": inbound,
                                "answer": latest_outbound,
                                "direction": direction,
                                "source": "chat_observed",
                            }
                        )
                status = _classify(inbound)
                result["status"] = status
                report["updated"] += 1

                # Жесткий стоп только для blocked.
                # Если раньше стоял not_interested, но лид снова написал — продолжаем диалог.
                if current_status == "blocked":
                    result["note"] = "hard_stop_by_previous_status"
                    report["details"].append(result)
                    continue

                has_prev_outbound = bool(str(item.get("last_outbound_text") or "").strip())
                is_dup, dup_reason = _is_duplicate_inbound(
                    prev_inbound_text=str(item.get("last_inbound_text") or ""),
                    current_inbound_text=inbound,
                    has_prev_outbound=has_prev_outbound,
                    inbound_dt=inbound_dt,
                    latest_outbound_dt=latest_outbound_dt,
                    inbound_msg_id=inbound_id,
                    latest_outbound_msg_id=latest_outbound_id,
                )
                if is_dup:
                    result["note"] = "already_processed_same_inbound"
                    result["duplicate_reason"] = dup_reason
                    report["duplicate_blocked"] += 1
                    report["details"].append(result)
                    continue

                # Бизнес-правило: для polygon-аккаунтов отвечаем на любое входящее, кроме явного отказа/блока.
                ai_actionable = ai_enabled and status not in {"not_interested", "blocked", "no_reply"}
                should_send_ai = bool(ai_actionable and send_ai_enabled)
                if should_send_ai:
                    base_step = _next_step_text(script, direction, ai_step)
                    force_booking_cta = status == "call_booked"
                    llm_reply = _openai_reply(
                        provider=ai_provider,
                        base_url=ai_base_url,
                        proxy_auth=ai_proxy_auth,
                        api_key=openai_key,
                        http_proxy=ai_http_proxy,
                        model=openai_model,
                        script_step_text=base_step,
                        inbound_text=inbound,
                        direction=direction,
                        ai_step=ai_step,
                        previous_outbound_text=latest_outbound or "",
                        chat_history=chat_history,
                        force_booking_cta=force_booking_cta,
                        examples=examples,
                        guardrails=guardrails,
                        strict_mode=ai_strict_mode,
                        timeout_sec=ai_timeout_sec,
                    )
                    outgoing = str(llm_reply.get("text") or "")
                    result["decision_source"] = str(llm_reply.get("decision_source") or "")
                    result["llm_status"] = str(llm_reply.get("llm_status") or "")
                    result["llm_error"] = str(llm_reply.get("llm_error") or "")
                    result["llm_code"] = int(llm_reply.get("llm_code") or 0)
                    if result["llm_status"] == "ok":
                        report["llm_ok"] += 1
                    elif result["llm_status"] == "failed":
                        report["llm_failed"] += 1
                    if bool(llm_reply.get("needs_review")):
                        result["status"] = "needs_review"
                        result["note"] = "needs_review_llm_failed"
                        report["needs_review"] += 1
                        report["details"].append(result)
                        continue
                    outgoing = re.sub(r"\s+\n", "\n", outgoing).strip()
                    if outgoing and _norm_text(outgoing) != _norm_text(latest_outbound):
                        await client.send_message(entity, outgoing)
                        result["outbound_text"] = outgoing
                        result["ai_step"] = ai_step + 1
                        report["ai_sent"] += 1
                        report["learn_pairs"].append(
                            {
                                "question": inbound,
                                "answer": outgoing,
                                "direction": direction,
                                "source": "ai_sent",
                            }
                        )
                    elif outgoing:
                        result["note"] = "skip_duplicate_outbound"
                        result["duplicate_reason"] = "same_as_latest_outbound"
                        report["duplicate_blocked"] += 1
                elif ai_actionable and ai_enabled and not send_ai_enabled:
                    result["note"] = "ai_send_blocked_non_polygon"
                    report["ai_blocked_non_polygon"] += 1
                elif status == "replied":
                    result["note"] = "replied_without_action"
                report["details"].append(result)
            except Exception as exc:
                result["status"] = "failed"
                result["error"] = str(exc)
                report["details"].append(result)

        # Fallback: обрабатываем непрочитанные личные диалоги, даже если лид еще не был создан в owner_outreach_leads.
        # Это закрывает кейс "лид написал первым", когда запись в lead-таблице еще отсутствует.
        if ai_enabled and send_ai_enabled:
            fallback_started = time.monotonic()
            fallback_done = 0
            scanned_dialogs = 0
            async for dlg in client.iter_dialogs(limit=200):
                try:
                    if fallback_done >= fallback_max_dialogs:
                        break
                    if (time.monotonic() - fallback_started) >= fallback_max_seconds:
                        break
                    if not bool(getattr(dlg, "is_user", False)):
                        continue
                    entity = getattr(dlg, "entity", None)
                    if entity is None:
                        continue
                    if bool(getattr(entity, "bot", False)):
                        continue
                    scanned_dialogs += 1
                    if scanned_dialogs > fallback_max_scan:
                        break
                    handle = str(getattr(entity, "username", "") or "").strip()
                    if handle:
                        handle = "@" + handle.lstrip("@")
                    else:
                        handle = f"id:{int(getattr(entity, 'id', 0) or 0)}"
                    if handle.strip().lower() in known_handles:
                        continue

                    messages = await client.get_messages(entity, limit=80)
                    transcribed_texts, t_ok, t_failed = await _resolve_transcribed_texts(
                        client,
                        entity,
                        messages,
                        enabled=transcribe_audio,
                        max_per_dialog=transcribe_max_per_dialog,
                        poll_attempts=transcribe_poll_attempts,
                        poll_delay_ms=transcribe_poll_delay_ms,
                    )
                    report["transcribe_ok"] = int(report.get("transcribe_ok") or 0) + int(t_ok or 0)
                    report["transcribe_failed"] = int(report.get("transcribe_failed") or 0) + int(t_failed or 0)
                    chat_history = _build_history(messages, max_lines=80, transcribed_texts=transcribed_texts)
                    last_msg_from_lead = False
                    # Определяем, есть ли ситуация "лид написал, но ему еще не ответили":
                    # последнее содержательное сообщение в чате — входящее от лида.
                    for m in messages:
                        txt = _msg_text(m, transcribed_texts, history_mode=False)
                        if not txt:
                            continue
                        last_msg_from_lead = not bool(getattr(m, "out", False))
                        break
                    inbound, inbound_dt, inbound_id, latest_outbound, latest_outbound_dt, latest_outbound_id = _extract_latest_messages(
                        messages,
                        transcribed_texts=transcribed_texts,
                    )
                    if not inbound:
                        try:
                            await client.send_read_acknowledge(entity)
                        except Exception:
                            pass
                        continue
                    # Не трогаем диалоги, где после входящего уже есть исходящий ответ.
                    dup_fallback, _ = _is_duplicate_inbound(
                        prev_inbound_text="",
                        current_inbound_text=inbound,
                        has_prev_outbound=False,
                        inbound_dt=inbound_dt,
                        latest_outbound_dt=latest_outbound_dt,
                        inbound_msg_id=inbound_id,
                        latest_outbound_msg_id=latest_outbound_id,
                    )
                    if dup_fallback:
                        continue
                    # Если последнее сообщение не от лида, здесь отвечать не нужно.
                    if not last_msg_from_lead:
                        continue
                    # Для полигон-аккаунтов отвечаем на любые новые входящие (кроме явных отказов/блоков),
                    # даже если сообщение короткое или нетипичное.

                    status = _classify(inbound)
                    detail = {
                        "lead_id": 0,
                        "telegram": handle,
                        "status": status,
                        "inbound_text": inbound,
                        "outbound_text": "",
                        "ai_step": 0,
                        "error": "",
                        "note": "unread_dialog_fallback",
                        "decision_source": "",
                        "llm_status": "",
                        "llm_error": "",
                        "llm_code": 0,
                        "duplicate_reason": "",
                    }
                    report["processed"] += 1
                    report["updated"] += 1
                    if status in {"not_interested", "blocked"}:
                        try:
                            await client.send_read_acknowledge(entity)
                        except Exception:
                            pass
                        report["details"].append(detail)
                        continue
                    force_booking_cta = status == "call_booked"
                    llm_reply = _openai_reply(
                        provider=ai_provider,
                        base_url=ai_base_url,
                        proxy_auth=ai_proxy_auth,
                        api_key=openai_key,
                        http_proxy=ai_http_proxy,
                        model=openai_model,
                        script_step_text="",
                        inbound_text=inbound,
                        direction="",
                        ai_step=1,
                        previous_outbound_text=latest_outbound or "",
                        chat_history=chat_history,
                        force_booking_cta=force_booking_cta,
                        examples=examples,
                        guardrails=guardrails,
                        strict_mode=ai_strict_mode,
                        timeout_sec=ai_timeout_sec,
                    )
                    outgoing = str(llm_reply.get("text") or "")
                    detail["decision_source"] = str(llm_reply.get("decision_source") or "")
                    detail["llm_status"] = str(llm_reply.get("llm_status") or "")
                    detail["llm_error"] = str(llm_reply.get("llm_error") or "")
                    detail["llm_code"] = int(llm_reply.get("llm_code") or 0)
                    if detail["llm_status"] == "ok":
                        report["llm_ok"] += 1
                    elif detail["llm_status"] == "failed":
                        report["llm_failed"] += 1
                    if bool(llm_reply.get("needs_review")):
                        detail["status"] = "needs_review"
                        detail["note"] = "needs_review_llm_failed"
                        report["needs_review"] += 1
                        try:
                            await client.send_read_acknowledge(entity)
                        except Exception:
                            pass
                        report["details"].append(detail)
                        fallback_done += 1
                        continue
                    outgoing = _strip_emoji(re.sub(r"\s+\n", "\n", outgoing).strip())
                    if outgoing and _norm_text(outgoing) != _norm_text(latest_outbound):
                        await client.send_message(entity, outgoing)
                        detail["outbound_text"] = outgoing
                        report["ai_sent"] += 1
                    elif outgoing:
                        detail["note"] = "skip_duplicate_outbound"
                        detail["duplicate_reason"] = "same_as_latest_outbound"
                        report["duplicate_blocked"] += 1
                    fallback_done += 1
                    try:
                        await client.send_read_acknowledge(entity)
                    except Exception:
                        pass
                    report["details"].append(detail)
                except Exception as exc:
                    report["details"].append(
                        {
                            "lead_id": 0,
                            "telegram": str(getattr(getattr(dlg, "entity", None), "username", "") or "unknown"),
                            "status": "failed",
                            "inbound_text": "",
                            "outbound_text": "",
                            "ai_step": 0,
                            "error": str(exc),
                            "note": "unread_dialog_fallback_error",
                        }
                    )

        Path(args.report_file).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        return 0
    finally:
        await client.disconnect()


def main() -> int:
    p = argparse.ArgumentParser(description="Collect replies and optionally send AI next step.")
    p.add_argument("--api-id", type=int, required=True)
    p.add_argument("--api-hash", type=str, required=True)
    p.add_argument("--session-file", type=str, required=True)
    p.add_argument("--leads-file", type=str, required=True)
    p.add_argument("--script-file", type=str, required=True)
    p.add_argument("--report-file", type=str, required=True)
    p.add_argument("--ai-enabled", type=int, default=0)
    p.add_argument("--send-ai", type=int, default=1)
    p.add_argument("--account-type", type=str, default=ACCOUNT_TYPE_STANDARD)
    p.add_argument("--examples-file", type=str, default="")
    p.add_argument("--guardrails-file", type=str, default="")
    p.add_argument("--openai-api-key", type=str, default=os.getenv("OWNER_OPENAI_API_KEY", ""))
    p.add_argument("--openai-model", type=str, default="gpt-5.4-mini")
    p.add_argument("--ai-provider", type=str, default=os.getenv("OWNER_AI_PROVIDER", "openai"))
    p.add_argument("--ai-base-url", type=str, default=os.getenv("OWNER_AI_BASE_URL", ""))
    p.add_argument("--ai-proxy-auth", type=str, default=os.getenv("OWNER_AI_PROXY_AUTH", ""))
    p.add_argument("--ai-http-proxy", type=str, default=os.getenv("OWNER_AI_HTTP_PROXY", ""))
    p.add_argument("--ai-timeout-sec", type=int, default=30)
    p.add_argument("--ai-strict-mode", type=int, default=1)
    p.add_argument("--proxy", type=str, default=str(os.getenv("OWNER_TG_PROXY", "") or ""))
    p.add_argument("--transcribe-audio", type=int, default=int(os.getenv("OWNER_TG_TRANSCRIBE_AUDIO", "1") or 1))
    p.add_argument(
        "--transcribe-max-per-dialog",
        type=int,
        default=int(os.getenv("OWNER_TG_TRANSCRIBE_MAX_PER_DIALOG", "4") or 4),
    )
    p.add_argument(
        "--transcribe-poll-attempts",
        type=int,
        default=int(os.getenv("OWNER_TG_TRANSCRIBE_POLL_ATTEMPTS", "3") or 3),
    )
    p.add_argument(
        "--transcribe-poll-delay-ms",
        type=int,
        default=int(os.getenv("OWNER_TG_TRANSCRIBE_POLL_DELAY_MS", "1200") or 1200),
    )
    p.add_argument("--fallback-max-dialogs", type=int, default=20)
    p.add_argument("--fallback-max-seconds", type=int, default=60)
    args = p.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
