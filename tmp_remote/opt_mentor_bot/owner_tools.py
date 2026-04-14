from __future__ import annotations

import csv
import concurrent.futures
import html
import io
import json
import os
import re
import requests
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from aiogram.types import BufferedInputFile, FSInputFile
from telebot import TeleBot
from telebot.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from app.db import (
    repo_admins,
    repo_analytics,
    repo_owner_hh_jobs,
    repo_owner_outreach,
    repo_owner_reactions,
    repo_owner_tg_accounts,
    repo_settings,
    repo_states,
)
from app.services.owner_hh_parser import run_hh_parse
from app.services.owner_outreach_scripts import (
    DEFAULT_AI_AUTO_INTERVAL_SEC,
    DEFAULT_PER_ACCOUNT_MAX,
    DEFAULT_SEND_DELAY_SEC,
    DIRECTIONS,
    SEND_MODE_ALL,
    SEND_MODE_SELECTED,
    ai_guardrails as script_ai_guardrails,
    ai_auto_enabled,
    ai_auto_interval_sec,
    ai_enabled as script_ai_enabled,
    get_first_message,
    get_script4,
    get_step,
    openai_model,
    reaction_channel,
    reaction_channels,
    outreach_runtime_config,
    reactions_auto_enabled,
    set_ai_enabled,
    set_ai_guardrails,
    set_ai_auto_enabled,
    set_ai_auto_interval_sec,
    set_reaction_channel,
    set_reaction_channels,
    set_outreach_runtime_config,
    set_reactions_auto_enabled,
    set_script_step,
    reset_ai_guardrails,
)
from app.services.timeutils import iso, now


STATE_OWNER_TOOLS = "OWNER_TOOLS"
HH_BASE_LINK_KEY = "owner.outreach.hh.base_link"
TG_CLOUD_PASSWORD_KEY = "owner.tg.cloud_password"
AI_PROXY_HEALTH_KEY = "owner.outreach.ai.proxy.health"
AI_REPLY_MEMORY_FILE = "ai_reply_memory.json"
AI_GUARDRAILS_FILE = "ai_guardrails.json"
AI_SCENARIOS_FILE = "ai_test_scenarios.json"
ACCOUNT_TYPE_STANDARD = "standard"
ACCOUNT_TYPE_POLYGON = "polygon"
TG_USERNAME_RE = re.compile(r"^@[A-Za-z0-9_]{5,32}$")
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def init(bot: TeleBot, ctx: dict) -> None:
    cfg = ctx["cfg"]
    conn: sqlite3.Connection = ctx["conn"]
    tz = cfg.TZ

    # helpers
    def _owner_id() -> int:
        try:
            return int(getattr(cfg, "OWNER_SUPERUSER_ID", 6553771455) or 6553771455)
        except Exception:
            return 6553771455

    def _is_owner(user_id: int) -> bool:
        return int(user_id) == _owner_id()

    def _stamp() -> str:
        return datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    def _root() -> Path:
        raw = str(getattr(cfg, "OWNER_TOOLS_DIR", "owner_tools_data") or "owner_tools_data").strip()
        p = Path(raw)
        if not p.is_absolute():
            p = (PROJECT_ROOT / p).resolve()
        p.mkdir(parents=True, exist_ok=True)
        return p

    def _dir_parser() -> Path:
        p = _root() / "parser_runs"
        p.mkdir(parents=True, exist_ok=True)
        return p

    def _dir_tg() -> Path:
        p = _root() / "telegram"
        p.mkdir(parents=True, exist_ok=True)
        return p

    def _dir_react() -> Path:
        p = _root() / "reactions"
        p.mkdir(parents=True, exist_ok=True)
        return p

    def _parser_root() -> str:
        return str(getattr(cfg, "OWNER_PARSER_ROOT", "") or "").strip()

    def _openai_key() -> str:
        return (
            str(getattr(cfg, "OWNER_OPENAI_API_KEY", "") or "").strip()
            or os.getenv("OPENAI_API_KEY", "").strip()
        )

    def _openai_model() -> str:
        # Для стабильного качества в отписе фиксируем модель ответов.
        return str(getattr(cfg, "OWNER_OPENAI_MODEL", "") or "").strip() or "gpt-5.4-mini"

    def _ai_provider() -> str:
        raw = str(getattr(cfg, "OWNER_AI_PROVIDER", "proxy") or "proxy").strip().lower()
        return raw if raw in {"openai", "proxy"} else "proxy"

    def _ai_base_url() -> str:
        return str(getattr(cfg, "OWNER_AI_BASE_URL", "") or "").strip().rstrip("/")

    def _ai_proxy_auth() -> str:
        return str(getattr(cfg, "OWNER_AI_PROXY_AUTH", "") or "").strip()

    def _ai_http_proxy() -> str:
        return str(getattr(cfg, "OWNER_AI_HTTP_PROXY", "") or "").strip()

    def _ai_timeout_sec() -> int:
        try:
            return max(5, int(getattr(cfg, "OWNER_AI_REQUEST_TIMEOUT_SEC", 30) or 30))
        except Exception:
            return 30

    def _ai_strict_mode() -> bool:
        return bool(getattr(cfg, "OWNER_AI_STRICT_MODE", True))

    def _tg_proxy() -> str:
        return (
            str(os.getenv("OWNER_TG_PROXY") or "").strip()
            or str(os.getenv("OWNER_TG_MTPROXY") or "").strip()
        )

    def _probe_ai_proxy(owner_id: int, force: bool = False) -> dict[str, Any]:
        now_ts = int(time.time())
        key = f"{AI_PROXY_HEALTH_KEY}:{int(owner_id)}"
        cached = _loads_dict(repo_settings.get(conn, key, ""))
        if not force and cached:
            checked_ts = int(cached.get("checked_ts") or 0)
            # Кеш 60с, чтобы не долбить сеть при каждом открытии /t.
            if checked_ts > 0 and (now_ts - checked_ts) <= 60:
                return cached

        provider = _ai_provider()
        status: dict[str, Any] = {
            "provider": provider,
            "checked_ts": now_ts,
            "ok": False,
            "http_code": 0,
            "error": "",
        }
        http_proxy = _ai_http_proxy()
        proxy_cfg = {"http": http_proxy, "https": http_proxy} if http_proxy else None
        try:
            if provider == "proxy":
                base = _ai_base_url()
                if not base:
                    status["error"] = "OWNER_AI_BASE_URL не задан"
                else:
                    headers = {"Content-Type": "application/json"}
                    token = _ai_proxy_auth()
                    if token:
                        headers["Authorization"] = f"Bearer {token}"
                    r = requests.post(
                        f"{base}/v1/chat/completions",
                        headers=headers,
                        json={
                            "model": _openai_model(),
                            "messages": [
                                {"role": "system", "content": "Reply OK"},
                                {"role": "user", "content": "ping"},
                            ],
                            "temperature": 0.0,
                            "max_completion_tokens": 12,
                        },
                        timeout=_ai_timeout_sec(),
                        proxies=proxy_cfg,
                    )
                    status["http_code"] = int(r.status_code or 0)
                    status["ok"] = 200 <= r.status_code < 300
                    if not status["ok"]:
                        status["error"] = (r.text or "").strip()[:200]
            else:
                key_openai = _openai_key()
                if not key_openai:
                    status["error"] = "OWNER_OPENAI_API_KEY не задан"
                else:
                    r = requests.post(
                        "https://api.openai.com/v1/chat/completions",
                        headers={
                            "Authorization": f"Bearer {key_openai}",
                            "Content-Type": "application/json",
                        },
                        json={
                            "model": _openai_model(),
                            "messages": [{"role": "user", "content": "ping"}],
                            "temperature": 0.0,
                            "max_completion_tokens": 12,
                        },
                        timeout=_ai_timeout_sec(),
                        proxies=proxy_cfg,
                    )
                    status["http_code"] = int(r.status_code or 0)
                    status["ok"] = 200 <= r.status_code < 300
                    if not status["ok"]:
                        status["error"] = (r.text or "").strip()[:200]
        except Exception as exc:
            status["error"] = str(exc)
            status["ok"] = False
        repo_settings.set(conn, key, json.dumps(status, ensure_ascii=False))
        return status

    def _read_json(path: Path) -> dict:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _loads_dict(raw: str | None) -> dict[str, Any]:
        text = str(raw or "").strip()
        if not text:
            return {}
        try:
            payload = json.loads(text)
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    def _hh_base_link(owner_id: int) -> str:
        saved = str(repo_settings.get(conn, f"{HH_BASE_LINK_KEY}:{int(owner_id)}", "") or "").strip()
        if saved:
            return saved
        cfg_default = str(getattr(cfg, "OWNER_HH_BASE_LINK", "") or "").strip()
        if cfg_default:
            return cfg_default
        last = repo_owner_hh_jobs.last_by_owner(conn, int(owner_id))
        if last:
            recent = str(last["search_link"] or "").strip()
            if recent:
                return recent
        return ""

    def _set_hh_base_link(owner_id: int, value: str) -> None:
        repo_settings.set(conn, f"{HH_BASE_LINK_KEY}:{int(owner_id)}", str(value or "").strip())

    def _tg_cloud_password(owner_id: int, account_id: int) -> str:
        return str(
            repo_settings.get(
                conn,
                f"{TG_CLOUD_PASSWORD_KEY}:{int(owner_id)}:{int(account_id)}",
                "",
            )
            or ""
        ).strip()

    def _set_tg_cloud_password(owner_id: int, account_id: int, value: str) -> None:
        repo_settings.set(
            conn,
            f"{TG_CLOUD_PASSWORD_KEY}:{int(owner_id)}:{int(account_id)}",
            str(value or "").strip(),
        )

    def _account_type(raw: Any) -> str:
        val = str(raw or "").strip().lower()
        return val if val in {ACCOUNT_TYPE_STANDARD, ACCOUNT_TYPE_POLYGON} else ACCOUNT_TYPE_STANDARD

    def _account_type_label(raw: Any) -> str:
        return "полигон" if _account_type(raw) == ACCOUNT_TYPE_POLYGON else "стандарт"

    def _ai_memory_path() -> Path:
        return _dir_parser() / AI_REPLY_MEMORY_FILE

    def _ai_guardrails_path() -> Path:
        return _dir_parser() / AI_GUARDRAILS_FILE

    def _ai_scenarios_path() -> Path:
        return _dir_parser() / AI_SCENARIOS_FILE

    def _save_ai_guardrails_file(owner_id: int) -> Path:
        p = _ai_guardrails_path()
        rules = script_ai_guardrails(conn)
        p.write_text(json.dumps(rules, ensure_ascii=False, indent=2), encoding="utf-8")
        # keep per-owner cache marker (future-proof)
        repo_settings.set(conn, f"owner.outreach.ai.guardrails.file:{int(owner_id)}", str(p))
        return p

    def _default_ai_test_scenarios() -> list[dict[str, Any]]:
        return [
            {
                "name": "price_question",
                "direction": "java",
                "ai_step": 1,
                "chat_history": "Лид: Привет, тема актуальна. Подскажи по стоимости.",
                "inbound_text": "Сколько стоят ваши услуги в рублях?",
                "expect_any": ["ментор", "mock", "диагност", "сайт"],
                "forbid": ["пишу по делу", "написал по делу", "если тема актуальна"],
            },
            {
                "name": "negative_stop",
                "direction": "python",
                "ai_step": 2,
                "chat_history": "Лид: Привет\nМентор: ...",
                "inbound_text": "Спасибо, не актуально",
                "expect_any": ["понял", "спасибо"],
                "forbid": ["сколько у тебя", "в каком направлении", "пишу по делу"],
            },
            {
                "name": "how_can_you_help",
                "direction": "golang",
                "ai_step": 1,
                "chat_history": "Лид: Поиск работы актуален",
                "inbound_text": "Как именно вы можете мне помочь?",
                "expect_any": ["ментор", "резюме", "собесед", "mock"],
                "forbid": ["пишу по делу", "если тема актуальна"],
            },
            {
                "name": "hh_account_question",
                "direction": "frontend",
                "ai_step": 1,
                "chat_history": "Лид: Привет",
                "inbound_text": "А что у вас за аккаунт hh?",
                "expect_any": ["бизнес-аккаунт", "hh"],
                "forbid": ["пишу по делу", "если тема актуальна"],
            },
            {
                "name": "call_cta_only_when_requested",
                "direction": "java",
                "ai_step": 3,
                "chat_history": "Лид: Ок, можно созвон?",
                "inbound_text": "Да, давай созвонимся",
                "force_booking_cta": True,
                "expect_any": ["@Leonov_Care_bot", "резюме pdf", "созвон"],
                "forbid": ["пишу по делу"],
            },
        ]

    def _ensure_ai_scenarios_file() -> Path:
        p = _ai_scenarios_path()
        if not p.exists() or not p.is_file():
            p.write_text(
                json.dumps(_default_ai_test_scenarios(), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        return p

    def _ai_rules_preview() -> str:
        rules = script_ai_guardrails(conn)
        subs = [str(x or "").strip() for x in (rules.get("forbidden_substrings") or []) if str(x or "").strip()]
        regs = [str(x or "").strip() for x in (rules.get("forbidden_regex") or []) if str(x or "").strip()]
        max_chars = int(rules.get("max_message_chars") or 900)
        lines = [
            "<b>AI-запреты</b>",
            f"Макс. длина сообщения: <b>{max_chars}</b> символов",
            "",
            "<b>Запрещенные фразы</b>:",
        ]
        if not subs:
            lines.append("• нет")
        else:
            for i, phrase in enumerate(subs, start=1):
                lines.append(f"{i}. <code>{html.escape(phrase)}</code>")
        lines.append("")
        lines.append("<b>Regex-запреты</b>:")
        if not regs:
            lines.append("• нет")
        else:
            for i, pattern in enumerate(regs, start=1):
                lines.append(f"{i}. <code>{html.escape(pattern)}</code>")
        return "\n".join(lines)

    def _kb_ai_rules() -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup()
        kb.row(InlineKeyboardButton("➕ Добавить фразу-запрет", callback_data="t:out:ai:rules:add"))
        kb.row(InlineKeyboardButton("🧹 Сбросить к базовым", callback_data="t:out:ai:rules:reset"))
        kb.row(InlineKeyboardButton("🗑 Очистить все фразы", callback_data="t:out:ai:rules:clear"))
        kb.row(InlineKeyboardButton("⬅️ Назад", callback_data="t:out"))
        return kb
    def _load_ai_memory() -> list[dict[str, str]]:
        p = _ai_memory_path()
        if not p.exists() or not p.is_file():
            return []
        raw = _read_json(p)
        items = raw.get("items") if isinstance(raw, dict) else []
        if not isinstance(items, list):
            return []
        out: list[dict[str, str]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            q = str(item.get("question") or "").strip()
            a = str(item.get("answer") or "").strip()
            if not q or not a:
                continue
            out.append({"question": q, "answer": a})
        return out

    def _save_ai_memory(items: list[dict[str, str]]) -> None:
        p = _ai_memory_path()
        p.write_text(json.dumps({"items": items}, ensure_ascii=False, indent=2), encoding="utf-8")

    def _update_ai_memory(pairs: list[dict[str, Any]], max_items: int = 800) -> int:
        existing = _load_ai_memory()
        seen: set[str] = set()
        out: list[dict[str, str]] = []
        for row in existing:
            q = str(row.get("question") or "").strip()
            a = str(row.get("answer") or "").strip()
            key = f"{q.lower()}||{a.lower()}"
            if not q or not a or key in seen:
                continue
            seen.add(key)
            out.append({"question": q, "answer": a})
        added = 0
        for item in pairs:
            if not isinstance(item, dict):
                continue
            q = str(item.get("question") or "").strip()
            a = str(item.get("answer") or "").strip()
            if not q or not a:
                continue
            key = f"{q.lower()}||{a.lower()}"
            if key in seen:
                continue
            seen.add(key)
            out.append({"question": q, "answer": a})
            added += 1
        if len(out) > max_items:
            out = out[-max_items:]
        _save_ai_memory(out)
        return added

    def _out_cfg(owner_id: int) -> dict[str, Any]:
        return outreach_runtime_config(conn, int(owner_id))

    def _set_out_cfg(owner_id: int, patch: dict[str, Any]) -> dict[str, Any]:
        return set_outreach_runtime_config(conn, int(owner_id), patch)

    def _send_file(chat_id: int, path: str, caption: str) -> bool:
        p = Path(str(path or "").strip())
        if not p.exists() or not p.is_file():
            return False
        bot.send_document(chat_id, FSInputFile(str(p)), caption=caption)
        return True

    def _prepare_session_copy(session_file: str, account_id: int, purpose: str) -> str:
        src = Path(str(session_file or "").strip()).expanduser()
        try:
            src_resolved = src.resolve()
        except Exception:
            src_resolved = src
        if not src_resolved.exists() or not src_resolved.is_file():
            return str(src_resolved)
        tmp_dir = _dir_tg() / "tmp_sessions"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        stamp_tail = int(time.time() * 1000) % 1000
        dst = tmp_dir / f"acc_{int(account_id)}_{purpose}_{_stamp()}_{stamp_tail}.session"
        try:
            shutil.copy2(src_resolved, dst)
        except Exception:
            return str(src_resolved)
        for suffix in ("-journal", "-wal", "-shm"):
            sidecar = Path(str(src_resolved) + suffix)
            if not sidecar.exists() or not sidecar.is_file():
                continue
            try:
                shutil.copy2(sidecar, Path(str(dst) + suffix))
            except Exception:
                pass
        return str(dst)

    def _cleanup_session_copy(session_for_run: str, original_session: str) -> None:
        if not str(session_for_run or "").strip():
            return
        run_path = Path(str(session_for_run).strip()).expanduser()
        orig_path = Path(str(original_session or "").strip()).expanduser()
        try:
            run_resolved = run_path.resolve()
        except Exception:
            run_resolved = run_path
        try:
            orig_resolved = orig_path.resolve()
        except Exception:
            orig_resolved = orig_path
        if str(run_resolved) == str(orig_resolved):
            return
        for suffix in ("", "-journal", "-wal", "-shm"):
            p = run_resolved if suffix == "" else Path(str(run_resolved) + suffix)
            try:
                if p.exists():
                    p.unlink()
            except Exception:
                pass

    def _unlink_file(path: Path) -> None:
        try:
            if path.exists():
                path.unlink()
        except Exception:
            pass

    def _delete_account_local_artifacts(owner_id: int, account: sqlite3.Row) -> None:
        aid = int(account["id"] or 0)
        session_file = Path(str(account["session_file"] or "").strip()).expanduser()
        for suffix in ("", "-journal", "-wal", "-shm"):
            p = session_file if suffix == "" else Path(str(session_file) + suffix)
            _unlink_file(p)
        _unlink_file(_dir_tg() / f"qr_status_{aid}.json")
        _unlink_file(_dir_tg() / f"qr_{aid}.png")
        for p in _dir_tg().glob(f"qr_pwd_{aid}_*.txt"):
            _unlink_file(p)
        _set_tg_cloud_password(owner_id, aid, "")

    def _label_dir(d: str) -> str:
        return {"java": "Java", "frontend": "Frontend", "golang": "Golang", "python": "Python"}.get(d, d)

    def _hh_query_for_direction(d: str) -> str:
        return {
            "java": "(Java OR Spring) AND (developer OR backend OR разработчик) AND (telegram OR телеграм OR tg OR t.me OR @)",
            "frontend": "(React OR Frontend OR JavaScript OR TypeScript) AND (developer OR frontend OR разработчик) AND (telegram OR телеграм OR tg OR t.me OR @)",
            "golang": "(Golang OR Go) AND (developer OR backend OR разработчик) AND (telegram OR телеграм OR tg OR t.me OR @)",
            "python": "(Python OR Django OR FastAPI) AND (developer OR backend OR разработчик) AND (telegram OR телеграм OR tg OR t.me OR @)",
        }.get(
            str(d or "").strip().lower(),
            "(Python OR Django OR FastAPI) AND (developer OR backend OR разработчик) AND (telegram OR телеграм OR tg OR t.me OR @)",
        )

    def _hh_broad_query_for_direction(d: str) -> str:
        return {
            "java": "(Java OR Spring) AND (telegram OR телеграм OR tg OR t.me OR @)",
            "frontend": "(Frontend OR React OR JavaScript OR TypeScript) AND (telegram OR телеграм OR tg OR t.me OR @)",
            "golang": "(Golang OR Go) AND (telegram OR телеграм OR tg OR t.me OR @)",
            "python": "(Python OR Django OR FastAPI) AND (telegram OR телеграм OR tg OR t.me OR @)",
        }.get(str(d or "").strip().lower(), "(Python OR Django OR FastAPI) AND (telegram OR телеграм OR tg OR t.me OR @)")

    def _hh_wide_query_for_direction(d: str) -> str:
        return {
            "java": "Java OR Spring",
            "frontend": "Frontend OR React OR JavaScript OR TypeScript",
            "golang": "Golang OR Go OR Backend",
            "python": "Python OR Django OR FastAPI OR Backend",
        }.get(str(d or "").strip().lower(), "Python OR Django OR FastAPI")

    def _label_ai(enabled: bool) -> str:
        return "включен" if bool(enabled) else "выключен"

    def _label_account_auth(account: sqlite3.Row | None) -> str:
        return "авторизован" if _authorized(account) else "не авторизован"

    def _label_job_status(raw: str | None) -> str:
        status = str(raw or "").strip().lower()
        return {
            "created": "создана",
            "running": "в работе",
            "done": "завершена",
            "failed": "ошибка",
        }.get(status, status or "—")

    def _label_job_stage(raw: str | None) -> str:
        stage = str(raw or "").strip().lower()
        return {
            "queued": "в очереди",
            "parsing": "парсинг",
            "parsed": "парсинг завершен",
            "sending": "отправка",
            "sent": "отправка завершена",
            "done": "готово",
            "failed": "ошибка",
        }.get(stage, stage or "—")

    def _label_out_status(key: str) -> str:
        k = str(key or "").strip().lower()
        return {
            "sent": "отправлено",
            "send_failed": "ошибка отправки",
            "blocked": "ограничение/блок",
            "no_reply": "без ответа",
            "replied": "ответили",
            "not_interested": "не интересно",
            "interested": "интересно",
            "call_booked": "созвон назначен",
            "call_done": "созвон проведен",
            "needs_review": "нужен ручной разбор",
        }.get(k, k or "—")

    def _fmt_out_summary(summary: dict[str, int]) -> str:
        ordered = [
            "sent",
            "send_failed",
            "blocked",
            "no_reply",
            "replied",
            "not_interested",
            "interested",
            "call_booked",
            "call_done",
            "needs_review",
        ]
        parts: list[str] = []
        for key in ordered:
            parts.append(f"{_label_out_status(key)}:{int(summary.get(key, 0))}")
        return ", ".join(parts)

    def _fmt_react_summary(summary: dict[str, int]) -> str:
        ok = int(summary.get("ok", 0))
        failed = int(summary.get("failed", 0))
        return f"успешно:{ok}, ошибки:{failed}"

    def _auto_parse_limit(send_limit: int) -> int:
        n = max(int(send_limit or 0), 0)
        if n <= 0:
            return 1
        # Парсим только в объеме целевого числа для отписа (без запаса).
        return min(max(n, 1), 500)

    def _active_job(owner_id: int) -> sqlite3.Row | None:
        return repo_owner_hh_jobs.active_by_owner(conn, owner_id)

    def _out_diag_text(owner_id: int) -> str:
        lines = ["<b>Проверка /t -> Отпис</b>"]
        parser_root = _parser_root()
        parser_path = Path(parser_root) if parser_root else None
        if parser_root and parser_path and parser_path.exists() and parser_path.is_dir():
            lines.append(f"✅ Парсер: <code>{html.escape(str(parser_path))}</code>")
        elif parser_root:
            lines.append(f"❌ Парсер не найден: <code>{html.escape(parser_root)}</code>")
        else:
            lines.append("❌ `OWNER_PARSER_ROOT` не настроен")

        cookies_path = parser_path / "hh_cookies.json" if parser_path else None
        if cookies_path and cookies_path.exists() and cookies_path.is_file():
            lines.append("✅ hh_cookies.json: найден")
        else:
            lines.append("⚠️ hh_cookies.json: не найден (парсинг HH может не пройти)")

        base_link = _hh_base_link(owner_id)
        if base_link:
            lines.append("✅ Фильтр HH: сохранен")
        else:
            lines.append("⚠️ Фильтр HH не задан (будет поиск только по направлению)")

        acc = _active_account(owner_id)
        out_cfg = _out_cfg(owner_id)
        auth_accounts = _authorized_accounts(owner_id)
        polygon_auth_accounts = _authorized_polygon_accounts(owner_id)
        send_accounts = _resolve_send_accounts(owner_id, out_cfg)
        per_account_max = max(1, int(out_cfg.get("per_account_max") or DEFAULT_PER_ACCOUNT_MAX))
        delay_sec = max(0, int(out_cfg.get("delay_sec") or DEFAULT_SEND_DELAY_SEC))
        mode = str(out_cfg.get("send_mode") or SEND_MODE_ALL).strip().lower()
        if not acc:
            lines.append("❌ Аккаунт Telegram: не выбран")
        else:
            auth = _authorized(acc)
            lines.append(
                f"{'✅' if auth else '❌'} Аккаунт Telegram: "
                f"<b>{html.escape(str(acc['title']))}</b> ({'авторизован' if auth else 'не авторизован'})"
            )
        lines.append(f"ℹ️ Авторизованных аккаунтов для отписа: <b>{len(auth_accounts)}</b>")
        lines.append(f"ℹ️ Авторизованных полигон-аккаунтов: <b>{len(polygon_auth_accounts)}</b>")
        lines.append(
            "ℹ️ Автодиалоги AI: "
            f"<b>{'вкл' if ai_auto_enabled(conn) else 'выкл'}</b> "
            f"(интервал {ai_auto_interval_sec(conn)} сек)"
        )
        lines.append(
            "ℹ️ Режим аккаунтов: "
            f"<b>{'выбранные' if mode == SEND_MODE_SELECTED else 'все авторизованные'}</b>"
        )
        lines.append(f"ℹ️ Аккаунтов в текущем запуске: <b>{len(send_accounts)}</b>")
        lines.append(f"ℹ️ Пауза отправки: <b>{delay_sec} сек</b> на каждый аккаунт")
        lines.append(f"ℹ️ Лимит на аккаунт: <b>{per_account_max}</b> сообщений")

        if script_ai_enabled(conn):
            lines.append(
                f"ℹ️ AI провайдер: <b>{html.escape(_ai_provider())}</b>, strict: <b>{'вкл' if _ai_strict_mode() else 'выкл'}</b>"
            )
            if _ai_provider() == "openai":
                lines.append(
                    f"{'✅' if bool(_openai_key()) else '⚠️'} OpenAI ключ: "
                    + ("задан" if bool(_openai_key()) else "не задан")
                )
            else:
                base = _ai_base_url()
                lines.append(
                    f"{'✅' if base else '⚠️'} AI proxy URL: "
                    + (f"<code>{html.escape(base)}</code>" if base else "не задан")
                )
            probe = _probe_ai_proxy(owner_id)
            checked = datetime.utcfromtimestamp(int(probe.get("checked_ts") or 0)).strftime("%Y-%m-%d %H:%M:%S")
            code = int(probe.get("http_code") or 0)
            ok = bool(probe.get("ok"))
            err = str(probe.get("error") or "").strip()
            lines.append(
                f"{'✅' if ok else '⚠️'} AI прокси: "
                f"{'доступен' if ok else 'недоступен'} · last check {checked} UTC · HTTP {code or '—'}"
            )
            if err:
                lines.append(f"ℹ️ AI ошибка: <code>{html.escape(err[:180])}</code>")
            if len(polygon_auth_accounts) <= 0:
                lines.append("⚠️ AI отправка отключена: нет авторизованных аккаунтов типа «полигон».")
        else:
            lines.append("ℹ️ AI-шаги: выключены")

        active = _active_job(owner_id)
        if active:
            lines.append(
                f"⏳ Активная задача: #{int(active['id'])} "
                f"({_label_job_stage(str(active['stage'] or 'running'))})"
            )
        else:
            lines.append("✅ Активных задач нет")

        return "\n".join(lines)

    # account helpers
    def _ensure_env_account(owner_id: int) -> None:
        if repo_owner_tg_accounts.list_all(conn, owner_id):
            return
        api_id = int(getattr(cfg, "OWNER_TG_API_ID", 0) or 0)
        api_hash = str(getattr(cfg, "OWNER_TG_API_HASH", "") or "").strip()
        if api_id <= 0 or not api_hash:
            return
        repo_owner_tg_accounts.create(
            conn,
            tz,
            owner_tg_id=owner_id,
            title="main-env",
            account_type=ACCOUNT_TYPE_STANDARD,
            api_id=api_id,
            api_hash=api_hash,
            session_file=str(_dir_tg() / "account_env.session"),
            is_active=True,
        )

    def _active_account(owner_id: int) -> sqlite3.Row | None:
        _ensure_env_account(owner_id)
        acc = repo_owner_tg_accounts.active(conn, owner_id)
        if acc and _authorized(acc):
            return acc
        for row in repo_owner_tg_accounts.list_all(conn, owner_id):
            if _authorized(row):
                if not acc or int(acc["id"] or 0) != int(row["id"] or 0):
                    repo_owner_tg_accounts.set_active(conn, tz, owner_id, int(row["id"]))
                return repo_owner_tg_accounts.active(conn, owner_id)
        return acc

    def _authorized_accounts(owner_id: int) -> list[sqlite3.Row]:
        _ensure_env_account(owner_id)
        rows = repo_owner_tg_accounts.list_all(conn, owner_id)
        out: list[sqlite3.Row] = []
        for row in rows:
            if _authorized(row):
                out.append(row)
        return out

    def _authorized_polygon_accounts(owner_id: int) -> list[sqlite3.Row]:
        return [row for row in _authorized_accounts(owner_id) if _account_type(row["account_type"]) == ACCOUNT_TYPE_POLYGON]

    def _resolve_send_accounts(owner_id: int, cfg_payload: dict[str, Any]) -> list[sqlite3.Row]:
        mode = str(cfg_payload.get("send_mode") or SEND_MODE_ALL).strip().lower()
        selected_ids = {
            int(x)
            for x in (cfg_payload.get("selected_account_ids") or [])
            if isinstance(x, int) and int(x) > 0
        }
        auth_rows = _authorized_accounts(owner_id)
        if mode == SEND_MODE_SELECTED:
            return [row for row in auth_rows if int(row["id"] or 0) in selected_ids]
        return auth_rows

    def _send_capacity(accounts: list[sqlite3.Row], cfg_payload: dict[str, Any]) -> int:
        per_account_max = max(1, int(cfg_payload.get("per_account_max") or DEFAULT_PER_ACCOUNT_MAX))
        return len(accounts) * per_account_max

    def _authorized(account: sqlite3.Row | None) -> bool:
        if not account:
            return False
        sid = int(account["id"])
        session_exists = Path(str(account["session_file"] or "")).exists()
        status_path = _dir_tg() / f"qr_status_{sid}.json"
        status = _read_json(status_path)
        state = str(status.get("status") or "").strip().lower()
        if state == "authorized":
            return session_exists
        if state in {"error", "pending", "expired"}:
            return False
        return session_exists

    def _normalize_tg(raw: str | None) -> tuple[str, str]:
        text = str(raw or "").strip()
        if not text:
            return "", ""
        if text.startswith("https://t.me/") or text.startswith("http://t.me/"):
            text = text.rsplit("/", 1)[-1]
        text = text.split("?", 1)[0].split("#", 1)[0].strip()
        if not text:
            return "", ""
        if not text.startswith("@"):
            text = "@" + text
        if not TG_USERNAME_RE.match(text):
            return "", ""
        return text, text.lower()

    def _is_tg_limit_error(err_text: str | None) -> bool:
        low = str(err_text or "").strip().lower()
        return any(
            marker in low
            for marker in (
                "too many requests",
                "peerflood",
                "floodwait",
                "flood",
                "rate limit",
                "retry after",
                "wait of",
            )
        )

    def _is_terminal_unreachable_error(err_text: str | None) -> bool:
        low = str(err_text or "").strip().lower()
        return any(
            marker in low
            for marker in (
                "no user has",
                "nobody is using this username",
                "username not occupied",
                "cannot find any entity",
                "entity you requested not found",
                "invalid username",
                "username is unacceptable",
                "you can't write in this chat",
                "can't write in this chat",
                "chat write forbidden",
                "privacy settings",
                "user is deactivated",
                "user deactivated",
                "deleted account",
                "peer id invalid",
            )
        )

    def _kb_main() -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup()
        kb.row(InlineKeyboardButton("🛠 Модерирование", callback_data="t:mod"), InlineKeyboardButton("📣 Отпис", callback_data="t:out"))
        kb.row(InlineKeyboardButton("⭐ Реакции", callback_data="t:react"), InlineKeyboardButton("📊 Общая статистика", callback_data="t:stats"))
        kb.row(InlineKeyboardButton("ℹ️ Как пользоваться /t", callback_data="t:help"))
        return kb

    def _kb_mod() -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup()
        kb.row(InlineKeyboardButton("👥 Аккаунты", callback_data="t:mod:list"), InlineKeyboardButton("➕ Добавить", callback_data="t:mod:add"))
        kb.row(InlineKeyboardButton("🧪 Типы аккаунтов", callback_data="t:mod:types"))
        kb.row(InlineKeyboardButton("🔐 QR основного", callback_data="t:mod:qr"))
        kb.row(InlineKeyboardButton("🗑 Удалить неавторизованные", callback_data="t:mod:prune_unauth"))
        kb.row(InlineKeyboardButton("⬅️ Назад", callback_data="t:menu"))
        return kb

    def _kb_out() -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup()
        kb.row(InlineKeyboardButton("⚡ Быстрый запуск", callback_data="t:out:quick"), InlineKeyboardButton("🧲 Ручной запуск", callback_data="t:out:new"))
        kb.row(InlineKeyboardButton("⚙️ Настройки отписа", callback_data="t:out:cfg"))
        kb.row(InlineKeyboardButton("📈 Отчёт", callback_data="t:out:status"), InlineKeyboardButton("🩺 Проверка", callback_data="t:out:diag"))
        kb.row(InlineKeyboardButton("🧹 Сброс активной", callback_data="t:out:unlock"))
        kb.row(InlineKeyboardButton("🔗 Фильтр HH", callback_data="t:out:link"), InlineKeyboardButton("📄 Файлы", callback_data="t:out:files"))
        kb.row(InlineKeyboardButton("📨 Обработать ответы", callback_data="t:out:process"))
        kb.row(
            InlineKeyboardButton("🤖 AI-автоответы: вкл" if script_ai_enabled(conn) else "🤖 AI-автоответы: выкл", callback_data="t:out:ai"),
            InlineKeyboardButton("📝 Скрипт 4", callback_data="t:out:script"),
        )
        kb.row(
            InlineKeyboardButton("⏱ Интервал AI", callback_data="t:out:ai:interval"),
            InlineKeyboardButton("🚫 Запреты AI", callback_data="t:out:ai:rules"),
        )
        kb.row(InlineKeyboardButton("🧪 Прогнать AI-тесты", callback_data="t:out:ai:test"))
        kb.row(InlineKeyboardButton("⬅️ Назад", callback_data="t:menu"))
        return kb

    def _kb_out_cfg(owner_id: int) -> InlineKeyboardMarkup:
        cfg_payload = _out_cfg(owner_id)
        mode = str(cfg_payload.get("send_mode") or SEND_MODE_ALL).strip().lower()
        selected_ids = {
            int(x)
            for x in (cfg_payload.get("selected_account_ids") or [])
            if isinstance(x, int) and int(x) > 0
        }
        rows = repo_owner_tg_accounts.list_all(conn, owner_id)
        kb = InlineKeyboardMarkup()
        kb.row(
            InlineKeyboardButton(
                f"{'✅' if mode == SEND_MODE_ALL else '▫️'} Все аккаунты",
                callback_data="t:out:cfg:mode:all",
            ),
            InlineKeyboardButton(
                f"{'✅' if mode == SEND_MODE_SELECTED else '▫️'} Выбранные",
                callback_data="t:out:cfg:mode:selected",
            ),
        )
        kb.row(
            InlineKeyboardButton("⏱ Интервал", callback_data="t:out:cfg:delay"),
            InlineKeyboardButton("📌 Лимит/аккаунт", callback_data="t:out:cfg:max"),
        )
        if rows:
            for row in rows:
                aid = int(row["id"] or 0)
                mark = "✅" if aid in selected_ids else "▫️"
                auth = "🟢" if _authorized(row) else "⚪"
                kb.row(
                    InlineKeyboardButton(
                        f"{mark} {auth} {row['title']} (id:{aid})",
                        callback_data=f"t:out:cfg:acc:{aid}",
                    )
                )
        kb.row(InlineKeyboardButton("⬅️ Назад", callback_data="t:out"))
        return kb

    def _kb_out_files() -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup()
        kb.row(InlineKeyboardButton("📄 Контакты CSV", callback_data="t:out:files:csv"), InlineKeyboardButton("📝 Контакты TXT", callback_data="t:out:files:txt"))
        kb.row(InlineKeyboardButton("📈 Отчёт", callback_data="t:out:files:report"))
        kb.row(InlineKeyboardButton("⬅️ Назад", callback_data="t:out"))
        return kb

    def _kb_react() -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup()
        auto_on = reactions_auto_enabled(conn)
        kb.row(
            InlineKeyboardButton(
                "🟢 Авто: новые посты" if auto_on else "⚪ Авто: выкл",
                callback_data="t:react:auto",
            ),
        )
        kb.row(InlineKeyboardButton("⭐ Реакции (5)", callback_data="t:react:run:5"), InlineKeyboardButton("⭐ Реакции (20)", callback_data="t:react:run:20"))
        kb.row(InlineKeyboardButton("📄 Экспорт логов", callback_data="t:react:export"))
        kb.row(InlineKeyboardButton("⚙️ Каналы", callback_data="t:react:channels"))
        kb.row(InlineKeyboardButton("⬅️ Назад", callback_data="t:menu"))
        return kb

    def _kb_react_channels() -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup()
        for ch in reaction_channels(conn):
            kb.row(InlineKeyboardButton(f"❌ @{ch}", callback_data=f"t:react:ch:del:{ch}"))
        kb.row(InlineKeyboardButton("➕ Добавить канал", callback_data="t:react:ch:add"))
        kb.row(InlineKeyboardButton("⬅️ Назад", callback_data="t:react"))
        return kb

    def _kb_stats() -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup()
        kb.row(InlineKeyboardButton("📄 Экспорт статистики", callback_data="t:stats:export"), InlineKeyboardButton("🧹 Убрать всех админов", callback_data="t:stats:clear_admins"))
        kb.row(InlineKeyboardButton("⬅️ Назад", callback_data="t:menu"))
        return kb

    def _kb_dirs(prefix: str, back_cb: str) -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup()
        kb.row(InlineKeyboardButton("Java", callback_data=f"{prefix}:java"), InlineKeyboardButton("Frontend", callback_data=f"{prefix}:frontend"))
        kb.row(InlineKeyboardButton("Golang", callback_data=f"{prefix}:golang"), InlineKeyboardButton("Python", callback_data=f"{prefix}:python"))
        kb.row(InlineKeyboardButton("⬅️ Назад", callback_data=back_cb))
        return kb

    def _script_steps_for_direction(direction: str) -> list[str]:
        d = str(direction or "").strip().lower()
        if d not in DIRECTIONS:
            d = "python"
        steps = list(get_script4(conn).get(d) or [])
        return steps if steps else [get_step(conn, d, 0)]

    def _kb_script_steps(direction: str) -> InlineKeyboardMarkup:
        d = str(direction or "").strip().lower()
        steps = _script_steps_for_direction(d)
        kb = InlineKeyboardMarkup()
        row: list[InlineKeyboardButton] = []
        for idx, _ in enumerate(steps):
            row.append(InlineKeyboardButton(f"Шаг {idx + 1}", callback_data=f"t:out:script:step:{d}:{idx}"))
            if len(row) == 2:
                kb.row(*row)
                row = []
        if row:
            kb.row(*row)
        kb.row(InlineKeyboardButton("⬅️ Назад", callback_data="t:out:script"))
        return kb

    def _kb_limit_pick(prefix: str, max_value: int, allow_zero: bool, back_cb: str) -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup()
        cap = max(int(max_value or 0), 0)
        base = [10, 20, 25, 30, 50]
        options: list[int] = [0] if allow_zero else []
        for n in base:
            if n <= cap and n not in options:
                options.append(n)
        if cap > 0 and cap not in options:
            options.append(cap)
        row: list[InlineKeyboardButton] = []
        for n in options:
            row.append(InlineKeyboardButton(str(n), callback_data=f"{prefix}:{n}"))
            if len(row) == 3:
                kb.row(*row)
                row = []
        if row:
            kb.row(*row)
        kb.row(InlineKeyboardButton("Свое число", callback_data=f"{prefix}:custom"))
        kb.row(InlineKeyboardButton("⬅️ Назад", callback_data=back_cb))
        return kb

    def _kb_accounts_pick(owner_id: int) -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup()
        for r in repo_owner_tg_accounts.list_all(conn, owner_id):
            mark = "⭐" if int(r["is_active"] or 0) == 1 else "▫️"
            tmark = "🧪" if _account_type(r["account_type"]) == ACCOUNT_TYPE_POLYGON else "👤"
            kb.row(
                InlineKeyboardButton(
                    f"{mark} {tmark} {r['title']} (id:{int(r['id'])})",
                    callback_data=f"t:mod:set:{int(r['id'])}",
                )
            )
        kb.row(InlineKeyboardButton("⬅️ Назад", callback_data="t:mod"))
        return kb

    def _kb_accounts_types(owner_id: int) -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup()
        for r in repo_owner_tg_accounts.list_all(conn, owner_id):
            aid = int(r["id"] or 0)
            kind = _account_type(r["account_type"])
            mark = "🧪" if kind == ACCOUNT_TYPE_POLYGON else "👤"
            next_kind = ACCOUNT_TYPE_STANDARD if kind == ACCOUNT_TYPE_POLYGON else ACCOUNT_TYPE_POLYGON
            kb.row(
                InlineKeyboardButton(
                    f"{mark} {r['title']} (id:{aid})",
                    callback_data=f"t:mod:type:set:{aid}:{next_kind}",
                )
            )
        kb.row(InlineKeyboardButton("⬅️ Назад", callback_data="t:mod"))
        return kb

    def _show_main(chat_id: int, owner_id: int, msg_id: int | None = None) -> None:
        acc = _active_account(owner_id)
        active = _active_job(owner_id)
        acc_text = html.escape(str(acc["title"])) if acc else "не выбран"
        active_line = (
            f"\nАктивная задача: <b>#{int(active['id'])}</b> ({_label_job_stage(str(active['stage'] or 'running'))})"
            if active
            else "\nАктивная задача: <b>нет</b>"
        )
        text = (
            "<b>Панель владельца /t</b>\n"
            f"Основной аккаунт: <b>{acc_text}</b>\n"
            f"AI шаги: <b>{_label_ai(script_ai_enabled(conn))}</b>"
            + active_line
        )
        if msg_id is None:
            bot.send_message(chat_id, text, reply_markup=_kb_main())
        else:
            bot.edit_message_text(text, chat_id, msg_id, reply_markup=_kb_main())

    def _show_mod(chat_id: int, owner_id: int, msg_id: int | None = None) -> None:
        rows = repo_owner_tg_accounts.list_all(conn, owner_id)
        lines = ["<b>Модерирование</b>", "", "⭐ — основной аккаунт (для QR/по умолчанию)"]
        if not rows:
            lines.append("Аккаунтов нет.")
        polygon_count = 0
        for r in rows:
            mark = "⭐" if int(r["is_active"] or 0) == 1 else "▫️"
            auth = _label_account_auth(r)
            kind = _account_type(r["account_type"])
            if kind == ACCOUNT_TYPE_POLYGON:
                polygon_count += 1
            lines.append(
                f"• {mark} {html.escape(str(r['title']))} "
                f"(id:{int(r['id'])}, {auth}, тип: {_account_type_label(kind)})"
            )
        lines.append("")
        lines.append(
            f"Полигон-аккаунтов: <b>{polygon_count}</b> / "
            f"авторизованных полигонов: <b>{len(_authorized_polygon_accounts(owner_id))}</b>"
        )
        text = "\n".join(lines)
        if msg_id is None:
            bot.send_message(chat_id, text, reply_markup=_kb_mod())
        else:
            bot.edit_message_text(text, chat_id, msg_id, reply_markup=_kb_mod())

    def _show_out(chat_id: int, owner_id: int, msg_id: int | None = None) -> None:
        last = repo_owner_hh_jobs.last_by_owner(conn, owner_id)
        active = _active_job(owner_id)
        s = repo_owner_outreach.summary(conn, owner_id)
        pool = repo_owner_outreach.pool_summary(conn, owner_id)
        cfg_payload = _out_cfg(owner_id)
        mode = str(cfg_payload.get("send_mode") or SEND_MODE_ALL).strip().lower()
        mode_label = "выбранные аккаунты" if mode == SEND_MODE_SELECTED else "все авторизованные"
        send_accounts = _resolve_send_accounts(owner_id, cfg_payload)
        send_capacity = _send_capacity(send_accounts, cfg_payload)
        delay_sec = max(0, int(cfg_payload.get("delay_sec") or DEFAULT_SEND_DELAY_SEC))
        per_account_max = max(1, int(cfg_payload.get("per_account_max") or DEFAULT_PER_ACCOUNT_MAX))
        ai_auto_on = script_ai_enabled(conn)
        ai_auto_sec = ai_auto_interval_sec(conn)
        poly_accounts = _authorized_polygon_accounts(owner_id)
        base_link = _hh_base_link(owner_id)
        text = "<b>Отпис</b>\n"
        text += (
            f"Ссылка-фильтр HH: <code>{html.escape(base_link)}</code>\n\n"
            if base_link
            else "Ссылка-фильтр HH: <i>не задана</i>\n\n"
        )
        text += (
            f"Режим аккаунтов: <b>{mode_label}</b>\n"
            f"Аккаунтов для запуска: <b>{len(send_accounts)}</b>\n"
            f"Лимит на аккаунт: <b>{per_account_max}</b>\n"
            f"Макс. отпис за запуск: <b>{send_capacity}</b>\n"
            f"Интервал между сообщениями: <b>{delay_sec} сек</b>\n"
            f"AI-автоответы: <b>{'вкл' if ai_auto_on else 'выкл'}</b> (интервал {ai_auto_sec} сек)\n"
            f"AI-провайдер: <b>{html.escape(_ai_provider())}</b>, strict: <b>{'вкл' if _ai_strict_mode() else 'выкл'}</b>\n"
            f"Полигон-аккаунтов: <b>{len(poly_accounts)}</b>\n"
            f"Пул контактов: всего <b>{pool['total']}</b>, готово <b>{pool['ready']}</b>, отписано <b>{pool['contacted']}</b>\n\n"
        )
        if active:
            text += (
                f"Активная задача: <b>#{int(active['id'])}</b> "
                f"[{_label_job_status(active['status'])} / {_label_job_stage(active['stage'])}]\n"
                f"Направление: {html.escape(_label_dir(str(active['direction'] or 'python')))}\n"
                f"Распарсено: {int(active['parsed_total'] or 0)}, найдено TG: {int(active['tg_found_total'] or 0)}, "
                f"отправка: {int(active['send_success'] or 0)}/{int(active['send_failed'] or 0)}\n\n"
            )
        if last and (not active or int(last["id"] or 0) != int(active["id"] or 0)):
            text += (
                f"Последняя задача: #{int(last['id'])} "
                f"[{_label_job_status(last['status'])} / {_label_job_stage(last['stage'])}]\n"
                f"Направление: {html.escape(_label_dir(str(last['direction'] or 'python')))}\n"
                f"Распарсено: {int(last['parsed_total'] or 0)}, найдено TG: {int(last['tg_found_total'] or 0)}, "
                f"отправка: {int(last['send_success'] or 0)}/{int(last['send_failed'] or 0)}\n\n"
            )
        text += "Статусы лидов:\n" + _fmt_out_summary(s)
        if msg_id is None:
            bot.send_message(chat_id, text, reply_markup=_kb_out())
        else:
            bot.edit_message_text(text, chat_id, msg_id, reply_markup=_kb_out())

    def _show_react(chat_id: int, owner_id: int, msg_id: int | None = None) -> None:
        channels = reaction_channels(conn)
        channel_text = ", ".join(f"@{html.escape(ch)}" for ch in channels) if channels else "—"
        s = repo_owner_reactions.summary(conn, owner_id)
        auto = reactions_auto_enabled(conn)
        auth_count = len(_authorized_accounts(owner_id))
        text = (
            f"<b>Реакции</b>\nКаналы: <code>{channel_text}</code>\n"
            f"Авторизованных аккаунтов: <b>{auth_count}</b>\n"
            f"Авто на новые посты: <b>{'вкл' if auto else 'выкл'}</b> (поток Telethon)\n"
            f"{_fmt_react_summary(s)}"
        )
        if msg_id is None:
            bot.send_message(chat_id, text, reply_markup=_kb_react())
        else:
            bot.edit_message_text(text, chat_id, msg_id, reply_markup=_kb_react())

    def _base_admin_ids() -> set[int]:
        out: set[int] = set()
        for raw in getattr(cfg, "ADMIN_IDS", []) or []:
            try:
                uid = int(raw)
            except Exception:
                continue
            if uid > 0:
                out.add(uid)
        return out

    def _admin_event_count(days: int = 30) -> int:
        row = conn.execute(
            "SELECT COUNT(1) AS c "
            "FROM event_log e "
            "JOIN user_profiles u ON u.user_id=e.user_id "
            "WHERE COALESCE(u.role_type,'')='admin' "
            "AND julianday('now') - julianday(e.created_at) <= ?",
            (float(max(int(days), 1)),),
        ).fetchone()
        return int(row["c"] or 0) if row else 0

    def _admin_events(limit: int = 1500) -> list[dict[str, Any]]:
        rows = conn.execute(
            "SELECT e.created_at, e.event_name, e.user_id, e.payload_json, COALESCE(u.username,'') AS username "
            "FROM event_log e "
            "JOIN user_profiles u ON u.user_id=e.user_id "
            "WHERE COALESCE(u.role_type,'')='admin' "
            "ORDER BY e.id DESC LIMIT ?",
            (max(1, min(int(limit or 0), 10000)),),
        ).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            payload: dict[str, Any] = {}
            raw = str(r["payload_json"] or "").strip()
            if raw:
                try:
                    val = json.loads(raw)
                    if isinstance(val, dict):
                        payload = val
                except Exception:
                    payload = {"_raw": raw[:500]}
            out.append(
                {
                    "created_at": str(r["created_at"] or ""),
                    "event_name": str(r["event_name"] or ""),
                    "user_id": int(r["user_id"] or 0),
                    "username": str(r["username"] or ""),
                    "payload": payload,
                }
            )
        return out

    def _outreach_account_stats(owner_id: int, days: int = 30) -> list[dict[str, Any]]:
        span_days = max(1, int(days or 30))
        cutoff = iso(now(tz) - timedelta(days=span_days))
        rows = conn.execute(
            "SELECT sender_payload_json FROM owner_hh_jobs "
            "WHERE owner_tg_id=? AND created_at>=? "
            "ORDER BY id DESC LIMIT 3000",
            (int(owner_id), cutoff),
        ).fetchall()
        by_acc: dict[int, dict[str, Any]] = {}
        for row in rows:
            payload = _loads_dict(str(row["sender_payload_json"] or ""))
            per_acc = payload.get("per_account") if isinstance(payload, dict) else None
            details = payload.get("details") if isinstance(payload, dict) else None
            if not isinstance(per_acc, list):
                continue
            for item in per_acc:
                if not isinstance(item, dict):
                    continue
                aid = int(item.get("account_id") or 0)
                if aid <= 0:
                    continue
                title = str(item.get("account_title") or f"account-{aid}").strip() or f"account-{aid}"
                slot = by_acc.setdefault(
                    aid,
                    {
                        "account_id": aid,
                        "account_title": title,
                        "campaigns": 0,
                        "attempted": 0,
                        "success": 0,
                        "failed": 0,
                        "blocked": 0,
                    },
                )
                slot["account_title"] = title
                slot["campaigns"] += 1
                slot["attempted"] += int(item.get("attempted") or 0)
                slot["success"] += int(item.get("success") or 0)
                slot["failed"] += int(item.get("failed") or 0)
            if isinstance(details, list):
                for drow in details:
                    if not isinstance(drow, dict):
                        continue
                    aid = int(drow.get("account_id") or 0)
                    if aid <= 0 or aid not in by_acc:
                        continue
                    d_status = str(drow.get("status") or "").strip().lower()
                    d_err = str(drow.get("error") or "").strip()
                    if d_status == "blocked" or _is_tg_limit_error(d_err):
                        by_acc[aid]["blocked"] += 1
        out = list(by_acc.values())
        out.sort(
            key=lambda x: (
                -int(x.get("success") or 0),
                -int(x.get("attempted") or 0),
                str(x.get("account_title") or ""),
            )
        )
        return out

    def _show_stats(chat_id: int, owner_id: int, msg_id: int | None = None) -> None:
        users = repo_analytics.user_totals(conn)
        dau = repo_analytics.dau_wau_mau(conn, tz)
        out_sum = repo_owner_outreach.summary(conn, owner_id)
        pool_sum = repo_owner_outreach.pool_summary(conn, owner_id)
        out_dirs = repo_owner_outreach.direction_stats(conn, owner_id)
        script_stats = repo_owner_outreach.script_stats(conn, owner_id)
        acc_stats = _outreach_account_stats(owner_id, days=30)
        react = repo_owner_reactions.summary(conn, owner_id)
        admin_interactions = _admin_event_count(days=30)
        lines = ["<b>Общая статистика</b>", ""]
        lines.append(
            f"Пользователи: всего={users['all']}, админов={users['admin']}, "
            f"учеников={users['student']}, обычных={users['regular']}"
        )
        lines.append(
            f"Активность: за сутки={int(dau.get('dau', 0))}, за 7 дней={int(dau.get('wau', 0))}, "
            f"за 30 дней={int(dau.get('mau', 0))}, липкость={float(dau.get('stickiness', 0.0))}%"
        )
        lines.append(f"Действия админов (30д): {admin_interactions}")
        lines.append("Отпис: " + _fmt_out_summary(out_sum))
        lines.append(
            f"Пул контактов: всего={pool_sum['total']}, готово={pool_sum['ready']}, "
            f"отписано={pool_sum['contacted']}"
        )
        lines.append("Реакции: " + _fmt_react_summary(react))
        lines.append("По направлениям:")
        if not out_dirs:
            lines.append("• данных нет")
        else:
            for r in out_dirs:
                lines.append(
                    f"• {_label_dir(str(r.get('direction') or ''))}: всего={r.get('total', 0)}, "
                    f"интересно={r.get('interested', 0)}, созвон назначен={r.get('call_booked', 0)}"
                )
        lines.append("По скриптам:")
        if not script_stats:
            lines.append("• данных нет")
        else:
            for r in script_stats:
                lines.append(f"• {r['script_version']}: кампаний={r['campaigns']}, доставляемость={r['delivery_rate_pct']}%")
        lines.append("По аккаунтам (30д):")
        if not acc_stats:
            lines.append("• данных нет")
        else:
            for r in acc_stats:
                lines.append(
                    "• "
                    + f"{str(r.get('account_title') or '')}: "
                    + f"успех={int(r.get('success') or 0)}, "
                    + f"ошибки={int(r.get('failed') or 0)}, "
                    + f"блоки={int(r.get('blocked') or 0)}, "
                    + f"попытки={int(r.get('attempted') or 0)}, "
                    + f"кампаний={int(r.get('campaigns') or 0)}"
                )
        text = "\n".join(lines)
        if msg_id is None:
            bot.send_message(chat_id, text, reply_markup=_kb_stats())
        else:
            bot.edit_message_text(text, chat_id, msg_id, reply_markup=_kb_stats())

    def _start_qr(chat_id: int, owner_id: int, account_id: int | None = None, cloud_password: str | None = None) -> None:
        account = repo_owner_tg_accounts.get(conn, int(account_id)) if account_id else _active_account(owner_id)
        if not account:
            bot.send_message(chat_id, "Нет основного аккаунта.")
            return
        if int(account["owner_tg_id"] or 0) != int(owner_id):
            bot.send_message(chat_id, "Аккаунт не принадлежит владельцу.")
            return
        script = PROJECT_ROOT / "scripts" / "owner_tg_qr_login.py"
        if not script.exists():
            bot.send_message(chat_id, "Скрипт QR не найден.")
            return
        aid = int(account["id"])
        pwd_value = str(cloud_password or "").strip() or _tg_cloud_password(owner_id, aid)
        status_file = _dir_tg() / f"qr_status_{aid}.json"
        qr_file = _dir_tg() / f"qr_{aid}.png"
        pwd_file = _dir_tg() / f"qr_pwd_{aid}_{_stamp()}.txt"
        for p in (status_file, qr_file):
            try:
                if p.exists():
                    p.unlink()
            except Exception:
                pass
        cmd = [
            sys.executable,
            str(script),
            "--api-id",
            str(int(account["api_id"] or 0)),
            "--api-hash",
            str(account["api_hash"] or ""),
            "--session-file",
            str(account["session_file"]),
            "--status-file",
            str(status_file),
            "--qr-file",
            str(qr_file),
            "--timeout",
            str(int(getattr(cfg, "OWNER_TG_LOGIN_TIMEOUT_SEC", 180) or 180)),
        ]
        proxy_raw = _tg_proxy()
        if proxy_raw:
            cmd.extend(["--proxy", proxy_raw])
        if pwd_value:
            try:
                pwd_file.write_text(pwd_value, encoding="utf-8")
                cmd.extend(["--cloud-password-file", str(pwd_file)])
            except Exception:
                pass
        subprocess.Popen(cmd, cwd=str(PROJECT_ROOT))
        for _ in range(40):
            if status_file.exists() or qr_file.exists():
                break
            time.sleep(0.2)
        st = _read_json(status_file)
        if str(st.get("status") or "").strip().lower() == "authorized":
            bot.send_message(chat_id, "Аккаунт уже авторизован ✅")
            return
        if str(st.get("status") or "").strip().lower() == "error":
            msg = str(st.get("error") or "unknown")
            if "cloud password" in msg.lower() or "2fa" in msg.lower() or "password required" in msg.lower():
                bot.send_message(chat_id, "Нужен облачный пароль Telegram. Запусти QR заново и введи пароль.")
            else:
                bot.send_message(chat_id, f"Ошибка: <code>{html.escape(msg)}</code>")
            return
        if qr_file.exists():
            bot.send_photo(chat_id, FSInputFile(str(qr_file)), caption="Сканируй QR в Telegram -> Настройки -> Устройства.")
        else:
            bot.send_message(chat_id, "QR пока не готов, попробуй ещё раз.")

    def _sync_leads(
        job_id: int,
        owner_id: int,
        direction: str,
        parse_payload: dict[str, Any],
        send_payload: dict[str, Any],
    ) -> None:
        for row in send_payload.get("details") or []:
            if not isinstance(row, dict):
                continue
            tg, tg_norm = _normalize_tg(row.get("telegram"))
            if not tg_norm:
                continue
            status = str(row.get("status") or "").strip().lower()
            err = str(row.get("error") or "").strip()
            err_l = err.lower()
            is_limit = _is_tg_limit_error(err)
            is_terminal_invalid = _is_terminal_unreachable_error(err)
            if status == "sent":
                st = repo_owner_outreach.LEAD_SENT
            elif status == "blocked" or is_limit:
                st = repo_owner_outreach.LEAD_BLOCKED
            elif status == "failed":
                st = (
                    repo_owner_outreach.LEAD_BLOCKED
                    if any(x in err_l for x in ("privacy", "forbidden", "blocked"))
                    else repo_owner_outreach.LEAD_SEND_FAILED
                )
            else:
                # skipped/unknown статусы не считаем отправкой
                continue
            repo_owner_outreach.add_or_update_lead(
                conn,
                tz,
                job_id,
                tg,
                direction,
                st,
                send_error=err or None,
                ai_step=1 if st == repo_owner_outreach.LEAD_SENT else 0,
            )
            if st == repo_owner_outreach.LEAD_SENT:
                repo_owner_outreach.mark_contacted(
                    conn,
                    tz,
                    int(owner_id),
                    tg,
                    job_id=int(job_id),
                    account_id=int(row.get("account_id") or 0) or None,
                )
            elif is_terminal_invalid and st in {
                repo_owner_outreach.LEAD_SEND_FAILED,
                repo_owner_outreach.LEAD_BLOCKED,
            }:
                repo_owner_outreach.mark_pool_status(
                    conn,
                    tz,
                    int(owner_id),
                    tg,
                    repo_owner_outreach.POOL_INVALID,
                )

    def _read_json_list(path: str) -> list[dict[str, Any]]:
        p = Path(str(path or "").strip())
        if not p.exists() or not p.is_file():
            return []
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return []
        if not isinstance(raw, list):
            return []
        out: list[dict[str, Any]] = []
        for item in raw:
            if isinstance(item, dict):
                out.append(item)
        return out

    def _merge_parse_outputs(job_id: int, chunks: list[dict[str, Any]]) -> dict[str, Any]:
        contacts_map: dict[str, dict[str, Any]] = {}
        resumes_map: dict[str, dict[str, Any]] = {}
        resume_fallback_idx = 0
        for ch in chunks:
            contacts_rows = _read_json_list(str(ch.get("contacts_json_path") or ""))
            for row in contacts_rows:
                tg = str(row.get("telegram") or "").strip()
                if not tg:
                    continue
                key = tg.lower()
                if key in contacts_map:
                    continue
                contacts_map[key] = {
                    "index": len(contacts_map) + 1,
                    "telegram": tg,
                    "name": str(row.get("name") or "").strip(),
                    "resume_id": str(row.get("resume_id") or "").strip(),
                    "resume_link": str(row.get("resume_link") or "").strip(),
                }
            resumes_rows = _read_json_list(str(ch.get("resumes_path") or ""))
            for row in resumes_rows:
                rid = str(row.get("id") or "").strip()
                rlink = str(row.get("link") or "").strip()
                if rid:
                    rkey = f"id:{rid.lower()}"
                elif rlink:
                    rkey = f"link:{rlink.lower()}"
                else:
                    resume_fallback_idx += 1
                    rkey = f"fallback:{resume_fallback_idx}"
                if rkey in resumes_map:
                    continue
                resumes_map[rkey] = row

        contacts = list(contacts_map.values())
        resumes = list(resumes_map.values())
        base = _dir_parser() / f"job_{job_id}"
        resumes_path = Path(str(base) + "_resumes.json")
        contacts_json_path = Path(str(base) + "_contacts.json")
        contacts_csv_path = Path(str(base) + "_contacts.csv")
        contacts_txt_path = Path(str(base) + "_contacts.txt")
        resumes_path.write_text(json.dumps(resumes, ensure_ascii=False, indent=2), encoding="utf-8")
        contacts_json_path.write_text(json.dumps(contacts, ensure_ascii=False, indent=2), encoding="utf-8")
        with contacts_csv_path.open("w", encoding="utf-8", newline="") as f:
            wr = csv.writer(f)
            wr.writerow(["index", "telegram", "name", "resume_id", "resume_link"])
            for c in contacts:
                wr.writerow([c["index"], c["telegram"], c["name"], c["resume_id"], c["resume_link"]])
        contacts_txt_path.write_text(
            "\n".join(str(c.get("telegram") or "").strip() for c in contacts if str(c.get("telegram") or "").strip()),
            encoding="utf-8",
        )
        return {
            "parsed_total": len(resumes),
            "tg_found_total": len(contacts),
            "resumes_path": str(resumes_path),
            "contacts_json_path": str(contacts_json_path),
            "contacts_csv_path": str(contacts_csv_path),
            "contacts_txt_path": str(contacts_txt_path),
        }

    def _sync_parsed_pool(owner_id: int, direction: str, job_id: int, parsed: dict[str, Any]) -> dict[str, int]:
        contacts_rows = _read_json_list(str(parsed.get("contacts_json_path") or ""))
        return repo_owner_outreach.upsert_parsed_contacts(
            conn,
            tz,
            owner_tg_id=int(owner_id),
            direction=str(direction or "").strip().lower(),
            contacts=contacts_rows,
            job_id=int(job_id),
        )

    def _pool_candidates(owner_id: int, direction: str, limit: int) -> list[dict[str, Any]]:
        rows = repo_owner_outreach.pool_contacts_for_send(
            conn,
            owner_tg_id=int(owner_id),
            direction=str(direction or "").strip().lower(),
            limit=max(int(limit) * 3, int(limit)),
        )
        out: list[dict[str, Any]] = []
        seen_norm: set[str] = set()
        for row in rows:
            tg, tg_norm = _normalize_tg(row.get("telegram"))
            if not tg_norm or tg_norm in seen_norm:
                continue
            seen_norm.add(tg_norm)
            item = dict(row)
            item["telegram"] = tg
            out.append(item)
            if len(out) >= int(limit):
                break
        for i, row in enumerate(out, start=1):
            row["index"] = i
        return out

    def _prune_invalid_pool_contacts(owner_id: int, max_scan: int = 5000) -> int:
        rows = conn.execute(
            "SELECT l.telegram, l.send_error "
            "FROM owner_outreach_leads l "
            "JOIN owner_hh_jobs j ON j.id=l.job_id "
            "WHERE j.owner_tg_id=? "
            "AND l.status IN (?, ?) "
            "AND COALESCE(l.send_error,'')<>'' "
            "ORDER BY l.updated_at DESC LIMIT ?",
            (
                int(owner_id),
                repo_owner_outreach.LEAD_SEND_FAILED,
                repo_owner_outreach.LEAD_BLOCKED,
                max(100, min(int(max_scan or 0), 20000)),
            ),
        ).fetchall()
        changed = 0
        seen_norm: set[str] = set()
        for row in rows:
            tg, tg_norm = _normalize_tg(row["telegram"])
            if not tg_norm or tg_norm in seen_norm:
                continue
            seen_norm.add(tg_norm)
            err = str(row["send_error"] or "").strip()
            if not _is_terminal_unreachable_error(err):
                continue
            if repo_owner_outreach.mark_pool_status(
                conn,
                tz,
                int(owner_id),
                tg,
                repo_owner_outreach.POOL_INVALID,
            ):
                changed += 1
        return changed

    def _write_contacts_files(job_id: int, contacts: list[dict[str, Any]]) -> tuple[str, str, str]:
        base = _dir_parser() / f"job_{job_id}_pool"
        contacts_json_path = Path(str(base) + "_contacts.json")
        contacts_csv_path = Path(str(base) + "_contacts.csv")
        contacts_txt_path = Path(str(base) + "_contacts.txt")
        contacts_json_path.write_text(json.dumps(contacts, ensure_ascii=False, indent=2), encoding="utf-8")
        with contacts_csv_path.open("w", encoding="utf-8", newline="") as f:
            wr = csv.writer(f)
            wr.writerow(["index", "telegram", "name", "resume_id", "resume_link"])
            for c in contacts:
                wr.writerow(
                    [
                        int(c.get("index") or 0),
                        str(c.get("telegram") or ""),
                        str(c.get("name") or ""),
                        str(c.get("resume_id") or ""),
                        str(c.get("resume_link") or ""),
                    ]
                )
        contacts_txt_path.write_text(
            "\n".join(
                str(c.get("telegram") or "").strip()
                for c in contacts
                if str(c.get("telegram") or "").strip()
            ),
            encoding="utf-8",
        )
        return str(contacts_json_path), str(contacts_csv_path), str(contacts_txt_path)

    def _split_contacts_round_robin(
        contacts: list[dict[str, Any]],
        accounts: list[sqlite3.Row],
        per_account_max: int,
    ) -> list[tuple[sqlite3.Row, list[dict[str, Any]]]]:
        if not contacts or not accounts:
            return []
        buckets: list[list[dict[str, Any]]] = [[] for _ in accounts]
        cap = max(1, int(per_account_max or 1))
        for idx, row in enumerate(contacts):
            start = idx % len(accounts)
            for offset in range(len(accounts)):
                k = (start + offset) % len(accounts)
                if len(buckets[k]) < cap:
                    buckets[k].append(row)
                    break
        out: list[tuple[sqlite3.Row, list[dict[str, Any]]]] = []
        for i, chunk in enumerate(buckets):
            if chunk:
                out.append((accounts[i], chunk))
        return out

    def _run_sender_for_account(
        job_id: int,
        account: sqlite3.Row,
        contacts_chunk: list[dict[str, Any]],
        message_file: Path,
        delay_sec: float,
    ) -> dict[str, Any]:
        account_id = int(account["id"] or 0)
        account_title = str(account["title"] or f"account-{account_id}")
        contacts_file = _dir_parser() / f"send_contacts_{job_id}_acc{account_id}.json"
        report_file = _dir_parser() / f"send_report_{job_id}_acc{account_id}.json"
        contacts_file.write_text(json.dumps(contacts_chunk, ensure_ascii=False, indent=2), encoding="utf-8")

        session_src = str(account["session_file"] or "")
        session_for_run = _prepare_session_copy(session_src, account_id, "send")
        proc_rc = 0
        proc_stdout = ""
        proc_stderr = ""
        try:
            cmd = [
                sys.executable,
                str(PROJECT_ROOT / "scripts" / "owner_tg_bulk_send.py"),
                "--api-id",
                str(int(account["api_id"] or 0)),
                "--api-hash",
                str(account["api_hash"] or ""),
                "--session-file",
                session_for_run,
                "--contacts-file",
                str(contacts_file),
                "--message-file",
                str(message_file),
                "--report-file",
                str(report_file),
                "--limit",
                str(len(contacts_chunk)),
                "--delay-sec",
                str(float(max(delay_sec, 0.0))),
            ]
            proxy_raw = _tg_proxy()
            if proxy_raw:
                cmd.extend(["--proxy", proxy_raw])
            timeout_sec = max(300, int(len(contacts_chunk) * (float(max(delay_sec, 0.0)) + 20)))
            proc_env = os.environ.copy()
            existing_pythonpath = str(proc_env.get("PYTHONPATH") or "").strip()
            proc_env["PYTHONPATH"] = (
                f"{PROJECT_ROOT}{os.pathsep}{existing_pythonpath}" if existing_pythonpath else str(PROJECT_ROOT)
            )
            if proxy_raw:
                proc_env["OWNER_TG_PROXY"] = proxy_raw
            proc = subprocess.run(
                cmd,
                cwd=str(PROJECT_ROOT),
                capture_output=True,
                text=True,
                timeout=timeout_sec,
                env=proc_env,
            )
            proc_rc = int(proc.returncode or 0)
            proc_stdout = str(proc.stdout or "").strip()
            proc_stderr = str(proc.stderr or "").strip()
        finally:
            _cleanup_session_copy(session_for_run, session_src)

        payload = _read_json(report_file) if report_file.exists() else {}
        if not isinstance(payload, dict):
            payload = {}
        if not payload and contacts_chunk:
            payload = {
                "status": "failed",
                "error": "sender report file is missing or invalid",
                "total": len(contacts_chunk),
                "attempted": 0,
                "success": 0,
                "failed": len(contacts_chunk),
                "details": [
                    {
                        "telegram": str(row.get("telegram") or ""),
                        "status": "failed",
                        "error": "sender report file is missing or invalid",
                    }
                    for row in contacts_chunk
                ],
            }
        # Если подпроцесс упал/не вернул отчет, превращаем это в явный failed,
        # чтобы не получать ложные 0/0 в итоговой задаче.
        if proc_rc != 0:
            if not payload:
                payload = {
                    "status": "failed",
                    "error": "",
                    "total": len(contacts_chunk),
                    "attempted": 0,
                    "success": 0,
                    "failed": len(contacts_chunk),
                    "details": [],
                }
            payload["status"] = str(payload.get("status") or "failed")
            if payload["status"] == "done":
                payload["status"] = "failed"
            err_text = (
                str(payload.get("error") or "").strip()
                or proc_stderr
                or proc_stdout
                or f"sender exited with code {proc_rc}"
            )
            payload["error"] = err_text
            if int(payload.get("total") or 0) <= 0:
                payload["total"] = len(contacts_chunk)
            if int(payload.get("failed") or 0) <= 0:
                payload["failed"] = len(contacts_chunk)
            details = payload.get("details")
            if not isinstance(details, list):
                details = []
            if not details and contacts_chunk:
                details = [
                    {
                        "telegram": str(row.get("telegram") or ""),
                        "status": "failed",
                        "error": err_text,
                    }
                    for row in contacts_chunk
                ]
            payload["details"] = details
            try:
                report_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            except Exception:
                pass
        payload["account_id"] = account_id
        payload["account_title"] = account_title
        payload["report_file"] = str(report_file)
        payload["delay_sec"] = float(max(delay_sec, 0.0))
        payload["subprocess_rc"] = int(proc_rc)
        payload["subprocess_stderr"] = str(proc_stderr or "")
        payload["subprocess_stdout"] = str(proc_stdout or "")
        if "details" not in payload or not isinstance(payload.get("details"), list):
            payload["details"] = []
        return payload

    def _run_campaign(job_id: int, owner_id: int, chat_id: int, draft: dict[str, Any]) -> None:
        def worker() -> None:
            try:
                repo_owner_hh_jobs.set_running(conn, tz, job_id, "parsing")
                bot.send_message(chat_id, f"Задача #{job_id}: подготовка контактов HH...")
                direction = str(draft.get("direction") or "python")
                source_link = str(draft.get("search_link") or "").strip() or None
                requested_send = max(int(draft.get("send_limit") or 0), 0)
                per_account_max = max(
                    1,
                    int(draft.get("per_account_max") or DEFAULT_PER_ACCOUNT_MAX),
                )
                delay_sec = max(
                    0.0,
                    float(draft.get("delay_sec") or DEFAULT_SEND_DELAY_SEC),
                )
                account_ids = [
                    int(x)
                    for x in (draft.get("account_ids") or [])
                    if isinstance(x, int) and int(x) > 0
                ]
                auth_by_id = {int(r["id"] or 0): r for r in _authorized_accounts(owner_id)}
                if account_ids:
                    send_accounts = [auth_by_id[aid] for aid in account_ids if aid in auth_by_id]
                else:
                    send_accounts = _resolve_send_accounts(
                        owner_id,
                        {
                            "send_mode": draft.get("send_mode"),
                            "selected_account_ids": draft.get("selected_account_ids") or [],
                            "per_account_max": per_account_max,
                        },
                    )
                if requested_send > 0 and not send_accounts:
                    raise RuntimeError("Нет авторизованных аккаунтов для отправки.")

                # Префлайт: перед парсингом проверяем реальную доступность аккаунтов к Telegram.
                if requested_send > 0 and send_accounts:
                    bot.send_message(
                        chat_id,
                        f"Задача #{job_id}: проверяю подключение {len(send_accounts)} аккаунтов к Telegram...",
                    )
                    probe_message_file = _dir_parser() / f"probe_message_{job_id}.txt"
                    probe_message_file.write_text("connection-probe", encoding="utf-8")
                    healthy_accounts: list[sqlite3.Row] = []
                    unhealthy_lines: list[str] = []
                    for acc in send_accounts:
                        probe_rep = _run_sender_for_account(
                            job_id=job_id,
                            account=acc,
                            contacts_chunk=[],
                            message_file=probe_message_file,
                            delay_sec=0.0,
                        )
                        st = str(probe_rep.get("status") or "").strip().lower()
                        err = str(probe_rep.get("error") or "").strip()
                        if st == "done" and not err:
                            healthy_accounts.append(acc)
                        else:
                            title = str(acc["title"] or f"account-{int(acc['id'] or 0)}")
                            unhealthy_lines.append(f"{title}: {err or st or 'connection failed'}")
                    send_accounts = healthy_accounts
                    if unhealthy_lines:
                        bot.send_message(
                            chat_id,
                            "Недоступные аккаунты:\n<code>" + html.escape("\n".join(unhealthy_lines[:8])) + "</code>",
                        )
                    if not send_accounts:
                        raise RuntimeError(
                            "Нет доступных аккаунтов для отправки: "
                            + (unhealthy_lines[0] if unhealthy_lines else "connection failed")
                        )
                send_capacity = len(send_accounts) * per_account_max
                target_send = min(requested_send, send_capacity) if requested_send > 0 else 0
                if requested_send > target_send and requested_send > 0:
                    bot.send_message(
                        chat_id,
                        f"Задача #{job_id}: число для отписа уменьшено до {target_send}, "
                        f"потому что лимит по аккаунтам = {send_capacity}.",
                    )

                # Сначала берем контакты из пула, затем добираем парсингом только недостающее.
                pruned_invalid = _prune_invalid_pool_contacts(owner_id)
                if pruned_invalid > 0:
                    bot.send_message(
                        chat_id,
                        f"Задача #{job_id}: исключил из пула недоступные контакты: {pruned_invalid}.",
                    )
                pool_ready_before = repo_owner_outreach.pool_ready_count(conn, owner_id, direction)
                selected_contacts = _pool_candidates(owner_id, direction, target_send)
                need_to_fill = max(target_send - len(selected_contacts), 0)
                if target_send > 0 and need_to_fill <= 0:
                    bot.send_message(
                        chat_id,
                        f"Задача #{job_id}: в пуле уже достаточно контактов ({len(selected_contacts)}/{target_send}), "
                        "новый парсинг не запускаю.",
                    )
                parse_attempt = 0
                parsed_rows_total = 0
                chunks: list[dict[str, Any]] = []
                parse_stage_names: list[str] = []
                parse_budget = need_to_fill
                parse_rate_limited = False

                stages: list[tuple[str, str | None, str | None, str | None]] = []
                stage_keys: set[tuple[str, str, str]] = set()

                def _add_stage(name: str, search_text: str | None, search_link: str | None, stage_direction: str | None) -> None:
                    key = (
                        str(search_text or "").strip(),
                        str(search_link or "").strip(),
                        str(stage_direction or "").strip().lower(),
                    )
                    if key in stage_keys:
                        return
                    stage_keys.add(key)
                    stages.append((name, search_text, search_link, stage_direction))

                # Для стабильности всегда используем поисковую фразу выбранного направления.
                # Это исключает ситуацию, когда в сохраненном фильтре остался старый/чужой text.
                base_text = _hh_query_for_direction(direction)
                broad_text = _hh_broad_query_for_direction(direction)
                wide_text = _hh_wide_query_for_direction(direction)
                if source_link:
                    _add_stage("основной (с фильтром HH)", base_text, source_link, direction)
                    _add_stage("резерв (широкий TG-запрос по направлению)", broad_text, None, direction)
                    _add_stage("резерв (широкий запрос по направлению)", wide_text, None, direction)
                else:
                    _add_stage("основной (без фильтра HH)", base_text, None, direction)
                    _add_stage("резерв (широкий TG-запрос по направлению)", broad_text, None, direction)
                    _add_stage("резерв (широкий запрос по направлению)", wide_text, None, direction)

                def _pool_ready_now() -> int:
                    return repo_owner_outreach.pool_ready_count(conn, owner_id, direction)

                def _is_hh_rate_limit(exc: Exception) -> bool:
                    low = str(exc or "").lower()
                    return any(
                        marker in low
                        for marker in (
                            "429",
                            "too many requests",
                            "rate limit",
                            "retry after",
                            "flood",
                        )
                    )

                def _estimate_parse_limit(remaining_need: int) -> int:
                    n = max(int(remaining_need or 0), 1)
                    # Парсим с рабочим запасом, чтобы не получать недобор x3-x4,
                    # но ограничиваем верх, чтобы не разгонять лишнюю нагрузку на HH.
                    base = max(n * 3, n + 40, 80)
                    return max(1, min(base, 300))

                if parse_budget > 0:
                    for stage_name, st_text, st_link, st_direction in stages:
                        if parse_rate_limited:
                            break
                        ready_before_stage = _pool_ready_now()
                        parse_budget = max(target_send - ready_before_stage, 0)
                        if ready_before_stage >= target_send or parse_budget <= 0:
                            break
                        parse_stage_names.append(stage_name)
                        stage_try = 0
                        max_stage_tries = 2
                        while stage_try < max_stage_tries:
                            ready_before_try = _pool_ready_now()
                            remaining_need = max(target_send - ready_before_try, 0)
                            if remaining_need <= 0:
                                break
                            stage_try += 1
                            parse_attempt += 1
                            current_limit = _estimate_parse_limit(remaining_need)
                            bot.send_message(
                                chat_id,
                                f"Задача #{job_id}: запускаю этап «{stage_name}» (попытка {stage_try}, лимит {current_limit}).",
                            )
                            try:
                                parsed_chunk = run_hh_parse(
                                    parser_root=_parser_root(),
                                    output_dir=str(_dir_parser()),
                                    search_text=st_text,
                                    search_link=st_link,
                                    direction=st_direction,
                                    limit=current_limit,
                                )
                            except Exception as exc:
                                if _is_hh_rate_limit(exc):
                                    parse_rate_limited = True
                                    bot.send_message(
                                        chat_id,
                                        "HH временно ограничил частоту запросов (429). "
                                        "Останавливаю дополнительные этапы парсинга в этой задаче.",
                                    )
                                else:
                                    bot.send_message(
                                        chat_id,
                                        f"Задача #{job_id}: этап «{stage_name}» завершился ошибкой: "
                                        f"<code>{html.escape(str(exc))}</code>",
                                    )
                                break
                            chunks.append(parsed_chunk)
                            parsed_rows_total += int(parsed_chunk.get("parsed_total") or 0)
                            merged_preview = _merge_parse_outputs(job_id, chunks)
                            pool_sync = _sync_parsed_pool(owner_id, direction, job_id, merged_preview)
                            ready_now = _pool_ready_now()
                            parse_budget = max(target_send - ready_now, 0)
                            bot.send_message(
                                chat_id,
                                f"Задача #{job_id}: этап «{stage_name}», пул готовых {ready_now}/{target_send} "
                                f"(попытка {parse_attempt}, лимит {current_limit}, +{int(pool_sync.get('inserted') or 0)})",
                            )
                            if ready_now >= target_send:
                                break
                            if int(pool_sync.get("inserted") or 0) <= 0:
                                break
                        if _pool_ready_now() >= target_send:
                            break

                parsed = _merge_parse_outputs(job_id, chunks)
                pool_sync_final = _sync_parsed_pool(owner_id, direction, job_id, parsed)
                pool_ready_after = repo_owner_outreach.pool_ready_count(conn, owner_id, direction)
                selected_contacts = _pool_candidates(owner_id, direction, target_send)
                send_limit = len(selected_contacts)

                def _is_tg_limit_detail(drow: dict[str, Any]) -> bool:
                    d_status = str(drow.get("status") or "").strip().lower()
                    d_err = str(drow.get("error") or "").strip()
                    if d_status == "blocked":
                        return True
                    return _is_tg_limit_error(d_err)

                # Гарантия доставки: если в пуле меньше цели, добираем парсингом перед отправкой.
                # Останавливаемся только при лимитах HH (429) или реальном отсутствии прироста.
                if target_send > 0 and send_limit < target_send and not parse_rate_limited:
                    topup_round = 0
                    topup_no_gain = 0
                    while send_limit < target_send and topup_round < 4 and not parse_rate_limited:
                        topup_round += 1
                        ready_before_topup = _pool_ready_now()
                        need_now = max(target_send - ready_before_topup, 0)
                        if need_now <= 0:
                            break
                        limit_hint = _estimate_parse_limit(need_now)
                        inserted_total_round = 0
                        for stage_name, st_text, st_link, st_direction in stages:
                            if parse_rate_limited:
                                break
                            parse_attempt += 1
                            parse_stage_names.append(f"{stage_name} [добор {topup_round}]")
                            current_limit = max(1, limit_hint)
                            bot.send_message(
                                chat_id,
                                f"Задача #{job_id}: добираю контакты ({stage_name}, добор {topup_round}, лимит {current_limit}).",
                            )
                            try:
                                parsed_chunk = run_hh_parse(
                                    parser_root=_parser_root(),
                                    output_dir=str(_dir_parser()),
                                    search_text=st_text,
                                    search_link=st_link,
                                    direction=st_direction,
                                    limit=current_limit,
                                )
                            except Exception as exc:
                                if _is_hh_rate_limit(exc):
                                    parse_rate_limited = True
                                    bot.send_message(
                                        chat_id,
                                        "HH временно ограничил частоту запросов (429). "
                                        "Останавливаю добор контактов в этой задаче.",
                                    )
                                else:
                                    bot.send_message(
                                        chat_id,
                                        f"Задача #{job_id}: добор ({stage_name}) завершился ошибкой: "
                                        f"<code>{html.escape(str(exc))}</code>",
                                    )
                                continue
                            chunks.append(parsed_chunk)
                            parsed_rows_total += int(parsed_chunk.get("parsed_total") or 0)
                            merged_preview = _merge_parse_outputs(job_id, chunks)
                            pool_sync = _sync_parsed_pool(owner_id, direction, job_id, merged_preview)
                            inserted = int(pool_sync.get("inserted") or 0)
                            inserted_total_round += inserted
                            ready_now = _pool_ready_now()
                            bot.send_message(
                                chat_id,
                                f"Задача #{job_id}: добор {topup_round}, пул готовых {ready_now}/{target_send} "
                                f"(этап: {stage_name}, лимит {current_limit}, +{inserted})",
                            )
                            if ready_now >= target_send:
                                break
                        pool_ready_after = _pool_ready_now()
                        selected_contacts = _pool_candidates(owner_id, direction, target_send)
                        send_limit = len(selected_contacts)
                        if inserted_total_round <= 0:
                            topup_no_gain += 1
                        else:
                            topup_no_gain = 0
                        if topup_no_gain >= 2:
                            break

                # Для выдачи файлов сохраняем актуальный срез пула.
                export_cap = max(send_limit, min(max(pool_ready_after, 1), 300))
                export_contacts = _pool_candidates(owner_id, direction, export_cap)
                contacts_json_path, contacts_csv_path, contacts_txt_path = _write_contacts_files(job_id, export_contacts)

                parsed["parse_attempts"] = parse_attempt
                parsed["parse_stages"] = parse_stage_names
                parsed["parse_rate_limited"] = parse_rate_limited
                parsed["parsed_rows_total"] = parsed_rows_total
                parsed["parse_budget_initial"] = need_to_fill
                parsed["parse_budget_left"] = parse_budget
                parsed["target_send_limit"] = target_send
                parsed["requested_send_limit"] = requested_send
                parsed["source_filter_link"] = source_link or ""
                parsed["pool_ready_before"] = pool_ready_before
                parsed["pool_ready_after"] = pool_ready_after
                parsed["pool_sync_inserted"] = int(pool_sync_final.get("inserted") or 0)
                parsed["pool_sync_updated"] = int(pool_sync_final.get("updated") or 0)
                parsed["selected_send_total"] = send_limit
                parsed["per_account_max"] = per_account_max
                parsed["accounts_planned"] = len(send_accounts)
                parsed["delay_sec"] = delay_sec
                parsed["contacts_json_path"] = contacts_json_path
                parsed["contacts_csv_path"] = contacts_csv_path
                parsed["contacts_txt_path"] = contacts_txt_path
                report_file = str(_dir_parser() / f"send_report_{job_id}.json")
                sender_payload: dict[str, Any] = {
                    "status": "done",
                    "error": "",
                    "total": send_limit,
                    "attempted": 0,
                    "success": 0,
                    "failed": 0,
                    "details": [],
                    "per_account": [],
                    "delay_sec_per_account": float(delay_sec),
                    "pool_ready_before": pool_ready_before,
                    "pool_ready_after": pool_ready_after,
                    "requested_send_limit": requested_send,
                    "target_send_limit": target_send,
                }
                repo_owner_hh_jobs.update_parse_result(
                    conn,
                    tz,
                    job_id,
                    parsed_total=int(parsed.get("parsed_total") or 0),
                    tg_found_total=int(parsed.get("tg_found_total") or 0),
                    output_file=str(parsed.get("resumes_path") or ""),
                    contacts_file=contacts_csv_path,
                    parser_payload=parsed,
                )
                if send_limit > 0:
                    repo_owner_hh_jobs.set_running(conn, tz, job_id, "sending")
                    msg_file = _dir_parser() / f"message_{job_id}.txt"
                    # Жёстко фиксируем: первое сообщение отписа всегда скриптовое.
                    direction_for_first = str(draft.get("direction") or "python")
                    first_message = get_first_message(conn, direction_for_first).strip()
                    msg_file.write_text(first_message, encoding="utf-8")
                    bot.send_message(
                        chat_id,
                        f"Задача #{job_id}: отправка с {len(send_accounts)} аккаунтов. "
                        f"Пауза {int(delay_sec)} сек на каждый аккаунт.",
                    )
                    per_reports: list[dict[str, Any]] = []
                    attempted_norms: set[str] = set()
                    delivery_stop_reason = ""
                    no_progress_waves = 0
                    wave_no = 0
                    max_waves = 8
                    max_wave_capacity = max(1, len(send_accounts) * per_account_max)
                    delivered_success = 0
                    delivered_failed = 0
                    delivered_blocked = 0

                    def _pick_wave_contacts(required_success: int) -> list[dict[str, Any]]:
                        need = max(int(required_success or 0), 1)
                        fetch_cap = min(5000, max(need * 4, need + len(attempted_norms) + 20, max_wave_capacity))
                        raw_candidates = _pool_candidates(owner_id, direction, fetch_cap)
                        out: list[dict[str, Any]] = []
                        for row in raw_candidates:
                            tg, tg_norm = _normalize_tg(row.get("telegram"))
                            if not tg_norm or tg_norm in attempted_norms:
                                continue
                            item = dict(row)
                            item["telegram"] = tg
                            out.append(item)
                            if len(out) >= max_wave_capacity:
                                break
                        return out

                    while delivered_success < target_send and wave_no < max_waves:
                        wave_no += 1
                        need_success = max(target_send - delivered_success, 0)
                        wave_contacts = _pick_wave_contacts(need_success)

                        # Если контактов не хватает — пробуем дозаполнить пул точечно.
                        if not wave_contacts and not parse_rate_limited:
                            inserted_total_round = 0
                            limit_hint = _estimate_parse_limit(need_success)
                            for stage_name, st_text, st_link, st_direction in stages:
                                if parse_rate_limited:
                                    break
                                parse_attempt += 1
                                parse_stage_names.append(f"{stage_name} [добор отправки {wave_no}]")
                                bot.send_message(
                                    chat_id,
                                    f"Задача #{job_id}: добираю для гарантированной доставки "
                                    f"({stage_name}, лимит {limit_hint}).",
                                )
                                try:
                                    parsed_chunk = run_hh_parse(
                                        parser_root=_parser_root(),
                                        output_dir=str(_dir_parser()),
                                        search_text=st_text,
                                        search_link=st_link,
                                        direction=st_direction,
                                        limit=limit_hint,
                                    )
                                except Exception as exc:
                                    if _is_hh_rate_limit(exc):
                                        parse_rate_limited = True
                                        bot.send_message(
                                            chat_id,
                                            "HH временно ограничил частоту запросов (429). "
                                            "Останавливаю добор перед отправкой.",
                                        )
                                    else:
                                        bot.send_message(
                                            chat_id,
                                            f"Задача #{job_id}: добор перед отправкой ({stage_name}) ошибка: "
                                            f"<code>{html.escape(str(exc))}</code>",
                                        )
                                    continue
                                chunks.append(parsed_chunk)
                                parsed_rows_total += int(parsed_chunk.get("parsed_total") or 0)
                                merged_preview = _merge_parse_outputs(job_id, chunks)
                                pool_sync = _sync_parsed_pool(owner_id, direction, job_id, merged_preview)
                                inserted = int(pool_sync.get("inserted") or 0)
                                inserted_total_round += inserted
                            if inserted_total_round > 0:
                                parsed = _merge_parse_outputs(job_id, chunks)
                                pool_sync_final = _sync_parsed_pool(owner_id, direction, job_id, parsed)
                                pool_ready_after = repo_owner_outreach.pool_ready_count(conn, owner_id, direction)
                                wave_contacts = _pick_wave_contacts(need_success)

                        if not wave_contacts:
                            delivery_stop_reason = "no_new_contacts"
                            break

                        send_chunks = _split_contacts_round_robin(wave_contacts, send_accounts, per_account_max)
                        if not send_chunks:
                            delivery_stop_reason = "split_failed"
                            break

                        max_workers = max(1, min(len(send_chunks), 8))
                        wave_success = 0
                        wave_failed = 0
                        wave_blocked = 0
                        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
                            futures: dict[
                                concurrent.futures.Future[dict[str, Any]],
                                tuple[sqlite3.Row, list[dict[str, Any]]],
                            ] = {}
                            for acc, contacts_chunk in send_chunks:
                                fut = pool.submit(_run_sender_for_account, job_id, acc, contacts_chunk, msg_file, delay_sec)
                                futures[fut] = (acc, contacts_chunk)
                            for fut, meta in futures.items():
                                acc, contacts_chunk = meta
                                try:
                                    rep = fut.result()
                                except Exception as exc:
                                    rep = {
                                        "status": "failed",
                                        "error": str(exc),
                                        "total": len(contacts_chunk),
                                        "attempted": len(contacts_chunk),
                                        "success": 0,
                                        "failed": len(contacts_chunk),
                                        "details": [
                                            {
                                                "telegram": str(row.get("telegram") or ""),
                                                "status": "failed",
                                                "error": f"worker error: {exc}",
                                            }
                                            for row in contacts_chunk
                                        ],
                                        "account_id": int(acc["id"] or 0),
                                        "account_title": str(acc["title"] or f"account-{int(acc['id'] or 0)}"),
                                        "delay_sec": float(delay_sec),
                                        "report_file": "",
                                    }
                                rep["wave"] = wave_no
                                per_reports.append(rep)
                                wave_success += int(rep.get("success") or 0)
                                wave_failed += int(rep.get("failed") or 0)
                                for drow in rep.get("details") or []:
                                    if not isinstance(drow, dict):
                                        continue
                                    tg, tg_norm = _normalize_tg(drow.get("telegram"))
                                    if tg_norm:
                                        attempted_norms.add(tg_norm)
                                    if str(drow.get("status") or "").strip().lower() == "sent":
                                        repo_owner_outreach.mark_contacted(
                                            conn,
                                            tz,
                                            owner_id,
                                            str(drow.get("telegram") or ""),
                                            job_id=job_id,
                                            account_id=int(rep.get("account_id") or 0) or None,
                                        )
                                    if _is_tg_limit_detail(drow):
                                        wave_blocked += 1

                        delivered_success += wave_success
                        delivered_failed += wave_failed
                        delivered_blocked += wave_blocked

                        if wave_success <= 0:
                            no_progress_waves += 1
                        else:
                            no_progress_waves = 0
                        if wave_success <= 0 and wave_blocked > 0:
                            delivery_stop_reason = "tg_rate_limit"
                            break
                        if no_progress_waves >= 4:
                            delivery_stop_reason = "no_progress"
                            break

                    all_details: list[dict[str, Any]] = []
                    sum_total = 0
                    sum_attempted = 0
                    sum_success = 0
                    sum_failed = 0
                    per_account: list[dict[str, Any]] = []
                    for rep in per_reports:
                        acc_id = int(rep.get("account_id") or 0)
                        acc_title = str(rep.get("account_title") or f"account-{acc_id}")
                        r_total = int(rep.get("total") or 0)
                        r_attempted = int(rep.get("attempted") or 0)
                        r_success = int(rep.get("success") or 0)
                        r_failed = int(rep.get("failed") or 0)
                        sum_total += r_total
                        sum_attempted += r_attempted
                        sum_success += r_success
                        sum_failed += r_failed
                        per_account.append(
                            {
                                "account_id": acc_id,
                                "account_title": acc_title,
                                "total": r_total,
                                "attempted": r_attempted,
                                "success": r_success,
                                "failed": r_failed,
                                "status": str(rep.get("status") or ""),
                                "error": str(rep.get("error") or ""),
                                "report_file": str(rep.get("report_file") or ""),
                                "wave": int(rep.get("wave") or 0),
                            }
                        )
                        for row in rep.get("details") or []:
                            if not isinstance(row, dict):
                                continue
                            tg, tg_norm = _normalize_tg(row.get("telegram"))
                            if not tg_norm:
                                continue
                            item = dict(row)
                            item["telegram"] = tg
                            item["account_id"] = acc_id
                            item["account_title"] = acc_title
                            all_details.append(item)
                    sender_payload = {
                        "status": "done" if sum_failed <= 0 else "partial",
                        "error": "",
                        "total": sum_total,
                        "attempted": sum_attempted,
                        "success": sum_success,
                        "failed": sum_failed,
                        "details": all_details,
                        "per_account": per_account,
                        "delay_sec_per_account": float(delay_sec),
                        "accounts_used": len(per_account),
                        "pool_ready_before": pool_ready_before,
                        "pool_ready_after": pool_ready_after,
                        "requested_send_limit": requested_send,
                        "target_send_limit": target_send,
                        "delivery_goal": target_send,
                        "delivery_gap": max(target_send - sum_success, 0),
                        "delivery_stop_reason": delivery_stop_reason,
                        "delivery_guaranteed_mode": True,
                        "waves_used": int(max((int(r.get("wave") or 0) for r in per_reports), default=0)),
                    }
                    blocked_cnt = 0
                    for drow in all_details:
                        if _is_tg_limit_detail(drow):
                            blocked_cnt += 1
                    sender_payload["blocked"] = blocked_cnt
                elif target_send > 0:
                    bot.send_message(
                        chat_id,
                        f"Задача #{job_id}: в пуле недостаточно новых контактов для отправки "
                        f"({send_limit}/{target_send}). Попробуй расширить фильтр HH.",
                    )
                Path(report_file).write_text(json.dumps(sender_payload, ensure_ascii=False, indent=2), encoding="utf-8")
                repo_owner_hh_jobs.update_send_result(
                    conn,
                    tz,
                    job_id,
                    send_success=int(sender_payload.get("success") or 0),
                    send_failed=int(sender_payload.get("failed") or 0),
                    report_file=report_file,
                    sender_payload=sender_payload,
                )
                _sync_leads(job_id, owner_id, direction, parsed, sender_payload)
                repo_owner_hh_jobs.mark_done(conn, tz, job_id)
                row = repo_owner_hh_jobs.get(conn, job_id)
                if row:
                    tail = ""
                    if target_send <= 0:
                        tail = (
                            "\n\n<i>Рассылка не выполнялась: для «отписа» было выбрано <b>0</b> сообщений. "
                            "Чтобы отправить первое сообщение кандидатам, при создании задачи укажи число &gt; 0 "
                            "на шаге «количество для отписа».</i>"
                        )
                    elif send_limit <= 0:
                        tail = (
                            "\n\n<i>Рассылка не выполнялась: новые контакты для отправки не найдены "
                            "(кандидаты закончились в пуле/уже есть в базе «кому отписали»).</i>"
                        )
                    else:
                        sender_payload_row = _loads_dict(str(row["sender_payload_json"] or ""))
                        blocked_cnt = int(sender_payload_row.get("blocked") or 0)
                        send_success = int(row["send_success"] or 0)
                        send_failed = int(row["send_failed"] or 0)
                        delivery_goal = int(sender_payload_row.get("delivery_goal") or target_send or 0)
                        delivery_gap = int(sender_payload_row.get("delivery_gap") or max(delivery_goal - send_success, 0))
                        delivery_stop_reason = str(sender_payload_row.get("delivery_stop_reason") or "").strip().lower()
                        if delivery_gap > 0:
                            if blocked_cnt > 0 or delivery_stop_reason == "tg_rate_limit":
                                tail += (
                                    f"\n\n<i>Гарантированная доставка не добрана на {delivery_gap}: "
                                    "остановились на лимитах Telegram (Too many requests / PeerFlood).</i>"
                                )
                            elif delivery_stop_reason == "no_new_contacts":
                                tail += (
                                    f"\n\n<i>Гарантированная доставка не добрана на {delivery_gap}: "
                                    "закончились новые контакты в пуле после добора парсингом.</i>"
                                )
                            elif delivery_stop_reason == "no_progress":
                                tail += (
                                    f"\n\n<i>Гарантированная доставка не добрана на {delivery_gap}: "
                                    "дополнительные волны отправки не дали прогресса.</i>"
                                )
                            else:
                                tail += (
                                    f"\n\n<i>Гарантированная доставка не добрана на {delivery_gap}: "
                                    "см. детали в файле отчета.</i>"
                                )
                        if blocked_cnt > 0:
                            tail += (
                                f"\n\n<i>Ограничение Telegram: {blocked_cnt} отправок остановлено из-за "
                                "лимитов/антиспама («Too many requests / PeerFlood»). "
                                "Нужен прогретый аккаунт или пауза перед следующей отписью.</i>"
                            )
                        if send_success <= 0 and send_failed > 0:
                            first_err = str(sender_payload_row.get("error") or "").strip()
                            if not first_err:
                                details = sender_payload_row.get("details") or []
                                if isinstance(details, list):
                                    for d in details:
                                        if isinstance(d, dict):
                                            err = str(d.get("error") or "").strip()
                                            if err:
                                                first_err = err
                                                break
                            if first_err:
                                tail += (
                                    "\n\n<i>Отправка не выполнена: "
                                    + html.escape(first_err[:300])
                                    + "</i>"
                                )
                    parser_payload = _loads_dict(str(row["parser_payload_json"] or ""))
                    if bool(parser_payload.get("parse_rate_limited")):
                        tail += (
                            "\n\n<i>Парсинг HH был остановлен из-за лимита площадки (HTTP 429). "
                            "Бот не повышал лимиты и не запускал дополнительные нерабочие этапы.</i>"
                        )
                    bot.send_message(
                        chat_id,
                        f"Задача #{job_id} завершена ✅\n"
                        f"Распарсено: {int(row['parsed_total'] or 0)}, найдено TG: {int(row['tg_found_total'] or 0)}, "
                        f"отправка: {int(row['send_success'] or 0)}/{int(row['send_failed'] or 0)}\n"
                        "Файлы доступны в разделе «📄 Файлы»."
                        + tail,
                    )
            except Exception as exc:
                repo_owner_hh_jobs.mark_failed(conn, tz, job_id, str(exc))
                bot.send_message(chat_id, f"Ошибка задачи #{job_id}: <code>{html.escape(str(exc))}</code>")

        threading.Thread(target=worker, daemon=True, name=f"owner-job-{job_id}").start()

    def _contacts_txt_path_from_job(row: sqlite3.Row) -> str:
        payload = _loads_dict(str(row["parser_payload_json"] or ""))
        direct = Path(str(payload.get("contacts_txt_path") or "").strip())
        if direct.exists() and direct.is_file():
            return str(direct)
        csv_path = Path(str(row["contacts_file"] or "").strip())
        if not csv_path.exists() or not csv_path.is_file():
            return ""
        txt_path = csv_path.with_suffix(".txt")
        lines: list[str] = []
        try:
            with csv_path.open("r", encoding="utf-8", newline="") as f:
                rd = csv.DictReader(f)
                for rec in rd:
                    tg = str((rec or {}).get("telegram") or "").strip()
                    if tg:
                        lines.append(tg)
        except Exception:
            return ""
        txt_path.write_text("\n".join(lines), encoding="utf-8")
        return str(txt_path)

    def _apply_runtime_to_draft(owner_id: int, draft: dict[str, Any]) -> tuple[dict[str, Any], list[sqlite3.Row], int]:
        cfg_payload = _out_cfg(owner_id)
        mode = str(cfg_payload.get("send_mode") or SEND_MODE_ALL).strip().lower()
        selected_ids = [
            int(x)
            for x in (cfg_payload.get("selected_account_ids") or [])
            if isinstance(x, int) and int(x) > 0
        ]
        accounts = _resolve_send_accounts(owner_id, cfg_payload)
        delay_sec = max(0, int(cfg_payload.get("delay_sec") or DEFAULT_SEND_DELAY_SEC))
        per_account_max = max(1, int(cfg_payload.get("per_account_max") or DEFAULT_PER_ACCOUNT_MAX))
        cap = _send_capacity(accounts, cfg_payload)
        draft["send_mode"] = mode
        draft["selected_account_ids"] = selected_ids
        draft["account_ids"] = [int(r["id"] or 0) for r in accounts]
        draft["delay_sec"] = delay_sec
        draft["per_account_max"] = per_account_max
        draft["send_capacity"] = cap
        return draft, accounts, cap

    def _send_out_confirm(chat_id: int, owner_id: int, draft: dict[str, Any], send_limit: int) -> None:
        draft, accounts, cap = _apply_runtime_to_draft(owner_id, draft)
        d = str(draft.get("direction") or "python")
        # Первое касание всегда берём строго из скрипта.
        msg = get_first_message(conn, d)
        base_query = _hh_query_for_direction(d)
        source = str(draft.get("search_link") or "").strip()
        source_line = f"Ссылка-фильтр: <code>{html.escape(source)}</code>" if source else "Ссылка-фильтр: <i>не задана</i>"
        target = int(max(send_limit, 0))
        if cap > 0:
            target = min(target, cap)
        draft["send_limit"] = target
        draft["message_text"] = msg
        repo_states.set_state(conn, int(owner_id), STATE_OWNER_TOOLS, {"section": "out", "step": "confirm", "draft": draft}, tz)
        kb = InlineKeyboardMarkup()
        kb.row(InlineKeyboardButton("🚀 Запустить", callback_data="t:out:run"), InlineKeyboardButton("⬅️ Отмена", callback_data="t:out"))
        mode_label = "выбранные аккаунты" if str(draft.get("send_mode") or "") == SEND_MODE_SELECTED else "все авторизованные"
        account_labels = ", ".join(str(r["title"]) for r in accounts[:10])
        if len(accounts) > 10:
            account_labels += f" и еще {len(accounts) - 10}"
        accounts_line = (
            f"Аккаунты: <code>{html.escape(account_labels)}</code>\n"
            if account_labels
            else "Аккаунты: <i>не выбраны</i>\n"
        )
        bot.send_message(
            chat_id,
            (
                "<b>Подтверждение</b>\n"
                f"{source_line}\n"
                f"Запрос по направлению HH: <code>{html.escape(base_query)}</code>\n"
                f"Профиль: <b>{_label_dir(d)}</b>\n"
                f"Режим аккаунтов: <b>{mode_label}</b>\n"
                f"Аккаунтов в запуске: <b>{len(accounts)}</b>\n"
                f"Лимит на аккаунт: <b>{int(draft.get('per_account_max') or DEFAULT_PER_ACCOUNT_MAX)}</b>\n"
                f"Интервал: <b>{int(draft.get('delay_sec') or DEFAULT_SEND_DELAY_SEC)} сек</b>\n"
                f"Макс. доступно в этом запуске: <b>{cap}</b>\n"
                f"Отпис: <b>{int(target)}</b>\n"
                f"{accounts_line}"
                f"AI: <b>{_label_ai(script_ai_enabled(conn))}</b>\n\n"
                f"Первое сообщение:\n<code>{html.escape(msg)}</code>"
            ),
            reply_markup=kb,
        )

    def _notify_active_job(chat_id: int, owner_id: int, callback_id: str | None = None) -> bool:
        active = _active_job(owner_id)
        if not active:
            return False
        jid = int(active["id"] or 0)
        stage = _label_job_stage(str(active["stage"] or "running"))
        if callback_id:
            bot.answer_callback_query(callback_id, f"Уже выполняется задача #{jid}")
        bot.send_message(
            chat_id,
            f"Сейчас уже выполняется задача #{jid} ({stage}).\n"
            "Дождись завершения, затем запускай следующую.",
            reply_markup=_kb_out(),
        )
        return True

    @bot.message_handler(commands=["t"])
    def cmd_t(m: Message):
        if not _is_owner(m.from_user.id):
            return
        repo_states.clear_state(conn, int(m.from_user.id), tz)
        _show_main(m.chat.id, int(m.from_user.id))

    @bot.callback_query_handler(func=lambda c: c.data == "t:menu")
    def cb_menu(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        repo_states.clear_state(conn, int(c.from_user.id), tz)
        _show_main(c.message.chat.id, int(c.from_user.id), c.message.message_id)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "t:help")
    def cb_help(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        bot.answer_callback_query(c.id)
        bot.send_message(
            c.message.chat.id,
            (
                "<b>Логика /t (коротко)</b>\n"
                "1) <b>Модерирование</b>: аккаунты Telegram, выбор основного (для QR/по умолчанию), типы (стандарт/полигон), QR-логин, очистка неавторизованных.\n"
                "2) <b>Отпис</b>:\n"
                "• <b>Быстрый запуск</b> — направление + сколько написать;\n"
                "• <b>Ручной запуск</b> — тот же сценарий с подтверждением;\n"
                "• <b>Настройки отписа</b> — выбор аккаунтов, интервал, лимит на аккаунт;\n"
                "• <b>Автодиалоги AI</b> — фоновая обработка ответов (пишет только с полигон-аккаунтов);\n"
                "• <b>Проверка</b> — быстрый диагноз перед запуском;\n"
                "• <b>Сброс активной</b> — аварийно снять зависшую задачу.\n"
                "3) <b>Реакции</b>: авто на новые посты/каналы со всех авторизованных аккаунтов и ручной прогон (5/20).\n"
                "4) <b>Общая статистика</b>: сводка и экспорт.\n\n"
                "Если что-то не работает, первым делом жми <b>Отпис -> Проверка</b>."
            ),
            reply_markup=_kb_main(),
        )

    @bot.callback_query_handler(func=lambda c: c.data == "noop")
    def cb_noop(c: CallbackQuery):
        bot.answer_callback_query(c.id, "Пока недоступно")

    @bot.callback_query_handler(func=lambda c: c.data in {"t:mod", "t:mod:list"})
    def cb_mod(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        _show_mod(c.message.chat.id, int(c.from_user.id), c.message.message_id)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "t:mod:prune_unauth")
    def cb_mod_prune_unauth(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        owner_id = int(c.from_user.id)
        rows = repo_owner_tg_accounts.list_all(conn, owner_id)
        to_delete = [row for row in rows if not _authorized(row)]
        if not to_delete:
            bot.answer_callback_query(c.id, "Неавторизованных нет")
            _show_mod(c.message.chat.id, owner_id, c.message.message_id)
            return

        deleted_ids: set[int] = set()
        deleted_count = 0
        for row in to_delete:
            aid = int(row["id"] or 0)
            if aid <= 0:
                continue
            _delete_account_local_artifacts(owner_id, row)
            if repo_owner_tg_accounts.delete(conn, owner_id, aid):
                deleted_ids.add(aid)
                deleted_count += 1

        if deleted_ids:
            cfg_payload = _out_cfg(owner_id)
            selected_ids = [
                int(x)
                for x in (cfg_payload.get("selected_account_ids") or [])
                if isinstance(x, int) and int(x) > 0 and int(x) not in deleted_ids
            ]
            _set_out_cfg(owner_id, {"selected_account_ids": selected_ids})

        _show_mod(c.message.chat.id, owner_id, c.message.message_id)
        bot.answer_callback_query(c.id, f"Удалено: {deleted_count}")

    @bot.callback_query_handler(func=lambda c: c.data == "t:mod:pick")
    def cb_mod_pick(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        bot.edit_message_text(
            "Выбери основной аккаунт (он нужен для QR и как аккаунт по умолчанию):",
            c.message.chat.id,
            c.message.message_id,
            reply_markup=_kb_accounts_pick(int(c.from_user.id)),
        )
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "t:mod:types")
    def cb_mod_types(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        owner_id = int(c.from_user.id)
        bot.edit_message_text(
            "Типы аккаунтов:\n"
            "• 👤 стандарт — AI только читает диалоги\n"
            "• 🧪 полигон — AI может отправлять ответы",
            c.message.chat.id,
            c.message.message_id,
            reply_markup=_kb_accounts_types(owner_id),
        )
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("t:mod:type:set:"))
    def cb_mod_type_set(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        parts = str(c.data or "").split(":")
        if len(parts) < 6:
            bot.answer_callback_query(c.id, "Некорректно")
            return
        try:
            aid = int(parts[4])
        except Exception:
            bot.answer_callback_query(c.id, "Некорректный id")
            return
        kind = _account_type(parts[5])
        ok = repo_owner_tg_accounts.set_type(conn, tz, int(c.from_user.id), aid, kind)
        if not ok:
            bot.answer_callback_query(c.id, "Аккаунт не найден")
            return
        owner_id = int(c.from_user.id)
        bot.edit_message_text(
            "Типы аккаунтов:\n"
            "• 👤 стандарт — AI только читает диалоги\n"
            "• 🧪 полигон — AI может отправлять ответы",
            c.message.chat.id,
            c.message.message_id,
            reply_markup=_kb_accounts_types(owner_id),
        )
        bot.answer_callback_query(c.id, f"Тип: {_account_type_label(kind)}")

    @bot.callback_query_handler(func=lambda c: c.data.startswith("t:mod:set:"))
    def cb_mod_set(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        aid = int(c.data.split(":")[3])
        if repo_owner_tg_accounts.set_active(conn, tz, int(c.from_user.id), aid):
            _show_mod(c.message.chat.id, int(c.from_user.id), c.message.message_id)
            bot.answer_callback_query(c.id, "Сделал основным")
        else:
            bot.answer_callback_query(c.id, "Не найден")

    @bot.callback_query_handler(func=lambda c: c.data == "t:mod:add")
    def cb_mod_add(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        repo_states.set_state(conn, int(c.from_user.id), STATE_OWNER_TOOLS, {"section": "mod", "step": "title", "draft": {}}, tz)
        bot.send_message(c.message.chat.id, "Название аккаунта:")
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "t:mod:qr")
    def cb_mod_qr(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        owner_id = int(c.from_user.id)
        acc = _active_account(owner_id)
        if not acc:
            bot.answer_callback_query(c.id, "Нет основного аккаунта")
            return
        aid = int(acc["id"])
        has_saved = bool(_tg_cloud_password(owner_id, aid))
        repo_states.set_state(
            conn,
            owner_id,
            STATE_OWNER_TOOLS,
            {"section": "mod", "step": "qr_password", "draft": {"account_id": aid}},
            tz,
        )
        bot.send_message(
            c.message.chat.id,
            (
                "Введи облачный пароль Telegram для QR-логина.\n"
                "Если облачного пароля нет — отправь <code>-</code>."
                + ("\nЕсли использовать сохраненный пароль, отправь <code>.</code>." if has_saved else "")
            ),
        )
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data in {"t:out", "t:out:status"})
    def cb_out(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        _show_out(c.message.chat.id, int(c.from_user.id), c.message.message_id)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "t:out:cfg")
    def cb_out_cfg(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        owner_id = int(c.from_user.id)
        cfg_payload = _out_cfg(owner_id)
        mode = "выбранные" if str(cfg_payload.get("send_mode") or "") == SEND_MODE_SELECTED else "все авторизованные"
        delay_sec = int(cfg_payload.get("delay_sec") or DEFAULT_SEND_DELAY_SEC)
        per_account_max = int(cfg_payload.get("per_account_max") or DEFAULT_PER_ACCOUNT_MAX)
        accounts = _resolve_send_accounts(owner_id, cfg_payload)
        cap = _send_capacity(accounts, cfg_payload)
        bot.edit_message_text(
            (
                "<b>Настройки отписа</b>\n"
                f"Режим: <b>{mode}</b>\n"
                f"Интервал: <b>{delay_sec} сек</b>\n"
                f"Лимит на аккаунт: <b>{per_account_max}</b>\n"
                f"Аккаунтов в запуске: <b>{len(accounts)}</b>\n"
                f"Макс. отпис за запуск: <b>{cap}</b>\n\n"
                "Если выбран режим «выбранные», отмечай нужные аккаунты ниже."
            ),
            c.message.chat.id,
            c.message.message_id,
            reply_markup=_kb_out_cfg(owner_id),
        )
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("t:out:cfg:mode:"))
    def cb_out_cfg_mode(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        mode = str(c.data.split(":")[4] or "").strip().lower()
        if mode not in {SEND_MODE_ALL, SEND_MODE_SELECTED}:
            bot.answer_callback_query(c.id, "Некорректный режим")
            return
        _set_out_cfg(int(c.from_user.id), {"send_mode": mode})
        cb_out_cfg(c)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("t:out:cfg:acc:"))
    def cb_out_cfg_acc(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        owner_id = int(c.from_user.id)
        try:
            aid = int(c.data.split(":")[4])
        except Exception:
            bot.answer_callback_query(c.id, "Некорректный аккаунт")
            return
        cfg_payload = _out_cfg(owner_id)
        selected = {
            int(x)
            for x in (cfg_payload.get("selected_account_ids") or [])
            if isinstance(x, int) and int(x) > 0
        }
        if aid in selected:
            selected.remove(aid)
        else:
            selected.add(aid)
        _set_out_cfg(owner_id, {"selected_account_ids": sorted(selected), "send_mode": SEND_MODE_SELECTED})
        cb_out_cfg(c)

    @bot.callback_query_handler(func=lambda c: c.data == "t:out:cfg:delay")
    def cb_out_cfg_delay(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        repo_states.set_state(conn, int(c.from_user.id), STATE_OWNER_TOOLS, {"section": "out_cfg", "step": "delay"}, tz)
        bot.send_message(c.message.chat.id, "Введи интервал между сообщениями на одном аккаунте (сек, 0-3600):")
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "t:out:cfg:max")
    def cb_out_cfg_max(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        repo_states.set_state(conn, int(c.from_user.id), STATE_OWNER_TOOLS, {"section": "out_cfg", "step": "per_account_max"}, tz)
        bot.send_message(c.message.chat.id, "Введи максимум сообщений на один аккаунт за запуск (1-500):")
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "t:out:new")
    def cb_out_new(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        owner_id = int(c.from_user.id)
        if _notify_active_job(c.message.chat.id, owner_id, c.id):
            return
        if not _parser_root():
            bot.answer_callback_query(c.id, "OWNER_PARSER_ROOT не настроен")
            return
        base_link = _hh_base_link(owner_id)
        draft = {
            "search_text": "",
            "search_link": base_link,
        }
        repo_states.set_state(conn, owner_id, STATE_OWNER_TOOLS, {"section": "out", "step": "direction", "draft": draft}, tz)
        source = f"Использую сохранённый фильтр HH:\n<code>{html.escape(base_link)}</code>" if base_link else "Сохранённый фильтр HH не задан.\nБудет использован только запрос по направлению."
        bot.send_message(
            c.message.chat.id,
            f"{source}\n\nВыбери профиль поиска:",
            reply_markup=_kb_dirs("t:out:dir", "t:out"),
        )
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "t:out:quick")
    def cb_out_quick(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        owner_id = int(c.from_user.id)
        if _notify_active_job(c.message.chat.id, owner_id, c.id):
            return
        if not _parser_root():
            bot.answer_callback_query(c.id, "OWNER_PARSER_ROOT не настроен")
            return
        base_link = _hh_base_link(owner_id)
        draft = {
            "search_text": "",
            "search_link": base_link,
            "flow": "quick",
        }
        repo_states.set_state(conn, owner_id, STATE_OWNER_TOOLS, {"section": "out", "step": "quick_direction", "draft": draft}, tz)
        source = (
            f"Быстрый запуск.\nИспользую сохранённый фильтр HH:\n<code>{html.escape(base_link)}</code>\n\n"
            "Выбери профиль поиска:"
            if base_link
            else "Быстрый запуск.\nФильтр HH не задан (поиск только по направлению).\n\nВыбери профиль поиска:"
        )
        bot.send_message(c.message.chat.id, source, reply_markup=_kb_dirs("t:out:qdir", "t:out"))
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "t:out:diag")
    def cb_out_diag(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        bot.answer_callback_query(c.id, "Готово")
        bot.send_message(c.message.chat.id, _out_diag_text(int(c.from_user.id)), reply_markup=_kb_out())

    @bot.callback_query_handler(func=lambda c: c.data == "t:out:unlock")
    def cb_out_unlock(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        owner_id = int(c.from_user.id)
        active = _active_job(owner_id)
        if not active:
            bot.answer_callback_query(c.id, "Активной задачи нет")
            return
        jid = int(active["id"] or 0)
        repo_owner_hh_jobs.mark_failed(conn, tz, jid, "manual reset via /t")
        bot.answer_callback_query(c.id, f"Сбросил #{jid}")
        bot.send_message(
            c.message.chat.id,
            f"Активная задача #{jid} помечена как остановленная вручную.",
            reply_markup=_kb_out(),
        )

    @bot.callback_query_handler(func=lambda c: c.data == "t:out:link")
    def cb_out_link(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        owner_id = int(c.from_user.id)
        cur = _hh_base_link(owner_id)
        repo_states.set_state(conn, owner_id, STATE_OWNER_TOOLS, {"section": "out", "step": "set_link", "draft": {}}, tz)
        bot.send_message(
            c.message.chat.id,
            "Отправь ссылку hh.ru/search/resume... для фильтра.\n"
            "Чтобы очистить сохранённую ссылку, отправь <code>-</code>.\n\n"
            f"Текущая: <code>{html.escape(cur or 'не задана')}</code>",
        )
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("t:out:plim:"))
    def cb_out_parse_limit_pick(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        st, data = repo_states.get_state(conn, int(c.from_user.id))
        if st != STATE_OWNER_TOOLS:
            bot.answer_callback_query(c.id, "Сессия устарела")
            return
        payload = str(c.data.split(":")[3] or "").strip().lower()
        draft = (data.get("draft") or {}).copy()
        if payload == "custom":
            repo_states.set_state(conn, int(c.from_user.id), STATE_OWNER_TOOLS, {"section": "out", "step": "direction", "draft": draft}, tz)
            bot.send_message(c.message.chat.id, "Выбери профиль поиска:", reply_markup=_kb_dirs("t:out:dir", "t:out"))
            bot.answer_callback_query(c.id)
            return
        repo_states.set_state(conn, int(c.from_user.id), STATE_OWNER_TOOLS, {"section": "out", "step": "direction", "draft": draft}, tz)
        bot.send_message(c.message.chat.id, "Выберите профиль поиска:", reply_markup=_kb_dirs("t:out:dir", "t:out"))
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("t:out:dir:"))
    def cb_out_dir(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        d = c.data.split(":")[3].strip().lower()
        st, data = repo_states.get_state(conn, int(c.from_user.id))
        if st != STATE_OWNER_TOOLS or d not in DIRECTIONS:
            bot.answer_callback_query(c.id)
            return
        draft = (data.get("draft") or {}).copy()
        draft["direction"] = d
        draft["search_text"] = _hh_query_for_direction(d)
        draft, _accounts, cap = _apply_runtime_to_draft(int(c.from_user.id), draft)
        repo_states.set_state(conn, int(c.from_user.id), STATE_OWNER_TOOLS, {"section": "out", "step": "send_limit", "draft": draft}, tz)
        if cap <= 0:
            bot.send_message(
                c.message.chat.id,
                "Нет авторизованных аккаунтов для отписа в текущих настройках.\n"
                "Открой «⚙️ Настройки отписа» и выбери аккаунты.",
                reply_markup=_kb_out(),
            )
            bot.answer_callback_query(c.id)
            return
        bot.send_message(
            c.message.chat.id,
            f"Выбери количество для отписа (0-{cap}):",
            reply_markup=_kb_limit_pick("t:out:slim", cap, allow_zero=True, back_cb="t:out"),
        )
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("t:out:qdir:"))
    def cb_out_quick_dir(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        d = c.data.split(":")[3].strip().lower()
        st, data = repo_states.get_state(conn, int(c.from_user.id))
        if st != STATE_OWNER_TOOLS or d not in DIRECTIONS or str((data or {}).get("step") or "") != "quick_direction":
            bot.answer_callback_query(c.id, "Сессия устарела")
            return
        draft = (data.get("draft") or {}).copy()
        draft["direction"] = d
        draft["search_text"] = _hh_query_for_direction(d)
        draft, accounts, cap = _apply_runtime_to_draft(int(c.from_user.id), draft)
        repo_states.set_state(conn, int(c.from_user.id), STATE_OWNER_TOOLS, {"section": "out", "step": "quick_send_limit", "draft": draft}, tz)
        if cap <= 0:
            bot.send_message(
                c.message.chat.id,
                "Нет авторизованных аккаунтов для отписа в текущих настройках.\n"
                "Открой «⚙️ Настройки отписа» и выбери аккаунты.",
                reply_markup=_kb_out(),
            )
            bot.answer_callback_query(c.id)
            return
        bot.send_message(
            c.message.chat.id,
            "Сколько людей нужно написать? (быстрый запуск)\n"
            "Парсинг рассчитаю автоматически.",
            reply_markup=_kb_limit_pick("t:out:qslim", cap, allow_zero=True, back_cb="t:out"),
        )
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("t:out:slim:"))
    def cb_out_send_limit_pick(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        st, data = repo_states.get_state(conn, int(c.from_user.id))
        if st != STATE_OWNER_TOOLS:
            bot.answer_callback_query(c.id, "Сессия устарела")
            return
        draft = (data.get("draft") or {}).copy()
        draft, _accounts, mx = _apply_runtime_to_draft(int(c.from_user.id), draft)
        payload = str(c.data.split(":")[3] or "").strip().lower()
        if payload == "custom":
            repo_states.set_state(conn, int(c.from_user.id), STATE_OWNER_TOOLS, {"section": "out", "step": "send_limit", "draft": draft}, tz)
            bot.send_message(c.message.chat.id, f"Введи количество для отписа (0-{mx}):")
            bot.answer_callback_query(c.id)
            return
        try:
            n = int(payload)
        except Exception:
            bot.answer_callback_query(c.id, "Некорректно")
            return
        if n < 0 or n > mx:
            bot.answer_callback_query(c.id, f"Диапазон 0-{mx}")
            return
        _send_out_confirm(c.message.chat.id, int(c.from_user.id), draft, n)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("t:out:qslim:"))
    def cb_out_quick_send_limit_pick(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        st, data = repo_states.get_state(conn, int(c.from_user.id))
        if st != STATE_OWNER_TOOLS or str((data or {}).get("step") or "") != "quick_send_limit":
            bot.answer_callback_query(c.id, "Сессия устарела")
            return
        draft = (data.get("draft") or {}).copy()
        draft, _accounts, mx = _apply_runtime_to_draft(int(c.from_user.id), draft)
        payload = str(c.data.split(":")[3] or "").strip().lower()
        if payload == "custom":
            repo_states.set_state(conn, int(c.from_user.id), STATE_OWNER_TOOLS, {"section": "out", "step": "quick_send_limit", "draft": draft}, tz)
            bot.send_message(c.message.chat.id, f"Введи количество для отписа (0-{mx}):")
            bot.answer_callback_query(c.id)
            return
        try:
            n = int(payload)
        except Exception:
            bot.answer_callback_query(c.id, "Некорректно")
            return
        if n < 0 or n > mx:
            bot.answer_callback_query(c.id, f"Диапазон 0-{mx}")
            return
        _send_out_confirm(c.message.chat.id, int(c.from_user.id), draft, n)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "t:out:run")
    def cb_out_run(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        st, data = repo_states.get_state(conn, int(c.from_user.id))
        if st != STATE_OWNER_TOOLS or (data or {}).get("step") != "confirm":
            bot.answer_callback_query(c.id, "Сессия устарела")
            return
        if _notify_active_job(c.message.chat.id, int(c.from_user.id), c.id):
            return
        draft = (data.get("draft") or {}).copy()
        draft, accounts, cap = _apply_runtime_to_draft(int(c.from_user.id), draft)
        direction = str(draft.get("direction") or "python")
        first_message = get_first_message(conn, direction)
        draft["message_text"] = first_message
        requested_send = int(max(int(draft.get("send_limit") or 0), 0))
        if requested_send > 0 and cap <= 0:
            bot.answer_callback_query(c.id, "Нет аккаунтов для запуска")
            bot.send_message(
                c.message.chat.id,
                "Нет авторизованных аккаунтов для отписа. Открой «⚙️ Настройки отписа».",
                reply_markup=_kb_out(),
            )
            return
        if requested_send > cap > 0:
            draft["send_limit"] = cap
        job_id = repo_owner_hh_jobs.create(
            conn,
            tz,
            owner_tg_id=int(c.from_user.id),
            owner_chat_id=int(c.message.chat.id),
            direction=direction,
            script_version="script4",
            ai_enabled=script_ai_enabled(conn),
            search_text=str(draft.get("search_text") or "").strip() or None,
            search_link=str(draft.get("search_link") or "").strip() or None,
            parse_limit=max(1, _auto_parse_limit(int(draft.get("send_limit") or 0))),
            send_limit=int(draft.get("send_limit") or 0),
            message_text=first_message.strip() or None,
        )
        repo_states.clear_state(conn, int(c.from_user.id), tz)
        _run_campaign(job_id, int(c.from_user.id), int(c.message.chat.id), draft)
        bot.edit_message_text(f"Задача #{job_id} запущена.", c.message.chat.id, c.message.message_id, reply_markup=_kb_out())
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "t:out:files")
    def cb_out_files(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        bot.edit_message_text(
            "Выбери формат выдачи контактов.\nJSON-контакты не отправляются.",
            c.message.chat.id,
            c.message.message_id,
            reply_markup=_kb_out_files(),
        )
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("t:out:files:"))
    def cb_out_files_kind(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        kind = str(c.data.split(":")[3] or "").strip().lower()
        row = repo_owner_hh_jobs.last_by_owner(conn, int(c.from_user.id))
        if not row:
            bot.answer_callback_query(c.id, "Нет задач")
            return
        ok = False
        if kind == "csv":
            ok = _send_file(c.message.chat.id, str(row["contacts_file"] or ""), f"Контакты CSV (#{int(row['id'])})")
        elif kind == "txt":
            ok = _send_file(c.message.chat.id, _contacts_txt_path_from_job(row), f"Контакты TXT (#{int(row['id'])})")
        elif kind == "report":
            ok = _send_file(c.message.chat.id, str(row["report_file"] or ""), f"Отчёт (#{int(row['id'])})")
        if not ok:
            bot.send_message(c.message.chat.id, "Файлов пока нет.")
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "t:out:ai")
    def cb_out_ai(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        new_state = not script_ai_enabled(conn)
        set_ai_enabled(conn, new_state)
        # В этой версии единый тумблер: включили AI = включили и авто-обработку.
        set_ai_auto_enabled(conn, new_state)
        _show_out(c.message.chat.id, int(c.from_user.id), c.message.message_id)
        bot.answer_callback_query(c.id, "Переключено")

    @bot.callback_query_handler(func=lambda c: c.data == "t:out:ai:auto")
    def cb_out_ai_auto(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        # backward-compatible callback для старых сообщений: ведет к общему тумблеру AI.
        new_state = not script_ai_enabled(conn)
        set_ai_enabled(conn, new_state)
        set_ai_auto_enabled(conn, new_state)
        _show_out(c.message.chat.id, int(c.from_user.id), c.message.message_id)
        bot.answer_callback_query(c.id, "Сохранено")

    @bot.callback_query_handler(func=lambda c: c.data == "t:out:ai:interval")
    def cb_out_ai_interval(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        current = ai_auto_interval_sec(conn)
        repo_states.set_state(conn, int(c.from_user.id), STATE_OWNER_TOOLS, {"section": "out_ai", "step": "interval"}, tz)
        bot.send_message(
            c.message.chat.id,
            f"Введи интервал авто-обработки ответов (сек, 10-3600).\nТекущий: <b>{current}</b>",
        )
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "t:out:ai:rules")
    def cb_out_ai_rules(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        _save_ai_guardrails_file(int(c.from_user.id))
        bot.send_message(c.message.chat.id, _ai_rules_preview(), reply_markup=_kb_ai_rules())
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "t:out:ai:rules:add")
    def cb_out_ai_rules_add(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        repo_states.set_state(conn, int(c.from_user.id), STATE_OWNER_TOOLS, {"section": "out_ai_rules", "step": "add"}, tz)
        bot.send_message(c.message.chat.id, "Отправь фразу, которую запрещаем в AI-ответах.")
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "t:out:ai:rules:reset")
    def cb_out_ai_rules_reset(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        reset_ai_guardrails(conn)
        _save_ai_guardrails_file(int(c.from_user.id))
        bot.send_message(c.message.chat.id, "AI-запреты сброшены к базовым.", reply_markup=_kb_ai_rules())
        bot.answer_callback_query(c.id, "Сброшено")

    @bot.callback_query_handler(func=lambda c: c.data == "t:out:ai:rules:clear")
    def cb_out_ai_rules_clear(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        rules = script_ai_guardrails(conn)
        rules["forbidden_substrings"] = []
        set_ai_guardrails(conn, rules)
        _save_ai_guardrails_file(int(c.from_user.id))
        bot.send_message(c.message.chat.id, "Список фраз-запретов очищен.", reply_markup=_kb_ai_rules())
        bot.answer_callback_query(c.id, "Очищено")

    @bot.callback_query_handler(func=lambda c: c.data == "t:out:ai:test")
    def cb_out_ai_test(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        owner_id = int(c.from_user.id)
        guardrails_path = _save_ai_guardrails_file(owner_id)
        scenarios_path = _ensure_ai_scenarios_file()
        out_json = _dir_parser() / f"ai_test_report_{_stamp()}.json"
        out_txt = _dir_parser() / f"ai_test_report_{_stamp()}.txt"
        out_html = _dir_parser() / f"ai_test_report_{_stamp()}.html"
        cmd = [
            sys.executable,
            str(PROJECT_ROOT / "scripts" / "owner_ai_dialog_tester.py"),
            "--provider",
            _ai_provider(),
            "--base-url",
            _ai_base_url(),
            "--http-proxy",
            _ai_http_proxy(),
            "--model",
            _openai_model(),
            "--timeout-sec",
            str(_ai_timeout_sec()),
            "--strict-mode",
            "1" if _ai_strict_mode() else "0",
            "--scenarios-file",
            str(scenarios_path),
            "--guardrails-file",
            str(guardrails_path),
            "--report-json",
            str(out_json),
            "--report-txt",
            str(out_txt),
            "--report-html",
            str(out_html),
        ]
        proc_env = os.environ.copy()
        existing_pythonpath = str(proc_env.get("PYTHONPATH") or "").strip()
        proc_env["PYTHONPATH"] = (
            f"{PROJECT_ROOT}{os.pathsep}{existing_pythonpath}" if existing_pythonpath else str(PROJECT_ROOT)
        )
        proc_env["OWNER_OPENAI_API_KEY"] = _openai_key()
        proc_env["OWNER_AI_PROXY_AUTH"] = _ai_proxy_auth()
        proc_env["OWNER_AI_HTTP_PROXY"] = _ai_http_proxy()
        bot.answer_callback_query(c.id, "Запускаю AI-тесты...")
        proc = subprocess.run(
            cmd,
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=240,
            env=proc_env,
        )
        rep = _read_json(out_json)
        if proc.returncode != 0:
            err_text = (proc.stderr or proc.stdout or "").strip()[:900]
            bot.send_message(
                c.message.chat.id,
                "AI-тесты завершились с ошибкой.\n"
                f"<code>{html.escape(err_text or 'unknown error')}</code>",
                reply_markup=_kb_out(),
            )
            return
        total = int(rep.get("total") or 0)
        passed = int(rep.get("passed") or 0)
        failed = int(rep.get("failed") or 0)
        llm_ok = int(rep.get("llm_ok") or 0)
        llm_failed = int(rep.get("llm_failed") or 0)
        bot.send_message(
            c.message.chat.id,
            (
                "<b>AI-тесты завершены</b>\n"
                f"Сценариев: <b>{total}</b>\n"
                f"Пройдено: <b>{passed}</b> • Провалено: <b>{failed}</b>\n"
                f"LLM ok/failed: <b>{llm_ok}/{llm_failed}</b>"
            ),
            reply_markup=_kb_out(),
        )
        _send_file(c.message.chat.id, str(out_txt), "Отчёт AI-тестов (TXT)")
        _send_file(c.message.chat.id, str(out_html), "Отчёт AI-тестов (HTML)")

    @bot.callback_query_handler(func=lambda c: c.data == "t:out:script")
    def cb_out_script(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        bot.edit_message_text("Скрипт 4: выбери направление.", c.message.chat.id, c.message.message_id, reply_markup=_kb_dirs("t:out:script:dir", "t:out"))
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("t:out:script:dir:"))
    def cb_out_script_dir(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        d = c.data.split(":")[4].strip().lower()
        if d not in DIRECTIONS:
            bot.answer_callback_query(c.id, "Неизвестно")
            return
        steps_count = len(_script_steps_for_direction(d))
        bot.edit_message_text(
            f"Скрипт 4 · {_label_dir(d)}\nШагов: <b>{steps_count}</b>",
            c.message.chat.id,
            c.message.message_id,
            reply_markup=_kb_script_steps(d),
        )
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("t:out:script:step:"))
    def cb_out_script_step(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        p = c.data.split(":")
        d = p[4].strip().lower()
        if d not in DIRECTIONS:
            bot.answer_callback_query(c.id, "Направление не найдено")
            return
        try:
            i = int(p[5])
        except Exception:
            bot.answer_callback_query(c.id, "Шаг не найден")
            return
        max_index = max(len(_script_steps_for_direction(d)) - 1, 0)
        if i < 0 or i > max_index:
            bot.answer_callback_query(c.id, "Шаг вне диапазона")
            return
        cur = get_step(conn, d, i)
        repo_states.set_state(conn, int(c.from_user.id), STATE_OWNER_TOOLS, {"section": "script", "direction": d, "step_index": i}, tz)
        bot.send_message(c.message.chat.id, f"Текущее:\n<code>{html.escape(cur)}</code>\n\nОтправь новый текст шага {i+1}.")
        bot.answer_callback_query(c.id)

    _replies_lock = threading.Lock()
    _auto_ai_thread_started = {"value": False}
    _manual_replies_priority_until = {"ts": 0.0}
    _manual_replies_worker_running = {"value": False}
    # Ограничиваем объем на один аккаунт за цикл, чтобы lock не висел слишком долго.
    REPLIES_MAX_LEADS_PER_ACCOUNT = 40

    def _process_replies_once(owner_id: int, chat_id: int | None, notify: bool, source: str = "manual") -> dict[str, Any]:
        is_manual = source == "manual"
        if is_manual:
            _manual_replies_priority_until["ts"] = time.time() + 120.0
        if is_manual:
            # Ручной запуск не должен отваливаться из-за авто-цикла:
            # ждем освобождения lock и выполняем запрос.
            acquired = _replies_lock.acquire(blocking=True)
        else:
            acquired = _replies_lock.acquire(blocking=False)
        if not acquired:
            if notify and chat_id is not None:
                bot.send_message(chat_id, "Обработка уже выполняется, подожди завершения.", reply_markup=_kb_out())
            return {"status": "busy", "source": source}
        try:
            accounts = _authorized_accounts(owner_id)
            if not accounts:
                if notify and chat_id is not None:
                    bot.send_message(chat_id, "Нет авторизованных аккаунтов.", reply_markup=_kb_mod())
                return {"status": "no_accounts", "source": source}
            polygons = [a for a in accounts if _account_type(a["account_type"]) == ACCOUNT_TYPE_POLYGON]
            observers = [a for a in accounts if _account_type(a["account_type"]) != ACCOUNT_TYPE_POLYGON]
            account_by_id = {int(a["id"] or 0): a for a in accounts}
            fallback_acc = _active_account(owner_id) or accounts[0]
            fallback_id = int(fallback_acc["id"] or 0)
            raw_leads = [
                r
                for r in repo_owner_outreach.list_recent(conn, owner_id, 1200)
                if str(r["status"] or "") in {
                    "sent",
                    "send_failed",
                    "replied",
                    "interested",
                    "call_booked",
                    "no_reply",
                    "not_interested",
                    "needs_review",
                }
            ]
            # Дедуп по telegram, чтобы в одном цикле не гонять одни и те же контакты из разных кампаний.
            seen_tg: set[str] = set()
            leads: list[sqlite3.Row] = []
            for r in raw_leads:
                _tg, tg_norm = _normalize_tg(str(r["telegram"] or ""))
                key = tg_norm or f"lead:{int(r['id'] or 0)}"
                if key in seen_tg:
                    continue
                seen_tg.add(key)
                leads.append(r)
            if not leads:
                # Важный кейс: лид может написать первым, и тогда в owner_outreach_leads ещё нет записи.
                # Если AI включён и есть полигон-аккаунты, всё равно запускаем fallback-скан непрочитанных диалогов.
                if not (script_ai_enabled(conn) and len(polygons) > 0):
                    if notify and chat_id is not None:
                        bot.send_message(chat_id, "Нет лидов для обработки.", reply_markup=_kb_out())
                    return {"status": "no_leads", "source": source}

            leads_by_account: dict[int, list[sqlite3.Row]] = {int(a["id"]): [] for a in accounts}
            for lead in leads:
                _tg, tg_norm = _normalize_tg(str(lead["telegram"] or ""))
                aid = 0
                if tg_norm:
                    row = conn.execute(
                        "SELECT last_account_id FROM owner_outreach_contacted "
                        "WHERE owner_tg_id=? AND telegram_norm=? LIMIT 1",
                        (owner_id, tg_norm),
                    ).fetchone()
                    if row and int(row["last_account_id"] or 0) > 0:
                        aid = int(row["last_account_id"] or 0)
                # Если аккаунт неизвестен, проверяем лид на всех аккаунтах.
                # Это нужно для кейса, когда лид написал первым или ранее не был закреплен в таблице contacted.
                if aid <= 0 or aid not in account_by_id:
                    unknown_pool = polygons if script_ai_enabled(conn) and len(polygons) > 0 else accounts
                    for acc_row in unknown_pool:
                        leads_by_account.setdefault(int(acc_row["id"] or 0), []).append(lead)
                    continue
                leads_by_account.setdefault(aid, []).append(lead)
            # Ограничиваем объем на цикл обработки, чтобы не держать lock слишком долго.
            for aid in list(leads_by_account.keys()):
                chunk = leads_by_account.get(aid) or []
                if len(chunk) > REPLIES_MAX_LEADS_PER_ACCOUNT:
                    leads_by_account[aid] = chunk[:REPLIES_MAX_LEADS_PER_ACCOUNT]

            if script_ai_enabled(conn) and len(polygons) <= 0:
                if notify and chat_id is not None:
                    bot.send_message(
                        chat_id,
                        "AI включен, но полигон-аккаунтов нет. "
                        "Добавь тип «полигон» в /t -> Модерирование -> Типы аккаунтов.",
                        reply_markup=_kb_mod(),
                    )
                return {"status": "no_polygon_accounts", "source": source}
            stamp = _stamp()
            f_script = _dir_parser() / f"script4_{stamp}.json"
            f_report = _dir_parser() / f"replies_report_{stamp}.json"
            f_script.write_text(json.dumps(get_script4(conn), ensure_ascii=False, indent=2), encoding="utf-8")
            memory_path = _ai_memory_path()
            if not memory_path.exists():
                _save_ai_memory([])

            summary: dict[str, Any] = {
                "status": "done",
                "source": source,
                "processed": 0,
                "updated": 0,
                "ai_sent": 0,
                "needs_review": 0,
                "duplicate_blocked": 0,
                "ai_blocked_non_polygon": 0,
                "llm_ok": 0,
                "llm_failed": 0,
                "transcribe_ok": 0,
                "transcribe_failed": 0,
                "accounts_total": len(accounts),
                "observer_accounts": len(observers),
                "polygon_accounts": len(polygons),
                "accounts": [],
                "details": [],
                "memory_added": 0,
                "report_file": str(f_report),
            }
            updated_leads: set[int] = set()

            def _apply_details(details: list[dict[str, Any]]) -> int:
                upd_local = 0
                for d in details:
                    if not isinstance(d, dict):
                        continue
                    lid = int(d.get("lead_id") or 0)
                    if lid <= 0:
                        continue
                    status_key = str(d.get("status") or "").strip().lower()
                    note = str(d.get("note") or "").strip().lower()
                    if note == "no_inbound_in_account":
                        # Лид назначен на проверку нескольким аккаунтам; без входящего по конкретному аккаунту
                        # не меняем статус, чтобы не затирать полезный апдейт с другого аккаунта.
                        continue
                    mapped = {
                        "no_reply": repo_owner_outreach.LEAD_NO_REPLY,
                        "not_interested": repo_owner_outreach.LEAD_NOT_INTERESTED,
                        "interested": repo_owner_outreach.LEAD_INTERESTED,
                        "call_booked": repo_owner_outreach.LEAD_CALL_BOOKED,
                        "replied": repo_owner_outreach.LEAD_REPLIED,
                        "call_done": repo_owner_outreach.LEAD_CALL_DONE,
                        "blocked": repo_owner_outreach.LEAD_BLOCKED,
                        "failed": repo_owner_outreach.LEAD_SEND_FAILED,
                        "needs_review": repo_owner_outreach.LEAD_NEEDS_REVIEW,
                    }.get(status_key, repo_owner_outreach.LEAD_REPLIED)
                    existing = conn.execute(
                        "SELECT status FROM owner_outreach_leads WHERE id=? LIMIT 1",
                        (lid,),
                    ).fetchone()
                    current_status = str(existing["status"] or "").strip().lower() if existing else ""
                    # Не откатываем "сильные" статусы назад в no_reply/replied.
                    if mapped in {repo_owner_outreach.LEAD_NO_REPLY, repo_owner_outreach.LEAD_REPLIED} and current_status in {
                        repo_owner_outreach.LEAD_NOT_INTERESTED,
                        repo_owner_outreach.LEAD_BLOCKED,
                        repo_owner_outreach.LEAD_INTERESTED,
                        repo_owner_outreach.LEAD_CALL_BOOKED,
                        repo_owner_outreach.LEAD_CALL_DONE,
                        repo_owner_outreach.LEAD_NEEDS_REVIEW,
                    }:
                        continue
                    repo_owner_outreach.set_lead_status(
                        conn,
                        tz,
                        lid,
                        mapped,
                        inbound_text=str(d.get("inbound_text") or "").strip() or None,
                        outbound_text=str(d.get("outbound_text") or "").strip() or None,
                        ai_step=int(d.get("ai_step") or 0),
                        send_error=str(d.get("error") or "").strip() or None,
                    )
                    if lid not in updated_leads:
                        updated_leads.add(lid)
                        upd_local += 1
                return upd_local

            # При ручном запуске обрабатываем все аккаунты.
            # В автоцикле приоритизируем polygon-аккаунты, чтобы ответы приходили быстрее
            # и lock не удерживался слишком долго на observer-аккаунтах.
            if source == "auto" and script_ai_enabled(conn) and polygons:
                run_plan = polygons
            else:
                # - полигон: анализ + отправка AI
                # - стандарт: только анализ/классификация (без отправки)
                # Это закрывает кейс "лид написал на стандартный аккаунт", но при этом
                # не нарушает правило "писать только с полигона".
                run_plan = observers + polygons
            for acc in run_plan:
                aid = int(acc["id"] or 0)
                acc_kind = _account_type(acc["account_type"])
                send_ai = 1 if acc_kind == ACCOUNT_TYPE_POLYGON and script_ai_enabled(conn) else 0
                leads_chunk = leads_by_account.get(aid) or []
                if not leads_chunk:
                    # Для полигонов запускаем даже с пустым списком — внутри скрипта сработает unread_dialog_fallback.
                    if send_ai == 1:
                        leads_chunk = []
                    else:
                        summary["accounts"].append(
                            {
                                "account_id": aid,
                                "account_title": str(acc["title"] or f"account-{aid}"),
                                "account_type": acc_kind,
                                "send_ai": 0,
                                "processed": 0,
                                "updated": 0,
                                "ai_sent": 0,
                                "status": "skipped_no_leads",
                            }
                        )
                        continue
                f_in = _dir_parser() / f"replies_in_{stamp}_acc{aid}.json"
                f_out = _dir_parser() / f"replies_report_{stamp}_acc{aid}.json"
                f_in.write_text(
                    json.dumps(
                        [
                            {
                                "lead_id": int(r["id"]),
                                "telegram": str(r["telegram"]),
                                "direction": str(r["direction"] or "python"),
                                "status": str(r["status"] or ""),
                                "ai_step": int(r["ai_step"] or 1),
                                "last_inbound_text": str(r["inbound_text"] or ""),
                                "last_outbound_text": str(r["outbound_text"] or ""),
                            }
                            for r in leads_chunk
                        ],
                        ensure_ascii=False,
                        indent=2,
                    ),
                    encoding="utf-8",
                )
                session_src = str(acc["session_file"] or "")
                session_for_run = _prepare_session_copy(session_src, aid, "replies")
                proc_err = ""
                try:
                    tg_proxy = _tg_proxy()
                    cmd = [
                        sys.executable,
                        str(PROJECT_ROOT / "scripts" / "owner_tg_process_replies.py"),
                        "--api-id",
                        str(int(acc["api_id"] or 0)),
                        "--api-hash",
                        str(acc["api_hash"] or ""),
                        "--session-file",
                        session_for_run,
                        "--leads-file",
                        str(f_in),
                        "--script-file",
                        str(f_script),
                        "--report-file",
                        str(f_out),
                        "--ai-enabled",
                        "1" if script_ai_enabled(conn) else "0",
                        "--send-ai",
                        str(int(send_ai)),
                        "--account-type",
                        str(acc_kind),
                        "--examples-file",
                        str(memory_path),
                        "--guardrails-file",
                        str(_save_ai_guardrails_file(owner_id)),
                        "--openai-model",
                        _openai_model(),
                        "--ai-provider",
                        _ai_provider(),
                        "--ai-base-url",
                        _ai_base_url(),
                        "--ai-http-proxy",
                        _ai_http_proxy(),
                        "--ai-timeout-sec",
                        str(_ai_timeout_sec()),
                        "--ai-strict-mode",
                        "1" if _ai_strict_mode() else "0",
                        "--transcribe-audio",
                        str(int(os.getenv("OWNER_TG_TRANSCRIBE_AUDIO", "1") or 1)),
                        "--transcribe-max-per-dialog",
                        str(int(os.getenv("OWNER_TG_TRANSCRIBE_MAX_PER_DIALOG", "4") or 4)),
                        "--transcribe-poll-attempts",
                        str(int(os.getenv("OWNER_TG_TRANSCRIBE_POLL_ATTEMPTS", "3") or 3)),
                        "--transcribe-poll-delay-ms",
                        str(int(os.getenv("OWNER_TG_TRANSCRIBE_POLL_DELAY_MS", "1200") or 1200)),
                        "--fallback-max-dialogs",
                        str(int(os.getenv("OWNER_TG_REPLIES_FALLBACK_MAX_DIALOGS", "20") or 20)),
                        "--fallback-max-seconds",
                        str(int(os.getenv("OWNER_TG_REPLIES_FALLBACK_MAX_SECONDS", "60") or 60)),
                    ]
                    if tg_proxy:
                        cmd.extend(["--proxy", tg_proxy])
                    proc_env = os.environ.copy()
                    existing_pythonpath = str(proc_env.get("PYTHONPATH") or "").strip()
                    proc_env["PYTHONPATH"] = (
                        f"{PROJECT_ROOT}{os.pathsep}{existing_pythonpath}" if existing_pythonpath else str(PROJECT_ROOT)
                    )
                    proc_env["OWNER_OPENAI_API_KEY"] = _openai_key()
                    proc_env["OWNER_AI_PROXY_AUTH"] = _ai_proxy_auth()
                    proc_env["OWNER_AI_HTTP_PROXY"] = _ai_http_proxy()
                    if tg_proxy:
                        proc_env["OWNER_TG_PROXY"] = tg_proxy
                    timeout_sec = max(
                        60,
                        int(
                            os.getenv(
                                "OWNER_TG_REPLIES_SUBPROCESS_TIMEOUT_SEC",
                                "180",
                            )
                            or 180
                        ),
                    )
                    proc = subprocess.run(
                        cmd,
                        cwd=str(PROJECT_ROOT),
                        capture_output=True,
                        text=True,
                        timeout=timeout_sec,
                        env=proc_env,
                    )
                    if proc.returncode != 0:
                        proc_err = (proc.stderr or proc.stdout or "").strip()[:1500]
                finally:
                    _cleanup_session_copy(session_for_run, session_src)

                rep = _read_json(f_out)
                if (not rep or not isinstance(rep, dict)) and proc_err:
                    rep = {
                        "status": "failed",
                        "error": proc_err,
                        "processed": 0,
                        "updated": 0,
                        "ai_sent": 0,
                        "needs_review": 0,
                        "duplicate_blocked": 0,
                        "llm_ok": 0,
                        "llm_failed": 0,
                        "details": [],
                    }
                details = rep.get("details") if isinstance(rep, dict) else []
                if not isinstance(details, list):
                    details = []
                upd_count = _apply_details(details)
                learned = rep.get("learn_pairs") if isinstance(rep, dict) else []
                if not isinstance(learned, list):
                    learned = []
                summary["memory_added"] = int(summary["memory_added"]) + _update_ai_memory(learned)
                summary["processed"] = int(summary["processed"]) + int(rep.get("processed") or 0)
                summary["updated"] = int(summary["updated"]) + upd_count
                summary["ai_sent"] = int(summary["ai_sent"]) + int(rep.get("ai_sent") or 0)
                summary["needs_review"] = int(summary["needs_review"]) + int(rep.get("needs_review") or 0)
                summary["duplicate_blocked"] = int(summary["duplicate_blocked"]) + int(rep.get("duplicate_blocked") or 0)
                summary["ai_blocked_non_polygon"] = int(summary["ai_blocked_non_polygon"]) + int(rep.get("ai_blocked_non_polygon") or 0)
                summary["llm_ok"] = int(summary["llm_ok"]) + int(rep.get("llm_ok") or 0)
                summary["llm_failed"] = int(summary["llm_failed"]) + int(rep.get("llm_failed") or 0)
                summary["transcribe_ok"] = int(summary["transcribe_ok"]) + int(rep.get("transcribe_ok") or 0)
                summary["transcribe_failed"] = int(summary["transcribe_failed"]) + int(rep.get("transcribe_failed") or 0)
                for d in details:
                    if isinstance(d, dict):
                        summary["details"].append(
                            {
                                **d,
                                "account_id": aid,
                                "account_title": str(acc["title"] or f"account-{aid}"),
                                "account_type": acc_kind,
                            }
                        )
                summary["accounts"].append(
                    {
                        "account_id": aid,
                        "account_title": str(acc["title"] or f"account-{aid}"),
                        "account_type": acc_kind,
                        "send_ai": int(send_ai),
                        "processed": int(rep.get("processed") or 0),
                        "updated": upd_count,
                        "ai_sent": int(rep.get("ai_sent") or 0),
                        "needs_review": int(rep.get("needs_review") or 0),
                        "duplicate_blocked": int(rep.get("duplicate_blocked") or 0),
                        "llm_ok": int(rep.get("llm_ok") or 0),
                        "llm_failed": int(rep.get("llm_failed") or 0),
                        "status": str(rep.get("status") or ("failed" if proc_err else "done")),
                        "error": str(rep.get("error") or "").strip() or proc_err,
                        "report_file": str(f_out),
                    }
                )

            f_report.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
            if notify and chat_id is not None:
                if script_ai_enabled(conn) and len(polygons) <= 0:
                    bot.send_message(
                        chat_id,
                        "AI включен, но полигон-аккаунтов нет. "
                        "Добавь тип «полигон» в /t -> Модерирование -> Типы аккаунтов.",
                        reply_markup=_kb_mod(),
                    )
                bot.send_message(
                    chat_id,
                    (
                        "Обработка завершена.\n"
                        f"Аккаунтов: {len(accounts)} (полигон: {len(polygons)}, стандарт: {len(observers)})\n"
                        f"Обработано: {int(summary.get('processed') or 0)}\n"
                        f"Обновлено лидов: {int(summary.get('updated') or 0)}\n"
                        f"AI-ответов отправлено: {int(summary.get('ai_sent') or 0)}\n"
                        f"AI заблокировано (не polygon): {int(summary.get('ai_blocked_non_polygon') or 0)}\n"
                        f"Требует ручного разбора: {int(summary.get('needs_review') or 0)}\n"
                        f"Антидубль сработал: {int(summary.get('duplicate_blocked') or 0)}\n"
                        f"LLM ok/failed: {int(summary.get('llm_ok') or 0)}/{int(summary.get('llm_failed') or 0)}\n"
                        f"Транскрибация голосовых ok/failed: {int(summary.get('transcribe_ok') or 0)}/{int(summary.get('transcribe_failed') or 0)}\n"
                        f"Обучающих пар добавлено: {int(summary.get('memory_added') or 0)}"
                    ),
                    reply_markup=_kb_out(),
                )
                _send_file(chat_id, str(f_report), "Отчёт обработки ответов")
            return summary
        except Exception as exc:
            if notify and chat_id is not None:
                bot.send_message(chat_id, f"Ошибка обработки: <code>{html.escape(str(exc))}</code>", reply_markup=_kb_out())
            return {"status": "failed", "error": str(exc), "source": source}
        finally:
            if is_manual:
                _manual_replies_priority_until["ts"] = 0.0
            _replies_lock.release()

    @bot.callback_query_handler(func=lambda c: c.data == "t:out:process")
    def cb_out_process(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        owner_id = int(c.from_user.id)
        if _notify_active_job(c.message.chat.id, owner_id, c.id):
            return
        if _manual_replies_worker_running["value"]:
            bot.answer_callback_query(c.id, "Ручная обработка уже запущена")
            return
        _manual_replies_worker_running["value"] = True
        bot.answer_callback_query(c.id, "Запустил обработку")

        def worker() -> None:
            try:
                _process_replies_once(owner_id, int(c.message.chat.id), notify=True, source="manual")
            finally:
                _manual_replies_worker_running["value"] = False

        threading.Thread(target=worker, daemon=True, name="owner-replies-manual").start()

    @bot.callback_query_handler(func=lambda c: c.data in {"t:react", "t:react:menu"})
    def cb_react(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        _show_react(c.message.chat.id, int(c.from_user.id), c.message.message_id)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "t:react:auto")
    def cb_react_auto(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        set_reactions_auto_enabled(conn, not reactions_auto_enabled(conn))
        _show_react(c.message.chat.id, int(c.from_user.id), c.message.message_id)
        bot.answer_callback_query(c.id, "Сохранено")

    @bot.callback_query_handler(func=lambda c: c.data == "t:react:run" or c.data.startswith("t:react:run:"))
    def cb_react_run(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        react_limit = 20
        parts = str(c.data or "").split(":")
        if len(parts) >= 4:
            try:
                react_limit = int(parts[3])
            except Exception:
                react_limit = 20
        react_limit = max(1, min(react_limit, 50))
        owner_id = int(c.from_user.id)
        accounts = _authorized_accounts(owner_id)
        if not accounts:
            bot.answer_callback_query(c.id, "Нет авторизованных аккаунтов")
            return
        channels = reaction_channels(conn)
        if not channels:
            bot.answer_callback_query(c.id, "Список каналов пуст")
            return
        bot.answer_callback_query(c.id, f"Запустил ({react_limit})")

        def worker() -> None:
            try:
                summary: dict[str, Any] = {
                    "status": "done",
                    "limit_per_channel": react_limit,
                    "channels": channels,
                    "accounts": [],
                    "ok": 0,
                    "failed": 0,
                    "processed": 0,
                }
                for acc in accounts:
                    aid = int(acc["id"] or 0)
                    atitle = str(acc["title"] or f"account-{aid}")
                    for ch in channels:
                        out = _dir_react() / f"reactions_{aid}_{ch}_{_stamp()}.json"
                        session_src = str(acc["session_file"] or "")
                        session_for_run = _prepare_session_copy(session_src, aid, "react")
                        try:
                            tg_proxy = _tg_proxy()
                            cmd = [
                                sys.executable,
                                str(PROJECT_ROOT / "scripts" / "owner_tg_channel_react.py"),
                                "--api-id",
                                str(int(acc["api_id"] or 0)),
                                "--api-hash",
                                str(acc["api_hash"] or ""),
                                "--session-file",
                                session_for_run,
                                "--channel",
                                ch,
                                "--limit",
                                str(react_limit),
                                "--report-file",
                                str(out),
                            ]
                            if tg_proxy:
                                cmd.extend(["--proxy", tg_proxy])
                            proc_env = os.environ.copy()
                            existing_pythonpath = str(proc_env.get("PYTHONPATH") or "").strip()
                            proc_env["PYTHONPATH"] = (
                                f"{PROJECT_ROOT}{os.pathsep}{existing_pythonpath}" if existing_pythonpath else str(PROJECT_ROOT)
                            )
                            if tg_proxy:
                                proc_env["OWNER_TG_PROXY"] = tg_proxy
                            proc = subprocess.run(
                                cmd,
                                cwd=str(PROJECT_ROOT),
                                capture_output=True,
                                text=True,
                                timeout=900,
                                env=proc_env,
                            )
                        finally:
                            _cleanup_session_copy(session_for_run, session_src)
                        rep = _read_json(out)
                        if not rep:
                            rep = {
                                "status": "failed",
                                "error": (proc.stderr or proc.stdout or "").strip() or "unknown",
                                "processed_messages": 0,
                                "ok": 0,
                                "failed": 1,
                                "details": [],
                            }
                        for d in rep.get("details") or []:
                            if isinstance(d, dict):
                                repo_owner_reactions.add(
                                    conn,
                                    tz,
                                    owner_tg_id=owner_id,
                                    account_id=aid,
                                    channel=ch,
                                    message_id=int(d.get("message_id") or 0),
                                    reaction=str(d.get("reaction") or ""),
                                    status=str(d.get("status") or "ok"),
                                    error_text=str(d.get("error") or "").strip() or None,
                                    payload={**d, "source": "manual_run"},
                                )
                        summary["ok"] = int(summary["ok"]) + int(rep.get("ok") or 0)
                        summary["failed"] = int(summary["failed"]) + int(rep.get("failed") or 0)
                        summary["processed"] = int(summary["processed"]) + int(rep.get("processed_messages") or 0)
                        summary["accounts"].append(
                            {
                                "account_id": aid,
                                "account_title": atitle,
                                "channel": ch,
                                "status": str(rep.get("status") or ""),
                                "ok": int(rep.get("ok") or 0),
                                "failed": int(rep.get("failed") or 0),
                                "processed_messages": int(rep.get("processed_messages") or 0),
                                "error": str(rep.get("error") or "").strip(),
                                "report_file": str(out),
                            }
                        )
                out = _dir_react() / f"reactions_{_stamp()}.json"
                out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
                bot.send_message(
                    c.message.chat.id,
                    (
                        "Реакции завершены.\n"
                        f"Аккаунтов: {len(accounts)}, каналов: {len(channels)}\n"
                        f"Обработано сообщений: {int(summary['processed'])}\n"
                        f"Успешно: {int(summary['ok'])}, ошибок: {int(summary['failed'])}"
                    ),
                    reply_markup=_kb_react(),
                )
                _send_file(c.message.chat.id, str(out), "Отчёт реакций")
            except Exception as exc:
                bot.send_message(c.message.chat.id, f"Ошибка реакций: <code>{html.escape(str(exc))}</code>", reply_markup=_kb_react())

        threading.Thread(target=worker, daemon=True, name="owner-react").start()

    @bot.callback_query_handler(func=lambda c: c.data == "t:react:export")
    def cb_react_export(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        rows = repo_owner_reactions.list_recent(conn, int(c.from_user.id), 2000)
        if not rows:
            bot.answer_callback_query(c.id, "Нет логов")
            return
        buf = io.StringIO()
        wr = csv.writer(buf)
        wr.writerow(["id", "created_at", "channel", "message_id", "reaction", "status", "error"])
        for r in rows:
            wr.writerow([int(r["id"]), str(r["created_at"] or ""), str(r["channel"] or ""), int(r["message_id"] or 0), str(r["reaction"] or ""), str(r["status"] or ""), str(r["error_text"] or "")])
        payload = buf.getvalue().encode("utf-8")
        bot.send_document(
            c.message.chat.id,
            BufferedInputFile(payload, filename=f"reaction_stats_{_stamp()}.csv"),
            caption="Логи реакций",
        )
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data in {"t:react:channels", "t:react:channel"})
    def cb_react_channel(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        channels = reaction_channels(conn)
        text = "<b>Каналы реакций</b>\n"
        if channels:
            text += "\n".join(f"• <code>@{html.escape(ch)}</code>" for ch in channels)
        else:
            text += "Список пуст."
        bot.edit_message_text(text, c.message.chat.id, c.message.message_id, reply_markup=_kb_react_channels())
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "t:react:ch:add")
    def cb_react_channel_add(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        repo_states.set_state(conn, int(c.from_user.id), STATE_OWNER_TOOLS, {"section": "react", "step": "channel_add"}, tz)
        bot.send_message(c.message.chat.id, "Введи канал (t.me/olegleonoff или @olegleonoff):")
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("t:react:ch:del:"))
    def cb_react_channel_del(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        channel = str(c.data.split(":")[4] or "").strip().lstrip("@")
        cur = reaction_channels(conn)
        nxt = [x for x in cur if x.lower() != channel.lower()]
        if not nxt:
            bot.answer_callback_query(c.id, "Нужен хотя бы один канал")
            return
        set_reaction_channels(conn, nxt)
        cb_react_channel(c)

    @bot.callback_query_handler(func=lambda c: c.data in {"t:stats", "t:stats:menu"})
    def cb_stats(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        _show_stats(c.message.chat.id, int(c.from_user.id), c.message.message_id)
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "t:stats:export")
    def cb_stats_export(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        payload = {
            "users": repo_analytics.user_totals(conn),
            "activity": repo_analytics.dau_wau_mau(conn, tz),
            "retention": repo_analytics.retention(conn),
            "operational": repo_analytics.operational_metrics(conn),
            "outreach_summary": repo_owner_outreach.summary(conn, int(c.from_user.id)),
            "outreach_pool": repo_owner_outreach.pool_summary(conn, int(c.from_user.id)),
            "outreach_directions": repo_owner_outreach.direction_stats(conn, int(c.from_user.id)),
            "script_stats": repo_owner_outreach.script_stats(conn, int(c.from_user.id)),
            "outreach_accounts_30d": _outreach_account_stats(int(c.from_user.id), days=30),
            "reaction_summary": repo_owner_reactions.summary(conn, int(c.from_user.id)),
            "admin_interactions_30d": _admin_event_count(days=30),
            "admin_interactions_log": _admin_events(limit=3000),
            "generated_at": _stamp(),
        }
        raw = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        bot.send_document(
            c.message.chat.id,
            BufferedInputFile(raw, filename=f"owner_stats_{_stamp()}.json"),
            caption="Общая статистика",
        )
        bot.answer_callback_query(c.id)

    @bot.callback_query_handler(func=lambda c: c.data == "t:stats:clear_admins")
    def cb_stats_clear_admins(c: CallbackQuery):
        if not _is_owner(c.from_user.id):
            bot.answer_callback_query(c.id, "Нет доступа")
            return
        base_ids = _base_admin_ids()
        rows = conn.execute("SELECT user_id FROM bot_admins WHERE is_active=1 ORDER BY user_id").fetchall()
        removed = 0
        for r in rows:
            uid = int(r["user_id"] or 0)
            if uid <= 0 or uid in base_ids:
                continue
            if repo_admins.deactivate(conn, tz, uid):
                removed += 1
                if uid == _owner_id():
                    continue
                row = conn.execute(
                    "SELECT id, archived FROM students WHERE owner_tg_id=? ORDER BY id DESC LIMIT 1",
                    (uid,),
                ).fetchone()
                if row and int(row["archived"] or 0) == 0:
                    repo_analytics.set_role(conn, tz, uid, repo_analytics.ROLE_STUDENT)
                else:
                    repo_analytics.set_role(conn, tz, uid, repo_analytics.ROLE_REGULAR)
        _show_stats(c.message.chat.id, int(c.from_user.id), c.message.message_id)
        bot.answer_callback_query(c.id, f"Отключено: {removed}")

    @bot.message_handler(
        func=lambda m: _is_owner(m.from_user.id) and repo_states.get_state(conn, int(m.from_user.id))[0] == STATE_OWNER_TOOLS,
        content_types=["text"],
    )
    def owner_state(m: Message):
        st, data = repo_states.get_state(conn, int(m.from_user.id))
        if st != STATE_OWNER_TOOLS:
            return
        data = data or {}
        sec = str(data.get("section") or "")
        step = str(data.get("step") or "")
        txt = (m.text or "").strip()

        if sec == "mod" and step == "title":
            if len(txt) < 2:
                bot.send_message(m.chat.id, "Короткое название.")
                return
            api_id = int(getattr(cfg, "OWNER_TG_API_ID", 0) or 0)
            api_hash = str(getattr(cfg, "OWNER_TG_API_HASH", "") or "").strip()
            if api_id <= 0 or not api_hash:
                repo_states.clear_state(conn, int(m.from_user.id), tz)
                bot.send_message(
                    m.chat.id,
                    "Не настроены OWNER_TG_API_ID / OWNER_TG_API_HASH в .env.",
                    reply_markup=_kb_mod(),
                )
                return
            aid = repo_owner_tg_accounts.create(
                conn,
                tz,
                owner_tg_id=int(m.from_user.id),
                title=txt,
                account_type=ACCOUNT_TYPE_STANDARD,
                api_id=api_id,
                api_hash=api_hash,
                session_file=str(_dir_tg() / f"account_{_stamp()}.session"),
                is_active=(_active_account(int(m.from_user.id)) is None),
            )
            repo_states.set_state(
                conn,
                int(m.from_user.id),
                STATE_OWNER_TOOLS,
                {"section": "mod", "step": "qr_password", "draft": {"account_id": aid}},
                tz,
            )
            has_saved = bool(_tg_cloud_password(int(m.from_user.id), aid))
            bot.send_message(
                m.chat.id,
                (
                    f"Аккаунт добавлен id={aid}.\n"
                    "Введи облачный пароль Telegram для QR-логина.\n"
                    "Если облачного пароля нет — отправь <code>-</code>."
                    + ("\nЕсли использовать сохраненный пароль, отправь <code>.</code>." if has_saved else "")
                ),
            )
            return

        if sec == "mod" and step in {"api_id", "api_hash"}:
            repo_states.clear_state(conn, int(m.from_user.id), tz)
            bot.send_message(
                m.chat.id,
                "Ручной ввод api_id/api_hash отключен. Теперь используются OWNER_TG_API_ID и OWNER_TG_API_HASH из .env.\n"
                "Нажми «➕ Добавить» снова.",
                reply_markup=_kb_mod(),
            )
            return

        if sec == "mod" and step == "qr_password":
            draft = (data.get("draft") or {}).copy()
            aid = int(draft.get("account_id") or 0)
            if aid <= 0:
                repo_states.clear_state(conn, int(m.from_user.id), tz)
                bot.send_message(m.chat.id, "Не найден аккаунт для QR.", reply_markup=_kb_mod())
                return
            owner_id = int(m.from_user.id)
            saved = _tg_cloud_password(owner_id, aid)
            if txt == ".":
                if not saved:
                    bot.send_message(m.chat.id, "Сохраненного пароля нет. Введи пароль или отправь <code>-</code>.")
                    return
                pwd = saved
            elif txt == "-":
                _set_tg_cloud_password(owner_id, aid, "")
                pwd = ""
            else:
                _set_tg_cloud_password(owner_id, aid, txt)
                pwd = txt
            repo_states.clear_state(conn, owner_id, tz)
            _start_qr(m.chat.id, owner_id, account_id=aid, cloud_password=pwd)
            return

        if sec == "out" and step == "query":
            draft = (data.get("draft") or {}).copy()
            if txt.startswith("http://") or txt.startswith("https://"):
                draft["search_link"] = txt
                draft["search_text"] = ""
            else:
                draft["search_text"] = txt
                draft["search_link"] = ""
            repo_states.set_state(conn, int(m.from_user.id), STATE_OWNER_TOOLS, {"section": "out", "step": "direction", "draft": draft}, tz)
            bot.send_message(m.chat.id, "Выберите профиль поиска:", reply_markup=_kb_dirs("t:out:dir", "t:out"))
            return

        if sec == "out" and step == "set_link":
            owner_id = int(m.from_user.id)
            if txt in {"-", "clear", "очистить"}:
                _set_hh_base_link(owner_id, "")
                repo_states.clear_state(conn, owner_id, tz)
                bot.send_message(m.chat.id, "Ссылка-фильтр HH очищена.", reply_markup=_kb_out())
                return
            if not (txt.startswith("http://") or txt.startswith("https://")):
                bot.send_message(m.chat.id, "Нужна ссылка, начинающаяся с http:// или https://")
                return
            if "hh.ru" not in txt.lower():
                bot.send_message(m.chat.id, "Это не похоже на ссылку HH (hh.ru).")
                return
            _set_hh_base_link(owner_id, txt)
            repo_states.clear_state(conn, owner_id, tz)
            bot.send_message(m.chat.id, "Ссылка-фильтр HH сохранена.", reply_markup=_kb_out())
            return

        if sec == "out" and step == "parse_limit":
            draft = (data.get("draft") or {}).copy()
            repo_states.set_state(conn, int(m.from_user.id), STATE_OWNER_TOOLS, {"section": "out", "step": "direction", "draft": draft}, tz)
            bot.send_message(m.chat.id, "Количество для парсинга больше не запрашивается. Выбери профиль поиска:", reply_markup=_kb_dirs("t:out:dir", "t:out"))
            return

        if sec == "out" and step == "send_limit":
            draft = (data.get("draft") or {}).copy()
            draft, _accounts, mx = _apply_runtime_to_draft(int(m.from_user.id), draft)
            try:
                n = int(txt)
            except Exception:
                bot.send_message(m.chat.id, f"Число 0-{mx}.")
                return
            if n < 0 or n > mx:
                bot.send_message(m.chat.id, f"Диапазон 0-{mx}.")
                return
            _send_out_confirm(m.chat.id, int(m.from_user.id), draft, n)
            return

        if sec == "out" and step == "quick_send_limit":
            draft = (data.get("draft") or {}).copy()
            draft, _accounts, mx = _apply_runtime_to_draft(int(m.from_user.id), draft)
            try:
                n = int(txt)
            except Exception:
                bot.send_message(m.chat.id, f"Число 0-{mx}.")
                return
            if n < 0 or n > mx:
                bot.send_message(m.chat.id, f"Диапазон 0-{mx}.")
                return
            _send_out_confirm(m.chat.id, int(m.from_user.id), draft, n)
            return

        if sec == "out_cfg" and step == "delay":
            try:
                n = int(txt)
            except Exception:
                bot.send_message(m.chat.id, "Нужно число 0-3600.")
                return
            if n < 0 or n > 3600:
                bot.send_message(m.chat.id, "Диапазон 0-3600.")
                return
            _set_out_cfg(int(m.from_user.id), {"delay_sec": n})
            repo_states.clear_state(conn, int(m.from_user.id), tz)
            bot.send_message(m.chat.id, f"Интервал сохранен: {n} сек.", reply_markup=_kb_out())
            return

        if sec == "out_cfg" and step == "per_account_max":
            try:
                n = int(txt)
            except Exception:
                bot.send_message(m.chat.id, "Нужно число 1-500.")
                return
            if n < 1 or n > 500:
                bot.send_message(m.chat.id, "Диапазон 1-500.")
                return
            _set_out_cfg(int(m.from_user.id), {"per_account_max": n})
            repo_states.clear_state(conn, int(m.from_user.id), tz)
            bot.send_message(m.chat.id, f"Лимит на аккаунт сохранен: {n}.", reply_markup=_kb_out())
            return

        if sec == "out_ai" and step == "interval":
            try:
                n = int(txt)
            except Exception:
                bot.send_message(m.chat.id, "Нужно число 10-3600.")
                return
            if n < 10 or n > 3600:
                bot.send_message(m.chat.id, "Диапазон 10-3600.")
                return
            n2 = set_ai_auto_interval_sec(conn, n)
            repo_states.clear_state(conn, int(m.from_user.id), tz)
            bot.send_message(m.chat.id, f"Интервал автодиалогов сохранен: {n2} сек.", reply_markup=_kb_out())
            return

        if sec == "out_ai_rules" and step == "add":
            phrase = str(txt or "").strip().lower()
            if len(phrase) < 3:
                bot.send_message(m.chat.id, "Фраза слишком короткая. Отправь минимум 3 символа.")
                return
            rules = script_ai_guardrails(conn)
            items = [str(x or "").strip().lower() for x in (rules.get("forbidden_substrings") or [])]
            if phrase not in items:
                items.append(phrase)
            rules["forbidden_substrings"] = items
            set_ai_guardrails(conn, rules)
            _save_ai_guardrails_file(int(m.from_user.id))
            repo_states.clear_state(conn, int(m.from_user.id), tz)
            bot.send_message(
                m.chat.id,
                "Фраза добавлена в запреты.\n\n" + _ai_rules_preview(),
                reply_markup=_kb_ai_rules(),
            )
            return

        if sec == "script":
            d = str(data.get("direction") or "").strip().lower()
            idx = int(data.get("step_index") or 0)
            if d in DIRECTIONS and set_script_step(conn, d, idx, txt):
                repo_states.clear_state(conn, int(m.from_user.id), tz)
                bot.send_message(m.chat.id, f"Сохранил {_label_dir(d)} шаг {idx+1}.", reply_markup=_kb_out())
            else:
                bot.send_message(m.chat.id, "Не сохранилось.")
            return

        if sec == "react" and step in {"channel", "channel_add"}:
            if set_reaction_channel(conn, txt):
                repo_states.clear_state(conn, int(m.from_user.id), tz)
                channels = reaction_channels(conn)
                bot.send_message(
                    m.chat.id,
                    "Каналы:\n" + "\n".join(f"• <code>@{html.escape(ch)}</code>" for ch in channels),
                    reply_markup=_kb_react(),
                )
            else:
                bot.send_message(m.chat.id, "Некорректный канал.")
            return

        bot.send_message(m.chat.id, "Состояние устарело, открой /t.")

    if not _auto_ai_thread_started["value"]:
        _auto_ai_thread_started["value"] = True

        def _auto_ai_worker() -> None:
            owner_id = _owner_id()
            while True:
                sleep_sec = 10
                try:
                    if script_ai_enabled(conn):
                        if time.time() < float(_manual_replies_priority_until["ts"] or 0.0):
                            time.sleep(2)
                            continue
                        interval = ai_auto_interval_sec(conn)
                        _process_replies_once(owner_id, None, notify=False, source="auto")
                        sleep_sec = max(10, int(interval))
                    else:
                        sleep_sec = 10
                except Exception:
                    sleep_sec = 10
                time.sleep(max(5, sleep_sec))

        threading.Thread(target=_auto_ai_worker, daemon=True, name="owner-ai-auto").start()
