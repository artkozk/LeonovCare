from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Config:
    bot_token: str
    okidoki_api_token: str
    okidoki_api_base: str
    mentor_chat_id: int | None
    mentor_contact_url: str
    test_exception_username: str
    db_path: Path
    log_file: Path


def _env(name: str, default: str = "") -> str:
    raw = os.getenv(name)
    return (raw or default).strip()


def _load_dotenv() -> None:
    dotenv_path = Path(".env")
    if not dotenv_path.exists():
        return
    try:
        lines = dotenv_path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return
    for line in lines:
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        key = key.strip()
        if not key:
            continue
        val = value.strip().strip('"').strip("'")
        if key not in os.environ:
            os.environ[key] = val


def load_config() -> Config:
    _load_dotenv()
    bot_token = _env("BOT_TOKEN")
    okidoki_api_token = _env("OKIDOKI_API_TOKEN")
    if not bot_token:
        raise RuntimeError("BOT_TOKEN is required")
    if not okidoki_api_token:
        raise RuntimeError("OKIDOKI_API_TOKEN is required")

    base = _env("OKIDOKI_API_BASE", "https://api.doki.online").rstrip("/")
    mentor_raw = _env("MENTOR_CHAT_ID")
    mentor_chat_id = int(mentor_raw) if mentor_raw else None
    mentor_contact_url = _env("MENTOR_CONTACT_URL", "https://t.me/mr_winchester1")

    db_path = Path(_env("DB_PATH", "data/testbot.db")).expanduser().resolve()
    log_file = Path(_env("LOG_FILE", "logs/testbot.log")).expanduser().resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    log_file.parent.mkdir(parents=True, exist_ok=True)

    return Config(
        bot_token=bot_token,
        okidoki_api_token=okidoki_api_token,
        okidoki_api_base=base,
        mentor_chat_id=mentor_chat_id,
        mentor_contact_url=mentor_contact_url,
        test_exception_username=_env("TEST_EXCEPTION_USERNAME", "artkozk").lstrip("@").lower(),
        db_path=db_path,
        log_file=log_file,
    )
