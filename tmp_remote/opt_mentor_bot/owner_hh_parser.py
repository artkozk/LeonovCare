from __future__ import annotations

import csv
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse


_TG_USERNAME_RE = re.compile(r"^@?[A-Za-z0-9_]{5,32}$")
_TG_RESERVED_PREFIXES: tuple[str, ...] = (
    "joinchat",
    "t.me",
    "telegram",
)
_TG_NON_PERSON_SUBSTRINGS: tuple[str, ...] = (
    "official",
    "news",
    "channel",
    "group",
    "chat",
    "vacanc",
    "career",
    "jobs",
    "daily",
)
_QUERY_BY_DIRECTION: dict[str, str] = {
    "java": "(Java OR Spring) AND (developer OR backend OR разработчик) AND (telegram OR телеграм OR tg OR t.me OR @)",
    "frontend": "(React OR Frontend OR JavaScript OR TypeScript) AND (developer OR frontend OR разработчик) AND (telegram OR телеграм OR tg OR t.me OR @)",
    "golang": "(Golang OR Go) AND (developer OR backend OR разработчик) AND (telegram OR телеграм OR tg OR t.me OR @)",
    "python": "(Python OR Django OR FastAPI) AND (developer OR backend OR разработчик) AND (telegram OR телеграм OR tg OR t.me OR @)",
}
_VIEWED_LABELS: tuple[str, str] = (
    "exclude_viewed_by_user_id",
    "exclude_viewed_by_employer_id",
)


def _normalize_tg_username(raw: str | None) -> str | None:
    text = (raw or "").strip()
    if not text:
        return None
    if "joinchat" in text.lower():
        return None
    if text.startswith("https://t.me/"):
        text = text.rsplit("/", 1)[-1]
    if text.startswith("http://t.me/"):
        text = text.rsplit("/", 1)[-1]
    text = text.split("?", 1)[0].split("#", 1)[0].strip()
    if not text:
        return None
    if not text.startswith("@"):
        text = "@" + text
    if not _TG_USERNAME_RE.match(text):
        return None
    uname = text.lstrip("@").lower()
    if uname.endswith("bot"):
        return None
    if any(uname.startswith(p) for p in _TG_RESERVED_PREFIXES):
        return None
    if any(part in uname for part in _TG_NON_PERSON_SUBSTRINGS):
        return None
    return text


def _timestamp() -> str:
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def _normalize_direction(direction: str | None) -> str:
    return str(direction or "").strip().lower()


def _query_for_direction(direction: str | None) -> str:
    return _QUERY_BY_DIRECTION.get(_normalize_direction(direction), "")


def _inject_text_into_link(search_link: str | None, query: str) -> str | None:
    raw = str(search_link or "").strip()
    q = str(query or "").strip()
    if not raw:
        return None
    parsed = urlparse(raw)
    pairs = parse_qsl(parsed.query, keep_blank_values=True)
    if q:
        pairs = [(k, v) for k, v in pairs if k.lower() != "text"]
        pairs.append(("text", q))

    existing_labels = {value for key, value in pairs if key.lower() == "label"}
    for required_label in _VIEWED_LABELS:
        if required_label not in existing_labels:
            pairs.append(("label", required_label))

    return urlunparse(parsed._replace(query=urlencode(pairs)))


def _load_parser(parser_root: Path):
    root = str(parser_root)
    if root not in sys.path:
        sys.path.insert(0, root)
    try:
        from application.services.parser_application_service import SyncParserApplicationService
        from infrastructure.hh_parser.client import HHClient
    except Exception as exc:
        raise RuntimeError(f"Не удалось импортировать parser_hh из {parser_root}: {exc}") from exc
    return SyncParserApplicationService, HHClient


def run_hh_parse(
    parser_root: str,
    output_dir: str,
    search_text: str | None,
    search_link: str | None,
    direction: str | None,
    limit: int,
) -> dict[str, Any]:
    parser_dir = Path(parser_root).expanduser().resolve()
    if not parser_dir.exists():
        raise RuntimeError(f"Папка parser_hh не найдена: {parser_dir}")

    out_dir = Path(output_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = _timestamp()
    resumes_path = out_dir / f"hh_resumes_{stamp}.json"
    contacts_json_path = out_dir / f"hh_contacts_{stamp}.json"
    contacts_csv_path = out_dir / f"hh_contacts_{stamp}.csv"
    contacts_txt_path = out_dir / f"hh_contacts_{stamp}.txt"

    SyncParserApplicationService, HHClient = _load_parser(parser_dir)
    cookies_path = parser_dir / "hh_cookies.json"
    hh_client = HHClient(cookie_file=str(cookies_path))
    parser_service = SyncParserApplicationService(hh_client=hh_client)
    direction_query = _query_for_direction(direction)
    provided_search_text = str(search_text or "").strip()
    effective_search_text = (provided_search_text or direction_query) or None
    effective_search_link = _inject_text_into_link(search_link, effective_search_text or "")

    try:
        rows = parser_service.run_parsing(
            search_text=effective_search_text,
            search_link=effective_search_link,
            limit=max(1, int(limit or 0)),
            items_on_page=100,
            save_to_file=False,
            debug=False,
        )
    finally:
        try:
            hh_client.close()
        except Exception:
            pass

    resumes: list[dict[str, Any]] = []
    seen_tg: set[str] = set()
    contacts: list[dict[str, Any]] = []
    for idx, row in enumerate(rows or [], start=1):
        item = dict(row or {})
        tg = _normalize_tg_username(item.get("telegram"))
        item["telegram"] = tg or ""
        resumes.append(item)
        if tg and tg.lower() not in seen_tg:
            seen_tg.add(tg.lower())
            contacts.append(
                {
                    "index": idx,
                    "telegram": tg,
                    "name": str(item.get("name") or "").strip(),
                    "resume_id": str(item.get("id") or "").strip(),
                    "resume_link": str(item.get("link") or "").strip(),
                }
            )

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
        "effective_search_text": effective_search_text or "",
        "effective_search_link": effective_search_link or "",
    }
