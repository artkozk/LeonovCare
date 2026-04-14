from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class Storage:
    def __init__(self, db_path: Path) -> None:
        self.db_path = str(db_path)
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS registration_requests(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_user_id INTEGER NOT NULL,
                tg_username TEXT,
                status TEXT NOT NULL,
                reason TEXT,
                contract_url TEXT,
                payload_json TEXT,
                prefill_json TEXT,
                missing_fields_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        self.conn.commit()

    def save_request(
        self,
        tg_user_id: int,
        tg_username: str | None,
        status: str,
        reason: str,
        contract_url: str,
        payload: dict[str, Any] | None,
        prefill: dict[str, Any] | None,
        missing_fields: list[str] | None,
    ) -> int:
        ts = _utc_now_iso()
        cur = self.conn.execute(
            """
            INSERT INTO registration_requests(
                tg_user_id, tg_username, status, reason, contract_url,
                payload_json, prefill_json, missing_fields_json, created_at, updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(tg_user_id),
                (tg_username or "").strip() or None,
                status.strip(),
                reason.strip(),
                (contract_url or "").strip() or None,
                json.dumps(payload or {}, ensure_ascii=False),
                json.dumps(prefill or {}, ensure_ascii=False),
                json.dumps(missing_fields or [], ensure_ascii=False),
                ts,
                ts,
            ),
        )
        self.conn.commit()
        return int(cur.lastrowid)

