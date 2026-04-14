#!/usr/bin/env bash
set -euo pipefail
python3 - <<'PY'
import sqlite3
conn=sqlite3.connect('/opt/mentor-bot/data/mentor_bot.db')
cur=conn.cursor()
rows=cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
for (name,) in rows:
    if name.startswith('owner_') or name in ('checkout_payments','students','payments'):
        c=cur.execute(f'SELECT COUNT(1) FROM {name}').fetchone()[0]
        print(name,c)
PY
