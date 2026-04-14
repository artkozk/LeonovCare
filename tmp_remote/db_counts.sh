#!/usr/bin/env bash
set -euo pipefail
python3 - <<'PY'
import sqlite3
conn=sqlite3.connect('/opt/mentor-bot/data/mentor_bot.db')
cur=conn.cursor()
for t in ['owner_tg_accounts','owner_tg_contacts','owner_tg_dialogue_events','checkout_payments','students','payments']:
    c=cur.execute(f'SELECT COUNT(1) FROM {t}').fetchone()[0]
    print(t,c)
PY
