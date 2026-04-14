cd /opt/mentor-bot
python3 - <<'PY'
import sqlite3
conn = sqlite3.connect('data/mentor_bot.db')
cur = conn.cursor()
rows = cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
print('TABLES:', ', '.join(r[0] for r in rows))
for t in ['owner_hh_jobs','owner_hh_contacts','owner_tg_ai_stats','owner_tg_ai_memory','owner_tg_accounts','owner_outreach_contacted','owner_outreach_parsed_pool']:
    row = cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (t,)).fetchone()
    print('\n===', t, '===')
    print(row[0] if row else 'missing')
PY
