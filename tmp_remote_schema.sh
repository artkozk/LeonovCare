cd /opt/mentor-bot
python3 - <<'"'"'PY'"'"'
import sqlite3
conn = sqlite3.connect('data/mentor_bot.db')
cur = conn.cursor()
for t in ['owner_tg_accounts','checkout_payments','owner_outreach_jobs','owner_outreach_leads','settings','owner_tg_messages']:
    row = cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (t,)).fetchone()
    print('===', t, '===')
    print(row[0] if row else 'missing')
    print()
PY
