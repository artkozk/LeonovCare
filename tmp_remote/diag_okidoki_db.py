import sqlite3, json
from pathlib import Path

db = '/opt/mentor-bot/data/mentor_bot.db'
con = sqlite3.connect(db)
con.row_factory = sqlite3.Row
cur = con.cursor()
needle = '6984f508854b2b99bcce3716'

cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY 1")
print('TABLES:', [r[0] for r in cur.fetchall() if ('contract' in r[0] or 'enroll' in r[0] or 'payment' in r[0] or 'student' in r[0])])

for t in ('enrollment_contracts','students','payments'):
    try:
        cur.execute(f'PRAGMA table_info({t})')
        cols = [r[1] for r in cur.fetchall()]
        print('COLUMNS', t, cols)
    except Exception as e:
        print('COLUMNS_ERR', t, e)

cur.execute('''
SELECT id, owner_tg_id, sign_url, created_at, updated_at,
       substr(coalesce(okidoki_request_json,''),1,250) as req,
       substr(coalesce(okidoki_response_json,''),1,250) as resp
FROM enrollment_contracts
WHERE coalesce(sign_url,'') LIKE ?
   OR coalesce(okidoki_request_json,'') LIKE ?
   OR coalesce(okidoki_response_json,'') LIKE ?
ORDER BY id DESC LIMIT 10
''', (f'%{needle}%', f'%{needle}%', f'%{needle}%'))
rows = cur.fetchall()
print('MATCHED_ROWS:', len(rows))
for r in rows:
    d = dict(r)
    print('ROW', json.dumps(d, ensure_ascii=False))

con.close()
