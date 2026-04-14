python3 - <<'PY'
import sqlite3
conn=sqlite3.connect('/opt/mentor-bot/data/mentor_bot.db')
conn.row_factory=sqlite3.Row
keys=['owner_script_ai_enabled','owner_ai_auto_enabled','owner_ai_auto_interval_sec','owner_openai_model','owner_outreach_send_mode','owner_outreach_selected_account_ids','owner_outreach_per_account_max','owner_outreach_delay_sec']
for k in keys:
    r=conn.execute('select value from settings where key=?',(k,)).fetchone()
    print(k, '=>', r['value'] if r else '<none>')
PY
