python3 - <<'PY'
import os
pid=None
for p in os.listdir('/proc'):
    if not p.isdigit():
        continue
    try:
        cmd=open(f'/proc/{p}/cmdline','rb').read().decode('utf-8','ignore')
    except Exception:
        continue
    if '/opt/mentor-bot/main.py' in cmd:
        pid=int(p); break
print('pid',pid)
if pid:
    raw=open(f'/proc/{pid}/environ','rb').read().decode('utf-8','ignore')
    env=dict(item.split('=',1) for item in raw.split('\x00') if '=' in item)
    for k in ['DB_PATH','OWNER_TOOLS_DIR','CARDLINK_SHOP_ID','INTERVIEW_HELPER_STUDENT_FREE']:
        print(k, env.get(k,''))
PY
