python3 - <<'PY'
from pathlib import Path
p=Path('/opt/mentor-bot/.env')
keys=['BOT_TOKEN','ADMIN_IDS','DB_PATH','CARDLINK_SHOP_ID','CARDLINK_BEARER_TOKEN','CARDLINK_RETURN_URL','OWNER_OPENAI_API_KEY','OWNER_AI_PROVIDER','OWNER_AI_BASE_URL','OWNER_AI_PROXY_AUTH','OWNER_AI_HTTP_PROXY','AUTOAPPLY_API_BASE_URL','AUTOAPPLY_INTERNAL_TOKEN','INTERVIEW_API_BASE_URL']
vals={}
if p.exists():
    for line in p.read_text(encoding='utf-8').splitlines():
        line=line.strip()
        if not line or line.startswith('#') or '=' not in line: continue
        k,v=line.split('=',1)
        vals[k.strip()]=v.strip()
for k in keys:
    v=vals.get(k,'')
    print(f"{k}: {'set' if v else 'EMPTY'} len={len(v)}")
PY
