cd /opt/mentor-bot
export PID=$(systemctl show -p MainPID --value mentor-bot.service)
python3 - <<'PY'
import os
from scripts.owner_tg_process_replies import _llm_chat_completion

pid = int(os.environ.get('PID') or 0)
env = {}
raw = open(f'/proc/{pid}/environ','rb').read().split(b'\0')
for item in raw:
    if b'=' in item:
        k,v=item.split(b'=',1)
        env[k.decode('utf-8','ignore')] = v.decode('utf-8','ignore')

provider = env.get('OWNER_AI_PROVIDER','openai')
base_url = env.get('OWNER_AI_BASE_URL','')
proxy_auth = env.get('OWNER_AI_PROXY_AUTH','')
api_key = env.get('OWNER_OPENAI_API_KEY','')
http_proxy = env.get('OWNER_AI_HTTP_PROXY','')
model = env.get('OWNER_OPENAI_MODEL','gpt-5.4-mini')
body={
    'model': model,
    'messages':[
        {'role':'system','content':'??????? ??????.'},
        {'role':'user','content':'?????? ????? ??????: ??'}
    ],
    'temperature':0.1,
}
ok, code, payload, err = _llm_chat_completion(provider, base_url, proxy_auth, api_key, 30, body, http_proxy=http_proxy)
print('provider=',provider)
print('http_proxy_set=',bool(http_proxy))
print('ok=',ok,'code=',code,'err=',err)
print('payload_preview=',(payload or '')[:120].replace('\n',' '))
PY
