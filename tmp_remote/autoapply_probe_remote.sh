cd /opt/mentor-bot
. .venv/bin/activate
python -c "from pathlib import Path; from scripts.preprod_audit import _load_env_file; _load_env_file(Path('/opt/mentor-bot/.env')); import os, json, requests; base=os.getenv('AUTOAPPLY_API_BASE_URL','').rstrip('/'); tok=os.getenv('AUTOAPPLY_INTERNAL_TOKEN',''); headers={'Content-Type':'application/json','X-Internal-Token':tok}; payload={'login':'ivanushkin.dev@gmail.com','password':'Azq123azq123','direction':'java','targetApplies':5,'queryText':'Java','active':True};
print('base=',base);
r=requests.post(base+'/api/internal/accounts',headers=headers,json=payload,timeout=45); print('create_status=',r.status_code); print('create_body=',(r.text or '')[:300]);
r2=requests.post(base+'/api/internal/accounts/run',headers=headers,json={'login':'ivanushkin.dev@gmail.com'},timeout=210); print('run_status=',r2.status_code); print('run_body=',(r2.text or '')[:500])"
