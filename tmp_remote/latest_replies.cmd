python3 - <<'PY'
import json,glob,os
base='/opt/mentor-bot/data/owner_tools/parser_runs'
files=sorted(glob.glob(base+'/replies_report_*.json'))
print('count',len(files))
for p in files[-5:]:
    try:
        d=json.load(open(p,'r',encoding='utf-8'))
    except Exception as e:
        print('bad',p,e); continue
    print('---',os.path.basename(p))
    print('status',d.get('status'),'processed',d.get('processed'),'updated',d.get('updated'),'ai_sent',d.get('ai_sent'),'needs_review',d.get('needs_review'),'llm_ok',d.get('llm_ok'),'llm_failed',d.get('llm_failed'),'dup',d.get('duplicate_blocked'))
    accs=d.get('accounts') or []
    for a in accs[:10]:
        print(' acc',a.get('account_id'),a.get('account_type'),a.get('send_ai'),'proc',a.get('processed'),'ai',a.get('ai_sent'),'err',str(a.get('error') or '')[:120])
PY
