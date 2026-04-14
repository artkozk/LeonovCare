python3 - <<'PY'
import json,glob,collections,os
arr=sorted(glob.glob('/opt/mentor-bot/data/owner_tools/parser_runs/replies_report_*_acc3.json'))
print('files',len(arr))
p=arr[-1]
d=json.load(open(p,'r',encoding='utf-8'))
print('file',os.path.basename(p))
print('processed',d.get('processed'),'updated',d.get('updated'),'ai_sent',d.get('ai_sent'))
notes=collections.Counter(); status=collections.Counter(); dec=collections.Counter(); llm=collections.Counter()
for x in d.get('details') or []:
    status[str(x.get('status'))]+=1
    notes[str(x.get('note'))]+=1
    dec[str(x.get('decision_source'))]+=1
    llm[str(x.get('llm_status'))]+=1
print('status',dict(status))
print('notes_top',notes.most_common(15))
print('decision',dict(dec))
print('llm',dict(llm))
for x in (d.get('details') or [])[:20]:
    print('---',x.get('telegram'),x.get('status'),'note=',x.get('note'),'dec=',x.get('decision_source'),'llm=',x.get('llm_status'),'dup=',x.get('duplicate_reason'))
PY
