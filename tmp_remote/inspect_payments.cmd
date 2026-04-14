python3 - <<'PY'
import sqlite3, json
db='/opt/mentor-bot/data/mentor_bot.db'
conn=sqlite3.connect(db)
conn.row_factory=sqlite3.Row
print('db',db)
rows=conn.execute('SELECT id,req_type,service_key,amount,status,confirmation_url,provider_payload_json,created_at FROM checkout_payments ORDER BY id DESC LIMIT 30').fetchall()
for r in rows:
    has_url=1 if str(r['confirmation_url'] or '').strip() else 0
    print(f"ID={r['id']} req={r['req_type']} svc={r['service_key']} amt={r['amount']} status={r['status']} has_url={has_url} created={r['created_at']}")
    if not has_url:
        payload={}
        try:
            payload=json.loads(r['provider_payload_json'] or '{}')
        except Exception:
            pass
        print(' keys=',list(payload.keys())[:20])
        for k in ('confirmation','confirmation_url','paymentUrl','payment_url','url','data','link','checkoutUrl','bill','pay_url','payUrl'):
            if k in payload:
                print(' ',k,'=',str(payload[k])[:200])
print('--- ACCOUNTS ---')
for r in conn.execute('SELECT id,title,is_active,account_type,session_file,api_id FROM owner_tg_accounts ORDER BY id').fetchall():
    print(dict(r))
print('--- JOBS ---')
for r in conn.execute('SELECT id,status,stage,direction,parsed_total,tg_found_total,send_success,send_failed,started_at,finished_at FROM owner_hh_jobs ORDER BY id DESC LIMIT 10').fetchall():
    print(dict(r))
PY
