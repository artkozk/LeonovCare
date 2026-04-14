cd /opt/mentor-bot
PID=$(systemctl show -p MainPID --value mentor-bot.service)
python3 - <<PY
import os, sqlite3, time
from app.db import repo_checkout_payments
from app.services.cardlink_client import CardlinkClient

pid = int(os.environ.get('PID') or 0)
env = {}
if pid > 0:
    raw = open(f'/proc/{pid}/environ','rb').read().split(b'\0')
    for item in raw:
        if b'=' in item:
            k,v = item.split(b'=',1)
            env[k.decode('utf-8','ignore')] = v.decode('utf-8','ignore')

for k in ['CARDLINK_SHOP_ID','CARDLINK_BEARER_TOKEN','CARDLINK_RETURN_URL','CARDLINK_API_BASE_URL','DB_PATH','TZ']:
    print(k, 'set' if env.get(k) else 'missing')

db_path = env.get('DB_PATH') or '/opt/mentor-bot/data/mentor_bot.db'
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
try:
    rid = repo_checkout_payments.create(
        conn=conn,
        tz=env.get('TZ','Europe/Moscow'),
        owner_tg_id=6553771455,
        owner_chat_id=6553771455,
        owner_username='it_vkatrf',
        req_type='SERVICE_PAYMENT',
        service_key='interview_helper',
        service_title='Interview Helper',
        purpose='???????? ??????',
        amount=500,
        currency='RUB',
        provider_payment_id='manual-test-'+str(int(time.time()*1000)),
        provider='cardlink',
        idempotence_key='manual-test-'+str(int(time.time()*1000)),
        status='PENDING',
        confirmation_url=None,
        metadata={'x':1},
        provider_payload={'y':2},
    )
    print('checkout_row_created', rid)
except Exception as e:
    print('checkout_row_error', type(e).__name__, e)

client = CardlinkClient(
    shop_id=env.get('CARDLINK_SHOP_ID',''),
    bearer_token=env.get('CARDLINK_BEARER_TOKEN',''),
    timeout_sec=30,
    api_base_url=env.get('CARDLINK_API_BASE_URL') or None,
)
try:
    p = client.create_payment(
        amount=500,
        description='Test from server',
        idempotence_key='manual-test-'+str(int(time.time()*1000)),
        success_redirect_url=env.get('CARDLINK_RETURN_URL'),
        fail_redirect_url=env.get('CARDLINK_RETURN_URL'),
        metadata={'source':'manual'},
    )
    print('cardlink_ok', p.payment_id, p.confirmation_url)
except Exception as e:
    print('cardlink_error', type(e).__name__, e)
PY
