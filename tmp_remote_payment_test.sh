cd /opt/mentor-bot
set -a
. /opt/mentor-bot/.env
set +a
python3 - <<'PY'
from app.db import core, repo_checkout_payments
from app.services.cardlink_client import CardlinkClient
import os

conn = core.connect(os.getenv('DB_PATH'))
core.init_schema(conn)
try:
    rid = repo_checkout_payments.create(
        conn=conn,
        tz=os.getenv('TZ','Europe/Moscow'),
        owner_tg_id=6553771455,
        owner_chat_id=6553771455,
        owner_username='it_vkatrf',
        req_type='SERVICE_PAYMENT',
        service_key='interview_helper',
        service_title='Interview Helper',
        purpose='???????? ??????',
        amount=500,
        currency='RUB',
        provider_payment_id='manual-test-'+str(__import__('time').time()).replace('.',''),
        provider='cardlink',
        idempotence_key='manual-test-'+str(__import__('time').time()).replace('.',''),
        status='PENDING',
        confirmation_url=None,
        metadata={'x':1},
        provider_payload={'y':2},
    )
    print('checkout_row_created', rid)
except Exception as e:
    print('checkout_row_error', type(e).__name__, e)

shop = os.getenv('CARDLINK_SHOP_ID','')
token = os.getenv('CARDLINK_BEARER_TOKEN','')
ret = os.getenv('CARDLINK_RETURN_URL','')
client = CardlinkClient(shop_id=shop, bearer_token=token, timeout_sec=30, api_base_url=os.getenv('CARDLINK_API_BASE_URL'))
try:
    req = client.create_payment(amount=500, description='Test from server', idempotence_key='manual-test-'+str(__import__('time').time()).replace('.',''), success_redirect_url=ret, fail_redirect_url=ret, metadata={'source':'manual'})
    print('cardlink_ok', req.payment_id, req.confirmation_url)
except Exception as e:
    print('cardlink_error', type(e).__name__, e)
PY
