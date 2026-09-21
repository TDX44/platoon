"""The webhook against REAL Stripe payloads, not hand-written ones.

Run with: python tests/test_billing_real_payloads.py

tests/test_billing.py builds its own event objects, so it proves the handler
against what we *believe* Stripe sends. These three objects are verbatim API
responses captured from the live test-mode account (acct_1UH36rDMHU4FbNeb) on
2026-09-21, so they prove it against what Stripe actually sends on the current
API version. Two shapes here are load-bearing and neither is obvious:

  * a subscription carries NO top-level current_period_end -- it lives on
    items.data[0]. Reading only the root would store NULL and every account
    would look expired.
  * an invoice carries NO top-level `subscription` -- it is under
    parent.subscription_details. Without that fallback _handle_stripe_event
    takes its "names no subscription, ignored" branch and no failed payment
    ever reaches an account.

If Stripe moves either one again, this file fails and test_billing.py does
not, because test_billing.py would still be asserting against the old shape.
"""
import hashlib
import hmac
import json
import logging
import os
import sys
import time
from datetime import timezone

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, _ROOT)
sys.path.insert(0, _HERE)

import dbharness  # noqa: E402

_SCHEMA = dbharness.setup()

WEBHOOK_SECRET = 'whsec_real_' + 'd' * 16
os.environ['CLERK_PUBLISHABLE_KEY'] = 'pk_test_' + 'a' * 48
os.environ['CLERK_FRONTEND_API_URL'] = 'https://real-payloads.clerk.accounts.dev'
os.environ['STRIPE_MODE'] = 'test'
# Never used: the webhook path makes no Stripe API call. construct_event is
# local HMAC, so a key only has to exist for billing to switch on.
os.environ['STRIPE_TEST_SECRET_KEY'] = 'sk_test_' + 'b' * 24
os.environ['STRIPE_TEST_WEBHOOK_SECRET'] = WEBHOOK_SECRET
os.environ['BILLING_DEFAULT'] = 'on'

import server  # noqa: E402  (must follow the env overrides above)
import billing_rules  # noqa: E402

PAYER = 'cus_VInEjJW3x3e9xI'
FAILER = 'cus_VInFG7aX3J1qxh'
PAYER_SUB = 'sub_1UIBeJDMHU4FbNebX8J75v8A'
FAILER_SUB = 'sub_1UIBecDMHU4FbNebG9YTmWbk'
PERIOD_END = 1792606147

# --- captured verbatim; do not tidy, the shape IS the assertion --------------

SUB_ACTIVE = json.loads("""
{"id":"sub_1UIBeJDMHU4FbNebX8J75v8A","object":"subscription","cancel_at":null,
 "cancel_at_period_end":false,"canceled_at":null,"collection_method":"charge_automatically",
 "created":1790014147,"currency":"usd","customer":"cus_VInEjJW3x3e9xI",
 "items":{"object":"list","data":[{"id":"si_VInEZXm4Zstx6P","object":"subscription_item",
   "created":1790014148,"current_period_end":1792606147,"current_period_start":1790014147,
   "price":{"id":"price_1UH3FcDMHU4FbNebezlB17cs","object":"price","active":true,"currency":"usd",
     "livemode":false,"lookup_key":"platoon_leader_monthly","nickname":"Monthly",
     "product":"prod_VHcUwHylICUi5y","recurring":{"interval":"month","interval_count":1},
     "type":"recurring","unit_amount":299},
   "quantity":1,"subscription":"sub_1UIBeJDMHU4FbNebX8J75v8A"}],"has_more":false,"total_count":1},
 "latest_invoice":"in_1UIBeKDMHU4FbNebp4WqUMhW","livemode":false,"start_date":1790014147,
 "status":"active","trial_end":null,"trial_start":null}
""")

SUB_CANCELLING = json.loads(json.dumps(SUB_ACTIVE))
SUB_CANCELLING.update({'cancel_at': PERIOD_END, 'cancel_at_period_end': True,
                       'canceled_at': 1790014158,
                       'cancellation_details': {'reason': 'cancellation_requested'}})

INVOICE_FAILED = json.loads("""
{"id":"in_1UIBecDMHU4FbNebTay9VZgd","object":"invoice","amount_due":299,"amount_paid":0,
 "amount_remaining":299,"attempt_count":1,"attempted":true,"billing_reason":"subscription_create",
 "collection_method":"charge_automatically","created":1790014166,"currency":"usd",
 "customer":"cus_VInFG7aX3J1qxh","customer_email":"rehearsal-fail@platoonmanager.test",
 "livemode":false,"next_payment_attempt":null,"number":"5Z25CIZX-0001",
 "parent":{"quote_details":null,
   "subscription_details":{"metadata":{},"subscription":"sub_1UIBecDMHU4FbNebG9YTmWbk"},
   "type":"subscription_details"},
 "period_end":1790014166,"period_start":1790014166,"status":"open","subtotal":299,"total":299}
""")

# ---------------------------------------------------------------------------

_EVENT_N = [0]


def post(type_, obj, event_id=None):
    _EVENT_N[0] += 1
    payload = {'id': event_id or f'evt_real_{_EVENT_N[0]}', 'type': type_,
               'livemode': False, 'data': {'object': obj}}
    body = json.dumps(payload).encode()
    ts = int(time.time())
    mac = hmac.new(WEBHOOK_SECRET.encode(), f'{ts}.'.encode() + body, hashlib.sha256).hexdigest()
    dbharness.as_user(None)
    return server.app.test_client().post(
        '/api/billing/webhook', data=body,
        headers={'Stripe-Signature': f't={ts},v1={mac}', 'Content-Type': 'application/json'})


def row(user_id):
    conn = dbharness.owner_conn()
    try:
        r = conn.execute('SELECT * FROM subscriptions WHERE user_id = %s', (user_id,)).fetchone()
        return dict(r) if r else None
    finally:
        conn.close()


def link(user, customer):
    """An account that has reached Stripe: a customer id and nothing else yet."""
    conn = dbharness.owner_conn()
    try:
        conn.execute('INSERT INTO subscriptions (user_id, root_id, stripe_customer_id) '
                     'VALUES (%s, %s, %s) ON CONFLICT (user_id) DO UPDATE '
                     'SET stripe_customer_id = EXCLUDED.stripe_customer_id',
                     (user['id'], user['root_id'], customer))
        conn.commit()
    finally:
        conn.close()


def test_the_captured_objects_really_have_the_awkward_shape():
    assert 'current_period_end' not in SUB_ACTIVE, 'a root current_period_end would void this file'
    assert SUB_ACTIVE['items']['data'][0]['current_period_end'] == PERIOD_END
    assert 'subscription' not in INVOICE_FAILED, 'a root subscription would void this file'
    assert INVOICE_FAILED['parent']['subscription_details']['subscription'] == FAILER_SUB


def test_a_real_subscription_created(payer):
    r = post('customer.subscription.created', SUB_ACTIVE, event_id='evt_created_1')
    assert r.status_code == 200 and r.get_json() == {'ok': True}, (r.status_code, r.get_json())
    s = row(payer['id'])
    assert s['stripe_subscription_id'] == PAYER_SUB, s
    assert s['stripe_status'] == 'active', s
    assert s['stripe_price_lookup_key'] == 'platoon_leader_monthly', s
    assert s['cancel_at_period_end'] is False, s
    # The whole point: this came off items.data[0], not the root.
    assert int(s['current_period_end'].replace(tzinfo=timezone.utc).timestamp()) == PERIOD_END, s


def test_the_same_delivery_twice_is_a_replay(payer):
    r = post('customer.subscription.created', SUB_ACTIVE, event_id='evt_created_1')
    assert r.status_code == 200 and r.get_json() == {'replay': True}, (r.status_code, r.get_json())


def test_a_real_cancel_at_period_end_keeps_access(payer):
    r = post('customer.subscription.updated', SUB_CANCELLING)
    assert r.status_code == 200, r.status_code
    s = row(payer['id'])
    assert s['cancel_at_period_end'] is True, s
    assert s['stripe_status'] == 'active', s
    assert billing_rules.billing_state({**s, 'billing_mode': 'billed'},
                                       billing_rules.utcnow(), True)['state'] != 'LOCKED', s


def test_a_real_failed_invoice_reaches_the_account(failer):
    """Also the regression for the NULL guard: stripe_status is still NULL
    here, because Stripe does not order deliveries and a first-charge failure
    can arrive before customer.subscription.created. `NULL IN (...)` is NULL,
    so a bare `NOT (... IN ...)` silently matched no row and the failure was
    dropped without a trace. billing_apply_stripe COALESCEs it."""
    assert row(failer['id'])['stripe_status'] is None, 'the guard is only interesting while NULL'
    r = post('invoice.payment_failed', INVOICE_FAILED)
    assert r.status_code == 200, r.status_code
    s = row(failer['id'])
    assert s['stripe_status'] == 'past_due', s
    assert s['stripe_subscription_id'] == FAILER_SUB, s


def test_a_real_deletion_closes_the_account(payer):
    gone = {**SUB_ACTIVE, 'status': 'canceled'}
    r = post('customer.subscription.deleted', gone)
    assert r.status_code == 200, r.status_code
    s = row(payer['id'])
    assert s['stripe_status'] == 'canceled', s
    assert 'canceled' not in billing_rules.OPEN_STATUSES


def test_an_unknown_customer_is_ignored_not_a_500():
    level = server.app.logger.level
    server.app.logger.setLevel(logging.CRITICAL)   # the warning below is on purpose
    try:
        r = post('customer.subscription.created', {**SUB_ACTIVE, 'customer': 'cus_NOBODY'})
    finally:
        server.app.logger.setLevel(level)
    assert r.status_code == 200, r.status_code


def main():
    try:
        tree = dbharness.make_tree('Real Payload Co')
        payer = dbharness.make_user(tree['root'], 'owner', 'real-payer')
        failer = dbharness.make_user(tree['root'], 'owner', 'real-failer')
        link(payer, f'test:{PAYER}')
        link(failer, f'test:{FAILER}')

        test_the_captured_objects_really_have_the_awkward_shape()
        test_a_real_subscription_created(payer)
        test_the_same_delivery_twice_is_a_replay(payer)
        test_a_real_cancel_at_period_end_keeps_access(payer)
        test_a_real_failed_invoice_reaches_the_account(failer)
        test_a_real_deletion_closes_the_account(payer)
        test_an_unknown_customer_is_ignored_not_a_500()
        print('ok')
    finally:
        dbharness.teardown(_SCHEMA)


if __name__ == '__main__':
    main()
