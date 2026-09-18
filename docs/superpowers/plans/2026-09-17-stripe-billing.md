# Stripe Billing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every leader account pays 2.99/month or 19.99/year through Stripe after a 14-day trial, one 7-day extension and 3 days of grace, with a hard lock behind a pricing screen, a global default and a per-account comp switch on `/admin`.

**Architecture:** One pure module (`billing_rules.py`) decides the state from a `subscriptions` row and a UTC clock. `server.py` creates the row lazily at sign-in, gates every tenant-decorated API route with a 402, exposes four `/api/billing/*` routes (extend, checkout, portal, webhook) and mirrors Stripe's truth into the row through SECURITY DEFINER functions. `index.html` grows a banner, a modal, one `#billingScreen` (pricing when locked, the Billing page otherwise) and an admin column with a comp toggle.

**Tech Stack:** Flask, psycopg3, Postgres 17 (RLS), `stripe==15.6.1`, vanilla JS in `index.html`, assert-based tests run one file at a time, node for lifted JS, Playwright for layout.

**Spec:** `docs/superpowers/specs/2026-09-17-stripe-billing-design.md` — sections 1–9 plus the amendments in section 10, which win on conflict.

## Global Constraints

- Prices by lookup key only: `platoon_leader_monthly` (299 cents) and `platoon_leader_annual` (1999 cents). No amount lives in code.
- Constants, env-overridable: `TRIAL_DAYS=14`, `EXTENSION_DAYS=7`, `GRACE_DAYS=3`.
- Env: `BILLING_DEFAULT` (`on`|`off`, default `on`), `STRIPE_MODE` (`test`|`live`, default `test`), `STRIPE_TEST_SECRET_KEY`, `STRIPE_LIVE_SECRET_KEY`, `STRIPE_TEST_WEBHOOK_SECRET`, `STRIPE_LIVE_WEBHOOK_SECRET`. No key for the active mode ⇒ everyone `COMPED` (A13).
- `stripe_customer_id` is stored as `<mode>:<cus_id>`.
- Exempt path prefixes: `/api/auth/`, `/api/billing/`, `/api/admin/`, and the exact path `/api/me`. `GET /api/units` is NOT exempt.
- Billing clock is UTC (`billing_rules.utcnow()`); every billing column is `timestamptz`. Never `date.today()` (tests/test_timezone.py greps for it).
- Audit actions: `BILLING_EXTEND`, `BILLING_CHECKOUT_STARTED`, `BILLING_ACTIVE`, `BILLING_PAST_DUE`, `BILLING_CANCELLED`, `BILLING_STATUS`, `BILLING_PAID`, `BILLING_COMP`. Unit NULL, like `LOGIN`.
- Every new assertion is proved red by a named mutation before the task is done (the project's standing rule). Each task lists its mutation.
- Commit messages: `<type>: <description>`, no Co-Authored-By trailer. Write files with the Write tool; commit with a plain `git commit -m` (the Bash hook blocks `git commit` inside a heredoc).
- Test recipe (a `pg` container on 127.0.0.1:5493 exists on this machine):

```bash
export TEST_DATABASE_URL='postgresql://platoon_owner:platoon@127.0.0.1:5493/platoon'
export TEST_APP_DATABASE_URL='postgresql://platoon_app:platoon_app_pw@127.0.0.1:5493/platoon'
export DATABASE_URL="$TEST_DATABASE_URL"
/tmp/mobile-venv/bin/python tests/test_billing.py        # one file at a time; /tmp/mobile-venv has Playwright + stripe
```

If `stripe` is missing from a venv: `/tmp/mobile-venv/bin/pip install stripe==15.6.1`.

## File structure

| File | Responsibility |
|---|---|
| `billing_rules.py` (new) | Pure: constants, `utcnow()`, `billing_state(row, now, default_on, platform_admin, enabled)`. No Flask, no DB. Portable to Resyrv. |
| `sql/billing_functions.sql` (new) | Four SECURITY DEFINER functions the webhook and the admin toggle use: `billing_find_by_customer`, `billing_apply_stripe`, `billing_record_event`, `billing_set_mode`. |
| `sql/admin_functions.sql` | Gains `admin_billing_rows()`. |
| `sql/rls.sql` | Adds `subscriptions` to the policy list. |
| `server.py` | Stripe config, `subscriptions` + `stripe_events` tables, row creation, the gate, `/api/me` payload, the four billing routes, webhook handler, admin overview + toggle, delete-user cancel, backup/restore fields. |
| `index.html` | `billingBannerHtml`, `pricingScreenHtml`, `billingPageHtml`, `billingModalDue` (pure), `#billingBanner`, `#billingScreen`, wiring, CSS, admin column + toggle. |
| `requirements.txt`, `.env.example`, `CLAUDE.md` | Pin, env names, docs. |
| `tests/test_billing_state.py` (new) | Table tests for the pure function. No DB. |
| `tests/test_billing.py` (new) | DB-backed: schema, functions, row creation, gate sweep, payload, routes with Stripe stubbed, webhook with real signatures, deletion, backup/restore, admin. |
| `tests/test_billing_js.py` (new) | The pure renderers under node. |
| `tests/test_smoke.py`, `tests/test_platform_admin.py`, `tests/test_mobile_layout.py` | Small extensions. |

---

### Task 1: The pure rules module and the spec amendments

**Files:**
- Create: `billing_rules.py`
- Create: `tests/test_billing_state.py`
- Modify: `docs/superpowers/specs/2026-09-17-stripe-billing-design.md` (append section 10 — the text is in the controller's scratchpad as `spec-amendments.md`; the controller copies it in before dispatching this task)

**Interfaces:**
- Produces: `billing_rules.TRIAL_DAYS`, `EXTENSION_DAYS`, `GRACE_DAYS`, `MODES = ('default', 'comped', 'billed')`, `OPEN_STATUSES`, `LOCKED_STATUSES`, `utcnow() -> datetime`, `billing_state(row, now, default_on, platform_admin=False, enabled=True) -> dict` with keys `state`, `reason`, `days_left`, `trial_ends_at`, `grace_ends_at`, `extension_available`, `subscribed`, `cancel_at_period_end`, `current_period_end`. Dates in the dict are ISO-8601 strings or `None`. `row` is a dict (or `None`) with the `subscriptions` columns; only `billing_mode`, `trial_ends_at`, `extended_at`, `stripe_status`, `cancel_at_period_end`, `current_period_end` are read.

- [ ] **Step 1: Write the failing table test**

Create `tests/test_billing_state.py`:

```python
"""billing_state(): the one function that decides access.

Run with: python tests/test_billing_state.py

Pure and clock-injected, so every branch and every boundary is a row in the
table below. No database, no Flask.
"""
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import billing_rules  # noqa: E402
from billing_rules import billing_state  # noqa: E402

NOW = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
DAY = timedelta(days=1)


def row(**kw):
    base = {'billing_mode': 'default', 'trial_started_at': None, 'trial_ends_at': None,
            'extended_at': None, 'stripe_status': None, 'cancel_at_period_end': False,
            'current_period_end': None, 'stripe_customer_id': None}
    base.update(kw)
    return base


def state(r, now=NOW, default_on=True, platform_admin=False, enabled=True):
    return billing_state(r, now, default_on=default_on, platform_admin=platform_admin, enabled=enabled)


def test_constants_are_the_spec_values():
    assert (billing_rules.TRIAL_DAYS, billing_rules.EXTENSION_DAYS, billing_rules.GRACE_DAYS) == (14, 7, 3)
    assert billing_rules.MODES == ('default', 'comped', 'billed')


def test_comped_beats_everything():
    # An explicit comp overrides even a cancelled Stripe status.
    s = state(row(billing_mode='comped', stripe_status='canceled', trial_ends_at=NOW - 30 * DAY))
    assert s['state'] == 'COMPED' and s['reason'] is None and s['days_left'] is None, s
    assert s['extension_available'] is False, s
    # The platform admin is always comped, whatever the row says.
    assert state(row(billing_mode='billed', trial_ends_at=NOW - 30 * DAY), platform_admin=True)['state'] == 'COMPED'
    # Default mode with the global default off is comped.
    assert state(row(), default_on=False)['state'] == 'COMPED'
    # ...but an explicitly billed account is billed even with the default off.
    assert state(row(billing_mode='billed', trial_ends_at=NOW + 5 * DAY), default_on=False)['state'] == 'TRIAL'
    # No Stripe key configured: nobody is billed, however the row reads.
    assert state(row(billing_mode='billed', trial_ends_at=NOW - 30 * DAY), enabled=False)['state'] == 'COMPED'
    # A row that does not exist yet is comped when the default is off, trial when on.
    assert state(None, default_on=False)['state'] == 'COMPED'
    assert state(None)['state'] == 'TRIAL'


def test_stripe_statuses():
    for st in ('active', 'trialing'):
        s = state(row(stripe_status=st, current_period_end=NOW + 20 * DAY, trial_ends_at=NOW - 30 * DAY))
        assert s['state'] == 'ACTIVE' and s['subscribed'] is True, s
        assert s['current_period_end'] == (NOW + 20 * DAY).isoformat(), s
    s = state(row(stripe_status='active', cancel_at_period_end=True, current_period_end=NOW + 2 * DAY))
    assert s['state'] == 'ACTIVE' and s['cancel_at_period_end'] is True, 'cancel-at-period-end is still ACTIVE until Stripe says otherwise'
    s = state(row(stripe_status='past_due', trial_ends_at=NOW - 30 * DAY))
    assert s['state'] == 'PAST_DUE' and s['subscribed'] is True, s
    for st in ('canceled', 'unpaid', 'incomplete_expired'):
        s = state(row(stripe_status=st, trial_ends_at=NOW + 5 * DAY))
        assert s['state'] == 'LOCKED' and s['reason'] == 'payment_required', (st, s)
        assert s['subscribed'] is False and s['extension_available'] is False, s
    # incomplete (an abandoned checkout) is "not subscribed": the trial dates decide.
    s = state(row(stripe_status='incomplete', trial_ends_at=NOW + 5 * DAY))
    assert s['state'] == 'TRIAL' and s['days_left'] == 5, s


def test_trial_grace_locked_boundaries():
    end = NOW + 5 * DAY
    s = state(row(trial_ends_at=end))
    assert s['state'] == 'TRIAL' and s['days_left'] == 5 and s['reason'] is None, s
    assert s['trial_ends_at'] == end.isoformat() and s['grace_ends_at'] == (end + 3 * DAY).isoformat(), s
    assert s['extension_available'] is True, s
    # Rounded up: half a day left is "1 day", never 0.
    assert state(row(trial_ends_at=NOW + timedelta(hours=12)))['days_left'] == 1
    assert state(row(trial_ends_at=NOW + timedelta(seconds=1)))['days_left'] == 1
    # Exactly at the stamp the trial is over: grace starts.
    s = state(row(trial_ends_at=NOW))
    assert s['state'] == 'GRACE' and s['reason'] == 'trial_expired' and s['days_left'] == 3, s
    assert s['extension_available'] is True, 'the extension is offered in grace'
    s = state(row(trial_ends_at=NOW - 2 * DAY - timedelta(hours=1)))
    assert s['state'] == 'GRACE' and s['days_left'] == 1, s
    # Exactly at the end of grace: locked.
    s = state(row(trial_ends_at=NOW - 3 * DAY))
    assert s['state'] == 'LOCKED' and s['reason'] == 'trial_expired' and s['days_left'] is None, s
    assert s['extension_available'] is True, 'still one extension available when locked after a plain trial'
    # A fresh row (no trial stamp yet) reads as a full trial.
    s = state(row())
    assert s['state'] == 'TRIAL' and s['days_left'] == 14, s


def test_extension_once():
    used = NOW - 1 * DAY
    for ends in (NOW + 5 * DAY, NOW - 1 * DAY, NOW - 10 * DAY):
        s = state(row(trial_ends_at=ends, extended_at=used))
        assert s['extension_available'] is False, (ends, s)
    assert state(row(trial_ends_at=NOW - 10 * DAY, extended_at=used))['state'] == 'LOCKED'


def test_output_shape_is_json_safe():
    s = state(row(trial_ends_at=NOW + DAY))
    assert set(s) == {'state', 'reason', 'days_left', 'trial_ends_at', 'grace_ends_at',
                      'extension_available', 'subscribed', 'cancel_at_period_end', 'current_period_end'}, s
    for v in s.values():
        assert v is None or isinstance(v, (str, int, bool)), s


def main():
    test_constants_are_the_spec_values()
    test_comped_beats_everything()
    test_stripe_statuses()
    test_trial_grace_locked_boundaries()
    test_extension_once()
    test_output_shape_is_json_safe()
    print('ok')


if __name__ == '__main__':
    main()
```

- [ ] **Step 2: Run it to verify it fails**

Run: `/tmp/mobile-venv/bin/python tests/test_billing_state.py`
Expected: `ModuleNotFoundError: No module named 'billing_rules'`

- [ ] **Step 3: Write the module**

Create `billing_rules.py`:

```python
"""Billing state: one pure function of an account's row and the clock.

No Flask, no database. server.py calls billing_state() on every request;
tests/test_billing_state.py tables every branch. The file is written so it
can be copied into Resyrv unchanged, which is why nothing in it knows what a
tenant is.

The clock is UTC on purpose. The duty day (app_now) belongs to the unit, but a
trial that ends "at 2026-10-01T12:00Z" ends at the same instant everywhere.
"""
import math
import os
from datetime import datetime, timedelta, timezone

TRIAL_DAYS = int(os.environ.get('TRIAL_DAYS', '14'))
EXTENSION_DAYS = int(os.environ.get('EXTENSION_DAYS', '7'))
GRACE_DAYS = int(os.environ.get('GRACE_DAYS', '3'))

MODES = ('default', 'comped', 'billed')
# Stripe's own status strings, mirrored by the webhook and never invented here.
OPEN_STATUSES = ('active', 'trialing')
LOCKED_STATUSES = ('canceled', 'unpaid', 'incomplete_expired')

DAY = timedelta(days=1)


def utcnow():
    return datetime.now(timezone.utc)


def _iso(dt):
    return dt.isoformat() if dt else None


def _days_left(until, now):
    """Whole days, rounded up: half a day is "1 day", never "0 days"."""
    return max(1, math.ceil((until - now) / DAY))


def billing_state(row, now, default_on, platform_admin=False, enabled=True):
    """Decide access for one account.

    row: the subscriptions row as a dict, or None before it exists.
    now: an aware UTC datetime.
    default_on: BILLING_DEFAULT == 'on'.
    platform_admin: the operator is always comped.
    enabled: a Stripe key exists for the active mode; without one nobody can
      pay, so nobody is billed.
    """
    row = row or {}
    mode = row.get('billing_mode') or 'default'
    billed = enabled and not platform_admin and mode != 'comped' and (
        mode == 'billed' or (mode == 'default' and default_on))
    trial_ends = row.get('trial_ends_at')
    grace_ends = trial_ends + GRACE_DAYS * DAY if trial_ends else None
    out = {
        'state': None, 'reason': None, 'days_left': None,
        'trial_ends_at': _iso(trial_ends), 'grace_ends_at': _iso(grace_ends),
        'extension_available': False, 'subscribed': False,
        'cancel_at_period_end': bool(row.get('cancel_at_period_end')),
        'current_period_end': _iso(row.get('current_period_end')),
    }
    if not billed:
        out['state'] = 'COMPED'
        return out
    status = row.get('stripe_status')
    if status in OPEN_STATUSES:
        out.update(state='ACTIVE', subscribed=True)
        return out
    if status == 'past_due':
        out.update(state='PAST_DUE', subscribed=True)
        return out
    if status in LOCKED_STATUSES:
        out.update(state='LOCKED', reason='payment_required')
        return out
    # Not subscribed (NULL, or an abandoned `incomplete` checkout): the dates decide.
    can_extend = row.get('extended_at') is None
    if trial_ends is None:
        # The caller initialises the row before asking; this is what a brand
        # new row reads as until then.
        out.update(state='TRIAL', days_left=TRIAL_DAYS, extension_available=can_extend)
        return out
    if now < trial_ends:
        out.update(state='TRIAL', days_left=_days_left(trial_ends, now), extension_available=can_extend)
        return out
    if now < grace_ends:
        out.update(state='GRACE', reason='trial_expired', days_left=_days_left(grace_ends, now),
                   extension_available=can_extend)
        return out
    out.update(state='LOCKED', reason='trial_expired', extension_available=can_extend)
    return out
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `/tmp/mobile-venv/bin/python tests/test_billing_state.py`
Expected: `ok`

- [ ] **Step 5: Prove three assertions can go red (mutations)**

Apply each mutation with a one-line edit, run the test, confirm the named assertion fails, then revert:
1. In `billing_state`, change `if now < grace_ends:` to `if now <= grace_ends:` → `test_trial_grace_locked_boundaries` fails at "Exactly at the end of grace".
2. Change `max(1, math.ceil(...))` to `max(1, math.floor(...))` → fails at "half a day left is 1 day" (floor gives 0 → max → 1... so instead change to `math.ceil(...) - 1`; the 5-day case reads 4).
3. Remove `and not platform_admin` → `test_comped_beats_everything` fails on the platform admin line.

`git diff --stat` must be empty of `billing_rules.py` changes afterwards (`git diff billing_rules.py` prints nothing).

- [ ] **Step 6: Append the spec amendments and commit**

The controller has placed section 10 at the end of the spec file already; confirm with `tail -5 docs/superpowers/specs/2026-09-17-stripe-billing-design.md` that it ends with ruling A15. Then:

```bash
git add billing_rules.py tests/test_billing_state.py docs/superpowers/specs/2026-09-17-stripe-billing-design.md
git commit -m "feat: billing_rules.billing_state, the pure trial/grace/lock rule"
```

---

### Task 2: Schema, RLS, the SECURITY DEFINER functions, Stripe config and the pin

**Files:**
- Create: `sql/billing_functions.sql`
- Modify: `sql/rls.sql:14-16` (table list), `sql/admin_functions.sql` (new function + header + grant loop), `server.py` (module top after `PLATFORM_ADMIN_EMAILS`; `init_db()` tables; the boot loader list at ~`server.py:627`), `requirements.txt`, `.env.example`
- Modify: `tests/test_platform_admin.py:454-467` (`ADMIN_FUNCTIONS` list) and `:604` (grep regex)
- Create: `tests/test_billing.py` (the structural section only; later tasks append)

**Interfaces:**
- Produces: tables `subscriptions` and `stripe_events`; functions `billing_find_by_customer(text) → (user_id int, root_id int)`, `billing_apply_stripe(text, text, text, text, timestamptz, boolean) → int`, `billing_record_event(text) → boolean`, `billing_set_mode(int, text, text) → int`, `admin_billing_rows() → TABLE(...)`; module globals `BILLING_DEFAULT_ON: bool`, `STRIPE_MODE: str`, `STRIPE_SECRET_KEY`, `STRIPE_WEBHOOK_SECRET`, `STRIPE_ENABLED: bool`, `PRICE_LOOKUP_KEYS`, `BILLING_EXEMPT_PREFIXES`, `WEBHOOK_MAX_BYTES`; `import billing_rules` at the top of `server.py`.

- [ ] **Step 1: Write the failing structural tests**

Create `tests/test_billing.py`. It grows in later tasks; this is its head and the first four tests. Note the env overrides go BEFORE `import server`, exactly like `tests/test_platform_admin.py`.

```python
"""Stripe billing: the row, the gate, the routes, the webhook.

Run with: python tests/test_billing.py

The app is exercised as platoon_app (RLS binds). Stripe itself is never
called: the five _stripe_* seams in server.py are replaced per test, and the
webhook is fed payloads signed with the real HMAC scheme.
"""
import hashlib
import hmac
import json
import os
import re
import sys
import time
from datetime import timedelta
from types import SimpleNamespace

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, _ROOT)
sys.path.insert(0, _HERE)

import psycopg  # noqa: E402

import dbharness  # noqa: E402

_SCHEMA = dbharness.setup()

os.environ['CLERK_PUBLISHABLE_KEY'] = 'pk_test_' + 'a' * 48
os.environ['CLERK_FRONTEND_API_URL'] = 'https://billing-test.clerk.accounts.dev'
os.environ['PLATFORM_ADMIN_EMAILS'] = 'jonathon.carr5@gmail.com'
os.environ['STRIPE_MODE'] = 'test'
os.environ['STRIPE_TEST_SECRET_KEY'] = 'sk_test_' + 'b' * 24
os.environ['STRIPE_TEST_WEBHOOK_SECRET'] = 'whsec_test_' + 'c' * 16
os.environ['BILLING_DEFAULT'] = 'on'

import server  # noqa: E402  (must follow the env overrides above)
import billing_rules  # noqa: E402
from billing_rules import utcnow  # noqa: E402

DAY = timedelta(days=1)
WEBHOOK_SECRET = os.environ['STRIPE_TEST_WEBHOOK_SECRET']

BILLING_FUNCTIONS = [
    'billing_find_by_customer(text)',
    'billing_apply_stripe(text, text, text, text, timestamptz, boolean)',
    'billing_record_event(text)',
    'billing_set_mode(int, text, text)',
]


def test_the_tables_exist_and_subscriptions_is_a_tenant_table():
    conn = dbharness.owner_conn()
    try:
        cols = {r['column_name']: r['data_type'] for r in conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND table_name = 'subscriptions'").fetchall()}
        for c in ('user_id', 'root_id', 'billing_mode', 'trial_started_at', 'trial_ends_at', 'extended_at',
                  'stripe_customer_id', 'stripe_subscription_id', 'stripe_status', 'stripe_price_lookup_key',
                  'current_period_end', 'cancel_at_period_end', 'comped_by', 'updated_at'):
            assert c in cols, f'subscriptions.{c} is missing'
        for c in ('trial_started_at', 'trial_ends_at', 'extended_at', 'current_period_end', 'updated_at'):
            assert cols[c] == 'timestamp with time zone', f'{c} must be timestamptz, is {cols[c]}'
        rls = conn.execute("SELECT relrowsecurity FROM pg_class WHERE relname = 'subscriptions' "
                           "AND relnamespace = current_schema()::regnamespace").fetchone()['relrowsecurity']
        assert rls, 'subscriptions has no row-level security'
        policy = conn.execute("SELECT count(*) AS n FROM pg_policies WHERE tablename = 'subscriptions' "
                              "AND policyname = 'tenant' AND schemaname = current_schema()").fetchone()['n']
        assert policy == 1, 'the tenant policy is not on subscriptions'
        assert conn.execute("SELECT to_regclass('stripe_events') IS NOT NULL AS ok").fetchone()['ok']
        # The mode is a CHECK constraint, not a Python-only rule.
        try:
            conn.execute("INSERT INTO subscriptions (user_id, root_id, billing_mode, updated_at) VALUES (999999, 1, 'free', now())")
        except psycopg.errors.CheckViolation:
            conn.rollback()
        except psycopg.errors.ForeignKeyViolation:
            raise AssertionError('billing_mode has no CHECK constraint (the FK fired first)')
        else:
            raise AssertionError('billing_mode accepted a value outside default/comped/billed')
    finally:
        conn.close()


def test_public_cannot_execute_the_billing_functions():
    conn = dbharness.owner_conn()
    try:
        schema = conn.execute('SELECT current_schema() AS s').fetchone()['s']
        for sig in BILLING_FUNCTIONS + ['admin_billing_rows()']:
            qualified = f'{schema}.{sig}'
            assert conn.execute("SELECT has_function_privilege('platoon_app', %s, 'EXECUTE') AS ok",
                                (qualified,)).fetchone()['ok'], f'{sig}: the app cannot call it'
            public = conn.execute(
                'SELECT EXISTS (SELECT 1 FROM pg_proc p, '
                "aclexplode(COALESCE(p.proacl, acldefault('f', p.proowner))) a "
                'WHERE p.oid = %s::regprocedure AND a.grantee = 0) AS public_exec',
                (qualified,)).fetchone()['public_exec']
            assert not public, f'{sig}: still executable by PUBLIC'
            definer = conn.execute('SELECT prosecdef FROM pg_proc WHERE oid = %s::regprocedure',
                                   (qualified,)).fetchone()['prosecdef']
            assert definer, f'{sig}: not SECURITY DEFINER'
            config = conn.execute('SELECT proconfig FROM pg_proc WHERE oid = %s::regprocedure',
                                  (qualified,)).fetchone()['proconfig'] or []
            assert any(c.startswith('search_path=') for c in config), f'{sig}: no pinned search_path'
    finally:
        conn.close()


def test_stripe_config_is_read_from_the_active_mode():
    assert server.STRIPE_MODE == 'test'
    assert server.STRIPE_ENABLED is True
    assert server.STRIPE_SECRET_KEY == os.environ['STRIPE_TEST_SECRET_KEY']
    assert server.STRIPE_WEBHOOK_SECRET == WEBHOOK_SECRET
    assert server.BILLING_DEFAULT_ON is True
    assert server.PRICE_LOOKUP_KEYS == ('platoon_leader_monthly', 'platoon_leader_annual')
    import stripe
    assert stripe.api_key == server.STRIPE_SECRET_KEY
    assert stripe.max_network_retries == 0, 'two sync gunicorn workers cannot afford retries'


def test_the_pin():
    req = open(os.path.join(_ROOT, 'requirements.txt'), encoding='utf-8').read()
    assert re.search(r'^stripe==\d+\.\d+\.\d+$', req, re.M), 'stripe must be pinned in requirements.txt'
    env = open(os.path.join(_ROOT, '.env.example'), encoding='utf-8').read()
    for name in ('STRIPE_MODE', 'STRIPE_TEST_SECRET_KEY', 'STRIPE_LIVE_SECRET_KEY',
                 'STRIPE_TEST_WEBHOOK_SECRET', 'STRIPE_LIVE_WEBHOOK_SECRET', 'BILLING_DEFAULT'):
        assert f'{name}=' in env, f'{name} is not documented in .env.example'


def main():
    try:
        test_the_tables_exist_and_subscriptions_is_a_tenant_table()
        test_public_cannot_execute_the_billing_functions()
        test_stripe_config_is_read_from_the_active_mode()
        test_the_pin()
        print('ok')
    finally:
        dbharness.teardown(_SCHEMA)


if __name__ == '__main__':
    main()
```

- [ ] **Step 2: Run it to verify it fails**

Run: `/tmp/mobile-venv/bin/python tests/test_billing.py`
Expected: `AssertionError: subscriptions.user_id is missing`

- [ ] **Step 3: Pin the SDK and document the env**

`requirements.txt` becomes:

```text
flask
PyJWT[crypto]
psycopg[binary]
psycopg_pool
stripe==15.6.1
```

Append to `.env.example`:

```bash
# Stripe. Dev runs test mode, production live mode; the key and webhook
# signing secret for the OTHER mode may be left empty. With no key for the
# active mode billing is off: every account is complimentary.
STRIPE_MODE=test
STRIPE_TEST_SECRET_KEY=sk_test_your_test_key
STRIPE_TEST_WEBHOOK_SECRET=whsec_your_test_endpoint_secret
STRIPE_LIVE_SECRET_KEY=
STRIPE_LIVE_WEBHOOK_SECRET=
# Whether accounts are billed unless the operator comps them from /admin.
BILLING_DEFAULT=on
```

- [ ] **Step 4: Stripe config at the top of `server.py`**

Add `import billing_rules` and `import stripe` with the other imports. Directly after the `PLATFORM_ADMIN_UNKNOWN` / `_PLATFORM_ADMIN_CACHE` lines (`server.py:109-110`), add:

```python
# ── Billing ──
# One Stripe mode per process. Dev runs `test`, production `live`; the key
# and webhook secret are read for that mode only. No key means billing is
# OFF for this instance — every account is complimentary — because a lock
# nobody can pay their way out of is a trap, and CI has no key.
BILLING_DEFAULT_ON = os.environ.get('BILLING_DEFAULT', 'on').strip().lower() == 'on'
STRIPE_MODE = os.environ.get('STRIPE_MODE', 'test').strip().lower()
if STRIPE_MODE not in ('test', 'live'):
    raise SystemExit(f'STRIPE_MODE must be test or live, not {STRIPE_MODE!r}')
STRIPE_SECRET_KEY = os.environ.get(f'STRIPE_{STRIPE_MODE.upper()}_SECRET_KEY', '').strip()
STRIPE_WEBHOOK_SECRET = os.environ.get(f'STRIPE_{STRIPE_MODE.upper()}_WEBHOOK_SECRET', '').strip()
STRIPE_ENABLED = bool(STRIPE_SECRET_KEY)
STRIPE_TIMEOUT = 5
PRICE_LOOKUP_KEYS = ('platoon_leader_monthly', 'platoon_leader_annual')
# The ways out of a lock. Everything else under /api/ answers 402 to a locked
# account. /api/me is exact; the rest are prefixes.
BILLING_EXEMPT_PREFIXES = ('/api/auth/', '/api/billing/', '/api/admin/')
WEBHOOK_MAX_BYTES = 64 * 1024
if STRIPE_ENABLED:
    stripe.api_key = STRIPE_SECRET_KEY
    # gunicorn runs two SYNC workers: a hung call to Stripe parks one, and a
    # retry parks it twice as long. Short timeout, no retries.
    stripe.default_http_client = stripe.RequestsClient(timeout=STRIPE_TIMEOUT)
    stripe.max_network_retries = 0
else:
    logging.getLogger(__name__).warning(
        'STRIPE_%s_SECRET_KEY is not set: billing is off, every account is complimentary', STRIPE_MODE.upper())
```

(`logging` is already imported by `server.py`; if it is not, add `import logging`.)

- [ ] **Step 5: The tables in `init_db()`**

After the `invites` table's `CREATE TABLE IF NOT EXISTS` (find it with `grep -n "CREATE TABLE IF NOT EXISTS invites" server.py`), add:

```python
        # ── Billing (see docs/superpowers/specs/2026-09-17-stripe-billing-design.md) ──
        # One row per leader account, created lazily at the first attached
        # sign-in. Tenant data: root_id + the same RLS policy as everything
        # else. The Stripe columns mirror Stripe's own words and are written
        # only by billing_apply_stripe() from the webhook; the trial stamps and
        # billing_mode are ours. Billing time is UTC (timestamptz), not the
        # duty day.
        cur.execute('''
            CREATE TABLE IF NOT EXISTS subscriptions (
                user_id                 INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
                root_id                 INTEGER NOT NULL,
                billing_mode            TEXT NOT NULL DEFAULT 'default'
                                        CHECK (billing_mode IN ('default', 'comped', 'billed')),
                trial_started_at        TIMESTAMPTZ,
                trial_ends_at           TIMESTAMPTZ,
                extended_at             TIMESTAMPTZ,
                stripe_customer_id      TEXT UNIQUE,
                stripe_subscription_id  TEXT UNIQUE,
                stripe_status           TEXT,
                stripe_price_lookup_key TEXT,
                current_period_end      TIMESTAMPTZ,
                cancel_at_period_end    BOOLEAN NOT NULL DEFAULT false,
                comped_by               TEXT,
                updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        ''')
        # Every webhook delivery id ever accepted, so a replay is a no-op.
        # Not tenant data; only billing_record_event() writes it.
        cur.execute('''
            CREATE TABLE IF NOT EXISTS stripe_events (
                event_id    TEXT PRIMARY KEY,
                received_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        ''')
```

Change the boot loader list at `server.py:627` from `('rls.sql', 'auth_functions.sql', 'admin_functions.sql')` to `('rls.sql', 'auth_functions.sql', 'admin_functions.sql', 'billing_functions.sql')`.

- [ ] **Step 6: RLS**

In `sql/rls.sql`, change the array to:

```sql
  FOREACH t IN ARRAY ARRAY['units', 'personnel', 'personnel_profile', 'scheduled_events',
                           'duty_roster', 'report_history', 'audit_log', 'settings',
                           'users', 'invites', 'subscriptions']
```

- [ ] **Step 7: `sql/billing_functions.sql`**

```sql
-- Billing's only cross-tenant surface.
--
-- SECURITY DEFINER, owned by platoon_owner, SET search_path FROM CURRENT,
-- EXECUTE revoked from PUBLIC and granted to platoon_app: the pattern of
-- sql/auth_functions.sql and sql/admin_functions.sql. The webhook has no
-- session and no tenant — Stripe is the caller — so it cannot go through RLS
-- and must go through these instead. Nothing else may call them:
-- tests/test_billing.py greps server.py and fails the build if
-- billing_find_by_customer / billing_apply_stripe / billing_record_event are
-- reached from anywhere but the webhook, or billing_set_mode from anywhere
-- but a route behind @platform_admin_required.
--
-- billing_apply_stripe writes ONLY the mirrored Stripe columns. It cannot
-- touch billing_mode or the trial stamps: those are ours, and a payload off
-- the wire must never be able to comp an account or extend a trial.
--
-- Dropped before recreated, like admin_functions.sql: CREATE OR REPLACE
-- cannot change a RETURNS TABLE column list, and a failing statement here
-- would stop the app booting.
DROP FUNCTION IF EXISTS billing_find_by_customer(text);
DROP FUNCTION IF EXISTS billing_apply_stripe(text, text, text, text, timestamptz, boolean);
DROP FUNCTION IF EXISTS billing_record_event(text);
DROP FUNCTION IF EXISTS billing_set_mode(int, text, text);

CREATE OR REPLACE FUNCTION billing_find_by_customer(p_customer text)
RETURNS TABLE (user_id int, root_id int)
LANGUAGE sql SECURITY DEFINER SET search_path FROM CURRENT AS $$
  SELECT s.user_id, s.root_id FROM subscriptions s WHERE s.stripe_customer_id = p_customer;
$$;

-- A subscription event for an id other than the stored one is applied only
-- when it opens access (a new subscription supersedes the old) or when
-- nothing is stored yet. A late `deleted` for a subscription the account has
-- since replaced therefore cannot lock it — the shape of Resyrv's
-- cancellation incident. NULL arguments keep the stored value (the
-- invoice.payment_failed path knows only the customer and the status).
CREATE OR REPLACE FUNCTION billing_apply_stripe(p_customer text, p_subscription text, p_status text,
                                                p_lookup_key text, p_period_end timestamptz, p_cancel boolean)
RETURNS int
LANGUAGE sql SECURITY DEFINER SET search_path FROM CURRENT AS $$
  UPDATE subscriptions s
     SET stripe_subscription_id  = COALESCE(p_subscription, s.stripe_subscription_id),
         stripe_status           = p_status,
         stripe_price_lookup_key = COALESCE(p_lookup_key, s.stripe_price_lookup_key),
         current_period_end      = COALESCE(p_period_end, s.current_period_end),
         cancel_at_period_end    = COALESCE(p_cancel, s.cancel_at_period_end),
         updated_at              = now()
   WHERE s.stripe_customer_id = p_customer
     AND (p_subscription IS NULL
          OR s.stripe_subscription_id IS NULL
          OR s.stripe_subscription_id = p_subscription
          OR p_status IN ('active', 'trialing'))
  RETURNING s.user_id;
$$;

-- true when this delivery is new; NULL (falsy) when it was already recorded.
CREATE OR REPLACE FUNCTION billing_record_event(p_event_id text)
RETURNS boolean
LANGUAGE sql SECURITY DEFINER SET search_path FROM CURRENT AS $$
  INSERT INTO stripe_events (event_id) VALUES (p_event_id)
  ON CONFLICT (event_id) DO NOTHING
  RETURNING true;
$$;

-- The /admin comp toggle. Creates the row if the account has not signed in
-- attached yet. Returns the account's root_id so the caller can audit it in
-- that tenant; no row when the user does not exist or belongs to no unit.
CREATE OR REPLACE FUNCTION billing_set_mode(p_user_id int, p_mode text, p_by text)
RETURNS int
LANGUAGE sql SECURITY DEFINER SET search_path FROM CURRENT AS $$
  INSERT INTO subscriptions (user_id, root_id, billing_mode, comped_by, updated_at)
  SELECT u.id, u.root_id, p_mode, p_by, now() FROM users u
   WHERE u.id = p_user_id AND u.root_id IS NOT NULL
  ON CONFLICT (user_id) DO UPDATE
     SET billing_mode = EXCLUDED.billing_mode, comped_by = EXCLUDED.comped_by, updated_at = now()
  RETURNING subscriptions.root_id;
$$;

DO $$
DECLARE f text;
BEGIN
  FOREACH f IN ARRAY ARRAY[
    'billing_find_by_customer(text)',
    'billing_apply_stripe(text, text, text, text, timestamptz, boolean)',
    'billing_record_event(text)',
    'billing_set_mode(int, text, text)']
  LOOP
    EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC', f);
    EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO platoon_app', f);
  END LOOP;
END $$;
```

- [ ] **Step 8: `admin_billing_rows()` in `sql/admin_functions.sql`**

Extend the header's "WHAT MAY BE EXPOSED HERE" paragraph with one sentence: `Billing adds an account's billing mode, trial stamps and Stripe STATUS word — never a Stripe customer or subscription id.` Add `DROP FUNCTION IF EXISTS admin_billing_rows();` beside the other drops, the function after `admin_recent_users`, and `'admin_billing_rows()'` to the grant loop's array:

```sql
-- Every attached account's billing columns, for the overview's Billing
-- column and its five counts. The state itself is computed in Python by
-- billing_rules.billing_state() so the rule lives in one place.
CREATE OR REPLACE FUNCTION admin_billing_rows()
RETURNS TABLE (user_id int, email text, billing_mode text, trial_ends_at timestamptz,
               extended_at timestamptz, stripe_status text, cancel_at_period_end boolean,
               current_period_end timestamptz)
LANGUAGE sql SECURITY DEFINER SET search_path FROM CURRENT AS $$
  SELECT u.id, u.email, COALESCE(s.billing_mode, 'default'), s.trial_ends_at, s.extended_at,
         s.stripe_status, COALESCE(s.cancel_at_period_end, false), s.current_period_end
    FROM users u LEFT JOIN subscriptions s ON s.user_id = u.id
   WHERE u.unit_id IS NOT NULL;
$$;
```

In `tests/test_platform_admin.py`, add `'admin_billing_rows()',` to `ADMIN_FUNCTIONS` and change the grep regex in `test_every_admin_call_is_behind_the_decorator` to `r'\badmin_(totals|organisations|recent_users|billing_rows)\s*\('`.

- [ ] **Step 9: Run the tests**

Run: `/tmp/mobile-venv/bin/python tests/test_billing.py` → `ok`
Run: `/tmp/mobile-venv/bin/python tests/test_platform_admin.py` → `ok`
Run: `/tmp/mobile-venv/bin/python tests/test_tenancy.py` → `ok` (RLS list changed)
Run: `/tmp/mobile-venv/bin/python tests/test_init_concurrency.py` → `ok` (boot loader changed)

- [ ] **Step 10: Mutations**

1. Remove `'subscriptions'` from `sql/rls.sql` → `test_the_tables_exist_and_subscriptions_is_a_tenant_table` fails at "no row-level security". Revert.
2. Delete the `GRANT EXECUTE` line in `sql/billing_functions.sql` → `test_public_cannot_execute_the_billing_functions` fails at "the app cannot call it". Revert.
3. Change `stripe.max_network_retries = 0` to `= 2` → `test_stripe_config_is_read_from_the_active_mode` fails. Revert.

- [ ] **Step 11: Commit**

```bash
git add requirements.txt .env.example server.py sql/rls.sql sql/billing_functions.sql sql/admin_functions.sql tests/test_billing.py tests/test_platform_admin.py
git commit -m "feat: subscriptions table, billing_* definer functions, Stripe config"
```

---

### Task 3: The row, the verdict on every request, the `/api/me` payload and the 402 gate

**Files:**
- Modify: `server.py` — `_resolved_user()` (~959), `login_required` / `attached_required` / `owner_required` (~974-1009), `_user_json` / `/api/me` (~1269, ~1322), `/api/auth/sync` (~1298), new helpers next to `_resolved_user`
- Modify: `tests/test_smoke.py:183-187` (`PUBLIC_API`)
- Modify: `tests/test_billing.py` (append)

**Interfaces:**
- Consumes: `billing_rules.billing_state`, `BILLING_DEFAULT_ON`, `STRIPE_ENABLED`, `_platform_admin_flag(u)`.
- Produces: `_billing_row(conn, user) -> dict`, `_load_billing(user)` (sets `g.billing_row`, `g.billing`), `_billing_block() -> response | None`, `_billing_payload() -> dict | None`, `_prices_cached() -> list` (a stub in this task: returns `[]`; Task 4 fills it), `_stripe_customer_for_mode(row) -> str | None`. `/api/me` and `/api/auth/sync` carry `billing`. Locked accounts get 402 `{'error': 'subscription_required', 'billing': {...}}` on every non-exempt `/api/` route.

- [ ] **Step 1: Write the failing tests (append to `tests/test_billing.py`, above `main()`)**

```python
def seed():
    """Alpha: an owner and a leader. Bravo: one owner. Plus one stray who
    joined nothing."""
    a = dbharness.make_tree('Alpha Co')
    b = dbharness.make_tree('Bravo Co')
    return {'a': a, 'b': b,
            'a_owner': dbharness.make_user(a['root'], 'owner', 'alpha-owner'),
            'a_leader': dbharness.make_user(a['child'], 'leader', 'alpha-leader'),
            'b_owner': dbharness.make_user(b['root'], 'owner', 'bravo-owner'),
            'stray': dbharness.make_user(None, 'leader', 'stray')}


def sub_row(user_id):
    conn = dbharness.owner_conn()
    try:
        r = conn.execute('SELECT * FROM subscriptions WHERE user_id = %s', (user_id,)).fetchone()
        return dict(r) if r else None
    finally:
        conn.close()


def set_sub(user_id, **cols):
    """Owner-side edit of an account's billing row (a fixture, so it bypasses RLS)."""
    conn = dbharness.owner_conn()
    try:
        sets = ', '.join(f'{k} = %s' for k in cols)
        conn.execute(f'UPDATE subscriptions SET {sets}, updated_at = now() WHERE user_id = %s',
                     (*cols.values(), user_id))
        conn.commit()
    finally:
        conn.close()


def client_as(user):
    dbharness.as_user(user)
    return server.app.test_client()


def test_no_row_before_an_attached_sign_in(fx):
    assert sub_row(fx['a_leader']['id']) is None, 'a row appeared before the account ever signed in'
    c = client_as(fx['stray'])
    me = c.get('/api/me')
    assert me.status_code == 200 and me.get_json()['billing'] is None, me.get_json()
    assert sub_row(fx['stray']['id']) is None, 'an unattached user has no tenant and must get no row'


def test_first_attached_sign_in_starts_the_trial(fx):
    before = utcnow()
    c = client_as(fx['a_leader'])
    me = c.get('/api/me').get_json()
    row = sub_row(fx['a_leader']['id'])
    assert row and row['root_id'] == fx['a']['root'], row
    assert row['trial_started_at'] is not None and before <= row['trial_started_at'] <= utcnow(), row
    assert row['trial_ends_at'] == row['trial_started_at'] + billing_rules.TRIAL_DAYS * DAY, row
    b = me['billing']
    assert b['state'] == 'TRIAL' and b['days_left'] == 14 and b['extension_available'] is True, b
    assert b['subscribed'] is False and b['portal_available'] is False and b['prices'] == [], b
    # A second request does not restart it.
    c.get('/api/me')
    assert sub_row(fx['a_leader']['id'])['trial_started_at'] == row['trial_started_at']


def test_the_trial_does_not_start_while_the_default_is_off(fx):
    server.BILLING_DEFAULT_ON = False
    try:
        c = client_as(fx['b_owner'])
        b = c.get('/api/me').get_json()['billing']
        assert b['state'] == 'COMPED', b
        row = sub_row(fx['b_owner']['id'])
        assert row is not None and row['trial_started_at'] is None, row
    finally:
        server.BILLING_DEFAULT_ON = True
    # Flipped on: the trial starts at the NEXT sign-in, from then, not retroactively.
    b = c.get('/api/me').get_json()['billing']
    assert b['state'] == 'TRIAL' and b['days_left'] == 14, b
    assert sub_row(fx['b_owner']['id'])['trial_started_at'] is not None


def test_sync_carries_billing_too(fx):
    """/api/auth/sync is @clerk_auth_required and declares its own tenant; it
    must create the row and answer with the same payload as /api/me."""
    real = server._verify_clerk_session_token
    server._verify_clerk_session_token = lambda: ({'sub': fx['a_owner']['clerk_user_id']}, None)
    try:
        r = server.app.test_client().post('/api/auth/sync', json={'username': 'alpha-owner', 'email': 'alpha-owner@example.com'})
        assert r.status_code == 200, r.get_json()
        assert r.get_json()['billing']['state'] == 'TRIAL', r.get_json()
        assert sub_row(fx['a_owner']['id']) is not None
    finally:
        server._verify_clerk_session_token = real


def api_rules():
    for rule in server.app.url_map.iter_rules():
        if not rule.rule.startswith('/api/'):
            continue
        methods = [m for m in ('GET', 'POST', 'PUT', 'DELETE') if m in rule.methods]
        path = re.sub(r'<int:[^>]+>', '1', rule.rule)
        path = re.sub(r'<[^>]+>', 'x', path)
        yield rule.rule, path, methods[0]


PUBLIC_API = {'/api/auth/config', '/api/logout', '/api/invites/<token>/preview', '/api/billing/webhook'}


def is_exempt(rule):
    return rule in PUBLIC_API or rule == '/api/me' or rule.startswith(server.BILLING_EXEMPT_PREFIXES)


def test_every_api_route_is_locked_for_a_locked_account(fx):
    leader = fx['a_leader']
    set_sub(leader['id'], trial_ends_at=utcnow() - 10 * DAY)
    c = client_as(leader)
    gated, exempt = [], []
    for rule, path, method in api_rules():
        r = c.open(path, method=method, json={})
        if is_exempt(rule):
            exempt.append(rule)
            assert r.status_code != 402, f'{rule} is on the exempt list but answered 402'
        else:
            gated.append(rule)
            body = r.get_json() or {}
            assert r.status_code == 402 and body.get('error') == 'subscription_required', (rule, r.status_code, body)
            assert body['billing']['state'] == 'LOCKED' and body['billing']['reason'] == 'trial_expired', body
    assert '/api/units' in gated, 'GET /api/units must not be a read carve-out'
    assert '/api/me' in exempt and '/api/billing/extend' in exempt, exempt
    assert len(gated) > 20, gated
    set_sub(leader['id'], trial_ends_at=utcnow() + 10 * DAY)


def test_open_states_are_not_gated(fx):
    leader = fx['a_leader']
    c = client_as(leader)
    for label, cols in (('trial', dict(trial_ends_at=utcnow() + 5 * DAY)),
                        ('grace', dict(trial_ends_at=utcnow() - 1 * DAY)),
                        ('active', dict(stripe_status='active')),
                        ('past_due', dict(stripe_status='past_due')),
                        ('comped', dict(billing_mode='comped', stripe_status='canceled'))):
        set_sub(leader['id'], **cols)
        r = c.get('/api/units')
        assert r.status_code == 200, (label, r.status_code, r.get_json())
        b = c.get('/api/me').get_json()['billing']
        assert b['state'] == {'trial': 'TRIAL', 'grace': 'GRACE', 'active': 'ACTIVE',
                              'past_due': 'PAST_DUE', 'comped': 'COMPED'}[label], (label, b)
    set_sub(leader['id'], billing_mode='default', stripe_status=None, trial_ends_at=utcnow() + 5 * DAY)


def test_rls_hides_another_tenants_row(fx):
    # As Bravo, the app role (RLS binds) can see only Bravo's rows.
    from psycopg.rows import dict_row
    conn = psycopg.connect(os.environ['DATABASE_URL'], row_factory=dict_row)
    try:
        server.set_tenant(conn, fx['b']['root'])
        ids = {r['user_id'] for r in conn.execute('SELECT user_id FROM subscriptions').fetchall()}
        conn.rollback()
    finally:
        conn.close()
    assert fx['a_leader']['id'] not in ids and fx['b_owner']['id'] in ids, ids
    # And with no tenant declared at all, nothing.
    conn = psycopg.connect(os.environ['DATABASE_URL'], row_factory=dict_row)
    try:
        assert conn.execute('SELECT count(*) AS n FROM subscriptions').fetchone()['n'] == 0
    finally:
        conn.close()
```

Also add to `main()`, after the four structural tests and before `print('ok')`:

```python
        fx = seed()
        test_no_row_before_an_attached_sign_in(fx)
        test_first_attached_sign_in_starts_the_trial(fx)
        test_the_trial_does_not_start_while_the_default_is_off(fx)
        test_sync_carries_billing_too(fx)
        test_every_api_route_is_locked_for_a_locked_account(fx)
        test_open_states_are_not_gated(fx)
        test_rls_hides_another_tenants_row(fx)
```

In `tests/test_smoke.py` add `'/api/billing/webhook',` to `PUBLIC_API` (the route arrives in Task 5; adding the entry now is harmless).

- [ ] **Step 2: Run to verify it fails**

Run: `/tmp/mobile-venv/bin/python tests/test_billing.py`
Expected: `KeyError: 'billing'` in `test_no_row_before_an_attached_sign_in`.

- [ ] **Step 3: Implement**

Directly above `_resolved_user()` add:

```python
def _billing_row(conn, user):
    """This attached account's subscriptions row, created on first sight.

    The trial starts here — at the first sign-in that finds the account
    effectively billed — not at account creation, so an account that existed
    before billing, or while BILLING_DEFAULT was off, gets its full trial from
    the day billing first applies to it. Two workers can race the INSERT;
    ON CONFLICT DO NOTHING makes the loser re-read the winner's row.
    """
    row = conn.execute('SELECT * FROM subscriptions WHERE user_id = %s', (user['id'],)).fetchone()
    if row is None:
        conn.execute('INSERT INTO subscriptions (user_id, root_id) VALUES (%s, %s) ON CONFLICT (user_id) DO NOTHING',
                     (user['id'], user['root_id']))
        row = conn.execute('SELECT * FROM subscriptions WHERE user_id = %s', (user['id'],)).fetchone()
    row = dict(row)
    if row['trial_started_at'] is None and _billing_verdict(row, user)['state'] != 'COMPED':
        now = billing_rules.utcnow()
        started = conn.execute(
            'UPDATE subscriptions SET trial_started_at = %s, trial_ends_at = %s, updated_at = now() '
            'WHERE user_id = %s AND trial_started_at IS NULL RETURNING *',
            (now, now + billing_rules.TRIAL_DAYS * billing_rules.DAY, user['id'])).fetchone()
        if started:
            row = dict(started)
    return row


def _billing_verdict(row, user):
    return billing_rules.billing_state(
        row, billing_rules.utcnow(), default_on=BILLING_DEFAULT_ON,
        platform_admin=_platform_admin_flag(user) is True, enabled=STRIPE_ENABLED)


def _load_billing(user):
    """Compute this request's billing verdict onto g. None for the unattached:
    they have no tenant, no row, and the create-unit screen is never gated."""
    g.billing_row = None
    g.billing = None
    if user.get('unit_id') is None:
        return
    g.billing_row = _billing_row(get_db(), user)
    g.billing = _billing_verdict(g.billing_row, user)


def _billing_block():
    """The 402, or None. Called by the three tenant decorators after
    _resolved_user(); platform_admin_required never comes through here
    (it declares no tenant and /api/admin/ is exempt anyway)."""
    b = g.get('billing')
    if not b or b['state'] != 'LOCKED':
        return None
    if request.path == '/api/me' or request.path.startswith(BILLING_EXEMPT_PREFIXES):
        return None
    return jsonify({'error': 'subscription_required', 'billing': _billing_payload()}), 402


def _stripe_customer_for_mode(row):
    """The bare Stripe customer id when the stored one belongs to the active
    mode; None otherwise (a mode switch simply makes a new customer)."""
    stored = (row or {}).get('stripe_customer_id') or ''
    prefix = f'{STRIPE_MODE}:'
    return stored[len(prefix):] if stored.startswith(prefix) else None


def _prices_cached():
    # Task 4 replaces this with the cached Stripe lookup.
    return []


def _billing_payload():
    b = g.get('billing')
    if b is None:
        return None
    row = g.get('billing_row') or {}
    wants_prices = not b['subscribed'] and b['state'] != 'COMPED'
    return {**b,
            'prices': [{k: v for k, v in p.items() if k != 'id'} for p in _prices_cached()] if wants_prices else [],
            'portal_available': _stripe_customer_for_mode(row) is not None}
```

In `_resolved_user()`, after the `g.tz = ...` line, add `_load_billing(user)`.

In the three decorators, after the user is resolved (and, for `attached_required` / `owner_required`, after their existing 403 checks), add:

```python
        blocked = _billing_block()
        if blocked:
            return blocked
```

`/api/me` and `/api/auth/sync` both gain `'billing': _billing_payload()` in their `jsonify({...})`. In `auth_sync`, add `_load_billing(user)` immediately after the existing `g.tz = ...` line (the tenant is declared by then).

- [ ] **Step 4: Run the tests**

Run: `/tmp/mobile-venv/bin/python tests/test_billing.py` → `ok`
Run: `/tmp/mobile-venv/bin/python tests/test_smoke.py` → `ok`
Run: `/tmp/mobile-venv/bin/python tests/test_auth_flow.py` → `ok`
Run: `/tmp/mobile-venv/bin/python tests/test_tenancy.py` → `ok`

- [ ] **Step 5: Mutations**

1. Add `'/api/units'` to `BILLING_EXEMPT_PREFIXES` → `test_every_api_route_is_locked_for_a_locked_account` fails at "GET /api/units must not be a read carve-out". Revert.
2. Remove the `blocked = _billing_block()` lines from `owner_required` only → the same test fails on an owner route (e.g. `/api/backup/restore`). Revert.
3. In `_billing_row`, drop `AND trial_started_at IS NULL` and the `if row['trial_started_at'] is None` guard → `test_first_attached_sign_in_starts_the_trial` fails at "A second request does not restart it". Revert.

- [ ] **Step 6: Commit**

```bash
git add server.py tests/test_billing.py tests/test_smoke.py
git commit -m "feat: subscriptions row at first attached sign-in, billing on /api/me, 402 gate"
```

---

### Task 4: Prices, extend, checkout and portal

**Files:**
- Modify: `server.py` — replace the `_prices_cached()` stub; add the five `_stripe_*` seams and three routes after `/api/logout` (~1317)
- Modify: `tests/test_billing.py` (append)

**Interfaces:**
- Consumes: `g.billing`, `g.billing_row`, `_load_billing`, `_billing_payload`, `_stripe_customer_for_mode`, `log_action`, `_unit_row(conn, unit_id)['slug']`.
- Produces: seams `_stripe_prices() -> list`, `_stripe_customer_create(params: dict, idempotency_key: str) -> obj with .id`, `_stripe_checkout(params: dict) -> obj with .url`, `_stripe_portal(params: dict) -> obj with .url`, `_stripe_cancel(subscription_id: str) -> None`; `_prices_cached() -> list[dict]` with `id`, `lookup_key`, `amount`, `interval`, `currency`; `_ensure_stripe_customer(conn, user, row) -> str`; routes `POST /api/billing/extend`, `POST /api/billing/checkout`, `POST /api/billing/portal`. Tests replace the seams with `server._stripe_prices = ...` etc. and reset `server._PRICES_CACHE = (0.0, [])`.

- [ ] **Step 1: Write the failing tests (append above `main()`)**

```python
def price(key, amount, interval, pid=None):
    return SimpleNamespace(id=pid or f'price_{key}', lookup_key=key, unit_amount=amount, currency='usd',
                           recurring=SimpleNamespace(interval=interval))


BOTH_PRICES = [price('platoon_leader_annual', 1999, 'year'), price('platoon_leader_monthly', 299, 'month')]


class Stripe:
    """The seams, recorded. Every _stripe_* call lands in .calls."""

    def __init__(self, prices=None, fail_prices=False):
        self.prices, self.fail_prices, self.calls = prices if prices is not None else BOTH_PRICES, fail_prices, []

    def install(self):
        server._PRICES_CACHE = (0.0, [])

        def prices():
            self.calls.append(('prices',))
            if self.fail_prices:
                raise OSError('stripe is down')
            return list(self.prices)

        def customer(params, idempotency_key):
            self.calls.append(('customer', params, idempotency_key))
            return SimpleNamespace(id='cus_NEW')

        def checkout(params):
            self.calls.append(('checkout', params))
            return SimpleNamespace(url='https://checkout.stripe.com/c/pay/cs_test_1')

        def portal(params):
            self.calls.append(('portal', params))
            return SimpleNamespace(url='https://billing.stripe.com/p/session/1')

        def cancel(sub_id):
            self.calls.append(('cancel', sub_id))

        server._stripe_prices, server._stripe_customer_create = prices, customer
        server._stripe_checkout, server._stripe_portal, server._stripe_cancel = checkout, portal, cancel
        return self


def test_prices_are_cached_and_ordered(fx):
    st = Stripe().install()
    c = client_as(fx['a_leader'])
    p = c.get('/api/me').get_json()['billing']['prices']
    assert [x['lookup_key'] for x in p] == ['platoon_leader_monthly', 'platoon_leader_annual'], p
    assert p[0] == {'lookup_key': 'platoon_leader_monthly', 'amount': 299, 'interval': 'month', 'currency': 'usd'}, p
    assert 'id' not in p[0], 'the Stripe price id stays server-side'
    c.get('/api/me')
    assert st.calls.count(('prices',)) == 1, 'a second /api/me must hit the cache, not Stripe'


def test_prices_empty_when_stripe_is_down_or_incomplete(fx):
    st = Stripe(fail_prices=True).install()
    c = client_as(fx['a_leader'])
    assert c.get('/api/me').get_json()['billing']['prices'] == []
    c.get('/api/me')
    assert st.calls.count(('prices',)) == 1, 'a failure is cached too (two sync workers)'
    # Only one of the two prices found: never show a lone wrong price.
    Stripe(prices=[price('platoon_leader_monthly', 299, 'month')]).install()
    assert c.get('/api/me').get_json()['billing']['prices'] == []
    # Failure TTL is the short one.
    exp, _ = server._PRICES_CACHE
    assert exp - time.monotonic() <= server.PRICES_FAIL_TTL + 1


def test_extend_once_in_trial_and_once_in_grace(fx):
    Stripe().install()
    leader = fx['a_leader']
    ends = utcnow() + 5 * DAY
    set_sub(leader['id'], trial_ends_at=ends, extended_at=None)
    c = client_as(leader)
    r = c.post('/api/billing/extend')
    assert r.status_code == 200, r.get_json()
    b = r.get_json()['billing']
    assert b['state'] == 'TRIAL' and b['days_left'] == 12 and b['extension_available'] is False, b
    row = sub_row(leader['id'])
    assert row['extended_at'] is not None
    assert abs((row['trial_ends_at'] - (ends + 7 * DAY)).total_seconds()) < 2, 'in trial, the end moves by 7 days'
    r = c.post('/api/billing/extend')
    assert r.status_code == 409, 'the extension is once'
    # In grace: the extension runs from now, and the account is back in TRIAL.
    set_sub(leader['id'], trial_ends_at=utcnow() - 1 * DAY, extended_at=None)
    assert c.get('/api/me').get_json()['billing']['state'] == 'GRACE'
    r = c.post('/api/billing/extend')
    assert r.status_code == 200, r.get_json()
    b = r.get_json()['billing']
    assert b['state'] == 'TRIAL' and b['days_left'] == 7, b
    conn = dbharness.owner_conn()
    try:
        n = conn.execute("SELECT count(*) AS n FROM audit_log WHERE action = 'BILLING_EXTEND' AND root_id = %s",
                         (fx['a']['root'],)).fetchone()['n']
    finally:
        conn.close()
    assert n == 2, 'each extension is audited in the tenant'
    # Locked after an extension already used: 409, not a second extension.
    set_sub(leader['id'], trial_ends_at=utcnow() - 10 * DAY)
    assert c.post('/api/billing/extend').status_code == 409
    set_sub(leader['id'], trial_ends_at=utcnow() + 5 * DAY, extended_at=None)


def test_checkout_creates_the_customer_once_and_opens_a_session(fx):
    st = Stripe().install()
    leader = fx['a_leader']
    set_sub(leader['id'], stripe_customer_id=None)
    c = client_as(leader)
    r = c.post('/api/billing/checkout', json={'lookup_key': 'platoon_leader_annual'},
               base_url='https://platoondev.carr7.com')
    assert r.status_code == 200 and r.get_json() == {'url': 'https://checkout.stripe.com/c/pay/cs_test_1'}, r.get_json()
    kinds = [x[0] for x in st.calls]
    assert kinds.count('customer') == 1 and kinds.count('checkout') == 1, kinds
    _, cparams, key = next(x for x in st.calls if x[0] == 'customer')
    assert key == f'user:{leader["id"]}:test', key
    assert cparams['email'] == leader['email'] and cparams['metadata']['user_id'] == str(leader['id']), cparams
    assert sub_row(leader['id'])['stripe_customer_id'] == 'test:cus_NEW'
    _, params = next(x for x in st.calls if x[0] == 'checkout')
    assert params['mode'] == 'subscription' and params['customer'] == 'cus_NEW', params
    assert params['line_items'] == [{'price': 'price_platoon_leader_annual', 'quantity': 1}], params
    assert params['client_reference_id'] == str(leader['id']), params
    assert params['metadata'] == {'user_id': str(leader['id']), 'root_id': str(fx['a']['root']), 'mode': 'test'}, params
    assert params['success_url'] == 'https://platoondev.carr7.com/2ndplatoon/settings?billing=success', params
    assert params['cancel_url'] == 'https://platoondev.carr7.com/2ndplatoon/settings', params
    # Second checkout reuses the customer.
    c.post('/api/billing/checkout', json={'lookup_key': 'platoon_leader_monthly'})
    assert [x[0] for x in st.calls].count('customer') == 1, 'the customer is created once per mode'
    # A customer from the other mode is ignored and a new one made.
    set_sub(leader['id'], stripe_customer_id='live:cus_OLD')
    c.post('/api/billing/checkout', json={'lookup_key': 'platoon_leader_monthly'})
    assert [x[0] for x in st.calls].count('customer') == 2
    assert sub_row(leader['id'])['stripe_customer_id'] == 'test:cus_NEW'
    # Unknown plan.
    assert c.post('/api/billing/checkout', json={'lookup_key': 'gold'}).status_code == 400
    conn = dbharness.owner_conn()
    try:
        n = conn.execute("SELECT count(*) AS n FROM audit_log WHERE action = 'BILLING_CHECKOUT_STARTED'").fetchone()['n']
    finally:
        conn.close()
    assert n == 3, n


def test_portal_needs_a_customer(fx):
    st = Stripe().install()
    leader = fx['a_leader']
    set_sub(leader['id'], stripe_customer_id=None)
    c = client_as(leader)
    assert c.post('/api/billing/portal').status_code == 409
    set_sub(leader['id'], stripe_customer_id='test:cus_NEW')
    r = c.post('/api/billing/portal', base_url='https://platoondev.carr7.com')
    assert r.status_code == 200 and r.get_json()['url'].startswith('https://billing.stripe.com/'), r.get_json()
    _, params = next(x for x in st.calls if x[0] == 'portal')
    assert params == {'customer': 'cus_NEW', 'return_url': 'https://platoondev.carr7.com/2ndplatoon/settings'}, params
    assert c.get('/api/me').get_json()['billing']['portal_available'] is True


def test_billing_routes_work_while_locked(fx):
    Stripe().install()
    leader = fx['a_leader']
    set_sub(leader['id'], trial_ends_at=utcnow() - 10 * DAY, extended_at=utcnow() - 12 * DAY)
    c = client_as(leader)
    assert c.get('/api/units').status_code == 402
    assert c.post('/api/billing/checkout', json={'lookup_key': 'platoon_leader_monthly'}).status_code == 200
    assert c.post('/api/billing/portal').status_code == 200
    set_sub(leader['id'], trial_ends_at=utcnow() + 5 * DAY, extended_at=None)


def test_unattached_and_signed_out_cannot_use_billing_routes(fx):
    c = client_as(fx['stray'])
    for path in ('/api/billing/extend', '/api/billing/checkout', '/api/billing/portal'):
        assert c.post(path, json={}).status_code == 403, path
    dbharness.as_user(None)
    for path in ('/api/billing/extend', '/api/billing/checkout', '/api/billing/portal'):
        assert server.app.test_client().post(path, json={}).status_code == 401, path
```

Add to `main()` in order after `test_rls_hides_another_tenants_row(fx)`:

```python
        test_prices_are_cached_and_ordered(fx)
        test_prices_empty_when_stripe_is_down_or_incomplete(fx)
        test_extend_once_in_trial_and_once_in_grace(fx)
        test_checkout_creates_the_customer_once_and_opens_a_session(fx)
        test_portal_needs_a_customer(fx)
        test_billing_routes_work_while_locked(fx)
        test_unattached_and_signed_out_cannot_use_billing_routes(fx)
```

- [ ] **Step 2: Run to verify it fails**

Run: `/tmp/mobile-venv/bin/python tests/test_billing.py`
Expected: `AttributeError: module 'server' has no attribute '_PRICES_CACHE'`

- [ ] **Step 3: Implement**

Replace the `_prices_cached()` stub with:

```python
# ── Stripe seams ──
# Five one-line functions, one per Stripe call the app makes. Everything
# above them is testable with these replaced; nothing else in server.py
# touches the SDK except the webhook's signature check.
def _stripe_prices():
    return stripe.Price.list(lookup_keys=list(PRICE_LOOKUP_KEYS), active=True, limit=10).data


def _stripe_customer_create(params, idempotency_key):
    return stripe.Customer.create(**params, idempotency_key=idempotency_key)


def _stripe_checkout(params):
    return stripe.checkout.Session.create(**params)


def _stripe_portal(params):
    return stripe.billing_portal.Session.create(**params)


def _stripe_cancel(subscription_id):
    stripe.Subscription.cancel(subscription_id)


PRICES_TTL = 3600
# A failure is cached too, briefly: one browser polling /api/me during a
# Stripe outage would otherwise park a sync worker per request.
PRICES_FAIL_TTL = 60
_PRICES_CACHE = (0.0, [])   # (expires_at_monotonic, prices)


def _prices_cached():
    """Both prices by lookup key, or [] — never one of them, never a stale
    amount. Per worker; a restart simply re-asks."""
    global _PRICES_CACHE
    expires, prices = _PRICES_CACHE
    if time.monotonic() < expires:
        return prices
    if not STRIPE_ENABLED:
        return []
    try:
        found = {p.lookup_key: p for p in _stripe_prices() if p.lookup_key in PRICE_LOOKUP_KEYS}
        prices = [{'id': found[k].id, 'lookup_key': k, 'amount': found[k].unit_amount,
                   'interval': found[k].recurring.interval, 'currency': found[k].currency}
                  for k in PRICE_LOOKUP_KEYS if k in found]
        if len(prices) != len(PRICE_LOOKUP_KEYS):
            app.logger.warning('Stripe returned %d of %d prices; showing none', len(prices), len(PRICE_LOOKUP_KEYS))
            prices, ttl = [], PRICES_FAIL_TTL
        else:
            ttl = PRICES_TTL
    except Exception as exc:
        app.logger.warning('could not fetch Stripe prices: %s', exc)
        prices, ttl = [], PRICES_FAIL_TTL
    _PRICES_CACHE = (time.monotonic() + ttl, prices)
    return prices
```

After the `/api/logout` route add the three routes:

```python
# ── Billing routes ──
# All three are for the account itself, attached, and none is gated by the
# 402 (/api/billing/ is exempt): they are how a locked account gets out.

def _billing_return_base():
    slug = _unit_row(get_db(), g.current_user['unit_id'])['slug']
    return f"{request.host_url.rstrip('/')}/{slug}/settings"


def _ensure_stripe_customer(conn, user, row):
    """The Stripe customer for this account in the active mode, made on first
    use. The idempotency key means a double-click cannot make two."""
    existing = _stripe_customer_for_mode(row)
    if existing:
        return existing
    customer = _stripe_customer_create(
        {'email': user.get('email') or None, 'name': user.get('full_name') or user['username'],
         'metadata': {'user_id': str(user['id']), 'root_id': str(user['root_id']), 'mode': STRIPE_MODE}},
        idempotency_key=f'user:{user["id"]}:{STRIPE_MODE}')
    conn.execute('UPDATE subscriptions SET stripe_customer_id = %s, updated_at = now() WHERE user_id = %s',
                 (f'{STRIPE_MODE}:{customer.id}', user['id']))
    g.billing_row['stripe_customer_id'] = f'{STRIPE_MODE}:{customer.id}'
    return customer.id


@app.route('/api/billing/extend', methods=['POST'])
@attached_required
def billing_extend():
    b = g.billing
    if not b or not b['extension_available'] or b['state'] not in ('TRIAL', 'GRACE', 'LOCKED'):
        return jsonify({'error': 'The trial extension is not available.'}), 409
    if b['state'] == 'LOCKED' and b['reason'] != 'trial_expired':
        return jsonify({'error': 'The trial extension is not available.'}), 409
    conn = get_db()
    now = billing_rules.utcnow()
    # In trial the end moves by 7 days; in grace (or just locked) it runs from now.
    ends = max(g.billing_row['trial_ends_at'] or now, now) + billing_rules.EXTENSION_DAYS * billing_rules.DAY
    cur = conn.execute('UPDATE subscriptions SET extended_at = %s, trial_ends_at = %s, updated_at = now() '
                       'WHERE user_id = %s AND extended_at IS NULL', (now, ends, g.current_user['id']))
    if cur.rowcount == 0:
        return jsonify({'error': 'The trial extension has already been used.'}), 409
    log_action('BILLING_EXTEND', f'trial extended {billing_rules.EXTENSION_DAYS} days to {ends.isoformat()}')
    _load_billing(g.current_user)
    return jsonify({'billing': _billing_payload()})


@app.route('/api/billing/checkout', methods=['POST'])
@attached_required
def billing_checkout():
    if not STRIPE_ENABLED:
        return jsonify({'error': 'Billing is not configured on this instance.'}), 503
    key = (request.get_json(silent=True) or {}).get('lookup_key')
    price = next((p for p in _prices_cached() if p['lookup_key'] == key), None)
    if not price:
        return jsonify({'error': 'Unknown plan.'}), 400
    conn = get_db()
    user = g.current_user
    customer_id = _ensure_stripe_customer(conn, user, g.billing_row)
    base = _billing_return_base()
    session = _stripe_checkout({
        'mode': 'subscription', 'customer': customer_id,
        'line_items': [{'price': price['id'], 'quantity': 1}],
        'client_reference_id': str(user['id']),
        'metadata': {'user_id': str(user['id']), 'root_id': str(user['root_id']), 'mode': STRIPE_MODE},
        'subscription_data': {'metadata': {'user_id': str(user['id'])}},
        'success_url': f'{base}?billing=success', 'cancel_url': base,
    })
    log_action('BILLING_CHECKOUT_STARTED', f'{key} by {user["username"]}')
    return jsonify({'url': session.url})


@app.route('/api/billing/portal', methods=['POST'])
@attached_required
def billing_portal():
    if not STRIPE_ENABLED:
        return jsonify({'error': 'Billing is not configured on this instance.'}), 503
    customer_id = _stripe_customer_for_mode(g.billing_row)
    if not customer_id:
        return jsonify({'error': 'No billing account yet. Choose a plan first.'}), 409
    session = _stripe_portal({'customer': customer_id, 'return_url': _billing_return_base()})
    return jsonify({'url': session.url})
```

`import time` is already in `server.py` (the admin cache uses it); confirm with `grep -n "^import time" server.py`.

- [ ] **Step 4: Run the tests**

Run: `/tmp/mobile-venv/bin/python tests/test_billing.py` → `ok`
Run: `/tmp/mobile-venv/bin/python tests/test_smoke.py` → `ok`

- [ ] **Step 5: Mutations**

1. In `_prices_cached`, set `ttl = PRICES_TTL` on the failure branch → `test_prices_empty_when_stripe_is_down_or_incomplete` fails at "Failure TTL is the short one". Revert.
2. In `billing_extend`, remove `AND extended_at IS NULL` → `test_extend_once_in_trial_and_once_in_grace` fails at "the extension is once". Revert.
3. In `_ensure_stripe_customer`, return `existing` only when `existing` is truthy **or** the stored id is non-empty (i.e. drop the mode check by returning `stored.split(':')[-1]`) → `test_checkout_creates_the_customer_once_and_opens_a_session` fails at "A customer from the other mode is ignored". Revert.

- [ ] **Step 6: Commit**

```bash
git add server.py tests/test_billing.py
git commit -m "feat: billing extend, Stripe Checkout and Billing Portal routes, cached prices"
```

---

### Task 5: The webhook

**Files:**
- Modify: `server.py` — after `billing_portal`
- Modify: `tests/test_billing.py` (append)

**Interfaces:**
- Consumes: `billing_find_by_customer`, `billing_apply_stripe`, `billing_record_event`, `set_tenant`, `log_action`, `STRIPE_WEBHOOK_SECRET`, `STRIPE_MODE`, `WEBHOOK_MAX_BYTES`.
- Produces: `POST /api/billing/webhook`; `_handle_stripe_event(conn, event)`; `_apply_stripe(conn, event, customer, subscription, status, lookup_key, period_end, cancel_at_period_end)`.

- [ ] **Step 1: Write the failing tests (append above `main()`)**

```python
def signed(payload, secret=WEBHOOK_SECRET, ts=None):
    """A body plus the Stripe-Signature header the SDK will accept."""
    body = json.dumps(payload).encode()
    ts = ts or int(time.time())
    mac = hmac.new(secret.encode(), f'{ts}.'.encode() + body, hashlib.sha256).hexdigest()
    return body, {'Stripe-Signature': f't={ts},v1={mac}', 'Content-Type': 'application/json'}


_EVENT_N = [0]


def event(type_, obj, livemode=False, event_id=None):
    _EVENT_N[0] += 1
    return {'id': event_id or f'evt_{_EVENT_N[0]}', 'type': type_, 'livemode': livemode,
            'data': {'object': obj}}


def subscription_obj(customer, sub_id='sub_1', status='active', lookup_key='platoon_leader_monthly',
                     period_end=1_800_000_000, cancel=False):
    return {'id': sub_id, 'object': 'subscription', 'customer': customer, 'status': status,
            'cancel_at_period_end': cancel,
            'items': {'data': [{'price': {'id': 'price_x', 'lookup_key': lookup_key},
                                'current_period_end': period_end}]}}


def post_webhook(payload, **kw):
    dbharness.as_user(None)
    body, headers = signed(payload, **kw)
    return server.app.test_client().post('/api/billing/webhook', data=body, headers=headers)


def audit_count(action, root_id):
    conn = dbharness.owner_conn()
    try:
        return conn.execute('SELECT count(*) AS n FROM audit_log WHERE action = %s AND root_id = %s',
                            (action, root_id)).fetchone()['n']
    finally:
        conn.close()


def test_webhook_rejects_a_bad_signature_and_a_huge_body(fx):
    body, headers = signed(event('customer.subscription.created', subscription_obj('cus_A')))
    r = server.app.test_client().post('/api/billing/webhook', data=body,
                                      headers={**headers, 'Stripe-Signature': 't=1,v1=deadbeef'})
    assert r.status_code == 400, r.get_json()
    r = server.app.test_client().post('/api/billing/webhook', data=body, headers={'Content-Type': 'application/json'})
    assert r.status_code == 400, 'no signature header must be 400, not 500'
    body2, headers2 = signed(event('x', {}), secret='whsec_wrong')
    assert server.app.test_client().post('/api/billing/webhook', data=body2, headers=headers2).status_code == 400
    # Stale timestamp (outside the SDK's 300 s tolerance).
    body3, headers3 = signed(event('x', {}), ts=int(time.time()) - 3600)
    assert server.app.test_client().post('/api/billing/webhook', data=body3, headers=headers3).status_code == 400
    # Too big is refused before the body is read.
    big = b'{' + b' ' * (server.WEBHOOK_MAX_BYTES + 1) + b'}'
    r = server.app.test_client().post('/api/billing/webhook', data=big, headers=headers)
    assert r.status_code == 413, r.status_code
    conn = dbharness.owner_conn()
    try:
        assert conn.execute('SELECT count(*) AS n FROM stripe_events').fetchone()['n'] == 0, 'a refused event was recorded'
    finally:
        conn.close()


def test_subscription_events_land_on_the_right_row_and_only_touch_stripe_columns(fx):
    leader, bravo = fx['a_leader'], fx['b_owner']
    set_sub(leader['id'], stripe_customer_id='test:cus_A', trial_ends_at=utcnow() - 10 * DAY, billing_mode='default')
    set_sub(bravo['id'], stripe_customer_id='test:cus_B', trial_ends_at=utcnow() + 5 * DAY)
    before = sub_row(leader['id'])
    r = post_webhook(event('customer.subscription.created', subscription_obj('cus_A', 'sub_A1', 'active')))
    assert r.status_code == 200, r.get_json()
    row = sub_row(leader['id'])
    assert row['stripe_subscription_id'] == 'sub_A1' and row['stripe_status'] == 'active', row
    assert row['stripe_price_lookup_key'] == 'platoon_leader_monthly', row
    assert row['current_period_end'].timestamp() == 1_800_000_000 and row['cancel_at_period_end'] is False, row
    for col in ('billing_mode', 'trial_started_at', 'trial_ends_at', 'extended_at', 'stripe_customer_id'):
        assert row[col] == before[col], f'{col} changed on a Stripe event'
    assert sub_row(bravo['id'])['stripe_status'] is None, "Bravo's row was touched by Alpha's event"
    assert audit_count('BILLING_ACTIVE', fx['a']['root']) == 1
    assert audit_count('BILLING_ACTIVE', fx['b']['root']) == 0
    # The locked account is now open.
    assert client_as(leader).get('/api/units').status_code == 200
    # Cancel at period end: still active, flag mirrored.
    post_webhook(event('customer.subscription.updated', subscription_obj('cus_A', 'sub_A1', 'active', cancel=True)))
    row = sub_row(leader['id'])
    assert row['stripe_status'] == 'active' and row['cancel_at_period_end'] is True, row
    assert client_as(leader).get('/api/me').get_json()['billing']['state'] == 'ACTIVE'
    # payment_failed → past_due, open with a warning; other columns kept.
    post_webhook(event('invoice.payment_failed', {'id': 'in_1', 'object': 'invoice', 'customer': 'cus_A'}))
    row = sub_row(leader['id'])
    assert row['stripe_status'] == 'past_due' and row['stripe_subscription_id'] == 'sub_A1', row
    assert audit_count('BILLING_PAST_DUE', fx['a']['root']) == 1
    assert client_as(leader).get('/api/units').status_code == 200
    # invoice.paid is audit only.
    post_webhook(event('invoice.paid', {'id': 'in_2', 'object': 'invoice', 'customer': 'cus_A'}))
    assert audit_count('BILLING_PAID', fx['a']['root']) == 1
    assert sub_row(leader['id'])['stripe_status'] == 'past_due'
    # deleted → canceled → locked (trial long over).
    post_webhook(event('customer.subscription.deleted', subscription_obj('cus_A', 'sub_A1', 'canceled')))
    assert sub_row(leader['id'])['stripe_status'] == 'canceled'
    assert audit_count('BILLING_CANCELLED', fx['a']['root']) == 1
    r = client_as(leader).get('/api/units')
    assert r.status_code == 402 and r.get_json()['billing']['reason'] == 'payment_required', r.get_json()


def test_a_late_delete_for_a_superseded_subscription_cannot_lock(fx):
    leader = fx['a_leader']
    post_webhook(event('customer.subscription.created', subscription_obj('cus_A', 'sub_A2', 'active')))
    assert sub_row(leader['id'])['stripe_subscription_id'] == 'sub_A2'
    post_webhook(event('customer.subscription.deleted', subscription_obj('cus_A', 'sub_A1', 'canceled')))
    row = sub_row(leader['id'])
    assert row['stripe_status'] == 'active' and row['stripe_subscription_id'] == 'sub_A2', row


def test_replay_other_mode_unknown_customer_and_unknown_type(fx):
    leader = fx['a_leader']
    ev = event('customer.subscription.updated', subscription_obj('cus_A', 'sub_A2', 'past_due'), event_id='evt_replay')
    assert post_webhook(ev).status_code == 200
    assert sub_row(leader['id'])['stripe_status'] == 'past_due'
    set_sub(leader['id'], stripe_status='active')
    r = post_webhook(ev)
    assert r.status_code == 200 and r.get_json().get('replay') is True, r.get_json()
    assert sub_row(leader['id'])['stripe_status'] == 'active', 'a replayed event was applied again'
    # An event from the other mode is acknowledged and ignored.
    r = post_webhook(event('customer.subscription.updated', subscription_obj('cus_A', 'sub_A2', 'canceled'), livemode=True))
    assert r.status_code == 200 and sub_row(leader['id'])['stripe_status'] == 'active'
    # Unknown customer: 200, nothing changes anywhere.
    assert post_webhook(event('customer.subscription.updated', subscription_obj('cus_NOBODY', 'sub_Z', 'canceled'))).status_code == 200
    # checkout.session.completed and anything else: acknowledged, ignored.
    assert post_webhook(event('checkout.session.completed', {'id': 'cs_1', 'customer': 'cus_A'})).status_code == 200
    assert post_webhook(event('charge.refunded', {'id': 'ch_1'})).status_code == 200
    assert sub_row(leader['id'])['stripe_status'] == 'active'


def test_a_failing_handler_is_500_and_the_event_is_not_recorded(fx):
    real = server._handle_stripe_event

    def boom(conn, ev):
        raise RuntimeError('handler died')
    server._handle_stripe_event = boom
    try:
        r = post_webhook(event('customer.subscription.updated', subscription_obj('cus_A'), event_id='evt_boom'))
        assert r.status_code == 500, r.status_code
    finally:
        server._handle_stripe_event = real
    conn = dbharness.owner_conn()
    try:
        assert conn.execute("SELECT count(*) AS n FROM stripe_events WHERE event_id = 'evt_boom'").fetchone()['n'] == 0, \
            'an event whose handler failed was recorded, so the Stripe retry will be treated as a replay'
    finally:
        conn.close()
    # The retry succeeds.
    assert post_webhook(event('customer.subscription.updated', subscription_obj('cus_A'), event_id='evt_boom')).status_code == 200


def test_webhook_functions_are_only_called_from_the_webhook():
    src = open(os.path.join(_ROOT, 'server.py'), encoding='utf-8').read()
    code = re.sub(r'#[^\n]*|"""[\s\S]*?"""', '', src)
    blocks = re.split(r'\n(?=@app\.route|\ndef )', code)
    allowed = {'billing_webhook', '_handle_stripe_event', '_apply_stripe', '_audit_for_customer'}
    for block in blocks:
        if re.search(r'\bbilling_(find_by_customer|apply_stripe|record_event)\s*\(', block):
            m = re.search(r'def (\w+)\(', block)
            assert m and m.group(1) in allowed, f'a webhook-only billing_ function is called outside the webhook:\n{block[:300]}'
        if re.search(r'\bbilling_set_mode\s*\(', block):
            assert '@platform_admin_required' in block, f'billing_set_mode called outside the admin gate:\n{block[:300]}'
    assert any(re.search(r'\bbilling_set_mode\s*\(', b) for b in blocks), 'nothing calls billing_set_mode — renamed?'
```

Add to `main()` after the Task 4 calls:

```python
        test_webhook_rejects_a_bad_signature_and_a_huge_body(fx)
        test_subscription_events_land_on_the_right_row_and_only_touch_stripe_columns(fx)
        test_a_late_delete_for_a_superseded_subscription_cannot_lock(fx)
        test_replay_other_mode_unknown_customer_and_unknown_type(fx)
        test_a_failing_handler_is_500_and_the_event_is_not_recorded(fx)
        test_webhook_functions_are_only_called_from_the_webhook()
```

(`test_webhook_functions_are_only_called_from_the_webhook` will fail until Task 7 adds the `billing_set_mode` caller; that is expected — leave it in `main()` commented with `# Task 7` and uncomment it there.)

- [ ] **Step 2: Run to verify it fails**

Run: `/tmp/mobile-venv/bin/python tests/test_billing.py`
Expected: `AssertionError` in `test_webhook_rejects_a_bad_signature_and_a_huge_body` with status 404.

- [ ] **Step 3: Implement**

After `billing_portal`:

```python
# ── The webhook ──
# Stripe is the caller: no session, no tenant, the signature is the auth.
# Deliberately undecorated (tests/test_smoke.py lists it in PUBLIC_API). Every
# database write goes through the billing_* SECURITY DEFINER functions; the
# only thing done as the tenant is the audit row, after set_tenant() on the
# root the customer was found in.

SUBSCRIPTION_EVENTS = ('customer.subscription.created', 'customer.subscription.updated',
                       'customer.subscription.deleted')


@app.route('/api/billing/webhook', methods=['POST'])
def billing_webhook():
    if not (STRIPE_ENABLED and STRIPE_WEBHOOK_SECRET):
        return jsonify({'error': 'Billing is not configured on this instance.'}), 503
    if (request.content_length or 0) > WEBHOOK_MAX_BYTES:
        return jsonify({'error': 'Payload too large.'}), 413
    payload = request.get_data(cache=False)
    try:
        event = stripe.Webhook.construct_event(payload, request.headers.get('Stripe-Signature', ''), STRIPE_WEBHOOK_SECRET)
    except Exception:
        app.logger.warning('stripe webhook: bad signature from %s', request.remote_addr)
        return jsonify({'error': 'Bad signature.'}), 400
    if bool(event.get('livemode')) != (STRIPE_MODE == 'live'):
        return jsonify({'ignored': 'other mode'})
    conn = get_db()
    fresh = conn.execute('SELECT billing_record_event(%s) AS fresh', (event['id'],)).fetchone()['fresh']
    if not fresh:
        return jsonify({'replay': True})
    # A handler that raises becomes a 500 through errorhandler(Exception), and
    # _close_db() rolls the transaction back — including the event record
    # above — so Stripe's retry is handled, not mistaken for a replay.
    _handle_stripe_event(conn, event)
    return jsonify({'ok': True})


def _handle_stripe_event(conn, event):
    obj = event['data']['object']
    kind = event['type']
    if kind in SUBSCRIPTION_EVENTS:
        items = ((obj.get('items') or {}).get('data') or [])
        first = items[0] if items else {}
        price = first.get('price') or {}
        # API versions before 2025-03 put current_period_end on the
        # subscription; since then it is on each item.
        period_end = obj.get('current_period_end') or first.get('current_period_end')
        _apply_stripe(conn, event, customer=obj.get('customer'), subscription=obj.get('id'),
                      status=obj.get('status'), lookup_key=price.get('lookup_key'),
                      period_end=period_end, cancel_at_period_end=bool(obj.get('cancel_at_period_end')))
    elif kind == 'invoice.payment_failed':
        _apply_stripe(conn, event, customer=obj.get('customer'), subscription=None, status='past_due',
                      lookup_key=None, period_end=None, cancel_at_period_end=None)
    elif kind == 'invoice.paid':
        _audit_for_customer(conn, obj.get('customer'), 'BILLING_PAID', f'invoice {obj.get("id")} paid')
    # checkout.session.completed and everything else: acknowledged, ignored —
    # the subscription events carry the truth.


def _audit_for_customer(conn, customer, action, details):
    target = conn.execute('SELECT * FROM billing_find_by_customer(%s)', (f'{STRIPE_MODE}:{customer}',)).fetchone()
    if not target:
        app.logger.warning('stripe event for unknown customer %s (%s)', customer, action)
        return None
    set_tenant(conn, target['root_id'])
    log_action(action, details)
    return target


def _apply_stripe(conn, event, customer, subscription, status, lookup_key, period_end, cancel_at_period_end):
    cid = f'{STRIPE_MODE}:{customer}'
    target = conn.execute('SELECT * FROM billing_find_by_customer(%s)', (cid,)).fetchone()
    if not target:
        app.logger.warning('stripe event %s for unknown customer %s', event['id'], customer)
        return
    ends = datetime.fromtimestamp(period_end, timezone.utc) if period_end else None
    conn.execute('SELECT billing_apply_stripe(%s, %s, %s, %s, %s, %s)',
                 (cid, subscription, status, lookup_key, ends, cancel_at_period_end))
    if status in billing_rules.OPEN_STATUSES:
        action = 'BILLING_ACTIVE'
    elif status == 'past_due':
        action = 'BILLING_PAST_DUE'
    elif status in billing_rules.LOCKED_STATUSES:
        action = 'BILLING_CANCELLED'
    else:
        action = 'BILLING_STATUS'
    set_tenant(conn, target['root_id'])
    log_action(action, f'stripe {event["type"]}: {status}' + (f', cancel at period end' if cancel_at_period_end else ''))
```

`server.py` imports `datetime, date` and `timedelta` from `datetime` but not `timezone`: change line 10 to `from datetime import datetime, date, timezone`.

- [ ] **Step 4: Run the tests**

Run: `/tmp/mobile-venv/bin/python tests/test_billing.py` → `ok` (with the Task-7 line still commented)
Run: `/tmp/mobile-venv/bin/python tests/test_smoke.py` → `ok`

- [ ] **Step 5: Mutations**

1. Move `billing_record_event` to after `_handle_stripe_event` (record only after success, but outside the transaction's failure path is impossible; instead: change `if not fresh: return jsonify({'replay': True})` to `pass`) → `test_replay_other_mode_unknown_customer_and_unknown_type` fails at "a replayed event was applied again". Revert.
2. In `sql/billing_functions.sql`, delete the `AND (p_subscription IS NULL OR ...)` guard → `test_a_late_delete_for_a_superseded_subscription_cannot_lock` fails. Revert.
3. In `billing_webhook`, replace the `construct_event` call with `event = json.loads(payload)` → `test_webhook_rejects_a_bad_signature_and_a_huge_body` fails at the first 400. Revert.

- [ ] **Step 6: Commit**

```bash
git add server.py tests/test_billing.py
git commit -m "feat: Stripe webhook mirrors subscription truth through billing_* functions"
```

---

### Task 6: Deletion cancels at Stripe; backup carries the trial

**Files:**
- Modify: `server.py` — `delete_user` (~1430), `export_backup` users SELECT (~2705), `import_backup` users leg (~2866)
- Modify: `tests/test_billing.py` (append)

**Interfaces:**
- Consumes: `_stripe_cancel`, `_stripe_customer_for_mode`.
- Produces: `users` rows in a backup carry optional `billing_mode`, `trial_started_at`, `trial_ends_at`, `extended_at` (ISO strings or null); restore upserts `subscriptions` from them.

- [ ] **Step 1: Write the failing tests (append above `main()`)**

```python
def test_deleting_a_user_cancels_their_subscription_best_effort(fx):
    st = Stripe().install()
    victim = dbharness.make_user(fx['a']['child'], 'leader', 'alpha-doomed')
    client_as(victim).get('/api/me')   # creates the row
    set_sub(victim['id'], stripe_customer_id='test:cus_D', stripe_subscription_id='sub_D', stripe_status='active')
    owner = client_as(fx['a_owner'])
    r = owner.delete(f'/api/users/{victim["id"]}')
    assert r.status_code == 200, r.get_json()
    assert ('cancel', 'sub_D') in st.calls, st.calls
    assert sub_row(victim['id']) is None, 'the subscriptions row did not cascade'
    # Stripe failing does not stop the delete.
    victim2 = dbharness.make_user(fx['a']['child'], 'leader', 'alpha-doomed-2')
    client_as(victim2).get('/api/me')
    set_sub(victim2['id'], stripe_customer_id='test:cus_E', stripe_subscription_id='sub_E', stripe_status='active')

    def cancel_boom(sub_id):
        raise OSError('stripe down')
    server._stripe_cancel = cancel_boom
    assert client_as(fx['a_owner']).delete(f'/api/users/{victim2["id"]}').status_code == 200
    assert sub_row(victim2['id']) is None
    # No subscription: nothing is called.
    st = Stripe().install()
    victim3 = dbharness.make_user(fx['a']['child'], 'leader', 'alpha-doomed-3')
    assert client_as(fx['a_owner']).delete(f'/api/users/{victim3["id"]}').status_code == 200
    assert not any(x[0] == 'cancel' for x in st.calls)


def test_backup_carries_the_trial_and_never_the_stripe_ids(fx):
    leader = fx['a_leader']
    set_sub(leader['id'], billing_mode='billed', extended_at=utcnow() - DAY, stripe_customer_id='test:cus_A',
            stripe_subscription_id='sub_A2', stripe_status='active')
    row = sub_row(leader['id'])
    dump = client_as(fx['a_owner']).get('/api/backup').get_json()
    u = next(x for x in dump['users'] if x['username'] == 'alpha-leader')
    assert u['billing_mode'] == 'billed' and u['trial_ends_at'] == row['trial_ends_at'].isoformat(), u
    assert u['trial_started_at'] == row['trial_started_at'].isoformat() and u['extended_at'] == row['extended_at'].isoformat(), u
    assert 'stripe' not in json.dumps(dump).lower(), 'a Stripe id or status reached the backup'
    # A leader's export has no users list at all (unchanged rule).
    assert 'users' not in client_as(leader).get('/api/backup').get_json()
    # Restore into the same tree: the trial stamps come back, the Stripe columns are untouched.
    set_sub(leader['id'], billing_mode='default', extended_at=None)
    r = client_as(fx['a_owner']).post('/api/backup/restore', json=dump)
    assert r.status_code == 200, r.get_json()
    after = sub_row(leader['id'])
    assert after['billing_mode'] == 'billed' and after['extended_at'] == row['extended_at'], after
    assert after['stripe_subscription_id'] == 'sub_A2' and after['stripe_status'] == 'active', 'restore must not touch Stripe columns'
    # A file without the keys restores as before (no row is invented).
    conn = dbharness.owner_conn()
    try:
        conn.execute('DELETE FROM subscriptions WHERE user_id = %s', (leader['id'],))
        conn.commit()
    finally:
        conn.close()
    for x in dump['users']:
        for k in ('billing_mode', 'trial_started_at', 'trial_ends_at', 'extended_at'):
            x.pop(k, None)
    assert client_as(fx['a_owner']).post('/api/backup/restore', json=dump).status_code == 200
    assert sub_row(leader['id']) is None
    client_as(leader).get('/api/me')   # fresh trial for the rest of the file
    set_sub(leader['id'], billing_mode='default', stripe_customer_id='test:cus_A', stripe_subscription_id='sub_A2',
            stripe_status=None, trial_ends_at=utcnow() + 5 * DAY, extended_at=None)
```

Add to `main()` after the Task 5 calls:

```python
        test_deleting_a_user_cancels_their_subscription_best_effort(fx)
        test_backup_carries_the_trial_and_never_the_stripe_ids(fx)
```

- [ ] **Step 2: Run to verify it fails**

Run: `/tmp/mobile-venv/bin/python tests/test_billing.py`
Expected: `AssertionError: [('prices',)]` (no cancel call) in the deletion test.

- [ ] **Step 3: Implement**

`delete_user` becomes:

```python
@app.route('/api/users/<int:user_id>', methods=['DELETE'])
@owner_required
def delete_user(user_id):
    if user_id == g.current_user['id']:
        return jsonify({'error': 'Cannot delete your own account'}), 400
    conn = get_db()
    # Nobody keeps paying for a deleted account: cancel at Stripe first, best
    # effort. The row is this tenant's (RLS), so a cross-tenant id finds nothing.
    sub = conn.execute('SELECT stripe_subscription_id, stripe_customer_id FROM subscriptions WHERE user_id = %s',
                       (user_id,)).fetchone()
    if sub and sub['stripe_subscription_id'] and _stripe_customer_for_mode(sub):
        try:
            _stripe_cancel(sub['stripe_subscription_id'])
            log_action('BILLING_CANCELLED', f'user {user_id} deleted; Stripe subscription cancelled')
        except Exception as exc:
            app.logger.error('could not cancel Stripe subscription %s for deleted user %s: %s',
                             sub['stripe_subscription_id'], user_id, exc)
    # RLS hides another root's users, so a cross-tenant id deletes nothing and
    # used to answer 200 — which told the caller the row had been theirs.
    cur = conn.execute("DELETE FROM users WHERE id = %s AND clerk_user_id != ''", (user_id,))
    if cur.rowcount == 0:
        return jsonify({'error': 'Not found'}), 404
    return jsonify({'success': True})
```

In `export_backup`, replace the `users` SELECT with:

```python
    if is_owner(g.current_user):
        # Billing rides along as four optional keys: the mode and the trial
        # stamps, never a Stripe id or status — a restored copy must not be
        # able to claim someone else's subscription.
        users = conn.execute(
            'SELECT u.username, u.email, u.full_name, u.clerk_user_id, u.unit_id, u.role, '
            's.billing_mode, s.trial_started_at, s.trial_ends_at, s.extended_at '
            'FROM users u LEFT JOIN subscriptions s ON s.user_id = u.id '
            "WHERE u.clerk_user_id != '' AND u.unit_id = ANY(%s) ORDER BY u.id", (ids,)).fetchall()
        payload['users'] = with_unit(users)
        for row in payload['users']:
            for k in ('trial_started_at', 'trial_ends_at', 'extended_at'):
                if row.get(k) is not None:
                    row[k] = row[k].isoformat()
```

In `import_backup`'s users loop, change the INSERT to end with `RETURNING id` and, inside the same `try:` after it, upsert the subscription:

```python
            new_id = conn.execute(
                'INSERT INTO users (username, password_hash, clerk_user_id, email, full_name, unit_id, role, root_id) '
                'VALUES (%s, %s, %s, %s, %s, %s, %s, %s) '
                'ON CONFLICT (username) DO UPDATE SET unit_id = EXCLUDED.unit_id, role = EXCLUDED.role '
                'RETURNING id',
                (u['username'], PLACEHOLDER_PASSWORD_HASH, u['clerk_user_id'], u.get('email', ''),
                 u.get('full_name', ''), uid, u.get('role', 'leader'), root_id)).fetchone()['id']
            if u.get('billing_mode') in billing_rules.MODES or u.get('trial_ends_at'):
                conn.execute(
                    'INSERT INTO subscriptions (user_id, root_id, billing_mode, trial_started_at, trial_ends_at, extended_at) '
                    'VALUES (%s, %s, %s, %s, %s, %s) '
                    'ON CONFLICT (user_id) DO UPDATE SET billing_mode = EXCLUDED.billing_mode, '
                    'trial_started_at = EXCLUDED.trial_started_at, trial_ends_at = EXCLUDED.trial_ends_at, '
                    'extended_at = EXCLUDED.extended_at, updated_at = now()',
                    (new_id, root_id, u.get('billing_mode') if u.get('billing_mode') in billing_rules.MODES else 'default',
                     u.get('trial_started_at') or None, u.get('trial_ends_at') or None, u.get('extended_at') or None))
```

(psycopg accepts ISO-8601 strings for `timestamptz` parameters; a hostile non-date string raises `psycopg.errors.InvalidDatetimeFormat`, which is a `psycopg.Error`, so the existing `except psycopg.Error:` savepoint rollback turns it into one skipped user.)

- [ ] **Step 4: Run the tests**

Run: `/tmp/mobile-venv/bin/python tests/test_billing.py` → `ok`
Run: `/tmp/mobile-venv/bin/python tests/test_restore.py` → `ok`
Run: `/tmp/mobile-venv/bin/python tests/test_user_admin.py` → `ok`

- [ ] **Step 5: Mutations**

1. Add `s.stripe_customer_id` to the export SELECT → `test_backup_carries_the_trial_and_never_the_stripe_ids` fails at "a Stripe id ... reached the backup". Revert.
2. Add `stripe_status = NULL` to the restore's `DO UPDATE SET` → fails at "restore must not touch Stripe columns". Revert.
3. Remove the `try/except` around `_stripe_cancel` → the deletion test fails on "Stripe failing does not stop the delete" (500). Revert.

- [ ] **Step 6: Commit**

```bash
git add server.py tests/test_billing.py
git commit -m "feat: deleting a user cancels at Stripe; backups carry the trial, never Stripe ids"
```

---

### Task 7: Admin overview counts and the comp toggle

**Files:**
- Modify: `server.py` — `admin_overview` (~1331) and a new route after it
- Modify: `tests/test_billing.py` (append; uncomment the Task 5 grep test in `main()`)

**Interfaces:**
- Consumes: `admin_billing_rows()`, `billing_set_mode(int, text, text)`, `billing_rules.billing_state`, `PLATFORM_ADMIN_EMAILS`.
- Produces: `GET /api/admin/overview` totals gain `billing_trial`, `billing_grace`, `billing_locked`, `billing_active`, `billing_comped`; each `recent_users` row gains `billing_state` (`'TRIAL'|'GRACE'|'LOCKED'|'ACTIVE'|'PAST_DUE'|'COMPED'|None`) and `billing_mode`; `PUT /api/admin/users/<int:user_id>/billing_mode {mode}` → `{user_id, billing_mode}`.

- [ ] **Step 1: Write the failing tests (append above `main()`)**

```python
ADMIN_EMAIL = 'jonathon.carr5@gmail.com'
_real_verify = server._verify_clerk_session_token


def admin_client(sub='clerk_boss', email=ADMIN_EMAIL):
    server._clerk_verified_email = lambda cid: email if cid == sub else ''
    server._PLATFORM_ADMIN_CACHE.clear()
    server._verify_clerk_session_token = lambda: ({'sub': sub}, None)
    return server.app.test_client()


def test_admin_overview_counts_and_labels_billing(fx):
    leader, a_owner, b_owner = fx['a_leader'], fx['a_owner'], fx['b_owner']
    set_sub(leader['id'], billing_mode='default', stripe_status=None, trial_ends_at=utcnow() + 5 * DAY)
    set_sub(a_owner['id'], billing_mode='comped')
    set_sub(b_owner['id'], billing_mode='default', stripe_status='active')
    try:
        body = admin_client().get('/api/admin/overview').get_json()
    finally:
        server._verify_clerk_session_token = _real_verify
    t = body['totals']
    assert (t['billing_trial'], t['billing_comped'], t['billing_active']) == (1, 1, 1), t
    assert t['billing_grace'] == 0 and t['billing_locked'] == 0, t
    by_email = {u['email']: u for u in body['recent_users']}
    assert by_email['alpha-leader@example.com']['billing_state'] == 'TRIAL', by_email['alpha-leader@example.com']
    assert by_email['alpha-leader@example.com']['billing_mode'] == 'default'
    assert by_email['alpha-owner@example.com']['billing_state'] == 'COMPED'
    assert by_email['bravo-owner@example.com']['billing_state'] == 'ACTIVE'
    assert by_email['stray@example.com']['billing_state'] is None, 'an unattached user has no billing'
    assert 'stripe' not in json.dumps(body).lower(), 'a Stripe id reached the admin payload'


def test_admin_comp_toggle(fx):
    leader = fx['a_leader']
    try:
        c = admin_client()
        r = c.put(f'/api/admin/users/{leader["id"]}/billing_mode', json={'mode': 'comped'})
        assert r.status_code == 200 and r.get_json() == {'user_id': leader['id'], 'billing_mode': 'comped'}, r.get_json()
        row = sub_row(leader['id'])
        assert row['billing_mode'] == 'comped' and row['comped_by'] == 'clerk_boss', row
        assert audit_count('BILLING_COMP', fx['a']['root']) == 1
        assert c.put(f'/api/admin/users/{leader["id"]}/billing_mode', json={'mode': 'free'}).status_code == 400
        assert c.put(f'/api/admin/users/{fx["stray"]["id"]}/billing_mode', json={'mode': 'comped'}).status_code == 404
        assert c.put('/api/admin/users/999999/billing_mode', json={'mode': 'comped'}).status_code == 404
        # Back to default.
        assert c.put(f'/api/admin/users/{leader["id"]}/billing_mode', json={'mode': 'default'}).status_code == 200
        assert sub_row(leader['id'])['billing_mode'] == 'default'
        # A non-admin is 404, like every /api/admin/ route.
        other = admin_client(sub='clerk_nobody', email='nobody@example.com')
        assert other.put(f'/api/admin/users/{leader["id"]}/billing_mode', json={'mode': 'comped'}).status_code == 404
        assert sub_row(leader['id'])['billing_mode'] == 'default'
    finally:
        server._verify_clerk_session_token = _real_verify
    # Comped overrides a cancelled Stripe status end to end.
    set_sub(leader['id'], billing_mode='comped', stripe_status='canceled')
    assert client_as(leader).get('/api/units').status_code == 200
    set_sub(leader['id'], billing_mode='default', stripe_status=None)
```

Add to `main()` after the Task 6 calls, and uncomment `test_webhook_functions_are_only_called_from_the_webhook()`:

```python
        test_admin_overview_counts_and_labels_billing(fx)
        test_admin_comp_toggle(fx)
        test_webhook_functions_are_only_called_from_the_webhook()
```

- [ ] **Step 2: Run to verify it fails**

Run: `/tmp/mobile-venv/bin/python tests/test_billing.py`
Expected: `KeyError: 'billing_trial'`

- [ ] **Step 3: Implement**

In `admin_overview`, after `recent = ...`, add:

```python
    # Billing per account, computed here with the same pure rule the gate
    # uses. For this display an account whose stored email is the operator's
    # counts as comped; the real verdict is still Clerk's, on the gate.
    now = billing_rules.utcnow()
    states = {}
    counts = {'billing_trial': 0, 'billing_grace': 0, 'billing_locked': 0, 'billing_active': 0, 'billing_comped': 0}
    bucket = {'TRIAL': 'billing_trial', 'GRACE': 'billing_grace', 'LOCKED': 'billing_locked',
              'ACTIVE': 'billing_active', 'PAST_DUE': 'billing_active', 'COMPED': 'billing_comped'}
    for b in conn.execute('SELECT * FROM admin_billing_rows()').fetchall():
        admin = (b['email'] or '').strip().lower() in PLATFORM_ADMIN_EMAILS
        s = billing_rules.billing_state(dict(b), now, default_on=BILLING_DEFAULT_ON, platform_admin=admin,
                                        enabled=STRIPE_ENABLED)
        states[b['user_id']] = (s['state'], b['billing_mode'])
        counts[bucket[s['state']]] += 1
```

and change the `jsonify` to:

```python
    return jsonify({
        'totals': {**dict(totals), **counts},
        'organisations': [dict(r) for r in orgs],
        'recent_users': [{**dict(r), 'billing_state': states.get(r['user_id'], (None, None))[0],
                          'billing_mode': states.get(r['user_id'], (None, None))[1]} for r in recent],
        'generated_at': now_stamp,
    })
```

(rename the existing local `now = app_stamp()` to `now_stamp` and use it in the two `admin_*(%s)` calls, since `now` is now the UTC datetime).

Add the route after `admin_overview`:

```python
@app.route('/api/admin/users/<int:user_id>/billing_mode', methods=['PUT'])
@platform_admin_required
def admin_set_billing_mode(user_id):
    mode = (request.get_json(silent=True) or {}).get('mode')
    if mode not in billing_rules.MODES:
        return jsonify({'error': 'mode must be default, comped or billed'}), 400
    conn = get_db()
    sub = g.auth_claims.get('sub')
    row = conn.execute('SELECT billing_set_mode(%s, %s, %s) AS root_id', (user_id, mode, sub)).fetchone()
    if not row or row['root_id'] is None:
        return jsonify({'error': 'Not found'}), 404
    # The audit row belongs to that account's tenant; the write itself went
    # through the definer function and needed none.
    set_tenant(conn, row['root_id'])
    log_action('BILLING_COMP', f'user {user_id} billing_mode set to {mode} by the platform admin')
    app.logger.info('platform admin %s set billing_mode=%s for user %s', sub, mode, user_id)
    return jsonify({'user_id': user_id, 'billing_mode': mode})
```

- [ ] **Step 4: Run the tests**

Run: `/tmp/mobile-venv/bin/python tests/test_billing.py` → `ok`
Run: `/tmp/mobile-venv/bin/python tests/test_platform_admin.py` → `ok`

- [ ] **Step 5: Mutations**

1. Swap `@platform_admin_required` on the new route for `@login_required` → `test_webhook_functions_are_only_called_from_the_webhook` fails ("billing_set_mode called outside the admin gate") and `test_admin_comp_toggle` fails on the non-admin 404. Revert.
2. Key the `states` dict on `b['email']` instead of `b['user_id']` → `billing_state` is `None` for every recent user and `test_admin_overview_counts_and_labels_billing` fails at `'TRIAL'`. Revert.
3. Remove the `mode not in billing_rules.MODES` check → the toggle test fails at the 400 (the CHECK constraint turns it into a 500). Revert.

- [ ] **Step 6: Commit**

```bash
git add server.py tests/test_billing.py
git commit -m "feat: admin overview counts billing states; comp toggle via billing_set_mode"
```

---

### Task 8: The frontend renderers, pure, under node

**Files:**
- Modify: `index.html` — new `// ─── Billing ───` block placed directly before `// ─── Auth ───` (`function showAppScreen`, ~8094); `ADMIN_USER_COLUMNS` (~7928) and `adminOverviewHtml` (~7967)
- Create: `tests/test_billing_js.py`

**Interfaces:**
- Consumes: `escapeHtml(str)`, `settingsRow(label, hint, action, fn, variant)`, `adminCell(value)`, `adminNum(n)`.
- Produces (all pure, all take `b = currentUser.billing` or a row): `billingDate(iso) -> string`, `billingPrice(p) -> '$2.99'`, `billingBannerHtml(b) -> html | ''`, `pricingScreenHtml(b) -> html`, `billingPageHtml(b) -> html`, `billingModalDue(b, seen, today) -> boolean`, `adminCompToggle(u) -> html`. The click handlers they name — `openBillingPage()`, `extendTrial()`, `startCheckout(key)`, `openBillingPortal()`, `refreshBilling()`, `setBillingMode(id, mode)`, `doLogout()`, `routeAfterLogin()` — are wired in Task 9 (except `doLogout`/`routeAfterLogin`, which exist).

- [ ] **Step 1: Write the failing test**

Create `tests/test_billing_js.py`:

```python
"""The billing surfaces' markup, lifted out of index.html and run under node.

Run with: python tests/test_billing_js.py

Every string in these renderers comes off the wire from /api/me. The state
words are looked up, the dates are parsed, the amounts are numbers — and the
one free-text field a hostile server (or a tampered response) could carry, a
price lookup_key, lands inside an onclick attribute, so it goes through
escapeHtml() like everything else.
"""
import json
import os
import re
import shutil
import subprocess
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX = os.path.join(ROOT, 'index.html')

HOSTILE = '"><img src=x onerror=alert(1)>'
PRICES = [{'lookup_key': 'platoon_leader_monthly', 'amount': 299, 'interval': 'month', 'currency': 'usd'},
          {'lookup_key': 'platoon_leader_annual', 'amount': 1999, 'interval': 'year', 'currency': 'usd'}]


def billing(**kw):
    b = {'state': 'TRIAL', 'reason': None, 'days_left': 9, 'trial_ends_at': '2026-10-01T12:00:00+00:00',
         'grace_ends_at': '2026-10-04T12:00:00+00:00', 'extension_available': True, 'subscribed': False,
         'cancel_at_period_end': False, 'current_period_end': None, 'prices': PRICES, 'portal_available': False}
    b.update(kw)
    return b


CASES = {
    'trial': billing(),
    'trial_last_day': billing(days_left=1),
    'trial_urgent': billing(days_left=3, extension_available=False),
    'grace': billing(state='GRACE', reason='trial_expired', days_left=2),
    'grace_no_ext': billing(state='GRACE', reason='trial_expired', days_left=2, extension_available=False),
    'past_due': billing(state='PAST_DUE', subscribed=True, portal_available=True, extension_available=False),
    'active': billing(state='ACTIVE', subscribed=True, portal_available=True, extension_available=False,
                      current_period_end='2026-10-17T12:00:00+00:00', prices=[]),
    'cancelling': billing(state='ACTIVE', subscribed=True, portal_available=True, extension_available=False,
                          current_period_end='2026-10-17T12:00:00+00:00', cancel_at_period_end=True, prices=[]),
    'comped': billing(state='COMPED', extension_available=False, prices=[]),
    'locked_trial': billing(state='LOCKED', reason='trial_expired', days_left=None, extension_available=False),
    'locked_payment': billing(state='LOCKED', reason='payment_required', days_left=None, extension_available=False,
                              portal_available=True),
    'locked_no_prices': billing(state='LOCKED', reason='trial_expired', days_left=None, extension_available=False, prices=[]),
    'hostile': billing(state=HOSTILE, reason=HOSTILE, trial_ends_at=HOSTILE, grace_ends_at=HOSTILE,
                       current_period_end=HOSTILE, days_left=HOSTILE,
                       prices=[{'lookup_key': HOSTILE, 'amount': 299, 'interval': HOSTILE, 'currency': HOSTILE},
                               {'lookup_key': 'platoon_leader_annual', 'amount': 1999, 'interval': 'year', 'currency': 'usd'}]),
    'none': None,
}

DRIVER = r'''
const CASES = ''' + json.dumps(CASES) + r''';
const out = {};
for (const [name, b] of Object.entries(CASES)) {
  out[name] = { banner: billingBannerHtml(b), pricing: pricingScreenHtml(b), page: billingPageHtml(b) };
}
out.modal = {
  due: billingModalDue(CASES.trial_urgent, null, '2026-09-28'),
  seenToday: billingModalDue(CASES.trial_urgent, '2026-09-28', '2026-09-28'),
  seenYesterday: billingModalDue(CASES.trial_urgent, '2026-09-27', '2026-09-28'),
  notUrgent: billingModalDue(CASES.trial, null, '2026-09-28'),
  grace: billingModalDue(CASES.grace, null, '2026-09-28'),
  none: billingModalDue(null, null, '2026-09-28'),
};
out.date = [billingDate('2026-10-01T12:00:00+00:00'), billingDate(''), billingDate(HOSTILE), billingDate(null)];
out.price = [billingPrice({amount: 299}), billingPrice({amount: 1999})];
out.toggle = [adminCompToggle({user_id: 7, billing_mode: 'default'}), adminCompToggle({user_id: 7, billing_mode: 'comped'}),
              adminCompToggle({user_id: 7, billing_mode: null}), adminCompToggle({user_id: HOSTILE, billing_mode: HOSTILE})];
console.log(JSON.stringify(out));
'''


def extract(source, pattern, what):
    m = re.search(pattern, source, re.S)
    assert m, f'could not find {what} in index.html — was it renamed or removed?'
    return m.group(0)


def render():
    node = shutil.which('node')
    assert node, 'node is required (it ships with the CI image)'
    src = open(INDEX, encoding='utf-8').read()
    js = '\n'.join([
        extract(src, r'function escapeHtml\(str\) \{.*?\n\}', 'escapeHtml()'),
        extract(src, r'function settingsRow\(.*?\n\}', 'settingsRow()'),
        extract(src, r'function adminNum\(.*?\n\}', 'adminNum()'),
        extract(src, r'function adminCell\(.*?\n\}', 'adminCell()'),
        extract(src, r'function billingDate\(.*?\n\}', 'billingDate()'),
        extract(src, r'function billingPrice\(.*?\n\}', 'billingPrice()'),
        extract(src, r'function billingDays\(.*?\n\}', 'billingDays()'),
        extract(src, r'function billingBannerHtml\(.*?\n\}', 'billingBannerHtml()'),
        extract(src, r'function billingPlanCards\(.*?\n\}', 'billingPlanCards()'),
        extract(src, r'function pricingScreenHtml\(.*?\n\}', 'pricingScreenHtml()'),
        extract(src, r'function billingPageHtml\(.*?\n\}', 'billingPageHtml()'),
        extract(src, r'function billingModalDue\(.*?\n\}', 'billingModalDue()'),
        extract(src, r'function adminCompToggle\(.*?\n\}', 'adminCompToggle()'),
        DRIVER,
    ])
    path = os.path.join(tempfile.mkdtemp(), 'billing.js')
    with open(path, 'w', encoding='utf-8') as fh:
        fh.write(js)
    proc = subprocess.run([node, path], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout)


ALLOWED_TAGS = {'div', '/div', 'span', '/span', 'section', '/section', 'h1', '/h1', 'h3', '/h3', 'p', '/p',
                'button', '/button', 'img', 'strong', '/strong'}


def tags(html):
    return set(re.findall(r'<(/?[a-zA-Z][\w-]*)', html))


def test_banner_copy(out):
    assert '9 days left in your trial' in out['trial']['banner'] and 'Add billing details' in out['trial']['banner']
    assert 'Extend my trial 7 days' in out['trial']['banner']
    assert '1 day left' in out['trial_last_day']['banner'] and '1 days' not in out['trial_last_day']['banner']
    assert 'billing-banner-warn' in out['trial_urgent']['banner'], 'the last three days turn the banner to the warning colour'
    assert 'billing-banner-warn' not in out['trial']['banner']
    assert 'Extend my trial' not in out['trial_urgent']['banner'], 'no extension link once it is used'
    g = out['grace']['banner']
    assert 'Your trial ended' in g and '2 days until access is paused' in g and 'Choose a plan' in g and 'billing-banner-danger' in g, g
    assert 'Extend my trial 7 days' in g and 'Extend my trial' not in out['grace_no_ext']['banner']
    p = out['past_due']['banner']
    assert "couldn't process your last payment" in p and 'Update your card' in p and 'openBillingPortal()' in p, p
    for name in ('active', 'cancelling', 'comped', 'locked_trial', 'none'):
        assert out[name]['banner'] == '', f'{name} must show no banner: {out[name]["banner"]!r}'


def test_pricing_screen(out):
    h = out['locked_trial']['pricing']
    assert 'Your trial has ended.' in h, h
    assert "startCheckout('platoon_leader_monthly')" in h and "startCheckout('platoon_leader_annual')" in h, h
    assert '$2.99' in h and '$19.99' in h and 'Save 44%' in h, h
    assert 'nothing is deleted' in h and 'doLogout()' in h, h
    assert 'Manage billing' not in h, 'no portal link without a Stripe customer'
    h2 = out['locked_payment']['pricing']
    assert 'Your subscription is no longer active.' in h2 and 'Manage billing' in h2, h2
    h3 = out['locked_no_prices']['pricing']
    assert 'startCheckout' not in h3 and 'Prices are loading' in h3 and 'refreshBilling()' in h3, h3
    assert 'id="pricingStatus"' in h, 'the success poll needs somewhere to write'


def test_billing_page(out):
    assert 'Trial ends Oct 1' in out['trial']['page'] and 'Extend 7 days' in out['trial']['page']
    assert "startCheckout('platoon_leader_monthly')" in out['trial']['page'] and '$19.99' in out['trial']['page'] and '/year' in out['trial']['page']
    assert 'Grace until Oct 4' in out['grace']['page']
    a = out['active']['page']
    assert 'Renews Oct 17' in a and 'openBillingPortal()' in a and 'Cancel subscription' in a and 'Invoices' in a, a
    assert 'startCheckout' not in a, 'a subscribed account is not sold a plan'
    c = out['cancelling']['page']
    assert 'Ends Oct 17' in c and 'not be billed again' in c and 'Cancel subscription' not in c, c
    assert 'Complimentary access' in out['comped']['page'] and 'startCheckout' not in out['comped']['page']
    assert 'Update your card' in out['past_due']['page'] or 'Update card' in out['past_due']['page']
    assert 'Billing does not apply' in out['none']['page']


def test_hostile_strings_never_become_markup(out):
    for surface in ('banner', 'pricing', 'page'):
        html = out['hostile'][surface]
        assert '<img src=x' not in html, f'{surface}: a server string reached the page as markup'
        assert tags(html) <= ALLOWED_TAGS, f'{surface}: a server string opened a tag of its own: {sorted(tags(html))}'
    assert '&quot;&gt;&lt;img' in out['hostile']['pricing'] or 'startCheckout' not in out['hostile']['pricing'], \
        'the hostile lookup_key is either escaped inside onclick or not rendered'
    assert '<img src=x' not in ''.join(out['toggle']) and 'NaN' not in out['toggle'][3]


def test_modal_rule(out):
    m = out['modal']
    assert m['due'] is True and m['seenYesterday'] is True
    assert m['seenToday'] is False, 'once a day'
    assert m['notUrgent'] is False and m['grace'] is False and m['none'] is False


def test_helpers(out):
    assert out['date'] == ['Oct 1', '', '', '']
    assert out['price'] == ['$2.99', '$19.99']
    assert "setBillingMode(7, 'comped')" in out['toggle'][0] and '>Comp<' in out['toggle'][0]
    assert "setBillingMode(7, 'default')" in out['toggle'][1] and '>Uncomp<' in out['toggle'][1]
    assert out['toggle'][2] == '', 'no billing row (unattached) — no toggle'


def main():
    out = render()
    test_banner_copy(out)
    test_pricing_screen(out)
    test_billing_page(out)
    test_hostile_strings_never_become_markup(out)
    test_modal_rule(out)
    test_helpers(out)
    print('ok')


if __name__ == '__main__':
    main()
```

- [ ] **Step 2: Run to verify it fails**

Run: `/tmp/mobile-venv/bin/python tests/test_billing_js.py`
Expected: `AssertionError: could not find billingDate() in index.html`

- [ ] **Step 3: Write the renderers**

Insert directly before the `// ─── Auth ───` comment that precedes `function showAppScreen`:

```javascript
// ─── Billing ───
// Pure renderers over currentUser.billing (the payload /api/me carries), run
// under node by tests/test_billing_js.py. The state words are looked up, the
// dates parsed, the amounts are integers; the only free text a tampered
// response could carry, a price lookup_key, lands in an onclick attribute and
// is escaped like everything else.
const BILLING_MODAL_KEY = 'platoon.billingSeen.';

function billingDate(iso) {
  if (!iso || typeof iso !== 'string') return '';
  const d = new Date(iso);
  return isNaN(d) ? '' : d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' });
}

function billingPrice(p) {
  return `$${(Number(p.amount) / 100).toFixed(2)}`;
}

function billingDays(n) {
  const days = Number(n);
  if (!Number.isFinite(days)) return '';
  return days === 1 ? '1 day' : `${days} days`;
}

function billingBannerHtml(b) {
  if (!b) return '';
  const days = billingDays(b.days_left);
  const link = (label, fn) => `<button type="button" class="billing-banner-link" onclick="${fn}">${label}</button>`;
  const ext = b.extension_available === true ? link('Extend my trial 7 days', 'extendTrial()') : '';
  if (b.state === 'TRIAL') {
    const urgent = Number(b.days_left) <= 3 ? ' billing-banner-warn' : '';
    return `<div class="billing-banner${urgent}" role="status">${days} left in your trial. ${link('Add billing details', 'openBillingPage()')}${ext}</div>`;
  }
  if (b.state === 'GRACE') {
    return `<div class="billing-banner billing-banner-danger" role="alert">Your trial ended. ${days} until access is paused. ${link('Choose a plan', 'openBillingPage()')}${ext}</div>`;
  }
  if (b.state === 'PAST_DUE') {
    return `<div class="billing-banner billing-banner-warn" role="alert">We couldn't process your last payment. ${link('Update your card', 'openBillingPortal()')}</div>`;
  }
  return '';
}

function billingPlanCards(b, compact) {
  const prices = Array.isArray(b && b.prices) ? b.prices : [];
  const monthly = prices.find(p => p.interval === 'month');
  const annual = prices.find(p => p.interval === 'year');
  if (!monthly || !annual) {
    return `<div class="pricing-loading">Prices are loading. <button type="button" class="billing-banner-link" onclick="refreshBilling()">Try again</button></div>`;
  }
  const save = Math.round((1 - Number(annual.amount) / (Number(monthly.amount) * 12)) * 100);
  const card = (p, name, per, best) => `<div class="pricing-card${best ? ' pricing-card-best' : ''}">
      <div class="pricing-name">${name}</div>
      <div class="pricing-amount">${escapeHtml(billingPrice(p))}<span class="pricing-per">/${per}</span></div>
      ${best && save > 0 ? `<div class="pricing-save">Save ${save}%</div>` : '<div class="pricing-save"></div>'}
      <button type="button" class="btn-login pricing-btn" onclick="startCheckout('${escapeHtml(String(p.lookup_key))}')">${best ? 'Go annual' : 'Go monthly'}</button>
    </div>`;
  return `<div class="pricing-cards${compact ? ' pricing-cards-compact' : ''}">${card(monthly, 'Monthly', 'month', false)}${card(annual, 'Annual', 'year', true)}</div>`;
}

function pricingScreenHtml(b) {
  const heading = b && b.reason === 'payment_required' ? 'Your subscription is no longer active.' : 'Your trial has ended.';
  const portal = b && b.portal_available === true
    ? '<button type="button" class="billing-banner-link" onclick="openBillingPortal()">Manage billing</button>' : '';
  return `<div class="login-card"><div class="login-brand pricing-panel">
      <img src="/images/app-logo.png" alt="" class="pricing-logo">
      <h1 class="login-title">${heading}</h1>
      <p class="login-subtitle">Pick a plan to keep going. Your roster and everything in it is kept; nothing is deleted.</p>
      ${billingPlanCards(b, false)}
      <p id="pricingStatus" class="pricing-status" aria-live="polite"></p>
      <div class="pricing-links">${portal}<button type="button" class="billing-banner-link" onclick="doLogout()">Sign out</button></div>
    </div></div>`;
}

function billingPageHtml(b) {
  const back = '<button type="button" class="soldier-back" onclick="routeAfterLogin()">&larr; Back</button>';
  if (!b) {
    return `<div class="login-card"><div class="login-brand pricing-panel">${back}<section class="soldier-card"><h3>Billing</h3><p class="settings-row-hint">Billing does not apply to this account.</p></section></div></div>`;
  }
  let status;
  if (b.state === 'COMPED') status = 'Complimentary access.';
  else if (b.state === 'TRIAL') status = `Trial ends ${billingDate(b.trial_ends_at)}.`;
  else if (b.state === 'GRACE') status = `Your trial ended. Grace until ${billingDate(b.grace_ends_at)}.`;
  else if (b.state === 'PAST_DUE') status = 'Your last payment failed. Update your card to keep access.';
  else if (b.state === 'ACTIVE') status = b.cancel_at_period_end === true
    ? `Ends ${billingDate(b.current_period_end)}. You will not be billed again.`
    : `Renews ${billingDate(b.current_period_end)}.`;
  else if (b.state === 'LOCKED') status = 'Access is paused.';
  else status = '';
  const rows = [];
  if (b.extension_available === true) {
    rows.push(settingsRow('Extend trial', 'One extension of 7 days. No card needed.', 'Extend 7 days', 'extendTrial()'));
  }
  if (b.subscribed !== true && b.state !== 'COMPED') rows.push(billingPlanCards(b, true));
  if (b.subscribed === true || b.portal_available === true) {
    rows.push(settingsRow('Update card', 'Card details, handled by Stripe.', 'Open', 'openBillingPortal()'));
    rows.push(settingsRow('Invoices', 'Receipts and past invoices.', 'Open', 'openBillingPortal()'));
    if (b.subscribed === true && b.cancel_at_period_end !== true) {
      rows.push(settingsRow('Cancel subscription', 'Ends at the period end. Nothing is deleted.', 'Cancel', 'openBillingPortal()', 'dash-btn-ghost'));
    }
  }
  return `<div class="login-card"><div class="login-brand pricing-panel">${back}
      <section class="soldier-card billing-card"><h3>Billing</h3><p class="billing-status">${escapeHtml(status)}</p>${rows.join('')}</section>
    </div></div>`;
}

// Once a calendar day (the tenant's day, by getTodayStr()) during the last
// three days of a trial. `seen` is the stored date string or null.
function billingModalDue(b, seen, today) {
  if (!b || b.state !== 'TRIAL') return false;
  if (!(Number(b.days_left) <= 3)) return false;
  return seen !== today;
}

function adminCompToggle(u) {
  if (!u || !u.billing_mode) return '';
  const id = Number(u.user_id);
  if (!Number.isInteger(id)) return '';
  const comped = u.billing_mode === 'comped';
  return `<button type="button" class="admin-btn admin-btn-ghost admin-btn-xs" onclick="setBillingMode(${id}, '${comped ? 'default' : 'comped'}')">${comped ? 'Uncomp' : 'Comp'}</button>`;
}
```

In `ADMIN_USER_COLUMNS` append `{ key: 'billing_state', label: 'Billing' },`. In `adminOverviewHtml`, add to the `totals` array after `['Pending invites', ...]`:

```javascript
    ['Trialing', adminNum(t.billing_trial)],
    ['In grace', adminNum(t.billing_grace)],
    ['Locked', adminNum(t.billing_locked)],
    ['Subscribed', adminNum(t.billing_active)],
    ['Comped', adminNum(t.billing_comped)],
```

and in `userRows` add, after the `signed_in` cell: `<td>${adminCell(u.billing_state)} ${adminCompToggle(u)}</td>`.

- [ ] **Step 4: Run the tests**

Run: `/tmp/mobile-venv/bin/python tests/test_billing_js.py` → `ok`
Run: `/tmp/mobile-venv/bin/python tests/test_platform_admin_js.py` → `ok` (its fixture rows carry no `billing_mode`, so no toggle renders and the tag allowlist holds)

- [ ] **Step 5: Mutations**

1. In `billingPlanCards`, drop `escapeHtml(` around `String(p.lookup_key)` → `test_hostile_strings_never_become_markup` fails. Revert.
2. In `billingModalDue`, change `seen !== today` to `true` → `test_modal_rule` fails at "once a day". Revert.
3. In `billingBannerHtml`, change `<= 3` to `< 3` → `test_banner_copy` fails at "the last three days turn the banner". Revert.

- [ ] **Step 6: Commit**

```bash
git add index.html tests/test_billing_js.py
git commit -m "feat: billing banner, pricing screen, billing page and admin toggle renderers"
```

---

### Task 9: Wiring, CSS and the layout checks

**Files:**
- Modify: `index.html` — `<body>` (~3441), a `#billingScreen` div after `#createUnitScreen` (~3525), CSS after the `#createUnitScreen` block (~333), `api()` (~4244), `showAppScreen` / `showLoginScreen` (~8094-8165), `routeAfterLogin` (~8835), `renderSettings` Account card (~4659), the Billing block from Task 8 (wiring functions appended to it)
- Modify: `tests/test_mobile_layout.py` — `USER_FIXTURE` (~166), `ADMIN_FIXTURE` totals/users (~184), `run_checks` (~548)

**Interfaces:**
- Consumes: Task 8's renderers; `api()`, `showAppScreen`, `routeAfterLogin`, `getTodayStr()`, `confirmDialog(title, body, okLabel, {html})`, `openSettings`, `doLogout`.
- Produces: `renderBillingBanner()`, `showBillingScreen(locked)`, `openBillingPage()`, `extendTrial()`, `startCheckout(key)`, `openBillingPortal()`, `refreshBilling()`, `awaitBillingActive()`, `maybeShowBillingModal()`, `setBillingMode(id, mode)`; `body.billing-active`; `#billingBanner`, `#billingScreen`.

- [ ] **Step 1: Extend the layout test (fails first)**

In `tests/test_mobile_layout.py`:

`USER_FIXTURE` gains:

```python
    # Two days left in the trial, with the extension still available, so the
    # banner is at its longest: the countdown plus two links.
    'billing': {'state': 'TRIAL', 'reason': None, 'days_left': 2,
                'trial_ends_at': TODAY.isoformat() + 'T12:00:00+00:00',
                'grace_ends_at': TODAY.isoformat() + 'T12:00:00+00:00',
                'extension_available': True, 'subscribed': False, 'cancel_at_period_end': False,
                'current_period_end': None, 'portal_available': False,
                'prices': [{'lookup_key': 'platoon_leader_monthly', 'amount': 299, 'interval': 'month', 'currency': 'usd'},
                           {'lookup_key': 'platoon_leader_annual', 'amount': 1999, 'interval': 'year', 'currency': 'usd'}]},
```

`ADMIN_FIXTURE['totals']` gains `'billing_trial': 12, 'billing_grace': 3, 'billing_locked': 4, 'billing_active': 118, 'billing_comped': 5`; each `recent_users` row gains `'billing_state': 'TRIAL', 'billing_mode': 'default'` (first) and `'billing_state': 'COMPED', 'billing_mode': 'comped'` (second).

In `INIT_JS`'s stub `api`, add `if (p === '/billing/extend') return { billing: fixture.user.billing };` and `if (p === '/billing/checkout' || p === '/billing/portal') return null;`.

Add a check function next to `check_home`:

```python
BILLING_LINKS = '#billingBanner .billing-banner-link'
PRICING_BUTTONS = '#billingScreen .pricing-btn, #billingScreen .billing-banner-link, #billingScreen .dash-btn, #billingScreen .soldier-back'


def check_billing(page, width):
    """The trial banner sits above every signed-in screen; the pricing screen
    and the Billing page are one full-screen panel. None of it may overflow,
    and every control on it is a real tap target."""
    assert page.evaluate("!!document.querySelector('#billingBanner .billing-banner')"), (
        f'billing @ {width}px: the trial banner did not render on the home screen')
    check_no_horizontal_overflow(page, width, 'home + billing banner')
    if width < 900:
        check_tap_targets(page, width, BILLING_LINKS, 'billing banner')
    page.evaluate('showBillingScreen(true)')
    assert page.evaluate("document.querySelectorAll('#billingScreen .pricing-btn').length") == 2, (
        f'pricing @ {width}px: two purchase buttons expected')
    assert not page.evaluate("!!document.querySelector('#billingBanner .billing-banner')"), (
        'the banner must not sit above the pricing screen')
    check_no_horizontal_overflow(page, width, 'pricing')
    check_fits_width(page, width, '#billingScreen .login-card', 'pricing')
    if width < 900:
        check_tap_targets(page, width, PRICING_BUTTONS, 'pricing')
    page.evaluate('showBillingScreen(false)')
    assert page.evaluate("!!document.querySelector('#billingScreen .billing-card')"), (
        f'billing page @ {width}px: the Billing card did not render')
    check_no_horizontal_overflow(page, width, 'billing page')
    check_fits_width(page, width, '#billingScreen .login-card', 'billing page')
    if width < 900:
        check_tap_targets(page, width, PRICING_BUTTONS, 'billing page')
    page.evaluate(SHOW_HOME_JS, UNITS_FIXTURE)
```

and call it in `run_checks` right after the two `check_home(...)` lines: `check_billing(page, width)`. Also, in the existing Settings block, after the `unitLogoInput` assertion, add:

```python
        assert page.evaluate("!!document.querySelector('#settingsView [onclick=\"openBillingPage()\"]')"), (
            f'settings @ {width}px: the Billing row did not render')
```

Run: `/tmp/mobile-venv/bin/python tests/test_mobile_layout.py`
Expected: FAIL at "the trial banner did not render on the home screen".

- [ ] **Step 2: Markup and CSS**

Right after `<body>`: `<div id="billingBanner"></div>`.

After the `#createUnitScreen` div:

```html
<!-- ─── Billing: the pricing screen (locked) and the Billing page ─── -->
<div id="billingScreen" style="display:none"></div>
```

CSS, after the `#createUnitScreen` block:

```css
  /* ─── Billing ─── */
  /* The banner is in normal flow at the top of <body>: nothing fixed, nothing
     to offset. body.dash-active zeroes the body padding, so it is full-bleed
     on the roster and inset like everything else elsewhere. */
  .billing-banner {
    display: flex; flex-wrap: wrap; align-items: center; justify-content: center; gap: 4px 14px;
    padding: 8px 16px; margin-bottom: 12px; font-size: 0.92em; text-align: center;
    background: var(--cp-steel-bg); color: var(--cp-text-strong); border-bottom: 1px solid var(--cp-border);
  }
  .billing-banner-warn { background: var(--cp-amber-bg); }
  .billing-banner-danger { background: var(--cp-red-bg); }
  .billing-banner-link {
    background: none; border: 0; padding: 6px 4px; min-height: 32px; cursor: pointer;
    color: var(--cp-accent); font: inherit; font-weight: 600; text-decoration: underline;
  }
  .billing-banner-link:hover { color: var(--cp-accent-hover); }
  body.dash-active .billing-banner { margin-bottom: 0; }
  #billingScreen {
    display: none; align-items: center; justify-content: center; min-height: calc(100vh - 48px); padding: 8px 0;
  }
  #billingScreen .login-card, body.light-mode #billingScreen .login-card { background: none; }
  #billingScreen .login-brand { align-items: stretch; padding: 28px; }
  #billingScreen .login-card { width: min(100%, 560px); }
  .pricing-logo { width: 72px; height: 72px; object-fit: contain; margin: 0 auto 10px; display: block; }
  .pricing-cards { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin: 18px 0 8px; }
  .pricing-cards-compact { margin: 8px 0; }
  .pricing-card {
    display: flex; flex-direction: column; gap: 6px; padding: 16px 14px; min-width: 0;
    border: 1px solid var(--cp-border); border-radius: var(--cp-radius-lg); background: var(--cp-surface);
  }
  .pricing-card-best { border-color: var(--cp-accent); }
  .pricing-name { font-weight: 600; color: var(--cp-text-strong); }
  .pricing-amount { font-size: 1.5em; font-weight: 700; color: var(--cp-text-strong); font-variant-numeric: tabular-nums; }
  .pricing-per { font-size: 0.55em; font-weight: 500; color: var(--cp-muted); }
  .pricing-save { min-height: 1.2em; font-size: 0.82em; font-weight: 600; color: var(--cp-green); }
  .pricing-btn { min-height: 44px; margin-top: auto; }
  .pricing-loading, .pricing-status { text-align: center; color: var(--cp-muted); font-size: 0.9em; margin: 10px 0 0; min-height: 1.2em; }
  .pricing-links { display: flex; justify-content: center; gap: 14px; flex-wrap: wrap; margin-top: 14px; }
  .billing-card { margin-top: 12px; }
  .billing-status { margin: 0 0 12px; color: var(--cp-text); }
  .admin-btn-xs { padding: 4px 8px; font-size: 0.78em; min-height: 28px; margin-left: 6px; }
  @media (max-width: 420px) {
    .pricing-cards { grid-template-columns: 1fr; }
  }
```

- [ ] **Step 3: Wiring**

`api()` — insert directly after the 401 line:

```javascript
  // 402 = the account is locked behind the pricing screen. The body carries
  // the billing verdict; show the screen and stop. Never a toast, never a
  // resync: every other call would 402 too.
  if (res.status === 402) {
    let j = null;
    try { j = await res.json(); } catch (e) {}
    if (currentUser && j && j.billing) currentUser.billing = j.billing;
    showBillingScreen(true);
    return null;
  }
```

`showAppScreen(screen)` — add after the `adminScreen` line:

```javascript
  document.getElementById('billingScreen').style.display = screen === 'billing' ? 'flex' : 'none';
  document.body.classList.toggle('billing-active', screen === 'billing');
  renderBillingBanner();
```

`showLoginScreen(view)` — add after the `adminScreen` line: `document.getElementById('billingScreen').style.display = 'none'; document.body.classList.remove('billing-active'); document.getElementById('billingBanner').innerHTML = '';`

`routeAfterLogin()` — after `document.getElementById('loginScreen').style.display = 'none';` and before `const route = parseAppRoute();`:

```javascript
  // Back from Stripe Checkout: strip the marker and wait for the webhook.
  const params = new URLSearchParams(window.location.search);
  if (params.get('billing') === 'success') {
    history.replaceState({}, '', window.location.pathname);
    awaitBillingActive();
  }
  // A locked account lands on the pricing screen and nowhere else.
  if (currentUser && currentUser.billing && currentUser.billing.state === 'LOCKED') { showBillingScreen(true); return; }
```

and at the very end of `routeAfterLogin()` (after the `else if (LIST_PAGES...)` line): `maybeShowBillingModal();`

`renderSettings()` — in the Account card, before the `Manage account` row:

```javascript
        ${settingsNavRow('Billing', billingSettingsHint(currentUser && currentUser.billing), 'openBillingPage()')}
```

Append to the Billing block (after `adminCompToggle`):

```javascript
function billingSettingsHint(b) {
  if (!b) return 'Plan, card and invoices.';
  if (b.state === 'COMPED') return 'Complimentary access.';
  if (b.state === 'TRIAL') return `Trial ends ${billingDate(b.trial_ends_at)}.`;
  if (b.state === 'GRACE') return `Trial ended. ${billingDays(b.days_left)} until access is paused.`;
  if (b.state === 'PAST_DUE') return 'Payment failed. Update your card.';
  if (b.state === 'ACTIVE') return b.cancel_at_period_end ? `Ends ${billingDate(b.current_period_end)}.` : `Renews ${billingDate(b.current_period_end)}.`;
  return 'Plan, card and invoices.';
}

function renderBillingBanner() {
  const el = document.getElementById('billingBanner');
  if (!el) return;
  const signedIn = document.body.classList.contains('app-authenticated');
  const onPricing = document.body.classList.contains('billing-active');
  el.innerHTML = signedIn && !onPricing ? billingBannerHtml(currentUser && currentUser.billing) : '';
}

// One screen, two faces: the pricing screen when locked, the Billing page
// otherwise. Neither pushes history — Back is routeAfterLogin(), which lands
// wherever the address bar still says.
function showBillingScreen(locked) {
  showAppScreen('billing');
  const b = currentUser && currentUser.billing;
  document.getElementById('billingScreen').innerHTML = locked ? pricingScreenHtml(b) : billingPageHtml(b);
  window.scrollTo({ top: 0 });
}

function openBillingPage() { showBillingScreen(false); }

function setPricingStatus(msg) {
  const el = document.getElementById('pricingStatus');
  if (el) el.textContent = msg;
}

async function refreshBilling() {
  const me = await api('GET', '/me');
  if (!me) return;
  currentUser = me;
  renderBillingBanner();
  if (document.body.classList.contains('billing-active')) {
    showBillingScreen(me.billing && me.billing.state === 'LOCKED');
  }
}

async function extendTrial() {
  const r = await api('POST', '/billing/extend');
  if (!r || !r.billing) return;
  currentUser.billing = r.billing;
  renderBillingBanner();
  if (document.body.classList.contains('billing-active')) showBillingScreen(false);
  else if (document.getElementById('settingsView') && document.body.classList.contains('settings-active')) renderSettings();
}

async function startCheckout(lookupKey) {
  setPricingStatus('Opening secure checkout…');
  const r = await api('POST', '/billing/checkout', { lookup_key: lookupKey });
  if (r && r.url) { window.location.assign(r.url); return; }
  setPricingStatus('');
}

async function openBillingPortal() {
  const r = await api('POST', '/billing/portal');
  if (r && r.url) window.location.assign(r.url);
}

// After Checkout, Stripe sends us back before the webhook has necessarily
// landed. Poll /api/me (exempt from the 402) until the account reads as
// subscribed, then route home; give up quietly after 30 s and leave the
// banner or the pricing screen to say what the server says.
async function awaitBillingActive() {
  for (let i = 0; i < 15; i++) {
    const me = await api('GET', '/me');
    if (me) {
      currentUser = me;
      if (me.billing && me.billing.subscribed) {
        renderBillingBanner();
        if (document.body.classList.contains('billing-active')) routeAfterLogin();
        return;
      }
    }
    setPricingStatus('Confirming your payment…');
    await new Promise(r => setTimeout(r, 2000));
  }
  setPricingStatus('Still confirming. Refresh in a moment, or check Settings → Billing.');
}

async function maybeShowBillingModal() {
  const b = currentUser && currentUser.billing;
  if (!b || !currentUser.id) return;
  const today = getTodayStr();
  let seen = null;
  try { seen = localStorage.getItem(BILLING_MODAL_KEY + currentUser.id); } catch (e) {}
  if (!billingModalDue(b, seen, today)) return;
  try { localStorage.setItem(BILLING_MODAL_KEY + currentUser.id, today); } catch (e) {}
  const ext = b.extension_available
    ? '<p><button type="button" class="billing-banner-link" onclick="settleConfirm(false); extendTrial()">Extend my trial 7 days</button></p>' : '';
  const body = `<p class="list-confirm-lead">Your trial ends ${escapeHtml(billingDate(b.trial_ends_at))}. Pick a plan to keep your roster and everything in it.</p>${ext}`;
  const choose = await confirmDialog('Keep your access', body, 'Choose a plan', { html: true });
  if (choose) openBillingPage();
}

async function setBillingMode(userId, mode) {
  const r = await api('PUT', `/admin/users/${userId}/billing_mode`, { mode });
  if (r) refreshAdmin();
}
```

Check the confirm modal's cancel button label: if `confirmDialog` has a fixed "Cancel" label, that is the "Remind me tomorrow" affordance; leave it. (`settleConfirm` exists at ~5723.)

- [ ] **Step 4: Run the tests**

Run: `/tmp/mobile-venv/bin/python tests/test_mobile_layout.py` → `ok` (three widths; must NOT print `SKIPPED`)
Run: `/tmp/mobile-venv/bin/python tests/test_billing_js.py` → `ok`
Run: `/tmp/mobile-venv/bin/python tests/test_platform_admin_js.py` → `ok`
Run: `/tmp/mobile-venv/bin/python tests/test_access_ux_js.py` → `ok`
Run: `/tmp/mobile-venv/bin/python tests/test_smoke.py` → `ok`

- [ ] **Step 5: Mutations**

1. In `renderBillingBanner`, drop `&& !onPricing` → the layout test fails at "the banner must not sit above the pricing screen". Revert.
2. Change `.pricing-cards` to `grid-template-columns: 1fr 1fr 1fr 1fr` and remove the 420px rule → the layout test's overflow check at 320px fails on `.pricing-card`. Revert.
3. In `api()`, delete the 402 branch → no automated test catches this (the layout test stubs `api`); prove it by hand in the dev rehearsal (Task 10, step 4c). Record that in the ledger.

- [ ] **Step 6: Commit**

```bash
git add index.html tests/test_mobile_layout.py
git commit -m "feat: billing banner, pricing screen, Billing page, last-days modal and 402 handling"
```

---

### Task 10: Docs, the full suite, CI, dev rehearsal

**Files:**
- Modify: `CLAUDE.md` (tests list; a new "Billing" subsection under Architecture; the Backup paragraph; the SECURITY DEFINER list)
- Modify: `docs/superpowers/specs/2026-09-17-stripe-billing-design.md` — set `**Status:** implemented on dev …` when step 4 passes
- Ledger: `.superpowers/sdd/2026-09-16-unit-tree/progress.md` in the main checkout (controller)

- [ ] **Step 1: CLAUDE.md**

Add to the test list:

```bash
python tests/test_billing_state.py  # billing_rules.billing_state(): every state and boundary, no DB
python tests/test_billing.py        # the subscriptions row, the 402 gate sweep, extend/checkout/portal
                                    # with Stripe stubbed, the signed webhook, deletion, backup, /admin comp
python tests/test_billing_js.py     # banner, pricing screen, Billing page and modal rule, under node
```

Add a subsection after "### Platform admin" (or the last Architecture subsection):

```markdown
### Billing

Per-account Stripe billing, spec `docs/superpowers/specs/2026-09-17-stripe-billing-design.md`
(read section 10 first — the rulings that fit the spec to this code).
**`billing_rules.py`** is the whole rule: `billing_state(row, now, default_on,
platform_admin, enabled)` → `COMPED | ACTIVE | PAST_DUE | TRIAL | GRACE | LOCKED`,
pure, UTC, clock-injected, no Flask. `_resolved_user()` creates the
`subscriptions` row at an attached account's first sign-in (the trial starts
at the first sign-in that finds the account billed, not at creation) and
computes `g.billing`; the three tenant decorators answer **402**
`{'error': 'subscription_required', 'billing': {...}}` to a `LOCKED` account
on every `/api/` route except `/api/me` and the `/api/auth/`, `/api/billing/`,
`/api/admin/` prefixes. `GET /api/units` is deliberately not exempt.
`platform_admin_required` declares no tenant and has no `g.billing`.

Env: `STRIPE_MODE=test|live` picks which of `STRIPE_TEST_*` / `STRIPE_LIVE_*`
(secret key + webhook signing secret) the process reads; dev is `test`,
production `live`. **No key for the active mode = billing off**, every
account `COMPED`, one warning at boot. `BILLING_DEFAULT=on|off` is the
default for accounts whose `billing_mode` is `default`; `/admin` comps or
bills any account (`PUT /api/admin/users/<id>/billing_mode`). The operator's
own account is always comped.

The webhook (`POST /api/billing/webhook`, undecorated, signature is the
auth, listed in the smoke test's `PUBLIC_API`) is the only writer of the
Stripe columns, through the four SECURITY DEFINER `billing_*` functions in
`sql/billing_functions.sql`; `tests/test_billing.py` greps `server.py` to
keep it that way. Replays are no-ops (`stripe_events`); a handler that
raises is a 500 and the event record rolls back with it, so Stripe's retry
is handled rather than skipped. Cancellation is at period end through the
Billing Portal and never flips local state — the webhook does. Stripe is
called through five one-line `_stripe_*` seams; tests replace those.
`stripe_customer_id` is stored `<mode>:<id>`.
```

In the Backup section add: `An owner's ` `users` ` rows also carry ` `billing_mode` `, ` `trial_started_at` `, ` `trial_ends_at` ` and ` `extended_at` ` (optional keys; restore upserts ` `subscriptions` ` from them and never writes a Stripe column).` In the Tenancy section's SECURITY DEFINER sentence, mention that `sql/billing_functions.sql` and `admin_billing_rows()` are the other enumerated exceptions.

- [ ] **Step 2: The whole suite, then CI**

```bash
for f in tests/test_*.py; do echo "== $f"; /tmp/mobile-venv/bin/python "$f" 2>&1 | tail -2; done
```

Every file prints `ok` and none prints `SKIPPED`. Then push the branch and wait for CI:

```bash
git add CLAUDE.md && git commit -m "docs: billing section, test list, backup keys"
git push -u origin stripe-billing
gh run watch --exit-status $(gh run list --branch stripe-billing --limit 1 --json databaseId --jq '.[0].databaseId')
```

- [ ] **Step 3: Operator prerequisites (the user, in the Stripe dashboard — test mode first)**

1. Product "Platoon Manager, leader" with two recurring prices; set lookup keys `platoon_leader_monthly` (2.99 USD/month) and `platoon_leader_annual` (19.99 USD/year).
2. Webhook endpoint `https://platoondev.carr7.com/api/billing/webhook` with events `customer.subscription.created`, `customer.subscription.updated`, `customer.subscription.deleted`, `invoice.paid`, `invoice.payment_failed`, `checkout.session.completed`; copy the signing secret.
3. Customer Portal enabled: update payment method, invoice history, cancel subscription (at period end).
4. Append to `/opt/homelab/platoon-dev/.env`: `STRIPE_MODE=test`, `STRIPE_TEST_SECRET_KEY=…`, `STRIPE_TEST_WEBHOOK_SECRET=…`, `BILLING_DEFAULT=on`.

- [ ] **Step 4: Dev rehearsal (controller, on prodsrv02's dev stack; production untouched)**

a. Merge `stripe-billing` into `main` locally (no push yet), `git pull` on `/opt/homelab/platoon-dev` from the branch, `docker compose up -d --build app`; check the app log for the boot warning being ABSENT (key present) and `/api/auth/config` 200.
b. Sign in as the dev test leader (`jonathon.carr5+platoon@gmail.com`): the trial banner shows 14 days; Settings → Billing shows both prices.
c. Prove the 402 by hand: as owner in `psql` (dev db) `UPDATE subscriptions SET trial_ends_at = now() - interval '10 days'`, reload: the pricing screen appears from `api()`'s 402, not a toast. `Extend my trial` returns to TRIAL with 7 days. Set it expired again with `extended_at = now()`: the pricing screen has no extension link.
d. Checkout with card `4242 4242 4242 4242`; Stripe returns to `/<slug>/settings?billing=success`; the poll lands home; Billing shows "Renews …"; `/admin` shows Subscribed 1.
e. In the Stripe dashboard: cancel at period end through the portal → Billing shows "Ends …, you will not be billed again", still open. Simulate a failed payment (dashboard "Update subscription" → invoice with a failing test card, or send `invoice.payment_failed` from the webhook test tool) → past-due banner, still open. Resend the last event from the dashboard → 200 `{"replay": true}` in the app log.
f. `/admin`: comp the account → banner gone; uncomp → back.
g. Record every observation in the ledger as REHEARSAL 7; any bug is a task before production.

- [ ] **Step 5: Production (only on the user's explicit go)**

Operator repeats step 3 in **live** mode with `https://platoon.carr7.com/api/billing/webhook` and appends the `STRIPE_LIVE_*` keys, `STRIPE_MODE=live`, `BILLING_DEFAULT=on` to `/opt/homelab/platoon/.env`. Then the ordinary deploy: push `main`, CI green, `git pull && docker compose up -d --build app` on prodsrv02, verify `/`, `/api/me` 401, no boot warning in the log, `/admin` totals. The five existing accounts start their 14-day trial at their next sign-in; comp any of them from `/admin`.

---

## Self-review

**Spec coverage.** §2 D1–D11: per-account (Task 3), lookup keys (Task 4), trial at first billed sign-in (Task 3), one extension no card in TRIAL/GRACE/locked-after-trial (Task 4), 3-day grace (Task 1), hard lock + pricing screen + no read carve-out (Tasks 3, 8, 9), past due open (Tasks 1, 5), `BILLING_DEFAULT` + tri-state + admin comped (Tasks 1, 3, 7), cancel at period end never flips local state (Task 5: only the webhook writes `stripe_status`), no emails (nothing), pinned SDK (Task 2). §3 pure function + table tests (Task 1). §4.1 table, `stripe_events`, backup (Tasks 2, 6). §4.2 row creation (Task 3). §4.3 four functions + grep test (Tasks 2, 5, 7). §4.4 mode prefix (Tasks 3, 4, 5). §5.1 gate + sweep (Task 3). §5.2 payload + price cache (Tasks 3, 4). §5.3 routes + audit actions (Task 4). §5.4 webhook (Task 5). §5.5 deletion (Task 6). §5.6 admin (Task 7). §6 banner, modal, grace/past-due banners, pricing screen with poll, Billing page, admin column + toggle (Tasks 8, 9). §7 tests (Tasks 1–9), layout (Task 9). §8 rollout (Task 10). §10 amendments: all fifteen are reflected (A1 Task 3, A2 Task 6, A3/A4 Task 9, A5 Task 9, A6 Tasks 8/9, A7 Task 6, A8 Task 5, A9 Task 2, A10 Task 1, A11 Task 4, A12 Task 1, A13 Tasks 1/2, A14 Tasks 2/5, A15 Task 7).

**Placeholders.** None: every step carries its code. Task 9 step 5 mutation 3 is an honest "no automated test; proved by hand in the rehearsal", recorded as such.

**Type consistency.** `billing_state(row, now, default_on, platform_admin=False, enabled=True)` everywhere; payload keys `state, reason, days_left, trial_ends_at, grace_ends_at, extension_available, subscribed, cancel_at_period_end, current_period_end, prices, portal_available` match between Task 3, the JS fixtures in Tasks 8/9 and the tests; `_stripe_*` seam names match between Task 4's definitions and the tests' `Stripe.install()`; `admin_billing_rows()` name matches SQL, the grant loop, `ADMIN_FUNCTIONS`, the grep regex and Task 7; `showBillingScreen(locked)` / `openBillingPage()` / `extendTrial()` / `startCheckout()` / `openBillingPortal()` / `refreshBilling()` / `setBillingMode()` match between the renderers (Task 8), the wiring (Task 9) and the layout test.
