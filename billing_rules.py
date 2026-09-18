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
