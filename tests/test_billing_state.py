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
