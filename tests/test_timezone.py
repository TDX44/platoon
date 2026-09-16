"""The duty day belongs to the unit, not to the server.

prodsrv02 runs UTC, so date.today() rolled over at 1900 Central and marked four
people away for a course that started the next morning. Every "what day is it"
question now goes through app_today(), which reads a fixed timezone.

Run with: python tests/test_timezone.py
"""
import os
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dbharness  # noqa: E402
_schema = dbharness.setup()
# The bug's exact conditions: a server whose own clock is UTC.
os.environ['TZ'] = 'UTC'
try:
    import time
    time.tzset()
except AttributeError:          # not POSIX; the assertions below still hold
    pass
os.environ['PLATOON_TZ'] = 'America/Chicago'

import server  # noqa: E402  (must follow the env overrides above)

CENTRAL = ZoneInfo('America/Chicago')


def check_today_is_the_units_day():
    """The evening hours are the whole bug: 1931 Central is already tomorrow UTC."""
    from datetime import date
    server_day = date.today().isoformat()
    unit_day = server.app_today()
    now_central = datetime.now(CENTRAL)
    assert unit_day == now_central.date().isoformat(), (
        f'app_today() returned {unit_day}, expected {now_central.date().isoformat()}'
    )
    # Only meaningful during the UTC-offset window, but free to assert always.
    if now_central.hour >= 19:
        assert unit_day != server_day, (
            'after 1900 Central the server is already on tomorrow in UTC; '
            'app_today() must still say today'
        )


def check_stamp_is_the_units_clock():
    stamp = server.app_stamp()
    parsed = datetime.strptime(stamp, '%Y-%m-%d %H:%M:%S')
    expected = datetime.now(CENTRAL).replace(tzinfo=None)
    drift = abs((parsed - expected).total_seconds())
    assert drift < 5, f'app_stamp() is {drift:.0f}s off the unit clock: {stamp}'
    # ...and demonstrably not UTC, which is what it used to be.
    utc_naive = datetime.now(timezone.utc).replace(tzinfo=None)
    assert abs((parsed - utc_naive).total_seconds()) > 3000, (
        'app_stamp() is still writing UTC'
    )


def check_no_raw_date_today_remains():
    """One missed call reintroduces the bug on that one code path."""
    source = open(os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), 'server.py')).read()
    code = [ln for ln in source.splitlines()
            if 'date.today()' in ln and not ln.lstrip().startswith('#')]
    assert not code, f'these lines still read the server clock directly: {code}'


def check_absence_activates_on_the_units_day():
    """The actual failure: an absence starting tomorrow must not be active tonight."""
    server.get_current_user = lambda: {'is_admin': 1, 'id': 1, 'username': 'boss', 'platoons': '*'}
    conn = server.get_db()
    conn.execute('DELETE FROM personnel')
    conn.execute('DELETE FROM scheduled_events')
    conn.execute("INSERT INTO personnel (id, rank, last, first, status, platoon) "
                 "VALUES (1, 'CW2', 'Boundy', 'Ray', 'present', '2nd')")
    tomorrow = (datetime.now(CENTRAL).date() + timedelta(days=1)).isoformat()
    conn.execute(
        'INSERT INTO scheduled_events (person_id, platoon, status, from_date, to_date, notes, state) '
        "VALUES (1, '2nd', 'tdy', %s, %s, 'IO - Dothan, AL', 'scheduled')",
        (tomorrow, tomorrow))
    conn.commit()
    conn.close()

    client = server.app.test_client()
    assert client.get('/api/personnel?platoon=2nd').status_code == 200

    conn = server.get_db()
    state = conn.execute('SELECT state FROM scheduled_events WHERE person_id = 1').fetchone()['state']
    status = conn.execute('SELECT status FROM personnel WHERE id = 1').fetchone()['status']
    conn.close()
    assert state == 'scheduled', f"tomorrow's TDY activated early (state={state})"
    assert status == 'present', f'roster shows {status}; the course has not started yet'


def check_timezone_is_an_org_setting():
    """One zone for the whole organisation, stored, admin-only, validated."""
    server.get_current_user = lambda: {'is_admin': 1, 'id': 1, 'username': 'boss', 'platoons': '*'}
    c = server.app.test_client()

    assert c.put('/api/settings?platoon=2nd', json={'timezone': 'Europe/Berlin'}).status_code == 200
    assert server.app_timezone() == 'Europe/Berlin'
    # It changes the duty day, which is the whole point of storing it.
    berlin_day = datetime.now(ZoneInfo('Europe/Berlin')).date().isoformat()
    assert server.app_today() == berlin_day, (server.app_today(), berlin_day)

    # Published on both the public config and the roster's own settings call,
    # so a client picks it up whichever it loads first.
    assert c.get('/api/auth/config').get_json()['timezone'] == 'Europe/Berlin'
    assert c.get('/api/settings?platoon=2nd').get_json()['timezone'] == 'Europe/Berlin'

    # One key for the organisation, not one per platoon.
    conn = server.get_db()
    keys = [r['key'] for r in conn.execute(
        "SELECT key FROM settings WHERE key LIKE '%timezone%'")]
    conn.close()
    assert keys == [server.TIMEZONE_KEY], keys

    # Survives a restart: the stored value is adopted, not the env fallback.
    server.set_app_timezone(server.FALLBACK_TZ)
    assert server.load_app_timezone() == 'Europe/Berlin'

    # A nonsense zone is refused and changes nothing.
    assert c.put('/api/settings?platoon=2nd', json={'timezone': 'Mars/Olympus'}).status_code == 400
    assert server.app_timezone() == 'Europe/Berlin'

    # A bad value already in the database must not stop the app booting.
    conn = server.get_db()
    conn.execute('UPDATE settings SET value = %s WHERE key = %s', ('Nowhere/Nothing', server.TIMEZONE_KEY))
    conn.commit()
    conn.close()
    assert server.load_app_timezone() == 'Europe/Berlin', 'a bad stored zone must fall back, not raise'

    # Non-admins cannot move the duty day for everyone else.
    server.get_current_user = lambda: {'is_admin': 0, 'id': 2, 'username': 'joe', 'platoons': '2nd'}
    assert c.put('/api/settings?platoon=2nd', json={'timezone': 'UTC'}).status_code == 403
    assert server.app_timezone() == 'Europe/Berlin'

    server.get_current_user = lambda: {'is_admin': 1, 'id': 1, 'username': 'boss', 'platoons': '*'}
    c.put('/api/settings?platoon=2nd', json={'timezone': 'America/Chicago'})


def _assert_unit_clock(stamp, what):
    parsed = datetime.strptime(str(stamp)[:19], '%Y-%m-%d %H:%M:%S')
    central = datetime.now(CENTRAL).replace(tzinfo=None)
    assert abs((parsed - central).total_seconds()) < 120, \
        f'{what} is {stamp}, which is not the unit clock'
    utc_naive = datetime.now(timezone.utc).replace(tzinfo=None)
    assert abs((parsed - utc_naive).total_seconds()) > 3000, \
        f'{what} is still on the database server clock (UTC): {stamp}'


def check_stored_timestamps_use_the_units_clock():
    """A column DEFAULT runs in the DATABASE, whose clock is the wrong one.

    report_history.created_at and scheduled_events.created_at both defaulted to
    to_char(now(), ...). now() is the db container's clock — UTC here and in
    production, where the GUC was baked at initdb — so a report generated 2130
    Sunday was stamped 0230 Monday: the wrong duty day, on a page people read
    by date. Both call sites now pass app_stamp() explicitly, which is what
    CLAUDE.md's Time rule already required.
    """
    server.get_current_user = lambda: {'is_admin': 1, 'id': 1, 'username': 'boss', 'platoons': '*'}
    c = server.app.test_client()

    r = c.post('/api/reports', json={'platoon': '2nd', 'unit_name': 'Alpha', 'text': 'tz probe'})
    assert r.status_code == 201, r.get_json()
    _assert_unit_clock(r.get_json()['created_at'], 'report_history.created_at')

    conn = server.get_db()
    person_id = conn.execute(
        "INSERT INTO personnel (rank, last, first, platoon) "
        "VALUES ('SGT', 'Tzprobe', 'Sam', '2nd') RETURNING id").fetchone()['id']
    conn.commit()
    conn.close()

    # Far-future window: it stays 'scheduled' and cannot disturb the absence
    # checks that run before this one.
    r = c.post(f'/api/personnel/{person_id}/schedule',
               json={'status': 'leave', 'from_date': '2099-01-01', 'to_date': '2099-01-05'})
    assert r.status_code == 201, r.get_json()
    _assert_unit_clock(r.get_json()['created_at'], 'scheduled_events.created_at')

    conn = server.get_db()
    conn.execute('DELETE FROM personnel WHERE id = %s', (person_id,))
    conn.execute("DELETE FROM report_history WHERE text = 'tz probe'")
    conn.commit()
    conn.close()


def check_config_publishes_the_timezone():
    """The frontend adopts this so the two clocks cannot drift apart."""
    payload = server.app.test_client().get('/api/auth/config').get_json()
    assert payload.get('timezone') == 'America/Chicago', payload


def main():
    check_today_is_the_units_day()
    check_stamp_is_the_units_clock()
    check_no_raw_date_today_remains()
    check_absence_activates_on_the_units_day()
    check_timezone_is_an_org_setting()
    check_stored_timestamps_use_the_units_clock()
    check_config_publishes_the_timezone()
    print('ok')
    dbharness.teardown(_schema)


if __name__ == '__main__':
    main()
