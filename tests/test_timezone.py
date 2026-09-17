"""The duty day belongs to the unit, not to the server.

prodsrv02 runs UTC, so date.today() rolled over at 1900 Central and marked four
people away for a course that started the next morning. Every "what day is it"
question now goes through app_today(), which reads the signed-in tenant's zone.

There is no process-wide clock any more: the zone is one row per root, read
once per request into g.tz by whatever declares the tenant, so two
organisations on the same worker can be on different duty days.

Run with: python tests/test_timezone.py
"""
import os
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))
sys.path.insert(0, _HERE)
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
from flask import g  # noqa: E402

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
    source = open(os.path.join(os.path.dirname(_HERE), 'server.py')).read()
    code = [ln for ln in source.splitlines()
            if 'date.today()' in ln and not ln.lstrip().startswith('#')]
    assert not code, f'these lines still read the server clock directly: {code}'


def check_absence_activates_on_the_units_day():
    """The actual failure: an absence starting tomorrow must not be active tonight."""
    t = dbharness.make_tree('Clock Co')
    dbharness.as_user(dbharness.make_user(t['root'], 'owner', 'clockboss'))
    conn = dbharness.owner_conn()
    pid = conn.execute(
        "INSERT INTO personnel (rank, last, first, status, unit_id, root_id) "
        "VALUES ('CW2', 'Boundy', 'Ray', 'present', %s, %s) RETURNING id",
        (t['child'], t['root'])).fetchone()['id']
    tomorrow = (datetime.now(CENTRAL).date() + timedelta(days=1)).isoformat()
    conn.execute(
        'INSERT INTO scheduled_events (person_id, unit_id, root_id, status, from_date, to_date, notes, state) '
        "VALUES (%s, %s, %s, 'tdy', %s, %s, 'IO - Dothan, AL', 'scheduled')",
        (pid, t['child'], t['root'], tomorrow, tomorrow))
    conn.commit(); conn.close()

    client = server.app.test_client()
    assert client.get(f"/api/personnel?unit={t['child']}").status_code == 200

    conn = dbharness.owner_conn()
    state = conn.execute('SELECT state FROM scheduled_events WHERE person_id = %s', (pid,)).fetchone()['state']
    status = conn.execute('SELECT status FROM personnel WHERE id = %s', (pid,)).fetchone()['status']
    conn.close()
    assert state == 'scheduled', f"tomorrow's TDY activated early (state={state})"
    assert status == 'present', f'roster shows {status}; the course has not started yet'


def check_timezone_is_a_tenant_setting():
    """One zone per organisation: stored at the root, owner-only, validated."""
    t = dbharness.make_tree('Tz Co')
    other = dbharness.make_tree('Tz Other Co')
    owner = dbharness.make_user(t['root'], 'owner', 'tzboss')
    leader = dbharness.make_user(t['child'], 'leader', 'tzsarge')
    c = server.app.test_client()

    dbharness.as_user(owner)
    assert c.put(f"/api/settings?unit={t['root']}", json={'timezone': 'Europe/Berlin'}).status_code == 200
    # Stored once at the root, so every unit under it reads the same duty day.
    assert c.get(f"/api/settings?unit={t['child']}").get_json()['timezone'] == 'Europe/Berlin'
    # ...and the frontend adopts it from /api/me, which is where it now rides.
    assert c.get('/api/me').get_json()['timezone'] == 'Europe/Berlin'

    conn = dbharness.owner_conn()
    rows = conn.execute('SELECT unit_id, value FROM settings WHERE root_id = %s AND key = %s',
                        (t['root'], server.TIMEZONE_KEY)).fetchall()
    conn.close()
    assert [dict(r) for r in rows] == [{'unit_id': None, 'value': 'Europe/Berlin'}], rows

    # A leader cannot move the duty day for everyone else.
    dbharness.as_user(leader)
    assert c.put(f"/api/settings?unit={t['child']}", json={'timezone': 'UTC'}).status_code == 403
    assert c.get(f"/api/settings?unit={t['child']}").get_json()['timezone'] == 'Europe/Berlin'

    # A nonsense zone is refused and changes nothing.
    dbharness.as_user(owner)
    assert c.put(f"/api/settings?unit={t['root']}", json={'timezone': 'Mars/Olympus'}).status_code == 400
    assert c.get(f"/api/settings?unit={t['root']}").get_json()['timezone'] == 'Europe/Berlin'

    # unit_name is no longer a setting; renaming is PUT /api/units/<id>.
    assert c.put(f"/api/settings?unit={t['root']}", json={'unit_name': 'Nope'}).status_code == 400

    # The clock is PER TENANT: the other tree is untouched by any of that.
    dbharness.as_user(dbharness.make_user(other['root'], 'owner', 'tzotherboss'))
    assert c.get(f"/api/settings?unit={other['root']}").get_json()['timezone'] == 'America/Chicago'

    # A bad value already in the database falls back rather than 500ing.
    conn = dbharness.owner_conn()
    conn.execute('UPDATE settings SET value = %s WHERE root_id = %s AND key = %s',
                 ('Nowhere/Nothing', t['root'], server.TIMEZONE_KEY))
    conn.commit(); conn.close()
    dbharness.as_user(owner)
    r = c.get(f"/api/settings?unit={t['root']}")
    assert r.status_code == 200, r.get_json()
    assert r.get_json()['timezone'] == 'America/Chicago', \
        f"a bad stored zone must fall back, not raise: {r.get_json()}"


def check_auth_sync_reports_the_tenants_zone():
    """The frontend adopts the zone from the sync response, so that response
    has to carry the TENANT's zone and not the process fallback."""
    t = dbharness.make_tree('Sync Tz Co')
    conn = dbharness.owner_conn()
    conn.execute('UPDATE settings SET value = %s WHERE root_id = %s AND key = %s',
                 ('Europe/Berlin', t['root'], server.TIMEZONE_KEY))
    conn.commit(); conn.close()
    u = dbharness.make_user(t['root'], 'owner', 'synctzboss')

    with server.app.test_request_context('/api/auth/sync', method='POST'):
        g.auth_claims = {'sub': u['clerk_user_id']}
        user, err = server.sync_clerk_user({'username': u['username'], 'email': u['email'],
                                            'full_name': 'T'})
        assert err is None, err
        assert server.app_timezone() == 'Europe/Berlin', server.app_timezone()
        payload = server._user_json(server.get_db(), user)
        g.db_commit = True
        server._close_db(None)
    assert payload['timezone'] == 'Europe/Berlin', payload


def check_invite_acceptance_uses_the_tenants_clock():
    """create_invite() writes expires_at with app_stamp(), i.e. on the tenant's
    clock. sync_clerk_user() therefore has to judge that expiry, and write
    accepted_at, on the same clock — not on the pre-tenant fallback."""
    t = dbharness.make_tree('Invite Tz Co')
    berlin_now = datetime.now(ZoneInfo('Europe/Berlin'))

    def mint(token, expires_at):
        conn = dbharness.owner_conn()
        conn.execute('UPDATE settings SET value = %s WHERE root_id = %s AND key = %s',
                     ('Europe/Berlin', t['root'], server.TIMEZONE_KEY))
        conn.execute(
            'INSERT INTO invites (token, label, unit_id, role, root_id, created_by, expires_at, created_at) '
            'VALUES (%s, %s, %s, %s, %s, %s, %s, %s)',
            (token, 'PSG', t['child'], 'leader', t['root'], 'boss',
             expires_at.strftime('%Y-%m-%d %H:%M:%S'), berlin_now.strftime('%Y-%m-%d %H:%M:%S')))
        conn.commit(); conn.close()

    def sync(clerk_id, token):
        with server.app.test_request_context('/api/auth/sync', method='POST'):
            g.auth_claims = {'sub': clerk_id}
            user, err = server.sync_clerk_user({'username': clerk_id, 'email': f'{clerk_id}@example.com',
                                                'full_name': 'T', 'invite_token': token})
            assert err is None, err
            g.db_commit = True
            server._close_db(None)
        return user

    mint('tz-invite', berlin_now + timedelta(days=1))
    user = sync('tzinvitee', 'tz-invite')
    assert user['unit_id'] == t['child'], user

    conn = dbharness.owner_conn()
    accepted = conn.execute(
        "SELECT accepted_at FROM invites WHERE token = 'tz-invite'").fetchone()['accepted_at']
    conn.close()
    stamped = datetime.strptime(accepted, '%Y-%m-%d %H:%M:%S')
    expected = datetime.now(ZoneInfo('Europe/Berlin')).replace(tzinfo=None)
    assert abs((stamped - expected).total_seconds()) < 120, \
        f'accepted_at {accepted} is not the tenant (Berlin) clock; expected about {expected}'
    # ...and demonstrably not the fallback zone, which is 7h behind Berlin.
    chicago = datetime.now(CENTRAL).replace(tzinfo=None)
    assert abs((stamped - chicago).total_seconds()) > 3000, \
        f'accepted_at {accepted} is still on the fallback (Chicago) clock'

    # An invite already expired on the TENANT's clock, but not yet on the
    # fallback's (Chicago is hours behind Berlin), must not attach anyone.
    # Only the re-check after set_tenant can catch this one.
    mint('tz-stale', datetime.now(ZoneInfo('Europe/Berlin')) - timedelta(hours=1))
    stale = sync('tzstale', 'tz-stale')
    assert stale['unit_id'] is None, \
        'an invite already expired on the tenant clock must not attach a user'
    conn = dbharness.owner_conn()
    row = conn.execute("SELECT accepted_at FROM invites WHERE token = 'tz-stale'").fetchone()
    conn.close()
    assert row['accepted_at'] == '', 'a refused invite must not be marked accepted'


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
    t = dbharness.make_tree('Stamp Co')
    dbharness.as_user(dbharness.make_user(t['root'], 'owner', 'stampboss'))
    c = server.app.test_client()

    r = c.post('/api/reports', json={'unit_id': t['child'], 'unit_name': 'Alpha', 'text': 'tz probe'})
    assert r.status_code == 201, r.get_json()
    _assert_unit_clock(r.get_json()['created_at'], 'report_history.created_at')

    conn = dbharness.owner_conn()
    person_id = conn.execute(
        "INSERT INTO personnel (rank, last, first, unit_id, root_id) "
        "VALUES ('SGT', 'Tzprobe', 'Sam', %s, %s) RETURNING id",
        (t['child'], t['root'])).fetchone()['id']
    conn.commit(); conn.close()

    # Far-future window: it stays 'scheduled' and cannot disturb the absence
    # checks that run before this one.
    r = c.post(f'/api/personnel/{person_id}/schedule',
               json={'status': 'leave', 'from_date': '2099-01-01', 'to_date': '2099-01-05'})
    assert r.status_code == 201, r.get_json()
    _assert_unit_clock(r.get_json()['created_at'], 'scheduled_events.created_at')


def check_config_no_longer_publishes_a_timezone():
    """There is no one clock to publish before sign-in. The zone arrives with
    the user, on /api/auth/sync and /api/me."""
    payload = server.app.test_client().get('/api/auth/config').get_json()
    assert 'timezone' not in payload, payload


def main():
    try:
        check_today_is_the_units_day()
        check_stamp_is_the_units_clock()
        check_no_raw_date_today_remains()
        check_absence_activates_on_the_units_day()
        check_timezone_is_a_tenant_setting()
        check_auth_sync_reports_the_tenants_zone()
        check_invite_acceptance_uses_the_tenants_clock()
        check_stored_timestamps_use_the_units_clock()
        check_config_no_longer_publishes_a_timezone()
        print('ok')
    finally:
        dbharness.teardown(_schema)


if __name__ == '__main__':
    main()
