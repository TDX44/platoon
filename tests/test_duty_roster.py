"""Duty roster ↔ absence conflicts — run with: python tests/test_duty_roster.py

The duty roster points at a real person_id, so it can ask the absence lifecycle
whether that soldier is away on the duty date. Covered here: the snapshot
fields come from the database and not the client, a soldier outside the
caller's subtree is refused, active/scheduled/open-ended/empty-bound absences
all report a conflict, a clear date reports none, the conflict warns instead of
blocking, and the legacy backfill only links unambiguous rows.
"""
import os
import sys
from datetime import date, timedelta

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))
sys.path.insert(0, _HERE)
import dbharness  # noqa: E402
_schema = dbharness.setup()

import server  # noqa: E402

# One tenant with a platoon child, plus a second tenant nobody here can see.
T = dbharness.make_tree('Duty Co')
OTHER = dbharness.make_tree('Duty Other Co')
OWNER = dbharness.make_user(T['root'], 'owner', 'dutyboss')
LEADER = dbharness.make_user(T['child'], 'leader', 'dutysarge')

# The app answers on the unit's clock (server.app_today()), so the tests
# must ask the same question. Using date.today() here made CI fail on its
# UTC runner every evening between 1900 and midnight Central.
TODAY = date.fromisoformat(server.app_today())


def day(offset):
    return (TODAY + timedelta(days=offset)).isoformat()


def setup():
    dbharness.as_user(OWNER)
    return server.app.test_client()


def add_unit(parent_id, name, slug, root_id=None):
    conn = dbharness.owner_conn()
    row = conn.execute(
        'INSERT INTO units (parent_id, root_id, kind, name, slug) VALUES (%s, %s, %s, %s, %s) RETURNING id',
        (parent_id, root_id or T['root'], 'platoon', name, slug)).fetchone()
    conn.commit(); conn.close()
    return row['id']


def add_person(rank, last, first, unit_id=None, root_id=None, platoon=None):
    conn = dbharness.owner_conn()
    row = conn.execute(
        'INSERT INTO personnel (rank, last, first, unit_id, root_id, platoon) '
        'VALUES (%s, %s, %s, %s, %s, %s) RETURNING id',
        (rank, last, first, unit_id or T['child'], root_id or T['root'], platoon)).fetchone()
    conn.commit(); conn.close()
    return row['id']


def add_absence(person_id, status, from_date, to_date, state, unit_id=None):
    conn = dbharness.owner_conn()
    conn.execute(
        'INSERT INTO scheduled_events (person_id, unit_id, root_id, status, from_date, to_date, state) '
        'VALUES (%s, %s, %s, %s, %s, %s, %s)',
        (person_id, unit_id or T['child'], T['root'], status, from_date, to_date, state))
    conn.commit(); conn.close()


def post_duty(client, person_id, date_str, unit_id=None, **extra):
    payload = {'unit_id': unit_id or T['child'], 'date': date_str,
               'duty_type': 'CQ', 'person_id': person_id}
    payload.update(extra)
    return client.post('/api/duty', json=payload)


def conflict(person_id, date_str):
    conn = dbharness.owner_conn()
    try:
        return server._duty_conflict(conn, person_id, date_str)
    finally:
        conn.close()


def check_snapshot_comes_from_db(client):
    pid = add_person('SGT', 'Alvarez', 'Dana')
    # The client sends a name too; the route must ignore it and write the
    # database's own values, so a stale or hostile client cannot forge one.
    r = post_duty(client, pid, day(0), rank='GEN', last='Forged', first='Name')
    assert r.status_code == 201, f'expected 201, got {r.status_code}: {r.get_data(as_text=True)}'
    row = r.get_json()
    assert row['person_id'] == pid, 'the duty row must link to the person'
    assert (row['rank'], row['last'], row['first']) == ('SGT', 'Alvarez', 'Dana'), \
        f'snapshot fields must come from personnel, not the client payload: {row}'
    # Filed where the soldier is, not where the caller was looking.
    assert row['unit_id'] == T['child'], row


def check_outside_the_subtree_is_refused(client):
    """A sibling unit the leader cannot see is 403; another tenant is 404."""
    sibling = add_unit(T['root'], '1st Platoon', '1stplatoon')
    outsider = add_person('SPC', 'Reyes', 'Luis', unit_id=sibling)
    # The owner is above both, so for them this is an ordinary booking.
    r = post_duty(client, outsider, day(0), unit_id=sibling)
    assert r.status_code == 201, r.get_json()
    sibling_entry = r.get_json()['id']

    # Another tenant's duty entry, written straight in: it must not exist here.
    conn = dbharness.owner_conn()
    other_entry = conn.execute(
        "INSERT INTO duty_roster (date, unit_id, root_id, duty_type, rank, last, first) "
        "VALUES (%s, %s, %s, 'CQ', 'SFC', 'Stranger', 'Sam') RETURNING id",
        (day(0), OTHER['child'], OTHER['root'])).fetchone()['id']
    conn.commit(); conn.close()

    assert client.delete(f'/api/duty/{other_entry}').status_code == 404, \
        "another tenant's duty entry does not exist from here"
    assert client.get(f"/api/duty?unit={OTHER['root']}").status_code == 403

    dbharness.as_user(LEADER)
    assert client.get(f'/api/duty?unit={sibling}').status_code == 403
    assert client.get(f'/api/duty/conflicts?unit={sibling}').status_code == 403
    assert post_duty(client, outsider, day(0), unit_id=sibling).status_code == 403, \
        'a leader cannot book onto a unit they cannot see'
    assert post_duty(client, outsider, day(0)).status_code == 400, \
        'a soldier outside the subtree must be refused'
    assert client.delete(f'/api/duty/{sibling_entry}').status_code == 403
    dbharness.as_user(OWNER)

    assert post_duty(client, 999999, day(0)).status_code == 400, 'an unknown person id must be refused'
    assert post_duty(client, None, day(0)).status_code == 400, 'a missing person id must be refused'

    conn = dbharness.owner_conn()
    left = conn.execute('SELECT COUNT(*) c FROM duty_roster WHERE person_id IS NULL '
                        'AND root_id = %s', (T['root'],)).fetchone()['c']
    conn.close()
    assert left == 0, 'a refused post must not write a row'


def check_active_absence_conflicts(client):
    pid = add_person('SPC', 'Boone', 'Chris')
    add_absence(pid, 'leave', day(-2), day(5), 'active')
    r = post_duty(client, pid, day(0))
    assert r.status_code == 201, 'a conflict must warn, never block'
    row = r.get_json()
    assert row['conflict'], 'duty inside an active absence must report a conflict'
    assert row['conflict']['status'] == 'leave'
    assert row['conflict']['label'].startswith('on leave '), row['conflict']['label']

    conn = dbharness.owner_conn()
    saved = conn.execute('SELECT * FROM duty_roster WHERE id = %s', (row['id'],)).fetchone()
    conn.close()
    assert saved is not None, 'the row must still be created despite the conflict'

    # GET reports it too, so an existing roster shows the clash on reload.
    listed = client.get(f"/api/duty?unit={T['child']}&date={day(0)}").get_json()
    mine = [e for e in listed if e['id'] == row['id']]
    assert mine and mine[0]['conflict'], 'GET /api/duty must report the conflict per row'


def check_future_scheduled_absence_conflicts(client):
    pid = add_person('PFC', 'Crane', 'Mia')
    add_absence(pid, 'tdy', day(20), day(30), 'scheduled')
    row = post_duty(client, pid, day(25)).get_json()
    assert row['conflict'], 'duty inside a future scheduled absence must report a conflict'
    assert row['conflict']['status'] == 'tdy'
    # A scheduled absence must not leak onto dates outside its window.
    assert conflict(pid, day(19)) is None, 'the day before the window is clear'
    assert conflict(pid, day(31)) is None, 'the day after the window is clear'

    # The picker feed agrees with the per-row answer.
    away = client.get(f"/api/duty/conflicts?unit={T['child']}&date={day(25)}").get_json()
    assert str(pid) in away, '/api/duty/conflicts must list a soldier away on that date'
    clear = client.get(f"/api/duty/conflicts?unit={T['child']}&date={day(19)}").get_json()
    assert str(pid) not in clear, '/api/duty/conflicts must not list them outside the window'


def check_clear_date_has_no_conflict(client):
    pid = add_person('SSG', 'Dunn', 'Rae')
    add_absence(pid, 'pass', day(40), day(41), 'scheduled')
    row = post_duty(client, pid, day(3)).get_json()
    assert row['conflict'] is None, 'a date outside every absence must be clear'


def check_bounds():
    open_ended = add_person('SGT', 'Ellis', 'Tom')
    add_absence(open_ended, 'other', day(2), '', 'scheduled')
    assert conflict(open_ended, day(1)) is None, \
        'an open-ended absence has not started the day before from_date'
    assert conflict(open_ended, day(2)), 'the start date itself is covered'
    assert conflict(open_ended, day(400)), \
        'an empty to_date means open-ended — every later date is covered'

    already_started = add_person('SPC', 'Frost', 'Jo')
    add_absence(already_started, 'ftr', '', day(3), 'active')
    assert conflict(already_started, day(-500)), 'an empty from_date means already started'
    assert conflict(already_started, day(3)), 'the end date itself is covered'
    assert conflict(already_started, day(4)) is None, 'the day after is clear'

    undated = add_person('SPC', 'Gantt', 'Ann')
    add_absence(undated, 'leave', '', '', 'active')
    assert conflict(undated, day(0)), 'an absence with no bounds covers everything'

    history = add_person('SPC', 'Hale', 'Nick')
    add_absence(history, 'leave', day(-30), day(-20), 'completed')
    assert conflict(history, day(-25)) is None, \
        "'completed' is terminal history and must not raise conflicts"

    assert conflict(None, day(0)) is None, 'an unlinked (NULL person_id) row has no conflict'


def check_matches_lifecycle():
    """The date rule must be _derive_state's, not a second interpretation."""
    conn = dbharness.owner_conn()
    rows = conn.execute("SELECT * FROM scheduled_events WHERE state != 'completed'").fetchall()
    assert rows, 'expected live absences to compare against'
    for r in rows:
        for offset in (-500, -1, 0, 1, 3, 25, 400):
            # Another of this person's rows may also cover the day, so only the
            # "covered implies conflict" direction is safe to assert row by row.
            if server._derive_state(r, day(offset)) == 'active':
                assert server._duty_conflict(conn, r['person_id'], day(offset)), \
                    f'event {r["id"]} covers {day(offset)} but no conflict was reported'
    conn.close()


def check_label():
    pid = add_person('CPL', 'Ives', 'Sam')
    add_absence(pid, 'leave', '2026-09-07', '2026-09-20', 'scheduled')
    assert conflict(pid, '2026-09-10')['label'] == 'on leave 7SEP-20SEP'

    partial = add_person('CPL', 'Judd', 'Lee')
    add_absence(partial, 'tdy', '2026-09-07', '', 'scheduled')
    assert conflict(partial, '2026-09-10')['label'] == 'on TDY from 7SEP'


def check_backfill():
    """Legacy rows carry only a name; link the unambiguous ones, guess at none.

    Pre-A1 rows have a `platoon` and no unit_id, which is exactly what the
    migration in init_db() matches on, so the fixtures here carry both.
    """
    solo = add_person('SGT', 'Unique', 'Person', platoon='2nd')
    add_person('SPC', 'Twin', 'Sam', platoon='2nd')
    add_person('SPC', 'Twin', 'Sam', platoon='2nd')

    conn = dbharness.owner_conn()
    conn.execute('ALTER TABLE duty_roster DROP COLUMN person_id')
    for rank, last, first in (('SGT', 'Unique', 'Person'), ('SPC', 'Twin', 'Sam'),
                              ('SSG', 'Deleted', 'Soldier')):
        conn.execute(
            "INSERT INTO duty_roster (date, platoon, unit_id, root_id, duty_type, rank, last, first) "
            "VALUES (%s, '2nd', %s, %s, 'CQ', %s, %s, %s)",
            (day(0), T['child'], T['root'], rank, last, first))
    conn.commit(); conn.close()

    server.init_db()  # re-adds the column and runs the backfill

    conn = dbharness.owner_conn()

    def person_id_of(last):
        return conn.execute(
            'SELECT person_id FROM duty_roster WHERE last = %s', (last,)).fetchone()['person_id']

    assert person_id_of('Unique') == solo, 'an unambiguous legacy row must link'
    assert person_id_of('Twin') is None, \
        'two soldiers share that name — the row must stay NULL rather than guess'
    assert person_id_of('Deleted') is None, 'a name that matches nobody must stay NULL'
    # Unit-model rows have no `platoon`, so the legacy matcher cannot touch them.
    assert person_id_of('Alvarez') is None, 'no platoon, no legacy match — and no guess either'
    conn.close()


def main():
    try:
        client = setup()
        check_snapshot_comes_from_db(client)
        check_outside_the_subtree_is_refused(client)
        check_active_absence_conflicts(client)
        check_future_scheduled_absence_conflicts(client)
        check_clear_date_has_no_conflict(client)
        check_bounds()
        check_matches_lifecycle()
        check_label()
        check_backfill()
        print('ok')
    finally:
        dbharness.teardown(_schema)


if __name__ == '__main__':
    main()
