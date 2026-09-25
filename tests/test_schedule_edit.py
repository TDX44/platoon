"""The absence display cache on personnel has exactly one owner.

_sync_person_status() re-derives every live scheduled_events row from its dates
(in both directions) and writes personnel.status/from_date/to_date/notes from
the one absence that is current. Every write path delegates to it.

Run with: python tests/test_schedule_edit.py
"""
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dbharness  # noqa: E402
_schema = dbharness.setup()

import server  # noqa: E402  (must follow the DATA_DIR override)

# One tenant, one platoon inside it. OWNER sees the whole tree; LEADER sees
# only the platoon, which is what the subtree scoping has to prove.
T = dbharness.make_tree()
OWNER = dbharness.make_user(T['root'], 'owner', 'boss')
LEADER = dbharness.make_user(T['child'], 'leader', 'sarge')

# The app answers on the unit's clock (server.app_today()), so the tests
# must ask the same question. Using date.today() here made CI fail on its
# UTC runner every evening between 1900 and midnight Central.
TODAY = date.fromisoformat(server.app_today())
def day(offset):
    return (TODAY + timedelta(days=offset)).isoformat()


ROSTER = f"/api/personnel?unit={T['child']}"


def add_person(rank, last, first, unit_id=None, status='present'):
    conn = dbharness.owner_conn()
    row = conn.execute(
        'INSERT INTO personnel (rank, last, first, status, unit_id, root_id) '
        'VALUES (%s, %s, %s, %s, %s, %s) RETURNING id',
        (rank, last, first, status, unit_id or T['child'], T['root'])).fetchone()
    conn.commit(); conn.close()
    return row['id']


PID = None


def setup():
    global PID
    dbharness.as_user(OWNER)
    conn = dbharness.owner_conn()
    conn.execute('DELETE FROM personnel')
    conn.commit(); conn.close()
    PID = add_person('SGT', 'Alvarez', 'Dana')
    return server.app.test_client()


def add_event(state, from_off, to_off, status='tdy'):
    conn = dbharness.owner_conn()
    cur = conn.execute(
        'INSERT INTO scheduled_events (person_id, unit_id, root_id, status, from_date, to_date, notes, state) '
        "VALUES (%s, %s, %s, %s, %s, %s, 'orig', %s) RETURNING id",
        (PID, T['child'], T['root'], status, day(from_off), day(to_off), state)
    )
    event_id = cur.fetchone()['id']
    if state == 'active':
        conn.execute('UPDATE personnel SET status=%s, from_date=%s, to_date=%s, notes=%s WHERE id=%s',
                     (status, day(from_off), day(to_off), 'orig', PID))
    conn.commit()
    conn.close()
    return event_id


def person(pid=None):
    conn = dbharness.owner_conn()
    row = dict(conn.execute(
        'SELECT status, from_date, to_date FROM personnel WHERE id = %s', (pid or PID,)).fetchone())
    conn.close()
    return row


def clear():
    """Fresh slate: no events, the soldier back on duty."""
    conn = dbharness.owner_conn()
    conn.execute('DELETE FROM scheduled_events')
    conn.execute("UPDATE personnel SET status='present', from_date='', to_date='', notes=''")
    conn.commit()
    conn.close()


def states():
    conn = dbharness.owner_conn()
    rows = [(r['id'], r['state']) for r in conn.execute(
        'SELECT id, state FROM scheduled_events WHERE person_id = %s ORDER BY id', (PID,))]
    conn.close()
    return rows


def event(event_id):
    conn = dbharness.owner_conn()
    row = dict(conn.execute('SELECT * FROM scheduled_events WHERE id = %s', (event_id,)).fetchone())
    conn.close()
    return row


def main():
    c = setup()

    # 1. Editing an upcoming absence keeps it upcoming.
    eid = add_event('scheduled', 10, 20)
    r = c.put(f'/api/schedules/{eid}', json={'status': 'leave', 'from_date': day(12),
                                             'to_date': day(18), 'notes': 'moved', 'location': 'Dallas TX'})
    assert r.status_code == 200, r.get_json()
    ev = event(eid)
    assert (ev['status'], ev['from_date'], ev['notes'], ev['state']) == ('leave', day(12), 'moved', 'scheduled'), ev
    assert ev['location'] == 'Dallas TX', ev
    assert person()['status'] == 'present', 'an upcoming absence must not mark the soldier away'

    # 2. Pushing an ACTIVE absence into the future returns the soldier to duty.
    eid = add_event('active', 0, 5)
    assert person()['status'] == 'tdy', 'precondition: the soldier is away'
    r = c.put(f'/api/schedules/{eid}', json={'status': 'tdy', 'from_date': day(7), 'to_date': day(12)})
    assert r.status_code == 200, r.get_json()
    assert event(eid)['state'] == 'scheduled', 'a future window must go back to scheduled'
    assert person() == {'status': 'present', 'from_date': '', 'to_date': ''}, person()

    # 3. Editing an absence that is still current keeps the roster in step.
    eid = add_event('active', -2, 5)
    r = c.put(f'/api/schedules/{eid}', json={'status': 'leave', 'from_date': day(-2),
                                             'to_date': day(9), 'notes': 'extended'})
    assert r.status_code == 200, r.get_json()
    assert event(eid)['state'] == 'active'
    assert person() == {'status': 'leave', 'from_date': day(-2), 'to_date': day(9)}, person()

    # 4. A finished absence is history and is not editable.
    eid = add_event('completed', -20, -10)
    r = c.put(f'/api/schedules/{eid}', json={'status': 'tdy', 'from_date': day(1), 'to_date': day(3)})
    assert r.status_code == 400, r.status_code
    assert event(eid)['from_date'] == day(-20), 'a rejected edit must not write anything'

    # 5. Garbage status is rejected.
    eid = add_event('scheduled', 3, 6)
    assert c.put(f'/api/schedules/{eid}', json={'status': 'vacation'}).status_code == 400
    assert c.put('/api/schedules/999999', json={'status': 'tdy'}).status_code == 404

    # ── The cache is derived, never hand-written ──────────────────────────────

    # 6. A scheduled absence activates the day it starts and fills the cache.
    clear()
    eid = add_event('scheduled', 0, 4, status='leave')
    assert c.get(ROSTER).status_code == 200
    assert event(eid)['state'] == 'active', 'from_date == today must activate'
    assert person() == {'status': 'leave', 'from_date': day(0), 'to_date': day(4)}, person()

    # 7. An active absence completes the day after it ends, and the soldier returns.
    clear()
    eid = add_event('active', -6, -1)
    assert person()['status'] == 'tdy', 'precondition: the soldier is away'
    c.get(ROSTER)
    assert event(eid)['state'] == 'completed', 'a past to_date must complete'
    assert person() == {'status': 'present', 'from_date': '', 'to_date': ''}, person()

    # 8. Editing an active absence into the future demotes it AND clears the cache
    #    (reconciliation alone only ever moves forward, so this is the regression
    #    the single owner exists to prevent).
    clear()
    eid = add_event('active', -1, 4)
    r = c.put(f'/api/schedules/{eid}', json={'status': 'tdy', 'from_date': day(6), 'to_date': day(9)})
    assert r.status_code == 200, r.get_json()
    assert event(eid)['state'] == 'scheduled', event(eid)
    assert person() == {'status': 'present', 'from_date': '', 'to_date': ''}, person()
    # ...and a later read must not silently re-activate it.
    c.get(ROSTER)
    assert event(eid)['state'] == 'scheduled'
    assert person()['status'] == 'present'

    # 9. Two overlapping absences resolve to exactly one active row.
    clear()
    old = add_event('active', -5, 5)
    new = add_event('active', -1, 7, status='leave')
    c.get(ROSTER)
    assert states() == [(old, 'completed'), (new, 'active')], states()
    assert person() == {'status': 'leave', 'from_date': day(-1), 'to_date': day(7)}, person()

    # 10. Deleting an in-progress absence returns the soldier to duty.
    clear()
    eid = add_event('active', -1, 5)
    assert c.delete(f'/api/schedules/{eid}').status_code == 200
    assert person() == {'status': 'present', 'from_date': '', 'to_date': ''}, person()

    # 12. A new absence booked for today activates on creation.
    clear()
    r = c.post(f'/api/personnel/{PID}/schedule',
               json={'status': 'pass', 'from_date': day(0), 'to_date': day(1), 'notes': 'p'})
    assert r.status_code == 201, r.get_json()
    assert r.get_json()['state'] == 'active', r.get_json()
    assert person() == {'status': 'pass', 'from_date': day(0), 'to_date': day(1)}, person()
    # ...and one booked for later does not.
    clear()
    r = c.post(f'/api/personnel/{PID}/schedule',
               json={'status': 'pass', 'from_date': day(4), 'to_date': day(6)})
    assert r.get_json()['state'] == 'scheduled', r.get_json()
    assert person()['status'] == 'present', person()

    # 13. A double-tapped Save must not book the same absence twice.
    clear()
    body = {'status': 'tdy', 'from_date': day(2), 'to_date': day(5), 'notes': 'Sim - Dothan'}
    first = c.post(f'/api/personnel/{PID}/schedule', json=body)
    second = c.post(f'/api/personnel/{PID}/schedule', json=body)
    assert first.status_code == 201 and second.status_code == 200, (first.status_code, second.status_code)
    assert first.get_json()['id'] == second.get_json()['id'], 'the retry got a second row'
    conn = dbharness.owner_conn()
    n = conn.execute('SELECT COUNT(*) AS n FROM scheduled_events WHERE person_id = %s',
                     (PID,)).fetchone()['n']
    conn.close()
    assert n == 1, f'expected one absence row, found {n}'
    # A genuinely different window is still a new absence.
    assert c.post(f'/api/personnel/{PID}/schedule',
                  json={**body, 'to_date': day(6)}).status_code == 201

    # 14. Marking a soldier present ends the absence they were on, instead of
    #     leaving it running underneath a 'present' roster line.
    clear()
    eid = add_event('active', -3, 4, status='leave')
    assert c.put(f'/api/personnel/{PID}', json={'status': 'present', 'notes': '',
                                                'from_date': '', 'to_date': ''}).status_code == 200
    conn = dbharness.owner_conn()
    row = dict(conn.execute('SELECT state, to_date FROM scheduled_events WHERE id = %s', (eid,)).fetchone())
    conn.close()
    assert row == {'state': 'completed', 'to_date': day(-1)}, row
    c.get(ROSTER)          # reconcile must leave them present
    assert person() == {'status': 'present', 'from_date': '', 'to_date': ''}, person()

    # An absence that started today and is ended today is still something that
    # happened -- every late and excused is one. It stays as history, ending
    # today rather than yesterday (which would put it before its own start).
    clear()
    eid = add_event('active', 0, 4, status='tdy')
    assert c.put(f'/api/personnel/{PID}', json={'status': 'present'}).status_code == 200
    ev = event(eid)
    assert (ev['state'], ev['from_date'], ev['to_date']) == ('completed', day(0), day(0)), ev
    c.get(ROSTER)          # completed is terminal: reconcile must not re-activate it
    assert event(eid)['state'] == 'completed', event(eid)
    assert person() == {'status': 'present', 'from_date': '', 'to_date': ''}, person()
    for status in ('late', 'excused'):
        clear()
        r = c.post(f'/api/personnel/{PID}/schedule',
                   json={'status': status, 'from_date': day(0), 'to_date': day(0), 'notes': 'why'})
        eid = r.get_json()['id']
        assert c.put(f'/api/personnel/{PID}', json={'status': 'present'}).status_code == 200
        assert event(eid)['state'] == 'completed', (status, event(eid))
        conn = dbharness.owner_conn()
        notes = conn.execute('SELECT notes FROM personnel WHERE id = %s', (PID,)).fetchone()['notes']
        conn.close()
        assert notes == '', f'the {status} reason stayed on a present soldier: {notes!r}'

    # One that had not begun yet was a mis-entry: it goes.
    clear()
    eid = add_event('active', 1, 4, status='tdy')
    c.put(f'/api/personnel/{PID}', json={'status': 'present'})
    conn = dbharness.owner_conn()
    gone = conn.execute('SELECT COUNT(*) AS n FROM scheduled_events WHERE id = %s', (eid,)).fetchone()['n']
    conn.close()
    assert gone == 0, 'an absence that never started should be removed, not kept'

    # PUT /api/personnel/<id> is not a way to book an absence, or to write
    # junk into the status column. Only 'present' or the status it already
    # has (apiUpdate() resends it on every save) gets through.
    clear()
    for bad in ('leave', 'x"><img src=x>', 'unaccounted'):
        r = c.put(f'/api/personnel/{PID}', json={'status': bad})
        assert r.status_code == 400, (bad, r.status_code)
        assert 'schedule' in r.get_json()['error'], r.get_json()
    assert person()['status'] == 'present', person()
    # ...and while an absence is running the body cannot rewrite its cache.
    eid = add_event('active', -2, 6, status='tdy')
    r = c.put(f'/api/personnel/{PID}', json={'status': 'tdy', 'notes': 'hacked',
                                             'from_date': 'x', 'to_date': 'y', 'present_date': day(0)})
    assert r.status_code == 200, r.get_json()
    assert person() == {'status': 'tdy', 'from_date': day(-2), 'to_date': day(6)}, person()
    conn = dbharness.owner_conn()
    row = conn.execute('SELECT notes, present_date FROM personnel WHERE id = %s', (PID,)).fetchone()
    conn.close()
    assert (row['notes'], row['present_date']) == ('orig', day(0)), row
    # Non-text names are a 400, not a 500 on .strip().
    for body in ({'rank': 5}, {'last': ['a']}, {'first': {'x': 1}}, {'notes': 7}):
        assert c.put(f'/api/personnel/{PID}', json=body).status_code == 400, body
    assert c.post('/api/personnel', json={'rank': 5, 'last': 'A', 'first': 'B',
                                          'unit_id': T['child']}).status_code == 400

    # Marking present for the day does NOT end a running absence — apiUpdate()
    # resends the current status, so this must stay a no-op on the event.
    clear()
    eid = add_event('active', -2, 6, status='tdy')
    c.put(f'/api/personnel/{PID}', json={'status': 'tdy', 'present_date': day(0)})
    conn = dbharness.owner_conn()
    state = conn.execute('SELECT state FROM scheduled_events WHERE id = %s', (eid,)).fetchone()['state']
    conn.close()
    assert state == 'active', 'present-for-today must not close a running absence'

    # 15. 'late' and 'excused' are ordinary absences: a reason in notes, a
    #     same-day window, and they complete themselves overnight like the rest.
    for status, reason in (('late', 'Traffic'), ('excused', 'Sick call')):
        clear()
        r = c.post(f'/api/personnel/{PID}/schedule',
                   json={'status': status, 'from_date': day(0), 'to_date': day(0),
                         'notes': reason})
        assert r.status_code == 201, (status, r.get_json())
        assert r.get_json()['state'] == 'active', r.get_json()
        assert person() == {'status': status, 'from_date': day(0), 'to_date': day(0)}, person()
        conn = dbharness.owner_conn()
        notes = conn.execute('SELECT notes FROM personnel WHERE id = %s', (PID,)).fetchone()['notes']
        conn.close()
        assert notes == reason, f'{status} lost its reason: {notes!r}'

        # Yesterday's lateness must not still be on the roster this morning.
        conn = dbharness.owner_conn()
        conn.execute('UPDATE scheduled_events SET from_date = %s, to_date = %s WHERE person_id = %s',
                     (day(-1), day(-1), PID))
        conn.commit()
        conn.close()
        c.get(ROSTER)
        assert person() == {'status': 'present', 'from_date': '', 'to_date': ''}, person()

    # 16. Dates are real ISO dates and a window does not end before it starts.
    clear()
    url = f'/api/personnel/{PID}/schedule'
    for body in ({'from_date': 'tomorrow'}, {'from_date': '2026-13-01'},
                 {'from_date': day(0), 'to_date': '20261001'},
                 {'from_date': day(3), 'to_date': day(1)}, {'from_date': 5}):
        r = c.post(url, json={'status': 'leave', **body})
        assert r.status_code == 400, (body, r.status_code)
    eid = add_event('scheduled', 3, 6)
    assert c.put(f'/api/schedules/{eid}', json={'status': 'tdy', 'from_date': day(6),
                                                'to_date': day(3)}).status_code == 400
    assert c.put(f'/api/schedules/{eid}', json={'status': 'tdy', 'from_date': 'x'}).status_code == 400
    assert event(eid)['from_date'] == day(3), 'a rejected edit must not write anything'
    # late/excused are same-day: an open end means today, not "until further notice".
    for status in ('late', 'excused'):
        clear()
        r = c.post(url, json={'status': status, 'from_date': day(0), 'notes': 'why'})
        assert r.status_code == 201, r.get_json()
        assert r.get_json()['to_date'] == day(0), r.get_json()
        eid = r.get_json()['id']
        r = c.put(f'/api/schedules/{eid}', json={'status': status, 'from_date': day(0), 'to_date': ''})
        assert r.get_json()['to_date'] == day(0), r.get_json()

    # 17. The double-tap guard is a unique index over LIVE rows, so two racing
    #     Saves cannot both insert, while a finished absence never blocks a new
    #     one on the same dates. init_db() replaces the old full index and
    #     dedupes live rows first, keeping the one carrying the roster.
    clear()
    conn = dbharness.owner_conn()
    conn.execute('DROP INDEX scheduled_events_live_dedupe')
    conn.execute('CREATE UNIQUE INDEX scheduled_events_dedupe '
                 'ON scheduled_events (person_id, status, from_date, to_date)')
    conn.commit(); conn.close()
    server.init_db()
    conn = dbharness.owner_conn()
    names = {r['indexname'] for r in conn.execute(
        "SELECT indexname FROM pg_indexes WHERE tablename = 'scheduled_events' "
        'AND schemaname = current_schema()').fetchall()}
    conn.close()
    assert 'scheduled_events_dedupe' not in names and 'scheduled_events_live_dedupe' in names, names

    conn = dbharness.owner_conn()
    conn.execute('DROP INDEX scheduled_events_live_dedupe')
    ids = [conn.execute(
        'INSERT INTO scheduled_events (person_id, unit_id, root_id, status, from_date, to_date, state) '
        "VALUES (%s, %s, %s, 'tdy', %s, %s, %s) RETURNING id",
        (PID, T['child'], T['root'], day(-1), day(3), st)).fetchone()['id']
        for st in ('completed', 'scheduled', 'active', 'active')]
    conn.commit(); conn.close()
    server.init_db()
    assert states() == [(ids[0], 'completed'), (ids[2], 'active')], states()
    conn = dbharness.owner_conn()
    try:
        conn.execute(
            'INSERT INTO scheduled_events (person_id, unit_id, root_id, status, from_date, to_date) '
            "VALUES (%s, %s, %s, 'tdy', %s, %s)", (PID, T['child'], T['root'], day(-1), day(3)))
        raise AssertionError('a duplicate absence got past the unique index')
    except server.psycopg.errors.UniqueViolation:
        pass
    finally:
        conn.close()
    # ...and an edit onto another row's exact window is refused, not a 500.
    other = add_event('scheduled', 5, 8)
    r = c.put(f'/api/schedules/{other}', json={'status': 'tdy', 'from_date': day(-1), 'to_date': day(3)})
    assert r.status_code == 409, r.status_code

    # late -> present -> late again, all today: the second late is a new
    # absence, not the finished one handed back.
    clear()
    body = {'status': 'late', 'from_date': day(0), 'to_date': day(0), 'notes': 'traffic'}
    first = c.post(f'/api/personnel/{PID}/schedule', json=body)
    assert first.status_code == 201, first.get_json()
    assert c.put(f'/api/personnel/{PID}', json={'status': 'present'}).status_code == 200
    again = c.post(f'/api/personnel/{PID}/schedule', json={**body, 'notes': 'left again'})
    assert again.status_code == 201, (again.status_code, again.get_json())
    assert again.get_json()['id'] != first.get_json()['id']
    assert states() == [(first.get_json()['id'], 'completed'), (again.get_json()['id'], 'active')], states()
    row = next(p for p in c.get(ROSTER).get_json() if p['id'] == PID)
    assert row['status'] == 'late' and row['notes'] == 'left again', row
    # ...and a double tap on that second late is still one row.
    assert c.post(f'/api/personnel/{PID}/schedule', json=body).status_code == 200
    assert len(states()) == 2, states()

    # 18. Every roster read reconciles, so it must only touch the people whose
    #     rows would actually change today, not the whole tenant each time.
    clear()
    add_event('active', -1, 5)
    upcoming = add_event('scheduled', 0, 3, status='leave')   # starts today: must change
    calls = []
    real_sync = server._sync_person_status
    server._sync_person_status = lambda conn, pid, today: calls.append(pid) or real_sync(conn, pid, today)
    try:
        c.get(ROSTER)
        assert calls == [PID], calls
        assert event(upcoming)['state'] == 'active', event(upcoming)
        calls.clear()
        c.get(ROSTER)
        assert calls == [], f'a settled roster was reconciled again: {calls}'
    finally:
        server._sync_person_status = real_sync

    # A status that is not a real one is still refused.
    clear()
    assert c.post(f'/api/personnel/{PID}/schedule',
                  json={'status': 'tardy', 'from_date': day(0)}).status_code == 400

    # ── Scope: a roster is one unit's subtree, never the whole tenant ─────────
    clear()
    dbharness.as_user(OWNER)
    r = c.post('/api/units', json={'parent_id': T['root'], 'kind': 'platoon', 'name': '3rd Platoon'})
    assert r.status_code == 201, r.get_json()
    sibling = r.get_json()['id']
    stranger = add_person('SPC', 'Ward', 'Kim', unit_id=sibling)

    dbharness.as_user(LEADER)
    ids = [p['id'] for p in c.get(ROSTER).get_json()]
    assert PID in ids, ids
    assert stranger not in ids, "a sibling unit's soldier leaked into the platoon roster"
    # ...and the leader cannot reach into that sibling at all.
    assert c.get(f'/api/personnel?unit={sibling}').status_code == 403
    assert c.put(f'/api/personnel/{stranger}', json={'status': 'present'}).status_code == 403

    # The owner, above both, sees the whole subtree.
    dbharness.as_user(OWNER)
    everyone = [p['id'] for p in c.get(f"/api/personnel?unit={T['root']}").get_json()]
    assert {PID, stranger} <= set(everyone), everyone

    # A soldier moved inside the caller's subtree takes their absences with them.
    eid = add_event('active', -1, 5)
    r = c.put(f'/api/personnel/{PID}', json={'unit_id': sibling})
    assert r.status_code == 200, r.get_json()
    assert event(eid)['unit_id'] == sibling, event(eid)
    dbharness.as_user(LEADER)
    assert [p['id'] for p in c.get(ROSTER).get_json()] == [], 'the moved soldier is off the old roster'
    dbharness.as_user(OWNER)
    assert c.put(f'/api/personnel/{PID}', json={'unit_id': T['child']}).status_code == 200

    print('ok')
    dbharness.teardown(_schema)


if __name__ == '__main__':
    main()
