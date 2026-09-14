"""The absence display cache on personnel has exactly one owner.

_sync_person_status() re-derives every live scheduled_events row from its dates
(in both directions) and writes personnel.status/from_date/to_date/notes from
the one absence that is current. Every write path delegates to it.

Run with: python tests/test_schedule_edit.py
"""
import os
import sys
import tempfile
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ['DATA_DIR'] = tempfile.mkdtemp()

import server  # noqa: E402  (must follow the DATA_DIR override)

# The app answers on the unit's clock (server.app_today()), so the tests
# must ask the same question. Using date.today() here made CI fail on its
# UTC runner every evening between 1900 and midnight Central.
TODAY = date.fromisoformat(server.app_today())
def day(offset):
    return (TODAY + timedelta(days=offset)).isoformat()


def setup():
    server.get_current_user = lambda: {'is_admin': 1, 'id': 1, 'username': 'boss', 'platoons': '*'}
    conn = server.get_db()
    conn.execute("DELETE FROM personnel")
    conn.execute("INSERT INTO personnel (id, rank, last, first, status, platoon) "
                 "VALUES (1, 'SGT', 'Alvarez', 'Dana', 'present', '2nd')")
    conn.commit()
    conn.close()
    return server.app.test_client()


def add_event(state, from_off, to_off, status='tdy'):
    conn = server.get_db()
    cur = conn.execute(
        'INSERT INTO scheduled_events (person_id, platoon, status, from_date, to_date, notes, state) '
        "VALUES (1, '2nd', ?, ?, ?, 'orig', ?)",
        (status, day(from_off), day(to_off), state)
    )
    if state == 'active':
        conn.execute('UPDATE personnel SET status=?, from_date=?, to_date=?, notes=? WHERE id=1',
                     (status, day(from_off), day(to_off), 'orig'))
    conn.commit()
    event_id = cur.lastrowid
    conn.close()
    return event_id


def person(pid=1):
    conn = server.get_db()
    row = dict(conn.execute(
        'SELECT status, from_date, to_date FROM personnel WHERE id = ?', (pid,)).fetchone())
    conn.close()
    return row


def clear():
    """Fresh slate: no events, person 1 back on duty."""
    conn = server.get_db()
    conn.execute('DELETE FROM scheduled_events')
    conn.execute("UPDATE personnel SET status='present', from_date='', to_date='', notes=''")
    conn.commit()
    conn.close()


def states():
    conn = server.get_db()
    rows = [tuple(r) for r in conn.execute(
        'SELECT id, state FROM scheduled_events WHERE person_id = 1 ORDER BY id')]
    conn.close()
    return rows


def event(event_id):
    conn = server.get_db()
    row = dict(conn.execute('SELECT * FROM scheduled_events WHERE id = ?', (event_id,)).fetchone())
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
    assert c.get('/api/personnel?platoon=2nd').status_code == 200
    assert event(eid)['state'] == 'active', 'from_date == today must activate'
    assert person() == {'status': 'leave', 'from_date': day(0), 'to_date': day(4)}, person()

    # 7. An active absence completes the day after it ends, and the soldier returns.
    clear()
    eid = add_event('active', -6, -1)
    assert person()['status'] == 'tdy', 'precondition: the soldier is away'
    c.get('/api/personnel?platoon=2nd')
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
    c.get('/api/personnel?platoon=2nd')
    assert event(eid)['state'] == 'scheduled'
    assert person()['status'] == 'present'

    # 9. Two overlapping absences resolve to exactly one active row.
    clear()
    old = add_event('active', -5, 5)
    new = add_event('active', -1, 7, status='leave')
    c.get('/api/personnel?platoon=2nd')
    assert states() == [(old, 'completed'), (new, 'active')], states()
    assert person() == {'status': 'leave', 'from_date': day(-1), 'to_date': day(7)}, person()

    # 10. Deleting an in-progress absence returns the soldier to duty.
    clear()
    eid = add_event('active', -1, 5)
    assert c.delete(f'/api/schedules/{eid}').status_code == 200
    assert person() == {'status': 'present', 'from_date': '', 'to_date': ''}, person()

    # 11. 'loan' has no dates and is never a scheduled event: reconciliation
    #     must not touch it, even when a stale absence row expires under it.
    clear()
    conn = server.get_db()
    conn.execute("INSERT OR REPLACE INTO personnel (id, rank, last, first, status, notes, platoon) "
                 "VALUES (2, 'SPC', 'Boone', 'Rae', 'loan', 'S2 NCOIC', '2nd')")
    conn.execute("INSERT INTO scheduled_events (person_id, platoon, status, from_date, to_date, state) "
                 "VALUES (2, '2nd', 'tdy', ?, ?, 'active')", (day(-9), day(-1)))
    conn.commit()
    conn.close()
    c.get('/api/personnel?platoon=2nd')
    assert person(2)['status'] == 'loan', 'a loaned soldier must never be reconciled to present'

    # 12. A new absence booked for today activates on creation.
    clear()
    r = c.post('/api/personnel/1/schedule',
               json={'status': 'pass', 'from_date': day(0), 'to_date': day(1), 'notes': 'p'})
    assert r.status_code == 201, r.get_json()
    assert r.get_json()['state'] == 'active', r.get_json()
    assert person() == {'status': 'pass', 'from_date': day(0), 'to_date': day(1)}, person()
    # ...and one booked for later does not.
    clear()
    r = c.post('/api/personnel/1/schedule',
               json={'status': 'pass', 'from_date': day(4), 'to_date': day(6)})
    assert r.get_json()['state'] == 'scheduled', r.get_json()
    assert person()['status'] == 'present', person()

    # 13. A double-tapped Save must not book the same absence twice.
    clear()
    body = {'status': 'tdy', 'from_date': day(2), 'to_date': day(5), 'notes': 'Sim - Dothan'}
    first = c.post('/api/personnel/1/schedule', json=body)
    second = c.post('/api/personnel/1/schedule', json=body)
    assert first.status_code == 201 and second.status_code == 200, (first.status_code, second.status_code)
    assert first.get_json()['id'] == second.get_json()['id'], 'the retry got a second row'
    conn = server.get_db()
    n = conn.execute('SELECT COUNT(*) FROM scheduled_events WHERE person_id = 1').fetchone()[0]
    conn.close()
    assert n == 1, f'expected one absence row, found {n}'
    # A genuinely different window is still a new absence.
    assert c.post('/api/personnel/1/schedule',
                  json={**body, 'to_date': day(6)}).status_code == 201

    # 14. Marking a soldier present ends the absence they were on, instead of
    #     leaving it running underneath a 'present' roster line.
    clear()
    eid = add_event('active', -3, 4, status='leave')
    assert c.put('/api/personnel/1', json={'status': 'present', 'notes': '',
                                           'from_date': '', 'to_date': ''}).status_code == 200
    conn = server.get_db()
    row = dict(conn.execute('SELECT state, to_date FROM scheduled_events WHERE id = ?', (eid,)).fetchone())
    conn.close()
    assert row == {'state': 'completed', 'to_date': day(-1)}, row
    c.get('/api/personnel?platoon=2nd')          # reconcile must leave them present
    assert person() == {'status': 'present', 'from_date': '', 'to_date': ''}, person()

    # An absence marked present before it began was a mis-entry: it goes.
    clear()
    eid = add_event('active', 0, 4, status='tdy')
    c.put('/api/personnel/1', json={'status': 'present'})
    conn = server.get_db()
    gone = conn.execute('SELECT COUNT(*) FROM scheduled_events WHERE id = ?', (eid,)).fetchone()[0]
    conn.close()
    assert gone == 0, 'an absence that never started should be removed, not kept'

    # Marking present for the day does NOT end a running absence — apiUpdate()
    # resends the current status, so this must stay a no-op on the event.
    clear()
    eid = add_event('active', -2, 6, status='tdy')
    c.put('/api/personnel/1', json={'status': 'tdy', 'present_date': day(0)})
    conn = server.get_db()
    state = conn.execute('SELECT state FROM scheduled_events WHERE id = ?', (eid,)).fetchone()[0]
    conn.close()
    assert state == 'active', 'present-for-today must not close a running absence'

    print('ok')


if __name__ == '__main__':
    main()
