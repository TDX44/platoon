"""Editing an absence must re-derive its state from the new dates.

_reconcile_absences only advances forward, so an edit that pushes an active
absence into the future has to undo the activation itself.

Run with: python tests/test_schedule_edit.py
"""
import os
import sys
import tempfile
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ['DATA_DIR'] = tempfile.mkdtemp()

import server  # noqa: E402  (must follow the DATA_DIR override)

TODAY = date.today()
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


def person():
    conn = server.get_db()
    row = dict(conn.execute('SELECT status, from_date, to_date FROM personnel WHERE id = 1').fetchone())
    conn.close()
    return row


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

    print('ok')


if __name__ == '__main__':
    main()
