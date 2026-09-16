"""Availability answers "who is free on date X" from scheduled_events.

personnel.status is a display cache of *today* and cannot answer a question
about next Tuesday, so /api/availability reads the events table and decides on
the dates alone — the same open-ended bounds _derive_state() uses.

Run with: python tests/test_availability.py
"""
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dbharness  # noqa: E402
_schema = dbharness.setup()

import server  # noqa: E402  (must follow the DATA_DIR override)

# One tenant, one platoon inside it, plus a sibling platoon under the same
# root. "Another unit's absences stay out of it" is the same question the old
# two-platoon fixture asked; the subtree is what answers it now.
T = dbharness.make_tree()
OWNER = dbharness.make_user(T['root'], 'owner', 'boss')

# The app answers on the unit's clock (server.app_today()), so the tests
# must ask the same question. Using date.today() here made CI fail on its
# UTC runner every evening between 1900 and midnight Central.
TODAY = date.fromisoformat(server.app_today())


def day(offset):
    return (TODAY + timedelta(days=offset)).isoformat()


def add_person(rank, last, first, unit_id=None, status='present'):
    conn = dbharness.owner_conn()
    row = conn.execute(
        'INSERT INTO personnel (rank, last, first, status, unit_id, root_id) '
        'VALUES (%s, %s, %s, %s, %s, %s) RETURNING id',
        (rank, last, first, status, unit_id or T['child'], T['root'])).fetchone()
    conn.commit()
    conn.close()
    return row['id']


P1 = P2 = P3 = SIBLING = None


def setup():
    global P1, P2, P3, SIBLING
    dbharness.as_user(OWNER)
    conn = dbharness.owner_conn()
    conn.execute('DELETE FROM personnel')
    conn.execute('DELETE FROM scheduled_events')
    conn.commit()
    conn.close()
    c = server.app.test_client()
    r = c.post('/api/units', json={'parent_id': T['root'], 'kind': 'platoon', 'name': '1st Platoon'})
    assert r.status_code == 201, r.get_json()
    SIBLING = r.get_json()['id']
    P1 = add_person('SGT', 'Alvarez', 'Dana')
    P2 = add_person('SPC', 'Boone', 'Rae')
    # Another unit's soldier must never show up in this platoon's answer.
    P3 = add_person('PFC', 'Crane', 'Lee', unit_id=SIBLING)
    return c


def clear():
    conn = dbharness.owner_conn()
    conn.execute('DELETE FROM scheduled_events')
    conn.execute("UPDATE personnel SET status='present', from_date='', to_date='', notes=''")
    conn.commit()
    conn.close()


def add_event(person_id, from_off, to_off, status='tdy', state='scheduled', notes='', unit_id=None):
    """from_off/to_off of None means that bound is open."""
    conn = dbharness.owner_conn()
    cur = conn.execute(
        'INSERT INTO scheduled_events (person_id, unit_id, root_id, status, from_date, to_date, notes, state) '
        'VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id',
        (person_id, unit_id or T['child'], T['root'], status,
         '' if from_off is None else day(from_off),
         '' if to_off is None else day(to_off),
         notes, state)
    )
    event_id = cur.fetchone()['id']
    conn.commit()
    conn.close()
    return event_id


def ask(c, offset, to_offset=None, unit=None):
    path = f'/api/availability?unit={unit or T["child"]}&date={day(offset)}'
    if to_offset is not None:
        path += f'&to={day(to_offset)}'
    r = c.get(path)
    assert r.status_code == 200, (r.status_code, r.get_json())
    return r.get_json()


def free(data):
    return sorted(p['id'] for p in data['available'])


def out(data):
    return sorted(p['id'] for p in data['unavailable'])


def reason(data, person_id):
    return next(p for p in data['unavailable'] if p['id'] == person_id)


def main():
    c = setup()
    both = sorted([P1, P2])

    # ── One closed window: the boundaries are what get this wrong ────────────
    clear()
    add_event(P1, 5, 9, status='leave', notes='Block leave')

    assert free(ask(c, 4)) == both, 'the day before an absence starts, everyone is free'
    assert out(ask(c, 5)) == [P1], 'the first day counts'
    assert out(ask(c, 7)) == [P1], 'a middle day counts'
    assert out(ask(c, 9)) == [P1], 'the last day counts'
    assert free(ask(c, 10)) == both, 'the day after, the soldier is back'

    why = reason(ask(c, 7), P1)
    assert (why['status'], why['from_date'], why['to_date'], why['notes']) == \
        ('leave', day(5), day(9), 'Block leave'), why
    assert why['days'] == [day(7)] and why['whole_range'] is True, why
    assert (why['rank'], why['last'], why['first']) == ('SGT', 'Alvarez', 'Dana'), why

    # ── Open-ended absences ──────────────────────────────────────────────────
    clear()
    add_event(P1, 3, None)                     # started, no end date
    assert free(ask(c, 2)) == both, 'an open-ended absence still has a start'
    assert out(ask(c, 3)) == [P1]
    assert out(ask(c, 400)) == [P1], 'no to_date means it never ends on its own'

    clear()
    add_event(P1, None, 3)                     # no start date: already running
    assert out(ask(c, -30)) == [P1], 'an empty from_date means already started'
    assert out(ask(c, 3)) == [P1]
    assert free(ask(c, 4)) == both

    # ── Two non-overlapping absences for one person ──────────────────────────
    clear()
    add_event(P1, 2, 4, status='tdy', notes='Sim - Dothan')
    add_event(P1, 10, 12, status='pass', notes='Long weekend')
    assert free(ask(c, 6)) == both, 'the gap between two absences is free'
    assert reason(ask(c, 3), P1)['status'] == 'tdy'
    assert reason(ask(c, 11), P1)['status'] == 'pass', 'the window covering the day supplies the reason'

    # A range spanning both reports every day either one covers, and says the
    # soldier is not out for the whole range.
    span = ask(c, 2, 12)
    why = reason(span, P1)
    assert why['days'] == [day(2), day(3), day(4), day(10), day(11), day(12)], why['days']
    assert why['whole_range'] is False, why
    assert span['span'] == 11 and free(span) == [P2], span

    # ── Ranges ───────────────────────────────────────────────────────────────
    clear()
    add_event(P1, 8, 8)
    assert out(ask(c, 6, 7)) == [], 'a range that misses the absence leaves everyone free'
    assert out(ask(c, 6, 8)) == [P1], 'unavailable on any one day of the range counts'
    assert reason(ask(c, 6, 10), P1)['days'] == [day(8)]

    # ── State never overrides the dates ──────────────────────────────────────
    # A completed row whose window still covers the day asked about is a real
    # booking that was never cancelled, so it must not silently vanish.
    clear()
    add_event(P1, 2, 6, state='completed')
    assert out(ask(c, 4)) == [P1], 'a completed row covering the day still counts'
    # ...and a row that genuinely finished cannot cover a future day anyway.
    clear()
    add_event(P1, -9, -2, state='completed')
    assert free(ask(c, 0)) == both, 'a finished absence does not follow the soldier around'
    # An active row is treated no differently from a scheduled one.
    clear()
    add_event(P1, -1, 3, state='active')
    assert out(ask(c, 2)) == [P1]

    # ── personnel.status is not consulted ────────────────────────────────────
    # A stale display cache (no event backing it) must not make someone look
    # unavailable next week, and a hand-set 'present' must not hide a booking.
    clear()
    conn = dbharness.owner_conn()
    conn.execute("UPDATE personnel SET status='tdy', from_date=%s, to_date=%s WHERE id = %s",
                 (day(-4), day(-1), P2))
    conn.commit()
    conn.close()
    assert free(ask(c, 5)) == both, 'the display cache of today must not answer for next week'
    clear()

    # ── Scope and input validation ───────────────────────────────────────────
    clear()
    add_event(P3, 0, 2, unit_id=SIBLING)
    data = ask(c, 1)
    assert free(data) == both and out(data) == [], "another unit's absences stay out of it"
    assert free(ask(c, 1, unit=SIBLING)) == [] and out(ask(c, 1, unit=SIBLING)) == [P3]

    # A unit the caller cannot see is not an answer at all.
    leader = dbharness.make_user(T['child'], 'leader', 'sarge')
    dbharness.as_user(leader)
    assert c.get(f'/api/availability?unit={SIBLING}&date={day(1)}').status_code == 403
    dbharness.as_user(OWNER)

    assert c.get(f'/api/availability?unit={T["child"]}&date=next-tuesday').status_code == 400
    assert c.get(f'/api/availability?unit={T["child"]}&date={day(5)}&to={day(1)}').status_code == 400
    assert c.get(f'/api/availability?unit={T["child"]}&date={day(0)}&to={day(400)}').status_code == 400
    # No date at all means today.
    assert c.get(f'/api/availability?unit={T["child"]}').get_json()['date'] == day(0)

    print('ok')
    dbharness.teardown(_schema)


if __name__ == '__main__':
    main()
