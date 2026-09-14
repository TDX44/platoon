"""Availability answers "who is free on date X" from scheduled_events.

personnel.status is a display cache of *today* and cannot answer a question
about next Tuesday, so /api/availability reads the events table and decides on
the dates alone — the same open-ended bounds _derive_state() uses.

Run with: python tests/test_availability.py
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
    conn.execute('DELETE FROM personnel')
    conn.execute('DELETE FROM scheduled_events')
    conn.execute("INSERT INTO personnel (id, rank, last, first, status, platoon) "
                 "VALUES (1, 'SGT', 'Alvarez', 'Dana', 'present', '2nd')")
    conn.execute("INSERT INTO personnel (id, rank, last, first, status, platoon) "
                 "VALUES (2, 'SPC', 'Boone', 'Rae', 'present', '2nd')")
    # Another platoon's soldier must never show up in 2nd's answer.
    conn.execute("INSERT INTO personnel (id, rank, last, first, status, platoon) "
                 "VALUES (3, 'PFC', 'Crane', 'Lee', 'present', '1st')")
    conn.commit()
    conn.close()
    return server.app.test_client()


def clear():
    conn = server.get_db()
    conn.execute('DELETE FROM scheduled_events')
    conn.execute("UPDATE personnel SET status='present', from_date='', to_date='', notes=''")
    conn.commit()
    conn.close()


def add_event(person_id, from_off, to_off, status='tdy', state='scheduled', notes='', platoon='2nd'):
    """from_off/to_off of None means that bound is open."""
    conn = server.get_db()
    cur = conn.execute(
        'INSERT INTO scheduled_events (person_id, platoon, status, from_date, to_date, notes, state) '
        'VALUES (?, ?, ?, ?, ?, ?, ?)',
        (person_id, platoon, status,
         '' if from_off is None else day(from_off),
         '' if to_off is None else day(to_off),
         notes, state)
    )
    conn.commit()
    event_id = cur.lastrowid
    conn.close()
    return event_id


def ask(c, offset, to_offset=None, platoon='2nd'):
    path = f'/api/availability?platoon={platoon}&date={day(offset)}'
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

    # ── One closed window: the boundaries are what get this wrong ────────────
    clear()
    add_event(1, 5, 9, status='leave', notes='Block leave')

    assert free(ask(c, 4)) == [1, 2], 'the day before an absence starts, everyone is free'
    assert out(ask(c, 5)) == [1], 'the first day counts'
    assert out(ask(c, 7)) == [1], 'a middle day counts'
    assert out(ask(c, 9)) == [1], 'the last day counts'
    assert free(ask(c, 10)) == [1, 2], 'the day after, the soldier is back'

    why = reason(ask(c, 7), 1)
    assert (why['status'], why['from_date'], why['to_date'], why['notes']) == \
        ('leave', day(5), day(9), 'Block leave'), why
    assert why['days'] == [day(7)] and why['whole_range'] is True, why
    assert (why['rank'], why['last'], why['first']) == ('SGT', 'Alvarez', 'Dana'), why

    # ── Open-ended absences ──────────────────────────────────────────────────
    clear()
    add_event(1, 3, None)                      # started, no end date
    assert free(ask(c, 2)) == [1, 2], 'an open-ended absence still has a start'
    assert out(ask(c, 3)) == [1]
    assert out(ask(c, 400)) == [1], 'no to_date means it never ends on its own'

    clear()
    add_event(1, None, 3)                      # no start date: already running
    assert out(ask(c, -30)) == [1], 'an empty from_date means already started'
    assert out(ask(c, 3)) == [1]
    assert free(ask(c, 4)) == [1, 2]

    # ── Two non-overlapping absences for one person ──────────────────────────
    clear()
    add_event(1, 2, 4, status='tdy', notes='Sim - Dothan')
    add_event(1, 10, 12, status='pass', notes='Long weekend')
    assert free(ask(c, 6)) == [1, 2], 'the gap between two absences is free'
    assert reason(ask(c, 3), 1)['status'] == 'tdy'
    assert reason(ask(c, 11), 1)['status'] == 'pass', 'the window covering the day supplies the reason'

    # A range spanning both reports every day either one covers, and says the
    # soldier is not out for the whole range.
    span = ask(c, 2, 12)
    why = reason(span, 1)
    assert why['days'] == [day(2), day(3), day(4), day(10), day(11), day(12)], why['days']
    assert why['whole_range'] is False, why
    assert span['span'] == 11 and free(span) == [2], span

    # ── Ranges ───────────────────────────────────────────────────────────────
    clear()
    add_event(1, 8, 8)
    assert out(ask(c, 6, 7)) == [], 'a range that misses the absence leaves everyone free'
    assert out(ask(c, 6, 8)) == [1], 'unavailable on any one day of the range counts'
    assert reason(ask(c, 6, 10), 1)['days'] == [day(8)]

    # ── On loan is reported separately, exactly like the strength report ─────
    clear()
    conn = server.get_db()
    conn.execute("UPDATE personnel SET status='loan', notes='S2 NCOIC' WHERE id = 2")
    conn.commit()
    conn.close()
    data = ask(c, 0)
    assert free(data) == [1], 'a loaned soldier is not available manpower'
    assert [p['id'] for p in data['on_loan']] == [2], data['on_loan']
    assert out(data) == [], 'on loan is not an absence'
    clear()

    # ── State never overrides the dates ──────────────────────────────────────
    # A completed row whose window still covers the day asked about is a real
    # booking that was never cancelled, so it must not silently vanish.
    clear()
    add_event(1, 2, 6, state='completed')
    assert out(ask(c, 4)) == [1], 'a completed row covering the day still counts'
    # ...and a row that genuinely finished cannot cover a future day anyway.
    clear()
    add_event(1, -9, -2, state='completed')
    assert free(ask(c, 0)) == [1, 2], 'a finished absence does not follow the soldier around'
    # An active row is treated no differently from a scheduled one.
    clear()
    add_event(1, -1, 3, state='active')
    assert out(ask(c, 2)) == [1]

    # ── personnel.status is not consulted ────────────────────────────────────
    # A stale display cache (no event backing it) must not make someone look
    # unavailable next week, and a hand-set 'present' must not hide a booking.
    clear()
    conn = server.get_db()
    conn.execute("UPDATE personnel SET status='tdy', from_date=?, to_date=? WHERE id = 2",
                 (day(-4), day(-1)))
    conn.commit()
    conn.close()
    assert free(ask(c, 5)) == [1, 2], 'the display cache of today must not answer for next week'
    clear()

    # ── Scope and input validation ───────────────────────────────────────────
    clear()
    add_event(3, 0, 2, platoon='1st')
    data = ask(c, 1)
    assert free(data) == [1, 2] and out(data) == [], "another platoon's absences stay out of it"
    assert free(ask(c, 1, platoon='1st')) == [] and out(ask(c, 1, platoon='1st')) == [3]

    assert c.get('/api/availability?platoon=2nd&date=next-tuesday').status_code == 400
    assert c.get(f'/api/availability?platoon=2nd&date={day(5)}&to={day(1)}').status_code == 400
    assert c.get(f'/api/availability?platoon=2nd&date={day(0)}&to={day(400)}').status_code == 400
    # No date at all means today.
    assert c.get('/api/availability?platoon=2nd').get_json()['date'] == day(0)

    print('ok')


if __name__ == '__main__':
    main()
