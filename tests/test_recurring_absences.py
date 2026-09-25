"""Repeating absences — run with: python tests/test_recurring_absences.py

A recurrence on POST /api/personnel/<id>/schedule is expanded into ordinary
scheduled_events rows sharing a series_id, so the lifecycle, availability and
history need nothing new. Covered: the expansion rule and its limits, the
booking (states, display cache, double-tap idempotency, one audit row),
availability on an occurrence, cancelling the rest of a series (and its 404 /
403 gates), and series_id through backup and restore.
"""
import json
import os
import sys
from datetime import date, timedelta

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))
sys.path.insert(0, _HERE)
import dbharness  # noqa: E402
_schema = dbharness.setup()

import server  # noqa: E402

T = dbharness.make_tree('Repeat Co')
OTHER = dbharness.make_tree('Repeat Other Co')
OWNER = dbharness.make_user(T['root'], 'owner', 'repeatboss')
LEADER = dbharness.make_user(T['child'], 'leader', 'repeatsarge')
OTHER_OWNER = dbharness.make_user(OTHER['root'], 'owner', 'otherboss')
TODAY = date.fromisoformat(server.app_today())


def day(offset):
    return (TODAY + timedelta(days=offset)).isoformat()


def sql(query, args=()):
    conn = dbharness.owner_conn()
    try:
        cur = conn.execute(query, args)
        rows = cur.fetchall() if cur.description else None
        conn.commit()
        return rows
    finally:
        conn.close()


def person(unit_id, root_id, last):
    return sql('INSERT INTO personnel (rank, last, first, unit_id, root_id) VALUES (%s, %s, %s, %s, %s) RETURNING id',
               ('SGT', last, 'One', unit_id, root_id))[0]['id']


def check_expansion():
    exp = server.expand_recurrence
    monday = date(2026, 10, 5)
    assert monday.weekday() == 0
    d = lambda n: (monday + timedelta(days=n)).isoformat()  # noqa: E731
    wins, err = exp({'type': 'weekly', 'weekdays': [0, 2], 'until': d(13)}, d(0), d(0), d(0))
    assert err is None and wins == [(d(0), d(0)), (d(2), d(2)), (d(7), d(7)), (d(9), d(9))], (wins, err)
    wins, err = exp({'type': 'interval', 'every': 3, 'until': d(7)}, d(0), d(1), d(0))
    assert wins == [(d(0), d(1)), (d(3), d(4)), (d(6), d(7))], wins
    bad = [
        ({'type': 'interval', 'every': 3, 'until': d(7)}, d(0), ''),              # no end date
        ({'type': 'interval', 'every': 1, 'until': d(-1)}, d(0), d(0)),           # stops before it starts
        ({'type': 'interval', 'every': 1, 'until': d(200)}, d(0), d(0)),          # too far ahead
        ({'type': 'interval', 'every': 1, 'until': d(61)}, d(0), d(0)),           # 62 occurrences
        ({'type': 'interval', 'every': 2, 'until': d(10)}, d(0), d(2)),           # overlapping windows
        ({'type': 'weekly', 'weekdays': [], 'until': d(10)}, d(0), d(0)),
        ({'type': 'weekly', 'weekdays': [7], 'until': d(10)}, d(0), d(0)),
        ({'type': 'weekly', 'weekdays': [True], 'until': d(10)}, d(0), d(0)),
        ({'type': 'interval', 'every': '3', 'until': d(10)}, d(0), d(0)),
        ({'type': 'monthly', 'until': d(10)}, d(0), d(0)),
        ({'type': 'interval', 'every': 3, 'until': '10/10/2026'}, d(0), d(0)),
        ('weekly', d(0), d(0)),
    ]
    for rec, f, t in bad:
        wins, err = exp(rec, f, t, d(0))
        assert wins is None and err, (rec, f, t)
    assert exp({'type': 'interval', 'every': 1, 'until': d(59)}, d(0), d(0), d(0))[1] is None  # exactly 60


def check_booking(client):
    pid = person(T['child'], T['root'], 'Weekly')
    body = {'status': 'other', 'from_date': day(0), 'to_date': day(0), 'notes': 'PT profile',
            'recurrence': {'type': 'interval', 'every': 7, 'until': day(21)}}
    audits = sql("SELECT count(*) AS n FROM audit_log WHERE action = 'SCHEDULE_SERIES'")[0]['n']
    r = client.post(f'/api/personnel/{pid}/schedule', json=body)
    assert r.status_code == 201, r.get_json()
    assert r.get_json()['created'] == 4
    rows = sql('SELECT * FROM scheduled_events WHERE person_id = %s ORDER BY from_date', (pid,))
    assert [x['from_date'] for x in rows] == [day(0), day(7), day(14), day(21)]
    assert len({x['series_id'] for x in rows}) == 1 and rows[0]['series_id']
    assert [x['state'] for x in rows] == ['active', 'scheduled', 'scheduled', 'scheduled']
    assert sql('SELECT status FROM personnel WHERE id = %s', (pid,))[0]['status'] == 'other'
    assert sql("SELECT count(*) AS n FROM audit_log WHERE action = 'SCHEDULE_SERIES'")[0]['n'] == audits + 1

    # A double-tapped Save books nothing more.
    r = client.post(f'/api/personnel/{pid}/schedule', json=body)
    assert r.status_code == 200 and r.get_json()['created'] == 0, r.get_json()
    assert sql('SELECT count(*) AS n FROM scheduled_events WHERE person_id = %s', (pid,))[0]['n'] == 4

    # A bad rule is a 400 and writes nothing.
    r = client.post(f'/api/personnel/{pid}/schedule', json=dict(body, from_date=day(1), to_date=day(1),
                                                                  recurrence={'type': 'weekly', 'weekdays': []}))
    assert r.status_code == 400

    # An occurrence is an ordinary absence to the availability page.
    a = client.get(f'/api/availability?unit={T["child"]}&date={day(14)}').get_json()
    assert pid in [p['id'] for p in a['unavailable']], a
    a = client.get(f'/api/availability?unit={T["child"]}&date={day(15)}').get_json()
    assert pid in [p['id'] for p in a['available']]
    return pid, rows[0]['series_id']


def check_cancel_rest(client, pid, series_id):
    # Gates: a malformed id and another tenant's series are both 404.
    assert client.delete('/api/schedules/series/not-a-series').status_code == 404
    other_pid = person(OTHER['root'], OTHER['root'], 'Elsewhere')
    dbharness.as_user(OTHER_OWNER)
    r = client.post(f'/api/personnel/{other_pid}/schedule', json={
        'status': 'leave', 'from_date': day(2), 'to_date': day(2),
        'recurrence': {'type': 'interval', 'every': 2, 'until': day(6)}})
    other_series = r.get_json()['series_id']
    dbharness.as_user(OWNER)
    assert client.delete(f'/api/schedules/series/{other_series}').status_code == 404
    assert sql('SELECT count(*) AS n FROM scheduled_events WHERE series_id = %s', (other_series,))[0]['n'] == 3

    # Own tenant, outside the leader's subtree: 403.
    root_pid = person(T['root'], T['root'], 'AtRoot')
    r = client.post(f'/api/personnel/{root_pid}/schedule', json={
        'status': 'pass', 'from_date': day(3), 'to_date': day(3),
        'recurrence': {'type': 'weekly', 'weekdays': [date.fromisoformat(day(3)).weekday()], 'until': day(10)}})
    root_series = r.get_json()['series_id']
    dbharness.as_user(LEADER)
    assert client.delete(f'/api/schedules/series/{root_series}').status_code == 403

    # The leader may cancel their own soldier's: only the not-yet-started go.
    r = client.delete(f'/api/schedules/series/{series_id}')
    assert r.status_code == 200 and r.get_json()['deleted'] == 3, r.get_json()
    left = sql('SELECT state FROM scheduled_events WHERE person_id = %s', (pid,))
    assert [x['state'] for x in left] == ['active']
    dbharness.as_user(OWNER)


def check_backup_round_trip(client):
    pid = person(T['child'], T['root'], 'Backed')
    r = client.post(f'/api/personnel/{pid}/schedule', json={
        'status': 'leave', 'from_date': day(30), 'to_date': day(31),
        'recurrence': {'type': 'interval', 'every': 10, 'until': day(50)}})
    series_id = r.get_json()['series_id']
    backup = json.loads(client.get('/api/backup').data)
    mine = [e for e in backup['scheduled_events'] if e.get('series_id') == series_id]
    assert len(mine) == 3, mine
    # A hand-edited file with a junk series_id keeps the absence and drops the id.
    for e in backup['scheduled_events']:
        if e['person_id'] == pid and e['from_date'] == day(40):
            e['series_id'] = "x'); DROP TABLE units; --"
    r = client.post('/api/backup/restore', json=backup)
    assert r.status_code == 200, r.get_json()
    got = sql("SELECT from_date, series_id FROM scheduled_events WHERE series_id IS NOT DISTINCT FROM %s "
              "OR (from_date = %s AND status = 'leave') ORDER BY from_date", (series_id, day(40)))
    assert [(g['from_date'], g['series_id']) for g in got] == [
        (day(30), series_id), (day(40), None), (day(50), series_id)], got


def main():
    try:
        dbharness.as_user(OWNER)
        client = server.app.test_client()
        check_expansion()
        pid, series_id = check_booking(client)
        check_cancel_rest(client, pid, series_id)
        check_backup_round_trip(client)
        print('ok')
    finally:
        dbharness.teardown(_schema)


if __name__ == '__main__':
    main()
