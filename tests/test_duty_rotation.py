"""Duty rotation — run with: python tests/test_duty_rotation.py

duty_rotation.propose() is the fairness rule (least often, then least
recently, never someone away, weekends on their own tally); POST
/api/duty/rotation feeds it from the database and writes nothing; POST
/api/duty/bulk saves the result in one transaction.
"""
import os
import sys
from datetime import date, timedelta

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))
sys.path.insert(0, _HERE)
import dbharness  # noqa: E402
_schema = dbharness.setup()

import duty_rotation  # noqa: E402
import server  # noqa: E402

T = dbharness.make_tree('Rotation Co')
OTHER = dbharness.make_tree('Rotation Other Co')
OWNER = dbharness.make_user(T['root'], 'owner', 'rotboss')
LEADER = dbharness.make_user(T['child'], 'leader', 'rotsarge')

MON = date(2026, 10, 5)
assert MON.weekday() == 0


def d(n):
    return (MON + timedelta(days=n)).isoformat()


def sql(query, args=()):
    conn = dbharness.owner_conn()
    try:
        cur = conn.execute(query, args)
        rows = cur.fetchall() if cur.description else None
        conn.commit()
        return rows
    finally:
        conn.close()


def person(last, unit_id=None, root_id=None):
    return sql('INSERT INTO personnel (rank, last, first, unit_id, root_id) VALUES (%s, %s, %s, %s, %s) RETURNING id',
               ('SPC', last, 'X', unit_id or T['child'], root_id or T['root']))[0]['id']


def picks(result):
    return [e['person_id'] for e in result]


def check_rule():
    P = duty_rotation.propose
    # Nobody has history: pool order, and nobody twice while someone is at zero.
    assert picks(P([d(0), d(1), d(2)], [1, 2, 3])) == [1, 2, 3]
    # Least often first, then least recently.
    hist = [(1, d(-10)), (1, d(-5)), (2, d(-3)), (3, d(-8))]
    assert picks(P([d(0), d(1)], [1, 2, 3], hist)) == [3, 2]
    # Away that day (open-ended too) is skipped; nobody free is None.
    away = {1: [{'from_date': d(0), 'to_date': d(1)}], 2: [{'from_date': '', 'to_date': ''}]}
    got = P([d(0), d(1), d(2)], [1, 2], absences=away)
    assert picks(got) == [None, None, 1] and got[0]['reason'] == 'nobody free', got
    # Taken days are left alone.
    got = P([d(0), d(1)], [1, 2], taken={d(0)})
    assert picks(got) == [None, 1] and got[0]['reason'] == 'taken'
    # Weekends keep their own tally: 1 has pulled weekends only, so is still
    # first in line for a weekday; with one tally 2 would be.
    hist = [(1, d(-2)), (1, d(-1))]          # a Saturday and a Sunday
    assert picks(P([d(0)], [1, 2], hist)) == [1]
    assert picks(P([d(0)], [1, 2], hist, separate_weekends=False)) == [2]
    assert duty_rotation.category(d(5)) == 'weekend' and duty_rotation.category(d(2), {d(2)}) == 'weekend'
    assert duty_rotation.category(d(5), separate_weekends=False) == 'all'
    # Not two days running when there is a choice at the same count.
    assert picks(P([d(0), d(1), d(2), d(3)], [1, 2], [(2, d(-1))])) == [1, 2, 1, 2]
    # A single soldier still gets every free day.
    assert picks(P([d(0), d(1)], [7])) == [7, 7]


def rotation(client, **body):
    payload = {'unit_id': T['child'], 'duty_type': 'CQ', 'from': d(0), 'to': d(6)}
    payload.update(body)
    return client.post('/api/duty/rotation', json=payload)


def check_preview(client):
    a, b, c = person('Alpha'), person('Bravo'), person('Charlie')
    # History: Alpha has done CQ twice, Bravo once, Charlie never; another duty does not count.
    for pid, day in ((a, d(-20)), (a, d(-13)), (b, d(-6)), (c, d(-6))):
        sql("INSERT INTO duty_roster (date, unit_id, root_id, duty_type, person_id) VALUES (%s, %s, %s, %s, %s)",
            (day, T['child'], T['root'], 'CQ' if pid != c else 'Runner', pid))
    # Charlie is on leave Tuesday-Wednesday (completed rows still count by their dates).
    sql("INSERT INTO scheduled_events (person_id, unit_id, root_id, status, from_date, to_date, state) "
        "VALUES (%s, %s, %s, 'leave', %s, %s, 'completed')", (c, T['child'], T['root'], d(1), d(2)))
    # Thursday is already covered.
    sql("INSERT INTO duty_roster (date, unit_id, root_id, duty_type, person_id, rank, last, first) "
        "VALUES (%s, %s, %s, 'CQ', %s, 'SPC', 'Alpha', 'X')", (d(3), T['child'], T['root'], a))
    before = sql('SELECT count(*) AS n FROM duty_roster')[0]['n']
    r = rotation(client, person_ids=[a, b, c], to=d(4))
    assert r.status_code == 200, r.get_json()
    body = r.get_json()
    got = [(e['date'], e['person_id']) for e in body['proposal']]
    assert got == [(d(0), c), (d(1), b), (d(2), a), (d(3), None), (d(4), c)], got
    assert body['proposal'][3]['existing'].startswith('SPC Alpha'), body['proposal'][3]
    assert body['away'][d(1)] == {str(c): body['away'][d(1)][str(c)]} and 'leave' in body['away'][d(1)][str(c)]
    assert body['history'][str(a)] == {'weekday': 2}
    assert sql('SELECT count(*) AS n FROM duty_roster')[0]['n'] == before, 'a proposal writes nothing'
    return a, b, c


def check_validation(client, a):
    outsider = person('Outsider', T['root'])
    assert rotation(client, person_ids=[a, outsider]).status_code == 400    # not in this unit
    assert rotation(client, person_ids=[]).status_code == 400
    assert rotation(client, person_ids=[a], to=d(100)).status_code == 400
    assert rotation(client, person_ids=[a], to=d(-1)).status_code == 400
    assert rotation(client, person_ids=[a], duty_type='').status_code == 400
    assert rotation(client, person_ids=[a], holidays=['someday']).status_code == 400
    assert rotation(client, person_ids=[a], unit_id=OTHER['child']).status_code == 404
    dbharness.as_user(LEADER)
    assert rotation(client, person_ids=[a], unit_id=T['root']).status_code == 403
    assert client.post('/api/duty/bulk', json={'unit_id': T['root'], 'entries': [
        {'date': d(0), 'duty_type': 'CQ', 'person_id': a}]}).status_code == 403
    dbharness.as_user(OWNER)
    assert client.post('/api/duty/bulk', json={'unit_id': OTHER['child'], 'entries': [
        {'date': d(0), 'duty_type': 'CQ', 'person_id': a}]}).status_code == 404


def check_bulk(client, a, b, c):
    entries = [{'date': d(10), 'duty_type': 'Staff Duty', 'person_id': a, 'notes': 'first'},
               {'date': d(11), 'duty_type': 'Staff Duty', 'person_id': b},
               {'date': d(12), 'duty_type': 'Staff Duty', 'person_id': c}]
    sql("INSERT INTO scheduled_events (person_id, unit_id, root_id, status, from_date, to_date, state) "
        "VALUES (%s, %s, %s, 'tdy', %s, %s, 'scheduled')", (c, T['child'], T['root'], d(12), d(14)))
    audits = sql("SELECT count(*) AS n FROM audit_log WHERE action = 'ADD_DUTY_ROTATION'")[0]['n']
    r = client.post('/api/duty/bulk', json={'unit_id': T['child'], 'entries': entries})
    assert r.status_code == 201, r.get_json()
    body = r.get_json()
    assert body['created'] == 3 and body['skipped'] == 0
    assert body['entries'][2]['conflict'] and not body['entries'][0]['conflict']
    rows = sql("SELECT * FROM duty_roster WHERE duty_type = 'Staff Duty' ORDER BY date")
    assert [(x['person_id'], x['last'], x['root_id']) for x in rows] == [
        (a, 'Alpha', T['root']), (b, 'Bravo', T['root']), (c, 'Charlie', T['root'])]
    assert rows[0]['notes'] == 'first'
    assert sql("SELECT count(*) AS n FROM audit_log WHERE action = 'ADD_DUTY_ROTATION'")[0]['n'] == audits + 1
    # The same Save again books nothing.
    r = client.post('/api/duty/bulk', json={'unit_id': T['child'], 'entries': entries})
    assert r.status_code == 200 and r.get_json() == {'created': 0, 'skipped': 3, 'entries': []}
    # One bad entry refuses the lot.
    bad = entries + [{'date': '12/1/2026', 'duty_type': 'Staff Duty', 'person_id': a}]
    assert client.post('/api/duty/bulk', json={'unit_id': T['child'], 'entries': bad}).status_code == 400
    outsider = person('Outsider2', T['root'])
    bad = [{'date': d(20), 'duty_type': 'CQ', 'person_id': outsider}]
    assert client.post('/api/duty/bulk', json={'unit_id': T['child'], 'entries': bad}).status_code == 400
    assert client.post('/api/duty/bulk', json={'unit_id': T['child'], 'entries': []}).status_code == 400


def main():
    try:
        check_rule()
        dbharness.as_user(OWNER)
        client = server.app.test_client()
        a, b, c = check_preview(client)
        check_validation(client, a)
        check_bulk(client, a, b, c)
        print('ok')
    finally:
        dbharness.teardown(_schema)


if __name__ == '__main__':
    main()
