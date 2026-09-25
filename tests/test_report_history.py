"""Server-side report history — run with: python tests/test_report_history.py

Report history moved from per-device localStorage to /api/reports so every
device shows the same record. Covers: save then list, list excludes body text,
fetching one returns the text, unit scoping, retention pruning, and who may
delete what.
"""
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))
sys.path.insert(0, _HERE)
import dbharness  # noqa: E402
_schema = dbharness.setup()

import server  # noqa: E402

T = dbharness.make_tree('Report Co')
OTHER = dbharness.make_tree('Report Other Co')
OWNER = dbharness.make_user(T['root'], 'owner', 'boss')
LEADER = dbharness.make_user(T['child'], 'leader', 'sarge')


def add_unit(parent_id, name, slug):
    conn = dbharness.owner_conn()
    row = conn.execute(
        'INSERT INTO units (parent_id, root_id, kind, name, slug) VALUES (%s, %s, %s, %s, %s) RETURNING id',
        (parent_id, T['root'], 'platoon', name, slug)).fetchone()
    conn.commit(); conn.close()
    return row['id']


def main():
    c = server.app.test_client()
    SIBLING = add_unit(T['root'], '1st Platoon', '1stplatoon')

    # 1. Save then list returns it, and the list excludes the body text.
    dbharness.as_user(OWNER)
    r = c.post('/api/reports', json={'unit_id': T['child'], 'unit_name': '2nd Platoon',
                                     'text': 'full body text'})
    assert r.status_code == 201, r.get_json()
    saved = r.get_json()
    assert saved['text'] == 'full body text', saved
    assert saved['unit_id'] == T['child'], saved

    r = c.get(f"/api/reports?unit={T['child']}")
    assert r.status_code == 200, r.get_json()
    rows = r.get_json()
    assert len(rows) == 1, rows
    assert rows[0]['unit_name'] == '2nd Platoon', rows[0]
    assert rows[0]['created_by'] == 'boss', rows[0]
    assert 'text' not in rows[0], 'the list must not include the report body'

    # 2. Fetching one by id returns the text.
    r = c.get(f'/api/reports/{saved["id"]}')
    assert r.status_code == 200, r.get_json()
    assert r.get_json()['text'] == 'full body text'

    # 3. Unit scoping: a sibling's report does not appear under this platoon,
    #    but the root above both sees everything in its subtree.
    r = c.post('/api/reports', json={'unit_id': SIBLING, 'unit_name': '1st Platoon',
                                     'text': 'other text'})
    assert r.status_code == 201, r.get_json()
    sibling_report = r.get_json()['id']
    assert len(c.get(f"/api/reports?unit={T['child']}").get_json()) == 1, \
        "a sibling unit's report must not show up under this platoon"
    assert len(c.get(f'/api/reports?unit={SIBLING}').get_json()) == 1
    assert len(c.get(f"/api/reports?unit={T['root']}").get_json()) == 2, \
        'the root sees its whole subtree'

    # A leader is confined to their own branch, listing, saving and reading.
    dbharness.as_user(LEADER)
    assert c.get(f'/api/reports?unit={SIBLING}').status_code == 403
    assert c.post('/api/reports', json={'unit_id': SIBLING, 'text': 'x'}).status_code == 403
    assert c.get(f'/api/reports/{sibling_report}').status_code == 403
    # ...and another tenant's report does not exist from here at all.
    conn = dbharness.owner_conn()
    foreign = conn.execute(
        "INSERT INTO report_history (unit_id, root_id, unit_name, text, created_by) "
        "VALUES (%s, %s, 'Elsewhere', 'not yours', 'stranger') RETURNING id",
        (OTHER['child'], OTHER['root'])).fetchone()['id']
    conn.commit(); conn.close()
    assert c.get(f'/api/reports/{foreign}').status_code == 404
    assert c.delete(f'/api/reports/{foreign}').status_code == 404
    dbharness.as_user(OWNER)

    # 4. Retention pruning actually caps the count, per unit.
    conn = dbharness.owner_conn()
    conn.execute('DELETE FROM report_history WHERE root_id = %s', (T['root'],))
    conn.commit(); conn.close()
    for i in range(server.REPORT_HISTORY_MAX + 10):
        r = c.post('/api/reports', json={'unit_id': T['child'], 'unit_name': f'r{i}', 'text': f't{i}'})
        assert r.status_code == 201, r.get_json()
    conn = dbharness.owner_conn()
    count = conn.execute('SELECT COUNT(*) AS n FROM report_history WHERE unit_id = %s',
                         (T['child'],)).fetchone()['n']
    conn.close()
    assert count == server.REPORT_HISTORY_MAX, \
        f'expected pruning to cap at {server.REPORT_HISTORY_MAX}, got {count}'
    # ...and the most recent one survives, not an arbitrary one.
    r = c.get(f"/api/reports?unit={T['child']}")
    assert r.get_json()[0]['unit_name'] == f'r{server.REPORT_HISTORY_MAX + 9}', r.get_json()[0]

    # 5. Who may delete what: the author may withdraw their own, a leader may
    #    not delete someone else's, an owner may delete anyone's.
    dbharness.as_user(LEADER)
    mine = c.post('/api/reports', json={'unit_id': T['child'], 'unit_name': 'sarge report',
                                        'text': 'mine'}).get_json()
    assert c.delete(f'/api/reports/{mine["id"]}').status_code == 200, \
        'the author may delete their own report'

    owners_report = c.get(f"/api/reports?unit={T['child']}").get_json()[0]
    assert owners_report['created_by'] == 'boss', owners_report
    assert c.delete(f'/api/reports/{owners_report["id"]}').status_code == 403, \
        "a leader must not delete someone else's report"

    dbharness.as_user(OWNER)
    r = c.delete(f'/api/reports/{owners_report["id"]}')
    assert r.status_code == 200, r.get_json()
    assert c.get(f'/api/reports/{owners_report["id"]}').status_code == 404, 'deleted report must be gone'
    assert c.delete('/api/reports/999999').status_code == 404

    # The owner may also delete a report they did not write.
    dbharness.as_user(LEADER)
    theirs = c.post('/api/reports', json={'unit_id': T['child'], 'unit_name': 'sarge again',
                                          'text': 'x'}).get_json()
    dbharness.as_user(OWNER)
    assert c.delete(f'/api/reports/{theirs["id"]}').status_code == 200, \
        "an owner may delete anyone's report"

    # 6. The localStorage import preserves each report's original save time,
    #    otherwise migrated history all lands stamped today and reads as wrong.
    r = c.post('/api/reports', json={'unit_id': SIBLING, 'unit_name': 'imported',
                                     'text': 'old report', 'created_at': '2026-07-04T13:22:05.000Z'})
    assert r.status_code == 201, r.get_json()
    # ...on the unit's clock, like every other stored stamp: that Z is UTC,
    # and 13:22 UTC in July is 08:22 in Chicago.
    assert r.get_json()['created_at'] == '2026-07-04 08:22:05', r.get_json()['created_at']
    r = c.post('/api/reports', json={'unit_id': SIBLING, 'unit_name': 'imported', 'text': 'x',
                                     'created_at': '2026-07-04T13:22:05+02:00'})
    assert r.get_json()['created_at'] == '2026-07-04 06:22:05', r.get_json()['created_at']
    # A stamp with no zone is already local, and is kept as written.
    r = c.post('/api/reports', json={'unit_id': SIBLING, 'unit_name': 'imported', 'text': 'x',
                                     'created_at': '2026-07-04 13:22:05'})
    assert r.get_json()['created_at'] == '2026-07-04 13:22:05', r.get_json()['created_at']

    # A bogus or future timestamp is ignored rather than trusted.
    for bad in ('not-a-date', '2099-01-01T00:00:00Z', '', None, 12345):
        r = c.post('/api/reports', json={'unit_id': SIBLING, 'unit_name': 'fallback',
                                         'text': 'x', 'created_at': bad})
        assert r.status_code == 201, r.get_json()
        got = r.get_json()['created_at']
        assert got and not got.startswith('2099'), f'created_at {bad!r} should have fallen back, got {got}'

    # A missing or unreachable unit_id is refused outright.
    assert c.post('/api/reports', json={'text': 'x'}).status_code == 403
    assert c.post('/api/reports', json={'unit_id': OTHER['root'], 'text': 'x'}).status_code == 403

    print('ok')


if __name__ == '__main__':
    try:
        main()
    finally:
        dbharness.teardown(_schema)
