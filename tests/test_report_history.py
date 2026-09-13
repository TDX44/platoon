"""Server-side report history — run with: python tests/test_report_history.py

Report history moved from per-device localStorage to /api/reports so every
device shows the same record. Covers: save then list, list excludes body text,
fetching one returns the text, platoon scoping, retention pruning, and delete
requiring admin.
"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ['DATA_DIR'] = tempfile.mkdtemp()

import server  # noqa: E402  (must follow the DATA_DIR override)

ADMIN = {'is_admin': 1, 'id': 1, 'username': 'boss', 'platoons': '*'}
SECOND_ONLY = {'is_admin': 0, 'id': 2, 'username': 'sarge', 'platoons': '2nd'}


def as_user(user):
    server.get_current_user = lambda: user


def main():
    c = server.app.test_client()

    # 1. Save then list returns it, and the list excludes the body text.
    as_user(ADMIN)
    r = c.post('/api/reports', json={'platoon': '2nd', 'unit_name': '2nd Platoon', 'text': 'full body text'})
    assert r.status_code == 201, r.get_json()
    saved = r.get_json()
    assert saved['text'] == 'full body text', saved

    r = c.get('/api/reports?platoon=2nd')
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

    # 3. Platoon scoping: a report saved to 1st does not appear for 2nd.
    r = c.post('/api/reports', json={'platoon': '1st', 'unit_name': '1st Platoon', 'text': 'other text'})
    assert r.status_code == 201, r.get_json()
    r = c.get('/api/reports?platoon=2nd')
    assert len(r.get_json()) == 1, 'a 1st platoon report must not show up under 2nd'
    r = c.get('/api/reports?platoon=1st')
    assert len(r.get_json()) == 1, r.get_json()

    # A user without access to a platoon is forbidden, both listing and saving.
    as_user(SECOND_ONLY)
    assert c.get('/api/reports?platoon=1st').status_code == 403
    assert c.post('/api/reports', json={'platoon': '1st', 'text': 'x'}).status_code == 403
    # ...and fetching a single report from a platoon they can't access is forbidden too.
    r = c.get(f'/api/reports/{saved["id"] + 1}')
    assert r.status_code == 403, r.get_json()
    as_user(ADMIN)

    # 4. Retention pruning actually caps the count.
    conn = server.get_db()
    conn.execute('DELETE FROM report_history')
    conn.commit()
    conn.close()
    for i in range(server.REPORT_HISTORY_MAX + 10):
        r = c.post('/api/reports', json={'platoon': 'hq', 'unit_name': f'r{i}', 'text': f't{i}'})
        assert r.status_code == 201, r.get_json()
    conn = server.get_db()
    count = conn.execute("SELECT COUNT(*) FROM report_history WHERE platoon = 'hq'").fetchone()[0]
    conn.close()
    assert count == server.REPORT_HISTORY_MAX, f'expected pruning to cap at {server.REPORT_HISTORY_MAX}, got {count}'
    # ...and the most recent one survives, not an arbitrary one.
    r = c.get('/api/reports?platoon=hq')
    assert r.get_json()[0]['unit_name'] == f'r{server.REPORT_HISTORY_MAX + 9}', r.get_json()[0]

    # 5. Delete requires admin.
    hq_reports = c.get('/api/reports?platoon=hq').get_json()
    target_id = hq_reports[0]['id']
    as_user(SECOND_ONLY)
    r = c.delete(f'/api/reports/{target_id}')
    assert r.status_code == 403, 'a non-admin must not be able to delete report history'
    as_user(ADMIN)
    r = c.delete(f'/api/reports/{target_id}')
    assert r.status_code == 200, r.get_json()
    assert c.get(f'/api/reports/{target_id}').status_code == 404, 'deleted report must be gone'
    assert c.delete('/api/reports/999999').status_code == 404

    # 6. The localStorage import preserves each report's original save time,
    #    otherwise migrated history all lands stamped today and reads as wrong.
    as_user(ADMIN)
    r = c.post('/api/reports', json={'platoon': '1st', 'unit_name': 'imported',
                                     'text': 'old report', 'created_at': '2026-07-04T13:22:05.000Z'})
    assert r.status_code == 201, r.get_json()
    assert r.get_json()['created_at'] == '2026-07-04 13:22:05', r.get_json()['created_at']

    # A bogus or future timestamp is ignored rather than trusted.
    for bad in ('not-a-date', '2099-01-01T00:00:00Z', '', None, 12345):
        r = c.post('/api/reports', json={'platoon': '1st', 'unit_name': 'fallback',
                                         'text': 'x', 'created_at': bad})
        assert r.status_code == 201, r.get_json()
        got = r.get_json()['created_at']
        assert got and not got.startswith('2099'), f'created_at {bad!r} should have fallen back, got {got}'

    print('ok')


if __name__ == '__main__':
    main()
