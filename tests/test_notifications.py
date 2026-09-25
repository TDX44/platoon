"""Email notifications — run with: python tests/test_notifications.py

Covered: the preferences routes and their validation; POST /api/cron/notify's
gate (404 with no CRON_SECRET, 403 on a wrong one, inert with no Resend
config); the timer walking every tenant through auth_notify_roots() under RLS,
each in its own timezone; what each rule says and when; at-most-once per rule
per duty day, including after a failed send; escaping; and preferences
through backup and restore. The HTTP send is replaced throughout.
"""
import json
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))
sys.path.insert(0, _HERE)
import dbharness  # noqa: E402
_schema = dbharness.setup()

import server  # noqa: E402

A = dbharness.make_tree('Notify Co')
B = dbharness.make_tree('Notify Other Co')
A_OWNER = dbharness.make_user(A['root'], 'owner', 'notifyboss')
A_LEADER = dbharness.make_user(A['child'], 'leader', 'notifysarge')
B_OWNER = dbharness.make_user(B['root'], 'owner', 'bboss')
B_QUIET = dbharness.make_user(B['root'], 'leader', 'bquiet')

SENT = []
REAL_NOW = server.app_now
FIXED = {'at': (9, 30)}


def fixed_now():
    h, m = FIXED['at']
    return datetime(2026, 10, 5, h, m, tzinfo=ZoneInfo(server.app_timezone()))


def fake_send(to, subject, text, html_body):
    SENT.append({'to': to, 'subject': subject, 'text': text, 'html': html_body})
    return True


def sql(query, args=()):
    conn = dbharness.owner_conn()
    try:
        cur = conn.execute(query, args)
        rows = cur.fetchall() if cur.description else None
        conn.commit()
        return rows
    finally:
        conn.close()


def person(last, unit, root, status='present', present_date='', to_date=''):
    return sql('INSERT INTO personnel (rank, last, first, unit_id, root_id, status, present_date, to_date) '
               "VALUES ('SGT', %s, 'One', %s, %s, %s, %s, %s) RETURNING id",
               (last, unit, root, status, present_date, to_date))[0]['id']


def event(pid, unit, root, status, f, t, state):
    sql('INSERT INTO scheduled_events (person_id, unit_id, root_id, status, from_date, to_date, state) '
        'VALUES (%s, %s, %s, %s, %s, %s, %s)', (pid, unit, root, status, f, t, state))


def cron(client, secret='s3cret'):
    return client.post('/api/cron/notify', headers={'X-Cron-Secret': secret} if secret is not None else {})


def check_gate(client):
    server.CRON_SECRET = ''
    assert cron(client).status_code == 404
    server.CRON_SECRET = 's3cret'
    assert cron(client, 'wrong').status_code == 403
    assert cron(client, None).status_code == 403
    server.RESEND_API_KEY, server.NOTIFY_FROM = '', ''
    r = cron(client)
    assert r.status_code == 200 and r.get_json()['email'] == 'disabled'
    server.RESEND_API_KEY, server.NOTIFY_FROM = 're_test', 'Platoon <alerts@example.com>'


def check_prefs(client):
    dbharness.as_user(A_LEADER)
    got = client.get('/api/me/notifications').get_json()
    assert got['accountability_enabled'] is False and got['digest_time'] == '06:00' and got['email_enabled']
    assert client.put('/api/me/notifications', json={'accountability_time': '9:00'}).status_code == 400
    assert client.put('/api/me/notifications', json={'digest_time': '24:00'}).status_code == 400
    assert client.put('/api/me/notifications', json={'digest_enabled': 'yes'}).status_code == 400
    r = client.put('/api/me/notifications', json={'accountability_enabled': True, 'accountability_time': '09:00',
                                                  'digest_enabled': True})
    assert r.status_code == 200, r.get_json()
    row = sql('SELECT * FROM notification_prefs WHERE user_id = %s', (A_LEADER['id'],))[0]
    assert row['root_id'] == A['root'] and row['accountability_enabled'] and row['digest_time'] == '06:00'
    # A partial PUT keeps the rest.
    client.put('/api/me/notifications', json={'digest_time': '06:30'})
    row = sql('SELECT * FROM notification_prefs WHERE user_id = %s', (A_LEADER['id'],))[0]
    assert row['accountability_enabled'] and row['digest_time'] == '06:30'
    dbharness.as_user(B_OWNER)
    client.put('/api/me/notifications', json={'digest_enabled': True, 'digest_time': '10:00'})
    # RLS: another tenant's prefs are invisible to this one.
    with server.app.test_request_context('/'):
        server._resolved_user()
        n = server.get_db().execute('SELECT count(*) AS n FROM notification_prefs').fetchone()['n']
        assert n == 1, n


def check_cron(client):
    # Tenant A, all in the leader's platoon; the root-level soldier is outside their subtree.
    today, yesterday = '2026-10-05', '2026-10-04'
    person('Missing', A['child'], A['root'])
    person('Here', A['child'], A['root'], present_date=today)
    person('Upstairs', A['root'], A['root'])
    back = person('Returned', A['child'], A['root'])
    event(back, A['child'], A['root'], 'leave', '2026-09-28', yesterday, 'active')
    going = person('Leaving', A['child'], A['root'])
    event(going, A['child'], A['root'], 'tdy', today, '2026-10-20', 'scheduled')
    person('Awol', A['child'], A['root'], status='ftr')
    person('Stale', A['child'], A['root'], status='pass', to_date='2026-10-01')
    sql("UPDATE units SET name = %s WHERE id = %s", ('2nd <b>Plt</b>', A['child']))

    server.app_now = fixed_now
    server._send_email = fake_send
    FIXED['at'] = (5, 0)
    SENT.clear()
    r = cron(client)
    assert r.status_code == 200 and r.get_json()['roots'] == 2, r.get_json()
    assert SENT == [], 'nothing is due before 06:30'

    FIXED['at'] = (9, 30)
    r = cron(client).get_json()
    assert r['sent'] == 2 and r['failed'] == 0, r
    by_subject = {m['subject'].split(':')[0]: m for m in SENT}
    acc, dig = by_subject['Accountability not complete'], by_subject['Morning digest']
    assert all(m['to'] == A_LEADER['email'] for m in SENT)
    assert 'SGT Missing, One' in acc['text'] and 'Here' not in acc['text'] and 'Upstairs' not in acc['text']
    assert '<b>Plt</b>' not in acc['html'] and '&lt;b&gt;Plt&lt;/b&gt;' in acc['html'], acc['html']
    assert '/2ndplatoon/accountability' in acc['text']
    text = dig['text']
    assert 'Due back today' in text and 'Returned' in text
    assert 'Starting an absence today' in text and 'Leaving' in text
    assert 'Overdue or FTR' in text and 'Awol' in text and 'Stale' in text
    # B's owner is not due until 10:00.
    assert all(m['to'] != B_OWNER['email'] for m in SENT)

    # At most once per rule per duty day.
    SENT.clear()
    cron(client)
    assert SENT == []
    # B at 10:30: due, but nothing to say, so no mail — and not re-evaluated all day.
    FIXED['at'] = (10, 30)
    cron(client)
    assert SENT == []
    assert sql("SELECT result FROM notification_sends WHERE user_id = %s", (B_OWNER['id'],))[0]['result'] == \
        'nothing to send'

    # A failed send is recorded and not retried.
    sql('DELETE FROM notification_sends WHERE user_id = %s', (A_LEADER['id'],))

    def failing(*_):
        raise OSError('resend down')
    server._send_email = failing
    r = cron(client).get_json()
    assert r['failed'] == 2 and r['sent'] == 0, r
    server._send_email = fake_send
    cron(client)
    assert SENT == []
    assert {x['result'] for x in sql('SELECT result FROM notification_sends WHERE user_id = %s',
                                     (A_LEADER['id'],))} == {'failed'}

    # Accountability complete: no alert, and no claim, so a soldier added later still triggers one.
    sql('DELETE FROM notification_sends')
    sql("UPDATE personnel SET present_date = %s WHERE status = 'present'", (today,))
    sql("UPDATE notification_prefs SET digest_enabled = false")
    cron(client)
    assert SENT == [] and not sql("SELECT 1 FROM notification_sends WHERE rule = 'accountability'")
    person('Late', A['child'], A['root'])
    cron(client)
    assert len(SENT) == 1 and 'Late' in SENT[0]['text']
    server.app_now = REAL_NOW


def check_backup(client):
    dbharness.as_user(A_OWNER)
    backup = json.loads(client.get('/api/backup').data)
    leader = next(u for u in backup['users'] if u['username'] == A_LEADER['username'])
    assert leader['notify_accountability_enabled'] is True and leader['notify_accountability_time'] == '09:00'
    owner = next(u for u in backup['users'] if u['username'] == A_OWNER['username'])
    assert 'notify_digest_time' not in owner
    leader['notify_digest_time'] = '7am'            # junk: that row's prefs are left alone
    r = client.post('/api/backup/restore', json=backup)
    assert r.status_code == 200, r.get_json()
    row = sql('SELECT * FROM notification_prefs WHERE user_id = %s', (A_LEADER['id'],))[0]
    assert row['digest_time'] == '06:30'
    leader['notify_digest_time'] = '07:15'
    leader['notify_digest_enabled'] = True
    client.post('/api/backup/restore', json=backup)
    row = sql('SELECT * FROM notification_prefs WHERE user_id = %s', (A_LEADER['id'],))[0]
    assert row['digest_time'] == '07:15' and row['digest_enabled'] and row['root_id'] == A['root']


def main():
    try:
        client = server.app.test_client()
        check_gate(client)
        check_prefs(client)
        check_cron(client)
        check_backup(client)
        print('ok')
    finally:
        server.app_now = REAL_NOW
        dbharness.teardown(_schema)


if __name__ == '__main__':
    main()
