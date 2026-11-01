"""Quick add — run with: python tests/test_quick_add.py

POST /api/absences/quick reads a sentence through Claude (stubbed here as
server._ask_claude) into proposed absences and writes nothing. It is off
without ANTHROPIC_API_KEY and for any organization not in QUICK_ADD_ROOTS,
only sends Claude the caller's own subtree, and never returns a person id
that was not on that roster.
"""
import os
import sys
from urllib.error import URLError

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))
sys.path.insert(0, _HERE)
import dbharness  # noqa: E402
_schema = dbharness.setup()

import server  # noqa: E402

T = dbharness.make_tree('Quick Co')
OTHER = dbharness.make_tree('Quick Other Co')
OWNER = dbharness.make_user(T['root'], 'owner', 'quickboss')
LEADER = dbharness.make_user(T['child'], 'leader', 'quicksarge')
STRANGER = dbharness.make_user(OTHER['root'], 'owner', 'quickother')


def sql(query, args=()):
    conn = dbharness.owner_conn()
    try:
        cur = conn.execute(query, args)
        rows = cur.fetchall() if cur.description else None
        conn.commit()
        return rows
    finally:
        conn.close()


def person(last, unit_id, root_id):
    return sql('INSERT INTO personnel (rank, last, first, unit_id, root_id) VALUES (%s, %s, %s, %s, %s) RETURNING id',
               ('CW2', last, 'X', unit_id, root_id))[0]['id']


SENT = []


def reply_with(absences):
    def fake(body):
        SENT.append(body)
        return {'content': [{'type': 'tool_use', 'input': {'absences': absences}}],
                'usage': {'input_tokens': 1, 'output_tokens': 1}}
    return fake


def post(client, text, unit_id):
    server._QUICK_ADD_LAST.clear()   # the cooldown is not what these checks are about
    return client.post('/api/absences/quick', json={'text': text, 'unit_id': unit_id})


def main():
    try:
        client = server.app.test_client()
        boundy = person('Boundy', T['child'], T['root'])
        hq = person('Headquarters', T['root'], T['root'])
        theirs = person('Theirs', OTHER['child'], OTHER['root'])

        # Off: no key, or an organization not on the list -> 404, and /api/me says so.
        dbharness.as_user(OWNER)
        server.ANTHROPIC_API_KEY, server.QUICK_ADD_ROOTS = '', frozenset({T['root']})
        assert client.get('/api/me').get_json()['quick_add'] is False
        assert post(client, 'Boundy to Bliss', T['root']).status_code == 404
        server.ANTHROPIC_API_KEY = 'sk-test'
        assert client.get('/api/me').get_json()['quick_add'] is True
        dbharness.as_user(STRANGER)
        assert client.get('/api/me').get_json()['quick_add'] is False
        assert post(client, 'Theirs to Bliss', OTHER['root']).status_code == 404

        # Input checks, and the unit gate: another tenant's unit is 404, one
        # above the leader's subtree 403.
        dbharness.as_user(OWNER)
        server._ask_claude = reply_with([])
        assert post(client, '   ', T['root']).status_code == 400
        assert post(client, 'x' * 1001, T['root']).status_code == 400
        assert post(client, 'Boundy', OTHER['root']).status_code == 404
        dbharness.as_user(LEADER)
        assert post(client, 'Boundy', T['root']).status_code == 403

        # The leader's roster is all Claude sees; a proposal naming anyone
        # else (HQ above them, another tenant) comes back unmatched.
        SENT.clear()
        server._ask_claude = reply_with([
            {'person_id': boundy, 'name_as_written': 'Boundy', 'status': 'tdy', 'from_date': '2027-01-01',
             'to_date': '2027-01-14', 'school': 'Live Aircraft', 'location': 'Fort Bliss', 'question': ''},
            {'person_id': hq, 'name_as_written': 'Headquarters', 'status': 'leave', 'from_date': '2027-01-02',
             'to_date': '2027-01-05'},
            {'person_id': theirs, 'name_as_written': 'Theirs', 'status': 'tdy', 'from_date': '2027-01-02',
             'to_date': ''},
        ])
        r = post(client, 'Boundy is going to Fort Bliss for 2 weeks starting Jan 1 for live aircraft', T['child'])
        assert r.status_code == 200, r.get_json()
        got = r.get_json()['proposals']
        assert got[0] == {'person_id': boundy, 'name': 'Boundy', 'status': 'tdy', 'from_date': '2027-01-01',
                          'to_date': '2027-01-14', 'school': 'Live Aircraft', 'location': 'Fort Bliss',
                          'reason': '', 'question': ''}, got[0]
        assert got[1]['person_id'] is None and got[2]['person_id'] is None, got
        roster_text = SENT[0]['messages'][0]['content']
        assert 'Boundy' in roster_text and 'Headquarters' not in roster_text and 'Theirs' not in roster_text

        # It wrote nothing.
        assert sql('SELECT count(*) AS n FROM scheduled_events WHERE person_id = %s', (boundy,))[0]['n'] == 0

        # A double tap inside the cooldown is a 429 without a second call.
        SENT.clear()
        assert post(client, 'Boundy', T['child']).status_code == 200
        assert client.post('/api/absences/quick', json={'text': 'Boundy', 'unit_id': T['child']}).status_code == 429
        assert len(SENT) == 1

        # Claude unreachable is a 503, not a 500.
        def down(body):
            raise URLError('no route')
        server._ask_claude = down
        assert post(client, 'Boundy', T['child']).status_code == 503
        print('ok')
    finally:
        dbharness.teardown(_schema)


if __name__ == '__main__':
    main()
