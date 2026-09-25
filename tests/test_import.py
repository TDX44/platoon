"""Alpha-roster import — run with: python tests/test_import.py

POST /api/personnel/import is the whole rule for a CSV/TSV import: rank
spelling, which unit a row lands in, duplicate or not, profile validation.
The preview is the same route with dry_run, so these checks cover both.
"""
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))
sys.path.insert(0, _HERE)
import dbharness  # noqa: E402
_schema = dbharness.setup()

import server  # noqa: E402

T = dbharness.make_tree('Import Co')
OTHER = dbharness.make_tree('Import Other Co')
OWNER = dbharness.make_user(T['root'], 'owner', 'importboss')
LEADER = dbharness.make_user(T['child'], 'leader', 'importsarge')


def owner_sql(sql, args=()):
    conn = dbharness.owner_conn()
    try:
        cur = conn.execute(sql, args)
        rows = cur.fetchall() if cur.description else None
        conn.commit()
        return rows
    finally:
        conn.close()


SQUAD = owner_sql("INSERT INTO units (parent_id, root_id, kind, name, slug) VALUES (%s, %s, 'squad', '1st Squad', '1stsquad') RETURNING id",
                  (T['child'], T['root']))[0]['id']
# Two units share a name: a label naming it is ambiguous.
owner_sql("INSERT INTO units (parent_id, root_id, kind, name, slug) VALUES (%s, %s, 'team', 'Alpha', 'alpha-a')", (SQUAD, T['root']))
owner_sql("INSERT INTO units (parent_id, root_id, kind, name, slug) VALUES (%s, %s, 'team', 'Alpha', 'alpha-b')", (SQUAD, T['root']))
owner_sql("INSERT INTO personnel (rank, last, first, unit_id, root_id) VALUES ('SGT', 'Existing', 'Ed', %s, %s)",
          (T['child'], T['root']))


def post(client, rows, **extra):
    body = {'unit_id': T['child'], 'rows': rows}
    body.update(extra)
    return client.post('/api/personnel/import', json=body)


def check_preview(client):
    rows = [
        {'rank': 'sgt.', 'last': 'Smith', 'first': 'John', 'unit': '1ST SQUAD', 'phone': '(555) 123-4567'},
        {'rank': 'PV1', 'last': 'Doe', 'first': 'Jane', 'unit': '1stsquad'},
        {'rank': 'Sp-4', 'last': 'Roe', 'first': 'Rick', 'unit': 'Nowhere'},
        {'rank': 'LT', 'last': 'Vague', 'first': 'Val'},
        {'rank': 'SPC', 'last': 'existing', 'first': 'ED'},            # rank-agnostic duplicate
        {'rank': 'PFC', 'last': 'Smith', 'first': 'John'},             # repeat within the file
        {'rank': 'CPL', 'last': 'Twin', 'first': 'Tom', 'unit': 'alpha'},
        {'rank': 'CPL', 'last': 'Bad', 'first': 'Mail', 'email': 'not-an-email'},
        {'rank': 'CPL', 'last': '', 'first': 'Nameless'},
    ]
    r = post(client, rows, dry_run=True)
    assert r.status_code == 200, r.get_json()
    v = r.get_json()['rows']
    assert [x['action'] for x in v] == ['add', 'add', 'add', 'error', 'duplicate', 'duplicate', 'add', 'error', 'error'], v
    assert v[0]['rank'] == 'SGT' and v[0]['unit_id'] == SQUAD and v[0]['unit_matched']
    assert v[0]['profile']['phone'], v[0]
    assert v[1]['rank'] == 'PVT' and v[1]['unit_id'] == SQUAD
    assert v[2]['rank'] == 'SPC' and v[2]['unit_id'] == T['child'] and not v[2]['unit_matched']
    assert 'Unknown rank' in v[3]['error']
    assert v[6]['unit_ambiguous'] and v[6]['unit_id'] == T['child']
    assert v[7]['error'].startswith('email'), v[7]
    assert r.get_json()['counts'] == {'add': 4, 'duplicate': 2, 'error': 3}
    # Nothing was written by a dry run.
    assert owner_sql("SELECT count(*) AS n FROM personnel WHERE last = 'Smith'")[0]['n'] == 0

    # include_duplicates: both duplicates become adds.
    r = post(client, rows[4:6], dry_run=True, include_duplicates=True)
    assert [x['action'] for x in r.get_json()['rows']] == ['add', 'add']


def check_import(client):
    before = owner_sql("SELECT count(*) AS n FROM audit_log WHERE action = 'IMPORT_PERSONNEL'")[0]['n']
    # A row with an error refuses the whole batch and writes nothing.
    r = post(client, [{'rank': 'SGT', 'last': 'Good', 'first': 'Gary'}, {'rank': 'XYZ', 'last': 'Bad', 'first': 'Bob'}])
    assert r.status_code == 400
    assert owner_sql("SELECT count(*) AS n FROM personnel WHERE last = 'Good'")[0]['n'] == 0

    r = post(client, [
        {'rank': 'sgt', 'last': 'Smith', 'first': 'John', 'unit': '1st Squad', 'phone': '5551234567', 'mos': '11b'},
        {'rank': 'SPC', 'last': 'Existing', 'first': 'Ed'},
        {'rank': 'PFC', 'last': 'Doe', 'first': 'Jane'},
    ])
    assert r.status_code == 201, r.get_json()
    body = r.get_json()
    assert body['added'] == 2 and body['skipped_duplicates'] == 1
    smith = owner_sql("SELECT p.*, pp.phone, pp.mos FROM personnel p LEFT JOIN personnel_profile pp ON pp.person_id = p.id "
                      "WHERE p.last = 'Smith'")
    assert len(smith) == 1 and smith[0]['unit_id'] == SQUAD and smith[0]['rank'] == 'SGT'
    assert smith[0]['root_id'] == T['root'] and smith[0]['mos'] == '11B' and smith[0]['phone']
    assert owner_sql("SELECT count(*) AS n FROM personnel WHERE last = 'Existing'")[0]['n'] == 1
    after = owner_sql("SELECT count(*) AS n FROM audit_log WHERE action = 'IMPORT_PERSONNEL'")[0]['n']
    assert after == before + 1, 'one audit row per import, not per soldier'


def check_limits(client):
    r = post(client, [{'rank': 'PVT', 'last': 'X', 'first': 'Y'}] * (server.IMPORT_MAX_ROWS + 1), dry_run=True)
    assert r.status_code == 400
    assert post(client, []).status_code == 400
    assert post(client, [{'rank': 1, 'last': 'X', 'first': 'Y'}], dry_run=True).get_json()['rows'][0]['action'] == 'error'


def check_gates():
    client = server.app.test_client()
    dbharness.as_user(LEADER)
    # Another tenant's unit: RLS never hands it over, so 404.
    assert post(client, [{'rank': 'PVT', 'last': 'A', 'first': 'B'}], unit_id=OTHER['child'], dry_run=True).status_code == 404
    # Own tenant, above the leader's subtree: 403.
    assert post(client, [{'rank': 'PVT', 'last': 'A', 'first': 'B'}], unit_id=T['root'], dry_run=True).status_code == 403
    # Naming a unit outside the subtree does not reach it: it falls back and is flagged.
    v = post(client, [{'rank': 'PVT', 'last': 'A', 'first': 'B', 'unit': 'Import Co'}], dry_run=True).get_json()['rows'][0]
    assert v['unit_id'] == T['child'] and not v['unit_matched'], v
    dbharness.as_user(OWNER)


def check_rank_normalisation():
    assert server.normalize_rank('1st Lt') == '1LT'
    assert server.normalize_rank('cw-2') == 'CW2'
    assert server.normalize_rank('LT') is None
    assert server.normalize_rank(None) is None


def main():
    try:
        dbharness.as_user(OWNER)
        client = server.app.test_client()
        check_rank_normalisation()
        check_preview(client)
        check_import(client)
        check_limits(client)
        check_gates()
        print('ok')
    finally:
        dbharness.teardown(_schema)


if __name__ == '__main__':
    main()
