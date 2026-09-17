#!/usr/bin/env python3
"""One-shot: an A0 database (three hardcoded platoons) becomes one company
root with three platoon children, in one transaction, verified before commit.

    MIGRATION_DATABASE_URL=postgresql://platoon_owner:...@db/platoon \\
        python scripts/platoons-to-units.py "Headquarters Company"

Run AFTER the new server has imported once (init_db() adds the nullable
unit_id/root_id columns this fills) and BEFORE the app serves. Not
reversible by redeploying: the rollback is the pg_dump taken just before.

Mapping (agreed in the A1 spec):
  users.platoons '*' or is_admin=1  -> root, owner
  a single platoon                  -> that child, leader
  several platoons                  -> root, leader
  ''                                -> unattached (unit_id NULL)
  invites map the same way. settings: org_timezone -> (root, NULL);
  tdy_<kind>_<p> -> (root, child_p, tdy_<kind>); unit_name* rows deleted.
"""
import json
import os
import re
import sys

import psycopg
from psycopg.rows import dict_row

PLATOON_NAMES = {'1st': '1st Platoon', '2nd': '2nd Platoon', 'hq': 'HQ Platoon'}
PLATOON_SLUGS = {'1st': '1stplatoon', '2nd': '2ndplatoon', 'hq': 'hq'}
SCOPED = ['personnel', 'scheduled_events', 'duty_roster', 'report_history', 'audit_log']
ALL = SCOPED + ['personnel_profile', 'settings', 'users', 'invites']
# Tables whose id comes from an identity sequence. The fixture (and any restore
# that inserted explicit ids) leaves those sequences behind the data; the first
# insert after the cutover would collide with an existing id.
ID_TABLES = SCOPED + ['users']


def slugify(name):
    return re.sub(r'[^a-z0-9]+', '-', name.lower()).strip('-') or 'unit'


def count(conn, table, where='TRUE', params=()):
    return conn.execute(f'SELECT count(*) AS n FROM {table} WHERE {where}', params).fetchone()['n']


def die(conn, msg):
    conn.rollback()
    print(f'migration ABORTED: {msg} — rolled back, nothing changed')
    sys.exit(1)


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    root_name = sys.argv[1].strip()
    url = os.environ.get('MIGRATION_DATABASE_URL') or os.environ['DATABASE_URL']
    conn = psycopg.connect(url, row_factory=dict_row)

    cols = {r['column_name'] for r in conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'personnel' AND table_schema = current_schema()")}
    if 'platoon' not in cols:
        die(conn, 'personnel has no platoon column: this database is already migrated (or never was A0)')
    if 'unit_id' not in cols:
        die(conn, 'personnel has no unit_id column: run the new server once (init_db) before this script')
    if count(conn, 'units'):
        die(conn, 'units already has rows')

    before = {t: count(conn, t) for t in ALL}
    unit_name_rows = count(conn, 'settings', "key LIKE 'unit_name%%'")

    # 1. The tree.
    root_id = conn.execute(
        "INSERT INTO units (parent_id, root_id, kind, name, slug) VALUES (NULL, 0, 'company', %s, %s) RETURNING id",
        (root_name, slugify(root_name))).fetchone()['id']
    conn.execute('UPDATE units SET root_id = %s WHERE id = %s', (root_id, root_id))
    child = {}
    for p in ('1st', '2nd', 'hq'):
        row = conn.execute("SELECT value FROM settings WHERE key = %s", (f'unit_name_{p}',)).fetchone()
        name = (row['value'] if row and row['value'] else PLATOON_NAMES[p])
        child[p] = conn.execute(
            "INSERT INTO units (parent_id, root_id, kind, name, slug) VALUES (%s, %s, 'platoon', %s, %s) RETURNING id",
            (root_id, root_id, name, PLATOON_SLUGS[p])).fetchone()['id']
    print(f'  units: root {root_id} "{root_name}" + {len(child)} platoons')

    # 2. Scoped rows follow their platoon.
    for t in SCOPED:
        for p, uid in child.items():
            conn.execute(f'UPDATE {t} SET unit_id = %s, root_id = %s WHERE platoon = %s', (uid, root_id, p))
        # `platoon IS NOT NULL` matters: init_db() dropped the NOT NULL on this
        # column, and NULL <> '' is NULL, so a NULL would slip past a bare
        # `platoon <> ''` and only surface as a raw traceback from step 5's
        # SET NOT NULL. Every anomaly here has to leave through die().
        stray = count(conn, t, "root_id IS NULL AND platoon IS NOT NULL AND platoon <> ''")
        if stray:
            die(conn, f'{t}: {stray} rows with a platoon that is not 1st/2nd/hq')
        if t == 'audit_log':
            # A company-wide entry (LOGIN, and anything the system logged with
            # no platoon) belongs to the root with no unit. '' and NULL both
            # mean exactly that.
            conn.execute("UPDATE audit_log SET root_id = %s WHERE platoon IS NULL OR platoon = ''", (root_id,))
        n = count(conn, t, 'root_id IS NULL')
        if n:
            die(conn, f'{t}: {n} rows with an empty or NULL platoon')
        print(f'  {t}: {count(conn, t)} rows scoped')
    conn.execute('UPDATE personnel_profile SET root_id = %s', (root_id,))

    # 3. People and invites.
    def place(table, r):
        # Strip and dedupe before comparing anything: A0's own reader was
        # `[p.strip() for p in platoons.split(',') if p.strip()]`, so ' 2nd' was
        # a working grant and '2nd,2nd' was one platoon, not two. Comparing the
        # raw string would quietly unattach the first and hand the second
        # company-wide visibility as a multi-platoon leader.
        keys = sorted({k.strip() for k in (r['platoons'] or '').split(',') if k.strip()})
        if r['is_admin'] or keys == ['*']:
            return root_id, 'owner', root_id
        # An unrecognised platoon is bad data, not an instruction to unattach
        # someone. Every other path here dies on it; so does this one.
        unknown = [k for k in keys if k not in child]
        if unknown:
            die(conn, f'{table} {r["k"]!r} ({r["label"]}): platoon {", ".join(repr(k) for k in unknown)} '
                      'is not 1st/2nd/hq — fix or remove the row and re-run')
        if len(keys) == 1:
            return child[keys[0]], 'leader', root_id
        if keys:
            return root_id, 'leader', root_id
        return None, 'leader', None
    for table, id_col, label_col in (('users', 'id', 'username'), ('invites', 'token', 'label')):
        for r in conn.execute(
                f'SELECT {id_col} AS k, {label_col} AS label, is_admin, platoons FROM {table}').fetchall():
            unit_id, role, rid = place(table, r)
            conn.execute(f'UPDATE {table} SET unit_id = %s, role = %s, root_id = %s WHERE {id_col} = %s',
                         (unit_id, role, rid, r['k']))
        print(f'  {table}: {count(conn, table)} placed')

    # 4. Settings.
    conn.execute("UPDATE settings SET root_id = %s, unit_id = NULL WHERE key = 'org_timezone'", (root_id,))
    for p, uid in child.items():
        for kind in ('schools', 'locations'):
            conn.execute("UPDATE settings SET root_id = %s, unit_id = %s, key = %s WHERE key = %s",
                         (root_id, uid, f'tdy_{kind}', f'tdy_{kind}_{p}'))
    conn.execute("DELETE FROM settings WHERE key LIKE 'unit_name%'")
    conn.execute("DELETE FROM settings WHERE root_id IS NULL")
    print(f'  settings: {count(conn, "settings")} rows scoped')

    # 5. Tighten and drop.
    # An invite that maps nowhere cannot survive `unit_id NOT NULL`, and the
    # alter would fail with a column name rather than a token. Name the tokens
    # instead: the operator revokes them during the rehearsal and re-runs.
    homeless = [r['token'] for r in conn.execute('SELECT token FROM invites WHERE unit_id IS NULL ORDER BY token').fetchall()]
    if homeless:
        die(conn, f'invites with no unit: {", ".join(homeless)} — revoke them and re-run')
    for t in ('personnel', 'scheduled_events', 'duty_roster', 'report_history'):
        conn.execute(f'ALTER TABLE {t} ALTER COLUMN unit_id SET NOT NULL, ALTER COLUMN root_id SET NOT NULL, DROP COLUMN platoon')
    conn.execute('ALTER TABLE audit_log ALTER COLUMN root_id SET NOT NULL, DROP COLUMN platoon')
    conn.execute('ALTER TABLE personnel_profile ALTER COLUMN root_id SET NOT NULL')
    conn.execute('ALTER TABLE settings ALTER COLUMN root_id SET NOT NULL')
    conn.execute('ALTER TABLE users DROP COLUMN is_admin, DROP COLUMN platoons')
    conn.execute('ALTER TABLE invites ALTER COLUMN unit_id SET NOT NULL, ALTER COLUMN root_id SET NOT NULL, '
                 'DROP COLUMN is_admin, DROP COLUMN platoons')

    # 6. Verify before commit.
    after = {t: count(conn, t) for t in ALL}
    expected = dict(before, settings=before['settings'] - unit_name_rows)
    bad = {t: (expected[t], after[t]) for t in ALL if expected[t] != after[t]}
    if bad:
        die(conn, f'row counts changed: {bad}')
    for t in SCOPED + ['personnel_profile', 'settings']:
        n = count(conn, t, 'root_id IS DISTINCT FROM %s', (root_id,))
        if n:
            die(conn, f'{t}: {n} rows not in root {root_id}')
    if count(conn, 'units') != 4:
        die(conn, 'expected exactly 4 units')
    for pid, last in ((1, 'Carr'), (3, 'Bennett')):
        row = conn.execute('SELECT last FROM personnel WHERE id = %s', (pid,)).fetchone()
        if row and row['last'] != last:
            die(conn, f'personnel id {pid} is {row["last"]!r}, expected {last!r}')
    owners = count(conn, 'users', "role = 'owner' AND unit_id = %s", (root_id,))
    # Nobody could grant anybody anything afterwards, and the only way back is
    # pg_restore. Refuse rather than commit a company no one administers.
    if not owners:
        die(conn, 'no user would hold owner at the root — grant an admin before migrating')
    print(f'  verified: {after}, {owners} owner(s) at the root')

    # 7. Resync the identity sequences. A no-op on a database whose ids all came
    # from the sequence; not a no-op after a restore that inserted explicit ids.
    for t in ID_TABLES:
        conn.execute(f"SELECT setval(pg_get_serial_sequence('{t}', 'id'), "
                     f'GREATEST((SELECT COALESCE(MAX(id), 1) FROM {t}), 1), true)')
    print(f'  sequences resynced: {", ".join(ID_TABLES)}')

    conn.commit()
    conn.close()
    print('migration verified')


if __name__ == '__main__':
    main()
