"""Copy one SQLite database into Postgres. Run once, at cutover.

Usage: python scripts/sqlite-to-pg.py /path/to/accountability.db

Reads MIGRATION_DATABASE_URL (or DATABASE_URL) for the destination. The target
schema must already exist — start the app once so init_db() creates it.

Production's SQLite file has accumulated ad-hoc `ALTER TABLE ... ADD COLUMN`
migrations over months (see CLAUDE.md), so a source table can have fewer
columns than the Postgres schema (fine — the destination's own DEFAULT fills
the gap) or, in principle, a column the destination does not recognize at all
(not fine — silently dropping it would be exactly the kind of quiet data loss
that turns into a subtly wrong roster days later). This script aborts loudly
on the second case instead of continuing or letting psycopg raise something
cryptic. TABLES below is an allowlist — legacy tables the app no longer reads
(e.g. old `training_*` tables) are left untouched.

"Start the app once so init_db() creates it" also seeds an empty personnel
table with one placeholder soldier per platoon (server.py's "Seed placeholder
data" block runs unconditionally, Clerk or no Clerk) — a stand-in that exists
only until real data lands. On a freshly created schema those rows land on
ids 1/2/3, which is exactly where a real production personnel table (its own
SQLite AUTOINCREMENT starts at 1 too) also has real soldiers. A plain INSERT
of id=1 over an existing id=1 raises; that is the point — this script clears
that specific, exactly-fingerprinted placeholder row before copying anything,
instead of leaving a collision to either silently swallow the real row (were
this using ON CONFLICT DO NOTHING) or permanently block the migration.

Row counts are verified per table as (destination count) - (destination count
captured for that table immediately before its insert), not an absolute
count, because the destination is not guaranteed empty going in (see above,
plus settings' seeded TDY picklists) — an absolute comparison would report a
false mismatch on a normal cutover. Every insert is a plain INSERT with no
ON CONFLICT clause: any remaining collision (e.g. a users.username clash with
a legacy bootstrap admin row, which only exists when Clerk is disabled) raises
immediately and aborts loudly rather than silently dropping the row.

The whole copy runs in one Postgres transaction: verification happens before
the commit, so a row-count mismatch or insert failure rolls back instead of
leaving a partially migrated destination sitting there while it reports
failure.
"""
import os
import sqlite3
import sys

import psycopg
from psycopg.rows import dict_row

# Parents before children: personnel_profile and scheduled_events reference
# personnel, so they cannot go first.
TABLES = [
    'personnel', 'personnel_profile', 'settings', 'users',
    'audit_log', 'duty_roster', 'scheduled_events', 'invites', 'report_history',
]

# Tables whose id comes from an identity sequence that must be advanced past
# the highest copied id.
SEQUENCED = ['personnel', 'users', 'audit_log', 'duty_roster',
             'scheduled_events', 'report_history']


def _dest_columns(dest, table):
    rows = dest.execute(
        'SELECT column_name FROM information_schema.columns '
        'WHERE table_schema = current_schema() AND table_name = %s',
        (table,)).fetchall()
    return {r['column_name'] for r in rows}


def _count(dest, table):
    return dest.execute(f'SELECT COUNT(*) AS n FROM {table}').fetchone()['n']


def _clear_init_db_placeholder(dest):
    """Remove exactly the row server.py's init_db() seeds, and only that row.

    Matches every literal init_db() inserts (rank, last, first, status, and
    the notes/from_date/to_date/present_date defaults) so a real soldier who
    happens to share one or two of these values is never touched.
    """
    removed = dest.execute(
        "DELETE FROM personnel WHERE rank = 'WO1' AND last = 'Smith' "
        "AND first = 'John' AND status = 'present' AND notes = '' "
        "AND from_date = '' AND to_date = '' AND present_date = '' "
        "RETURNING id").fetchall()
    if removed:
        print(f'  cleared {len(removed)} placeholder personnel row(s) seeded by init_db()')


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    src_path = sys.argv[1]
    dest_url = os.environ.get('MIGRATION_DATABASE_URL') or os.environ['DATABASE_URL']

    src = sqlite3.connect(src_path)
    src.row_factory = sqlite3.Row
    dest = psycopg.connect(dest_url, row_factory=dict_row)

    _clear_init_db_placeholder(dest)

    counts = {}
    baseline = {}
    for table in TABLES:
        try:
            rows = src.execute(f'SELECT * FROM {table}').fetchall()
        except sqlite3.OperationalError:
            print(f'  {table}: absent in source, skipped')
            continue
        baseline[table] = _count(dest, table)
        if not rows:
            counts[table] = 0
            print(f'  {table}: 0')
            continue

        cols = list(rows[0].keys())
        missing = [c for c in cols if c not in _dest_columns(dest, table)]
        if missing:
            dest.close()
            src.close()
            sys.exit(
                f"migration ABORTED: source table '{table}' has column(s) "
                f"{missing} that do not exist in the destination schema. "
                f"Resolve this before re-running — dropping the column "
                f"silently would lose that data.")

        collist = ', '.join(f'"{c}"' for c in cols)
        ph = ', '.join(['%s'] * len(cols))
        try:
            with dest.cursor() as cur:
                for row in rows:
                    cur.execute(
                        f'INSERT INTO {table} ({collist}) VALUES ({ph})',
                        tuple(row[c] for c in cols))
        except psycopg.errors.IntegrityError as e:
            dest.rollback()
            dest.close()
            src.close()
            sys.exit(
                f"migration ABORTED: inserting into '{table}' failed: {e}. "
                f"A row collides with existing destination data — resolve "
                f"before re-running; nothing from this run was committed.")
        counts[table] = len(rows)
        print(f'  {table}: {len(rows)}')

    src.close()

    # Verify before committing: a mismatch rolls the whole migration back
    # rather than leaving a half-right destination in place. Compared against
    # each table's own pre-insert baseline, not an absolute count — the
    # destination is not guaranteed empty (TDY picklist seeds, etc).
    failed = False
    for table, expected in counts.items():
        got = _count(dest, table) - baseline[table]
        if got != expected:
            print(f'MISMATCH {table}: expected {expected} new rows, got {got}')
            failed = True

    if failed:
        dest.rollback()
        dest.close()
        sys.exit('migration verification FAILED — rolled back, do not cut over')

    for table in SEQUENCED:
        if counts.get(table):
            dest.execute(
                f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), "
                f'COALESCE((SELECT MAX(id) FROM {table}), 1), true)')

    dest.commit()
    dest.close()
    print('migration verified')


if __name__ == '__main__':
    main()
