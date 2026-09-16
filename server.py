import json
import os
import re
import secrets
import string
import threading
import time
import base64
from datetime import datetime, date
from functools import wraps
from datetime import timedelta
from zoneinfo import ZoneInfo
from urllib.error import URLError
from flask import Flask, request, jsonify, send_from_directory, session, g, has_request_context
from werkzeug.security import generate_password_hash
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.exceptions import HTTPException
import jwt
from jwt import PyJWKClient
from jwt.exceptions import PyJWKClientConnectionError
import psycopg
from psycopg.rows import dict_row

app = Flask(__name__, static_folder=None)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)
app.secret_key = os.environ.get('SECRET_KEY', 'platoon-tracker-change-in-production')
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SECURE'] = os.environ.get('HTTPS', 'false').lower() == 'true'
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=12)
app.config['SESSION_PERMANENT'] = True

DATABASE_URL = os.environ['DATABASE_URL']
# init_db() creates tables, so it connects as the owner. Everything else uses
# the unprivileged role — see scripts/pg-roles.sql, added in Task 6.
MIGRATION_DATABASE_URL = os.environ.get('MIGRATION_DATABASE_URL', DATABASE_URL)

# Arbitrary fixed key for the session-level advisory lock init_db() takes on
# its own connection, so concurrent gunicorn workers serialize the schema
# work instead of racing CREATE TABLE IF NOT EXISTS / ALTER TABLE against
# each other. Any 64-bit int works; it just has to be the same one every call.
INIT_DB_LOCK_KEY = 84120041902231

from psycopg_pool import ConnectionPool

# gunicorn runs -w 2, so each worker keeps a small pool of its own.
_pool = ConnectionPool(DATABASE_URL, min_size=1, max_size=4, open=False,
                       kwargs={'row_factory': dict_row})

APP_ENV = os.environ.get('APP_ENV', 'production')
PLACEHOLDER_PASSWORD_HASH = 'clerk-managed'


def _parse_csv_env(name):
    return [item.strip() for item in os.environ.get(name, '').split(',') if item.strip()]


def _decode_clerk_publishable_key(publishable_key):
    try:
        encoded = publishable_key.split('_', 2)[-1].split('$', 1)[0]
        encoded += '=' * (-len(encoded) % 4)
        return base64.urlsafe_b64decode(encoded).decode('utf-8')
    except Exception:
        return ''


CLERK_PUBLISHABLE_KEY = os.environ.get('CLERK_PUBLISHABLE_KEY', '').strip()
CLERK_FRONTEND_API_URL = os.environ.get('CLERK_FRONTEND_API_URL', '').strip() or _decode_clerk_publishable_key(CLERK_PUBLISHABLE_KEY)
CLERK_JWKS_URL = f'{CLERK_FRONTEND_API_URL.rstrip("/")}/.well-known/jwks.json' if CLERK_FRONTEND_API_URL else ''
CLERK_AUTHORIZED_PARTIES = _parse_csv_env('CLERK_AUTHORIZED_PARTIES')
CLERK_ADMIN_EMAILS = {email.lower() for email in _parse_csv_env('CLERK_ADMIN_EMAILS')}
CLERK_ENABLED = bool(CLERK_PUBLISHABLE_KEY and CLERK_JWKS_URL)
# lifespan: the default refetches Clerk's key set every 5 minutes, so any DNS or
# network blip had ~288 chances a day to land on a refetch and 401 everyone. A
# rotated key still refreshes immediately, because PyJWKClient refetches on a
# kid miss. timeout: the 30s default would park a sync gunicorn worker.
_JWKS_CLIENT = PyJWKClient(CLERK_JWKS_URL, lifespan=3600, timeout=5) if CLERK_ENABLED else None
_JWKS_LAST_GOOD = None
CLERK_UNREACHABLE = 'Sign-in is temporarily unavailable — could not reach Clerk. Try again in a moment.'

PLATOONS = {
    '1st': '1st Platoon Accountability',
    '2nd': '2nd Platoon Accountability',
    'hq':  'HQ Platoon Accountability'
}

# Every dated absence lives in scheduled_events and is mirrored onto
# The duty day belongs to the unit, not to the server or the viewer. prodsrv02
# runs UTC, so date.today() rolled over at 1900 local and marked people away for
# a course starting the next morning. Every "what day is it" question goes
# through app_today(); nothing reads date.today() directly.
#
# The zone belongs to the ORGANISATION and there is exactly one of it: every
# platoon shares a duty day, so this key is deliberately unsuffixed while
# unit_name_<platoon> and the TDY lists are per-platoon. When a second
# organisation arrives this becomes org_timezone_<org> and only
# load_app_timezone()/set_app_timezone() need to learn about it.
#
# It falls back to PLATOON_TZ and then Central, and is cached in a module global
# because app_now() is called from inside open transactions, where opening a
# second connection to read it would block waiting on the first.
FALLBACK_TZ = os.environ.get('PLATOON_TZ', 'America/Chicago')
TIMEZONE_KEY = 'org_timezone'

_app_tz_name = FALLBACK_TZ
_app_tz = ZoneInfo(FALLBACK_TZ)


def app_timezone():
    """The organisation's timezone name, e.g. 'America/Chicago'."""
    return _app_tz_name


def validate_timezone(name):
    """The normalised zone name, or ValueError. Changes nothing.

    Separate from set_app_timezone() because adopting a zone mutates a module
    global that no rollback can undo: update_settings() has to know the value
    is good *before* it commits to anything, and only adopt it once the whole
    request has succeeded. Otherwise a later validation failure 4xxs, the
    rollback-on-4xx discards the settings row, and the worker keeps serving
    the new zone until restart — with -w 2, one worker on a different duty
    day from the other.
    """
    name = (name or '').strip()
    try:
        return name, ZoneInfo(name)
    except Exception:
        raise ValueError(f'{name!r} is not a known timezone')


def set_app_timezone(name):
    """Adopt a timezone. Raises ValueError if it is not a real IANA zone."""
    global _app_tz_name, _app_tz
    _app_tz_name, _app_tz = validate_timezone(name)
    return _app_tz_name


def load_app_timezone(conn=None):
    """Read the stored zone at startup. A bad stored value must not stop the
    app booting, so it falls back and logs instead of raising."""
    owned = conn is None
    conn = conn or get_db()
    try:
        row = conn.execute('SELECT value FROM settings WHERE key = %s', (TIMEZONE_KEY,)).fetchone()
    finally:
        if owned:
            conn.close()
    if not row or not row['value']:
        return _app_tz_name
    try:
        return set_app_timezone(row['value'])
    except ValueError:
        app.logger.warning('Stored timezone %r is not valid; using %s',
                           row['value'], _app_tz_name)
        return _app_tz_name


def app_now():
    return datetime.now(_app_tz)


def app_today():
    return app_now().date().isoformat()


def app_stamp():
    """Wall-clock timestamp for stored rows, in the unit's timezone."""
    return app_now().strftime('%Y-%m-%d %H:%M:%S')


# 'late' and 'excused' are same-day states in practice, but they are absences
# like any other: a reason in notes, a date window, and a row in the soldier's
# history. Riding the existing lifecycle means they complete themselves
# overnight and show up in reports and availability without special cases.
ABSENCE_STATUSES = ('tdy', 'leave', 'pass', 'other', 'ftr', 'late', 'excused')

# ── TDY picklists ──────────────────────────────────────────────────────────
# Seeded once per platoon into `settings` (keys tdy_schools_<platoon> /
# tdy_locations_<platoon>) as JSON arrays, then owned by the TDY Lists page.
# These starting values are the deduplicated schools/locations already entered
# by each platoon before the picklists existed.
DEFAULT_TDY_SCHOOLS = {
    '1st': ['CCNA', 'CLS', 'R&U'],
    '2nd': ['ATM', 'ATM Flight', 'AWOIC', 'C3', 'FFI', 'Global MX', 'IO', 'IPC',
            'Phase 2', 'Recurrent', 'Sim', 'Sim Progression', 'TF Hunter', 'Wet Sim'],
    'hq':  [],
}
DEFAULT_TDY_LOCATIONS = {
    '1st': [],
    '2nd': ['Bliss', 'Burnett', 'Dothan, AL', 'El Paso', 'Frankfurt',
            'Grapevine', 'Kadena, Japan', 'Wilcox'],
    'hq':  [],
}
TDY_LIST_MAX_ITEMS = 300
TDY_LIST_MAX_LEN = 80

# Report history: generated reports were per-device localStorage; now server-side
# so every device sees the same record. Capped per platoon so it cannot grow forever.
REPORT_HISTORY_MAX = 200


def _clean_tdy_list(values):
    """Trim, drop blanks, cap length, and dedupe case-insensitively."""
    if not isinstance(values, list):
        raise ValueError('Expected a list of strings.')
    seen, out = set(), []
    for value in values[:TDY_LIST_MAX_ITEMS]:
        if not isinstance(value, str):
            raise ValueError('List entries must be strings.')
        item = ' '.join(value.split())[:TDY_LIST_MAX_LEN]
        if item and item.lower() not in seen:
            seen.add(item.lower())
            out.append(item)
    return out


def _get_tdy_list(conn, kind, platoon):
    row = conn.execute('SELECT value FROM settings WHERE key = %s', (f'tdy_{kind}_{platoon}',)).fetchone()
    if not row:
        return []
    try:
        return _clean_tdy_list(json.loads(row['value']))
    except (ValueError, TypeError):
        return []


def get_db():
    """The connection for this request.

    One connection per request, not per call. A1 sets a session variable on it
    for row-level security, which only works if every statement in a request
    runs on the same connection. Outside a request context — init_db() at
    import, the midnight worker, the tests — this hands back a fresh plain
    connection that the caller closes itself; it is not drawn from the pool
    because those callers own their connection's lifecycle and call .close()
    on it directly, which would leak a pooled connection instead of returning
    it.
    """
    if has_request_context():
        _pool.open()
        if not hasattr(g, 'db'):
            g.db = _pool.getconn()
        return g.db
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


# ── Tenancy ──
# Every request runs inside one transaction on one connection (A0). The tenant
# is declared on that transaction with a transaction-local GUC, which is what
# the RLS policies in sql/rls.sql compare against. set_config(..., true) is
# SET LOCAL with a bound parameter; it dies with the transaction, so a pooled
# connection cannot carry one tenant into the next request.

UNIT_KINDS = ('company', 'platoon', 'squad', 'team', 'section', 'detachment', 'flight', 'crew')
ROLES = ('owner', 'leader')


def set_tenant(conn, root_id):
    """Declare (or, with root_id None, explicitly withdraw) this transaction's tenant.

    None is NOT tenant 0. 0 is an ordinary value a row can hold —
    auth_create_root_unit inserts root_id = 0 transiently — so parking every
    unattached user on it would hand them one shared, writable tenant across
    organisations. An unattached user declares the empty string, which NULLIF
    folds to NULL in the policies: they see nothing and can write nothing.
    Setting '' rather than skipping the call also clears a value an earlier
    statement on this connection may have set.
    """
    conn.execute("SELECT set_config('app.root_id', %s, true)",
                 ('' if root_id is None else str(int(root_id)),))


def subtree_ids(conn, unit_id):
    """Every unit id at or below unit_id (the tree is the truth; root_id is a cache)."""
    if unit_id is None:
        return set()
    rows = conn.execute('''
        WITH RECURSIVE sub AS (
            SELECT id FROM units WHERE id = %s
            UNION ALL
            SELECT u.id FROM units u JOIN sub ON u.parent_id = sub.id
        ) SELECT id FROM sub
    ''', (unit_id,)).fetchall()
    return {r['id'] for r in rows}


def current_subtree():
    if not hasattr(g, 'subtree'):
        user = getattr(g, 'current_user', None)
        g.subtree = subtree_ids(get_db(), user['unit_id']) if user else set()
    return g.subtree


def can_access(unit_id):
    """The interior doors: is unit_id inside the signed-in user's subtree?"""
    try:
        return int(unit_id) in current_subtree()
    except (TypeError, ValueError):
        return False


def is_owner(user):
    return bool(user) and user.get('role') == 'owner'


def slugify(name):
    s = re.sub(r'[^a-z0-9]+', '-', (name or '').lower()).strip('-')
    return s or 'unit'


def unique_slug(conn, root_id, name):
    base = slugify(name)
    slug, n = base, 1
    while conn.execute('SELECT 1 FROM units WHERE root_id = %s AND slug = %s', (root_id, slug)).fetchone():
        n += 1
        slug = f'{base}-{n}'
    return slug


def _columns(cur, table):
    """Column names for a table in the current schema.

    Replaces the old SQLite table-info pragma, which is the shape the ad-hoc
    ALTER migrations below were written against.
    """
    cur.execute(
        'SELECT column_name FROM information_schema.columns '
        'WHERE table_schema = current_schema() AND table_name = %s', (table,))
    return [r['column_name'] for r in cur.fetchall()]


def init_db():
    conn = psycopg.connect(MIGRATION_DATABASE_URL, row_factory=dict_row)
    cur = conn.cursor()
    # Guard the whole body with a session-level advisory lock: gunicorn's
    # workers import this module (and so call init_db()) concurrently, and
    # Postgres's CREATE TABLE IF NOT EXISTS is not safe against concurrent DDL
    # (SQLite never hit this — one file, one writer). One worker does the
    # schema work; the rest block here and find it already done.
    cur.execute('SELECT pg_advisory_lock(%s)', (INIT_DB_LOCK_KEY,))
    try:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS personnel (
                id           INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                rank         TEXT,
                last         TEXT,
                first        TEXT,
                status       TEXT DEFAULT 'present',
                notes        TEXT DEFAULT '',
                from_date    TEXT DEFAULT '',
                to_date      TEXT DEFAULT '',
                present_date TEXT DEFAULT '',
                platoon      TEXT DEFAULT '2nd'
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                root_id INTEGER,
                unit_id INTEGER,
                key     TEXT NOT NULL,
                value   TEXT
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id            INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                username      TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                is_admin      INTEGER DEFAULT 0,
                platoons      TEXT DEFAULT '',
                clerk_user_id TEXT DEFAULT '',
                email         TEXT DEFAULT '',
                full_name     TEXT DEFAULT ''
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS audit_log (
                id        INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                timestamp TEXT DEFAULT (to_char(now(), 'YYYY-MM-DD HH24:MI:SS')),
                user_id   INTEGER DEFAULT 0,
                username  TEXT DEFAULT '',
                action    TEXT DEFAULT '',
                details   TEXT DEFAULT '',
                platoon   TEXT DEFAULT ''
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS duty_roster (
                id        INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                date      TEXT NOT NULL,
                platoon   TEXT NOT NULL,
                duty_type TEXT NOT NULL DEFAULT 'CQ',
                person_id INTEGER,
                rank      TEXT DEFAULT '',
                last      TEXT DEFAULT '',
                first     TEXT DEFAULT '',
                notes     TEXT DEFAULT ''
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS scheduled_events (
                id         INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                person_id  INTEGER NOT NULL,
                platoon    TEXT NOT NULL,
                status     TEXT NOT NULL,
                from_date  TEXT DEFAULT '',
                to_date    TEXT DEFAULT '',
                notes      TEXT DEFAULT '',
                created_at TEXT DEFAULT (to_char(now(), 'YYYY-MM-DD HH24:MI:SS')),
                state      TEXT DEFAULT 'scheduled',
                FOREIGN KEY(person_id) REFERENCES personnel(id) ON DELETE CASCADE
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS personnel_profile (
                person_id         INTEGER PRIMARY KEY,
                phone             TEXT DEFAULT '',
                email             TEXT DEFAULT '',
                address           TEXT DEFAULT '',
                emergency_name    TEXT DEFAULT '',
                emergency_phone   TEXT DEFAULT '',
                spouse_dependents TEXT DEFAULT '',
                next_of_kin       TEXT DEFAULT '',
                dod_id            TEXT DEFAULT '',
                date_of_rank      TEXT DEFAULT '',
                mos               TEXT DEFAULT '',
                clearance         TEXT DEFAULT '',
                ets_date          TEXT DEFAULT '',
                section           TEXT DEFAULT '',
                profile_notes     TEXT DEFAULT '',
                flags             TEXT DEFAULT '',
                medical_date      TEXT DEFAULT '',
                dental_date       TEXT DEFAULT '',
                weapons_qual      TEXT DEFAULT '',
                FOREIGN KEY(person_id) REFERENCES personnel(id) ON DELETE CASCADE
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS invites (
                token       TEXT PRIMARY KEY,
                label       TEXT DEFAULT '',
                platoons    TEXT DEFAULT '',
                is_admin    INTEGER DEFAULT 0,
                created_by  TEXT DEFAULT '',
                created_at  TEXT DEFAULT (to_char(now(), 'YYYY-MM-DD HH24:MI:SS')),
                expires_at  TEXT DEFAULT '',
                accepted_at TEXT DEFAULT '',
                accepted_by TEXT DEFAULT ''
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS report_history (
                id         INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                platoon    TEXT NOT NULL,
                unit_name  TEXT DEFAULT '',
                text       TEXT NOT NULL,
                created_at TEXT DEFAULT (to_char(now(), 'YYYY-MM-DD HH24:MI:SS')),
                created_by TEXT DEFAULT ''
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS units (
                id         INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                parent_id  INTEGER REFERENCES units(id),
                root_id    INTEGER NOT NULL,
                kind       TEXT NOT NULL,
                name       TEXT NOT NULL,
                slug       TEXT NOT NULL,
                created_at TEXT DEFAULT (to_char(now(), 'YYYY-MM-DD HH24:MI:SS')),
                UNIQUE (root_id, slug)
            )
        ''')
        cur.execute('CREATE INDEX IF NOT EXISTS units_parent ON units(parent_id)')

        # ── A1 tenancy columns. Additive and nullable here so an A0 database
        #    boots; scripts/platoons-to-units.py backfills it, tightens to NOT
        #    NULL and drops `platoon`. A fresh database gets the strict shape
        #    from the CREATE TABLEs above (Task 7). ──
        for table in ('personnel', 'scheduled_events', 'duty_roster', 'report_history', 'audit_log'):
            tcols = _columns(cur, table)
            if 'unit_id' not in tcols:
                cur.execute(f'ALTER TABLE {table} ADD COLUMN unit_id INTEGER')
            if 'root_id' not in tcols:
                cur.execute(f'ALTER TABLE {table} ADD COLUMN root_id INTEGER')
            cur.execute(f'CREATE INDEX IF NOT EXISTS {table}_root ON {table}(root_id)')
            cur.execute(f'CREATE INDEX IF NOT EXISTS {table}_unit ON {table}(unit_id)')
        if 'root_id' not in _columns(cur, 'personnel_profile'):
            cur.execute('ALTER TABLE personnel_profile ADD COLUMN root_id INTEGER')
        for table in ('users', 'invites'):
            tcols = _columns(cur, table)
            if 'unit_id' not in tcols:
                cur.execute(f'ALTER TABLE {table} ADD COLUMN unit_id INTEGER')
            if 'role' not in tcols:
                cur.execute(f"ALTER TABLE {table} ADD COLUMN role TEXT DEFAULT 'leader'")
            if 'root_id' not in tcols:
                cur.execute(f'ALTER TABLE {table} ADD COLUMN root_id INTEGER')
        st = _columns(cur, 'settings')
        if 'root_id' not in st:
            cur.execute('ALTER TABLE settings ADD COLUMN root_id INTEGER')
        if 'unit_id' not in st:
            cur.execute('ALTER TABLE settings ADD COLUMN unit_id INTEGER')
        # A0's settings were keyed on `key` alone; per-root settings need the
        # scope in the key. The unique index below is the new key.
        cur.execute('ALTER TABLE settings DROP CONSTRAINT IF EXISTS settings_pkey')
        cur.execute('CREATE UNIQUE INDEX IF NOT EXISTS settings_scope_key '
                    'ON settings (root_id, COALESCE(unit_id, 0), key)')

        # ── Migrations ──
        cols = _columns(cur, 'personnel')
        if 'present_date' not in cols:
            cur.execute("ALTER TABLE personnel ADD COLUMN present_date TEXT DEFAULT ''")
        if 'platoon' not in cols:
            cur.execute("ALTER TABLE personnel ADD COLUMN platoon TEXT DEFAULT '2nd'")
            cur.execute("UPDATE personnel SET platoon = '2nd' WHERE platoon IS NULL OR platoon = ''")

        ucols = _columns(cur, 'users')
        if 'pin_hash' not in ucols:
            cur.execute("ALTER TABLE users ADD COLUMN pin_hash TEXT DEFAULT ''")
        for col in ('clerk_user_id', 'email', 'full_name'):
            if col not in ucols:
                cur.execute(f"ALTER TABLE users ADD COLUMN {col} TEXT DEFAULT ''")

        # Only enforce uniqueness for actual Clerk IDs; legacy rows may still have empty values.
        cur.execute('DROP INDEX IF EXISTS idx_users_clerk_user_id')
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_clerk_user_id "
            "ON users(clerk_user_id) WHERE clerk_user_id IS NOT NULL AND clerk_user_id != ''"
        )

        pcols = _columns(cur, 'personnel_profile')
        for col in ('flags', 'medical_date', 'dental_date', 'weapons_qual', 'dob'):
            if pcols and col not in pcols:
                cur.execute(f"ALTER TABLE personnel_profile ADD COLUMN {col} TEXT DEFAULT ''")

        scols = _columns(cur, 'scheduled_events')
        if 'location' not in scols:
            cur.execute("ALTER TABLE scheduled_events ADD COLUMN location TEXT DEFAULT ''")
        # personnel.sched_* predates scheduled_events. Drain any leftovers into the
        # real table, then drop the columns — nothing reads them.
        if 'sched_status' in cols:
            cur.execute(
                "INSERT INTO scheduled_events (person_id, platoon, status, from_date, to_date, notes) "
                "SELECT id, platoon, sched_status, sched_from, sched_to, sched_notes FROM personnel p "
                "WHERE sched_status != '' AND NOT EXISTS ("
                "  SELECT 1 FROM scheduled_events s "
                "  WHERE s.person_id = p.id AND s.status = p.sched_status "
                "  AND s.from_date = p.sched_from AND s.to_date = p.sched_to "
                "  AND s.notes = p.sched_notes"
                ")"
            )
            for col in ('sched_status', 'sched_from', 'sched_to', 'sched_notes'):
                cur.execute(f'ALTER TABLE personnel DROP COLUMN {col}')
        if scols and 'state' not in scols:
            cur.execute("ALTER TABLE scheduled_events ADD COLUMN state TEXT DEFAULT 'scheduled'")
            # Old-model rows whose whole window already passed were never activated
            # (activation was broken in production); file them as history.
            # The cutoff comes from Python, not from date('now','localtime') —
            # that is SQLite syntax Postgres has no function for, and the
            # database's own clock is the wrong clock anyway (the db container
            # is UTC). init_db() runs before load_app_timezone(), so this is
            # PLATOON_TZ rather than the stored org zone; for a one-shot
            # backfill of windows that already ended, a few hours either side
            # of midnight is immaterial, and it beats UTC.
            cur.execute(
                "UPDATE scheduled_events SET state = 'completed' "
                "WHERE to_date != '' AND to_date < %s", (app_today(),)
            )
        # Soldiers already away have no event row under the old model (activation
        # deleted it); backfill an active event so reconciliation owns their return.
        cur.execute(
            "INSERT INTO scheduled_events (person_id, platoon, status, from_date, to_date, notes, state) "
            "SELECT id, platoon, status, from_date, to_date, notes, 'active' FROM personnel p "
            "WHERE status IN ('tdy', 'leave', 'pass', 'other', 'ftr') AND NOT EXISTS ("
            "  SELECT 1 FROM scheduled_events s WHERE s.person_id = p.id AND s.state = 'active'"
            ")"
        )

        # Duty entries predate person_id and stored only a name snapshot. Link the
        # ones that resolve to exactly one soldier; a name shared by two people (or
        # since renamed/deleted) stays NULL and keeps only its snapshot — guessing
        # would silently attach the wrong soldier's absences to an old duty row.
        # Deliberately not a foreign key: deleting a soldier must not erase history.
        dcols = _columns(cur, 'duty_roster')
        if 'person_id' not in dcols:
            cur.execute('ALTER TABLE duty_roster ADD COLUMN person_id INTEGER')
            match = ('FROM personnel p WHERE p.platoon = duty_roster.platoon AND p.rank = duty_roster.rank '
                     'AND p.last = duty_roster.last AND p.first = duty_roster.first')
            cur.execute(
                f'UPDATE duty_roster SET person_id = (SELECT p.id {match}) '
                f'WHERE (SELECT COUNT(*) {match}) = 1'
            )

        # ── Seed legacy admin user only when Clerk is not configured ──
        cur.execute('SELECT COUNT(*) AS n FROM users')
        if cur.fetchone()['n'] == 0 and not CLERK_ENABLED:
            password = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(12))
            cur.execute(
                'INSERT INTO users (username, password_hash, is_admin, platoons) '
                'VALUES (%s, %s, 1, %s) ON CONFLICT (username) DO NOTHING',
                ('admin', generate_password_hash(password), '*')
            )
            import sys
            sys.stderr.write(f'\n{"=" * 52}\n')
            sys.stderr.write(f'  First-run admin account created\n')
            sys.stderr.write(f'  Username : admin\n')
            sys.stderr.write(f'  Password : {password}\n')
            sys.stderr.write(f'  Change this password after first login!\n')
            sys.stderr.write(f'{"=" * 52}\n\n')
            sys.stderr.flush()
        elif CLERK_ENABLED and not CLERK_ADMIN_EMAILS:
            import sys
            sys.stderr.write(
                '\n[auth] Clerk is enabled without CLERK_ADMIN_EMAILS. '
                'The first Clerk user to sign in will be granted admin access automatically.\n\n'
            )
            sys.stderr.flush()

        # ── RLS policies and the pre-tenant front door: every boot, idempotent ──
        for name in ('rls.sql', 'auth_functions.sql'):
            with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sql', name), encoding='utf-8') as fh:
                cur.execute(fh.read())

        conn.commit()
    finally:
        # A failed body leaves the transaction aborted, so this unlock can
        # raise — and an exception here would replace the real one. Closing
        # the connection releases a session-level advisory lock anyway, so
        # the explicit unlock is a courtesy, not a requirement.
        try:
            cur.execute('SELECT pg_advisory_unlock(%s)', (INIT_DB_LOCK_KEY,))
        except Exception:
            pass
        conn.close()


init_db()
# Adopt the organisation's stored timezone before the first request; until this
# runs the module falls back to PLATOON_TZ.
load_app_timezone()


def _assert_rls_safe_role():
    """Table owners and BYPASSRLS roles skip policies silently, and that looks
    exactly like working. Refuse to run as one. init_db() connects as the
    owner on purpose; everything else in this process uses DATABASE_URL.

    Three ways in, not one: the explicit BYPASSRLS attribute, SUPERUSER (which
    bypasses policies without ever setting rolbypassrls), and owning the table —
    including owning it through a granted role, which is why ownership is tested
    with pg_has_role rather than by comparing names.
    """
    with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
        me = conn.execute('SELECT current_user AS u, rolsuper, rolbypassrls '
                          'FROM pg_roles WHERE rolname = current_user').fetchone()
        owned = conn.execute(
            'SELECT tablename FROM pg_tables WHERE schemaname = current_schema() '
            "AND pg_has_role(current_user, tableowner, 'USAGE')"
        ).fetchall()
    if me['rolsuper'] or me['rolbypassrls'] or owned:
        raise SystemExit(
            f"refusing to start: DATABASE_URL connects as {me['u']}, which could bypass row-level security "
            f"(superuser={me['rolsuper']}, bypassrls={me['rolbypassrls']}, owns {len(owned)} tables). "
            'Use the platoon_app role.')


_assert_rls_safe_role()


# ── Audit log helper ──

def log_action(action, details='', platoon=''):
    """Write one audit row onto the caller's transaction.

    It commits with the change it describes, so an audit row can never survive
    a change that failed. The converse is the trap: under SQLite this had its
    own connection, so a failed audit insert hurt nobody. On the shared
    request connection a failed statement ABORTS the whole transaction —
    including the change being audited — and Postgres then turns the COMMIT in
    _close_db() into a silent ROLLBACK. Swallowing that exception used to mean
    the user marked a soldier present, got a 200, and lost the change with
    nothing written anywhere. It still must not raise (an audit failure is not
    worth breaking a request over), but it is never invisible again, and
    _close_db() no longer reports success on an aborted transaction.
    """
    try:
        conn = get_db()
        user_id, username = 0, 'system'
        try:
            user = getattr(g, 'current_user', None)
            if user:
                user_id, username = user['id'], user['username']
        except Exception:
            pass
        # root_id comes from the transaction's own tenant GUC rather than from
        # the caller: an audit row belongs to whoever's transaction wrote it,
        # and taking it from anywhere else would be a row the RLS policy on
        # audit_log rejects (WITH CHECK) — i.e. a silently aborted request.
        root_id = conn.execute(
            "SELECT NULLIF(current_setting('app.root_id', true), '')::int AS root_id"
        ).fetchone()['root_id']
        if root_id is None:
            # Pre-tenant actions (a stranger's first sign-in) are not audited;
            # UNIT_CREATE is the first row of a new tenant.
            return
        conn.execute(
            'INSERT INTO audit_log (user_id, username, action, details, platoon, timestamp, root_id) '
            'VALUES (%s, %s, %s, %s, %s, %s, %s)',
            (user_id, username, action, str(details), platoon, app_stamp(), root_id)
        )
    except Exception:
        app.logger.exception(
            'audit log write failed for action %r (platoon %r) — the request '
            'transaction is now aborted and its change will NOT be committed',
            action, platoon)


# ── Auth helpers ──

def _get_request_origin():
    forwarded_proto = request.headers.get('X-Forwarded-Proto', '').split(',')[0].strip()
    forwarded_host = request.headers.get('X-Forwarded-Host', '').split(',')[0].strip()
    proto = forwarded_proto or request.scheme
    host = forwarded_host or request.host
    return f'{proto}://{host}'


def _get_session_token():
    auth_header = request.headers.get('Authorization', '')
    if auth_header.lower().startswith('bearer '):
        return auth_header.split(' ', 1)[1].strip()
    return request.cookies.get('__session', '').strip()


def _signing_key_for(token):
    """Resolve the token's signing key, tolerating a brief Clerk outage.

    A DNS or network blip while refetching the key set must not look like an
    invalid token, so fall back to the last key set we fetched successfully.
    Matching is still by exact `kid`, so this can never validate a token with
    the wrong key -- it only survives the window where Clerk is unreachable.
    """
    global _JWKS_LAST_GOOD
    try:
        key = _JWKS_CLIENT.get_signing_key_from_jwt(token)
        _JWKS_LAST_GOOD = _JWKS_CLIENT.get_jwk_set()  # already cached; no extra fetch
        return key
    except PyJWKClientConnectionError:
        if _JWKS_LAST_GOOD is None:
            raise
        kid = jwt.get_unverified_header(token).get('kid')
        for key in _JWKS_LAST_GOOD.keys:
            if key.key_id == kid:
                app.logger.warning('Clerk JWKS unreachable; using the last known-good key set.')
                return key
        raise


def _verify_clerk_session_token():
    if not CLERK_ENABLED:
        return None, 'Clerk is not configured on the server.'

    token = _get_session_token()
    if not token:
        return None, 'Unauthorized'

    try:
        signing_key = _signing_key_for(token)
        claims = jwt.decode(
            token,
            signing_key.key,
            algorithms=['RS256'],
            options={'require': ['exp', 'iat', 'nbf', 'sub']},
        )
    except (PyJWKClientConnectionError, URLError) as exc:
        # Clerk itself is unreachable. This is our problem, not a bad session:
        # report it as 503 so the client retries instead of signing the user out.
        app.logger.warning('Clerk JWKS fetch failed: %s', exc)
        return None, CLERK_UNREACHABLE
    except (jwt.PyJWTError, ValueError) as exc:
        # PyJWTError covers InvalidTokenError plus PyJWKClientError, which is
        # raised when the token's signing key isn't in our instance's JWKS
        # (e.g. a token minted by a different Clerk instance). Treat all of
        # these as an auth failure (401) rather than letting them 500.
        return None, str(exc) or 'Unauthorized'

    permitted_origins = CLERK_AUTHORIZED_PARTIES or [_get_request_origin()]
    azp = claims.get('azp')
    if azp and azp not in permitted_origins:
        return None, 'Unauthorized'

    if claims.get('sts') == 'pending':
        return None, 'Account setup is still pending in Clerk.'

    return claims, None


def _auth_status_for(error):
    if error.startswith('Clerk is not configured'):
        return 500
    # 503, not 401: the session may be perfectly valid, we just could not check
    # it. A 401 makes the client sign the user out over a transient blip.
    return 503 if error == CLERK_UNREACHABLE else 401


def clerk_auth_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        claims, error = _verify_clerk_session_token()
        if error:
            return jsonify({'error': error}), _auth_status_for(error)
        g.auth_claims = claims
        return f(*args, **kwargs)
    return decorated


def get_current_user():
    if hasattr(g, 'current_user'):
        return g.current_user

    claims = getattr(g, 'auth_claims', None)
    if not claims:
        claims, error = _verify_clerk_session_token()
        if error:
            g.auth_error = error
            return None
        g.auth_claims = claims

    clerk_user_id = claims.get('sub')
    if not clerk_user_id:
        return None
    conn = get_db()
    user = conn.execute('SELECT * FROM auth_user_by_clerk_id(%s)', (clerk_user_id,)).fetchone()
    g.current_user = dict(user) if user else None
    return g.current_user


# ── Invitations ──
# Sign-up is invite-only: a Clerk account that has never synced here is turned
# away unless it presents a live invite token (or qualifies for the admin
# bootstrap below, which is how the first/CLERK_ADMIN_EMAILS accounts get in).
INVITE_EXPIRY_DAYS = 7
INVITE_REQUIRED = 'This app is invite-only. Ask an admin for an invite link.'


def _clean_platoons(value, is_admin):
    if is_admin:
        return '*'
    keys = [p.strip() for p in (value or '').split(',') if p.strip() in PLATOONS]
    return ','.join(dict.fromkeys(keys))


def _valid_invite(conn, token):
    """The invite row if the token exists, is unused and unexpired, else None."""
    if not token:
        return None
    return conn.execute(
        "SELECT * FROM invites WHERE token = %s AND accepted_at = '' "
        'AND expires_at > %s',
        (token, app_stamp())
    ).fetchone()


def _display_name_for_user(payload):
    for key in ('full_name', 'username', 'email'):
        value = (payload.get(key) or '').strip()
        if value:
            return value
    return 'User'


def _should_auto_grant_admin(conn, email):
    if email and email.lower() in CLERK_ADMIN_EMAILS:
        return True

    if CLERK_ADMIN_EMAILS:
        return False

    synced_admin = conn.execute(
        'SELECT 1 FROM users WHERE clerk_user_id != \'\' AND is_admin = 1 LIMIT 1'
    ).fetchone()
    any_synced = conn.execute(
        'SELECT 1 FROM users WHERE clerk_user_id != \'\' LIMIT 1'
    ).fetchone()
    return not synced_admin and not any_synced


def sync_clerk_user(payload):
    claims = getattr(g, 'auth_claims', None)
    if not claims:
        claims, error = _verify_clerk_session_token()
        if error:
            return None, error
        g.auth_claims = claims

    clerk_user_id = claims.get('sub')
    if not clerk_user_id:
        return None, 'Missing Clerk user id.'

    username = (payload.get('username') or '').strip()
    email = (payload.get('email') or '').strip().lower()
    full_name = (payload.get('full_name') or '').strip()
    if not username:
        username = email or f'user-{clerk_user_id[:8]}'

    conn = get_db()
    invite = None
    try:
        existing = conn.execute('SELECT * FROM users WHERE clerk_user_id = %s', (clerk_user_id,)).fetchone()
        username_conflict = conn.execute(
            'SELECT * FROM users WHERE LOWER(username) = %s',
            (username.lower(),)
        ).fetchone() if username else None
        email_conflict = conn.execute(
            'SELECT * FROM users WHERE LOWER(email) = %s',
            (email,)
        ).fetchone() if email else None

        if existing:
            conn.execute(
                'UPDATE users SET username = %s, email = %s, full_name = %s WHERE clerk_user_id = %s',
                (username, email, full_name, clerk_user_id)
            )
        else:
            is_admin = 1 if _should_auto_grant_admin(conn, email) else 0
            legacy = None

            for candidate in (email_conflict, username_conflict):
                if candidate and not candidate['clerk_user_id']:
                    legacy = candidate
                    break

            invite = _valid_invite(conn, (payload.get('invite_token') or '').strip())
            if not invite and not is_admin and not legacy:
                return None, INVITE_REQUIRED
            if invite:
                is_admin = 1 if (invite['is_admin'] or is_admin) else 0

            if legacy:
                platoons = invite['platoons'] if invite else legacy['platoons']
                should_be_admin = bool(invite['is_admin']) if invite else bool(legacy['is_admin'])
                should_be_admin = should_be_admin or bool(is_admin)
                if should_be_admin and not platoons:
                    platoons = '*'
                conn.execute(
                    'UPDATE users SET username = %s, password_hash = %s, is_admin = %s, platoons = %s, '
                    'clerk_user_id = %s, email = %s, full_name = %s, pin_hash = \'\' WHERE id = %s',
                    (username, PLACEHOLDER_PASSWORD_HASH, 1 if should_be_admin else 0, platoons,
                     clerk_user_id, email, full_name, legacy['id'])
                )
            else:
                if username_conflict and username_conflict['clerk_user_id'] and username_conflict['clerk_user_id'] != clerk_user_id:
                    username = email or f'user-{clerk_user_id[:8]}'
                platoons = '*' if is_admin else (invite['platoons'] if invite else '')
                conn.execute(
                    'INSERT INTO users (username, password_hash, is_admin, platoons, clerk_user_id, email, full_name) '
                    'VALUES (%s, %s, %s, %s, %s, %s, %s)',
                    (username, PLACEHOLDER_PASSWORD_HASH, is_admin, platoons, clerk_user_id, email, full_name)
                )
        if invite:
            conn.execute(
                'UPDATE invites SET accepted_at = %s, accepted_by = %s WHERE token = %s',
                (app_stamp(), clerk_user_id, invite['token'])
            )
        row = conn.execute('SELECT * FROM users WHERE clerk_user_id = %s', (clerk_user_id,)).fetchone()
        g.current_user = dict(row) if row else None
        return g.current_user, None
    except psycopg.errors.UniqueViolation:
        return None, 'That username is already in use locally. Ask an admin to rename or merge the account.'


def has_platoon_access(user, platoon):
    # Interim: a user created on the unit tree has neither key, so a route that
    # Tasks 2-6 have not converted yet answers 403 rather than 500. Deleted in
    # Task 7 along with `platoon`.
    if user.get('is_admin'):
        return True
    return platoon in [p.strip() for p in user.get('platoons', '').split(',') if p.strip()]


def _unauthenticated_response():
    error = getattr(g, 'auth_error', '') or 'Unauthorized'
    return jsonify({'error': error}), _auth_status_for(error)


def _resolved_user():
    user = get_current_user()
    if not user:
        return None
    g.current_user = user
    # Declare the tenant on this request's transaction before any other
    # statement. An unattached user has no root_id and so declares no tenant
    # at all: RLS default-deny is exactly the right answer for them.
    set_tenant(get_db(), user.get('root_id'))
    return user


def login_required(f):
    """Signed in. Unattached users pass — only /api/me, /api/units GET/POST and
    /api/auth/sync use this alone; everything else wants attached_required."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not _resolved_user():
            return _unauthenticated_response()
        return f(*args, **kwargs)
    return decorated


def attached_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        user = _resolved_user()
        if not user:
            return _unauthenticated_response()
        if user.get('unit_id') is None:
            return jsonify({'error': 'Create or join a unit first.'}), 403
        return f(*args, **kwargs)
    return decorated


def owner_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        user = _resolved_user()
        if not user:
            return _unauthenticated_response()
        if user.get('unit_id') is None or not is_owner(user):
            return jsonify({'error': 'Forbidden'}), 403
        return f(*args, **kwargs)
    return decorated


# ── Error handling ──
# Without this an unhandled exception is a bare 500 that nobody ever sees: the
# traceback goes nowhere useful and an /api/ caller gets an HTML error page it
# cannot parse. Log it with enough context to find the request, and keep the
# JSON contract.

@app.errorhandler(Exception)
def handle_unexpected_error(exc):
    # 404/401/405 and friends are deliberate answers, not faults — let them be.
    if isinstance(exc, HTTPException):
        return exc

    user = 'anonymous'
    current = getattr(g, 'current_user', None)
    if current:
        user = current.get('username') or f'user {current.get("id")}'
    app.logger.exception('Unhandled error: %s %s (user=%s)', request.method, request.path, user)

    if request.path.startswith('/api/'):
        return jsonify({'error': 'Something went wrong on the server.'}), 500
    return 'Something went wrong on the server.', 500


@app.after_request
def _mark_db_success(response):
    # errorhandler(Exception) above turns a raised exception into a normal
    # response, so teardown_request alone cannot tell "the handler ran to
    # completion" from "the handler raised and got turned into a 500" — both
    # arrive at teardown with exc=None. after_request runs before teardown and
    # is skipped when an exception escapes with no handler, so this flag is
    # the signal teardown actually needs: set only for a response that says
    # the request succeeded.
    #
    # ...and only if the transaction can actually still commit. A statement
    # that failed earlier (log_action swallows its own on purpose) leaves the
    # connection INERROR, where Postgres answers COMMIT with ROLLBACK and
    # raises nothing. A success response there tells the user their soldier is
    # marked present when nothing was saved. This is the last point that can
    # still change the answer, so change it.
    #
    # Only a success response, deliberately. A 4xx on an aborted transaction
    # is a route that already knows: update_user() and sync_clerk_user() both
    # catch a UniqueViolation and answer 409 / "username already in use", and
    # those answers are correct and must keep their status code.
    if response.status_code < 400:
        conn = g.get('db')
        if conn is not None and conn.info.transaction_status == psycopg.pq.TransactionStatus.INERROR:
            app.logger.error(
                'Aborted transaction on %s %s: an earlier statement failed, so nothing '
                'from this request can be saved — answering 500 instead of %s',
                request.method, request.path, response.status_code)
            # after_request must hand back a real response object, not a tuple.
            body = (jsonify({'error': 'Something went wrong on the server.'})
                    if request.path.startswith('/api/')
                    else 'Something went wrong on the server.')
            return app.make_response((body, 500))
        g.db_commit = True
    return response


@app.teardown_request
def _close_db(exc):
    conn = g.pop('db', None)
    if conn is None:
        return
    try:
        # A statement that failed earlier in the request (log_action swallows
        # its own, deliberately) leaves the transaction INERROR. Postgres
        # answers COMMIT on such a transaction with ROLLBACK and raises
        # nothing, so without this check the request would report success
        # while discarding the user's change. Say so loudly instead.
        aborted = conn.info.transaction_status == psycopg.pq.TransactionStatus.INERROR
        if aborted:
            app.logger.error(
                '%s %s: transaction was aborted by an earlier failed statement; '
                'rolling back — nothing from this request was saved',
                request.method, request.path)
            conn.rollback()
        elif exc is None and g.get('db_commit'):
            conn.commit()
        else:
            conn.rollback()
    finally:
        _pool.putconn(conn)


# ── Auth routes ──

@app.route('/')
def index():
    return send_from_directory('.', 'index.html')


@app.route('/welcome')
@app.route('/privacy')
@app.route('/terms')
@app.route('/legal/privacy')
@app.route('/legal/terms')
def public_page():
    """The signed-out pages: the marketing placeholder plus the two legal pages
    Google's OAuth consent screen links to. They live outside index.html because
    they have to render with no Clerk, no session and no JS.

    /legal/* answers the same pages because that is the shape the sibling apps
    use and it is the URL people reach for; without it the path falls through to
    spa_fallback and quietly serves the app shell instead. The filename comes
    from the last segment, and only the literal paths above reach this function.
    """
    return send_from_directory('public', request.path.rsplit('/', 1)[-1] + '.html')


# Everything the browser may fetch from the repo root. The fallback below used to
# serve any file that existed, which handed server.py to anyone who asked for it;
# only these are assets.
STATIC_DIRS = ('images/', 'public/')
STATIC_FILES = ('manifest.json', 'sw.js')


@app.route('/<path:path>')
def spa_fallback(path):
    if path.startswith('api/'):
        return jsonify({'error': 'Not found'}), 404
    root = app.static_folder or '.'
    is_asset = path in STATIC_FILES or path.startswith(STATIC_DIRS)
    if is_asset and os.path.isfile(os.path.join(root, path)):
        return send_from_directory(root, path)
    return send_from_directory('.', 'index.html')


@app.route('/api/auth/config', methods=['GET'])
def auth_config():
    return jsonify({
        'enabled': CLERK_ENABLED,
        'publishable_key': CLERK_PUBLISHABLE_KEY,
        'frontend_api_url': CLERK_FRONTEND_API_URL,
        'timezone': app_timezone(),
        'app_env': APP_ENV,
    })


@app.route('/api/auth/sync', methods=['POST'])
@clerk_auth_required
def auth_sync():
    payload = request.get_json() or {}
    user, error = sync_clerk_user(payload)
    if error:
        return jsonify({'error': error}), 403 if error == INVITE_REQUIRED else 409
    log_action('LOGIN', f'Clerk user signed in: {_display_name_for_user(user)}')
    return jsonify({
        'id': user['id'],
        'username': user['username'],
        'email': user.get('email', ''),
        'full_name': user.get('full_name', ''),
        'is_admin': bool(user['is_admin']),
        'platoons': user['platoons'],
    })


@app.route('/api/logout', methods=['POST'])
def logout():
    return jsonify({'success': True})


@app.route('/api/me', methods=['GET'])
@login_required
def me():
    user = g.current_user
    return jsonify({
        'id': user['id'],
        'username': user['username'],
        'email': user.get('email', ''),
        'full_name': user.get('full_name', ''),
        'is_admin': bool(user['is_admin']),
        'platoons': user['platoons'],
    })


# ── User management (admin only) ──

@app.route('/api/users', methods=['GET'])
@owner_required
def get_users():
    conn = get_db()
    rows = conn.execute(
        'SELECT id, username, email, full_name, is_admin, platoons FROM users '
        'WHERE clerk_user_id != \'\' ORDER BY username'
    ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route('/api/users/<int:user_id>', methods=['PUT'])
@owner_required
def update_user(user_id):
    data = request.get_json()
    fields, values = [], []
    if 'is_admin' in data:
        fields.append('is_admin = %s')
        values.append(1 if data['is_admin'] else 0)
    if 'platoons' in data:
        fields.append('platoons = %s')
        values.append(data['platoons'])
    if 'username' in data:
        fields.append('username = %s')
        values.append((data['username'] or '').strip())
    if not fields:
        return jsonify({'error': 'Nothing to update'}), 400
    values.append(user_id)
    conn = get_db()
    try:
        conn.execute(f'UPDATE users SET {", ".join(fields)} WHERE id = %s AND clerk_user_id != \'\'', values)
        row = conn.execute(
            'SELECT id, username, email, full_name, is_admin, platoons FROM users WHERE id = %s',
            (user_id,)
        ).fetchone()
        return jsonify(dict(row))
    except psycopg.errors.UniqueViolation:
        return jsonify({'error': 'Username already exists'}), 409


@app.route('/api/users/<int:user_id>', methods=['DELETE'])
@owner_required
def delete_user(user_id):
    if user_id == g.current_user['id']:
        return jsonify({'error': 'Cannot delete your own account'}), 400
    conn = get_db()
    conn.execute('DELETE FROM users WHERE id = %s AND clerk_user_id != \'\'', (user_id,))
    return jsonify({'success': True})


# ── Invitations ──

@app.route('/api/invites', methods=['GET'])
@owner_required
def get_invites():
    conn = get_db()
    rows = conn.execute('SELECT * FROM invites ORDER BY created_at DESC LIMIT 50').fetchall()
    now = app_stamp()
    return jsonify([{
        'token': r['token'],
        'label': r['label'],
        'platoons': r['platoons'],
        'is_admin': bool(r['is_admin']),
        'created_by': r['created_by'],
        'expires_at': r['expires_at'],
        'status': 'accepted' if r['accepted_at'] else ('expired' if r['expires_at'] <= now else 'pending'),
    } for r in rows])


@app.route('/api/invites', methods=['POST'])
@owner_required
def create_invite():
    data = request.get_json() or {}
    label = ' '.join((data.get('label') or '').split())[:80]
    is_admin = 1 if data.get('is_admin') else 0
    platoons = _clean_platoons(data.get('platoons'), is_admin)
    if not platoons:
        return jsonify({'error': 'Pick at least one platoon, or make the invite an administrator.'}), 400

    token = secrets.token_urlsafe(24)
    conn = get_db()
    conn.execute(
        'INSERT INTO invites (token, label, platoons, is_admin, created_by, expires_at, created_at) '
        'VALUES (%s, %s, %s, %s, %s, %s, %s)',
        (token, label, platoons, is_admin, g.current_user['username'],
         (app_now() + timedelta(days=INVITE_EXPIRY_DAYS)).strftime('%Y-%m-%d %H:%M:%S'),
         app_stamp())
    )
    log_action('INVITE_CREATE', f'Invited {label or "(unnamed)"} — {"administrator" if is_admin else platoons}')
    return jsonify({'token': token, 'url': f'{_get_request_origin()}/invite/{token}'})


@app.route('/api/invites/<token>', methods=['DELETE'])
@owner_required
def revoke_invite(token):
    conn = get_db()
    row = conn.execute('SELECT label FROM invites WHERE token = %s', (token,)).fetchone()
    conn.execute('DELETE FROM invites WHERE token = %s', (token,))
    if row:
        log_action('INVITE_REVOKE', f'Revoked invite for {row["label"] or "(unnamed)"}')
    return jsonify({'success': True})


@app.route('/api/invites/<token>/preview', methods=['GET'])
def preview_invite(token):
    """Unauthenticated — the token itself is the secret. Lets the sign-up page
    tell an invitee what they were invited to before they create an account."""
    conn = get_db()
    row = _valid_invite(conn, token)
    if not row:
        return jsonify({'valid': False}), 404
    access = 'all platoons (administrator)' if row['is_admin'] else ' + '.join(
        PLATOONS[p].replace(' Accountability', '') for p in row['platoons'].split(',') if p in PLATOONS
    )
    return jsonify({'valid': True, 'label': row['label'], 'access': access})


# ── Platoon & Personnel routes ──

@app.route('/api/platoons', methods=['GET'])
@login_required
def get_platoons():
    user = get_current_user()
    conn = get_db()
    result = {}
    for key, default_name in PLATOONS.items():
        if not has_platoon_access(user, key):
            continue
        row = conn.execute('SELECT value FROM settings WHERE key = %s', (f'unit_name_{key}',)).fetchone()
        count = conn.execute('SELECT COUNT(*) AS n FROM personnel WHERE platoon = %s', (key,)).fetchone()['n']
        result[key] = {'name': row['value'] if row else default_name, 'count': count}
    return jsonify(result)


@app.route('/api/personnel', methods=['GET'])
@login_required
def get_personnel():
    platoon = request.args.get('platoon', '2nd')
    user = get_current_user()
    if not has_platoon_access(user, platoon):
        return jsonify({'error': 'Forbidden'}), 403
    conn = get_db()
    _reconcile_absences(conn, app_today())
    rows = conn.execute(
        'SELECT * FROM personnel WHERE platoon = %s ORDER BY rank, last, first', (platoon,)
    ).fetchall()
    scheduled_rows = conn.execute(
        "SELECT * FROM scheduled_events WHERE platoon = %s AND state != 'completed' "
        'ORDER BY from_date, to_date, id', (platoon,)
    ).fetchall()
    scheduled_by_person = {}
    for r in scheduled_rows:
        scheduled_by_person.setdefault(r['person_id'], []).append(dict(r))
    result = []
    for r in rows:
        item = dict(r)
        item['scheduled_events'] = scheduled_by_person.get(r['id'], [])
        result.append(item)
    return jsonify(result)


@app.route('/api/personnel', methods=['POST'])
@login_required
def add_person():
    data = request.get_json()
    platoon = data.get('platoon', '2nd')
    user = get_current_user()
    if not has_platoon_access(user, platoon):
        return jsonify({'error': 'Forbidden'}), 403
    conn = get_db()
    cur = conn.execute(
        'INSERT INTO personnel (rank, last, first, platoon) VALUES (%s, %s, %s, %s) RETURNING id',
        (data.get('rank', ''), data.get('last', ''), data.get('first', ''), platoon)
    )
    new_id = cur.fetchone()['id']
    row = conn.execute('SELECT * FROM personnel WHERE id = %s', (new_id,)).fetchone()
    log_action('ADD_PERSON', f'{data.get("rank","")} {data.get("last","")}, {data.get("first","")}', platoon)
    return jsonify(dict(row)), 201


@app.route('/api/personnel/<int:person_id>', methods=['PUT'])
@login_required
def update_person(person_id):
    data = request.get_json()
    fields, values = [], []
    for col in ('rank', 'last', 'first', 'status', 'notes', 'from_date', 'to_date', 'present_date'):
        if col in data:
            fields.append(f'{col} = %s')
            values.append(data[col])
    if not fields:
        return jsonify({'error': 'No fields to update'}), 400
    conn = get_db()
    person = conn.execute('SELECT rank, last, first, status, platoon FROM personnel WHERE id = %s', (person_id,)).fetchone()
    if person is None:
        return jsonify({'error': 'Not found'}), 404
    user = get_current_user()
    if not has_platoon_access(user, person['platoon']):
        return jsonify({'error': 'Forbidden'}), 403
    values.append(person_id)
    conn.execute(f'UPDATE personnel SET {", ".join(fields)} WHERE id = %s', values)
    # Only the transition matters: apiUpdate() resends the current status on
    # every save, so a TDY soldier being marked present-for-today still PUTs
    # status='tdy' and must not have their absence closed.
    if data.get('status') == 'present' and person['status'] in ABSENCE_STATUSES:
        _end_running_absence(conn, person_id, app_today())
    row = conn.execute('SELECT * FROM personnel WHERE id = %s', (person_id,)).fetchone()
    if 'status' in data and data['status'] != person['status']:
        log_action('UPDATE_STATUS', f'{person["rank"]} {person["last"]}, {person["first"]}: {person["status"]} -> {data["status"]}', person['platoon'])
    return jsonify(dict(row))


PROFILE_FIELDS = (
    'phone', 'email', 'address', 'emergency_name', 'emergency_phone',
    'spouse_dependents', 'next_of_kin', 'dod_id', 'date_of_rank', 'mos',
    'clearance', 'ets_date', 'section', 'profile_notes',
    'flags', 'medical_date', 'dental_date', 'weapons_qual', 'dob',
)


@app.route('/api/personnel/<int:person_id>/profile', methods=['GET'])
@login_required
def get_profile(person_id):
    conn = get_db()
    person = conn.execute('SELECT id, rank, last, first, platoon FROM personnel WHERE id = %s', (person_id,)).fetchone()
    if person is None:
        return jsonify({'error': 'Not found'}), 404
    user = get_current_user()
    if not has_platoon_access(user, person['platoon']):
        return jsonify({'error': 'Forbidden'}), 403
    row = conn.execute('SELECT * FROM personnel_profile WHERE person_id = %s', (person_id,)).fetchone()
    profile = dict(row) if row else {'person_id': person_id, **{f: '' for f in PROFILE_FIELDS}}
    return jsonify(profile)


@app.route('/api/personnel/<int:person_id>/profile', methods=['PUT'])
@login_required
def update_profile(person_id):
    data = request.get_json() or {}
    conn = get_db()
    person = conn.execute('SELECT id, rank, last, first, platoon FROM personnel WHERE id = %s', (person_id,)).fetchone()
    if person is None:
        return jsonify({'error': 'Not found'}), 404
    user = get_current_user()
    if not has_platoon_access(user, person['platoon']):
        return jsonify({'error': 'Forbidden'}), 403

    updates = {f: data[f] for f in PROFILE_FIELDS if f in data}
    if not updates:
        return jsonify({'error': 'No fields to update'}), 400

    # Ensure a row exists, then update only the provided columns.
    conn.execute(
        'INSERT INTO personnel_profile (person_id) VALUES (%s) ON CONFLICT (person_id) DO NOTHING',
        (person_id,)
    )
    assignments = ', '.join(f'{col} = %s' for col in updates)
    conn.execute(
        f'UPDATE personnel_profile SET {assignments} WHERE person_id = %s',
        [*updates.values(), person_id]
    )
    row = conn.execute('SELECT * FROM personnel_profile WHERE person_id = %s', (person_id,)).fetchone()
    log_action('UPDATE_PROFILE', f'{person["rank"]} {person["last"]}, {person["first"]}', person['platoon'])
    return jsonify(dict(row))


@app.route('/api/personnel/<int:person_id>/schedule', methods=['POST'])
@login_required
def add_scheduled_event(person_id):
    data = request.get_json()
    conn = get_db()
    person = conn.execute('SELECT id, rank, last, first, platoon FROM personnel WHERE id = %s', (person_id,)).fetchone()
    if person is None:
        return jsonify({'error': 'Not found'}), 404
    user = get_current_user()
    if not has_platoon_access(user, person['platoon']):
        return jsonify({'error': 'Forbidden'}), 403

    status = data.get('status', '').strip()
    if status not in ABSENCE_STATUSES:
        return jsonify({'error': 'Invalid scheduled status'}), 400

    from_date = (data.get('from_date') or '').strip() or app_today()
    to_date = (data.get('to_date') or '').strip()

    # ponytail: a double-tapped Save used to insert a second identical row —
    # four of them once. The same person, status and window is never a real
    # second absence, so hand back the row that already exists.
    dup = conn.execute(
        'SELECT * FROM scheduled_events WHERE person_id = %s AND status = %s '
        'AND from_date = %s AND to_date = %s',
        (person_id, status, from_date, to_date)
    ).fetchone()
    if dup is not None:
        return jsonify(dict(dup)), 200

    # created_at comes from app_stamp(), not the column DEFAULT: the DEFAULT's
    # now() runs in the db container, whose timezone is UTC, which would stamp
    # a 2130 absence with tomorrow's date. See the Time section of CLAUDE.md.
    cur = conn.execute(
        'INSERT INTO scheduled_events (person_id, platoon, status, from_date, to_date, notes, location, state, created_at) '
        "VALUES (%s, %s, %s, %s, %s, %s, %s, 'scheduled', %s) RETURNING id",
        (person_id, person['platoon'], status, from_date, to_date,
         data.get('notes', ''), data.get('location', ''), app_stamp())
    )
    new_id = cur.fetchone()['id']
    _sync_person_status(conn, person_id, app_today())
    row = conn.execute('SELECT * FROM scheduled_events WHERE id = %s', (new_id,)).fetchone()
    log_action('SCHEDULE_STATUS', f'{person["rank"]} {person["last"]}: {status} on {data.get("from_date", "")}', person['platoon'])
    return jsonify(dict(row)), 201


@app.route('/api/directory', methods=['GET'])
@login_required
def get_directory():
    platoon = request.args.get('platoon', '2nd')
    user = get_current_user()
    if not has_platoon_access(user, platoon):
        return jsonify({'error': 'Forbidden'}), 403
    conn = get_db()
    _reconcile_absences(conn, app_today())
    rows = conn.execute(
        'SELECT p.id, p.rank, p.last, p.first, p.status, p.from_date, p.to_date, p.notes, '
        '       pp.dod_id, pp.dob, pp.mos, pp.section, pp.phone '
        'FROM personnel p LEFT JOIN personnel_profile pp ON pp.person_id = p.id '
        'WHERE p.platoon = %s ORDER BY p.rank, p.last, p.first', (platoon,)
    ).fetchall()
    upcoming = conn.execute(
        "SELECT person_id, status, from_date, to_date FROM scheduled_events "
        "WHERE platoon = %s AND state = 'scheduled' ORDER BY from_date, id", (platoon,)
    ).fetchall()
    next_by_person = {}
    for e in upcoming:
        next_by_person.setdefault(e['person_id'], dict(e))
    result = []
    for r in rows:
        item = dict(r)
        item['next_absence'] = next_by_person.get(r['id'])
        result.append(item)
    return jsonify(result)


# A planning question, not a logbook question: a year is plenty and it keeps
# the day-by-day loop below bounded whatever arrives in the query string.
MAX_AVAILABILITY_DAYS = 366


def _absence_covers(row, day_str):
    """Does this absence window cover `day_str`?

    Same open-ended semantics as _derive_state(): an empty from_date means
    "already started", an empty to_date means "open-ended".
    """
    if row['from_date'] and row['from_date'] > day_str:
        return False
    if row['to_date'] and row['to_date'] < day_str:
        return False
    return True


def _covered_days(row, start_str, end_str):
    """Which days of [start, end] this absence covers, as ISO strings."""
    day, end = date.fromisoformat(start_str), date.fromisoformat(end_str)
    days = []
    while day <= end:
        iso = day.isoformat()
        if _absence_covers(row, iso):
            days.append(iso)
        day += timedelta(days=1)
    return days


@app.route('/api/availability', methods=['GET'])
@login_required
def get_availability():
    """Who is free on a date (or across a range), and who is not, and why.

    Derived from scheduled_events, never from personnel.status: that column is
    only a display cache of *today*, so it cannot answer a question about next
    Tuesday.

    The rule on state: the dates decide, whatever the state says. 'completed'
    is a statement about today — _derive_state() completes a row the day after
    its to_date — so it is not evidence about the day being asked about. A row
    can be completed while its to_date is still in the future in exactly two
    ways: _sync_person_status() files a losing overlapping window as history,
    and a v1 backup restores everything unreconciled. In both the window itself
    was never cancelled, so the soldier really is spoken for and calling them
    available is the dangerous error. A soldier who genuinely came back early
    has their to_date rewritten to yesterday by _end_running_absence(), so that
    row can never cover a future day and drops out on the dates alone. One rule
    covers all of it, and nothing silently vanishes.
    """
    platoon = request.args.get('platoon', '2nd')
    user = get_current_user()
    if not has_platoon_access(user, platoon):
        return jsonify({'error': 'Forbidden'}), 403

    start = (request.args.get('date') or '').strip() or app_today()
    end = (request.args.get('to') or '').strip() or start
    try:
        span = (date.fromisoformat(end) - date.fromisoformat(start)).days + 1
    except ValueError:
        return jsonify({'error': 'Dates must be YYYY-MM-DD'}), 400
    if span < 1:
        return jsonify({'error': 'The end of the range is before the start'}), 400
    if span > MAX_AVAILABILITY_DAYS:
        return jsonify({'error': f'Ask about {MAX_AVAILABILITY_DAYS} days or fewer'}), 400

    conn = get_db()
    people = conn.execute(
        'SELECT id, rank, last, first, status FROM personnel WHERE platoon = %s '
        'ORDER BY rank, last, first', (platoon,)
    ).fetchall()
    # Windows that overlap the question at all; _covered_days works out which
    # days exactly. Open bounds are stored as '' and must not be compared.
    events = conn.execute(
        'SELECT * FROM scheduled_events WHERE platoon = %s '
        "AND (from_date = '' OR from_date <= %s) AND (to_date = '' OR to_date >= %s) "
        'ORDER BY from_date, id', (platoon, end, start)
    ).fetchall()

    by_person = {}
    for e in events:
        by_person.setdefault(e['person_id'], []).append(e)

    available, unavailable = [], []
    for p in people:
        who = {'id': p['id'], 'rank': p['rank'], 'last': p['last'], 'first': p['first']}
        covering = []
        for e in by_person.get(p['id'], []):
            hit = _covered_days(e, start, end)
            if hit:
                covering.append((e, hit))
        if not covering:
            available.append(who)
            continue
        # The newest window supplies the reason — the same tie-break
        # _sync_person_status() uses to pick the one current absence.
        primary = covering[-1][0]
        days = sorted({d for _, hit in covering for d in hit})
        unavailable.append({
            **who,
            'status': primary['status'],
            'from_date': primary['from_date'],
            'to_date': primary['to_date'],
            'notes': primary['notes'] or '',
            'location': primary['location'] or '',
            'days': days,
            'whole_range': len(days) == span,
        })

    return jsonify({
        'platoon': platoon, 'date': start, 'to': end, 'span': span,
        'available': available, 'unavailable': unavailable,
    })


@app.route('/api/personnel/<int:person_id>/absences', methods=['GET'])
@login_required
def get_absences(person_id):
    conn = get_db()
    person = conn.execute('SELECT id, platoon FROM personnel WHERE id = %s', (person_id,)).fetchone()
    if person is None:
        return jsonify({'error': 'Not found'}), 404
    user = get_current_user()
    if not has_platoon_access(user, person['platoon']):
        return jsonify({'error': 'Forbidden'}), 403
    _reconcile_absences(conn, app_today())
    rows = conn.execute(
        'SELECT * FROM scheduled_events WHERE person_id = %s ORDER BY from_date DESC, id DESC',
        (person_id,)
    ).fetchall()
    return jsonify({'absences': [dict(r) for r in rows]})


@app.route('/api/schedules/<int:event_id>', methods=['PUT'])
@login_required
def update_scheduled_event(event_id):
    data = request.get_json() or {}
    conn = get_db()
    row = conn.execute('SELECT * FROM scheduled_events WHERE id = %s', (event_id,)).fetchone()
    if row is None:
        return jsonify({'error': 'Not found'}), 404
    user = get_current_user()
    if not has_platoon_access(user, row['platoon']):
        return jsonify({'error': 'Forbidden'}), 403
    if row['state'] == 'completed':
        return jsonify({'error': 'That absence is already over and can no longer be edited.'}), 400

    status = (data.get('status') or '').strip()
    if status not in ABSENCE_STATUSES:
        return jsonify({'error': 'Invalid scheduled status'}), 400

    today = app_today()
    from_date = (data.get('from_date') or '').strip() or today
    to_date = (data.get('to_date') or '').strip()
    notes = data.get('notes', '')

    conn.execute(
        'UPDATE scheduled_events SET status = %s, from_date = %s, to_date = %s, notes = %s, location = %s '
        'WHERE id = %s',
        (status, from_date, to_date, notes, data.get('location', ''), event_id)
    )

    # An edit can move the window in either direction; _sync_person_status
    # re-derives the row's state from its new dates and owns the display cache.
    _sync_person_status(conn, row['person_id'], today)
    updated = conn.execute('SELECT * FROM scheduled_events WHERE id = %s', (event_id,)).fetchone()
    person = conn.execute('SELECT rank, last FROM personnel WHERE id = %s', (row['person_id'],)).fetchone()
    who = f'{person["rank"]} {person["last"]}: ' if person else ''
    log_action('EDIT_SCHEDULE', f'{who}{status} {from_date} - {to_date or "open"}', row['platoon'])
    return jsonify(dict(updated))


@app.route('/api/schedules/<int:event_id>', methods=['DELETE'])
@login_required
def delete_scheduled_event(event_id):
    conn = get_db()
    row = conn.execute('SELECT * FROM scheduled_events WHERE id = %s', (event_id,)).fetchone()
    if row is None:
        return jsonify({'error': 'Not found'}), 404
    user = get_current_user()
    if not has_platoon_access(user, row['platoon']):
        return jsonify({'error': 'Forbidden'}), 403
    person_id = row['person_id']
    conn.execute('DELETE FROM scheduled_events WHERE id = %s', (event_id,))
    # Cancelling an in-progress absence returns the soldier to duty.
    _sync_person_status(conn, person_id, app_today())
    log_action('DELETE_SCHEDULE', f'{row["status"]} on {row["from_date"]}', row['platoon'])
    return jsonify({'success': True})


@app.route('/api/personnel/<int:person_id>', methods=['DELETE'])
@login_required
def delete_person(person_id):
    conn = get_db()
    row = conn.execute('SELECT rank, last, first, platoon FROM personnel WHERE id = %s', (person_id,)).fetchone()
    if row is None:
        return jsonify({'error': 'Not found'}), 404
    user = get_current_user()
    if not has_platoon_access(user, row['platoon']):
        return jsonify({'error': 'Forbidden'}), 403
    log_action('DELETE_PERSON', f'{row["rank"]} {row["last"]}, {row["first"]}', row['platoon'])
    conn.execute('DELETE FROM scheduled_events WHERE person_id = %s', (person_id,))
    conn.execute('DELETE FROM personnel_profile WHERE person_id = %s', (person_id,))
    conn.execute('DELETE FROM personnel WHERE id = %s', (person_id,))
    return jsonify({'success': True})


@app.route('/api/settings', methods=['GET'])
@login_required
def get_settings():
    platoon = request.args.get('platoon', '2nd')
    key = f'unit_name_{platoon}'
    conn = get_db()
    row = conn.execute('SELECT value FROM settings WHERE key = %s', (key,)).fetchone()
    payload = {
        'unit_name': row['value'] if row else PLATOONS.get(platoon, f'{platoon} Platoon'),
        'tdy_schools': _get_tdy_list(conn, 'schools', platoon),
        'tdy_locations': _get_tdy_list(conn, 'locations', platoon),
        # Organisation-wide, not platoon-scoped: one duty day for everyone.
        'timezone': app_timezone(),
    }
    return jsonify(payload)


@app.route('/api/settings', methods=['PUT'])
@login_required
def update_settings():
    platoon = request.args.get('platoon', '2nd')
    user = get_current_user()
    if not has_platoon_access(user, platoon):
        return jsonify({'error': 'Forbidden'}), 403
    data = request.get_json()
    conn = get_db()

    # The timezone is the organisation's, so it is not gated on platoon access
    # like the rest of this route — it changes the duty day for every platoon,
    # which makes it an admin decision.
    new_tz = None
    if 'timezone' in data:
        if not (user and user.get('is_admin')):
            return jsonify({'error': 'Only an administrator can change the organisation timezone.'}), 403
        try:
            new_tz, _ = validate_timezone(data['timezone'])
        except ValueError as exc:
            return jsonify({'error': str(exc)}), 400
        conn.execute(
            'INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT(key) DO UPDATE SET value = %s',
            (TIMEZONE_KEY, new_tz, new_tz)
        )

    if 'unit_name' in data:
        key = f'unit_name_{platoon}'
        conn.execute(
            'INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT(key) DO UPDATE SET value = %s',
            (key, data['unit_name'], data['unit_name'])
        )
    logs = []
    for field, kind in (('tdy_schools', 'schools'), ('tdy_locations', 'locations')):
        if field not in data:
            continue
        try:
            cleaned = _clean_tdy_list(data[field])
        except ValueError as exc:
            return jsonify({'error': str(exc)}), 400
        value = json.dumps(cleaned)
        conn.execute(
            'INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT(key) DO UPDATE SET value = %s',
            (f'tdy_{kind}_{platoon}', value, value)
        )
        logs.append((f'Updated TDY {kind} list', f'{len(cleaned)} entries'))
    # Adopt the zone only now: every 4xx above it would have rolled the row
    # back while leaving this worker — and only this worker — on the new duty
    # day until the next restart.
    if new_tz:
        set_app_timezone(new_tz)
        logs.append(('ORG_TIMEZONE', f'Organisation timezone set to {new_tz}'))
    for action, details in logs:
        log_action(action, details, platoon)
    return get_settings()


# ── Audit log ──

@app.route('/api/audit', methods=['GET'])
@owner_required
def get_audit():
    platoon = request.args.get('platoon', '')
    try:
        limit = min(int(request.args.get('limit', 200)), 5000)
    except ValueError:
        limit = 200
    conn = get_db()
    if platoon:
        rows = conn.execute(
            'SELECT * FROM audit_log WHERE platoon = %s ORDER BY id DESC LIMIT %s', (platoon, limit)
        ).fetchall()
    else:
        rows = conn.execute('SELECT * FROM audit_log ORDER BY id DESC LIMIT %s', (limit,)).fetchall()
    return jsonify([dict(r) for r in rows])


# ── Duty roster ──
# A duty assignment conflicts when the soldier has an absence covering that
# date. This is a pure READER of scheduled_events: coverage is decided by
# _derive_state(row, date) == 'active', the lifecycle's own rule for "this
# window contains this day" (empty from_date = already started, empty to_date =
# open-ended), so duty and the roster can never disagree about a date. Both live
# states are searched — 'scheduled' is the whole point, since duty is usually
# planned before the absence starts — while 'completed' is terminal history.

_MONTHS_UPPER = ('JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
                 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC')

ABSENCE_LABELS = {'tdy': 'TDY', 'leave': 'leave', 'pass': 'pass', 'other': 'other', 'ftr': 'FTR'}


def _short_date(value):
    """'2026-09-07' -> '7SEP', matching the frontend's formatDateShort()."""
    try:
        d = date.fromisoformat(value)
    except (TypeError, ValueError):
        return ''
    return f'{d.day}{_MONTHS_UPPER[d.month - 1]}'


def _conflict_from(row):
    """Describe an absence row as a duty conflict, e.g. 'on leave 7SEP-20SEP'."""
    start, end = _short_date(row['from_date']), _short_date(row['to_date'])
    if start and end:
        span = f'{start}-{end}'
    elif start:
        span = f'from {start}'
    elif end:
        span = f'until {end}'
    else:
        span = 'dates open'
    return {
        'status': row['status'],
        'from_date': row['from_date'],
        'to_date': row['to_date'],
        'label': f'on {ABSENCE_LABELS.get(row["status"], row["status"])} {span}',
    }


def _duty_conflict(conn, person_id, date_str):
    """The absence covering date_str for this soldier, described, or None."""
    if not person_id or not date_str:
        return None
    rows = conn.execute(
        "SELECT * FROM scheduled_events WHERE person_id = %s AND state != 'completed' "
        'ORDER BY from_date, id', (person_id,)
    ).fetchall()
    # Newest window wins when two overlap, the same tie-break _sync_person_status uses.
    covering = [r for r in rows if _derive_state(r, date_str) == 'active']
    return _conflict_from(covering[-1]) if covering else None


@app.route('/api/duty', methods=['GET'])
@login_required
def get_duty():
    platoon = request.args.get('platoon', '2nd')
    user = get_current_user()
    if not has_platoon_access(user, platoon):
        return jsonify({'error': 'Forbidden'}), 403
    date_filter = request.args.get('date', '')
    conn = get_db()
    if date_filter:
        rows = conn.execute(
            'SELECT * FROM duty_roster WHERE platoon = %s AND date = %s ORDER BY duty_type, id',
            (platoon, date_filter)
        ).fetchall()
    else:
        rows = conn.execute(
            'SELECT * FROM duty_roster WHERE platoon = %s ORDER BY date DESC, duty_type, id LIMIT 90',
            (platoon,)
        ).fetchall()
    out = []
    for r in rows:
        entry = dict(r)
        entry['conflict'] = _duty_conflict(conn, entry['person_id'], entry['date'])
        out.append(entry)
    return jsonify(out)


@app.route('/api/duty/conflicts', methods=['GET'])
@login_required
def get_duty_conflicts():
    """Who in this platoon is away on a given date, keyed by person id.

    Feeds the duty picker so a soldier reads as unavailable *before* you assign
    them, not after.
    """
    platoon = request.args.get('platoon', '2nd')
    user = get_current_user()
    if not has_platoon_access(user, platoon):
        return jsonify({'error': 'Forbidden'}), 403
    date_str = request.args.get('date', '') or app_today()
    conn = get_db()
    rows = conn.execute(
        'SELECT s.* FROM scheduled_events s JOIN personnel p ON p.id = s.person_id '
        "WHERE p.platoon = %s AND s.state != 'completed' ORDER BY s.from_date, s.id",
        (platoon,)
    ).fetchall()
    # Same ordering as _duty_conflict, so a later overlapping window wins here too.
    away = {str(r['person_id']): _conflict_from(r)
            for r in rows if _derive_state(r, date_str) == 'active'}
    return jsonify(away)


@app.route('/api/duty', methods=['POST'])
@login_required
def add_duty():
    data = request.get_json()
    platoon = data.get('platoon', '2nd')
    user = get_current_user()
    if not has_platoon_access(user, platoon):
        return jsonify({'error': 'Forbidden'}), 403
    conn = get_db()
    try:
        person_id = int(data.get('person_id'))
    except (TypeError, ValueError):
        person_id = 0
    person = conn.execute('SELECT * FROM personnel WHERE id = %s', (person_id,)).fetchone()
    if person is None or person['platoon'] != platoon:
        return jsonify({'error': 'Pick a soldier from this platoon.'}), 400

    date_str = data.get('date', '')
    duty_type = data.get('duty_type', 'CQ')
    # rank/last/first come from the database, never the client: they are a
    # snapshot so the entry still reads correctly once the soldier is gone.
    cur = conn.execute(
        'INSERT INTO duty_roster (date, platoon, duty_type, person_id, rank, last, first, notes) '
        'VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id',
        (date_str, platoon, duty_type, person['id'],
         person['rank'], person['last'], person['first'], data.get('notes', ''))
    )
    new_id = cur.fetchone()['id']
    row = dict(conn.execute('SELECT * FROM duty_roster WHERE id = %s', (new_id,)).fetchone())
    # Warn, never block: assigning someone who is away is sometimes the real
    # answer, and a tool that refuses just gets worked around.
    row['conflict'] = _duty_conflict(conn, person['id'], date_str)
    detail = f'{duty_type} on {date_str} — {person["rank"]} {person["last"]}'
    if row['conflict']:
        detail += f' (CONFLICT: {row["conflict"]["label"]})'
    log_action('ADD_DUTY', detail, platoon)
    return jsonify(row), 201


@app.route('/api/duty/<int:entry_id>', methods=['DELETE'])
@login_required
def delete_duty(entry_id):
    conn = get_db()
    row = conn.execute('SELECT * FROM duty_roster WHERE id = %s', (entry_id,)).fetchone()
    user = get_current_user()
    if not row:
        return jsonify({'error': 'Not found'}), 404
    if not has_platoon_access(user, row['platoon']):
        return jsonify({'error': 'Forbidden'}), 403
    if row:
        log_action('DELETE_DUTY', f'{row["duty_type"]} on {row["date"]}', row['platoon'])
    conn.execute('DELETE FROM duty_roster WHERE id = %s', (entry_id,))
    return jsonify({'success': True})


# ── Report history ──

def _import_timestamp(value):
    """A client-supplied created_at, normalised, or None to use the default.

    Only ever set by the localStorage import, and never trusted blindly: a value
    that does not parse, or one in the future, falls back to 'now'.
    """
    if not value or not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None
    stamp = parsed.strftime('%Y-%m-%d %H:%M:%S')
    return stamp if stamp <= datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S') else None


def _prune_report_history(conn, platoon):
    """Keep only the most recent REPORT_HISTORY_MAX rows for a platoon."""
    conn.execute(
        'DELETE FROM report_history WHERE platoon = %s AND id NOT IN ('
        '  SELECT id FROM report_history WHERE platoon = %s ORDER BY id DESC LIMIT %s'
        ')',
        (platoon, platoon, REPORT_HISTORY_MAX)
    )


@app.route('/api/reports', methods=['GET'])
@login_required
def get_reports():
    platoon = request.args.get('platoon', '')
    user = get_current_user()
    if not has_platoon_access(user, platoon):
        return jsonify({'error': 'Forbidden'}), 403
    conn = get_db()
    rows = conn.execute(
        'SELECT id, unit_name, created_at, created_by FROM report_history '
        'WHERE platoon = %s ORDER BY id DESC LIMIT %s',
        (platoon, REPORT_HISTORY_MAX)
    ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route('/api/reports', methods=['POST'])
@login_required
def add_report():
    data = request.get_json() or {}
    platoon = data.get('platoon', '')
    user = get_current_user()
    if not has_platoon_access(user, platoon):
        return jsonify({'error': 'Forbidden'}), 403
    text = (data.get('text') or '').strip()
    if not text:
        return jsonify({'error': 'Report text is required.'}), 400
    unit_name = (data.get('unit_name') or '').strip()
    # The one-time localStorage import sends the report's original save time.
    # Without it every migrated report lands stamped today, which makes the
    # history actively misleading for a record people read by date.
    # Otherwise it is now, in the UNIT's timezone. Not the column DEFAULT:
    # that now() runs in the db container, which is UTC, so a report generated
    # at 2130 Sunday would be filed under Monday — the wrong duty day, on a
    # page people read by date. See the Time section of CLAUDE.md.
    created_at = _import_timestamp(data.get('created_at')) or app_stamp()
    conn = get_db()
    cur = conn.execute(
        'INSERT INTO report_history (platoon, unit_name, text, created_by, created_at) '
        'VALUES (%s, %s, %s, %s, %s) RETURNING id',
        (platoon, unit_name, text, user['username'], created_at)
    )
    new_id = cur.fetchone()['id']
    _prune_report_history(conn, platoon)
    row = conn.execute('SELECT * FROM report_history WHERE id = %s', (new_id,)).fetchone()
    log_action('SAVE_REPORT', unit_name, platoon)
    if not row:
        # Cannot happen with REPORT_HISTORY_MAX >= 1, but don't 500 if it ever does.
        return jsonify({'success': True}), 201
    return jsonify(dict(row)), 201


@app.route('/api/reports/<int:report_id>', methods=['GET'])
@login_required
def get_report(report_id):
    conn = get_db()
    row = conn.execute('SELECT * FROM report_history WHERE id = %s', (report_id,)).fetchone()
    if not row:
        return jsonify({'error': 'Not found'}), 404
    user = get_current_user()
    if not has_platoon_access(user, row['platoon']):
        return jsonify({'error': 'Forbidden'}), 403
    return jsonify(dict(row))


@app.route('/api/reports/<int:report_id>', methods=['DELETE'])
@owner_required
def delete_report(report_id):
    conn = get_db()
    row = conn.execute('SELECT * FROM report_history WHERE id = %s', (report_id,)).fetchone()
    if not row:
        return jsonify({'error': 'Not found'}), 404
    conn.execute('DELETE FROM report_history WHERE id = %s', (report_id,))
    log_action('DELETE_REPORT', row['unit_name'], row['platoon'])
    return jsonify({'success': True})


# ── Backup / Restore ──

@app.route('/api/backup', methods=['GET'])
@login_required
def export_backup():
    user = get_current_user()
    conn = get_db()
    import json
    from flask import Response

    if user['is_admin']:
        personnel = [dict(r) for r in conn.execute('SELECT * FROM personnel').fetchall()]
        scheduled_events = [dict(r) for r in conn.execute('SELECT * FROM scheduled_events').fetchall()]
        profiles  = [dict(r) for r in conn.execute('SELECT * FROM personnel_profile').fetchall()]
        settings  = [dict(r) for r in conn.execute('SELECT * FROM settings').fetchall()]
        users     = [dict(r) for r in conn.execute(
            'SELECT id, username, email, full_name, is_admin, platoons, clerk_user_id FROM users'
        ).fetchall()]
        label = 'full'
    else:
        accessible = [p.strip() for p in (user['platoons'] or '').split(',') if p.strip()]
        if not accessible:
            return jsonify({'error': 'No platoon access'}), 403
        placeholders = ','.join(['%s'] * len(accessible))
        personnel = [dict(r) for r in conn.execute(
            f'SELECT * FROM personnel WHERE platoon IN ({placeholders})', accessible
        ).fetchall()]
        scheduled_events = [dict(r) for r in conn.execute(
            f'SELECT * FROM scheduled_events WHERE platoon IN ({placeholders})', accessible
        ).fetchall()]
        profiles  = [dict(r) for r in conn.execute(
            f'SELECT pp.* FROM personnel_profile pp JOIN personnel p ON p.id = pp.person_id '
            f'WHERE p.platoon IN ({placeholders})', accessible
        ).fetchall()]
        settings  = [dict(r) for r in conn.execute('SELECT * FROM settings').fetchall()]
        users     = []
        label = '-'.join(accessible)

    payload = {
        'version': 2,
        'exported_at': datetime.utcnow().isoformat() + 'Z',
        'personnel': personnel,
        'scheduled_events': scheduled_events,
        'personnel_profile': profiles,
        'settings': settings,
        'users': users,
    }
    log_action('BACKUP_EXPORT', f'Backup exported ({label})')
    return Response(
        json.dumps(payload, indent=2),
        mimetype='application/json',
        headers={'Content-Disposition': f'attachment; filename=platoon-backup-{label}-{app_today()}.json'}
    )


@app.route('/api/backup/restore', methods=['POST'])
@login_required
def import_backup():
    user = get_current_user()
    payload = request.get_json()
    if not payload or payload.get('version') not in (1, 2):
        return jsonify({'error': 'Invalid or unsupported backup file'}), 400

    conn = get_db()
    accessible = [p.strip() for p in (user['platoons'] or '').split(',') if p.strip()]
    try:
        restored_personnel = 0
        # Non-admin restores re-insert personnel under fresh ids; map backup id -> new id
        # so scheduled_events and profiles reattach to the right people.
        person_id_map = {}
        # Tables an explicit id was inserted into. GENERATED BY DEFAULT AS IDENTITY
        # does not advance its sequence for an explicit-id insert, so any table
        # added here needs its sequence resynced before commit or the next
        # ordinary insert collides with a restored id.
        resync_id_tables = set()

        if 'personnel' in payload:
            if user['is_admin']:
                # FK cascade wipes profiles along with personnel; keep the current ones
                # when the backup carries none of its own (ids survive a full restore).
                preserved_profiles = []
                if 'personnel_profile' not in payload:
                    preserved_profiles = [dict(r) for r in conn.execute('SELECT * FROM personnel_profile').fetchall()]
                conn.execute('DELETE FROM scheduled_events')
                conn.execute('DELETE FROM personnel')
                restored_ids = set()
                # Two passes, not one interleaved loop: a row with an explicit
                # id never advances the identity sequence, so an id-less row
                # inserted *between* two explicit-id rows can draw an
                # auto-assigned id that a not-yet-processed explicit row later
                # collides with. Insert every explicit-id row first, resync the
                # sequence past all of them, then let the id-less rows
                # auto-assign — now guaranteed clear of every id this batch uses.
                explicit_rows = [p for p in payload['personnel'] if p.get('id') is not None]
                idless_rows = [p for p in payload['personnel'] if p.get('id') is None]
                for p in explicit_rows:
                    pid = p['id']
                    conn.execute(
                        'INSERT INTO personnel (id, rank, last, first, status, notes, from_date, to_date, present_date, platoon) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                        (pid, p.get('rank',''), p.get('last',''), p.get('first',''),
                         p.get('status','present'), p.get('notes',''),
                         p.get('from_date',''), p.get('to_date',''), p.get('present_date',''),
                         p.get('platoon','2nd'))
                    )
                    restored_ids.add(pid)
                    restored_personnel += 1
                if explicit_rows:
                    resync_id_tables.add('personnel')
                    conn.execute(
                        "SELECT setval(pg_get_serial_sequence('personnel', 'id'), "
                        "COALESCE((SELECT MAX(id) FROM personnel), 1), true)"
                    )
                for p in idless_rows:
                    # No id in the backup row: omit the column entirely so the
                    # identity assigns one, rather than inserting an explicit
                    # NULL (which GENERATED BY DEFAULT AS IDENTITY rejects —
                    # it is NOT NULL, and only an *omitted* column triggers
                    # identity generation).
                    conn.execute(
                        'INSERT INTO personnel (rank, last, first, status, notes, from_date, to_date, present_date, platoon) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)',
                        (p.get('rank',''), p.get('last',''), p.get('first',''),
                         p.get('status','present'), p.get('notes',''),
                         p.get('from_date',''), p.get('to_date',''), p.get('present_date',''),
                         p.get('platoon','2nd'))
                    )
                    restored_personnel += 1
                cols = ('person_id', *PROFILE_FIELDS)
                col_ph = ', '.join(['%s'] * len(cols))
                update_set = ', '.join(f'{c} = EXCLUDED.{c}' for c in cols[1:])
                for pp in preserved_profiles:
                    if pp['person_id'] in restored_ids:
                        conn.execute(
                            f'INSERT INTO personnel_profile ({", ".join(cols)}) VALUES ({col_ph}) '
                            f'ON CONFLICT (person_id) DO UPDATE SET {update_set}',
                            [pp.get(c, '') for c in cols]
                        )
            else:
                for p in payload['personnel']:
                    if p.get('platoon') not in accessible:
                        continue
                    conn.execute(
                        'DELETE FROM scheduled_events WHERE platoon = %s AND person_id IN ('
                        'SELECT id FROM personnel WHERE platoon = %s AND last = %s AND first = %s'
                        ')',
                        (p['platoon'], p['platoon'], p.get('last',''), p.get('first',''))
                    )
                    conn.execute(
                        'DELETE FROM personnel WHERE platoon = %s AND last = %s AND first = %s',
                        (p['platoon'], p.get('last',''), p.get('first',''))
                    )
                    cur = conn.execute(
                        'INSERT INTO personnel (rank, last, first, status, notes, from_date, to_date, present_date, platoon) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id',
                        (p.get('rank',''), p.get('last',''), p.get('first',''),
                         p.get('status','present'), p.get('notes',''),
                         p.get('from_date',''), p.get('to_date',''), p.get('present_date',''),
                         p.get('platoon','2nd'))
                    )
                    if p.get('id') is not None:
                        person_id_map[p['id']] = cur.fetchone()['id']
                    restored_personnel += 1

        if 'scheduled_events' in payload:
            # Resolve person_id / event_id for every row first, then insert in
            # two passes (explicit ids, then id-less) for the same reason as
            # the personnel loop above: an id-less row auto-assigned mid-batch
            # can otherwise collide with an explicit-id row processed later in
            # the same payload.
            resolved = []
            for s in payload['scheduled_events']:
                if user['is_admin']:
                    person_id = s.get('person_id')
                else:
                    if s.get('platoon') not in accessible:
                        continue
                    person_id = person_id_map.get(s.get('person_id'))
                    if person_id is None:
                        continue
                # A non-admin restore never carries a trustworthy backup id (the
                # row is reattached to a remapped person_id), and an admin
                # backup row may simply lack one. Either way there is no id to
                # conflict on, so omit the column and let the identity assign —
                # an explicit NULL there is a NOT NULL violation, not an
                # auto-assign, under GENERATED BY DEFAULT AS IDENTITY.
                event_id = s.get('id') if user['is_admin'] else None
                resolved.append((event_id, person_id, s))

            for event_id, person_id, s in resolved:
                if event_id is None:
                    continue
                conn.execute(
                    'INSERT INTO scheduled_events (id, person_id, platoon, status, from_date, to_date, notes, location, created_at, state) '
                    'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) '
                    'ON CONFLICT (id) DO UPDATE SET person_id = EXCLUDED.person_id, platoon = EXCLUDED.platoon, '
                    'status = EXCLUDED.status, from_date = EXCLUDED.from_date, to_date = EXCLUDED.to_date, '
                    'notes = EXCLUDED.notes, location = EXCLUDED.location, created_at = EXCLUDED.created_at, '
                    'state = EXCLUDED.state',
                    (event_id, person_id, s.get('platoon', '2nd'),
                     s.get('status', ''), s.get('from_date', ''), s.get('to_date', ''),
                     s.get('notes', ''), s.get('location', ''), s.get('created_at', datetime.utcnow().isoformat()),
                     s.get('state', 'scheduled'))
                )
            if any(event_id is not None for event_id, _, _ in resolved):
                resync_id_tables.add('scheduled_events')
                conn.execute(
                    "SELECT setval(pg_get_serial_sequence('scheduled_events', 'id'), "
                    "COALESCE((SELECT MAX(id) FROM scheduled_events), 1), true)"
                )

            for event_id, person_id, s in resolved:
                if event_id is not None:
                    continue
                conn.execute(
                    'INSERT INTO scheduled_events (person_id, platoon, status, from_date, to_date, notes, location, created_at, state) '
                    'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)',
                    (person_id, s.get('platoon', '2nd'),
                     s.get('status', ''), s.get('from_date', ''), s.get('to_date', ''),
                     s.get('notes', ''), s.get('location', ''), s.get('created_at', datetime.utcnow().isoformat()),
                     s.get('state', 'scheduled'))
                )

        if 'personnel_profile' in payload:
            cols = ('person_id', *PROFILE_FIELDS)
            col_ph = ', '.join(['%s'] * len(cols))
            update_set = ', '.join(f'{c} = EXCLUDED.{c}' for c in cols[1:])
            if user['is_admin']:
                conn.execute('DELETE FROM personnel_profile')
                for pp in payload['personnel_profile']:
                    conn.execute(
                        f'INSERT INTO personnel_profile ({", ".join(cols)}) VALUES ({col_ph}) '
                        f'ON CONFLICT (person_id) DO UPDATE SET {update_set}',
                        [pp.get(c, '') for c in cols]
                    )
            else:
                for pp in payload['personnel_profile']:
                    new_id = person_id_map.get(pp.get('person_id'))
                    if new_id is None:
                        continue
                    conn.execute(
                        f'INSERT INTO personnel_profile ({", ".join(cols)}) VALUES ({col_ph}) '
                        f'ON CONFLICT (person_id) DO UPDATE SET {update_set}',
                        [new_id, *[pp.get(c, '') for c in PROFILE_FIELDS]]
                    )

        if 'settings' in payload:
            if user['is_admin']:
                conn.execute('DELETE FROM settings')
                for s in payload['settings']:
                    conn.execute('INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value', (s['key'], s['value']))
            else:
                allowed_keys = {f'unit_name_{p}' for p in accessible}
                for s in payload['settings']:
                    if s.get('key') in allowed_keys:
                        conn.execute('INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value', (s['key'], s['value']))

        restored_users = 0
        if user['is_admin'] and 'users' in payload:
            current_uid = user['id']
            conn.execute('DELETE FROM users WHERE id != %s', (current_uid,))
            for u in payload['users']:
                if u['id'] == current_uid:
                    continue
                conn.execute(
                    'INSERT INTO users (id, username, password_hash, is_admin, platoons, clerk_user_id, email, full_name, pin_hash) '
                    'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) '
                    'ON CONFLICT (id) DO UPDATE SET username = EXCLUDED.username, '
                    'password_hash = EXCLUDED.password_hash, is_admin = EXCLUDED.is_admin, '
                    'platoons = EXCLUDED.platoons, clerk_user_id = EXCLUDED.clerk_user_id, '
                    'email = EXCLUDED.email, full_name = EXCLUDED.full_name, pin_hash = EXCLUDED.pin_hash',
                    (u['id'], u['username'], PLACEHOLDER_PASSWORD_HASH, u.get('is_admin', 0),
                     u.get('platoons', ''), u.get('clerk_user_id', ''), u.get('email', ''),
                     u.get('full_name', ''), '')
                )
                resync_id_tables.add('users')
                restored_users += 1

        # Training data in older (version 1) backups is intentionally ignored —
        # the 350-1 tracker feature was removed.
        # Explicit-id inserts above (personnel/scheduled_events/users, admin-only)
        # do not advance GENERATED BY DEFAULT AS IDENTITY's sequence the way a
        # normal insert does, so the next ordinary insert into that table would
        # collide with a restored id. Resync before commit, only for tables this
        # restore actually gave an explicit id to.
        for table in resync_id_tables:
            conn.execute(
                f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), "
                f"COALESCE((SELECT MAX(id) FROM {table}), 1), true)"
            )
        log_action('BACKUP_RESTORE', f'Backup restored: {restored_personnel} personnel, {restored_users} users')
        return jsonify({'success': True, 'personnel': restored_personnel, 'users': restored_users})
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500


@app.route('/api/activate-scheduled', methods=['POST'])
@login_required
def activate_scheduled():
    today_str = app_today()
    conn = get_db()
    result = _reconcile_absences(conn, today_str)
    return jsonify(result)


# ── Reset route (used by auto-reset and manual reset) ──

@app.route('/api/reset', methods=['POST'])
@login_required
def reset_day():
    data = request.get_json() or {}
    platoon = data.get('platoon', '')
    user = get_current_user()
    if platoon:
        if not has_platoon_access(user, platoon):
            return jsonify({'error': 'Forbidden'}), 403
    elif not user['is_admin']:
        # A company-wide reset touches every platoon; only admins may do it.
        return jsonify({'error': 'Forbidden'}), 403
    conn = get_db()
    if platoon:
        conn.execute(
            "UPDATE personnel SET present_date = '' WHERE status = 'present' AND platoon = %s",
            (platoon,)
        )
    else:
        conn.execute("UPDATE personnel SET present_date = '' WHERE status = 'present'")
    log_action('RESET_DAY', f'Day reset for platoon: {platoon or "all"}', platoon)
    return jsonify({'success': True})


# ── Midnight auto-reset background thread ──

def _absence_audit(conn, action, row, details):
    conn.execute(
        'INSERT INTO audit_log (user_id, username, action, details, platoon, timestamp) '
        'VALUES (0, %s, %s, %s, %s, %s)',
        ('system', action, details, row['platoon'], app_stamp())
    )


def _derive_state(row, today_str):
    """The state a live absence row should be in today, from its dates alone.

    An empty from_date means "already started" and an empty to_date means
    "open-ended" — the two bounds are optional in the same way.
    """
    if row['to_date'] and row['to_date'] < today_str:
        return 'completed'
    if row['from_date'] and row['from_date'] > today_str:
        return 'scheduled'
    return 'active'


def _end_running_absence(conn, person_id, today_str):
    """Close out whatever absence is running when a soldier is marked present.

    Marking someone present used to leave the active scheduled_events row alone,
    so the roster said 'present' while an absence was still running underneath —
    and weeks later the roster produced an ABSENCE_COMPLETE for an absence that
    was never recorded as taken. Coming back is an early return: the absence
    ends yesterday. One that had not started yet was a mis-entry, so it goes.

    Returns the number of rows closed or removed.
    """
    yesterday = (date.fromisoformat(today_str) - timedelta(days=1)).isoformat()
    running = conn.execute(
        "SELECT * FROM scheduled_events WHERE person_id = %s AND state = 'active'",
        (person_id,)
    ).fetchall()
    for row in running:
        if row['from_date'] and row['from_date'] > yesterday:
            conn.execute('DELETE FROM scheduled_events WHERE id = %s', (row['id'],))
            _absence_audit(conn, 'ABSENCE_CANCELLED', row,
                           f'person {person_id}: {row["status"]} from {row["from_date"]} '
                           'removed — marked present before it began')
        else:
            conn.execute(
                "UPDATE scheduled_events SET to_date = %s, state = 'completed' WHERE id = %s",
                (yesterday, row['id'])
            )
            _absence_audit(conn, 'ABSENCE_ENDED_EARLY', row,
                           f'person {person_id}: {row["status"]} cut short at {yesterday} '
                           '— marked present')
    return len(running)


def _sync_person_status(conn, person_id, today_str):
    """THE single owner of personnel's absence display cache.

    Re-derives each of this person's live scheduled_events rows from its dates —
    in both directions, so an edit that pushes a window into the future demotes
    an active row back to 'scheduled' — then writes
    personnel.status/from_date/to_date/notes from the one absence that is current
    today, or clears them back to 'present' when none is. Nothing else may write
    those four columns for an absence reason.

    'completed' is terminal: history rows are never resurrected.

    Returns {'activated': n, 'completed': n} counting only the date-driven
    transitions, which are the ones that get an audit row.
    """
    live = conn.execute(
        "SELECT * FROM scheduled_events WHERE person_id = %s AND state != 'completed' "
        'ORDER BY from_date, id', (person_id,)
    ).fetchall()

    # Only one absence can be current; the newest window wins and the rest are
    # filed as history, otherwise a stale row would later "return" the soldier
    # mid-absence.
    current_ids = [r['id'] for r in live if _derive_state(r, today_str) == 'active']
    winner_id = current_ids[-1] if current_ids else None

    activated = completed = 0
    winner_is_new = False
    for r in live:
        natural = _derive_state(r, today_str)
        want = 'completed' if (natural == 'active' and r['id'] != winner_id) else natural
        if want == r['state']:
            continue
        conn.execute('UPDATE scheduled_events SET state = %s WHERE id = %s', (want, r['id']))
        if want == 'active':
            winner_is_new = True
            activated += 1
            _absence_audit(conn, 'ABSENCE_ACTIVATE', r,
                           f'person {r["person_id"]}: {r["status"]} from {r["from_date"]}')
        elif r['state'] == 'active' and natural == 'completed':
            completed += 1
            _absence_audit(conn, 'ABSENCE_COMPLETE', r,
                           f'person {r["person_id"]}: {r["status"]} ended {r["to_date"]}')

    person = conn.execute('SELECT status FROM personnel WHERE id = %s', (person_id,)).fetchone()
    if person is None:
        return {'activated': activated, 'completed': completed}
    # Only ever overwrite a cached absence (or a fresh activation). That leaves a
    # soldier someone marked present by hand alone until the absence that is
    # still running actually ends.
    cached_absence = person['status'] in ABSENCE_STATUSES
    if winner_id is not None:
        if winner_is_new or cached_absence:
            w = next(r for r in live if r['id'] == winner_id)
            conn.execute(
                'UPDATE personnel SET status=%s, from_date=%s, to_date=%s, notes=%s WHERE id=%s',
                (w['status'], w['from_date'], w['to_date'], w['notes'], person_id)
            )
    elif cached_absence:
        conn.execute(
            "UPDATE personnel SET status='present', from_date='', to_date='', notes='' WHERE id = %s",
            (person_id,)
        )
    return {'activated': activated, 'completed': completed}


def _reconcile_absences(conn, today_str):
    """Advance the absence lifecycle for everyone who still has a live event.

    ponytail: one _sync_person_status() pass per person with a live row — a
    handful of people per platoon, so the per-person queries are cheaper than
    the set-based version was to keep correct.
    """
    totals = {'activated': 0, 'completed': 0}
    people = conn.execute(
        "SELECT DISTINCT person_id FROM scheduled_events WHERE state != 'completed' ORDER BY person_id"
    ).fetchall()
    for p in people:
        counts = _sync_person_status(conn, p['person_id'], today_str)
        totals['activated'] += counts['activated']
        totals['completed'] += counts['completed']
    return totals


def _midnight_reset_worker():
    last_reset_date = None
    while True:
        now = app_now()
        today = now.date()
        today_str = today.isoformat()
        if now.hour == 0 and now.minute == 0 and today != last_reset_date:
            try:
                conn = get_db()
                conn.execute("UPDATE personnel SET present_date = '' WHERE status = 'present'")
                result = _reconcile_absences(conn, today_str)
                conn.commit()
                conn.close()
                last_reset_date = today
                print(f'[auto-reset] Day reset at {now}; absences reconciled: {result}', flush=True)
            except Exception as e:
                print(f'[auto-reset] Error: {e}', flush=True)
        time.sleep(30)


if __name__ == '__main__':
    init_db()
    t = threading.Thread(target=_midnight_reset_worker, daemon=True)
    t.start()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=True)
