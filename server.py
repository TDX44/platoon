import base64
import binascii
import json
import logging
import os
import re
import secrets
import string
import struct
import time
from datetime import datetime, date, timezone
from functools import wraps
from datetime import timedelta
from zoneinfo import ZoneInfo
from urllib.error import URLError
from urllib.parse import quote
from urllib.request import Request, urlopen
from flask import Flask, Response, request, jsonify, redirect, send_from_directory, session, g, has_request_context
from werkzeug.security import generate_password_hash
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.exceptions import HTTPException
import jwt
from jwt import PyJWKClient
from jwt.exceptions import PyJWKClientConnectionError
import psycopg
from psycopg.rows import dict_row
import stripe

import billing_rules

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
CLERK_ENABLED = bool(CLERK_PUBLISHABLE_KEY and CLERK_JWKS_URL)
# lifespan: the default refetches Clerk's key set every 5 minutes, so any DNS or
# network blip had ~288 chances a day to land on a refetch and 401 everyone. A
# rotated key still refreshes immediately, because PyJWKClient refetches on a
# kid miss. timeout: the 30s default would park a sync gunicorn worker.
_JWKS_CLIENT = PyJWKClient(CLERK_JWKS_URL, lifespan=3600, timeout=5) if CLERK_ENABLED else None
_JWKS_LAST_GOOD = None
CLERK_UNREACHABLE = 'Sign-in is temporarily unavailable — could not reach Clerk. Try again in a moment.'

# ── The platform operator ──
# One person runs this instance, and /api/admin/* is theirs: read-only, across
# every tenant. The grant is an email address, and the only trustworthy source
# for one is Clerk itself. `users.email` is written from the /api/auth/sync
# request BODY, so it is the caller's own claim about themselves and can never
# be more than a hint; the session JWT carries `sub` and no email (this
# instance uses Clerk's default token, which has no email claim), so the
# address is fetched from Clerk's Backend API keyed on that `sub`.
# Set PLATFORM_ADMIN_EMAILS to an empty string to turn the dashboard off.
PLATFORM_ADMIN_EMAILS = frozenset(
    e.strip().lower() for e in
    os.environ.get('PLATFORM_ADMIN_EMAILS', 'jonathon.carr5@gmail.com').split(',') if e.strip())
CLERK_SECRET_KEY = os.environ.get('CLERK_SECRET_KEY', '').strip()
CLERK_API_USERS = 'https://api.clerk.com/v1/users/'
CLERK_API_TIMEOUT = 3
PLATFORM_ADMIN_TTL_SECONDS = 300
# A failure gets its own, much shorter TTL. It has to be cached at all because
# gunicorn runs two SYNC workers and this lookup blocks: with Clerk's API
# unreachable but sessions still valid on the stale-key fallback, one browser
# polling /api/me would park a worker per request, and two in flight is the
# whole app down for every tenant — which anyone can aim on purpose by putting
# the operator's address in their own users.email. But it is cached as
# UNKNOWN, never as a refusal: a five-minute negative would outlive the outage
# that caused it, and it must never be cached as a grant.
PLATFORM_ADMIN_FAIL_TTL_SECONDS = 30
# {clerk_user_id: (expires_at_monotonic, True | False | UNKNOWN)}. Per worker
# process, so a restart simply re-asks. Never keyed on anything the caller typed.
PLATFORM_ADMIN_UNKNOWN = None
_PLATFORM_ADMIN_CACHE = {}

# ── Billing ──
# One Stripe mode per process. Dev runs `test`, production `live`; the key
# and webhook secret are read for that mode only. No key means billing is
# OFF for this instance — every account is complimentary — because a lock
# nobody can pay their way out of is a trap, and CI has no key.
BILLING_DEFAULT_ON = os.environ.get('BILLING_DEFAULT', 'on').strip().lower() == 'on'
STRIPE_MODE = os.environ.get('STRIPE_MODE', 'test').strip().lower()
if STRIPE_MODE not in ('test', 'live'):
    raise SystemExit(f'STRIPE_MODE must be test or live, not {STRIPE_MODE!r}')
STRIPE_SECRET_KEY = os.environ.get(f'STRIPE_{STRIPE_MODE.upper()}_SECRET_KEY', '').strip()
STRIPE_WEBHOOK_SECRET = os.environ.get(f'STRIPE_{STRIPE_MODE.upper()}_WEBHOOK_SECRET', '').strip()
STRIPE_ENABLED = bool(STRIPE_SECRET_KEY)
# Both or neither. A key with no webhook secret charges cards while every
# delivery is answered 503, so the app never learns what it sold: an account
# pays and stays locked, a cancellation never lands. Taking money that cannot
# be reconciled is worse than not booting.
if STRIPE_ENABLED and not STRIPE_WEBHOOK_SECRET:
    raise SystemExit(f'STRIPE_{STRIPE_MODE.upper()}_WEBHOOK_SECRET is required whenever '
                     f'STRIPE_{STRIPE_MODE.upper()}_SECRET_KEY is set: without it cards are '
                     'charged and every Stripe webhook delivery is refused.')
STRIPE_TIMEOUT = 5
PRICE_LOOKUP_KEYS = ('platoon_leader_monthly', 'platoon_leader_annual')
# The ways out of a lock. Everything else under /api/ answers 402 to a locked
# account. /api/me is exact; the rest are prefixes.
BILLING_EXEMPT_PREFIXES = ('/api/auth/', '/api/billing/', '/api/admin/')
WEBHOOK_MAX_BYTES = 64 * 1024
if STRIPE_ENABLED:
    stripe.api_key = STRIPE_SECRET_KEY
    # gunicorn runs two SYNC workers: a hung call to Stripe parks one, and a
    # retry parks it twice as long. Short timeout, no retries.
    stripe.default_http_client = stripe.RequestsClient(timeout=STRIPE_TIMEOUT)
    stripe.max_network_retries = 0
else:
    logging.getLogger(__name__).warning(
        'STRIPE_%s_SECRET_KEY is not set: billing is off, every account is complimentary', STRIPE_MODE.upper())

# The duty day belongs to the unit, not to the server or the viewer. prodsrv02
# runs UTC, so date.today() rolled over at 1900 local and marked people away for
# a course starting the next morning. Every "what day is it" question goes
# through app_today(); nothing reads date.today() directly.
#
# The zone belongs to one ROOT, not to the process: every unit in a tenant
# shares a duty day, so it is stored once per root (unit_id IS NULL) and read
# once per request into g.tz by whatever declares the tenant. Outside a request
# there is no tenant to ask, so FALLBACK_TZ stands in.
FALLBACK_TZ = os.environ.get('PLATOON_TZ', 'America/Chicago')
TIMEZONE_KEY = 'org_timezone'


def validate_timezone(name):
    """The normalised zone name, or ValueError. Changes nothing."""
    name = (name or '').strip()
    try:
        return name, ZoneInfo(name)
    except Exception:
        raise ValueError(f'{name!r} is not a known timezone')


# A bad PLATOON_TZ is a boot failure, not a 500 on every request that asks the
# time. The old module-level ZoneInfo(FALLBACK_TZ) caught it here; keep that.
validate_timezone(FALLBACK_TZ)


def _tenant_timezone(conn, root_id):
    """The stored zone for a root, falling back rather than failing: a bad
    stored value must never take the app down."""
    row = conn.execute(
        'SELECT value FROM settings WHERE root_id = %s AND unit_id IS NULL AND key = %s',
        (root_id, TIMEZONE_KEY)).fetchone()
    if row and row['value']:
        try:
            return validate_timezone(row['value'])[0]
        except ValueError:
            app.logger.warning('Stored timezone %r for root %s is not valid; using %s',
                               row['value'], root_id, FALLBACK_TZ)
    return FALLBACK_TZ


def app_timezone():
    """The signed-in organization's timezone name inside a request; the
    fallback outside one (init_db, tests' fixtures, the CLI scripts)."""
    if has_request_context() and getattr(g, 'tz', None):
        return g.tz
    return FALLBACK_TZ


def app_now():
    # ZoneInfo caches its instances, so ZoneInfo(name) per call is a dict
    # lookup, not a file read -- no module-level handle needed.
    return datetime.now(ZoneInfo(app_timezone()))


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
# One `settings` row per unit (keys tdy_schools / tdy_locations, scoped by
# root_id + unit_id) holding a JSON array, seeded empty when the unit is
# created and owned from there on by the TDY Lists page.
TDY_LIST_MAX_ITEMS = 300
TDY_LIST_MAX_LEN = 80

# Report history: generated reports were per-device localStorage; now server-side
# so every device sees the same record. Capped per unit so it cannot grow forever.
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


def _get_tdy_list(conn, kind, unit_id):
    row = conn.execute('SELECT value FROM settings WHERE unit_id = %s AND key = %s',
                       (unit_id, f'tdy_{kind}')).fetchone()
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
    import, the CLI scripts, the tests — this hands back a fresh plain
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
    organizations. An unattached user declares the empty string, which NULLIF
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


# Client routes that are not a unit. parseAppRoute() in index.html looks a
# slug up before it considers anything else, so a unit that took one of these
# would either shadow the page or — if the page wins — be unreachable itself.
# Reserving the slug rather than refusing the NAME keeps "Admin" a legal thing
# to call a section; only the URL moves aside.
RESERVED_SLUGS = ('admin',)


def slugify(name):
    s = re.sub(r'[^a-z0-9]+', '-', (name or '').lower()).strip('-')
    if s in RESERVED_SLUGS:
        return f'{s}-unit'
    return s or 'unit'


def unique_slug(conn, root_id, name):
    base = slugify(name)
    slug, n = base, 1
    while conn.execute('SELECT 1 FROM units WHERE root_id = %s AND slug = %s', (root_id, slug)).fetchone():
        n += 1
        slug = f'{base}-{n}'
    return slug


def _root():
    return g.current_user['root_id']


def _unit_scope(raw):
    """(unit_id, ids of its subtree) for a ?unit= / body unit_id the caller may
    see, else None. A unit's roster is its whole subtree: a company view is
    the company, a platoon view is the platoon and its squads."""
    try:
        unit_id = int(raw)
    except (TypeError, ValueError):
        return None
    if not can_access(unit_id):
        return None
    return unit_id, subtree_ids(get_db(), unit_id)


def _person_or_none(conn, person_id):
    """The soldier's scoping row, or None if it does not exist *from here*
    (another tenant's soldier is a 404 by RLS, never a 403)."""
    return conn.execute(
        'SELECT id, rank, last, first, status, unit_id, root_id FROM personnel WHERE id = %s', (person_id,)
    ).fetchone()


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
                unit_id      INTEGER NOT NULL,
                root_id      INTEGER NOT NULL
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                root_id INTEGER NOT NULL,
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
                clerk_user_id TEXT DEFAULT '',
                email         TEXT DEFAULT '',
                full_name     TEXT DEFAULT '',
                unit_id       INTEGER,
                role          TEXT NOT NULL DEFAULT 'leader',
                root_id       INTEGER
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
                unit_id   INTEGER,
                root_id   INTEGER NOT NULL
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS duty_roster (
                id        INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                date      TEXT NOT NULL,
                unit_id   INTEGER NOT NULL,
                root_id   INTEGER NOT NULL,
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
                unit_id    INTEGER NOT NULL,
                root_id    INTEGER NOT NULL,
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
                root_id           INTEGER NOT NULL,
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
                unit_id     INTEGER NOT NULL,
                role        TEXT NOT NULL DEFAULT 'leader',
                root_id     INTEGER NOT NULL,
                created_by  TEXT DEFAULT '',
                created_at  TEXT DEFAULT (to_char(now(), 'YYYY-MM-DD HH24:MI:SS')),
                expires_at  TEXT DEFAULT '',
                accepted_at TEXT DEFAULT '',
                accepted_by TEXT DEFAULT ''
            )
        ''')

        # ── Billing (see docs/superpowers/specs/2026-09-17-stripe-billing-design.md) ──
        # One row per leader account, created lazily at the first attached
        # sign-in. Tenant data: root_id + the same RLS policy as everything
        # else. The Stripe columns mirror Stripe's own words and are written
        # only by billing_apply_stripe() from the webhook; the trial stamps and
        # billing_mode are ours. Billing time is UTC (timestamptz), not the
        # duty day.
        cur.execute('''
            CREATE TABLE IF NOT EXISTS subscriptions (
                user_id                 INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
                root_id                 INTEGER NOT NULL,
                billing_mode            TEXT NOT NULL DEFAULT 'default'
                                        CHECK (billing_mode IN ('default', 'comped', 'billed')),
                trial_started_at        TIMESTAMPTZ,
                trial_ends_at           TIMESTAMPTZ,
                extended_at             TIMESTAMPTZ,
                stripe_customer_id      TEXT UNIQUE,
                stripe_subscription_id  TEXT UNIQUE,
                stripe_status           TEXT,
                stripe_price_lookup_key TEXT,
                current_period_end      TIMESTAMPTZ,
                cancel_at_period_end    BOOLEAN NOT NULL DEFAULT false,
                comped_by               TEXT,
                updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        ''')
        # Every webhook delivery id ever accepted, so a replay is a no-op.
        # Not tenant data; only billing_record_event() writes it.
        cur.execute('''
            CREATE TABLE IF NOT EXISTS stripe_events (
                event_id    TEXT PRIMARY KEY,
                received_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS report_history (
                id         INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                unit_id    INTEGER NOT NULL,
                root_id    INTEGER NOT NULL,
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
        #    boots; scripts/platoons-to-units.py then backfills them, tightens
        #    them to NOT NULL and drops the old fixed-platoon columns. A fresh
        #    database already has the strict shape from the CREATE TABLEs
        #    above, so every ALTER below is a no-op on it. ──
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
        if scols and 'state' not in scols:
            cur.execute("ALTER TABLE scheduled_events ADD COLUMN state TEXT DEFAULT 'scheduled'")
            # Old-model rows whose whole window already passed were never activated
            # (activation was broken in production); file them as history.
            # The cutoff comes from Python, not from date('now','localtime') —
            # that is SQLite syntax Postgres has no function for, and the
            # database's own clock is the wrong clock anyway (the db container
            # is UTC). init_db() runs outside any request, so this is
            # PLATOON_TZ rather than a tenant's stored zone; for a one-shot
            # backfill of windows that already ended, a few hours either side
            # of midnight is immaterial, and it beats UTC.
            cur.execute(
                "UPDATE scheduled_events SET state = 'completed' "
                "WHERE to_date != '' AND to_date < %s", (app_today(),)
            )
        # Duty entries predate person_id and stored only a name snapshot.
        # Deliberately not a foreign key: deleting a soldier must not erase
        # history. The one-off backfill that linked the old snapshots to
        # soldiers matched on the fixed platoon, so it went with `platoon`;
        # production ran it long before the cutover.
        if 'person_id' not in _columns(cur, 'duty_roster'):
            cur.execute('ALTER TABLE duty_roster ADD COLUMN person_id INTEGER')

        # ── Seed legacy admin user only when Clerk is not configured ──
        cur.execute('SELECT COUNT(*) AS n FROM users')
        if cur.fetchone()['n'] == 0 and not CLERK_ENABLED:
            password = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(12))
            # Attached to no unit: signing in, they are offered "create a
            # unit", and creating a root makes them its owner.
            cur.execute(
                'INSERT INTO users (username, password_hash) VALUES (%s, %s) '
                'ON CONFLICT (username) DO NOTHING',
                ('admin', generate_password_hash(password))
            )
            import sys
            sys.stderr.write(f'\n{"=" * 52}\n')
            sys.stderr.write(f'  First-run admin account created\n')
            sys.stderr.write(f'  Username : admin\n')
            sys.stderr.write(f'  Password : {password}\n')
            sys.stderr.write(f'  Change this password after first login!\n')
            sys.stderr.write(f'{"=" * 52}\n\n')
            sys.stderr.flush()

        # ── RLS policies and the pre-tenant front door: every boot, idempotent ──
        for name in ('rls.sql', 'auth_functions.sql', 'admin_functions.sql', 'billing_functions.sql'):
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

def log_action(action, details='', unit_id=None):
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
            'INSERT INTO audit_log (user_id, username, action, details, unit_id, timestamp, root_id) '
            'VALUES (%s, %s, %s, %s, %s, %s, %s)',
            (user_id, username, action, str(details),
             unit_id if isinstance(unit_id, int) else None, app_stamp(), root_id)
        )
    except Exception:
        app.logger.exception(
            'audit log write failed for action %r (unit %r) — the request '
            'transaction is now aborted and its change will NOT be committed',
            action, unit_id)


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
# Sign-up is open: anyone with a Clerk account gets a row here, attached to
# nothing, and either creates their own tree or presents an invite. An invite
# is what attaches an account to somebody else's tree, at one unit and in one
# role.
INVITE_EXPIRY_DAYS = 7


def _display_name_for_user(payload):
    for key in ('full_name', 'username', 'email'):
        value = (payload.get(key) or '').strip()
        if value:
            return value
    return 'User'


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
    # Pre-tenant, so this is FALLBACK_TZ: it is only the coarse filter that
    # fetches the invite row. The expiry that counts is re-checked below on the
    # invite's OWN tenant clock, which is the clock create_invite wrote it on.
    # ponytail: when FALLBACK_TZ runs ahead of the tenant's zone this filter is
    # the stricter of the two and can drop an invite a few hours early. That
    # fails safe (it never admits an expired one) and the alternative is
    # changing auth_invite(), which belongs to Task 1's sql/auth_functions.sql.
    now = app_stamp()
    try:
        existing = conn.execute('SELECT * FROM auth_user_by_clerk_id(%s)', (clerk_user_id,)).fetchone()
        if existing:
            set_tenant(conn, existing.get('root_id'))
            g.tz = _tenant_timezone(conn, existing['root_id']) if existing.get('root_id') else FALLBACK_TZ
            if existing['root_id'] is not None:
                conn.execute('UPDATE users SET username = %s, email = %s, full_name = %s WHERE id = %s',
                             (username, email, full_name, existing['id']))
            row = conn.execute('SELECT * FROM auth_user_by_clerk_id(%s)', (clerk_user_id,)).fetchone()
            g.current_user = dict(row)
            return g.current_user, None

        token = (payload.get('invite_token') or '').strip()
        invite = conn.execute('SELECT * FROM auth_invite(%s, %s)', (token, now)).fetchone() if token else None
        if invite:
            # Declare the invite's tenant now, so both the expiry it is judged
            # against and the accepted_at it is stamped with are read on the
            # clock that minted it. An invite that has run out on that clock is
            # simply absent — the caller lands unattached, as if they had none.
            set_tenant(conn, invite['root_id'])
            g.tz = _tenant_timezone(conn, invite['root_id']) if invite['root_id'] else FALLBACK_TZ
            now = app_stamp()
            if invite['expires_at'] <= now:
                invite = None
        legacy = conn.execute('SELECT * FROM auth_user_by_identity(%s, %s)', (email, username)).fetchone()
        # The email and username arrive in the request body, so a matching row
        # is not proof of who is signing in — the invite is. An attached row may
        # only be claimed when a live invite for that same tenant vouches for
        # it; otherwise this is an ordinary stranger sign-in and the row is not
        # even acknowledged.
        if legacy and legacy['root_id'] is not None and not (
                invite and invite['root_id'] == legacy['root_id']):
            legacy = None

        if legacy:
            unit_id, role, root_id = legacy['unit_id'], legacy['role'], legacy['root_id']
            if invite:
                unit_id, role, root_id = invite['unit_id'], invite['role'], invite['root_id']
            conn.execute('SELECT * FROM auth_claim_legacy_user(%s, %s, %s, %s, %s, %s, %s, %s)',
                         (legacy['id'], clerk_user_id, username, email, full_name, unit_id, role, root_id))
        else:
            unit_id, role, root_id = ((invite['unit_id'], invite['role'], invite['root_id'])
                                      if invite else (None, 'leader', None))
            conn.execute('SELECT * FROM auth_create_user(%s, %s, %s, %s, %s, %s, %s)',
                         (clerk_user_id, username, email, full_name, unit_id, role, root_id))
        set_tenant(conn, root_id)
        g.tz = _tenant_timezone(conn, root_id) if root_id else FALLBACK_TZ
        if invite:
            conn.execute('UPDATE invites SET accepted_at = %s, accepted_by = %s WHERE token = %s',
                         (now, clerk_user_id, invite['token']))
        row = conn.execute('SELECT * FROM auth_user_by_clerk_id(%s)', (clerk_user_id,)).fetchone()
        if not row:
            # Two Clerk accounts raced the same legacy row: the loser's claim
            # matched nothing and raised nothing, so there is no user to return.
            # Saying so beats handing the route a None it would 500 on.
            return None, 'Sign-in could not be completed; try again.'
        g.current_user = dict(row)
        return g.current_user, None
    except psycopg.errors.UniqueViolation:
        # username is UNIQUE; the caller's Clerk handle collides with someone
        # else's local username. Retry once with the email-derived fallback.
        conn.rollback()
        fallback = email or f'user-{clerk_user_id[:8]}'
        if payload.get('username') != fallback:
            return sync_clerk_user(dict(payload, username=fallback))
        return None, 'That username is already in use locally. Ask an owner to rename or merge the account.'


def _unauthenticated_response():
    error = getattr(g, 'auth_error', '') or 'Unauthorized'
    return jsonify({'error': error}), _auth_status_for(error)


def _billing_row(conn, user):
    """This attached account's subscriptions row, created on first sight.

    The trial starts here — at the first sign-in that finds the account
    effectively billed — not at account creation, so an account that existed
    before billing, or while BILLING_DEFAULT was off, gets its full trial from
    the day billing first applies to it. Two workers can race the INSERT;
    ON CONFLICT DO NOTHING makes the loser re-read the winner's row.

    The request's transaction commits only on a success response, so a first
    request that 4xxs rolls the new row back; the trial is keyed to the first
    successful request.
    """
    row = conn.execute('SELECT * FROM subscriptions WHERE user_id = %s', (user['id'],)).fetchone()
    if row is None:
        conn.execute('INSERT INTO subscriptions (user_id, root_id) VALUES (%s, %s) ON CONFLICT (user_id) DO NOTHING',
                     (user['id'], user['root_id']))
        row = conn.execute('SELECT * FROM subscriptions WHERE user_id = %s', (user['id'],)).fetchone()
    row = dict(row)
    if row['trial_started_at'] is None and _billing_verdict(row, user)['state'] != 'COMPED':
        now = billing_rules.utcnow()
        started = conn.execute(
            'UPDATE subscriptions SET trial_started_at = %s, trial_ends_at = %s, updated_at = now() '
            'WHERE user_id = %s AND trial_started_at IS NULL RETURNING *',
            (now, now + billing_rules.TRIAL_DAYS * billing_rules.DAY, user['id'])).fetchone()
        if started:
            row = dict(started)
    return row


def _billing_verdict(row, user):
    return billing_rules.billing_state(
        row, billing_rules.utcnow(), default_on=BILLING_DEFAULT_ON,
        platform_admin=_platform_admin_flag(user) is True, enabled=STRIPE_ENABLED)


def _load_billing(user):
    """Compute this request's billing verdict onto g. None for the unattached:
    they have no tenant, no row, and the create-unit screen is never gated."""
    g.billing_row = None
    g.billing = None
    if user.get('unit_id') is None:
        return
    g.billing_row = _billing_row(get_db(), user)
    g.billing = _billing_verdict(g.billing_row, user)


def _billing_block():
    """The 402, or None. Called by the three tenant decorators after
    _resolved_user(); platform_admin_required never comes through here
    (it declares no tenant and /api/admin/ is exempt anyway).

    It runs ahead of each decorator's own 403, so a locked account gets the
    same answer on every route whatever its role: the lock is a fact about the
    account, not about who may use that route. Without that, a locked leader
    hitting an owner route would be told 'Forbidden' and go looking for a
    permission problem that isn't there."""
    b = g.get('billing')
    if not b or b['state'] != 'LOCKED':
        return None
    if request.path == '/api/me' or request.path.startswith(BILLING_EXEMPT_PREFIXES):
        return None
    return jsonify({'error': 'subscription_required', 'billing': _billing_payload()}), 402


def _stripe_customer_for_mode(row):
    """The bare Stripe customer id when the stored one belongs to the active
    mode; None otherwise (a mode switch simply makes a new customer)."""
    stored = (row or {}).get('stripe_customer_id') or ''
    prefix = f'{STRIPE_MODE}:'
    return (stored[len(prefix):] or None) if stored.startswith(prefix) else None


# ── Stripe seams ──
# Five one-line functions, one per Stripe call the app makes. Everything
# above them is testable with these replaced; nothing else in server.py
# touches the SDK except the webhook's signature check.
def _stripe_prices():
    return stripe.Price.list(lookup_keys=list(PRICE_LOOKUP_KEYS), active=True, limit=10).data


def _stripe_customer_create(params, idempotency_key):
    return stripe.Customer.create(**params, idempotency_key=idempotency_key)


def _stripe_checkout(params):
    return stripe.checkout.Session.create(**params)


def _stripe_portal(params):
    return stripe.billing_portal.Session.create(**params)


def _stripe_cancel(subscription_id):
    stripe.Subscription.cancel(subscription_id)


PRICES_TTL = 3600
# A failure is cached too, briefly: one browser polling /api/me during a
# Stripe outage would otherwise park a sync worker per request.
PRICES_FAIL_TTL = 60
_PRICES_CACHE = (0.0, [])   # (expires_at_monotonic, prices)


def _prices_cached():
    """Both prices by lookup key, or [] — never one of them, never a stale
    amount. Per worker; a restart simply re-asks."""
    global _PRICES_CACHE
    expires, prices = _PRICES_CACHE
    if time.monotonic() < expires:
        return prices
    if not STRIPE_ENABLED:
        return []
    try:
        found = {p.lookup_key: p for p in _stripe_prices() if p.lookup_key in PRICE_LOOKUP_KEYS}
        prices = [{'id': found[k].id, 'lookup_key': k, 'amount': found[k].unit_amount,
                   'interval': found[k].recurring.interval, 'currency': found[k].currency}
                  for k in PRICE_LOOKUP_KEYS if k in found]
        if len(prices) != len(PRICE_LOOKUP_KEYS):
            app.logger.warning('Stripe returned %d of %d prices; showing none', len(prices), len(PRICE_LOOKUP_KEYS))
            prices, ttl = [], PRICES_FAIL_TTL
        else:
            ttl = PRICES_TTL
    except Exception as exc:
        app.logger.warning('could not fetch Stripe prices: %s', exc)
        prices, ttl = [], PRICES_FAIL_TTL
    _PRICES_CACHE = (time.monotonic() + ttl, prices)
    return prices


def _billing_payload():
    b = g.get('billing')
    if b is None:
        return None
    row = g.get('billing_row') or {}
    wants_prices = not b['subscribed'] and b['state'] != 'COMPED'
    return {**b,
            'prices': [{k: v for k, v in p.items() if k != 'id'} for p in _prices_cached()] if wants_prices else [],
            'portal_available': _stripe_customer_for_mode(row) is not None}


def _resolved_user():
    user = get_current_user()
    if not user:
        return None
    g.current_user = user
    # Declare the tenant on this request's transaction before any other
    # statement. An unattached user has no root_id and so declares no tenant
    # at all: RLS default-deny is exactly the right answer for them.
    set_tenant(get_db(), user.get('root_id'))
    # The duty day is the tenant's, so it is read once here and cached on g for
    # the rest of the request; app_timezone() has nothing else to ask.
    g.tz = _tenant_timezone(get_db(), user['root_id']) if user.get('root_id') else FALLBACK_TZ
    _load_billing(user)
    return user


def login_required(f):
    """Signed in. Unattached users pass — only /api/me, /api/units GET/POST and
    /api/auth/sync use this alone; everything else wants attached_required."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not _resolved_user():
            return _unauthenticated_response()
        blocked = _billing_block()
        if blocked:
            return blocked
        return f(*args, **kwargs)
    return decorated


def attached_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        user = _resolved_user()
        if not user:
            return _unauthenticated_response()
        blocked = _billing_block()
        if blocked:
            return blocked
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
        blocked = _billing_block()
        if blocked:
            return blocked
        if user.get('unit_id') is None or not is_owner(user):
            return jsonify({'error': 'Forbidden'}), 403
        return f(*args, **kwargs)
    return decorated


# ── The platform operator's gate ──

def _clerk_verified_email(clerk_user_id):
    """This Clerk account's primary email address — but only if Clerk itself
    says it is verified. '' when it is not, or when there is none.

    Raises on anything that is not a clean answer (no secret key, a timeout, a
    non-2xx, unparseable JSON) so the caller fails closed instead of guessing.

    api.clerk.com sits behind Cloudflare, which answers urllib's default
    User-Agent with error 1010 — hence the explicit one. The timeout matters
    more: gunicorn runs sync workers, and a hung request here parks one, so it
    is short and the caller caches the failure (see _platform_admin_verdict).
    """
    if not CLERK_SECRET_KEY:
        raise RuntimeError('CLERK_SECRET_KEY is not set; no platform admin can be verified')
    req = Request(CLERK_API_USERS + quote(str(clerk_user_id), safe=''),
                  headers={'Authorization': f'Bearer {CLERK_SECRET_KEY}',
                           'Accept': 'application/json',
                           'User-Agent': 'platoon-accountability/1.0'})
    with urlopen(req, timeout=CLERK_API_TIMEOUT) as resp:
        data = json.load(resp)
    primary = data.get('primary_email_address_id')
    for entry in data.get('email_addresses') or []:
        if entry.get('id') != primary:
            continue
        # An unverified address is one anybody can type into a sign-up form.
        if (entry.get('verification') or {}).get('status') != 'verified':
            return ''
        return (entry.get('email_address') or '').strip().lower()
    return ''


def _platform_admin_verdict(clerk_user_id):
    """True, False, or PLATFORM_ADMIN_UNKNOWN when Clerk could not be asked.

    Never raises, and never grants on failure — the caller decides what to do
    with "don't know" (403/503 on the dashboard, "no menu" on /api/me), and
    neither of them can mistake it for a yes.

    Cached per Clerk id, all three answers: the dashboard is several requests
    plus a Refresh button and none of them should cost a round trip to Clerk;
    caching only the "yes" would let any signed-in stranger make us call Clerk
    at will; and caching the failure is what stops a Clerk outage from parking
    both sync workers (see PLATFORM_ADMIN_FAIL_TTL_SECONDS). The failure's TTL
    is its own short one, so the outage is re-tested soon rather than pinned
    for five minutes.
    """
    if not clerk_user_id or not PLATFORM_ADMIN_EMAILS:
        return False
    now = time.monotonic()
    hit = _PLATFORM_ADMIN_CACHE.get(clerk_user_id)
    if hit and hit[0] > now:
        return hit[1]
    try:
        verdict = _clerk_verified_email(clerk_user_id) in PLATFORM_ADMIN_EMAILS
        ttl = PLATFORM_ADMIN_TTL_SECONDS
    except Exception as exc:
        app.logger.warning('platform admin check could not reach Clerk: %s', exc)
        verdict, ttl = PLATFORM_ADMIN_UNKNOWN, PLATFORM_ADMIN_FAIL_TTL_SECONDS
    if len(_PLATFORM_ADMIN_CACHE) > 512:
        # ponytail: one operator and a handful of curious accounts — a flush is
        # cheaper than an LRU. Revisit if this ever holds real traffic.
        _PLATFORM_ADMIN_CACHE.clear()
    _PLATFORM_ADMIN_CACHE[clerk_user_id] = (now + ttl, verdict)
    return verdict


def platform_admin_required(f):
    """The person who runs the instance, and nobody else.

    Deliberately not one of the tenant decorators: it declares NO tenant (the
    routes behind it read across all of them, through the SECURITY DEFINER
    admin_ functions) and it does not care whether the caller is attached to a
    unit. The identity comes from the verified session token's `sub` and
    nothing else — never a header, a query string or a body.

    A signed-in non-admin gets 404, not 403: there is no reason to tell them
    the surface exists. No session at all is 401, like every other API route.
    An unreachable Clerk is 503 and closed.

    Every outcome is logged against the token's `sub` — the only cross-tenant
    read in the app should leave a trace, and log_action() cannot help here
    because it needs a tenant. The sub and nothing else: an email out of the
    request would put an attacker's text in the log.
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        claims, error = _verify_clerk_session_token()
        if error:
            return jsonify({'error': error}), _auth_status_for(error)
        g.auth_claims = claims
        sub = claims.get('sub')
        allowed = _platform_admin_verdict(sub)
        if allowed is PLATFORM_ADMIN_UNKNOWN:
            app.logger.warning('platform admin request could not be checked with Clerk: %s', sub)
            return jsonify({'error': CLERK_UNREACHABLE}), 503
        if not allowed:
            app.logger.warning('platform admin request refused for %s', sub)
            return jsonify({'error': 'Not found'}), 404
        return f(*args, **kwargs)
    return decorated


def _platform_admin_flag(u):
    """Whether to offer this user the Admin menu, for /api/me and /api/auth/sync.

    The stored email is only a hint, so it decides one thing: whether asking
    Clerk is worth it at all. Every ordinary sign-in stops on the first line
    and costs nothing; the operator's costs one lookup per TTL, and it is
    Clerk's verified answer — not the stored row — that returns True.

    `is True` rather than a truth test: an unreachable Clerk answers UNKNOWN,
    and a menu item is not worth blocking a sign-in over. The verdict is
    cached, so an outage costs one lookup per failure TTL, not one per request.
    """
    if (u.get('email') or '').strip().lower() not in PLATFORM_ADMIN_EMAILS:
        return False
    return _platform_admin_verdict(u.get('clerk_user_id') or '') is True


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

# platoonmanager.com serves the marketing site at '/'; every other host --
# app.platoonmanager.com, the LAN address, localhost -- serves the app there, as
# it always has. The list names the MARKETING side on purpose: unset it or get it
# wrong and a visitor misses the brochure, which is recoverable, rather than a
# leader losing the product at 0630, which is not.
MARKETING_HOSTS = tuple(h.strip().lower() for h in os.environ.get(
    'MARKETING_HOSTS', 'platoonmanager.com,www.platoonmanager.com').split(',') if h.strip())

# Where the marketing site's 'Sign in' sends people when the app is on its own
# subdomain.
APP_URL = os.environ.get('APP_URL', 'https://app.platoonmanager.com')

# The file each public path serves. Only the literal routes declared below reach
# this map, so a filename is never built out of the request.
PUBLIC_PAGES = {
    '/': 'home.html',
    '/welcome': 'home.html',
    '/home': 'home.html',
    '/privacy': 'privacy.html',
    '/legal/privacy': 'privacy.html',
    '/terms': 'terms.html',
    '/legal/terms': 'terms.html',
}


def _is_marketing_host():
    return (request.host or '').split(':')[0].lower() in MARKETING_HOSTS


# The hostname the app answered on before the product had its own domain. It is
# retired by 301, not by switching it off: people have it bookmarked.
LEGACY_HOSTS = tuple(h.strip().lower() for h in os.environ.get(
    'LEGACY_HOSTS', 'platoon.carr7.com').split(',') if h.strip())

MARKETING_URL = 'https://' + MARKETING_HOSTS[0] if MARKETING_HOSTS else '/'


@app.before_request
def redirect_legacy_host():
    """301 the old hostname onto the new domain.

    Every URL on the old host is an *app* URL -- a leader's bookmark is
    `/<unit>/accountability`, not a front page -- so only '/' goes to the new
    marketing site and every other path keeps itself, and its query, on the app
    subdomain. Sending them all to the front page would turn every bookmark in
    the company into a brochure.

    Two deliberate exemptions. `/api/` is left alone because a cross-origin 301
    does not move anybody: it breaks an open tab's in-flight request instead,
    and at 0630 that is somebody's accountability entry. Non-GET is left alone
    because a 301 turns a POST into a GET and drops the body. Either way the
    next navigation moves them, which is what actually retires the host.
    """
    if request.method not in ('GET', 'HEAD') or request.path.startswith('/api/'):
        return None
    if (request.host or '').split(':')[0].lower() not in LEGACY_HOSTS:
        return None
    if request.path == '/':
        return redirect(MARKETING_URL, code=301)
    target = APP_URL.rstrip('/') + request.path
    if request.query_string:
        target += '?' + request.query_string.decode('latin-1')
    return redirect(target, code=301)


@app.route('/')
def index():
    if _is_marketing_host():
        return send_from_directory('public', PUBLIC_PAGES['/'])
    return send_from_directory('.', 'index.html')


@app.route('/app')
def app_entry():
    """Where every 'Sign in' and 'Start free trial' on the marketing site points.
    On the marketing host that is the app's own subdomain; anywhere else the app
    is already at '/', so go there rather than bouncing a developer -- or the LAN
    address -- out to production."""
    return redirect(APP_URL if _is_marketing_host() else '/', code=302)


@app.route('/welcome')
@app.route('/home')
@app.route('/privacy')
@app.route('/terms')
@app.route('/legal/privacy')
@app.route('/legal/terms')
def public_page():
    """The signed-out pages: the marketing site plus the two legal pages Google's
    OAuth consent screen links to. They live outside index.html because they have
    to render with no Clerk, no session and no JS.

    /legal/* answers the same pages because that is the shape the sibling apps
    use and it is the URL people reach for; without it the path falls through to
    spa_fallback and quietly serves the app shell instead. /welcome is kept
    because it was the marketing URL before the site moved to the root.
    """
    return send_from_directory('public', PUBLIC_PAGES[request.path])


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
        'app_env': APP_ENV,
    })


def _user_json(conn, u):
    unit = _unit_row(conn, u['unit_id']) if u.get('unit_id') else None
    return {'id': u['id'], 'username': u['username'], 'email': u.get('email', ''),
            'full_name': u.get('full_name', ''), 'unit_id': u.get('unit_id'),
            'unit_name': unit['name'] if unit else '', 'unit_slug': unit['slug'] if unit else '',
            'role': u.get('role'), 'root_id': u.get('root_id'),
            'timezone': app_timezone(),
            'needs_unit': u.get('unit_id') is None}


def _invited_by(conn, u):
    """The display name of whoever's invite this user walked in on, or ''.

    Deliberately NOT part of _user_json(): that also builds every row of
    /api/users, and one invite query per person there is an N+1. Only the two
    routes that answer "who am I" pay for it, and only after they have declared
    a tenant — `invites` and `users` are both under RLS, so this can never see
    past the caller's own organization.
    """
    if not u.get('root_id') or not u.get('clerk_user_id'):
        return ''
    row = conn.execute(
        'SELECT COALESCE(NULLIF(usr.full_name, %s), i.created_by) AS name '
        'FROM invites i LEFT JOIN users usr ON usr.username = i.created_by '
        'WHERE i.accepted_by = %s ORDER BY i.accepted_at DESC LIMIT 1',
        ('', u['clerk_user_id'])).fetchone()
    return (row['name'] or '') if row else ''


@app.route('/api/auth/sync', methods=['POST'])
@clerk_auth_required
def auth_sync():
    payload = request.get_json() or {}
    user, error = sync_clerk_user(payload)
    if error:
        return jsonify({'error': error}), 409
    # This route is @clerk_auth_required alone, so nothing has declared a
    # tenant for it. log_action() takes root_id from the GUC and writes
    # nothing when there is none — without this the LOGIN row was silently
    # dropped for everyone. A stranger's first sign-in still isn't audited:
    # they belong to no tenant yet, and UNIT_CREATE is their first row.
    set_tenant(get_db(), user['root_id'])
    g.tz = _tenant_timezone(get_db(), user['root_id']) if user['root_id'] else FALLBACK_TZ
    _load_billing(user)
    log_action('LOGIN', f'Clerk user signed in: {_display_name_for_user(user)}')
    return jsonify({**_user_json(get_db(), user), 'invited_by': _invited_by(get_db(), user),
                    'platform_admin': _platform_admin_flag(user),
                    'billing': _billing_payload()})


@app.route('/api/logout', methods=['POST'])
def logout():
    return jsonify({'success': True})


# ── Billing routes ──
# All three are for the account itself, attached, and none is gated by the
# 402 (/api/billing/ is exempt): they are how a locked account gets out.

def _billing_return_base():
    slug = _unit_row(get_db(), g.current_user['unit_id'])['slug']
    return f"{request.host_url.rstrip('/')}/{slug}/settings"


def _ensure_stripe_customer(conn, user, row):
    """The Stripe customer for this account in the active mode, made on first
    use. The idempotency key means a double-click cannot make two."""
    existing = _stripe_customer_for_mode(row)
    if existing:
        return existing
    customer = _stripe_customer_create(
        {'email': user.get('email') or None, 'name': user.get('full_name') or user['username'],
         'metadata': {'user_id': str(user['id']), 'root_id': str(user['root_id']), 'mode': STRIPE_MODE}},
        idempotency_key=f'user:{user["id"]}:{STRIPE_MODE}')
    conn.execute('UPDATE subscriptions SET stripe_customer_id = %s, updated_at = now() WHERE user_id = %s',
                 (f'{STRIPE_MODE}:{customer.id}', user['id']))
    g.billing_row['stripe_customer_id'] = f'{STRIPE_MODE}:{customer.id}'
    return customer.id


@app.route('/api/billing/extend', methods=['POST'])
@attached_required
def billing_extend():
    b = g.billing
    if not b or not b['extension_available'] or b['state'] not in ('TRIAL', 'GRACE', 'LOCKED'):
        return jsonify({'error': 'The trial extension is not available.'}), 409
    conn = get_db()
    now = billing_rules.utcnow()
    # In trial the end moves by 7 days; in grace (or just locked) it runs from now.
    ends = max(g.billing_row['trial_ends_at'] or now, now) + billing_rules.EXTENSION_DAYS * billing_rules.DAY
    cur = conn.execute('UPDATE subscriptions SET extended_at = %s, trial_ends_at = %s, updated_at = now() '
                       'WHERE user_id = %s AND extended_at IS NULL', (now, ends, g.current_user['id']))
    if cur.rowcount == 0:
        return jsonify({'error': 'The trial extension has already been used.'}), 409
    log_action('BILLING_EXTEND', f'trial extended {billing_rules.EXTENSION_DAYS} days to {ends.isoformat()}')
    _load_billing(g.current_user)
    return jsonify({'billing': _billing_payload()})


@app.route('/api/billing/checkout', methods=['POST'])
@attached_required
def billing_checkout():
    if not STRIPE_ENABLED:
        return jsonify({'error': 'Billing is not configured on this instance.'}), 503
    # A stale tab or a Back onto the pricing screen would otherwise open a
    # second Checkout on the same card: the supersede rule adopts the newer
    # subscription and the older one keeps charging. Changing a live plan is
    # the portal's job.
    if g.billing and g.billing['subscribed']:
        return jsonify({'error': 'This account already has a subscription. '
                                 'Change or cancel it in the billing portal.'}), 409
    key = (request.get_json(silent=True) or {}).get('lookup_key')
    price = next((p for p in _prices_cached() if p['lookup_key'] == key), None)
    if not price:
        return jsonify({'error': 'Unknown plan.'}), 400
    conn = get_db()
    user = g.current_user
    customer_id = _ensure_stripe_customer(conn, user, g.billing_row)
    base = _billing_return_base()
    session = _stripe_checkout({
        'mode': 'subscription', 'customer': customer_id,
        'line_items': [{'price': price['id'], 'quantity': 1}],
        'client_reference_id': str(user['id']),
        'metadata': {'user_id': str(user['id']), 'root_id': str(user['root_id']), 'mode': STRIPE_MODE},
        'subscription_data': {'metadata': {'user_id': str(user['id'])}},
        'success_url': f'{base}?billing=success', 'cancel_url': base,
    })
    log_action('BILLING_CHECKOUT_STARTED', f'{key} by {user["username"]}')
    return jsonify({'url': session.url})


@app.route('/api/billing/portal', methods=['POST'])
@attached_required
def billing_portal():
    if not STRIPE_ENABLED:
        return jsonify({'error': 'Billing is not configured on this instance.'}), 503
    customer_id = _stripe_customer_for_mode(g.billing_row)
    if not customer_id:
        return jsonify({'error': 'No billing account yet. Choose a plan first.'}), 409
    session = _stripe_portal({'customer': customer_id, 'return_url': _billing_return_base()})
    return jsonify({'url': session.url})


# ── The webhook ──
# Stripe is the caller: no session, no tenant, the signature is the auth.
# Deliberately undecorated (tests/test_smoke.py lists it in PUBLIC_API). Every
# database write goes through the billing_* SECURITY DEFINER functions; the
# only thing done as the tenant is the audit row, after set_tenant() on the
# root the customer was found in.

SUBSCRIPTION_EVENTS = ('customer.subscription.created', 'customer.subscription.updated',
                       'customer.subscription.deleted')


@app.route('/api/billing/webhook', methods=['POST'])
def billing_webhook():
    if not (STRIPE_ENABLED and STRIPE_WEBHOOK_SECRET):
        return jsonify({'error': 'Billing is not configured on this instance.'}), 503
    if (request.content_length or 0) > WEBHOOK_MAX_BYTES:
        return jsonify({'error': 'Payload too large.'}), 413
    # A bounded read, not get_data(): the check above trusts Content-Length,
    # and a chunked request does not send one. MAX_CONTENT_LENGTH is not set
    # app-wide, so this unauthenticated route would otherwise read whatever it
    # is handed. One byte over the cap is enough to know it is over.
    payload = request.stream.read(WEBHOOK_MAX_BYTES + 1)
    if len(payload) > WEBHOOK_MAX_BYTES:
        return jsonify({'error': 'Payload too large.'}), 413
    try:
        # construct_event is the signature check (HMAC + 300 s timestamp
        # tolerance). Its StripeObject return is not dict-like in
        # stripe-python >= 12, so the now-verified payload is read back as
        # plain JSON; construct_event has already parsed it once, so this
        # cannot fail on anything that got past the signature.
        stripe.Webhook.construct_event(payload, request.headers.get('Stripe-Signature', ''), STRIPE_WEBHOOK_SECRET)
    except Exception:
        app.logger.warning('stripe webhook: bad signature from %s', request.remote_addr)
        return jsonify({'error': 'Bad signature.'}), 400
    event = json.loads(payload)
    if bool(event.get('livemode')) != (STRIPE_MODE == 'live'):
        return jsonify({'ignored': 'other mode'})
    conn = get_db()
    fresh = conn.execute('SELECT billing_record_event(%s) AS fresh', (event['id'],)).fetchone()['fresh']
    if not fresh:
        return jsonify({'replay': True})
    # A handler that raises becomes a 500 through errorhandler(Exception), and
    # _close_db() rolls the transaction back — including the event record
    # above — so Stripe's retry is handled, not mistaken for a replay.
    _handle_stripe_event(conn, event)
    return jsonify({'ok': True})


def _handle_stripe_event(conn, event):
    obj = event['data']['object']
    kind = event['type']
    if kind in SUBSCRIPTION_EVENTS:
        items = ((obj.get('items') or {}).get('data') or [])
        first = items[0] if items else {}
        price = first.get('price') or {}
        # API versions before 2025-03 put current_period_end on the
        # subscription; since then it is on each item.
        period_end = obj.get('current_period_end') or first.get('current_period_end')
        _apply_stripe(conn, event, customer=obj.get('customer'), subscription=obj.get('id'),
                      status=obj.get('status'), lookup_key=price.get('lookup_key'),
                      period_end=period_end, cancel_at_period_end=bool(obj.get('cancel_at_period_end')))
    elif kind == 'invoice.payment_failed':
        # The subscription this invoice is for: a plain id on API versions
        # before 2025-03, under parent.subscription_details since, and absent
        # on a one-off invoice. Without it the apply guard cannot tell a live
        # subscription's failed payment from a late delivery for one the
        # account has already cancelled, and past_due is an OPEN state — a
        # stray invoice would re-open a cancelled account.
        sub = obj.get('subscription')
        if not isinstance(sub, str):
            sub = ((obj.get('parent') or {}).get('subscription_details') or {}).get('subscription')
        if not isinstance(sub, str) or not sub:
            # Nothing to check the stored subscription against: passing NULL
            # makes billing_apply_stripe's `p_subscription IS NULL OR ...`
            # guard true by construction, and past_due is an OPEN, subscribed
            # state — a one-off invoice would then hold an account open past
            # its trial. The subscription events carry the truth anyway.
            app.logger.warning('stripe event %s: invoice.payment_failed names no subscription, ignored',
                               event['id'])
            return
        _apply_stripe(conn, event, customer=obj.get('customer'),
                      subscription=sub, status='past_due',
                      lookup_key=None, period_end=None, cancel_at_period_end=None)
    elif kind == 'invoice.paid':
        _audit_for_customer(conn, obj.get('customer'), 'BILLING_PAID', f'invoice {obj.get("id")} paid')
    # checkout.session.completed and everything else: acknowledged, ignored —
    # the subscription events carry the truth.


def _audit_for_customer(conn, customer, action, details):
    customer = customer.get('id') if isinstance(customer, dict) else customer
    target = conn.execute('SELECT * FROM billing_find_by_customer(%s)', (f'{STRIPE_MODE}:{customer}',)).fetchone()
    if not target:
        app.logger.warning('stripe event for unknown customer %s (%s)', customer, action)
        return None
    set_tenant(conn, target['root_id'])
    log_action(action, details)
    return target


def _apply_stripe(conn, event, customer, subscription, status, lookup_key, period_end, cancel_at_period_end):
    # An expanded customer arrives as the object, not the id.
    customer = customer.get('id') if isinstance(customer, dict) else customer
    cid = f'{STRIPE_MODE}:{customer}'
    target = conn.execute('SELECT * FROM billing_find_by_customer(%s)', (cid,)).fetchone()
    if not target:
        app.logger.warning('stripe event %s for unknown customer %s', event['id'], customer)
        return
    ends = datetime.fromtimestamp(period_end, timezone.utc) if period_end else None
    applied = conn.execute('SELECT billing_apply_stripe(%s, %s, %s, %s, %s, %s) AS user_id',
                           (cid, subscription, status, lookup_key, ends, cancel_at_period_end)).fetchone()['user_id']
    if applied is None:
        # The function's own guards refused it: a late delivery for a
        # subscription this account has replaced, or a failed invoice for one
        # it has already cancelled. Nothing changed, so nothing is audited —
        # an audit row for a write that did not happen is a lie the support
        # desk would act on.
        app.logger.warning('stripe event %s for customer %s not applied (superseded or refused)',
                           event['id'], customer)
        return
    # One account, one live subscription: a second Checkout completing is the
    # case this exists for, so only `created` may cancel. billing_apply_stripe
    # adopts ANY id on an active/trialing status (A14, deliberate), and Stripe
    # does not order deliveries — a retried `updated` for an older
    # subscription, arriving after the new one's `created`, adopts the old id,
    # and cancelling on that would kill the subscription just paid for.
    # Best effort: the adoption has already happened and a Stripe outage must
    # not undo it.
    superseded = None
    if (event['type'] == 'customer.subscription.created' and subscription
            and target['stripe_subscription_id'] and target['stripe_subscription_id'] != subscription):
        try:
            _stripe_cancel(target['stripe_subscription_id'])
            superseded = target['stripe_subscription_id']
        except Exception as exc:
            app.logger.error('could not cancel superseded Stripe subscription %s: %s',
                             target['stripe_subscription_id'], exc)
    if status in billing_rules.OPEN_STATUSES:
        action = 'BILLING_ACTIVE'
    elif status == 'past_due':
        action = 'BILLING_PAST_DUE'
    elif status in billing_rules.LOCKED_STATUSES:
        action = 'BILLING_CANCELLED'
    else:
        action = 'BILLING_STATUS'
    set_tenant(conn, target['root_id'])
    log_action(action, f'stripe {event["type"]}: {status}' + (f', cancel at period end' if cancel_at_period_end else ''))
    if superseded:
        log_action('BILLING_CANCELLED', f'superseded subscription {superseded} cancelled at Stripe')


@app.route('/api/me', methods=['GET'])
@login_required
def me():
    return jsonify({**_user_json(get_db(), g.current_user),
                    'invited_by': _invited_by(get_db(), g.current_user),
                    'platform_admin': _platform_admin_flag(g.current_user),
                    'billing': _billing_payload()})


# ── Platform operator dashboard ──
# The only cross-tenant read surface in the app. It declares no tenant and
# every number in it comes from sql/admin_functions.sql, which is where the
# rule about what may and may not be exposed is written down.

@app.route('/api/admin/overview', methods=['GET'])
@platform_admin_required
def admin_overview():
    conn = get_db()
    # No tenant, so this is FALLBACK_TZ — the same coarse clock auth_invite()
    # is filtered on before a tenant is known. Invite expiry stamps are written
    # in each tenant's own zone, so "pending" here can be a few hours out at
    # the edges; it is a dashboard count, not a gate.
    now_stamp = app_stamp()
    totals = conn.execute('SELECT * FROM admin_totals(%s)', (now_stamp,)).fetchone()
    orgs = conn.execute('SELECT * FROM admin_organizations(%s)', (now_stamp,)).fetchall()
    recent = conn.execute('SELECT * FROM admin_recent_users(%s)', (25,)).fetchall()
    # Billing per account, computed here with the same pure rule the gate
    # uses. For this display an account whose stored email is the operator's
    # counts as comped; the real verdict is still Clerk's, on the gate.
    now = billing_rules.utcnow()
    states = {}
    counts = {'billing_trial': 0, 'billing_grace': 0, 'billing_locked': 0, 'billing_active': 0, 'billing_comped': 0}
    bucket = {'TRIAL': 'billing_trial', 'GRACE': 'billing_grace', 'LOCKED': 'billing_locked',
              'ACTIVE': 'billing_active', 'PAST_DUE': 'billing_active', 'COMPED': 'billing_comped'}
    for b in conn.execute('SELECT * FROM admin_billing_rows()').fetchall():
        admin = (b['email'] or '').strip().lower() in PLATFORM_ADMIN_EMAILS
        s = billing_rules.billing_state(dict(b), now, default_on=BILLING_DEFAULT_ON, platform_admin=admin,
                                        enabled=STRIPE_ENABLED)
        states[b['user_id']] = (s['state'], b['billing_mode'])
        counts[bucket[s['state']]] += 1
    # log_action() writes to audit_log, which is a tenant table and needs a
    # tenant; this read belongs to no tenant. The process log is the only place
    # it can be recorded, and the token's sub is the only identifier worth
    # recording — never an address out of the request.
    app.logger.info('platform admin overview read by %s', g.auth_claims.get('sub'))
    return jsonify({
        'totals': {**dict(totals), **counts},
        'organizations': [dict(r) for r in orgs],
        'recent_users': [{**dict(r), 'billing_state': states.get(r['user_id'], (None, None))[0],
                          'billing_mode': states.get(r['user_id'], (None, None))[1]} for r in recent],
        'generated_at': now_stamp,
    })


@app.route('/api/admin/users/<int:user_id>/billing_mode', methods=['PUT'])
@platform_admin_required
def admin_set_billing_mode(user_id):
    mode = (request.get_json(silent=True) or {}).get('mode')
    if mode not in billing_rules.MODES:
        return jsonify({'error': 'mode must be default, comped or billed'}), 400
    conn = get_db()
    sub = g.auth_claims.get('sub')
    row = conn.execute('SELECT billing_set_mode(%s, %s, %s) AS root_id', (user_id, mode, sub)).fetchone()
    if not row or row['root_id'] is None:
        return jsonify({'error': 'Not found'}), 404
    # The audit row belongs to that account's tenant; the write itself went
    # through the definer function and needed none.
    set_tenant(conn, row['root_id'])
    log_action('BILLING_COMP', f'user {user_id} billing_mode set to {mode} by the platform admin')
    app.logger.info('platform admin %s set billing_mode=%s for user %s', sub, mode, user_id)
    return jsonify({'user_id': user_id, 'billing_mode': mode})


# ── User management ──
# Everything here is bounded twice: RLS keeps a caller inside their own root,
# and current_subtree()/can_access() keep them inside their own branch of it.

@app.route('/api/users', methods=['GET'])
@attached_required
def get_users():
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM users WHERE clerk_user_id != '' AND unit_id = ANY(%s) ORDER BY username",
        (list(current_subtree()),)).fetchall()
    return jsonify([_user_json(conn, dict(r)) for r in rows])


@app.route('/api/users/<int:user_id>', methods=['PUT'])
@attached_required
def update_user(user_id):
    data = request.get_json() or {}
    conn = get_db()
    target = conn.execute("SELECT * FROM users WHERE id = %s AND clerk_user_id != ''", (user_id,)).fetchone()
    if target is None or not can_access(target['unit_id']):
        return jsonify({'error': 'Not found'}), 404
    # An owner's row may only be changed by an owner. Granting owner was
    # already owner-only, but *taking it away* was not: a root-attached leader
    # could demote every owner (or rename one past recognition) and brick the
    # tenant, because nobody left could ever grant owner back.
    if target['role'] == 'owner' and {'role', 'unit_id', 'username'} & set(data) \
            and not is_owner(g.current_user):
        return jsonify({'error': 'Only an owner can change an owner.'}), 403
    fields, values = [], []
    new_unit = target['unit_id']
    if 'unit_id' in data:
        if not can_access(data['unit_id']):
            return jsonify({'error': 'Forbidden'}), 403
        new_unit = int(data['unit_id'])
        fields.append('unit_id = %s'); values.append(new_unit)
    if 'role' in data:
        if data['role'] not in ROLES:
            return jsonify({'error': 'role must be owner or leader'}), 400
        if data['role'] == 'owner':
            if not is_owner(g.current_user):
                return jsonify({'error': 'Only an owner can grant owner.'}), 403
            if new_unit != g.current_user['root_id']:
                return jsonify({'error': 'owner is a root role; attach the user at the root first.'}), 400
        fields.append('role = %s'); values.append(data['role'])
    if 'username' in data:
        fields.append('username = %s'); values.append((data['username'] or '').strip())
    if not fields:
        return jsonify({'error': 'Nothing to update'}), 400
    # The other half of the same brick: the last owner may not demote himself
    # or walk off the root. Owner is granted only by an owner, so a root with
    # none is a root nobody can ever administer again.
    if target['role'] == 'owner' and target['unit_id'] == _root() \
            and (data.get('role', 'owner') != 'owner' or new_unit != _root()):
        owners = conn.execute(
            "SELECT count(*) AS n FROM users WHERE role = 'owner' AND unit_id = %s AND clerk_user_id != ''",
            (_root(),)).fetchone()['n']
        if owners <= 1:
            return jsonify({'error': 'The organization must keep at least one owner.'}), 400
    values.append(user_id)
    try:
        conn.execute(f'UPDATE users SET {", ".join(fields)} WHERE id = %s', values)
        if 'role' in data or 'unit_id' in data:
            log_action('ROLE_CHANGE', f'{target["username"]}: unit {new_unit}, role {data.get("role", target["role"])}', new_unit)
        row = conn.execute('SELECT * FROM users WHERE id = %s', (user_id,)).fetchone()
        return jsonify(_user_json(conn, dict(row)))
    except psycopg.errors.UniqueViolation:
        return jsonify({'error': 'Username already exists'}), 409


@app.route('/api/users/<int:user_id>', methods=['DELETE'])
@owner_required
def delete_user(user_id):
    if user_id == g.current_user['id']:
        return jsonify({'error': 'Cannot delete your own account'}), 400
    conn = get_db()
    # Nobody keeps paying for a deleted account: cancel at Stripe first, best
    # effort. The row is this tenant's (RLS), so a cross-tenant id finds nothing.
    sub = conn.execute('SELECT stripe_subscription_id, stripe_customer_id FROM subscriptions WHERE user_id = %s',
                       (user_id,)).fetchone()
    if sub and sub['stripe_subscription_id'] and _stripe_customer_for_mode(sub):
        try:
            _stripe_cancel(sub['stripe_subscription_id'])
            log_action('BILLING_CANCELLED', f'user {user_id} deleted; Stripe subscription cancelled')
        except Exception as exc:
            app.logger.error('could not cancel Stripe subscription %s for deleted user %s: %s',
                             sub['stripe_subscription_id'], user_id, exc)
    # RLS hides another root's users, so a cross-tenant id deletes nothing and
    # used to answer 200 — which told the caller the row had been theirs.
    cur = conn.execute("DELETE FROM users WHERE id = %s AND clerk_user_id != ''", (user_id,))
    if cur.rowcount == 0:
        return jsonify({'error': 'Not found'}), 404
    return jsonify({'success': True})


# ── Invitations ──

@app.route('/api/invites', methods=['GET'])
@attached_required
def get_invites():
    conn = get_db()
    rows = conn.execute(
        'SELECT i.*, u.name AS unit_name FROM invites i LEFT JOIN units u ON u.id = i.unit_id '
        'WHERE i.unit_id = ANY(%s) ORDER BY i.created_at DESC LIMIT 50', (list(current_subtree()),)).fetchall()
    now = app_stamp()
    return jsonify([{
        'token': r['token'], 'label': r['label'], 'unit_id': r['unit_id'], 'unit_name': r['unit_name'] or '',
        'role': r['role'], 'created_by': r['created_by'], 'expires_at': r['expires_at'],
        'status': 'accepted' if r['accepted_at'] else ('expired' if r['expires_at'] <= now else 'pending'),
    } for r in rows])


@app.route('/api/invites', methods=['POST'])
@attached_required
def create_invite():
    data = request.get_json() or {}
    label = ' '.join((data.get('label') or '').split())[:80]
    role = (data.get('role') or 'leader').strip()
    unit_id = data.get('unit_id')
    if role not in ROLES:
        return jsonify({'error': 'role must be owner or leader'}), 400
    if not can_access(unit_id):
        return jsonify({'error': 'Forbidden'}), 403
    if role == 'owner' and (not is_owner(g.current_user) or int(unit_id) != g.current_user['root_id']):
        return jsonify({'error': 'Only an owner can invite an owner, and only at the root.'}), 403
    conn = get_db()
    unit = _unit_row(conn, unit_id)
    token = secrets.token_urlsafe(24)
    conn.execute(
        'INSERT INTO invites (token, label, unit_id, role, root_id, created_by, expires_at, created_at) '
        'VALUES (%s, %s, %s, %s, %s, %s, %s, %s)',
        (token, label, int(unit_id), role, unit['root_id'], g.current_user['username'],
         (app_now() + timedelta(days=INVITE_EXPIRY_DAYS)).strftime('%Y-%m-%d %H:%M:%S'), app_stamp()))
    log_action('INVITE_CREATE', f'Invited {label or "(unnamed)"} — {role} at {unit["name"]}', int(unit_id))
    return jsonify({'token': token, 'url': f'{_get_request_origin()}/invite/{token}'})


@app.route('/api/invites/<token>', methods=['DELETE'])
@attached_required
def revoke_invite(token):
    conn = get_db()
    row = conn.execute('SELECT label, unit_id FROM invites WHERE token = %s', (token,)).fetchone()
    if row is None or not can_access(row['unit_id']):
        return jsonify({'error': 'Not found'}), 404
    conn.execute('DELETE FROM invites WHERE token = %s', (token,))
    log_action('INVITE_REVOKE', f'Revoked invite for {row["label"] or "(unnamed)"}', row['unit_id'])
    return jsonify({'success': True})


@app.route('/api/invites/<token>/preview', methods=['GET'])
def preview_invite(token):
    """Unauthenticated and pre-tenant — the token is the secret, and
    auth_invite() is the front door that may read it."""
    conn = get_db()
    row = conn.execute('SELECT * FROM auth_invite(%s, %s)', (token, app_stamp())).fetchone()
    if not row:
        return jsonify({'valid': False}), 404
    set_tenant(conn, row['root_id'])
    g.tz = _tenant_timezone(conn, row['root_id']) if row['root_id'] else FALLBACK_TZ
    unit = _unit_row(conn, row['unit_id'])
    return jsonify({'valid': True, 'label': row['label'], 'unit_name': unit['name'] if unit else '',
                    'kind': unit['kind'] if unit else '', 'role': row['role']})


# ── Units ──

def _unit_row(conn, unit_id):
    if unit_id is None:
        return None
    return conn.execute('SELECT * FROM units WHERE id = %s', (unit_id,)).fetchone()


# ── Unit logo ──
# A `settings` row (unit_id, key='logo', value=base64 PNG) — no new table and no
# migration, because that is all the shape this needs. It is inherited: a unit
# with none of its own shows the nearest one above it, so a company sets one
# logo and every team under it is branded.

LOGO_KEY = 'logo'
LOGO_MAX_PX = 512           # the built-in mark is 512x512; nothing may exceed it
LOGO_MAX_BYTES = 400 * 1024
# Refuse a monstrous payload on its length alone, before base64 builds a decoded
# copy of it in memory. 4/3 with room for padding and a data: prefix.
LOGO_MAX_B64 = (LOGO_MAX_BYTES + 2) // 3 * 4 + 64
PNG_SIGNATURE = b'\x89PNG\r\n\x1a\n'
PNG_IHDR_LENGTH = b'\x00\x00\x00\x0d'      # IHDR is 13 bytes, always, by spec
PNG_IEND = b'\x00\x00\x00\x00IEND\xaeB\x60\x82'   # the whole terminating chunk
PNG_DATA_URI = 'data:image/png;base64,'


def _validate_logo_png(value):
    """(png bytes, width, height) — or ValueError carrying a sentence for the user.

    The one gate every logo passes, whether it arrived from the browser or out
    of a backup file, which is user input wearing a hat. Stdlib only: a PNG's
    size lives at a fixed offset, so there is nothing here worth an image
    library.

    What it checks: the base64 is canonical and within the byte cap; the file
    opens with the PNG signature; the FIRST chunk is an IHDR of the 13 bytes
    the spec fixes it at; the width and height in that IHDR are each 1..512;
    and the file ENDS with the IEND chunk, so nothing is stapled on behind the
    image (a PNG with a script tag after IEND is still a valid PNG to a
    browser, and would be a valid anything-else to whatever reads it next).

    What it does not check: pixel data, chunk CRCs, or any chunk between IHDR
    and IEND. This decides whether to store a blob and hand it back with
    Content-Type: image/png and nosniff, not whether it renders prettily.

    Whitespace inside the base64 is refused rather than stripped: a hand-edited
    or line-wrapped backup should lose its logo into skipped_rows loudly, not
    have this guess at what the editor meant.
    """
    if not isinstance(value, str) or not value.strip():
        raise ValueError('Send the logo as base64-encoded PNG bytes.')
    raw64 = value.strip()
    if raw64.startswith(PNG_DATA_URI):
        raw64 = raw64[len(PNG_DATA_URI):]
    too_big = (f'That image is larger than {LOGO_MAX_BYTES // 1024} KB. '
               'Save it smaller and try again.')
    if len(raw64) > LOGO_MAX_B64:
        raise ValueError(too_big)
    try:
        raw = base64.b64decode(raw64, validate=True)
    except (binascii.Error, ValueError):
        raise ValueError('That does not decode as base64. Re-upload the image.')
    if len(raw) > LOGO_MAX_BYTES:
        raise ValueError(too_big)
    # A PNG opens with the 8-byte signature, then an 8-byte chunk header, then
    # IHDR's width and height. Anything shorter than 24 bytes is lying about
    # being one, and slicing it without this check is a 500, not a refusal.
    if (len(raw) < 24 or not raw.startswith(PNG_SIGNATURE)
            or raw[8:12] != PNG_IHDR_LENGTH or raw[12:16] != b'IHDR'
            or not raw.endswith(PNG_IEND)):
        raise ValueError('That file is not a PNG. Upload a PNG image.')
    width, height = struct.unpack('>II', raw[16:24])
    if not (1 <= width <= LOGO_MAX_PX and 1 <= height <= LOGO_MAX_PX):
        raise ValueError(f'The logo must be at most {LOGO_MAX_PX} x {LOGO_MAX_PX} pixels; '
                         f'that one is {width} x {height}.')
    return raw, width, height


def _resolved_logos(conn):
    """{unit_id: {'unit_id': owner, 'v': hash, 'name': owner's name} or None}.

    Two queries for the whole tenant; the walk upward happens in Python,
    because one query per unit is how a units page becomes forty round trips.
    Resolution has to see units ABOVE the caller's own subtree — a team
    leader's sidebar shows the company's logo — which is exactly what RLS
    already scopes these two SELECTs to: the tenant, no more and no less.

    `name` travels with the logo because the owning unit is, whenever the logo
    is inherited, an ANCESTOR — and /api/units returns only the caller's own
    subtree. A client looking the owner up in what it was given would miss
    every single time and conclude there was no logo.
    """
    rows = conn.execute('SELECT id, parent_id, name FROM units').fetchall()
    parent = {r['id']: r['parent_id'] for r in rows}
    names = {r['id']: r['name'] for r in rows}
    owned = {r['unit_id']: r['v'] for r in conn.execute(
        'SELECT unit_id, substr(md5(value), 1, 12) AS v FROM settings '
        'WHERE key = %s AND unit_id IS NOT NULL', (LOGO_KEY,)).fetchall()}
    resolved = {}
    for start in parent:
        chain, cur, seen = [], start, set()
        while cur is not None and cur not in resolved and cur not in owned and cur not in seen:
            seen.add(cur)
            chain.append(cur)
            cur = parent.get(cur)
        if cur is None or cur in seen:
            found = None                       # nothing up the path (or a cycle)
        elif cur in resolved:
            found = resolved[cur]
        else:
            found = {'unit_id': cur, 'v': owned[cur], 'name': names.get(cur, '')}
        for unit_id in chain:
            resolved[unit_id] = found
        resolved.setdefault(start, found)
    return resolved


def _unit_json(row, count=None, logos=None):
    out = {k: row[k] for k in ('id', 'parent_id', 'kind', 'name', 'slug')}
    if count is not None:
        out['count'] = count
    # Always present, never guessed: a caller that skipped resolution would
    # otherwise quietly report "no logo" for a unit that has one.
    out['logo'] = (logos or {}).get(row['id'])
    return out


def _seed_root_defaults(conn, root_id, unit_id):
    """What a brand-new root starts with: the org clock and empty TDY lists."""
    conn.execute('INSERT INTO settings (root_id, unit_id, key, value) VALUES (%s, NULL, %s, %s)',
                 (root_id, TIMEZONE_KEY, FALLBACK_TZ))
    for kind in ('schools', 'locations'):
        conn.execute('INSERT INTO settings (root_id, unit_id, key, value) VALUES (%s, %s, %s, %s)',
                     (root_id, unit_id, f'tdy_{kind}', '[]'))


@app.route('/api/units', methods=['GET'])
@login_required
def list_units():
    conn = get_db()
    ids = current_subtree()
    if not ids:
        return jsonify([])
    rows = conn.execute(
        'SELECT u.*, (SELECT COUNT(*) FROM personnel p WHERE p.unit_id = u.id) AS n '
        'FROM units u WHERE u.id = ANY(%s) ORDER BY u.parent_id NULLS FIRST, u.name', (list(ids),)
    ).fetchall()
    logos = _resolved_logos(conn)
    return jsonify([_unit_json(r, r['n'], logos) for r in rows])


@app.route('/api/units', methods=['POST'])
@login_required
def create_unit():
    data = request.get_json() or {}
    name = ' '.join((data.get('name') or '').split())[:80]
    kind = (data.get('kind') or '').strip().lower()
    if not name:
        return jsonify({'error': 'Give the unit a name.'}), 400
    if kind not in UNIT_KINDS:
        return jsonify({'error': f'kind must be one of {", ".join(UNIT_KINDS)}'}), 400
    user = g.current_user
    conn = get_db()
    parent_id = data.get('parent_id')
    if parent_id is not None:
        try:
            parent_id = int(parent_id)
        except (TypeError, ValueError):
            return jsonify({'error': 'parent_id must be a unit id.'}), 400

    if parent_id is None:
        # A new root. Only someone attached nowhere may do this; an attached
        # user adds children to the tree they are in.
        if user.get('unit_id') is not None:
            return jsonify({'error': 'You already belong to a unit; add a child unit instead.'}), 400
        root_id = conn.execute('SELECT auth_create_root_unit(%s, %s, %s, %s) AS id',
                               (name, kind, slugify(name), user['id'])).fetchone()['id']
        # From here on this request IS in the new tenant.
        set_tenant(conn, root_id)
        g.current_user = dict(user, unit_id=root_id, role='owner', root_id=root_id)
        g.tz = FALLBACK_TZ
        g.pop('subtree', None)
        _seed_root_defaults(conn, root_id, root_id)
        log_action('UNIT_CREATE', f'Created {kind} "{name}" (new organization)', root_id)
        return jsonify(_unit_json(_unit_row(conn, root_id), 0, _resolved_logos(conn))), 201

    if user.get('unit_id') is None or not can_access(parent_id):
        return jsonify({'error': 'Forbidden'}), 403
    parent = _unit_row(conn, parent_id)
    if parent is None:
        return jsonify({'error': 'Not found'}), 404
    slug = unique_slug(conn, parent['root_id'], name)
    row = conn.execute(
        'INSERT INTO units (parent_id, root_id, kind, name, slug) VALUES (%s, %s, %s, %s, %s) RETURNING *',
        (parent_id, parent['root_id'], kind, name, slug)).fetchone()
    for k in ('schools', 'locations'):
        conn.execute('INSERT INTO settings (root_id, unit_id, key, value) VALUES (%s, %s, %s, %s)',
                     (parent['root_id'], row['id'], f'tdy_{k}', '[]'))
    g.pop('subtree', None)
    log_action('UNIT_CREATE', f'Created {kind} "{name}" under {parent["name"]}', row['id'])
    # A brand-new unit has no logo of its own but already inherits its parent's.
    return jsonify(_unit_json(row, 0, _resolved_logos(conn))), 201


@app.route('/api/units/<int:unit_id>', methods=['PUT'])
@attached_required
def update_unit(unit_id):
    conn = get_db()
    row = _unit_row(conn, unit_id)
    if row is None:
        return jsonify({'error': 'Not found'}), 404
    if not can_access(unit_id):
        return jsonify({'error': 'Forbidden'}), 403
    if row['parent_id'] is None and not is_owner(g.current_user):
        return jsonify({'error': 'Only an owner can change the root unit.'}), 403
    data = request.get_json() or {}
    fields, values = [], []
    if 'name' in data:
        name = ' '.join((data['name'] or '').split())[:80]
        if not name:
            return jsonify({'error': 'Give the unit a name.'}), 400
        fields.append('name = %s'); values.append(name)
    if 'kind' in data:
        if data['kind'] not in UNIT_KINDS:
            return jsonify({'error': f'kind must be one of {", ".join(UNIT_KINDS)}'}), 400
        fields.append('kind = %s'); values.append(data['kind'])
    if not fields:
        return jsonify({'error': 'Nothing to update'}), 400
    values.append(unit_id)
    conn.execute(f'UPDATE units SET {", ".join(fields)} WHERE id = %s', values)
    log_action('UNIT_RENAME', f'{row["name"]} -> {data.get("name", row["name"])} ({data.get("kind", row["kind"])})', unit_id)
    return jsonify(_unit_json(_unit_row(conn, unit_id), None, _resolved_logos(conn)))


@app.route('/api/units/<int:unit_id>', methods=['DELETE'])
@attached_required
def delete_unit(unit_id):
    conn = get_db()
    row = _unit_row(conn, unit_id)
    if row is None:
        return jsonify({'error': 'Not found'}), 404
    if not can_access(unit_id):
        return jsonify({'error': 'Forbidden'}), 403
    if row['parent_id'] is None and not is_owner(g.current_user):
        return jsonify({'error': 'Only an owner can delete the root unit.'}), 403
    blockers = conn.execute(
        'SELECT (SELECT COUNT(*) FROM units WHERE parent_id = %s) AS children, '
        '(SELECT COUNT(*) FROM personnel WHERE unit_id = %s) AS people, '
        '(SELECT COUNT(*) FROM users WHERE unit_id = %s) AS users',
        (unit_id, unit_id, unit_id)).fetchone()
    if any(blockers.values()):
        return jsonify({'error': 'Move or remove its units, soldiers and users first.', **blockers}), 409
    conn.execute('DELETE FROM settings WHERE unit_id = %s', (unit_id,))
    # Invites are short-lived credentials, not data (which is why backup skips
    # them); one naming a unit that no longer exists could only ever fail.
    conn.execute('DELETE FROM invites WHERE unit_id = %s', (unit_id,))
    conn.execute('DELETE FROM units WHERE id = %s', (unit_id,))
    g.pop('subtree', None)
    log_action('UNIT_DELETE', f'Deleted {row["kind"]} "{row["name"]}"', row['parent_id'])
    return jsonify({'success': True})


def _logo_writable(conn, unit_id):
    """The guard both writing verbs share.

    Another tenant's id is a 404 (RLS never showed us the row) and so is a
    made-up one: same answer either way, so nothing here says whether a unit
    exists. Inside the tenant but outside your subtree is an ordinary 403.
    """
    if _unit_row(conn, unit_id) is None:
        return jsonify({'error': 'Not found'}), 404
    if not can_access(unit_id):
        return jsonify({'error': 'Forbidden'}), 403
    return None


@app.route('/api/units/<int:unit_id>/logo', methods=['PUT'])
@attached_required
def set_unit_logo(unit_id):
    conn = get_db()
    denied = _logo_writable(conn, unit_id)
    if denied:
        return denied
    data = request.get_json(silent=True) or {}
    try:
        raw, width, height = _validate_logo_png(data.get('png_base64'))
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400
    # Store the re-encoded bytes, not the string that arrived: a data: prefix,
    # stray whitespace or non-canonical padding would otherwise change the
    # version hash without changing the image.
    value = base64.b64encode(raw).decode('ascii')
    conn.execute(
        'INSERT INTO settings (root_id, unit_id, key, value) VALUES (%s, %s, %s, %s) '
        'ON CONFLICT (root_id, COALESCE(unit_id, 0), key) DO UPDATE SET value = EXCLUDED.value',
        (_root(), unit_id, LOGO_KEY, value))
    log_action('UNIT_LOGO', f'Logo set ({width}x{height}, {len(raw)} bytes)', unit_id)
    # No logo object here on purpose. The client re-reads /api/units for the
    # new version anyway, and a second implementation of the version hash —
    # in Python, next to the one in SQL — is two things to keep in step and
    # one of them unread.
    return jsonify({'success': True})


@app.route('/api/units/<int:unit_id>/logo', methods=['DELETE'])
@attached_required
def clear_unit_logo(unit_id):
    conn = get_db()
    denied = _logo_writable(conn, unit_id)
    if denied:
        return denied
    removed = conn.execute('DELETE FROM settings WHERE unit_id = %s AND key = %s',
                           (unit_id, LOGO_KEY)).rowcount
    # Idempotent, and only a real removal is audited: the log says what changed.
    if removed:
        log_action('UNIT_LOGO', 'Logo removed', unit_id)
    return jsonify({'success': True})


@app.route('/api/units/<int:unit_id>/logo', methods=['GET'])
@attached_required
def get_unit_logo(unit_id):
    """The PNG of the nearest unit on self -> parent -> ... -> root that has one.

    Deliberately NOT gated on can_access: any attached member of the tenant may
    read any unit's resolved logo, because a team leader's own sidebar shows
    the company's mark and a logo is branding, not data.
    """
    conn = get_db()
    if _unit_row(conn, unit_id) is None:
        return jsonify({'error': 'Not found'}), 404
    resolved = _resolved_logos(conn).get(unit_id)
    row = conn.execute('SELECT value FROM settings WHERE unit_id = %s AND key = %s',
                       (resolved['unit_id'], LOGO_KEY)).fetchone() if resolved else None
    try:
        raw, _, _ = _validate_logo_png(row['value']) if row else (None, 0, 0)
    except ValueError:
        # Stored bytes that would not survive their own validator: say there is
        # no logo rather than hand a browser something nobody vouched for.
        app.logger.warning('unit %s holds an unreadable logo', resolved['unit_id'])
        raw = None
    if raw is None:
        return jsonify({'error': 'Not found'}), 404
    return Response(raw, mimetype='image/png', headers={
        # The client cache-busts with ?v=<hash>, so the bytes at a given URL
        # genuinely never change. Private: it is one tenant's mark.
        'Cache-Control': 'private, max-age=31536000, immutable',
        'X-Content-Type-Options': 'nosniff',
    })


# ── Personnel routes ──

@app.route('/api/personnel', methods=['GET'])
@attached_required
def get_personnel():
    scope = _unit_scope(request.args.get('unit'))
    if scope is None:
        return jsonify({'error': 'Forbidden'}), 403
    unit_id, ids = scope
    conn = get_db()
    _reconcile_absences(conn, app_today())
    rows = conn.execute(
        'SELECT * FROM personnel WHERE unit_id = ANY(%s) ORDER BY rank, last, first', (list(ids),)
    ).fetchall()
    scheduled_rows = conn.execute(
        "SELECT * FROM scheduled_events WHERE unit_id = ANY(%s) AND state != 'completed' "
        'ORDER BY from_date, to_date, id', (list(ids),)
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
@attached_required
def add_person():
    data = request.get_json() or {}
    unit_id = data.get('unit_id')
    if not can_access(unit_id):
        return jsonify({'error': 'Forbidden'}), 403
    conn = get_db()
    cur = conn.execute(
        'INSERT INTO personnel (rank, last, first, unit_id, root_id) VALUES (%s, %s, %s, %s, %s) RETURNING id',
        (data.get('rank', ''), data.get('last', ''), data.get('first', ''), int(unit_id), _root())
    )
    new_id = cur.fetchone()['id']
    row = conn.execute('SELECT * FROM personnel WHERE id = %s', (new_id,)).fetchone()
    log_action('ADD_PERSON', f'{data.get("rank","")} {data.get("last","")}, {data.get("first","")}', int(unit_id))
    return jsonify(dict(row)), 201


@app.route('/api/personnel/<int:person_id>', methods=['PUT'])
@attached_required
def update_person(person_id):
    data = request.get_json() or {}
    fields, values = [], []
    for col in ('rank', 'last', 'first', 'status', 'notes', 'from_date', 'to_date', 'present_date'):
        if col in data:
            fields.append(f'{col} = %s')
            values.append(data[col])
    conn = get_db()
    person = _person_or_none(conn, person_id)
    if person is None:
        return jsonify({'error': 'Not found'}), 404
    if not can_access(person['unit_id']):
        return jsonify({'error': 'Forbidden'}), 403
    moved_to = None
    if 'unit_id' in data:
        # can_access() before int(): it already answers False for anything that
        # is not a unit id the caller may see, so junk in the body is a 403
        # rather than a 500 on the cast.
        if not can_access(data['unit_id']):
            return jsonify({'error': 'Forbidden'}), 403
        if int(data['unit_id']) != person['unit_id']:
            moved_to = int(data['unit_id'])
            fields.append('unit_id = %s'); values.append(moved_to)
    if not fields:
        return jsonify({'error': 'No fields to update'}), 400
    values.append(person_id)
    conn.execute(f'UPDATE personnel SET {", ".join(fields)} WHERE id = %s', values)
    if moved_to is not None:
        # Child rows follow the soldier so the subtree view and RLS cache stay true.
        conn.execute('UPDATE scheduled_events SET unit_id = %s WHERE person_id = %s', (moved_to, person_id))
        conn.execute('UPDATE duty_roster SET unit_id = %s WHERE person_id = %s', (moved_to, person_id))
        log_action('PERSON_MOVE', f'{person["rank"]} {person["last"]}, {person["first"]}: unit {person["unit_id"]} -> {moved_to}', moved_to)
    # Only the transition matters: apiUpdate() resends the current status on
    # every save, so a TDY soldier being marked present-for-today still PUTs
    # status='tdy' and must not have their absence closed.
    if data.get('status') == 'present' and person['status'] in ABSENCE_STATUSES:
        _end_running_absence(conn, person_id, app_today())
    row = conn.execute('SELECT * FROM personnel WHERE id = %s', (person_id,)).fetchone()
    if 'status' in data and data['status'] != person['status']:
        log_action('UPDATE_STATUS', f'{person["rank"]} {person["last"]}, {person["first"]}: {person["status"]} -> {data["status"]}', person['unit_id'])
    return jsonify(dict(row))


PROFILE_FIELDS = (
    'phone', 'email', 'address', 'emergency_name', 'emergency_phone',
    'spouse_dependents', 'next_of_kin', 'dod_id', 'date_of_rank', 'mos',
    'clearance', 'ets_date', 'section', 'profile_notes',
    'flags', 'medical_date', 'dental_date', 'weapons_qual', 'dob',
)


@app.route('/api/personnel/<int:person_id>/profile', methods=['GET'])
@attached_required
def get_profile(person_id):
    conn = get_db()
    person = _person_or_none(conn, person_id)
    if person is None:
        return jsonify({'error': 'Not found'}), 404
    if not can_access(person['unit_id']):
        return jsonify({'error': 'Forbidden'}), 403
    row = conn.execute('SELECT * FROM personnel_profile WHERE person_id = %s', (person_id,)).fetchone()
    profile = dict(row) if row else {'person_id': person_id, **{f: '' for f in PROFILE_FIELDS}}
    return jsonify(profile)


@app.route('/api/personnel/<int:person_id>/profile', methods=['PUT'])
@attached_required
def update_profile(person_id):
    data = request.get_json() or {}
    conn = get_db()
    person = _person_or_none(conn, person_id)
    if person is None:
        return jsonify({'error': 'Not found'}), 404
    if not can_access(person['unit_id']):
        return jsonify({'error': 'Forbidden'}), 403

    updates = {f: data[f] for f in PROFILE_FIELDS if f in data}
    if not updates:
        return jsonify({'error': 'No fields to update'}), 400

    # Ensure a row exists, then update only the provided columns. The tenant
    # comes from the soldier, never from the request.
    conn.execute(
        'INSERT INTO personnel_profile (person_id, root_id) VALUES (%s, %s) ON CONFLICT (person_id) DO NOTHING',
        (person_id, person['root_id'])
    )
    assignments = ', '.join(f'{col} = %s' for col in updates)
    conn.execute(
        f'UPDATE personnel_profile SET {assignments} WHERE person_id = %s',
        [*updates.values(), person_id]
    )
    row = conn.execute('SELECT * FROM personnel_profile WHERE person_id = %s', (person_id,)).fetchone()
    log_action('UPDATE_PROFILE', f'{person["rank"]} {person["last"]}, {person["first"]}', person['unit_id'])
    return jsonify(dict(row))


@app.route('/api/personnel/<int:person_id>/schedule', methods=['POST'])
@attached_required
def add_scheduled_event(person_id):
    data = request.get_json() or {}
    conn = get_db()
    person = _person_or_none(conn, person_id)
    if person is None:
        return jsonify({'error': 'Not found'}), 404
    if not can_access(person['unit_id']):
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
        'INSERT INTO scheduled_events (person_id, unit_id, root_id, status, from_date, to_date, notes, location, state, created_at) '
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'scheduled', %s) RETURNING id",
        (person_id, person['unit_id'], person['root_id'], status, from_date, to_date,
         data.get('notes', ''), data.get('location', ''), app_stamp())
    )
    new_id = cur.fetchone()['id']
    _sync_person_status(conn, person_id, app_today())
    row = conn.execute('SELECT * FROM scheduled_events WHERE id = %s', (new_id,)).fetchone()
    log_action('SCHEDULE_STATUS', f'{person["rank"]} {person["last"]}: {status} on {data.get("from_date", "")}', person['unit_id'])
    return jsonify(dict(row)), 201


@app.route('/api/directory', methods=['GET'])
@attached_required
def get_directory():
    scope = _unit_scope(request.args.get('unit'))
    if scope is None:
        return jsonify({'error': 'Forbidden'}), 403
    unit_id, ids = scope
    conn = get_db()
    _reconcile_absences(conn, app_today())
    # unit_id rides along so a company-level directory can label which platoon
    # each soldier is in; a single-unit view simply ignores it.
    rows = conn.execute(
        'SELECT p.id, p.rank, p.last, p.first, p.status, p.from_date, p.to_date, p.notes, p.unit_id, '
        '       pp.dod_id, pp.dob, pp.mos, pp.section, pp.phone '
        'FROM personnel p LEFT JOIN personnel_profile pp ON pp.person_id = p.id '
        'WHERE p.unit_id = ANY(%s) ORDER BY p.rank, p.last, p.first', (list(ids),)
    ).fetchall()
    upcoming = conn.execute(
        "SELECT person_id, status, from_date, to_date FROM scheduled_events "
        "WHERE unit_id = ANY(%s) AND state = 'scheduled' ORDER BY from_date, id", (list(ids),)
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
@attached_required
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
    scope = _unit_scope(request.args.get('unit'))
    if scope is None:
        return jsonify({'error': 'Forbidden'}), 403
    unit_id, ids = scope

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
        'SELECT id, rank, last, first, status FROM personnel WHERE unit_id = ANY(%s) '
        'ORDER BY rank, last, first', (list(ids),)
    ).fetchall()
    # Windows that overlap the question at all; _covered_days works out which
    # days exactly. Open bounds are stored as '' and must not be compared.
    events = conn.execute(
        'SELECT * FROM scheduled_events WHERE unit_id = ANY(%s) '
        "AND (from_date = '' OR from_date <= %s) AND (to_date = '' OR to_date >= %s) "
        'ORDER BY from_date, id', (list(ids), end, start)
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
        'unit': unit_id, 'date': start, 'to': end, 'span': span,
        'available': available, 'unavailable': unavailable,
    })


@app.route('/api/personnel/<int:person_id>/absences', methods=['GET'])
@attached_required
def get_absences(person_id):
    conn = get_db()
    person = _person_or_none(conn, person_id)
    if person is None:
        return jsonify({'error': 'Not found'}), 404
    if not can_access(person['unit_id']):
        return jsonify({'error': 'Forbidden'}), 403
    _reconcile_absences(conn, app_today())
    rows = conn.execute(
        'SELECT * FROM scheduled_events WHERE person_id = %s ORDER BY from_date DESC, id DESC',
        (person_id,)
    ).fetchall()
    return jsonify({'absences': [dict(r) for r in rows]})


@app.route('/api/schedules/<int:event_id>', methods=['PUT'])
@attached_required
def update_scheduled_event(event_id):
    data = request.get_json() or {}
    conn = get_db()
    row = conn.execute('SELECT * FROM scheduled_events WHERE id = %s', (event_id,)).fetchone()
    if row is None:
        return jsonify({'error': 'Not found'}), 404
    if not can_access(row['unit_id']):
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
    log_action('EDIT_SCHEDULE', f'{who}{status} {from_date} - {to_date or "open"}', row['unit_id'])
    return jsonify(dict(updated))


@app.route('/api/schedules/<int:event_id>', methods=['DELETE'])
@attached_required
def delete_scheduled_event(event_id):
    conn = get_db()
    row = conn.execute('SELECT * FROM scheduled_events WHERE id = %s', (event_id,)).fetchone()
    if row is None:
        return jsonify({'error': 'Not found'}), 404
    if not can_access(row['unit_id']):
        return jsonify({'error': 'Forbidden'}), 403
    person_id = row['person_id']
    conn.execute('DELETE FROM scheduled_events WHERE id = %s', (event_id,))
    # Cancelling an in-progress absence returns the soldier to duty.
    _sync_person_status(conn, person_id, app_today())
    log_action('DELETE_SCHEDULE', f'{row["status"]} on {row["from_date"]}', row['unit_id'])
    return jsonify({'success': True})


@app.route('/api/personnel/<int:person_id>', methods=['DELETE'])
@attached_required
def delete_person(person_id):
    conn = get_db()
    row = _person_or_none(conn, person_id)
    if row is None:
        return jsonify({'error': 'Not found'}), 404
    if not can_access(row['unit_id']):
        return jsonify({'error': 'Forbidden'}), 403
    log_action('DELETE_PERSON', f'{row["rank"]} {row["last"]}, {row["first"]}', row['unit_id'])
    conn.execute('DELETE FROM scheduled_events WHERE person_id = %s', (person_id,))
    conn.execute('DELETE FROM personnel_profile WHERE person_id = %s', (person_id,))
    conn.execute('DELETE FROM personnel WHERE id = %s', (person_id,))
    return jsonify({'success': True})


@app.route('/api/settings', methods=['GET'])
@attached_required
def get_settings():
    scope = _unit_scope(request.args.get('unit'))
    if scope is None:
        return jsonify({'error': 'Forbidden'}), 403
    unit_id, _ = scope
    conn = get_db()
    unit = _unit_row(conn, unit_id)
    return jsonify({
        'unit_name': unit['name'],
        'kind': unit['kind'],
        'tdy_schools': _get_tdy_list(conn, 'schools', unit_id),
        'tdy_locations': _get_tdy_list(conn, 'locations', unit_id),
        # One clock for the whole tenant, whichever unit was asked about.
        'timezone': app_timezone(),
    })


@app.route('/api/settings', methods=['PUT'])
@attached_required
def update_settings():
    scope = _unit_scope(request.args.get('unit'))
    if scope is None:
        return jsonify({'error': 'Forbidden'}), 403
    unit_id, _ = scope
    data = request.get_json() or {}
    conn = get_db()
    logs = []
    if 'timezone' in data:
        if not is_owner(g.current_user):
            return jsonify({'error': 'Only an owner can change the organization timezone.'}), 403
        try:
            new_tz, _ = validate_timezone(data['timezone'])
        except ValueError as exc:
            return jsonify({'error': str(exc)}), 400
        # The ON CONFLICT target is spelled exactly like the settings_scope_key
        # index expression: Postgres matches an expression index by the text of
        # the expression, not by what it evaluates to.
        conn.execute(
            'INSERT INTO settings (root_id, unit_id, key, value) VALUES (%s, NULL, %s, %s) '
            'ON CONFLICT (root_id, COALESCE(unit_id, 0), key) DO UPDATE SET value = EXCLUDED.value',
            (_root(), TIMEZONE_KEY, new_tz))
        g.tz = new_tz   # this request's own stamps use the new day from here on
        logs.append(('ORG_TIMEZONE', f'Organization timezone set to {new_tz}'))
    for field, kind in (('tdy_schools', 'schools'), ('tdy_locations', 'locations')):
        if field not in data:
            continue
        try:
            cleaned = _clean_tdy_list(data[field])
        except ValueError as exc:
            return jsonify({'error': str(exc)}), 400
        conn.execute(
            'INSERT INTO settings (root_id, unit_id, key, value) VALUES (%s, %s, %s, %s) '
            'ON CONFLICT (root_id, COALESCE(unit_id, 0), key) DO UPDATE SET value = EXCLUDED.value',
            (_root(), unit_id, f'tdy_{kind}', json.dumps(cleaned)))
        logs.append((f'Updated TDY {kind} list', f'{len(cleaned)} entries'))
    if 'unit_name' in data:
        return jsonify({'error': 'Rename the unit from the Units page.'}), 400
    for action, details in logs:
        log_action(action, details, unit_id)
    return get_settings()


# ── Audit log ──

@app.route('/api/audit', methods=['GET'])
@attached_required
def get_audit():
    raw = request.args.get('unit', '')
    if raw:
        scope = _unit_scope(raw)
        if scope is None:
            return jsonify({'error': 'Forbidden'}), 403
        ids = scope[1]
    else:
        ids = current_subtree()
    try:
        limit = min(int(request.args.get('limit', 200)), 5000)
    except ValueError:
        limit = 200
    conn = get_db()
    # Org-level rows (LOGIN, INVITE_*, BACKUP_*) belong to the tenant rather
    # than to any one unit; RLS already confines them to this root, so everyone
    # inside it sees them.
    rows = conn.execute(
        'SELECT * FROM audit_log WHERE (unit_id = ANY(%s) OR unit_id IS NULL) ORDER BY id DESC LIMIT %s',
        (list(ids), limit)
    ).fetchall()
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
@attached_required
def get_duty():
    scope = _unit_scope(request.args.get('unit'))
    if scope is None:
        return jsonify({'error': 'Forbidden'}), 403
    _, ids = scope
    date_filter = request.args.get('date', '')
    conn = get_db()
    if date_filter:
        rows = conn.execute(
            'SELECT * FROM duty_roster WHERE unit_id = ANY(%s) AND date = %s ORDER BY duty_type, id',
            (list(ids), date_filter)
        ).fetchall()
    else:
        rows = conn.execute(
            'SELECT * FROM duty_roster WHERE unit_id = ANY(%s) ORDER BY date DESC, duty_type, id LIMIT 90',
            (list(ids),)
        ).fetchall()
    out = []
    for r in rows:
        entry = dict(r)
        entry['conflict'] = _duty_conflict(conn, entry['person_id'], entry['date'])
        out.append(entry)
    return jsonify(out)


@app.route('/api/duty/conflicts', methods=['GET'])
@attached_required
def get_duty_conflicts():
    """Who in this unit's subtree is away on a given date, keyed by person id.

    Feeds the duty picker so a soldier reads as unavailable *before* you assign
    them, not after.
    """
    scope = _unit_scope(request.args.get('unit'))
    if scope is None:
        return jsonify({'error': 'Forbidden'}), 403
    _, ids = scope
    date_str = request.args.get('date', '') or app_today()
    conn = get_db()
    rows = conn.execute(
        'SELECT s.* FROM scheduled_events s JOIN personnel p ON p.id = s.person_id '
        "WHERE p.unit_id = ANY(%s) AND s.state != 'completed' ORDER BY s.from_date, s.id",
        (list(ids),)
    ).fetchall()
    # Same ordering as _duty_conflict, so a later overlapping window wins here too.
    away = {str(r['person_id']): _conflict_from(r)
            for r in rows if _derive_state(r, date_str) == 'active'}
    return jsonify(away)


@app.route('/api/duty', methods=['POST'])
@attached_required
def add_duty():
    data = request.get_json() or {}
    if not can_access(data.get('unit_id')):
        return jsonify({'error': 'Forbidden'}), 403
    unit_id = int(data['unit_id'])
    conn = get_db()
    try:
        person_id = int(data.get('person_id'))
    except (TypeError, ValueError):
        person_id = 0
    person = _person_or_none(conn, person_id)
    if person is None or not can_access(person['unit_id']):
        return jsonify({'error': 'Pick a soldier from this unit.'}), 400
    # Reachable is not the same as belonging: an owner can see every soldier in
    # the tree, but booking one onto a unit they are not in is the pre-A1
    # "another platoon" mistake wearing a new name.
    if person['unit_id'] not in subtree_ids(conn, unit_id):
        return jsonify({'error': 'That soldier is not in this unit.'}), 400

    date_str = data.get('date', '')
    duty_type = data.get('duty_type', 'CQ')
    # The entry is filed where the soldier actually is, not where the caller
    # was looking: that is what keeps it on their unit's roster after a move.
    # rank/last/first come from the database, never the client: they are a
    # snapshot so the entry still reads correctly once the soldier is gone.
    cur = conn.execute(
        'INSERT INTO duty_roster (date, unit_id, root_id, duty_type, person_id, rank, last, first, notes) '
        'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id',
        (date_str, person['unit_id'], _root(), duty_type, person['id'],
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
    log_action('ADD_DUTY', detail, person['unit_id'])
    return jsonify(row), 201


@app.route('/api/duty/<int:entry_id>', methods=['DELETE'])
@attached_required
def delete_duty(entry_id):
    conn = get_db()
    row = conn.execute('SELECT * FROM duty_roster WHERE id = %s', (entry_id,)).fetchone()
    if not row:
        return jsonify({'error': 'Not found'}), 404
    if not can_access(row['unit_id']):
        return jsonify({'error': 'Forbidden'}), 403
    log_action('DELETE_DUTY', f'{row["duty_type"]} on {row["date"]}', row['unit_id'])
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


def _prune_report_history(conn, unit_id):
    """Keep only the most recent REPORT_HISTORY_MAX rows for a unit."""
    conn.execute(
        'DELETE FROM report_history WHERE unit_id = %s AND id NOT IN ('
        '  SELECT id FROM report_history WHERE unit_id = %s ORDER BY id DESC LIMIT %s'
        ')',
        (unit_id, unit_id, REPORT_HISTORY_MAX)
    )


@app.route('/api/reports', methods=['GET'])
@attached_required
def get_reports():
    scope = _unit_scope(request.args.get('unit'))
    if scope is None:
        return jsonify({'error': 'Forbidden'}), 403
    _, ids = scope
    conn = get_db()
    rows = conn.execute(
        'SELECT id, unit_name, created_at, created_by FROM report_history '
        'WHERE unit_id = ANY(%s) ORDER BY id DESC LIMIT %s',
        (list(ids), REPORT_HISTORY_MAX)
    ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route('/api/reports', methods=['POST'])
@attached_required
def add_report():
    data = request.get_json() or {}
    if not can_access(data.get('unit_id')):
        return jsonify({'error': 'Forbidden'}), 403
    unit_id = int(data['unit_id'])
    user = g.current_user
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
        'INSERT INTO report_history (unit_id, root_id, unit_name, text, created_by, created_at) '
        'VALUES (%s, %s, %s, %s, %s, %s) RETURNING id',
        (unit_id, _root(), unit_name, text, user['username'], created_at)
    )
    new_id = cur.fetchone()['id']
    _prune_report_history(conn, unit_id)
    row = conn.execute('SELECT * FROM report_history WHERE id = %s', (new_id,)).fetchone()
    log_action('SAVE_REPORT', unit_name, unit_id)
    if not row:
        # Cannot happen with REPORT_HISTORY_MAX >= 1, but don't 500 if it ever does.
        return jsonify({'success': True}), 201
    return jsonify(dict(row)), 201


@app.route('/api/reports/<int:report_id>', methods=['GET'])
@attached_required
def get_report(report_id):
    conn = get_db()
    row = conn.execute('SELECT * FROM report_history WHERE id = %s', (report_id,)).fetchone()
    if not row:
        return jsonify({'error': 'Not found'}), 404
    if not can_access(row['unit_id']):
        return jsonify({'error': 'Forbidden'}), 403
    return jsonify(dict(row))


@app.route('/api/reports/<int:report_id>', methods=['DELETE'])
@attached_required
def delete_report(report_id):
    conn = get_db()
    row = conn.execute('SELECT * FROM report_history WHERE id = %s', (report_id,)).fetchone()
    if not row:
        return jsonify({'error': 'Not found'}), 404
    if not can_access(row['unit_id']):
        return jsonify({'error': 'Forbidden'}), 403
    # Your own report is yours to withdraw; everyone else's takes an owner.
    if row['created_by'] != g.current_user['username'] and not is_owner(g.current_user):
        return jsonify({'error': 'Forbidden'}), 403
    conn.execute('DELETE FROM report_history WHERE id = %s', (report_id,))
    log_action('DELETE_REPORT', row['unit_name'], row['unit_id'])
    return jsonify({'success': True})


# ── Backup / Restore ──

@app.route('/api/backup', methods=['GET'])
@attached_required
def export_backup():
    from flask import Response
    conn = get_db()
    ids = list(current_subtree())
    units = conn.execute('SELECT * FROM units WHERE id = ANY(%s) ORDER BY id', (ids,)).fetchall()
    slug_of = {u['id']: u['slug'] for u in units}

    def with_unit(rows):
        out = []
        for r in rows:
            d = dict(r)
            d['unit'] = slug_of.get(d.pop('unit_id'))
            d.pop('root_id', None)
            out.append(d)
        return out

    payload = {
        'version': 3,
        'exported_at': app_stamp(),
        'root_unit': next(u['slug'] for u in units if u['id'] == g.current_user['unit_id']),
        'units': [{'slug': u['slug'], 'parent_slug': slug_of.get(u['parent_id']),
                   'kind': u['kind'], 'name': u['name']} for u in units],
        'personnel': with_unit(conn.execute(
            'SELECT * FROM personnel WHERE unit_id = ANY(%s) ORDER BY id', (ids,)).fetchall()),
        'personnel_profile': [dict(r) for r in conn.execute(
            'SELECT pp.* FROM personnel_profile pp JOIN personnel p ON p.id = pp.person_id '
            'WHERE p.unit_id = ANY(%s) ORDER BY pp.person_id', (ids,)).fetchall()],
        'scheduled_events': with_unit(conn.execute(
            'SELECT * FROM scheduled_events WHERE unit_id = ANY(%s) ORDER BY id', (ids,)).fetchall()),
        'duty_roster': with_unit(conn.execute(
            'SELECT * FROM duty_roster WHERE unit_id = ANY(%s) ORDER BY id', (ids,)).fetchall()),
        'report_history': with_unit(conn.execute(
            'SELECT * FROM report_history WHERE unit_id = ANY(%s) ORDER BY id', (ids,)).fetchall()),
        # The organization-wide settings (unit_id NULL — the clock) belong to
        # the whole tenant, so only a backup taken from the top carries them.
        'settings': [{'unit': slug_of.get(r['unit_id']), 'key': r['key'], 'value': r['value']}
                     for r in conn.execute(
                         'SELECT unit_id, key, value FROM settings '
                         'WHERE unit_id = ANY(%s) OR (unit_id IS NULL AND %s)',
                         (ids, is_owner(g.current_user))).fetchall()],
    }
    for row in payload['personnel_profile']:
        row.pop('root_id', None)
    if is_owner(g.current_user):
        # Billing rides along as four optional keys: the mode and the trial
        # stamps, never a Stripe id or status — a restored copy must not be
        # able to claim someone else's subscription.
        users = conn.execute(
            'SELECT u.username, u.email, u.full_name, u.clerk_user_id, u.unit_id, u.role, '
            's.billing_mode, s.trial_started_at, s.trial_ends_at, s.extended_at '
            'FROM users u LEFT JOIN subscriptions s ON s.user_id = u.id '
            "WHERE u.clerk_user_id != '' AND u.unit_id = ANY(%s) ORDER BY u.id", (ids,)).fetchall()
        payload['users'] = with_unit(users)
        for row in payload['users']:
            for k in ('trial_started_at', 'trial_ends_at', 'extended_at'):
                if row.get(k) is not None:
                    row[k] = row[k].isoformat()
    log_action('BACKUP_EXPORT', f'{len(payload["personnel"])} personnel, {len(units)} units')
    body = json.dumps(payload, indent=2)
    return Response(body, mimetype='application/json',
                    headers={'Content-Disposition': f'attachment; filename=platoon-backup-{app_today()}.json'})


def _restored_stamp(value, ceiling):
    """A trial stamp off an uploaded backup, never later than `ceiling`.

    None when the key is absent or the value is not a date — what the caller
    does with that is the caller's decision, and differs per column.
    """
    if not value:
        return None
    try:
        dt = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return min(dt, ceiling)


@app.route('/api/backup/restore', methods=['POST'])
@owner_required
def import_backup():
    payload = request.get_json()
    if not payload or payload.get('version') != 3:
        return jsonify({'error': 'This file predates the unit tree; restore it before the A1 migration, '
                                 'or export a fresh version 3 backup.'}), 400
    conn = get_db()
    root_id = _root()
    ids = list(current_subtree())

    # The file's column names are attacker-supplied JSON keys, and they are
    # interpolated into the INSERT (a placeholder cannot stand in for an
    # identifier). Only real columns of the target table get through.
    known_columns = {}

    def columns_of(table):
        if table not in known_columns:
            known_columns[table] = {r['column_name'] for r in conn.execute(
                'SELECT column_name FROM information_schema.columns '
                'WHERE table_schema = current_schema() AND table_name = %s', (table,)).fetchall()}
        return known_columns[table]

    # 1. Units: match by slug within this tree, create the missing ones under
    #    their parent (parents first — the export lists them in id order, which
    #    is creation order, so a parent always precedes its child).
    existing = {u['slug']: u['id'] for u in conn.execute(
        'SELECT id, slug FROM units WHERE id = ANY(%s)', (ids,)).fetchall()}
    created_units = 0
    #    The file's slug is user input and it becomes a URL, so it goes through
    #    the same normaliser creation uses: `admin` is the platform
    #    dashboard's own route and a unit holding it could never be opened
    #    again, and `../x` or `<script>` are not slugs at all. `existing` stays
    #    keyed on the FILE's slug, because every other row in the file — every
    #    soldier, setting, user and duty turn — names its unit by that string;
    #    only what gets STORED changes.
    for u in payload.get('units', []):
        if u['slug'] in existing:
            continue
        stored = slugify(u['slug'])
        if stored in existing:
            # An earlier restore of this same file already renamed it. Re-use
            # that unit instead of making another copy on every restore.
            existing[u['slug']] = existing[stored]
            continue
        parent_id = existing.get(u.get('parent_slug')) or g.current_user['unit_id']
        row = conn.execute(
            'INSERT INTO units (parent_id, root_id, kind, name, slug) VALUES (%s, %s, %s, %s, %s) RETURNING id',
            (parent_id, root_id, u.get('kind', 'platoon'), u['name'],
             unique_slug(conn, root_id, u['slug']))).fetchone()
        existing[u['slug']] = row['id']
        created_units += 1
    g.pop('subtree', None)
    ids = list(current_subtree())

    # 2. Replace the subtree's data. FK cascade takes profiles and events with personnel.
    conn.execute('DELETE FROM duty_roster WHERE unit_id = ANY(%s)', (ids,))
    conn.execute('DELETE FROM report_history WHERE unit_id = ANY(%s)', (ids,))
    conn.execute('DELETE FROM personnel WHERE unit_id = ANY(%s)', (ids,))
    conn.execute('DELETE FROM settings WHERE unit_id = ANY(%s)', (ids,))

    resync = set()
    skipped_units = set()
    skipped_rows = 0
    # The explicit personnel ids this restore actually honoured. A dependent
    # row's person_id is just a number out of the file, and personnel(id) is a
    # *global* primary key: a profile or event whose person was skipped either
    # aborts the whole restore on the foreign key, or — when that id happens to
    # belong to another organization — lands on their soldier carrying our
    # root_id, because referential integrity is checked outside the RLS
    # policies. An id-less personnel row gets an id nobody in the file can name,
    # so its dependents have nothing to attach to either.
    restored_people = set()

    def unit_id_for(row):
        uid = existing.get(row.get('unit'))
        if uid is None:
            skipped_units.add(row.get('unit'))
        return uid

    def insert_rows(table, rows, drop=('unit',), keep_ids=None):
        nonlocal skipped_rows
        allowed = columns_of(table)
        n = 0
        for r in rows:
            uid = unit_id_for(r)
            if uid is None:
                skipped_rows += 1
                continue
            d = {k: v for k, v in r.items() if k not in drop and k in allowed}
            d['unit_id'] = uid
            d['root_id'] = root_id
            cols = ', '.join(f'"{c}"' for c in d)
            conn.execute(f'INSERT INTO {table} ({cols}) VALUES ({", ".join(["%s"] * len(d))})',
                         tuple(d.values()))
            if keep_ids is not None and 'id' in d:
                keep_ids.add(d['id'])
            n += 1
        if any('id' in r for r in rows):
            resync.add(table)
        return n

    def attached(rows, required):
        """Only the rows naming a soldier this restore actually put back.

        `required` is False for duty_roster, whose person_id is deliberately
        nullable — an old duty turn keeps its name snapshot with no soldier.
        """
        nonlocal skipped_rows
        keep = []
        for r in rows:
            person_id = r.get('person_id')
            if person_id in restored_people or (person_id is None and not required):
                keep.append(r)
            else:
                skipped_rows += 1
        return keep

    n_people = insert_rows('personnel', payload.get('personnel', []), keep_ids=restored_people)
    profile_cols = columns_of('personnel_profile')
    for r in attached(payload.get('personnel_profile', []), True):
        d = {k: v for k, v in r.items() if k in profile_cols}
        d['root_id'] = root_id
        cols = ', '.join(f'"{c}"' for c in d)
        conn.execute(f'INSERT INTO personnel_profile ({cols}) VALUES ({", ".join(["%s"] * len(d))}) '
                     'ON CONFLICT (person_id) DO NOTHING', tuple(d.values()))
    insert_rows('scheduled_events', attached(payload.get('scheduled_events', []), True))
    insert_rows('duty_roster', attached(payload.get('duty_roster', []), False))
    insert_rows('report_history', payload.get('report_history', []))
    for s in payload.get('settings', []):
        uid = existing.get(s['unit']) if s.get('unit') else None
        if s.get('unit') and uid is None:
            continue
        if uid is None and s['key'] != TIMEZONE_KEY:
            continue
        value = s['value']
        if s['key'] == LOGO_KEY:
            # A backup file is user input, so its logo goes through the same
            # gate a PUT does. One rotten value drops its own row and is
            # counted — it must not take the whole restore down with it.
            try:
                raw, _, _ = _validate_logo_png(value)
            except ValueError:
                skipped_rows += 1
                continue
            value = base64.b64encode(raw).decode('ascii')
        conn.execute(
            'INSERT INTO settings (root_id, unit_id, key, value) VALUES (%s, %s, %s, %s) '
            'ON CONFLICT (root_id, COALESCE(unit_id, 0), key) DO UPDATE SET value = EXCLUDED.value',
            (root_id, uid, s['key'], value))

    skipped_users = []
    # A backup file is attacker-supplied: /api/backup/restore is only
    # @owner_required, so an owner can hand-edit an export. billing_mode
    # belongs to billing_set_mode behind @platform_admin_required, so 'comped'
    # off the wire becomes 'default'; the trial stamps are clamped so a
    # restore can never buy more trial than a fresh account gets. extended_at
    # is taken as it stands — it only ever removes an entitlement, and a row
    # that carries one is allowed the extension days it already bought.
    trial_ceiling = billing_rules.utcnow() + billing_rules.TRIAL_DAYS * billing_rules.DAY
    extended_ceiling = trial_ceiling + billing_rules.EXTENSION_DAYS * billing_rules.DAY
    for u in payload.get('users', []):
        # Never the caller's own row: a backup taken before a promotion would
        # otherwise demote the very owner running the restore.
        if u.get('username') == g.current_user['username']:
            continue
        uid = existing.get(u.get('unit'))
        if uid is None or not u.get('clerk_user_id'):
            skipped_users.append(u.get('username'))
            continue
        # users.username is unique across every tenant, and RLS hides the row
        # holding it — so ON CONFLICT DO UPDATE on another tenant's username
        # raises and would abort the whole restore. A savepoint per user turns
        # that into one reported skip.
        conn.execute('SAVEPOINT u')
        try:
            new_id = conn.execute(
                'INSERT INTO users (username, password_hash, clerk_user_id, email, full_name, unit_id, role, root_id) '
                'VALUES (%s, %s, %s, %s, %s, %s, %s, %s) '
                'ON CONFLICT (username) DO UPDATE SET unit_id = EXCLUDED.unit_id, role = EXCLUDED.role '
                'RETURNING id',
                (u['username'], PLACEHOLDER_PASSWORD_HASH, u['clerk_user_id'], u.get('email', ''),
                 u.get('full_name', ''), uid, u.get('role', 'leader'), root_id)).fetchone()['id']
            if u.get('billing_mode') in billing_rules.MODES or u.get('trial_ends_at'):
                ceiling = extended_ceiling if u.get('extended_at') else trial_ceiling
                # The floor matters as much as the ceiling: a missing or
                # unparseable end date used to be written as NULL, which
                # _billing_row's backfill never repairs (it only fires on a
                # NULL trial_started_at) and billing_state reads as a trial
                # with TRIAL_DAYS left — for ever. A restored trial always has
                # an end, and it is never later than a fresh one's.
                conn.execute(
                    'INSERT INTO subscriptions (user_id, root_id, billing_mode, trial_started_at, trial_ends_at, extended_at) '
                    'VALUES (%s, %s, %s, %s, %s, %s) '
                    'ON CONFLICT (user_id) DO UPDATE SET billing_mode = EXCLUDED.billing_mode, '
                    'trial_started_at = EXCLUDED.trial_started_at, trial_ends_at = EXCLUDED.trial_ends_at, '
                    'extended_at = EXCLUDED.extended_at, updated_at = now()',
                    (new_id, root_id, u.get('billing_mode') if u.get('billing_mode') in ('default', 'billed') else 'default',
                     _restored_stamp(u.get('trial_started_at'), ceiling),
                     _restored_stamp(u.get('trial_ends_at'), ceiling) or ceiling,
                     u.get('extended_at') or None))
        except psycopg.Error:
            conn.execute('ROLLBACK TO SAVEPOINT u')
            skipped_users.append(u.get('username'))
        else:
            conn.execute('RELEASE SAVEPOINT u')

    # GENERATED BY DEFAULT AS IDENTITY does not advance for explicit ids (A0
    # lesson), so the sequence has to be wound past everything this restore
    # claimed. COALESCE(MAX(id), 1) alone is the A0 form and is wrong here:
    # RLS hides every other tenant's rows, so the visible maximum can sit far
    # below where the shared sequence actually stands, and setval() would wind
    # it *backwards* onto ids another organization already holds. nextval() is
    # the floor — the sequence only ever moves forward.
    for table in resync:
        seq = f"pg_get_serial_sequence('{table}', 'id')"
        conn.execute(f'SELECT setval({seq}, GREATEST(nextval({seq}), '
                     f'COALESCE((SELECT MAX(id) FROM {table}), 1)), true)')
    log_action('BACKUP_RESTORE', f'{n_people} personnel, {created_units} units created, '
                                 f'{skipped_rows} rows skipped, {len(skipped_users)} users skipped')
    return jsonify({'success': True, 'personnel': n_people, 'units_created': created_units,
                    'skipped_units': sorted(u for u in skipped_units if u),
                    'skipped_rows': skipped_rows, 'skipped_users': skipped_users})


@app.route('/api/activate-scheduled', methods=['POST'])
@attached_required
def activate_scheduled():
    today_str = app_today()
    conn = get_db()
    result = _reconcile_absences(conn, today_str)
    return jsonify(result)


# ── Reset route ──

@app.route('/api/reset', methods=['POST'])
@attached_required
def reset_day():
    data = request.get_json() or {}
    unit_id = data.get('unit_id')
    conn = get_db()
    if unit_id:
        scope = _unit_scope(unit_id)
        if scope is None:
            return jsonify({'error': 'Forbidden'}), 403
        unit_id, ids = scope
        conn.execute(
            "UPDATE personnel SET present_date = '' WHERE status = 'present' AND unit_id = ANY(%s)",
            (list(ids),)
        )
    else:
        # A whole-organization reset touches every unit; only an owner may do
        # it. RLS is what bounds the unscoped UPDATE to this tenant.
        if not is_owner(g.current_user):
            return jsonify({'error': 'Forbidden'}), 403
        conn.execute("UPDATE personnel SET present_date = '' WHERE status = 'present'")
    log_action('RESET_DAY', f'Day reset for unit: {unit_id or "all"}', unit_id)
    return jsonify({'success': True})


# ── Absence reconciliation (runs from every roster read) ──

def _absence_audit(conn, action, row, details):
    # The scope comes off the event row itself: this runs from reconciliation,
    # which has no request user, and root_id must match the row RLS is holding
    # this transaction to or the whole request aborts.
    conn.execute(
        'INSERT INTO audit_log (user_id, username, action, details, unit_id, root_id, timestamp) '
        'VALUES (0, %s, %s, %s, %s, %s, %s)',
        ('system', action, details, row['unit_id'], row['root_id'], app_stamp())
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
    handful of people per unit, so the per-person queries are cheaper than
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


if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=True)
