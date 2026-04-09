import sqlite3
import os
import secrets
import string
import threading
import time
import base64
from datetime import datetime, date
from functools import wraps
from datetime import timedelta
from urllib.error import URLError
from flask import Flask, request, jsonify, send_from_directory, session, g
from werkzeug.security import generate_password_hash
from werkzeug.middleware.proxy_fix import ProxyFix
import jwt
from jwt import PyJWKClient

app = Flask(__name__, static_folder='.', static_url_path='')
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)
app.secret_key = os.environ.get('SECRET_KEY', 'platoon-tracker-change-in-production')
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SECURE'] = os.environ.get('HTTPS', 'false').lower() == 'true'
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=12)
app.config['SESSION_PERMANENT'] = True

_default_data_dir = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(os.environ.get('DATA_DIR', _default_data_dir), 'accountability.db')
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
_JWKS_CLIENT = PyJWKClient(CLERK_JWKS_URL) if CLERK_ENABLED else None

PLATOONS = {
    '1st': '1st Platoon Accountability',
    '2nd': '2nd Platoon Accountability',
    'hq':  'HQ Platoon Accountability'
}


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    cur = conn.cursor()

    cur.execute('''
        CREATE TABLE IF NOT EXISTS personnel (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
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
            key   TEXT PRIMARY KEY,
            value TEXT
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
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
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT DEFAULT (datetime('now')),
            user_id   INTEGER DEFAULT 0,
            username  TEXT DEFAULT '',
            action    TEXT DEFAULT '',
            details   TEXT DEFAULT '',
            platoon   TEXT DEFAULT ''
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS duty_roster (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            date      TEXT NOT NULL,
            platoon   TEXT NOT NULL,
            duty_type TEXT NOT NULL DEFAULT 'CQ',
            rank      TEXT DEFAULT '',
            last      TEXT DEFAULT '',
            first     TEXT DEFAULT '',
            notes     TEXT DEFAULT ''
        )
    ''')

    # ── Migrations ──
    cols = [row[1] for row in cur.execute('PRAGMA table_info(personnel)').fetchall()]
    if 'present_date' not in cols:
        cur.execute('ALTER TABLE personnel ADD COLUMN present_date TEXT DEFAULT ""')
    if 'platoon' not in cols:
        cur.execute('ALTER TABLE personnel ADD COLUMN platoon TEXT DEFAULT "2nd"')
        cur.execute('UPDATE personnel SET platoon = "2nd" WHERE platoon IS NULL OR platoon = ""')

    ucols = [row[1] for row in cur.execute('PRAGMA table_info(users)').fetchall()]
    if 'pin_hash' not in ucols:
        cur.execute('ALTER TABLE users ADD COLUMN pin_hash TEXT DEFAULT ""')
    for col in ('clerk_user_id', 'email', 'full_name'):
        if col not in ucols:
            cur.execute(f'ALTER TABLE users ADD COLUMN {col} TEXT DEFAULT ""')

    # Only enforce uniqueness for actual Clerk IDs; legacy rows may still have empty values.
    cur.execute('DROP INDEX IF EXISTS idx_users_clerk_user_id')
    cur.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_clerk_user_id "
        "ON users(clerk_user_id) WHERE clerk_user_id IS NOT NULL AND clerk_user_id != ''"
    )

    # Scheduled TDY/Leave columns
    for col, default in [('sched_status',''), ('sched_from',''), ('sched_to',''), ('sched_notes','')]:
        if col not in cols:
            cur.execute(f'ALTER TABLE personnel ADD COLUMN {col} TEXT DEFAULT ""')

    # ── Seed legacy admin user only when Clerk is not configured ──
    cur.execute('SELECT COUNT(*) FROM users')
    if cur.fetchone()[0] == 0 and not CLERK_ENABLED:
        password = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(12))
        cur.execute(
            'INSERT OR IGNORE INTO users (username, password_hash, is_admin, platoons) VALUES (?, ?, 1, ?)',
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

    # ── Seed placeholder data ──
    for platoon in ('1st', '2nd', 'hq'):
        cur.execute('SELECT COUNT(*) FROM personnel WHERE platoon = ?', (platoon,))
        if cur.fetchone()[0] == 0:
            cur.execute(
                'INSERT INTO personnel (rank, last, first, status, platoon) VALUES (?, ?, ?, ?, ?)',
                ('WO1', 'Smith', 'John', 'present', platoon)
            )

    conn.commit()
    conn.close()


init_db()


# ── Audit log helper ──

def log_action(action, details='', platoon=''):
    try:
        conn = get_db()
        user_id, username = 0, 'system'
        try:
            user = getattr(g, 'current_user', None)
            if user:
                user_id, username = user['id'], user['username']
        except Exception:
            pass
        conn.execute(
            'INSERT INTO audit_log (user_id, username, action, details, platoon) VALUES (?, ?, ?, ?, ?)',
            (user_id, username, action, str(details), platoon)
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


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


def _verify_clerk_session_token():
    if not CLERK_ENABLED:
        return None, 'Clerk is not configured on the server.'

    token = _get_session_token()
    if not token:
        return None, 'Unauthorized'

    try:
        signing_key = _JWKS_CLIENT.get_signing_key_from_jwt(token)
        claims = jwt.decode(
            token,
            signing_key.key,
            algorithms=['RS256'],
            options={'require': ['exp', 'iat', 'nbf', 'sub']},
        )
    except (jwt.InvalidTokenError, URLError, ValueError) as exc:
        return None, str(exc) or 'Unauthorized'

    permitted_origins = CLERK_AUTHORIZED_PARTIES or [_get_request_origin()]
    azp = claims.get('azp')
    if azp and azp not in permitted_origins:
        return None, 'Unauthorized'

    if claims.get('sts') == 'pending':
        return None, 'Account setup is still pending in Clerk.'

    return claims, None


def clerk_auth_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        claims, error = _verify_clerk_session_token()
        if error:
            status = 500 if error.startswith('Clerk is not configured') else 401
            return jsonify({'error': error}), status
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
            return None
        g.auth_claims = claims

    clerk_user_id = claims.get('sub')
    if not clerk_user_id:
        return None
    conn = get_db()
    user = conn.execute('SELECT * FROM users WHERE clerk_user_id = ?', (clerk_user_id,)).fetchone()
    conn.close()
    g.current_user = dict(user) if user else None
    return g.current_user


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
        'SELECT 1 FROM users WHERE clerk_user_id != "" AND is_admin = 1 LIMIT 1'
    ).fetchone()
    any_synced = conn.execute(
        'SELECT 1 FROM users WHERE clerk_user_id != "" LIMIT 1'
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
    try:
        existing = conn.execute('SELECT * FROM users WHERE clerk_user_id = ?', (clerk_user_id,)).fetchone()
        if existing:
            conn.execute(
                'UPDATE users SET username = ?, email = ?, full_name = ? WHERE clerk_user_id = ?',
                (username, email, full_name, clerk_user_id)
            )
        else:
            is_admin = 1 if _should_auto_grant_admin(conn, email) else 0
            legacy = None
            if email:
                legacy = conn.execute(
                    'SELECT * FROM users WHERE LOWER(email) = ? AND (clerk_user_id IS NULL OR clerk_user_id = "")',
                    (email,)
                ).fetchone()
            if not legacy and username:
                legacy = conn.execute(
                    'SELECT * FROM users WHERE LOWER(username) = ? AND (clerk_user_id IS NULL OR clerk_user_id = "")',
                    (username.lower(),)
                ).fetchone()

            if legacy:
                platoons = legacy['platoons']
                should_be_admin = bool(legacy['is_admin']) or is_admin
                if should_be_admin and not platoons:
                    platoons = '*'
                conn.execute(
                    'UPDATE users SET username = ?, password_hash = ?, is_admin = ?, platoons = ?, '
                    'clerk_user_id = ?, email = ?, full_name = ?, pin_hash = "" WHERE id = ?',
                    (username, PLACEHOLDER_PASSWORD_HASH, 1 if should_be_admin else 0, platoons,
                     clerk_user_id, email, full_name, legacy['id'])
                )
            else:
                platoons = '*' if is_admin else ''
                conn.execute(
                    'INSERT INTO users (username, password_hash, is_admin, platoons, clerk_user_id, email, full_name) '
                    'VALUES (?, ?, ?, ?, ?, ?, ?)',
                    (username, PLACEHOLDER_PASSWORD_HASH, is_admin, platoons, clerk_user_id, email, full_name)
                )
        conn.commit()
        row = conn.execute('SELECT * FROM users WHERE clerk_user_id = ?', (clerk_user_id,)).fetchone()
        g.current_user = dict(row) if row else None
        return g.current_user, None
    except sqlite3.IntegrityError:
        return None, 'That username is already in use locally. Ask an admin to rename or merge the account.'
    finally:
        conn.close()


def has_platoon_access(user, platoon):
    if user['is_admin']:
        return True
    return platoon in [p.strip() for p in user['platoons'].split(',') if p.strip()]


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        user = get_current_user()
        if not user:
            return jsonify({'error': 'Unauthorized'}), 401
        g.current_user = user
        return f(*args, **kwargs)
    return decorated


def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        user = get_current_user()
        if not user:
            return jsonify({'error': 'Unauthorized'}), 401
        g.current_user = user
        if not user['is_admin']:
            return jsonify({'error': 'Forbidden'}), 403
        return f(*args, **kwargs)
    return decorated


# ── Auth routes ──

@app.route('/')
def index():
    return send_from_directory('.', 'index.html')


@app.route('/api/auth/config', methods=['GET'])
def auth_config():
    return jsonify({
        'enabled': CLERK_ENABLED,
        'publishable_key': CLERK_PUBLISHABLE_KEY,
        'frontend_api_url': CLERK_FRONTEND_API_URL,
    })


@app.route('/api/auth/sync', methods=['POST'])
@clerk_auth_required
def auth_sync():
    payload = request.get_json() or {}
    user, error = sync_clerk_user(payload)
    if error:
        return jsonify({'error': error}), 409
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
@admin_required
def get_users():
    conn = get_db()
    rows = conn.execute(
        'SELECT id, username, email, full_name, is_admin, platoons FROM users '
        'WHERE clerk_user_id != "" ORDER BY username'
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/users/<int:user_id>', methods=['PUT'])
@admin_required
def update_user(user_id):
    data = request.get_json()
    fields, values = [], []
    if 'is_admin' in data:
        fields.append('is_admin = ?')
        values.append(1 if data['is_admin'] else 0)
    if 'platoons' in data:
        fields.append('platoons = ?')
        values.append(data['platoons'])
    if 'username' in data:
        fields.append('username = ?')
        values.append((data['username'] or '').strip())
    if not fields:
        return jsonify({'error': 'Nothing to update'}), 400
    values.append(user_id)
    conn = get_db()
    try:
        conn.execute(f'UPDATE users SET {", ".join(fields)} WHERE id = ? AND clerk_user_id != ""', values)
        conn.commit()
        row = conn.execute(
            'SELECT id, username, email, full_name, is_admin, platoons FROM users WHERE id = ?',
            (user_id,)
        ).fetchone()
        return jsonify(dict(row))
    except sqlite3.IntegrityError:
        return jsonify({'error': 'Username already exists'}), 409
    finally:
        conn.close()


@app.route('/api/users/<int:user_id>', methods=['DELETE'])
@admin_required
def delete_user(user_id):
    if user_id == g.current_user['id']:
        return jsonify({'error': 'Cannot delete your own account'}), 400
    conn = get_db()
    conn.execute('DELETE FROM users WHERE id = ? AND clerk_user_id != ""', (user_id,))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


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
        row = conn.execute('SELECT value FROM settings WHERE key = ?', (f'unit_name_{key}',)).fetchone()
        count = conn.execute('SELECT COUNT(*) FROM personnel WHERE platoon = ?', (key,)).fetchone()[0]
        result[key] = {'name': row['value'] if row else default_name, 'count': count}
    conn.close()
    return jsonify(result)


@app.route('/api/personnel', methods=['GET'])
@login_required
def get_personnel():
    platoon = request.args.get('platoon', '2nd')
    user = get_current_user()
    if not has_platoon_access(user, platoon):
        return jsonify({'error': 'Forbidden'}), 403
    conn = get_db()
    rows = conn.execute(
        'SELECT * FROM personnel WHERE platoon = ? ORDER BY rank, last, first', (platoon,)
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


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
        'INSERT INTO personnel (rank, last, first, platoon) VALUES (?, ?, ?, ?)',
        (data.get('rank', ''), data.get('last', ''), data.get('first', ''), platoon)
    )
    new_id = cur.lastrowid
    conn.commit()
    row = conn.execute('SELECT * FROM personnel WHERE id = ?', (new_id,)).fetchone()
    conn.close()
    log_action('ADD_PERSON', f'{data.get("rank","")} {data.get("last","")}, {data.get("first","")}', platoon)
    return jsonify(dict(row)), 201


@app.route('/api/personnel/<int:person_id>', methods=['PUT'])
@login_required
def update_person(person_id):
    data = request.get_json()
    fields, values = [], []
    for col in ('rank', 'last', 'first', 'status', 'notes', 'from_date', 'to_date', 'present_date',
                'sched_status', 'sched_from', 'sched_to', 'sched_notes'):
        if col in data:
            fields.append(f'{col} = ?')
            values.append(data[col])
    if not fields:
        return jsonify({'error': 'No fields to update'}), 400
    values.append(person_id)
    conn = get_db()
    conn.execute(f'UPDATE personnel SET {", ".join(fields)} WHERE id = ?', values)
    conn.commit()
    row = conn.execute('SELECT * FROM personnel WHERE id = ?', (person_id,)).fetchone()
    conn.close()
    if row is None:
        return jsonify({'error': 'Not found'}), 404
    return jsonify(dict(row))


@app.route('/api/personnel/<int:person_id>', methods=['DELETE'])
@login_required
def delete_person(person_id):
    conn = get_db()
    row = conn.execute('SELECT rank, last, first, platoon FROM personnel WHERE id = ?', (person_id,)).fetchone()
    if row:
        log_action('DELETE_PERSON', f'{row["rank"]} {row["last"]}, {row["first"]}', row['platoon'])
    conn.execute('DELETE FROM personnel WHERE id = ?', (person_id,))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/settings', methods=['GET'])
@login_required
def get_settings():
    platoon = request.args.get('platoon', '2nd')
    key = f'unit_name_{platoon}'
    conn = get_db()
    row = conn.execute('SELECT value FROM settings WHERE key = ?', (key,)).fetchone()
    conn.close()
    return jsonify({'unit_name': row['value'] if row else PLATOONS.get(platoon, f'{platoon} Platoon')})


@app.route('/api/settings', methods=['PUT'])
@login_required
def update_settings():
    platoon = request.args.get('platoon', '2nd')
    user = get_current_user()
    if not has_platoon_access(user, platoon):
        return jsonify({'error': 'Forbidden'}), 403
    data = request.get_json()
    conn = get_db()
    if 'unit_name' in data:
        key = f'unit_name_{platoon}'
        conn.execute(
            'INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = ?',
            (key, data['unit_name'], data['unit_name'])
        )
    conn.commit()
    conn.close()
    return get_settings()


# ── Audit log ──

@app.route('/api/audit', methods=['GET'])
@admin_required
def get_audit():
    platoon = request.args.get('platoon', '')
    limit = min(int(request.args.get('limit', 200)), 500)
    conn = get_db()
    if platoon:
        rows = conn.execute(
            'SELECT * FROM audit_log WHERE platoon = ? ORDER BY id DESC LIMIT ?', (platoon, limit)
        ).fetchall()
    else:
        rows = conn.execute('SELECT * FROM audit_log ORDER BY id DESC LIMIT ?', (limit,)).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


# ── Duty roster ──

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
            'SELECT * FROM duty_roster WHERE platoon = ? AND date = ? ORDER BY duty_type, id',
            (platoon, date_filter)
        ).fetchall()
    else:
        rows = conn.execute(
            'SELECT * FROM duty_roster WHERE platoon = ? ORDER BY date DESC, duty_type, id LIMIT 90',
            (platoon,)
        ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/duty', methods=['POST'])
@login_required
def add_duty():
    data = request.get_json()
    platoon = data.get('platoon', '2nd')
    user = get_current_user()
    if not has_platoon_access(user, platoon):
        return jsonify({'error': 'Forbidden'}), 403
    conn = get_db()
    cur = conn.execute(
        'INSERT INTO duty_roster (date, platoon, duty_type, rank, last, first, notes) VALUES (?, ?, ?, ?, ?, ?, ?)',
        (data.get('date', ''), platoon, data.get('duty_type', 'CQ'),
         data.get('rank', ''), data.get('last', ''), data.get('first', ''), data.get('notes', ''))
    )
    new_id = cur.lastrowid
    conn.commit()
    row = conn.execute('SELECT * FROM duty_roster WHERE id = ?', (new_id,)).fetchone()
    conn.close()
    log_action('ADD_DUTY', f'{data.get("duty_type","CQ")} on {data.get("date","")}', platoon)
    return jsonify(dict(row)), 201


@app.route('/api/duty/<int:entry_id>', methods=['DELETE'])
@login_required
def delete_duty(entry_id):
    conn = get_db()
    row = conn.execute('SELECT * FROM duty_roster WHERE id = ?', (entry_id,)).fetchone()
    if row:
        log_action('DELETE_DUTY', f'{row["duty_type"]} on {row["date"]}', row['platoon'])
    conn.execute('DELETE FROM duty_roster WHERE id = ?', (entry_id,))
    conn.commit()
    conn.close()
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
        settings  = [dict(r) for r in conn.execute('SELECT * FROM settings').fetchall()]
        users     = [dict(r) for r in conn.execute(
            'SELECT id, username, email, full_name, is_admin, platoons, clerk_user_id FROM users'
        ).fetchall()]
        label = 'full'
    else:
        accessible = [p.strip() for p in (user['platoons'] or '').split(',') if p.strip()]
        placeholders = ','.join('?' * len(accessible))
        personnel = [dict(r) for r in conn.execute(
            f'SELECT * FROM personnel WHERE platoon IN ({placeholders})', accessible
        ).fetchall()]
        settings  = [dict(r) for r in conn.execute('SELECT * FROM settings').fetchall()]
        users     = []
        label = '-'.join(accessible)

    conn.close()
    payload = {
        'version': 1,
        'exported_at': datetime.utcnow().isoformat() + 'Z',
        'personnel': personnel,
        'settings': settings,
        'users': users,
    }
    log_action('BACKUP_EXPORT', f'Backup exported ({label})')
    return Response(
        json.dumps(payload, indent=2),
        mimetype='application/json',
        headers={'Content-Disposition': f'attachment; filename=platoon-backup-{label}-{date.today()}.json'}
    )


@app.route('/api/backup/restore', methods=['POST'])
@login_required
def import_backup():
    user = get_current_user()
    payload = request.get_json()
    if not payload or payload.get('version') != 1:
        return jsonify({'error': 'Invalid or unsupported backup file'}), 400

    conn = get_db()
    try:
        restored_personnel = 0

        if 'personnel' in payload:
            if user['is_admin']:
                conn.execute('DELETE FROM personnel')
                for p in payload['personnel']:
                    conn.execute(
                        'INSERT INTO personnel (id, rank, last, first, status, notes, from_date, to_date, present_date, platoon) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                        (p.get('id'), p.get('rank',''), p.get('last',''), p.get('first',''),
                         p.get('status','present'), p.get('notes',''),
                         p.get('from_date',''), p.get('to_date',''), p.get('present_date',''),
                         p.get('platoon','2nd'))
                    )
                    restored_personnel += 1
            else:
                accessible = [p.strip() for p in (user['platoons'] or '').split(',') if p.strip()]
                for p in payload['personnel']:
                    if p.get('platoon') not in accessible:
                        continue
                    conn.execute(
                        'DELETE FROM personnel WHERE platoon = ? AND last = ? AND first = ?',
                        (p['platoon'], p.get('last',''), p.get('first',''))
                    )
                    conn.execute(
                        'INSERT INTO personnel (rank, last, first, status, notes, from_date, to_date, present_date, platoon) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                        (p.get('rank',''), p.get('last',''), p.get('first',''),
                         p.get('status','present'), p.get('notes',''),
                         p.get('from_date',''), p.get('to_date',''), p.get('present_date',''),
                         p.get('platoon','2nd'))
                    )
                    restored_personnel += 1

        if 'settings' in payload:
            conn.execute('DELETE FROM settings')
            for s in payload['settings']:
                conn.execute('INSERT INTO settings (key, value) VALUES (?, ?)', (s['key'], s['value']))

        restored_users = 0
        if user['is_admin'] and 'users' in payload:
            current_uid = user['id']
            conn.execute('DELETE FROM users WHERE id != ?', (current_uid,))
            for u in payload['users']:
                if u['id'] == current_uid:
                    continue
                conn.execute(
                    'INSERT OR REPLACE INTO users (id, username, password_hash, is_admin, platoons, clerk_user_id, email, full_name, pin_hash) '
                    'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    (u['id'], u['username'], PLACEHOLDER_PASSWORD_HASH, u.get('is_admin', 0),
                     u.get('platoons', ''), u.get('clerk_user_id', ''), u.get('email', ''),
                     u.get('full_name', ''), '')
                )
                restored_users += 1

        conn.commit()
        log_action('BACKUP_RESTORE', f'Backup restored: {restored_personnel} personnel, {restored_users} users')
        return jsonify({'success': True, 'personnel': restored_personnel, 'users': restored_users})
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()


@app.route('/api/activate-scheduled', methods=['POST'])
@login_required
def activate_scheduled():
    today_str = date.today().isoformat()
    conn = get_db()
    activated = _activate_scheduled(conn, today_str)
    conn.commit()
    conn.close()
    return jsonify({'activated': activated})


# ── Reset route (used by auto-reset and manual reset) ──

@app.route('/api/reset', methods=['POST'])
@login_required
def reset_day():
    data = request.get_json() or {}
    platoon = data.get('platoon', '')
    user = get_current_user()
    if platoon and not has_platoon_access(user, platoon):
        return jsonify({'error': 'Forbidden'}), 403
    conn = get_db()
    if platoon:
        conn.execute(
            "UPDATE personnel SET present_date = '' WHERE status = 'present' AND platoon = ?",
            (platoon,)
        )
    else:
        conn.execute("UPDATE personnel SET present_date = '' WHERE status = 'present'")
    conn.commit()
    conn.close()
    log_action('RESET_DAY', f'Day reset for platoon: {platoon or "all"}', platoon)
    return jsonify({'success': True})


# ── Midnight auto-reset background thread ──

def _activate_scheduled(conn, today_str):
    """Promote sched_* entries whose start date has arrived."""
    rows = conn.execute(
        "SELECT id, sched_status, sched_from, sched_to, sched_notes FROM personnel "
        "WHERE sched_status != '' AND sched_from <= ?", (today_str,)
    ).fetchall()
    for r in rows:
        conn.execute(
            "UPDATE personnel SET status=?, from_date=?, to_date=?, notes=?, "
            "sched_status='', sched_from='', sched_to='', sched_notes='' WHERE id=?",
            (r['sched_status'], r['sched_from'], r['sched_to'], r['sched_notes'], r['id'])
        )
    return len(rows)


def _midnight_reset_worker():
    last_reset_date = None
    while True:
        now = datetime.now()
        today = now.date()
        today_str = today.isoformat()
        if now.hour == 0 and now.minute == 0 and today != last_reset_date:
            try:
                conn = get_db()
                conn.execute("UPDATE personnel SET present_date = '' WHERE status = 'present'")
                activated = _activate_scheduled(conn, today_str)
                conn.commit()
                conn.close()
                last_reset_date = today
                print(f'[auto-reset] Day reset at {now}; {activated} scheduled entries activated', flush=True)
            except Exception as e:
                print(f'[auto-reset] Error: {e}', flush=True)
        time.sleep(30)


if __name__ == '__main__':
    init_db()
    t = threading.Thread(target=_midnight_reset_worker, daemon=True)
    t.start()
    app.run(host='0.0.0.0', port=5000, debug=True)
