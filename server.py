import sqlite3
import os
import secrets
import string
import threading
import time
from datetime import datetime, date
from functools import wraps
from flask import Flask, request, jsonify, send_from_directory, session
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__, static_folder='.', static_url_path='')
app.secret_key = os.environ.get('SECRET_KEY', 'platoon-tracker-change-in-production')
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SECURE'] = False

_default_data_dir = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(os.environ.get('DATA_DIR', _default_data_dir), 'accountability.db')

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
            platoons      TEXT DEFAULT ''
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

    # Scheduled TDY/Leave columns
    for col, default in [('sched_status',''), ('sched_from',''), ('sched_to',''), ('sched_notes','')]:
        if col not in cols:
            cur.execute(f'ALTER TABLE personnel ADD COLUMN {col} TEXT DEFAULT ""')

    # ── Seed admin user ──
    cur.execute('SELECT COUNT(*) FROM users')
    if cur.fetchone()[0] == 0:
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
            uid = session.get('user_id')
            if uid:
                row = conn.execute('SELECT id, username FROM users WHERE id = ?', (uid,)).fetchone()
                if row:
                    user_id, username = row['id'], row['username']
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

def get_current_user():
    if 'user_id' not in session:
        return None
    conn = get_db()
    user = conn.execute('SELECT * FROM users WHERE id = ?', (session['user_id'],)).fetchone()
    conn.close()
    return dict(user) if user else None


def has_platoon_access(user, platoon):
    if user['is_admin']:
        return True
    return platoon in [p.strip() for p in user['platoons'].split(',') if p.strip()]


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            return jsonify({'error': 'Unauthorized'}), 401
        return f(*args, **kwargs)
    return decorated


def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            return jsonify({'error': 'Unauthorized'}), 401
        user = get_current_user()
        if not user or not user['is_admin']:
            return jsonify({'error': 'Forbidden'}), 403
        return f(*args, **kwargs)
    return decorated


# ── Auth routes ──

@app.route('/')
def index():
    return send_from_directory('.', 'index.html')


@app.route('/api/login', methods=['POST'])
def login():
    data = request.get_json()
    username = (data.get('username') or '').strip().lower()
    password = data.get('password') or ''
    conn = get_db()
    user = conn.execute('SELECT * FROM users WHERE LOWER(username) = ?', (username,)).fetchone()
    conn.close()
    if not user or not check_password_hash(user['password_hash'], password):
        return jsonify({'error': 'Invalid username or password'}), 401
    session['user_id'] = user['id']
    session['_last_active'] = time.time()
    log_action('LOGIN', f'User {username} logged in')
    return jsonify({
        'id': user['id'], 'username': user['username'],
        'is_admin': bool(user['is_admin']), 'platoons': user['platoons']
    })


@app.route('/api/logout', methods=['POST'])
def logout():
    session.clear()
    return jsonify({'success': True})


@app.route('/api/me', methods=['GET'])
def me():
    user = get_current_user()
    if not user:
        return jsonify({'error': 'Unauthorized'}), 401
    return jsonify({
        'id': user['id'], 'username': user['username'],
        'is_admin': bool(user['is_admin']), 'platoons': user['platoons']
    })


# ── User management (admin only) ──

@app.route('/api/me/password', methods=['PUT'])
@login_required
def change_own_password():
    data = request.get_json()
    current = data.get('current_password') or ''
    new_pw  = data.get('new_password') or ''
    if not current or not new_pw:
        return jsonify({'error': 'Current and new password are required'}), 400
    if len(new_pw) < 6:
        return jsonify({'error': 'New password must be at least 6 characters'}), 400
    user = get_current_user()
    if not check_password_hash(user['password_hash'], current):
        return jsonify({'error': 'Current password is incorrect'}), 401
    conn = get_db()
    conn.execute('UPDATE users SET password_hash = ? WHERE id = ?',
                 (generate_password_hash(new_pw), user['id']))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/users', methods=['GET'])
@admin_required
def get_users():
    conn = get_db()
    rows = conn.execute('SELECT id, username, is_admin, platoons FROM users ORDER BY username').fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/users', methods=['POST'])
@admin_required
def create_user():
    data = request.get_json()
    username = (data.get('username') or '').strip().lower()
    password = data.get('password') or ''
    if not username or not password:
        return jsonify({'error': 'Username and password are required'}), 400
    is_admin = 1 if data.get('is_admin') else 0
    platoons = data.get('platoons') or ''
    try:
        conn = get_db()
        cur = conn.execute(
            'INSERT INTO users (username, password_hash, is_admin, platoons) VALUES (?, ?, ?, ?)',
            (username, generate_password_hash(password), is_admin, platoons)
        )
        new_id = cur.lastrowid
        conn.commit()
        row = conn.execute('SELECT id, username, is_admin, platoons FROM users WHERE id = ?', (new_id,)).fetchone()
        conn.close()
        return jsonify(dict(row)), 201
    except Exception:
        return jsonify({'error': 'Username already exists'}), 409


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
    if data.get('password'):
        fields.append('password_hash = ?')
        values.append(generate_password_hash(data['password']))
    if not fields:
        return jsonify({'error': 'Nothing to update'}), 400
    values.append(user_id)
    conn = get_db()
    conn.execute(f'UPDATE users SET {", ".join(fields)} WHERE id = ?', values)
    conn.commit()
    row = conn.execute('SELECT id, username, is_admin, platoons FROM users WHERE id = ?', (user_id,)).fetchone()
    conn.close()
    return jsonify(dict(row))


@app.route('/api/users/<int:user_id>', methods=['DELETE'])
@admin_required
def delete_user(user_id):
    if user_id == session.get('user_id'):
        return jsonify({'error': 'Cannot delete your own account'}), 400
    conn = get_db()
    conn.execute('DELETE FROM users WHERE id = ?', (user_id,))
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
            'SELECT id, username, is_admin, platoons, pin_hash FROM users'
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
            current_uid = session.get('user_id')
            conn.execute('DELETE FROM users WHERE id != ?', (current_uid,))
            for u in payload['users']:
                if u['id'] == current_uid:
                    continue
                conn.execute(
                    'INSERT OR REPLACE INTO users (id, username, password_hash, is_admin, platoons, pin_hash) VALUES (?, ?, ?, ?, ?, ?)',
                    (u['id'], u['username'], u.get('password_hash',''), u.get('is_admin',0),
                     u.get('platoons',''), u.get('pin_hash',''))
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


# ── Session timeout ──
SESSION_TIMEOUT_MINUTES = 30

@app.before_request
def check_session_timeout():
    if 'user_id' in session:
        last = session.get('_last_active')
        now = time.time()
        if last and (now - last) > SESSION_TIMEOUT_MINUTES * 60:
            session.clear()
            return jsonify({'error': 'Session expired'}), 401
        session['_last_active'] = now


# ── PIN login ──

@app.route('/api/pin-login', methods=['POST'])
def pin_login():
    data = request.get_json()
    username = (data.get('username') or '').strip().lower()
    pin = (data.get('pin') or '').strip()
    if not username or not pin or len(pin) != 4 or not pin.isdigit():
        return jsonify({'error': 'Invalid PIN format'}), 400
    conn = get_db()
    user = conn.execute('SELECT * FROM users WHERE LOWER(username) = ?', (username,)).fetchone()
    conn.close()
    if not user or not user['pin_hash'] or not check_password_hash(user['pin_hash'], pin):
        return jsonify({'error': 'Invalid username or PIN'}), 401
    session['user_id'] = user['id']
    session['_last_active'] = time.time()
    return jsonify({
        'id': user['id'], 'username': user['username'],
        'is_admin': bool(user['is_admin']), 'platoons': user['platoons']
    })


@app.route('/api/me/pin', methods=['PUT'])
@login_required
def set_pin():
    data = request.get_json()
    pin = (data.get('pin') or '').strip()
    if pin and (len(pin) != 4 or not pin.isdigit()):
        return jsonify({'error': 'PIN must be exactly 4 digits'}), 400
    conn = get_db()
    new_hash = generate_password_hash(pin) if pin else ''
    conn.execute('UPDATE users SET pin_hash = ? WHERE id = ?', (new_hash, session['user_id']))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


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
