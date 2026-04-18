import sqlite3
import os
import secrets
import string
import threading
import time
import base64
import re
import zipfile
import xml.etree.ElementTree as ET
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

    cur.execute('''
        CREATE TABLE IF NOT EXISTS scheduled_events (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id  INTEGER NOT NULL,
            platoon    TEXT NOT NULL,
            status     TEXT NOT NULL,
            from_date  TEXT DEFAULT '',
            to_date    TEXT DEFAULT '',
            notes      TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY(person_id) REFERENCES personnel(id) ON DELETE CASCADE
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS training_imports (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            filename          TEXT DEFAULT '',
            uploaded_by       TEXT DEFAULT '',
            uploaded_at       TEXT DEFAULT (datetime('now')),
            personnel_count   INTEGER DEFAULT 0,
            requirement_count INTEGER DEFAULT 0
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS training_requirements (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            import_id       INTEGER NOT NULL,
            key             TEXT NOT NULL,
            display_name    TEXT NOT NULL,
            required_by     TEXT DEFAULT '',
            interval_months INTEGER,
            delivery_method TEXT DEFAULT '',
            source_column   TEXT DEFAULT '',
            FOREIGN KEY(import_id) REFERENCES training_imports(id) ON DELETE CASCADE
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS training_records (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            import_id        INTEGER NOT NULL,
            platoon          TEXT DEFAULT '',
            rank             TEXT DEFAULT '',
            last             TEXT DEFAULT '',
            first            TEXT DEFAULT '',
            full_name        TEXT DEFAULT '',
            requirement_key  TEXT NOT NULL,
            requirement_name TEXT NOT NULL,
            raw_value        TEXT DEFAULT '',
            completed_on     TEXT DEFAULT '',
            due_on           TEXT DEFAULT '',
            status           TEXT DEFAULT '',
            FOREIGN KEY(import_id) REFERENCES training_imports(id) ON DELETE CASCADE
        )
    ''')

    cur.execute('CREATE INDEX IF NOT EXISTS idx_training_records_import_platoon ON training_records(import_id, platoon)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_training_records_status ON training_records(import_id, status)')

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

    scols = [row[1] for row in cur.execute('PRAGMA table_info(scheduled_events)').fetchall()]
    if scols:
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


# ── 350-1 Training tracker helpers ──

XLSX_NS = {
    'main': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main',
    'rel': 'http://schemas.openxmlformats.org/officeDocument/2006/relationships',
}

TRAINING_HEADER_ROW = 4
TRAINING_FIRST_DATA_ROW = 5
TRAINING_FIRST_REQUIREMENT_COL = 5  # E; A-D are PLT, rank, name, and assignment metadata.
TRAINING_DUE_SOON_DAYS = 30

TRAINING_LINK_ALIASES = {
    'atl1': 'atlevel1',
    'opsecatis': 'armyopsec',
    'tarpatis': 'tarp',
    'informationsecurityinfosec': 'informationsecurity',
    'managingclearanceandaccesscplandabove': 'managingpersonnelwithclearancesandaccesstoclassifiedinformation',
    'unauthorizeddisc': 'unauthorizeddisclosure',
}


def _normalize_training_key(value):
    return re.sub(r'[^a-z0-9]+', '', str(value or '').lower())


def _excel_column_number(cell_ref):
    match = re.match(r'([A-Z]+)', cell_ref or '')
    if not match:
        return 0
    number = 0
    for char in match.group(1):
        number = number * 26 + ord(char) - 64
    return number


def _excel_column_name(number):
    name = ''
    while number:
        number, remainder = divmod(number - 1, 26)
        name = chr(65 + remainder) + name
    return name


def _cell_value(cell, shared_strings):
    inline = cell.find('.//main:t', XLSX_NS)
    if cell.attrib.get('t') == 'inlineStr' and inline is not None:
        return inline.text or ''

    value = cell.find('main:v', XLSX_NS)
    if value is None:
        return None

    text = value.text or ''
    if cell.attrib.get('t') == 's':
        try:
            return shared_strings[int(text)]
        except (ValueError, IndexError):
            return text
    return text


def _xlsx_row_values(sheet_root, shared_strings, row_number, max_cols=80):
    row = sheet_root.find(f".//main:row[@r='{row_number}']", XLSX_NS)
    values = [None] * max_cols
    if row is None:
        return values
    for cell in row.findall('main:c', XLSX_NS):
        col_index = _excel_column_number(cell.attrib.get('r', '')) - 1
        if 0 <= col_index < max_cols:
            values[col_index] = _cell_value(cell, shared_strings)
    return values


def _xlsx_all_rows(sheet_root, shared_strings, max_cols=80):
    rows = []
    for row in sheet_root.findall('main:sheetData/main:row', XLSX_NS):
        row_number = int(row.attrib.get('r', len(rows) + 1))
        values = [None] * max_cols
        for cell in row.findall('main:c', XLSX_NS):
            col_index = _excel_column_number(cell.attrib.get('r', '')) - 1
            if 0 <= col_index < max_cols:
                values[col_index] = _cell_value(cell, shared_strings)
        rows.append((row_number, values))
    return rows


def _xlsx_sheets(file_obj):
    try:
        archive = zipfile.ZipFile(file_obj)
    except zipfile.BadZipFile as exc:
        raise ValueError('Upload must be a valid .xlsx workbook.') from exc

    try:
        names = set(archive.namelist())
        shared_strings = []
        if 'xl/sharedStrings.xml' in names:
            shared_root = ET.fromstring(archive.read('xl/sharedStrings.xml'))
            for item in shared_root.findall('main:si', XLSX_NS):
                text = ''.join(t.text or '' for t in item.findall('.//main:t', XLSX_NS))
                shared_strings.append(text)

        workbook = ET.fromstring(archive.read('xl/workbook.xml'))
        rels = ET.fromstring(archive.read('xl/_rels/workbook.xml.rels'))
        rel_map = {rel.attrib['Id']: rel.attrib['Target'] for rel in rels}
        sheets = {}
        for sheet in workbook.findall('main:sheets/main:sheet', XLSX_NS):
            title = sheet.attrib.get('name', '')
            rel_id = sheet.attrib.get('{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id')
            target = rel_map.get(rel_id, '')
            path = target.lstrip('/')
            if not path.startswith('xl/'):
                path = f'xl/{path}'
            if path not in names:
                continue
            sheets[title] = ET.fromstring(archive.read(path))
        return sheets, shared_strings
    finally:
        archive.close()


def _parse_interval_months(value):
    text = str(value or '').strip()
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _parse_links_sheet(sheet_root, shared_strings):
    rows = _xlsx_all_rows(sheet_root, shared_strings, max_cols=4)
    links = []
    for row_number, values in rows:
        if row_number == 1:
            continue
        topic = str(values[0] or '').strip()
        if not topic:
            continue
        links.append({
            'topic': topic,
            'key': _normalize_training_key(topic),
            'required_by': str(values[1] or '').strip(),
            'interval_months': _parse_interval_months(values[2]),
            'delivery_method': str(values[3] or '').strip(),
        })
    return links


def _match_training_link(header, links):
    header_key = _normalize_training_key(header)
    lookup_key = TRAINING_LINK_ALIASES.get(header_key, header_key)
    best = None
    best_score = 0
    for link in links:
        link_key = link['key']
        if not link_key:
            continue
        score = 0
        if lookup_key == link_key:
            score = 100
        elif lookup_key in link_key or link_key in lookup_key:
            score = min(len(lookup_key), len(link_key))
        if score > best_score:
            best = link
            best_score = score
    return best if best_score >= 4 else None


def _add_months(source, months):
    if months is None:
        return ''
    month = source.month - 1 + months
    year = source.year + month // 12
    month = month % 12 + 1
    days = [31, 29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28,
            31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    day = min(source.day, days[month - 1])
    return date(year, month, day)


def _excel_serial_to_date(value):
    try:
        number = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    if number <= 0:
        return None
    return date(1899, 12, 30) + timedelta(days=int(number))


def _normalize_tracker_platoon(value):
    text = str(value or '').strip().lower()
    if text.startswith('1st'):
        return '1st'
    if text.startswith('2nd'):
        return '2nd'
    if text in ('hq', 'hqs', 'headquarters') or text.startswith('hq '):
        return 'hq'
    return text.replace(' plt', '').strip()


def _split_tracker_name(value):
    text = ' '.join(str(value or '').replace('\n', ' ').split())
    if ',' not in text:
        return text, ''
    last, first = text.split(',', 1)
    return last.strip(), first.strip()


def _training_record_status(raw_value, interval_months):
    text = str(raw_value or '').strip()
    if not text:
        return 'missing', '', ''
    if text.upper() in ('N/A', 'NA', 'EXEMPT'):
        return 'exempt', '', ''

    completed = _excel_serial_to_date(text)
    if completed is None:
        return 'unknown', '', ''

    due = _add_months(completed, interval_months if interval_months is not None else 12)
    today = date.today()
    if due < today:
        status = 'expired'
    elif due <= today + timedelta(days=TRAINING_DUE_SOON_DAYS):
        status = 'due_soon'
    else:
        status = 'current'
    return status, completed.isoformat(), due.isoformat()


def parse_training_tracker(file_obj):
    sheets, shared_strings = _xlsx_sheets(file_obj)
    tier_sheet = sheets.get('Tier 1')
    if tier_sheet is None:
        raise ValueError('Training tracker must include a "Tier 1" sheet.')

    header_values = _xlsx_row_values(tier_sheet, shared_strings, TRAINING_HEADER_ROW)
    expected_headers = [_normalize_training_key(header_values[i]) for i in range(3)]
    if expected_headers != ['plt', 'rank', 'lastnamefirstnamemi']:
        raise ValueError('Tier 1 sheet does not match the expected 350-1 tracker template.')

    links = _parse_links_sheet(sheets['Links'], shared_strings) if 'Links' in sheets else []
    requirements = []
    for col_number in range(TRAINING_FIRST_REQUIREMENT_COL, len(header_values) + 1):
        header = str(header_values[col_number - 1] or '').strip()
        if not header:
            continue
        matched = _match_training_link(header, links)
        requirements.append({
            'key': _normalize_training_key(header) or f'col{col_number}',
            'display_name': header,
            'required_by': matched['required_by'] if matched else '',
            'interval_months': matched['interval_months'] if matched and matched['interval_months'] else 12,
            'delivery_method': matched['delivery_method'] if matched else '',
            'source_column': _excel_column_name(col_number),
        })

    if not requirements:
        raise ValueError('No training requirement columns were found in the Tier 1 sheet.')

    rows = _xlsx_all_rows(tier_sheet, shared_strings)
    records = []
    personnel_seen = set()
    for row_number, values in rows:
        if row_number < TRAINING_FIRST_DATA_ROW:
            continue
        platoon = _normalize_tracker_platoon(values[0])
        rank = str(values[1] or '').strip()
        full_name = str(values[2] or '').strip()
        if not platoon or not rank or not full_name:
            continue
        last, first = _split_tracker_name(full_name)
        personnel_seen.add((platoon, rank, last, first))
        for requirement in requirements:
            col_index = ord(requirement['source_column'][0]) - ord('A')
            if len(requirement['source_column']) > 1:
                col_index = _excel_column_number(requirement['source_column']) - 1
            raw_value = values[col_index] if col_index < len(values) else None
            status, completed_on, due_on = _training_record_status(raw_value, requirement['interval_months'])
            records.append({
                'platoon': platoon,
                'rank': rank,
                'last': last,
                'first': first,
                'full_name': full_name,
                'requirement_key': requirement['key'],
                'requirement_name': requirement['display_name'],
                'raw_value': str(raw_value or '').strip(),
                'completed_on': completed_on,
                'due_on': due_on,
                'status': status,
            })

    if not records:
        raise ValueError('No personnel rows were found in the Tier 1 sheet.')

    return {
        'requirements': requirements,
        'records': records,
        'personnel_count': len(personnel_seen),
        'requirement_count': len(requirements),
    }


def _latest_training_import_id(conn):
    row = conn.execute('SELECT id FROM training_imports ORDER BY id DESC LIMIT 1').fetchone()
    return row['id'] if row else None


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
        username_conflict = conn.execute(
            'SELECT * FROM users WHERE LOWER(username) = ?',
            (username.lower(),)
        ).fetchone() if username else None
        email_conflict = conn.execute(
            'SELECT * FROM users WHERE LOWER(email) = ?',
            (email,)
        ).fetchone() if email else None

        if existing:
            conn.execute(
                'UPDATE users SET username = ?, email = ?, full_name = ? WHERE clerk_user_id = ?',
                (username, email, full_name, clerk_user_id)
            )
        else:
            is_admin = 1 if _should_auto_grant_admin(conn, email) else 0
            legacy = None

            for candidate in (email_conflict, username_conflict):
                if candidate and not candidate['clerk_user_id']:
                    legacy = candidate
                    break

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
                if username_conflict and username_conflict['clerk_user_id'] and username_conflict['clerk_user_id'] != clerk_user_id:
                    username = email or f'user-{clerk_user_id[:8]}'
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
    scheduled_rows = conn.execute(
        'SELECT * FROM scheduled_events WHERE platoon = ? ORDER BY from_date, to_date, id', (platoon,)
    ).fetchall()
    conn.close()
    scheduled_by_person = {}
    for r in scheduled_rows:
        scheduled_by_person.setdefault(r['person_id'], []).append(dict(r))
    result = []
    for r in rows:
        item = dict(r)
        events = scheduled_by_person.get(r['id'], [])
        item['scheduled_events'] = events
        if events:
            item['sched_status'] = events[0]['status']
            item['sched_from'] = events[0]['from_date']
            item['sched_to'] = events[0]['to_date']
            item['sched_notes'] = events[0]['notes']
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


@app.route('/api/personnel/<int:person_id>/schedule', methods=['POST'])
@login_required
def add_scheduled_event(person_id):
    data = request.get_json()
    conn = get_db()
    person = conn.execute('SELECT id, rank, last, first, platoon FROM personnel WHERE id = ?', (person_id,)).fetchone()
    if person is None:
        conn.close()
        return jsonify({'error': 'Not found'}), 404
    user = get_current_user()
    if not has_platoon_access(user, person['platoon']):
        conn.close()
        return jsonify({'error': 'Forbidden'}), 403

    status = data.get('status', '').strip()
    if status not in ('tdy', 'leave', 'pass', 'other', 'ftr'):
        conn.close()
        return jsonify({'error': 'Invalid scheduled status'}), 400

    cur = conn.execute(
        'INSERT INTO scheduled_events (person_id, platoon, status, from_date, to_date, notes) VALUES (?, ?, ?, ?, ?, ?)',
        (person_id, person['platoon'], status, data.get('from_date', ''), data.get('to_date', ''), data.get('notes', ''))
    )
    new_id = cur.lastrowid
    first = conn.execute(
        'SELECT * FROM scheduled_events WHERE person_id = ? ORDER BY from_date, to_date, id LIMIT 1', (person_id,)
    ).fetchone()
    conn.execute(
        'UPDATE personnel SET sched_status = ?, sched_from = ?, sched_to = ?, sched_notes = ? WHERE id = ?',
        (first['status'], first['from_date'], first['to_date'], first['notes'], person_id)
    )
    conn.commit()
    row = conn.execute('SELECT * FROM scheduled_events WHERE id = ?', (new_id,)).fetchone()
    conn.close()
    log_action('SCHEDULE_STATUS', f'{person["rank"]} {person["last"]}: {status} on {data.get("from_date", "")}', person['platoon'])
    return jsonify(dict(row)), 201


@app.route('/api/schedules/<int:event_id>', methods=['DELETE'])
@login_required
def delete_scheduled_event(event_id):
    conn = get_db()
    row = conn.execute('SELECT * FROM scheduled_events WHERE id = ?', (event_id,)).fetchone()
    if row is None:
        conn.close()
        return jsonify({'error': 'Not found'}), 404
    user = get_current_user()
    if not has_platoon_access(user, row['platoon']):
        conn.close()
        return jsonify({'error': 'Forbidden'}), 403
    person_id = row['person_id']
    conn.execute('DELETE FROM scheduled_events WHERE id = ?', (event_id,))
    first = conn.execute(
        'SELECT * FROM scheduled_events WHERE person_id = ? ORDER BY from_date, to_date, id LIMIT 1', (person_id,)
    ).fetchone()
    if first:
        conn.execute(
            'UPDATE personnel SET sched_status = ?, sched_from = ?, sched_to = ?, sched_notes = ? WHERE id = ?',
            (first['status'], first['from_date'], first['to_date'], first['notes'], person_id)
        )
    else:
        conn.execute(
            "UPDATE personnel SET sched_status = '', sched_from = '', sched_to = '', sched_notes = '' WHERE id = ?",
            (person_id,)
        )
    conn.commit()
    conn.close()
    log_action('DELETE_SCHEDULE', f'{row["status"]} on {row["from_date"]}', row['platoon'])
    return jsonify({'success': True})


@app.route('/api/personnel/<int:person_id>', methods=['DELETE'])
@login_required
def delete_person(person_id):
    conn = get_db()
    row = conn.execute('SELECT rank, last, first, platoon FROM personnel WHERE id = ?', (person_id,)).fetchone()
    if row:
        log_action('DELETE_PERSON', f'{row["rank"]} {row["last"]}, {row["first"]}', row['platoon'])
    conn.execute('DELETE FROM scheduled_events WHERE person_id = ?', (person_id,))
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
    user = get_current_user()
    if not row:
        conn.close()
        return jsonify({'error': 'Not found'}), 404
    if not has_platoon_access(user, row['platoon']):
        conn.close()
        return jsonify({'error': 'Forbidden'}), 403
    if row:
        log_action('DELETE_DUTY', f'{row["duty_type"]} on {row["date"]}', row['platoon'])
    conn.execute('DELETE FROM duty_roster WHERE id = ?', (entry_id,))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


# ── 350-1 Training tracker ──

@app.route('/api/training/latest', methods=['GET'])
@login_required
def get_training_latest():
    user = get_current_user()
    platoon = request.args.get('platoon', '').strip()
    if platoon and not has_platoon_access(user, platoon):
        return jsonify({'error': 'Forbidden'}), 403

    conn = get_db()
    import_id = _latest_training_import_id(conn)
    if not import_id:
        conn.close()
        return jsonify({'import': None, 'requirements': [], 'records': [], 'summary': {}})

    import_row = conn.execute('SELECT * FROM training_imports WHERE id = ?', (import_id,)).fetchone()
    requirements = [dict(r) for r in conn.execute(
        'SELECT key, display_name, required_by, interval_months, delivery_method, source_column '
        'FROM training_requirements WHERE import_id = ? ORDER BY id',
        (import_id,)
    ).fetchall()]

    params = [import_id]
    where = ['import_id = ?']
    if platoon:
        where.append('platoon = ?')
        params.append(platoon)
    elif not user['is_admin']:
        accessible = [p.strip() for p in (user['platoons'] or '').split(',') if p.strip()]
        if '*' not in accessible:
            if not accessible:
                conn.close()
                return jsonify({'error': 'Forbidden'}), 403
            where.append(f"platoon IN ({','.join('?' * len(accessible))})")
            params.extend(accessible)

    records = [dict(r) for r in conn.execute(
        f'SELECT * FROM training_records WHERE {" AND ".join(where)} ORDER BY platoon, last, first, requirement_name',
        params
    ).fetchall()]
    conn.close()

    counts = {}
    for record in records:
        counts[record['status']] = counts.get(record['status'], 0) + 1
    required_total = len([r for r in records if r['status'] != 'exempt'])
    valid_total = counts.get('current', 0) + counts.get('due_soon', 0)
    personnel = {(r['platoon'], r['rank'], r['last'], r['first']) for r in records}
    summary = {
        'personnel_count': len(personnel),
        'requirement_count': len(requirements),
        'record_count': len(records),
        'required_record_count': required_total,
        'current': counts.get('current', 0),
        'due_soon': counts.get('due_soon', 0),
        'expired': counts.get('expired', 0),
        'missing': counts.get('missing', 0),
        'exempt': counts.get('exempt', 0),
        'unknown': counts.get('unknown', 0),
        'completion_percent': round((valid_total / required_total) * 100, 1) if required_total else 0,
    }
    return jsonify({
        'import': dict(import_row),
        'requirements': requirements,
        'records': records,
        'summary': summary,
    })


@app.route('/api/training/upload', methods=['POST'])
@admin_required
def upload_training_tracker():
    uploaded = request.files.get('tracker')
    if not uploaded or not uploaded.filename:
        return jsonify({'error': 'Select a 350-1 tracker .xlsx file.'}), 400
    if not uploaded.filename.lower().endswith('.xlsx'):
        return jsonify({'error': 'Training tracker upload must be an .xlsx file.'}), 400

    try:
        parsed = parse_training_tracker(uploaded.stream)
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400
    except ET.ParseError:
        return jsonify({'error': 'Training tracker XML could not be read.'}), 400

    user = get_current_user()
    conn = get_db()
    try:
        cur = conn.execute(
            'INSERT INTO training_imports (filename, uploaded_by, personnel_count, requirement_count) '
            'VALUES (?, ?, ?, ?)',
            (os.path.basename(uploaded.filename), user['username'], parsed['personnel_count'], parsed['requirement_count'])
        )
        import_id = cur.lastrowid
        for requirement in parsed['requirements']:
            conn.execute(
                'INSERT INTO training_requirements '
                '(import_id, key, display_name, required_by, interval_months, delivery_method, source_column) '
                'VALUES (?, ?, ?, ?, ?, ?, ?)',
                (import_id, requirement['key'], requirement['display_name'], requirement['required_by'],
                 requirement['interval_months'], requirement['delivery_method'], requirement['source_column'])
            )
        for record in parsed['records']:
            conn.execute(
                'INSERT INTO training_records '
                '(import_id, platoon, rank, last, first, full_name, requirement_key, requirement_name, '
                'raw_value, completed_on, due_on, status) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (import_id, record['platoon'], record['rank'], record['last'], record['first'], record['full_name'],
                 record['requirement_key'], record['requirement_name'], record['raw_value'],
                 record['completed_on'], record['due_on'], record['status'])
            )
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise
    conn.close()

    log_action(
        'TRAINING_IMPORT',
        f'{uploaded.filename}: {parsed["personnel_count"]} personnel, {parsed["requirement_count"]} requirements'
    )
    return get_training_latest()


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
        training_imports = [dict(r) for r in conn.execute('SELECT * FROM training_imports').fetchall()]
        training_requirements = [dict(r) for r in conn.execute('SELECT * FROM training_requirements').fetchall()]
        training_records = [dict(r) for r in conn.execute('SELECT * FROM training_records').fetchall()]
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
        scheduled_events = [dict(r) for r in conn.execute(
            f'SELECT * FROM scheduled_events WHERE platoon IN ({placeholders})', accessible
        ).fetchall()]
        training_imports = []
        training_requirements = []
        training_records = []
        settings  = [dict(r) for r in conn.execute('SELECT * FROM settings').fetchall()]
        users     = []
        label = '-'.join(accessible)

    conn.close()
    payload = {
        'version': 1,
        'exported_at': datetime.utcnow().isoformat() + 'Z',
        'personnel': personnel,
        'scheduled_events': scheduled_events,
        'training_imports': training_imports,
        'training_requirements': training_requirements,
        'training_records': training_records,
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
                conn.execute('DELETE FROM scheduled_events')
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
                        'DELETE FROM scheduled_events WHERE platoon = ? AND person_id IN ('
                        'SELECT id FROM personnel WHERE platoon = ? AND last = ? AND first = ?'
                        ')',
                        (p['platoon'], p['platoon'], p.get('last',''), p.get('first',''))
                    )
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

        if 'scheduled_events' in payload:
            accessible = ['*'] if user['is_admin'] else [p.strip() for p in (user['platoons'] or '').split(',') if p.strip()]
            for s in payload['scheduled_events']:
                if not user['is_admin'] and s.get('platoon') not in accessible:
                    continue
                conn.execute(
                    'INSERT OR REPLACE INTO scheduled_events (id, person_id, platoon, status, from_date, to_date, notes, created_at) '
                    'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                    (s.get('id') if user['is_admin'] else None, s.get('person_id'), s.get('platoon', '2nd'),
                     s.get('status', ''), s.get('from_date', ''), s.get('to_date', ''),
                     s.get('notes', ''), s.get('created_at', datetime.utcnow().isoformat()))
                )

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

        restored_training = 0
        if user['is_admin'] and any(k in payload for k in ('training_imports', 'training_requirements', 'training_records')):
            conn.execute('DELETE FROM training_records')
            conn.execute('DELETE FROM training_requirements')
            conn.execute('DELETE FROM training_imports')
            for item in payload.get('training_imports', []):
                conn.execute(
                    'INSERT OR REPLACE INTO training_imports '
                    '(id, filename, uploaded_by, uploaded_at, personnel_count, requirement_count) '
                    'VALUES (?, ?, ?, ?, ?, ?)',
                    (item.get('id'), item.get('filename', ''), item.get('uploaded_by', ''),
                     item.get('uploaded_at', datetime.utcnow().isoformat()), item.get('personnel_count', 0),
                     item.get('requirement_count', 0))
                )
            for item in payload.get('training_requirements', []):
                conn.execute(
                    'INSERT OR REPLACE INTO training_requirements '
                    '(id, import_id, key, display_name, required_by, interval_months, delivery_method, source_column) '
                    'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                    (item.get('id'), item.get('import_id'), item.get('key', ''), item.get('display_name', ''),
                     item.get('required_by', ''), item.get('interval_months'), item.get('delivery_method', ''),
                     item.get('source_column', ''))
                )
            for item in payload.get('training_records', []):
                conn.execute(
                    'INSERT OR REPLACE INTO training_records '
                    '(id, import_id, platoon, rank, last, first, full_name, requirement_key, requirement_name, '
                    'raw_value, completed_on, due_on, status) '
                    'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    (item.get('id'), item.get('import_id'), item.get('platoon', ''), item.get('rank', ''),
                     item.get('last', ''), item.get('first', ''), item.get('full_name', ''),
                     item.get('requirement_key', ''), item.get('requirement_name', ''), item.get('raw_value', ''),
                     item.get('completed_on', ''), item.get('due_on', ''), item.get('status', ''))
                )
                restored_training += 1

        conn.commit()
        log_action('BACKUP_RESTORE', f'Backup restored: {restored_personnel} personnel, {restored_users} users')
        return jsonify({'success': True, 'personnel': restored_personnel, 'users': restored_users, 'training': restored_training})
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
    """Promote scheduled entries whose start date has arrived."""
    rows = conn.execute(
        "SELECT * FROM scheduled_events WHERE from_date != '' AND from_date <= ? ORDER BY from_date, id",
        (today_str,)
    ).fetchall()
    for r in rows:
        conn.execute(
            "UPDATE personnel SET status=?, from_date=?, to_date=?, notes=?, "
            "sched_status='', sched_from='', sched_to='', sched_notes='' WHERE id=?",
            (r['status'], r['from_date'], r['to_date'], r['notes'], r['person_id'])
        )
        conn.execute('DELETE FROM scheduled_events WHERE id = ?', (r['id'],))
        first = conn.execute(
            'SELECT * FROM scheduled_events WHERE person_id = ? ORDER BY from_date, to_date, id LIMIT 1',
            (r['person_id'],)
        ).fetchone()
        if first:
            conn.execute(
                'UPDATE personnel SET sched_status = ?, sched_from = ?, sched_to = ?, sched_notes = ? WHERE id = ?',
                (first['status'], first['from_date'], first['to_date'], first['notes'], r['person_id'])
            )

    legacy_rows = conn.execute(
        "SELECT id, sched_status, sched_from, sched_to, sched_notes FROM personnel "
        "WHERE sched_status != '' AND sched_from <= ? AND NOT EXISTS ("
        "  SELECT 1 FROM scheduled_events s WHERE s.person_id = personnel.id"
        ")",
        (today_str,)
    ).fetchall()
    for r in legacy_rows:
        conn.execute(
            "UPDATE personnel SET status=?, from_date=?, to_date=?, notes=?, "
            "sched_status='', sched_from='', sched_to='', sched_notes='' WHERE id=?",
            (r['sched_status'], r['sched_from'], r['sched_to'], r['sched_notes'], r['id'])
        )
    return len(rows) + len(legacy_rows)


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
