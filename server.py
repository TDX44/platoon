import sqlite3
import os
from flask import Flask, request, jsonify, send_from_directory

app = Flask(__name__, static_folder='.', static_url_path='')
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'accountability.db')

PLATOONS = {'1st': '1st Platoon Accountability', '2nd': '2nd Platoon Accountability', 'hq': 'HQ Platoon Accountability'}


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    cur = conn.cursor()

    cur.execute('''
        CREATE TABLE IF NOT EXISTS personnel (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rank TEXT,
            last TEXT,
            first TEXT,
            status TEXT DEFAULT 'present',
            notes TEXT DEFAULT '',
            from_date TEXT DEFAULT '',
            to_date TEXT DEFAULT '',
            present_date TEXT DEFAULT '',
            platoon TEXT DEFAULT '2nd'
        )
    ''')

    # Migrations for existing DBs
    cols = [row[1] for row in cur.execute('PRAGMA table_info(personnel)').fetchall()]
    if 'present_date' not in cols:
        cur.execute('ALTER TABLE personnel ADD COLUMN present_date TEXT DEFAULT ""')
    if 'platoon' not in cols:
        cur.execute('ALTER TABLE personnel ADD COLUMN platoon TEXT DEFAULT "2nd"')
        cur.execute('UPDATE personnel SET platoon = "2nd" WHERE platoon IS NULL OR platoon = ""')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')

    # Seed 2nd platoon only if empty
    cur.execute('SELECT COUNT(*) FROM personnel WHERE platoon = "2nd"')
    if cur.fetchone()[0] == 0:
        seed_data = [
            ('CW2', 'Carr',        'Jonathon',    'present', '',                      '', '',             '2nd'),
            ('CW3', 'Kunkle',      'Brandon',     'leave',   '',                      '2026-03-16', '2026-03-27', '2nd'),
            ('CW2', 'Bennett',     'Christopher', 'present', '',                      '', '',             '2nd'),
            ('CW2', 'Cabral',      'Jeudy',       'present', '',                      '', '',             '2nd'),
            ('CW2', 'Carroll',     'Hunter',      'present', '',                      '', '',             '2nd'),
            ('CW2', 'Dunn',        'Aaron J.',    'present', '',                      '', '',             '2nd'),
            ('CW2', 'Federman',    'Simon',       'present', '',                      '', '',             '2nd'),
            ('CW2', 'Fry',         'Zachary',     'present', '',                      '', '',             '2nd'),
            ('CW2', 'Funk',        'Caleb',       'present', '',                      '', '',             '2nd'),
            ('CW2', 'Glossup',     'Jaden',       'present', '',                      '', '',             '2nd'),
            ('CW2', 'Haertner',    'Nicholas',    'present', '',                      '', '',             '2nd'),
            ('CW2', 'Hilts',       'Brian',       'tdy',     'TF Hunter',             '2025-12-28', '2026-05-28', '2nd'),
            ('CW2', 'May',         'Kevin',       'present', '',                      '', '',             '2nd'),
            ('CW2', 'Michot',      'Ryan A.',     'present', '',                      '', '',             '2nd'),
            ('CW2', 'Moenga',      'Leslie',      'present', '',                      '', '',             '2nd'),
            ('CW2', 'Peart',       'Sachin',      'tdy',     'IPC',                   '2026-03-03', '2026-04-09', '2nd'),
            ('CW2', 'Ren',         'Norman',      'tdy',     'Phase 1 WOBC - Dothan AL', '2026-02-26', '2026-03-28', '2nd'),
            ('CW2', 'Smith',       'Benjamin L.', 'tdy',     'Phase 1 WOBC - Dothan AL', '2026-02-26', '2026-03-28', '2nd'),
            ('CW2', 'Taylor',      'Anthony M.',  'present', '',                      '', '',             '2nd'),
            ('CW2', 'Wood',        'Albert',      'tdy',     'UC35 - Orlando FL',     '2026-03-02', '2026-04-03', '2nd'),
            ('CW2', 'Torres',      'Ricardo J.',  'present', '',                      '', '',             '2nd'),
            ('WO1', 'Harrington',  'Brendan',     'present', '',                      '', '',             '2nd'),
            ('WO1', 'Morlan',      'Evan',        'present', '',                      '', '',             '2nd'),
            ('WO1', 'Schilt',      'Emmett',      'present', '',                      '', '',             '2nd'),
            ('WO1', 'White',       'Kenneth J',   'present', '',                      '', '',             '2nd'),
            ('WO1', 'Yaden',       'Paul',        'tdy',     'SERE 220',              '2026-03-23', '2026-03-28', '2nd'),
        ]
        cur.executemany(
            'INSERT INTO personnel (rank, last, first, status, notes, from_date, to_date, platoon) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
            seed_data
        )

    conn.commit()
    conn.close()


# --- Routes ---

@app.route('/')
def index():
    return send_from_directory('.', 'index.html')


@app.route('/api/platoons', methods=['GET'])
def get_platoons():
    conn = get_db()
    result = {}
    for key, default_name in PLATOONS.items():
        row = conn.execute('SELECT value FROM settings WHERE key = ?', (f'unit_name_{key}',)).fetchone()
        count = conn.execute('SELECT COUNT(*) FROM personnel WHERE platoon = ?', (key,)).fetchone()[0]
        result[key] = {
            'name': row['value'] if row else default_name,
            'count': count
        }
    conn.close()
    return jsonify(result)


@app.route('/api/personnel', methods=['GET'])
def get_personnel():
    platoon = request.args.get('platoon', '2nd')
    conn = get_db()
    rows = conn.execute(
        'SELECT * FROM personnel WHERE platoon = ? ORDER BY rank, last, first',
        (platoon,)
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/personnel', methods=['POST'])
def add_person():
    data = request.get_json()
    rank    = data.get('rank', '')
    last    = data.get('last', '')
    first   = data.get('first', '')
    platoon = data.get('platoon', '2nd')
    conn = get_db()
    cur = conn.execute(
        'INSERT INTO personnel (rank, last, first, platoon) VALUES (?, ?, ?, ?)',
        (rank, last, first, platoon)
    )
    new_id = cur.lastrowid
    conn.commit()
    row = conn.execute('SELECT * FROM personnel WHERE id = ?', (new_id,)).fetchone()
    conn.close()
    return jsonify(dict(row)), 201


@app.route('/api/personnel/<int:person_id>', methods=['PUT'])
def update_person(person_id):
    data = request.get_json()
    fields, values = [], []
    for col in ('rank', 'last', 'first', 'status', 'notes', 'from_date', 'to_date', 'present_date'):
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
def delete_person(person_id):
    conn = get_db()
    conn.execute('DELETE FROM personnel WHERE id = ?', (person_id,))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/settings', methods=['GET'])
def get_settings():
    platoon = request.args.get('platoon', '2nd')
    key = f'unit_name_{platoon}'
    conn = get_db()
    row = conn.execute('SELECT value FROM settings WHERE key = ?', (key,)).fetchone()
    conn.close()
    return jsonify({'unit_name': row['value'] if row else PLATOONS.get(platoon, f'{platoon} Platoon')})


@app.route('/api/settings', methods=['PUT'])
def update_settings():
    platoon = request.args.get('platoon', '2nd')
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


if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5000, debug=True)
