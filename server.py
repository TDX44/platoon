import sqlite3
import os
from flask import Flask, request, jsonify, send_from_directory

app = Flask(__name__, static_folder='.', static_url_path='')
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'accountability.db')


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
            present_date TEXT DEFAULT ''
        )
    ''')

    # Migration: add present_date column if missing (existing DBs)
    cols = [row[1] for row in cur.execute('PRAGMA table_info(personnel)').fetchall()]
    if 'present_date' not in cols:
        cur.execute('ALTER TABLE personnel ADD COLUMN present_date TEXT DEFAULT ""')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')

    # Seed only if tables are empty
    cur.execute('SELECT COUNT(*) FROM personnel')
    if cur.fetchone()[0] == 0:
        seed_data = [
            ('CW2', 'Carr', 'Jonathon', 'present', '', '', ''),
            ('CW3', 'Kunkle', 'Brandon', 'leave', '', '2026-03-16', '2026-03-27'),
            ('CW2', 'Bennett', 'Christopher', 'present', '', '', ''),
            ('CW2', 'Cabral', 'Jeudy', 'present', '', '', ''),
            ('CW2', 'Carroll', 'Hunter', 'present', '', '', ''),
            ('CW2', 'Dunn', 'Aaron J.', 'present', '', '', ''),
            ('CW2', 'Federman', 'Simon', 'present', '', '', ''),
            ('CW2', 'Fry', 'Zachary', 'present', '', '', ''),
            ('CW2', 'Funk', 'Caleb', 'present', '', '', ''),
            ('CW2', 'Glossup', 'Jaden', 'present', '', '', ''),
            ('CW2', 'Haertner', 'Nicholas', 'present', '', '', ''),
            ('CW2', 'Hilts', 'Brian', 'tdy', 'TF Hunter', '2025-12-28', '2026-05-28'),
            ('CW2', 'May', 'Kevin', 'present', '', '', ''),
            ('CW2', 'Michot', 'Ryan A.', 'present', '', '', ''),
            ('CW2', 'Moenga', 'Leslie', 'present', '', '', ''),
            ('CW2', 'Peart', 'Sachin', 'tdy', 'IPC', '2026-03-03', '2026-04-09'),
            ('CW2', 'Ren', 'Norman', 'tdy', 'Phase 1 WOBC - Dothan AL', '2026-02-26', '2026-03-28'),
            ('CW2', 'Smith', 'Benjamin L.', 'tdy', 'Phase 1 WOBC - Dothan AL', '2026-02-26', '2026-03-28'),
            ('CW2', 'Taylor', 'Anthony M.', 'present', '', '', ''),
            ('CW2', 'Wood', 'Albert', 'tdy', 'UC35 - Orlando FL', '2026-03-02', '2026-04-03'),
            ('CW2', 'Torres', 'Ricardo J.', 'present', '', '', ''),
            ('WO1', 'Harrington', 'Brendan', 'present', '', '', ''),
            ('WO1', 'Morlan', 'Evan', 'present', '', '', ''),
            ('WO1', 'Schilt', 'Emmett', 'present', '', '', ''),
            ('WO1', 'White', 'Kenneth J', 'present', '', '', ''),
            ('WO1', 'Yaden', 'Paul', 'tdy', 'SERE 220', '2026-03-23', '2026-03-28'),
        ]
        cur.executemany(
            'INSERT INTO personnel (rank, last, first, status, notes, from_date, to_date) VALUES (?, ?, ?, ?, ?, ?, ?)',
            seed_data
        )

    cur.execute('SELECT COUNT(*) FROM settings')
    if cur.fetchone()[0] == 0:
        cur.execute("INSERT INTO settings (key, value) VALUES (?, ?)", ('unit_name', '2nd Platoon Accountability'))

    conn.commit()
    conn.close()


# --- Routes ---

@app.route('/')
def index():
    return send_from_directory('.', 'index.html')


@app.route('/api/personnel', methods=['GET'])
def get_personnel():
    conn = get_db()
    rows = conn.execute('SELECT * FROM personnel ORDER BY rank, last, first').fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/personnel', methods=['POST'])
def add_person():
    data = request.get_json()
    rank = data.get('rank', '')
    last = data.get('last', '')
    first = data.get('first', '')
    conn = get_db()
    cur = conn.execute(
        'INSERT INTO personnel (rank, last, first) VALUES (?, ?, ?)',
        (rank, last, first)
    )
    new_id = cur.lastrowid
    conn.commit()
    row = conn.execute('SELECT * FROM personnel WHERE id = ?', (new_id,)).fetchone()
    conn.close()
    return jsonify(dict(row)), 201


@app.route('/api/personnel/<int:person_id>', methods=['PUT'])
def update_person(person_id):
    data = request.get_json()
    fields = []
    values = []
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
    conn = get_db()
    rows = conn.execute('SELECT key, value FROM settings').fetchall()
    conn.close()
    return jsonify({r['key']: r['value'] for r in rows})


@app.route('/api/settings', methods=['PUT'])
def update_settings():
    data = request.get_json()
    conn = get_db()
    for key, value in data.items():
        conn.execute(
            'INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = ?',
            (key, value, value)
        )
    conn.commit()
    rows = conn.execute('SELECT key, value FROM settings').fetchall()
    conn.close()
    return jsonify({r['key']: r['value'] for r in rows})


if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5000, debug=True)
