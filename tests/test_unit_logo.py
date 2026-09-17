"""A unit carries its own logo, and every unit beneath it inherits it.

Run with: python tests/test_unit_logo.py

The logo is a `settings` row (unit_id, key='logo', value=base64 PNG) — no new
table, no migration. Three things have to hold and none of them are obvious:

  * the validator is a trust boundary. The bytes arrive as base64 in JSON from
    a browser, and they also arrive from a *backup file*, which is user input
    wearing a hat. A short or lying IHDR must be a 400, never a traceback.
  * resolution walks UP the tree, so a team with no logo shows its company's.
    A leader of that team cannot write the company's logo but must be able to
    read it — a logo is not sensitive, and the sidebar would otherwise be
    blank for everyone below the top.
  * it is a tenant row like any other. Another organisation's unit id is a
    404 on all three verbs: same answer whether the unit exists or not, so
    nothing here is an existence oracle.
"""
import base64
import json
import os
import struct
import sys
import zlib

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))
sys.path.insert(0, _HERE)

import dbharness  # noqa: E402

_SCHEMA = dbharness.setup()

import server  # noqa: E402

PNG_SIG = b'\x89PNG\r\n\x1a\n'


def png(w, h, noisy=False):
    """A real PNG of exactly w x h — correct IHDR, IDAT and CRCs.

    `noisy` fills it with incompressible bytes, which is the only honest way
    to build a file that is small in pixels and large on disk.
    """
    def chunk(tag, data):
        return (struct.pack('>I', len(data)) + tag + data +
                struct.pack('>I', zlib.crc32(tag + data) & 0xffffffff))
    ihdr = struct.pack('>IIBBBBB', w, h, 8, 2, 0, 0, 0)      # 8-bit truecolour
    if noisy:
        rows = b''.join(b'\x00' + os.urandom(w * 3) for _ in range(h))
    else:
        rows = b''.join(b'\x00' + b'\x11\x22\x33' * w for _ in range(h))
    return (PNG_SIG + chunk(b'IHDR', ihdr) + chunk(b'IDAT', zlib.compress(rows))
            + chunk(b'IEND', b''))


def b64(raw):
    return base64.b64encode(raw).decode('ascii')


def deep_tree(name):
    """company -> 1st Platoon -> Alpha Team, plus a sibling 2nd Platoon.

    make_tree() only goes two deep, and inheritance is not a rule until there
    is a grandchild to inherit through.
    """
    conn = dbharness.owner_conn()
    try:
        slug = name.lower().replace(' ', '-')
        root = conn.execute(
            'INSERT INTO units (parent_id, root_id, kind, name, slug) '
            'VALUES (NULL, 0, %s, %s, %s) RETURNING id', ('company', name, slug)).fetchone()['id']
        conn.execute('UPDATE units SET root_id = %s WHERE id = %s', (root, root))
        made = {'root': root}
        for key, kind, label, parent in (
                ('plt', 'platoon', '1st Platoon', 'root'),
                ('team', 'team', 'Alpha Team', 'plt'),
                ('sibling', 'platoon', '2nd Platoon', 'root')):
            made[key] = conn.execute(
                'INSERT INTO units (parent_id, root_id, kind, name, slug) '
                'VALUES (%s, %s, %s, %s, %s) RETURNING id',
                (made[parent], root, kind, label, f'{slug}-{key}')).fetchone()['id']
        conn.execute("INSERT INTO settings (root_id, unit_id, key, value) "
                     "VALUES (%s, NULL, 'org_timezone', 'America/Chicago')", (root,))
        conn.commit()
        return made
    finally:
        conn.close()


def audit(root_id, action='UNIT_LOGO'):
    conn = dbharness.owner_conn()
    try:
        return conn.execute(
            'SELECT details, unit_id FROM audit_log WHERE root_id = %s AND action = %s ORDER BY id',
            (root_id, action)).fetchall()
    finally:
        conn.close()


def client_for(unit_id, role='leader'):
    dbharness.as_user(dbharness.make_user(unit_id, role))
    return server.app.test_client()


def logo_of(client, unit_id):
    """The `logo` object /api/units reports for one unit."""
    units = client.get('/api/units').get_json()
    for u in units:
        if u['id'] == unit_id:
            assert 'logo' in u, f'/api/units omits the logo field entirely: {u}'
            return u['logo']
    return 'not-in-list'


# ── The validator: everything it must refuse ──

def test_a_real_512_square_round_trips():
    t = deep_tree('Alpha Co')
    c = client_for(t['root'], 'owner')
    raw = png(512, 512)
    r = c.put(f'/api/units/{t["root"]}/logo', json={'png_base64': b64(raw)})
    assert r.status_code == 200, (r.status_code, r.get_json())
    body = r.get_json()
    assert body['logo']['unit_id'] == t['root'], body
    assert body['logo']['v'], 'no version to cache-bust with'

    g = c.get(f'/api/units/{t["root"]}/logo')
    assert g.status_code == 200, (g.status_code, g.get_data()[:200])
    assert g.mimetype == 'image/png', g.mimetype
    assert g.get_data() == raw, 'the bytes that came back are not the bytes that went in'
    assert g.headers.get('X-Content-Type-Options') == 'nosniff', dict(g.headers)
    assert 'immutable' in (g.headers.get('Cache-Control') or ''), g.headers.get('Cache-Control')
    assert 'private' in (g.headers.get('Cache-Control') or ''), g.headers.get('Cache-Control')

    rows = audit(t['root'])
    assert rows and rows[-1]['unit_id'] == t['root'], rows
    assert '512' in rows[-1]['details'] and 'bytes' in rows[-1]['details'], rows[-1]['details']


def test_a_data_uri_prefix_is_accepted_and_stripped():
    t = deep_tree('Prefix Co')
    c = client_for(t['root'], 'owner')
    raw = png(16, 16)
    r = c.put(f'/api/units/{t["root"]}/logo',
              json={'png_base64': 'data:image/png;base64,' + b64(raw)})
    assert r.status_code == 200, r.get_json()
    assert c.get(f'/api/units/{t["root"]}/logo').get_data() == raw


def test_everything_the_validator_must_refuse():
    t = deep_tree('Reject Co')
    c = client_for(t['root'], 'owner')
    jpeg = b'\xff\xd8\xff\xe0' + b'\x00' * 200 + b'\xff\xd9'
    cases = {
        'one pixel too wide': b64(png(513, 512)),
        'one pixel too tall': b64(png(512, 513)),
        'zero wide': b64(png(0, 8)),
        'zero tall': b64(png(8, 0)),
        'a JPEG': b64(jpeg),
        'not base64 at all': 'this is not base64 !!!',
        'base64 of nothing': '',
        'missing entirely': None,
        'not a string': 12345,
        # The signature lies: it says PNG, then the header runs out mid-IHDR.
        # struct.unpack on eight bytes that are not there is a 500, not a 400.
        'a truncated IHDR': b64(PNG_SIG + b'\x00\x00\x00\x0dIHDR\x00\x00\x02'),
        'the signature and nothing else': b64(PNG_SIG),
        'first chunk is not IHDR': b64(PNG_SIG + b'\x00\x00\x00\x0dpHYs' + b'\x00' * 16),
        'over the byte cap': b64(png(512, 512, noisy=True)),
        # Refused on length before a single byte is decoded.
        'a megabyte of padding': 'A' * (4 << 20),
    }
    for what, value in cases.items():
        r = c.put(f'/api/units/{t["root"]}/logo', json={'png_base64': value})
        assert r.status_code == 400, f'{what}: expected 400, got {r.status_code} {r.get_data()[:200]}'
        msg = (r.get_json() or {}).get('error', '')
        assert msg and msg[0].isupper() and msg.endswith('.'), \
            f'{what}: the refusal is not a sentence a person can read: {msg!r}'
    assert not audit(t['root']), 'a refused logo was still written to the audit log'


def test_an_oversize_png_is_refused_but_a_smaller_noisy_one_is_not():
    """The cap is bytes, not pixels — prove the noisy fixture is honest."""
    assert len(png(512, 512, noisy=True)) > server.LOGO_MAX_BYTES
    assert len(png(128, 128, noisy=True)) <= server.LOGO_MAX_BYTES
    t = deep_tree('Noisy Co')
    c = client_for(t['root'], 'owner')
    assert c.put(f'/api/units/{t["root"]}/logo',
                 json={'png_base64': b64(png(128, 128, noisy=True))}).status_code == 200


# ── Inheritance ──

def test_the_nearest_logo_up_the_tree_wins():
    t = deep_tree('Inherit Co')
    c = client_for(t['root'], 'owner')
    company = png(64, 64)
    platoon = png(32, 32)
    assert c.put(f'/api/units/{t["root"]}/logo', json={'png_base64': b64(company)}).status_code == 200

    # Nothing of its own, nothing at the platoon: the team shows the company's.
    for unit in ('root', 'plt', 'team', 'sibling'):
        got = c.get(f'/api/units/{t[unit]}/logo')
        assert got.status_code == 200, unit
        assert got.get_data() == company, f'{unit} did not inherit the company logo'
    assert logo_of(c, t['team'])['unit_id'] == t['root'], 'units JSON does not name the inherited owner'

    # The platoon takes its own: the team follows the nearer one, the sibling
    # is unaffected.
    assert c.put(f'/api/units/{t["plt"]}/logo', json={'png_base64': b64(platoon)}).status_code == 200
    assert c.get(f'/api/units/{t["team"]}/logo').get_data() == platoon
    assert c.get(f'/api/units/{t["sibling"]}/logo').get_data() == company
    assert logo_of(c, t['team'])['unit_id'] == t['plt']
    assert logo_of(c, t['plt'])['unit_id'] == t['plt']
    assert logo_of(c, t['root'])['unit_id'] == t['root']

    # Removing the nearer one falls back to the company again.
    assert c.delete(f'/api/units/{t["plt"]}/logo').status_code == 200
    assert c.get(f'/api/units/{t["team"]}/logo').get_data() == company
    assert logo_of(c, t['team'])['unit_id'] == t['root']

    # And with nothing anywhere on the path there is simply no logo.
    assert c.delete(f'/api/units/{t["root"]}/logo').status_code == 200
    assert c.delete(f'/api/units/{t["root"]}/logo').status_code == 200, 'DELETE is not idempotent'
    for unit in ('root', 'plt', 'team'):
        assert c.get(f'/api/units/{t[unit]}/logo').status_code == 404, unit
        assert logo_of(c, t[unit]) is None, f'{unit} still claims a logo'
    assert 'removed' in audit(t['root'])[-1]['details'].lower(), audit(t['root'])


def test_the_units_list_resolves_without_a_query_per_unit():
    """Every unit in one page load, two queries for the whole resolution."""
    t = deep_tree('Counting Co')
    c = client_for(t['root'], 'owner')
    c.put(f'/api/units/{t["root"]}/logo', json={'png_base64': b64(png(8, 8))})
    units = c.get('/api/units').get_json()
    assert len(units) == 4 and all(u['logo'] and u['logo']['unit_id'] == t['root'] for u in units), units
    assert len({u['logo']['v'] for u in units}) == 1, 'one stored logo, one version'

    # A created and a renamed unit answer in the same shape as a listed one.
    made = c.post('/api/units', json={'name': '3rd Platoon', 'kind': 'platoon',
                                      'parent_id': t['root']}).get_json()
    assert made['logo'] == {'unit_id': t['root'], 'v': units[0]['logo']['v']}, made
    renamed = c.put(f'/api/units/{made["id"]}', json={'name': '4th Platoon'}).get_json()
    assert renamed['logo'] == made['logo'], renamed


# ── Who may write, who may read ──

def test_a_leader_below_may_read_the_company_logo_but_not_replace_it():
    t = deep_tree('Reach Co')
    boss = client_for(t['root'], 'owner')
    company = png(48, 48)
    boss.put(f'/api/units/{t["root"]}/logo', json={'png_base64': b64(company)})

    team = client_for(t['team'])
    assert team.get(f'/api/units/{t["root"]}/logo').get_data() == company, \
        'a team leader cannot see the company logo their own sidebar shows'
    assert team.get(f'/api/units/{t["team"]}/logo').get_data() == company
    for verb in (team.put, team.delete):
        r = verb(f'/api/units/{t["root"]}/logo', json={'png_base64': b64(png(8, 8))})
        assert r.status_code == 403, (verb, r.status_code)
    # Their own unit is theirs to set.
    assert team.put(f'/api/units/{t["team"]}/logo',
                    json={'png_base64': b64(png(8, 8))}).status_code == 200


def test_a_sibling_leader_is_forbidden_not_confused():
    t = deep_tree('Sibling Co')
    sib = client_for(t['sibling'])
    for r in (sib.put(f'/api/units/{t["plt"]}/logo', json={'png_base64': b64(png(8, 8))}),
              sib.delete(f'/api/units/{t["plt"]}/logo')):
        assert r.status_code == 403, (r.status_code, r.get_json())


def test_another_tenants_unit_is_a_404_on_every_verb():
    a = deep_tree('Tenant A')
    b = deep_tree('Tenant B')
    boss_a = client_for(a['root'], 'owner')
    secret = png(24, 24)
    boss_a.put(f'/api/units/{a["root"]}/logo', json={'png_base64': b64(secret)})

    boss_b = client_for(b['root'], 'owner')
    for r in (boss_b.get(f'/api/units/{a["root"]}/logo'),
              boss_b.put(f'/api/units/{a["root"]}/logo', json={'png_base64': b64(png(8, 8))}),
              boss_b.delete(f'/api/units/{a["root"]}/logo')):
        assert r.status_code == 404, \
            f'tenant A leaked as {r.status_code} — 403 vs 404 is an existence oracle'
    assert not any(u.get('logo') for u in boss_b.get('/api/units').get_json()), \
        "tenant B's units JSON carries a logo it does not own"
    # And A still has hers.
    client_for(a['root'], 'owner')
    assert server.app.test_client().get(f'/api/units/{a["root"]}/logo').get_data() == secret


# ── Backup ──

def test_the_logo_survives_export_and_restore_into_a_clean_tenant():
    a = deep_tree('Export Co')
    boss_a = client_for(a['root'], 'owner')
    company = png(64, 40)
    boss_a.put(f'/api/units/{a["root"]}/logo', json={'png_base64': b64(company)})
    dump = json.loads(boss_a.get('/api/backup').get_data(as_text=True))
    assert any(s['key'] == 'logo' for s in dump['settings']), \
        'the export dropped the logo row: keys are exported generically, not by whitelist'

    b = deep_tree('Import Co')
    boss_b = client_for(b['root'], 'owner')
    r = boss_b.post('/api/backup/restore', json=dump)
    assert r.status_code == 200, r.get_json()
    restored = {u['slug']: u['id'] for u in
                dbharness.owner_conn().execute(
                    'SELECT slug, id FROM units WHERE root_id = %s', (b['root'],)).fetchall()}
    target = restored['export-co']
    got = boss_b.get(f'/api/units/{target}/logo')
    assert got.status_code == 200, (got.status_code, got.get_data()[:200])
    assert got.get_data() == company, 'the restored logo is not byte-identical'


def test_a_backup_carrying_a_rotten_logo_loses_the_row_not_the_restore():
    """A backup file is user input. The decision: drop the bad row, count it in
    skipped_rows, and let everything else land — a whole restore must not fail
    because one setting value was tampered with."""
    a = deep_tree('Rotten Co')
    boss_a = client_for(a['root'], 'owner')
    boss_a.put(f'/api/units/{a["root"]}/logo', json={'png_base64': b64(png(20, 20))})
    dump = json.loads(boss_a.get('/api/backup').get_data(as_text=True))
    for s in dump['settings']:
        if s['key'] == 'logo':
            s['value'] = b64(PNG_SIG + b'\x00\x00\x00\x0dIHDR\x00\x00\x02')

    b = deep_tree('Clean Co')
    boss_b = client_for(b['root'], 'owner')
    r = boss_b.post('/api/backup/restore', json=dump)
    assert r.status_code == 200, r.get_json()
    assert r.get_json()['skipped_rows'] >= 1, r.get_json()
    restored = dbharness.owner_conn().execute(
        'SELECT id FROM units WHERE root_id = %s AND slug = %s', (b['root'], 'rotten-co')).fetchone()
    assert restored, 'the rest of the restore did not land'
    assert boss_b.get(f'/api/units/{restored["id"]}/logo').status_code == 404, \
        'a logo that would not survive its own validator was stored anyway'
    conn = dbharness.owner_conn()
    left = conn.execute("SELECT 1 FROM settings WHERE root_id = %s AND key = 'logo'",
                        (b['root'],)).fetchone()
    conn.close()
    assert not left, 'the rotten row is in the database waiting to 500 something'


def main():
    try:
        test_a_real_512_square_round_trips()
        test_a_data_uri_prefix_is_accepted_and_stripped()
        test_everything_the_validator_must_refuse()
        test_an_oversize_png_is_refused_but_a_smaller_noisy_one_is_not()
        test_the_nearest_logo_up_the_tree_wins()
        test_the_units_list_resolves_without_a_query_per_unit()
        test_a_leader_below_may_read_the_company_logo_but_not_replace_it()
        test_a_sibling_leader_is_forbidden_not_confused()
        test_another_tenants_unit_is_a_404_on_every_verb()
        test_the_logo_survives_export_and_restore_into_a_clean_tenant()
        test_a_backup_carrying_a_rotten_logo_loses_the_row_not_the_restore()
        print('ok')
    finally:
        dbharness.teardown(_SCHEMA)


if __name__ == '__main__':
    main()
