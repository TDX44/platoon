"""Field rules: that they are right, and that both copies of them agree.

validation.py is the boundary. index.html carries the same rules inline so a
leader is told what is wrong before they lose the keystrokes — there is no
build step to share a module with, so there are two copies and they can drift.
This runs BOTH over the same table and fails the moment they disagree.
"""
import json, os, re, shutil, subprocess, sys, tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import validation  # noqa: E402

INDEX = os.path.join(ROOT, 'index.html')

# (field, value) -> checked by both implementations. Anything surprising is
# pinned by name in EXPECTED below.
CASES = [
    ('phone', ''), ('phone', '2125550143'), ('phone', '212-555-0143'),
    ('phone', '+1 (212) 555-0143'), ('phone', '1-212-555-0143'),
    ('phone', '555-1234'), ('phone', '212-555-0143 x204'),
    ('phone', 'DSN 312-555-0100'), ('phone', '21255501431'),
    ('emergency_phone', '3105550147'), ('emergency_phone', '310555014'),
    # A '+' declares a country; a bare value is the NANP, which is what every
    # row written before the country picker existed is.
    ('phone', '+49 30 12345678'), ('phone', '+49 3012345678'),
    ('phone', '+44 20 7946 0958'), ('phone', '+1 212 555 0143'),
    ('phone', '+49 12'), ('phone', '+351 912 345 678'),
    ('phone', '+61 2 9374 4000'), ('phone', '+999 123 4567'),
    ('phone', '+49 1234567890123456'), ('emergency_phone', '+82 2 7914 4000'),

    ('email', ''), ('email', 'a@b.co'), ('email', 'first.last@army.mil'),
    ('email', 'nope'), ('email', 'a@b'), ('email', 'a b@c.com'), ('email', 'a@@b.com'),

    ('dod_id', ''), ('dod_id', '1234567890'), ('dod_id', '123456789'),
    ('dod_id', '12345678901'), ('dod_id', '12345678ab'),

    ('mos', ''), ('mos', '11B'), ('mos', '35F'), ('mos', '155E'), ('mos', '153A'),
    ('mos', '11b'), ('mos', '1B'), ('mos', '1155E'), ('mos', '11'), ('mos', '11BB'),

    ('last', 'Smith'), ('last', "O'Brien"), ('last', 'Walker-Leahy'),
    ('last', 'Williams-White'), ('last', 'St. John'), ('last', 'Smith2'),
    ('last', '2Pac'), ('last', ''), ('first', 'Jean-Luc'),
    ('emergency_name', 'Doe, Jane'), ('spouse_dependents', 'Jane Doe'),
    ('spouse_dependents', '2 kids'), ('next_of_kin', 'Mary-Anne OBrien'),

    ('clearance', ''), ('clearance', 'Secret'), ('clearance', 'TS/SCI'),
    ('clearance', 'Top Secret'), ('clearance', 'secret'), ('clearance', 'Cosmic'),
    ('clearance', 'TS-SCI'), ('clearance', 'ts'), ('clearance', '  TOP   SECRET  '),

    ('dob', ''), ('dob', '1990-02-28'), ('dob', '1992-02-29'), ('dob', '1990-02-30'),
    ('dob', '1990-13-01'), ('dob', '90-01-01'), ('dob', '1800-01-01'),
    ('ets_date', '2027-06-30'), ('medical_date', '2026-11-31'),

    ('weapons_qual', ''),
    ('weapons_qual', '[{"weapon":"M4","date":"2026-03-14"}]'),
    ('weapons_qual', '[{"weapon":"M4","date":""},{"weapon":"M9","date":"2026-01-02"}]'),
    ('weapons_qual', '[{"weapon":"Trebuchet","date":""}]'),
    ('weapons_qual', '[{"weapon":"M4","date":"2026-02-30"}]'),
    ('weapons_qual', '[{"weapon":"","date":""}]'),
    ('weapons_qual', 'M4 qual 14MAR26'),          # legacy prose: never rejected
    ('weapons_qual', '{"weapon":"M4"}'),          # JSON, but not a list

    ('address', '123 Main St, Apt 4'), ('section', 'S2'), ('flags', 'None'),

    ('address_street', '123 Main St'), ('address_street2', 'Apt 4B'),
    ('address_city', ''), ('address_city', 'Fort Walton Beach'),
    ('address_city', "O'Fallon"), ('address_city', 'Winston-Salem'),
    ('address_city', 'Apt 4'), ('address_city', '90210'),
    ('address_state', ''), ('address_state', 'FL'), ('address_state', 'fl'),
    ('address_state', 'AE'), ('address_state', 'XX'), ('address_state', 'Florida'),
    ('address_zip', ''), ('address_zip', '32547'), ('address_zip', '32547-1234'),
    ('address_zip', '325471234'), ('address_zip', '3254'), ('address_zip', 'ABCDE'),
    ('profile_notes', 'x' * 300),
    ('section', 'x' * 201),
]

# The cases whose verdict is worth stating out loud rather than only comparing.
EXPECTED_OK = {
    ('phone', '212-555-0143'), ('phone', '1-212-555-0143'),
    # A letter means the field carries more than a number — a DSN, an
    # extension — and is stored exactly as typed rather than rejected.
    ('phone', 'DSN 312-555-0100'), ('phone', '212-555-0143 x204'),
    ('email', 'first.last@army.mil'), ('dod_id', '1234567890'), ('mos', '155E'),
    ('mos', '11b'), ('last', "O'Brien"), ('last', 'Walker-Leahy'), ('last', 'St. John'),
    ('clearance', 'TS/SCI'), ('dob', '1992-02-29'), ('ets_date', '2027-06-30'),
    ('address_street', '123 Main St'), ('address_city', 'Fort Walton Beach'),
    ('address_city', "O'Fallon"), ('address_state', 'FL'), ('address_state', 'AE'),
    ('address_zip', '32547'), ('address_zip', '32547-1234'),
    ('weapons_qual', 'M4 qual 14MAR26'), ('profile_notes', 'x' * 300),
    ('phone', '+49 30 12345678'), ('phone', '+44 20 7946 0958'),
    ('phone', '+1 212 555 0143'), ('phone', '+351 912 345 678'),
    ('emergency_phone', '+82 2 7914 4000'),
}
EXPECTED_BAD = {
    ('phone', '555-1234'), ('phone', '21255501431'),
    ('phone', '+49 12'),                       # too short for any plan
    ('phone', '+999 123 4567'),                # not a dial code we know at all
    ('phone', '+49 1234567890123456'),         # past E.164's 15

    ('emergency_phone', '310555014'),
    ('email', 'nope'), ('email', 'a@b'), ('email', 'a b@c.com'),
    ('dod_id', '123456789'), ('dod_id', '12345678901'), ('dod_id', '12345678ab'),
    ('mos', '1B'), ('mos', '1155E'), ('mos', '11'), ('mos', '11BB'),
    ('last', 'Smith2'), ('last', '2Pac'), ('spouse_dependents', '2 kids'),
    ('clearance', 'secret'), ('clearance', 'Cosmic'),
    ('dob', '1990-02-30'), ('dob', '1990-13-01'), ('dob', '90-01-01'), ('dob', '1800-01-01'),
    ('medical_date', '2026-11-31'),
    ('address_city', 'Apt 4'), ('address_city', '90210'),
    ('address_state', 'XX'), ('address_state', 'Florida'),
    ('address_zip', '3254'), ('address_zip', 'ABCDE'),
    ('weapons_qual', '[{"weapon":"Trebuchet","date":""}]'),
    ('weapons_qual', '[{"weapon":"M4","date":"2026-02-30"}]'),
    ('weapons_qual', '[{"weapon":"","date":""}]'),
    ('section', 'x' * 201),
}


def extract(src, pattern, what):
    m = re.search(pattern, src, re.S)
    assert m, f'could not find {what} in index.html'
    return m.group(0)


def run_js(src):
    js = '\n'.join([
        extract(src, r'const PHONE_COUNTRIES = \[.*?\n\];', 'PHONE_COUNTRIES'),
        extract(src, r'const DIAL_CODES = .*?;\n', 'DIAL_CODES'),
        extract(src, r'const NANP_DIGITS = .*?;', 'NANP_DIGITS'),
        extract(src, r'const E164_MAX = .*?;', 'E164_MAX'),
        extract(src, r'function hasLetters\(value\) \{.*?\n\}', 'hasLetters'),
        extract(src, r'function phoneSplit\(value\) \{.*?\n\}', 'phoneSplit'),
        extract(src, r'function phoneJoin\(dial, national\) \{.*?\n\}', 'phoneJoin'),
        extract(src, r'function maxNationalDigits\(dial\) \{.*?\n\}', 'maxNationalDigits'),
        extract(src, r'function nationalDigits\(dial, national\) \{.*?\n\}', 'nationalDigits'),
        extract(src, r'function formatPhone\(value\) \{.*?\n\}', 'formatPhone'),
        extract(src, r"const CLEARANCES = \[.*?\];", 'CLEARANCES'),
        extract(src, r"const WEAPONS = \[.*?\];", 'WEAPONS'),
        extract(src, r"const CLEARANCE_ALIASES = \{.*?\n\};", 'CLEARANCE_ALIASES'),
        extract(src, r"const US_STATES = \[.*?\];", 'US_STATES'),
        extract(src, r"const ADDRESS_FIELDS = \[.*?\];", 'ADDRESS_FIELDS'),
        extract(src, r"const NAME_FIELDS = \[.*?\];", 'NAME_FIELDS'),
        extract(src, r"const PROFILE_PHONE_IDS = \[.*?\];", 'PROFILE_PHONE_IDS'),
        extract(src, r"const PROFILE_DATE_IDS = \[.*?\];", 'PROFILE_DATE_IDS'),
        extract(src, r'const NAME_RE = .*?;', 'NAME_RE'),
        extract(src, r'const EMAIL_RE = .*?;', 'EMAIL_RE'),
        extract(src, r'const MOS_RE = .*?;', 'MOS_RE'),
        extract(src, r'const CITY_RE = .*?;', 'CITY_RE'),
        extract(src, r'const ZIP_RE = .*?;', 'ZIP_RE'),
        extract(src, r'const ISO_DATE_RE = .*?;', 'ISO_DATE_RE'),
        extract(src, r'const MAX_LEN = .*?;', 'MAX_LEN'),
        extract(src, r'function isRealDate\(s\) \{.*?\n\}', 'isRealDate'),
        extract(src, r'function parseWeapons\(raw\) \{.*?\n\}', 'parseWeapons'),
        extract(src, r'function validateField\(name, value\) \{.*?\n\}', 'validateField'),
        extract(src, r'function normalizeField\(name, value\) \{.*?\n\}', 'normalizeField'),
        'const CASES = ' + json.dumps(CASES) + ';',
        'console.log(JSON.stringify(CASES.map(([f, v]) => '
        '[validateField(f, v), normalizeField(f, v)])));',
    ])
    path = os.path.join(tempfile.mkdtemp(), 'rules.js')
    open(path, 'w').write(js)
    proc = subprocess.run([shutil.which('node'), path], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout)


def main():
    src = open(INDEX, encoding='utf-8').read()
    js = run_js(src)
    assert len(js) == len(CASES)

    drift = []
    for (field, value), (js_err, js_norm) in zip(CASES, js):
        py_err = validation.validate_field(field, value) or ''
        py_norm = validation.normalize_field(field, value)
        if py_err != js_err:
            drift.append(f'validate {field}={value!r}: py={py_err!r} js={js_err!r}')
        if py_norm != js_norm:
            drift.append(f'normalize {field}={value!r}: py={py_norm!r} js={js_norm!r}')
    assert not drift, 'validation.py and index.html disagree:\n  ' + '\n  '.join(drift)

    for case in EXPECTED_OK:
        assert not validation.validate_field(*case), f'{case} should be allowed'
    for case in EXPECTED_BAD:
        assert validation.validate_field(*case), f'{case} should be rejected'
    # Nothing is required: every field takes ''.
    for field in ('phone', 'email', 'dod_id', 'mos', 'last', 'clearance', 'dob', 'weapons_qual'):
        assert validation.validate_field(field, '') is None, field

    # Normalisation is what actually lands in the column.
    assert validation.normalize_field('phone', '212-555-0143') == '(212) 555-0143'
    assert validation.normalize_field('mos', '11b') == '11B'
    assert validation.normalize_field('dod_id', '123-456-7890') == '1234567890'
    # The spelling prod already had, and the ones people type instead.
    assert validation.normalize_field('address_state', 'fl') == 'FL'
    assert validation.normalize_field('address_zip', '325471234') == '32547-1234'
    assert validation.normalize_field('clearance', 'TS-SCI') == 'TS/SCI'
    assert validation.normalize_field('clearance', 'secret') == 'Secret'
    assert validation.normalize_field('clearance', '  TOP   SECRET  ') == 'Top Secret'
    # Normalising is what makes those legal, so the route order matters: a
    # clearance is normalised and only then checked.
    for raw in ('TS-SCI', 'secret', 'ts'):
        assert validation.validate_field('clearance', validation.normalize_field('clearance', raw)) is None, raw
    # Something genuinely unknown is still refused rather than guessed at.
    assert validation.normalize_field('clearance', 'Cosmic') == 'Cosmic'
    assert validation.validate_field('clearance', 'Cosmic')
    updates = {'clearance': 'TS-SCI', 'mos': '11b', 'phone': '212-555-0143'}
    assert validation.validate_profile(updates) == []
    assert updates == {'clearance': 'TS/SCI', 'mos': '11B', 'phone': '(212) 555-0143'}, updates
    # Idempotent, or a round trip through the form would keep changing the row.
    for field, value in CASES:
        once = validation.normalize_field(field, value)
        assert validation.normalize_field(field, once) == once, (field, value, once)

    # The two vocabularies have to be the same on both sides too, since the
    # dropdowns are built from the JS copy and enforced by the Python one.
    server = open(os.path.join(ROOT, 'server.py'), encoding='utf-8').read()
    js_clear = json.loads(extract(src, r'const CLEARANCES = \[.*?\];', 'CLEARANCES')
                          .split('=', 1)[1].strip().rstrip(';').replace("'", '"'))
    js_weapons = json.loads(extract(src, r'const WEAPONS = \[.*?\];', 'WEAPONS')
                            .split('=', 1)[1].strip().rstrip(';').replace("'", '"'))
    assert tuple(js_clear) == validation.CLEARANCES, (js_clear, validation.CLEARANCES)
    assert tuple(js_weapons) == validation.WEAPONS, (js_weapons, validation.WEAPONS)
    js_states = json.loads(extract(src, r'const US_STATES = \[.*?\];', 'US_STATES')
                           .split('=', 1)[1].strip().rstrip(';').replace("'", '"'))
    assert tuple(js_states) == validation.US_STATES, 'the state lists differ'
    js_countries = json.loads(extract(src, r'const PHONE_COUNTRIES = \[.*?\n\];', 'PHONE_COUNTRIES')
                              .split('=', 1)[1].strip().rstrip(';').replace("'", '"'))
    assert [tuple(c) for c in js_countries] == list(validation.PHONE_COUNTRIES), \
        'the country lists differ'
    # A dial code that appears twice under different digits would make phone_split
    # ambiguous; duplicates under the SAME country code are fine (US/Canada).
    for name, iso, dial in validation.PHONE_COUNTRIES:
        assert dial.isdigit() and 1 <= len(dial) <= 3, (name, dial)
        assert len(iso) == 2 and iso.isupper(), (name, iso)

    # Splitting and rejoining is a fixed point, or a save would walk the value.
    for raw in ('(212) 555-0143', '+49 30 12345678', '+44 20 7946 0958', '', 'DSN 1'):
        dial, national = validation.phone_split(raw)
        assert validation.phone_split(validation.phone_join(dial, national)) == (dial, national), raw
    # Ten is a hard stop for +1 and only for +1.
    assert validation.max_national_digits('1') == 10
    assert validation.max_national_digits('49') == 13
    assert validation.max_national_digits('351') == 12
    assert validation.phone_e164('+49 30 12345678') == '+493012345678'
    assert validation.phone_e164('(212) 555-0143') == '+12125550143'
    assert validation.phone_e164('21255501431') == '', 'an 11-digit +1 number is not dialable'

    # The address columns the form writes must all be columns the route accepts,
    # or a leader fills the form in and the city silently never leaves the page.
    for field in validation.ADDRESS_FIELDS:
        assert f"'{field}'" in server, f'{field} is not in server.py'
    assert server.count("'address_street', 'address_street2', 'address_city',") >= 1

    # The server must actually call this, or none of the above is a boundary.

    assert 'validation.validate_profile(updates)' in server, 'the profile route does not validate'
    assert server.count('err = _name_errors(data)') == 2, 'add and edit must both check the name'

    print('ok')


if __name__ == '__main__':
    main()
