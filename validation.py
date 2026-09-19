"""What a leader is allowed to type into a field. Pure: no Flask, no DB, no clock.

Every one of these rules exists twice — here, and inline in ``index.html`` as
``FIELD_RULES`` — because this app has no build step and nothing to share a
module with. Two copies drift, so ``tests/test_validation.py`` lifts the JS
out of ``index.html`` and runs both implementations over the same table of
cases, and fails the build the moment they disagree. Change a rule here and
that test will tell you the other half is still on the old one.

Nothing is required. Every field accepts '' — a roster is filled in over
weeks, and a half-finished profile must still save.
"""
import json
import re

# ─── Vocabularies ────────────────────────────────────────────────────────────
# Offered in the UI as dropdowns, enforced here. '' is "not recorded".
CLEARANCES = (
    '', 'None', 'Confidential', 'Interim Secret', 'Secret',
    'Interim Top Secret', 'Top Secret', 'TS/SCI',
)

# The ones a line unit actually qualifies on. Not the whole catalogue: a list
# nobody can find their weapon in is worse than a list that is one short, so
# add to it rather than opening it up to free text.
WEAPONS = (
    'M4', 'M4A1', 'M16A2', 'M16A4', 'M17', 'M18', 'M9', 'M11',
    'M249', 'M240B', 'M240L', 'M2', 'M2A1', 'MK19',
    'M320', 'M203', 'M136/AT4', 'M141', 'M72 LAW',
    'M107', 'M110', 'M24', 'M2010', 'M500', 'M590',
)

# What the field already holds, spelled the way people actually write it.
# Prod had a row reading 'TS-SCI'; a dropdown that does not know that spelling
# silently blanks the field on the next save, which is data loss disguised as
# a UI improvement. Anything not here and not a case variant of a real option
# is left alone and refused, so it is visible rather than guessed at.
CLEARANCE_ALIASES = {
    'ts': 'Top Secret', 'tssci': 'TS/SCI', 'ts-sci': 'TS/SCI', 'ts sci': 'TS/SCI',
    'ts/sci': 'TS/SCI', 'top secret/sci': 'TS/SCI', 'top secret sci': 'TS/SCI',
    'sci': 'TS/SCI', 's': 'Secret', 'sec': 'Secret', 'c': 'Confidential',
    'n/a': 'None', 'na': 'None', 'none': 'None',
}

NAME_FIELDS = ('last', 'first', 'emergency_name', 'spouse_dependents', 'next_of_kin')
PHONE_FIELDS = ('phone', 'emergency_phone')
DATE_FIELDS = ('dob', 'date_of_rank', 'ets_date', 'medical_date', 'dental_date')

# ─── Patterns ────────────────────────────────────────────────────────────────
# Letters, spaces, hyphens, apostrophes, periods and commas. No digits: a name
# field holding "2 kids" is how "Spouse / dependents" stopped being a name and
# started being prose nobody could search.
NAME_RE = re.compile(r"^[A-Za-z][A-Za-z .,'\-]*$")
# Deliberately not RFC 5322. One @, no spaces, a dot in the domain — that
# rejects every typo anybody actually makes and accepts every address.
EMAIL_RE = re.compile(r'^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$')
# 11B, 35F, 155E, 153A. Two or three digits and one letter.
MOS_RE = re.compile(r'^\d{2,3}[A-Za-z]$')
DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')

MAX_LEN = 200          # every short field
MAX_LEN_LONG = 2000    # address, notes


def digits_of(value):
    """The dialable digits of a phone, or '' when the field is not just a number.

    A letter anywhere means the field carries more than a number — "DSN
    312-555-0100", "x204" — and such a value is stored exactly as typed rather
    than reformatted into something a phone would cheerfully dial.
    """
    s = '' if value is None else str(value)
    if re.search(r'[A-Za-z]', s):
        return ''
    d = re.sub(r'\D', '', s)
    return d[1:] if len(d) == 11 and d.startswith('1') else d


def format_phone(value):
    s = ('' if value is None else str(value)).strip()
    d = digits_of(s)
    return f'({d[0:3]}) {d[3:6]}-{d[6:]}' if len(d) == 10 else s


def _is_real_date(s):
    if not DATE_RE.match(s):
        return False
    y, m, d = int(s[0:4]), int(s[5:7]), int(s[8:10])
    if not (1 <= m <= 12 and 1 <= d <= 31):
        return False
    days = [31, 29 if (y % 4 == 0 and y % 100 != 0) or y % 400 == 0 else 28,
            31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    return d <= days[m - 1] and 1900 <= y <= 2200


def parse_weapons(value):
    """The stored weapons_qual as a list of {weapon, date}, or None if it is legacy.

    The column used to hold one line of prose ("M4 qual 14MAR26"). That is not
    an error and is never thrown away — None means "this is the old shape",
    and the caller shows it to the leader to re-enter rather than deleting it.
    """
    s = ('' if value is None else str(value)).strip()
    if not s:
        return []
    try:
        parsed = json.loads(s)
    except (ValueError, TypeError):
        return None
    if not isinstance(parsed, list):
        return None
    out = []
    for row in parsed:
        if not isinstance(row, dict):
            return None
        out.append({'weapon': str(row.get('weapon', '')), 'date': str(row.get('date', ''))})
    return out


def validate_field(name, value):
    """One field. Returns an error message, or None when the value is allowed."""
    s = '' if value is None else str(value).strip()
    if not s:
        return None                      # nothing is required

    limit = MAX_LEN_LONG if name in ('address', 'profile_notes') else MAX_LEN
    if len(s) > limit:
        return f'Too long (max {limit} characters).'

    if name in PHONE_FIELDS:
        # A lettered value is a DSN or an extension and is taken as written;
        # a plain number has to be a real one.
        if re.search(r'[A-Za-z]', s):
            return None
        d = re.sub(r'\D', '', s)
        if len(d) == 11 and d.startswith('1'):
            d = d[1:]
        return None if len(d) == 10 else 'Enter a 10-digit phone number.'

    if name == 'email':
        return None if EMAIL_RE.match(s) else 'Enter a valid email address.'

    if name == 'dod_id':
        return None if re.fullmatch(r'\d{10}', s) else 'DOD ID is exactly 10 digits.'

    if name == 'mos':
        return None if MOS_RE.match(s) else 'MOS is 2-3 numbers then a letter, e.g. 11B or 155E.'

    if name in NAME_FIELDS:
        return None if NAME_RE.match(s) else 'Letters, spaces, hyphens and apostrophes only.'

    if name == 'clearance':
        return None if s in CLEARANCES else 'Choose a clearance from the list.'

    if name in DATE_FIELDS:
        return None if _is_real_date(s) else 'Enter a valid date.'

    if name == 'weapons_qual':
        rows = parse_weapons(s)
        if rows is None:
            return None                  # legacy prose: left alone, never rejected
        for row in rows:
            if row['weapon'] not in WEAPONS:
                return f'{row["weapon"] or "(blank)"} is not on the weapons list.'
            if row['date'] and not _is_real_date(row['date']):
                return 'Enter a valid qualification date.'
        return None

    return None                          # address, section, flags, notes: free text


def normalize_field(name, value):
    """The one spelling that gets stored. Runs before validate_field()."""
    s = '' if value is None else str(value).strip()
    if not s:
        return ''
    if name in PHONE_FIELDS:
        return format_phone(s)
    if name == 'mos':
        return s.upper()
    if name == 'dod_id':
        return re.sub(r'\D', '', s) if re.fullmatch(r'[\d\s-]+', s) else s
    if name == 'clearance':
        key = ' '.join(s.lower().split())
        if key in CLEARANCE_ALIASES:
            return CLEARANCE_ALIASES[key]
        for option in CLEARANCES:
            if option and option.lower() == key:
                return option
        return s
    return s


def validate_profile(updates):
    """[(field, message)] for everything wrong, normalized values applied in place."""
    errors = []
    for field in list(updates):
        updates[field] = normalize_field(field, updates[field])
        msg = validate_field(field, updates[field])
        if msg:
            errors.append((field, msg))
    return errors
