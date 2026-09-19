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

# States, territories, and the three military "states" — AA/AE/AP are what an
# APO or FPO address carries, and a roster that cannot record one is no use to
# anybody stationed overseas.
US_STATES = (
    '', 'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL', 'GA', 'HI',
    'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN',
    'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH',
    'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA',
    'WV', 'WI', 'WY',
    'AS', 'GU', 'MP', 'PR', 'VI',           # territories
    'AA', 'AE', 'AP',                        # APO / FPO / DPO
)

ADDRESS_FIELDS = ('address_street', 'address_street2', 'address_city',
                  'address_state', 'address_zip')

# (name, ISO 3166-1 alpha-2, dial code). The alpha-2 is only there so the
# frontend can build the flag from it; nothing here needs it.
#
# Curated, not exhaustive: every NATO member, every place US forces are
# stationed in numbers, and the large countries people marry into. A wrong
# dial code is worse than a missing one — it makes a tel: link that quietly
# calls a stranger — so this list is only ever extended by hand.
#
# +1 is shared by the US and Canada and the NANP is identical either way, so a
# stored +1 number always reads back as the United States. Nothing is lost:
# the dial code and the ten-digit rule are the whole of what the value means.
PHONE_COUNTRIES = (
    ('United States', 'US', '1'), ('Canada', 'CA', '1'), ('Mexico', 'MX', '52'),
    ('United Kingdom', 'GB', '44'), ('Germany', 'DE', '49'), ('Italy', 'IT', '39'),
    ('Spain', 'ES', '34'), ('France', 'FR', '33'), ('Poland', 'PL', '48'),
    ('Netherlands', 'NL', '31'), ('Belgium', 'BE', '32'), ('Portugal', 'PT', '351'),
    ('Greece', 'GR', '30'), ('Turkey', 'TR', '90'), ('Romania', 'RO', '40'),
    ('Bulgaria', 'BG', '359'), ('Hungary', 'HU', '36'), ('Czechia', 'CZ', '420'),
    ('Slovakia', 'SK', '421'), ('Austria', 'AT', '43'), ('Switzerland', 'CH', '41'),
    ('Sweden', 'SE', '46'), ('Norway', 'NO', '47'), ('Denmark', 'DK', '45'),
    ('Finland', 'FI', '358'), ('Iceland', 'IS', '354'), ('Ireland', 'IE', '353'),
    ('Lithuania', 'LT', '370'), ('Latvia', 'LV', '371'), ('Estonia', 'EE', '372'),
    ('Ukraine', 'UA', '380'), ('Croatia', 'HR', '385'), ('Slovenia', 'SI', '386'),
    ('Serbia', 'RS', '381'), ('Albania', 'AL', '355'), ('North Macedonia', 'MK', '389'),
    ('Montenegro', 'ME', '382'), ('Bosnia and Herzegovina', 'BA', '387'),
    ('Luxembourg', 'LU', '352'), ('Malta', 'MT', '356'), ('Cyprus', 'CY', '357'),
    ('Moldova', 'MD', '373'), ('Georgia', 'GE', '995'), ('Armenia', 'AM', '374'),
    ('Azerbaijan', 'AZ', '994'),
    ('South Korea', 'KR', '82'), ('Japan', 'JP', '81'), ('China', 'CN', '86'),
    ('Taiwan', 'TW', '886'), ('Philippines', 'PH', '63'), ('Thailand', 'TH', '66'),
    ('Vietnam', 'VN', '84'), ('Singapore', 'SG', '65'), ('Malaysia', 'MY', '60'),
    ('Indonesia', 'ID', '62'), ('India', 'IN', '91'), ('Pakistan', 'PK', '92'),
    ('Bangladesh', 'BD', '880'), ('Sri Lanka', 'LK', '94'), ('Nepal', 'NP', '977'),
    ('Australia', 'AU', '61'), ('New Zealand', 'NZ', '64'),
    ('Kuwait', 'KW', '965'), ('Qatar', 'QA', '974'), ('Bahrain', 'BH', '973'),
    ('United Arab Emirates', 'AE', '971'), ('Saudi Arabia', 'SA', '966'),
    ('Oman', 'OM', '968'), ('Jordan', 'JO', '962'), ('Israel', 'IL', '972'),
    ('Iraq', 'IQ', '964'), ('Lebanon', 'LB', '961'), ('Egypt', 'EG', '20'),
    ('Djibouti', 'DJ', '253'),
    ('South Africa', 'ZA', '27'), ('Nigeria', 'NG', '234'), ('Kenya', 'KE', '254'),
    ('Ghana', 'GH', '233'), ('Ethiopia', 'ET', '251'), ('Morocco', 'MA', '212'),
    ('Tunisia', 'TN', '216'),
    ('Brazil', 'BR', '55'), ('Argentina', 'AR', '54'), ('Chile', 'CL', '56'),
    ('Colombia', 'CO', '57'), ('Peru', 'PE', '51'), ('Ecuador', 'EC', '593'),
    ('Venezuela', 'VE', '58'), ('Panama', 'PA', '507'), ('Costa Rica', 'CR', '506'),
    ('Guatemala', 'GT', '502'), ('Honduras', 'HN', '504'), ('El Salvador', 'SV', '503'),
    ('Nicaragua', 'NI', '505'),
)

# Longest first, so +35 never shadows +351 when a value is split.
DIAL_CODES = tuple(sorted({d for _, _, d in PHONE_COUNTRIES}, key=lambda d: (-len(d), d)))

# E.164 caps a whole number at 15 digits including the country code. The NANP
# is exactly 10 and the user asked for that to be a hard stop.
NANP_DIGITS = 10
E164_MAX = 15

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
# Winston-Salem, O'Fallon, Coeur d'Alene, Fort Walton Beach. No digits.
CITY_RE = re.compile(r"^[A-Za-z][A-Za-z .'\-]*$")
ZIP_RE = re.compile(r'^\d{5}(-\d{4})?$')
DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')

MAX_LEN = 200          # every short field
MAX_LEN_LONG = 2000    # address, notes


def has_letters(value):
    """A DSN, an extension, a note. Such a value is stored exactly as typed."""
    return bool(re.search(r'[A-Za-z]', '' if value is None else str(value)))


def phone_split(value):
    """(dial, national). A value with no '+' is a NANP number, which is the default.

    Only a leading '+' declares a country. Everything already in the column
    predates the country picker and is a bare US number, so treating a bare
    value as +1 is what keeps those rows meaning what they meant.
    """
    s = ('' if value is None else str(value)).strip()
    if not s.startswith('+'):
        return '1', s
    rest = s[1:]
    digits = re.sub(r'\D', '', rest)
    dial = next((d for d in DIAL_CODES if digits.startswith(d)), None)
    if dial is None:
        # A '+' with a country code that is not on the list. Reading it as +1
        # would take "+999 123 4567", find ten digits and build a tel: link
        # that calls a stranger. '' means "declared a country, and not one we
        # know" — which is a thing to refuse, not to guess at.
        return '', s
    taken = i = 0
    while i < len(rest) and taken < len(dial):
        if rest[i].isdigit():
            taken += 1
        i += 1
    return dial, rest[i:].strip()


def phone_join(dial, national):
    """The stored spelling. +1 carries no prefix: it is the default and implied."""
    national = (national or '').strip()
    if not national or not dial:
        return national          # unknown country: the '+' is still in there
    return national if dial == '1' else f'+{dial} {national}'


def max_national_digits(dial):
    return NANP_DIGITS if dial == '1' else E164_MAX - len(dial)


def national_digits(dial, national):
    """The national digits, with a NANP trunk '1' dropped."""
    d = re.sub(r'\D', '', national or '')
    if dial == '1' and len(d) == 11 and d.startswith('1'):
        d = d[1:]
    return d


def format_phone(value):
    s = ('' if value is None else str(value)).strip()
    if not s or has_letters(s):
        return s
    dial, national = phone_split(s)
    if not dial:
        return s
    d = national_digits(dial, national)
    if dial == '1':
        return phone_join('1', f'({d[0:3]}) {d[3:6]}-{d[6:]}') if len(d) == NANP_DIGITS else s
    # No invented grouping for a numbering plan we do not know; just one space
    # between whatever groups the person typed.
    return phone_join(dial, ' '.join(national.split()))


def phone_e164(value):
    """'+<dial><digits>' for a tel: link, or '' when the value cannot be dialled."""
    s = ('' if value is None else str(value)).strip()
    if not s or has_letters(s):
        return ''
    dial, national = phone_split(s)
    if not dial:
        return ''
    d = national_digits(dial, national)
    if dial == '1':
        return f'+1{d}' if len(d) == NANP_DIGITS else ''
    return f'+{dial}{d}' if 4 <= len(d) <= max_national_digits(dial) else ''


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
        # a plain number has to be a real one for the country it declares.
        if has_letters(s):
            return None
        dial, national = phone_split(s)
        if not dial:
            return 'Pick the country code from the list.'
        d = national_digits(dial, national)
        if dial == '1':
            return None if len(d) == NANP_DIGITS else 'A +1 number is exactly 10 digits.'
        limit = max_national_digits(dial)
        if len(d) < 4:
            return f'Enter at least 4 digits after +{dial}.'
        return None if len(d) <= limit else f'At most {limit} digits after +{dial}.'

    if name == 'email':
        return None if EMAIL_RE.match(s) else 'Enter a valid email address.'

    if name == 'dod_id':
        return None if re.fullmatch(r'\d{10}', s) else 'DOD ID is exactly 10 digits.'

    if name == 'mos':
        return None if MOS_RE.match(s) else 'MOS is 2-3 numbers then a letter, e.g. 11B or 155E.'

    if name in NAME_FIELDS:
        return None if NAME_RE.match(s) else 'Letters, spaces, hyphens and apostrophes only.'

    if name == 'address_city':
        return None if CITY_RE.match(s) else 'Letters, spaces, hyphens and apostrophes only.'

    if name == 'address_state':
        return None if s in US_STATES else 'Choose a state from the list.'

    if name == 'address_zip':
        return None if ZIP_RE.match(s) else 'ZIP is 5 digits, or 5+4 as 12345-6789.'

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
    if name == 'address_state':
        return s.upper() if len(s) == 2 else s
    if name == 'address_zip':
        # A pasted ZIP+4 arrives as nine straight digits about as often as not.
        d = re.sub(r'\D', '', s)
        if len(d) == 9 and re.fullmatch(r'[\d\s-]+', s):
            return f'{d[:5]}-{d[5:]}'
        return d if len(d) == 5 and re.fullmatch(r'[\d\s-]+', s) else s
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
