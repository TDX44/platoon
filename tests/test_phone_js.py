"""The frontend's phone helpers, lifted out of index.html and run under node.

Twenty warrants posted their numbers into a group chat in twenty spellings.
One spelling is stored, every dialable one is a tel: link, and anything that
is not ten digits stays text rather than becoming a link that dials nothing.
"""
import json, os, re, shutil, subprocess, tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX = os.path.join(ROOT, 'index.html')

RAW = [
    '2125550143',          # bare ten, the common case
    '310-555-0147',        # hyphenated
    '(310) 555-0147',      # already formatted: formatting is a fixed point
    '+1 310 555 0147',     # leading country code
    '1-310-555-0147',
    '  3105550147  ',      # stray whitespace off a paste
    '310.555.0147',
    '555-1234',            # too short: not a number we can dial
    '310-555-0147 x204',   # an extension is not a dialable string
    'DSN 312-555-0100',
    '',
    None,
]


def extract(src, pattern, what):
    m = re.search(pattern, src, re.S)
    assert m, f'could not find {what} in index.html'
    return m.group(0)


def main():
    src = open(INDEX, encoding='utf-8').read()
    # The directory cell and the details modal must go through phoneHtml, not
    # print the raw column — that was the whole point.
    assert "phoneHtml(r.phone) || dash" in src, 'the directory column no longer renders through phoneHtml'
    assert "if (kind === 'phone') html = phoneHtml(shown, true);" in src, \
        'the details modal no longer renders phone rows as links'
    assert "if (PROFILE_PHONE_IDS.includes(key)) return 'phone';" in src, \
        'detailKind() no longer recognises the phone fields'
    assert "escapeHtml(r.phone" not in src, 'the directory column still prints the raw phone'

    js = '\n'.join([
        extract(src, r'function escapeHtml\(str\) \{.*?\n\}', 'escapeHtml'),
        extract(src, r'function phoneDigits\(raw\) \{.*?\n\}', 'phoneDigits'),
        extract(src, r'function formatPhone\(raw\) \{.*?\n\}', 'formatPhone'),
        extract(src, r'function phoneHtml\(raw, withSms = false\) \{.*?\n\}', 'phoneHtml'),
        'const RAW = ' + json.dumps(RAW) + ';',
        'console.log(JSON.stringify({',
        '  fmt: RAW.map(formatPhone),',
        '  plain: RAW.map(v => phoneHtml(v)),',
        '  sms: phoneHtml("3105550147", true),',
        '  twice: formatPhone(formatPhone("+1 310 555 0147")),',
        '  xss: phoneHtml("<script>alert(1)</script>"),',
        '}));',
    ])
    path = os.path.join(tempfile.mkdtemp(), 'phone.js')
    open(path, 'w').write(js)
    proc = subprocess.run([shutil.which('node'), path], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    out = json.loads(proc.stdout)

    assert out['fmt'][:7] == [
        '(212) 555-0143', '(310) 555-0147', '(310) 555-0147', '(310) 555-0147',
        '(310) 555-0147', '(310) 555-0147', '(310) 555-0147',
    ], out['fmt']
    # Not ten digits: handed back as typed, trimmed, never mangled into a
    # number somebody would then dial.
    assert out['fmt'][7:] == ['555-1234', '310-555-0147 x204', 'DSN 312-555-0100', '', ''], out['fmt']
    assert out['twice'] == '(310) 555-0147', out['twice']

    dialable = out['plain'][:7]
    assert all(h.startswith('<a class="phone-link" href="tel:+1') for h in dialable), dialable
    assert all('+13105550147' in h for h in dialable[1:]), dialable
    # The directory row is a click target of its own; the link must not also
    # open the soldier page on the way to the dialler.
    assert all('event.stopPropagation()' in h for h in dialable), dialable
    assert out['plain'][7] == '555-1234' and out['plain'][8] == '310-555-0147 x204', out['plain']
    assert out['plain'][10] == '' and out['plain'][11] == '', out['plain']

    assert 'href="sms:+13105550147"' in out['sms'] and '>Text<' in out['sms'], out['sms']
    assert '<script>' not in out['xss'] and '&lt;script&gt;' in out['xss'], out['xss']
    print('ok')


if __name__ == '__main__':
    main()
