"""Small pure guards in index.html, lifted out and run under node.

A status is free text from the server, so it reaches markup only as a known
key or escaped; CSV cells never start as a spreadsheet formula; a bulk
"Present" never touches someone on a current absence; a save cannot run twice
at once; and a null rank or name does not throw halfway through a report.

Run with: python tests/test_frontend_guards_js.py
"""
import json
import os
import re
import shutil
import subprocess
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX = os.path.join(ROOT, 'index.html')

DRIVER = r'''
(async () => {
const out = {};
const hostile = '"><img src=x onerror=alert(1)>';
out.pillKnown = dashPill('tdy');
out.pillHostile = dashPill(hostile);
out.textHostile = statusText(hostile);
out.badgeHostile = statusBadge(hostile);
out.protoKey = statusLabel('constructor');
out.escNull = escapeHtml(null);
out.escUndef = escapeHtml(undefined);
out.csv = ['=SUM(A1)', '+1', '-2', '@x', '\tx', '\rx', 'plain', 'say "hi"', null].map(csvCell);
const people = [{id: 1, status: 'present'}, {id: 2, status: 'tdy'}, {id: 3, status: 'late'},
                {id: 4, status: 'present'}];
const split = bulkPresentSplit(people, new Set([1, 2, 3]));
out.mark = split.mark.map(p => p.id);
out.away = split.away.map(p => p.id);
out.reportName = reportName({rank: null, last: 'Ash', first: null});
out.problems = [['', '', ''], ['leave', '2026-03-01', ''], ['pass', '', ''],
                ['leave', '2026-03-01', '2026-03-04'], ['tdy', '2026-03-01', ''],
                ['late', '2026-03-01', '2026-03-01']]
  .map(([s, f, t]) => absenceFormProblem(s, f, t));
let calls = 0, release;
const slow = singleFlight(() => { calls++; return new Promise(r => { release = r; }); });
const first = slow(); slow(); slow();
release('done');
out.firstResult = await first;
out.callsWhileBusy = calls;
const again = slow();   // the first has settled, so this one runs
out.callsAfter = calls;
release('again');
await again;
console.log(JSON.stringify(out));
})();
'''


def extract(source, pattern, what):
    m = re.search(pattern, source, re.S)
    assert m, f'could not find {what} in index.html — was it renamed or removed?'
    return m.group(0)


def main():
    src = open(INDEX, encoding='utf-8').read()
    js = '\n'.join([
        extract(src, r'const STATUS_LABELS = \{.*?\};', 'STATUS_LABELS'),
        extract(src, r'function escapeHtml\(str\) \{.*?\n\}', 'escapeHtml()'),
        extract(src, r'function isKnownStatus\(status\) \{.*?\n\}', 'isKnownStatus()'),
        extract(src, r'function statusLabel\(status\) \{.*?\n\}', 'statusLabel()'),
        extract(src, r'function statusClass\(prefix, status\) \{.*?\n\}', 'statusClass()'),
        extract(src, r'function dashPill\(status\) \{.*?\n\}', 'dashPill()'),
        extract(src, r'function statusText\(status\) \{.*?\n\}', 'statusText()'),
        extract(src, r'function statusBadge\(status\) \{.*?\n\}', 'statusBadge()'),
        extract(src, r'function csvCell\(v\) \{.*?\n\}', 'csvCell()'),
        extract(src, r'function bulkPresentSplit\(people, selectedIds\) \{.*?\n\}', 'bulkPresentSplit()'),
        extract(src, r'function lastFirst\(p\) \{.*?\n\}', 'lastFirst()'),
        extract(src, r'function reportName\(p\) \{.*?\n\}', 'reportName()'),
        extract(src, r'function singleFlight\(fn\) \{.*?\n\}', 'singleFlight()'),
        extract(src, r'function absenceFormProblem\(status, from, to\) \{.*?\n\}', 'absenceFormProblem()'),
        DRIVER,
    ])
    path = os.path.join(tempfile.mkdtemp(), 'guards.js')
    with open(path, 'w', encoding='utf-8') as fh:
        fh.write(js)
    proc = subprocess.run([shutil.which('node'), path], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    out = json.loads(proc.stdout)

    assert out['pillKnown'] == '<span class="dash-pill p-tdy">TDY</span>', out['pillKnown']
    for key in ('pillHostile', 'textHostile', 'badgeHostile'):
        html = out[key]
        assert '<img' not in html and '"><' not in html, f'{key} lets a status write markup: {html}'
        assert 'class="' in html and 'onerror' in html and '&lt;img' in html, html
    assert out['protoKey'] == 'constructor', 'a prototype key is treated as a known status'
    assert out['escNull'] == '' and out['escUndef'] == '', 'escapeHtml() does not tolerate null'

    assert out['csv'] == ['"\'=SUM(A1)"', '"\'+1"', '"\'-2"', '"\'@x"', '"\'\tx"', '"\'\rx"',
                          '"plain"', '"say ""hi"""', '""'], out['csv']

    assert out['mark'] == [1] and out['away'] == [2, 3], \
        f'bulk Present would end an absence: {out}'
    # No first name on file: 'Ash', not a dangling 'Ash, '.
    assert out['reportName'] == '      Ash', repr(out['reportName'])
    # Nothing picked, or Leave/Pass with no end, cannot save; the rest can.
    p = out['problems']
    assert p[0] and p[1] and p[2], f'an unpicked status or an open-ended leave/pass saved: {p}'
    assert p[3:] == ['', '', ''], f'a complete absence was refused: {p}'

    assert out['firstResult'] == 'done' and out['callsWhileBusy'] == 1, \
        f'a second tap ran the save again while the first was in flight: {out}'
    assert out['callsAfter'] == 2, 'the guard never releases after a save settles'
    print('ok')


if __name__ == '__main__':
    main()
