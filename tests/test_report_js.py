"""The accountability report text, lifted out of index.html and run under node.

buildReport() is the whole of both report formats (Detailed and Strength):
who is counted where, the per-sub-unit breakdown, and the rule that anyone
still unaccounted is listed rather than silently left off.

Run with: python tests/test_report_js.py
"""
import json
import os
import re
import shutil
import subprocess
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX = os.path.join(ROOT, 'index.html')
TODAY = '2026-03-17'


def extract(source, pattern, what):
    m = re.search(pattern, source, re.S)
    assert m, f'could not find {what} in index.html — was it renamed or removed?'
    return m.group(0)


def soldier(pid, rank, last, status='present', unit=2, present=True, **kw):
    p = {'id': pid, 'rank': rank, 'last': last, 'first': 'A', 'status': status,
         'unit_id': unit, 'notes': '', 'from': '', 'to': '',
         'present_date': TODAY if (status == 'present' and present) else ''}
    p.update(kw)
    return p


ROSTER = [
    soldier(1, 'SFC', 'Boone', unit=2),
    soldier(2, 'SGT', 'Crane', unit=3),
    soldier(3, 'SPC', 'Ash', 'leave', unit=2, **{'from': '2026-03-10', 'to': '2026-03-20'}),
    soldier(4, 'PFC', 'Dale', 'pass', unit=3, **{'from': '2026-03-17', 'to': '2026-03-18'}),
    soldier(5, 'SPC', 'Eve', 'tdy', unit=3, notes='Air Assault - Fort Example',
            **{'from': '2026-03-01', 'to': '2026-03-30'}),
    soldier(6, 'PV2', 'Fox', 'late', unit=2, notes='Traffic', **{'from': TODAY, 'to': TODAY}),
    soldier(7, 'SPC', 'Gray', present=False, unit=3),     # unaccounted
    soldier(8, '1SG', 'Hale', unit=1),                     # directly in the company: HQ
    soldier(9, 'SPC', 'Ivy', 'ftr', unit=2),
]
# Company 1 with 1st PLT (2) and 2nd PLT (3, which has squad 4).
GROUPS = [{'name': '1st PLT', 'ids': [2]}, {'name': '2nd PLT', 'ids': [3, 4]}]

DRIVER = r'''
const input = JSON.parse(process.argv[2]);
const out = {};
for (const [key, opts, groups] of input.cases) {
  out[key] = buildReport(input.people, opts,
    { unitName: 'HHC', dateLabel: '17 Mar 2026', today: input.today, groups });
}
console.log(JSON.stringify(out));
'''


def run(cases, people=ROSTER):
    src = open(INDEX, encoding='utf-8').read()
    js = '\n'.join([
        extract(src, r'const RANK_ORDER = \[.*?\];', 'RANK_ORDER'),
        extract(src, r'const MONTHS_UPPER = \[.*?\];', 'MONTHS_UPPER'),
        extract(src, r'function rankSort\(a, b\) \{.*?\n\}', 'rankSort()'),
        extract(src, r'function endDateSort\(a, b\) \{.*?\n\}', 'endDateSort()'),
        extract(src, r'function formatDateShort\(dateStr\) \{.*?\n\}', 'formatDateShort()'),
        extract(src, r'function reportName\(p\) \{.*?\n\}', 'reportName()'),
        extract(src, r'const REPORT_LIST_NAMES_MAX = \d+;', 'REPORT_LIST_NAMES_MAX'),
        extract(src, r'function reportCategories\(splitLeavePass\) \{.*?\n\}', 'reportCategories()'),
        extract(src, r'const REPORT_AWAY = \[.*?\];', 'REPORT_AWAY'),
        extract(src, r'function reportBucket\(p, today\) \{.*?\n\}', 'reportBucket()'),
        extract(src, r'function reportCounts\(people, today\) \{.*?\n\}', 'reportCounts()'),
        extract(src, r'function reportCountsLine\(c, splitLeavePass\) \{.*?\n\}', 'reportCountsLine()'),
        extract(src, r'function reportBreakdown\(people, ctx, splitLeavePass\) \{.*?\n\}', 'reportBreakdown()'),
        extract(src, r'function reportDateRange\(p\) \{.*?\n\}', 'reportDateRange()'),
        extract(src, r'function reportDetailLine\(p, bucket\) \{.*?\n\}', 'reportDetailLine()'),
        extract(src, r'function buildReport\(people, opts, ctx\) \{.*?\n\}', 'buildReport()'),
        DRIVER,
    ])
    path = os.path.join(tempfile.mkdtemp(), 'report.js')
    with open(path, 'w', encoding='utf-8') as fh:
        fh.write(js)
    proc = subprocess.run([shutil.which('node'), path,
                           json.dumps({'people': people, 'today': TODAY, 'cases': cases})],
                          capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout)


def main():
    detailed = {'format': 'detailed', 'listPresent': None, 'splitLeavePass': False}
    out = run([
        ['flat', detailed, []],
        ['tree', detailed, GROUPS],
        ['split', {**detailed, 'splitLeavePass': True}, []],
        ['noNames', {**detailed, 'listPresent': False}, []],
        ['strengthFlat', {**detailed, 'format': 'strength'}, []],
        ['strengthTree', {**detailed, 'format': 'strength'}, GROUPS],
    ])

    flat = out['flat']
    lines = flat.split('\n')
    assert lines[:3] == ['HHC', '17 Mar 2026', '9 Assigned'], lines[:3]
    # PDY lists names for a small unit (auto), senior first.
    pdy = lines.index('PDY 3')
    assert [l.split()[0] for l in lines[pdy + 1:pdy + 4]] == ['1SG', 'SFC', 'SGT'], lines[pdy:pdy + 4]
    # The unaccounted soldier is on the report, never silently dropped.
    assert 'UNACCOUNTED 1' in lines, flat
    assert any('Gray' in l for l in lines[lines.index('UNACCOUNTED 1'):]), flat
    assert 'Leave & Pass 2' in lines and 'TDY 1' in lines and 'FTR 1' in lines, flat
    assert any('Air Assault | Fort Example | 1MAR-30MAR' in l for l in lines), flat
    # Late lists its reason, not a today-to-today range.
    late = next(l for l in lines if 'Fox' in l)
    assert late.endswith('Traffic') and 'MAR' not in late, late
    assert 'BY UNIT' not in flat, 'a leaf unit got a breakdown'

    tree = out['tree'].split('\n')
    i = tree.index('BY UNIT')
    assert tree[i + 1] == '1st PLT: 4 Assigned, 1 PDY, 1 Leave & Pass, 1 Late, 1 FTR', tree[i + 1]
    assert tree[i + 2] == '2nd PLT: 4 Assigned, 1 PDY, 1 TDY, 1 Leave & Pass, 1 Unaccounted', tree[i + 2]
    assert tree[i + 3] == 'HQ: 1 Assigned, 1 PDY', tree[i + 3]
    assert tree[i + 4].startswith('TOTAL: 9 Assigned, 3 PDY'), tree[i + 4]
    assert tree[i + 4].endswith('1 Unaccounted'), tree[i + 4]

    split = out['split'].split('\n')
    assert 'Leave 1' in split and 'Pass 1' in split and 'Leave & Pass 2' not in split, split

    no_names = out['noNames'].split('\n')
    pdy = no_names.index('PDY 3')
    assert no_names[pdy + 1] == '', f'present names listed with the option off: {no_names[pdy:pdy + 3]}'

    assert out['strengthFlat'].split('\n') == [
        'HHC', '17 Mar 2026', '',
        '9 Assigned, 3 PDY, 1 TDY, 2 Leave & Pass, 1 Late, 1 FTR, 1 Unaccounted'], out['strengthFlat']
    st = out['strengthTree'].split('\n')
    assert st[3].startswith('1st PLT: ') and st[5] == 'HQ: 1 Assigned, 1 PDY' and st[6].startswith('TOTAL: 9'), st

    # A big unit leaves PDY names off by default; an explicit "on" lists them.
    big = [soldier(100 + n, 'SPC', f'Z{n:02d}') for n in range(20)]
    out = run([['auto', detailed, []], ['on', {**detailed, 'listPresent': True}, []]], people=big)
    assert 'Z00' not in out['auto'] and 'PDY 20' in out['auto'], out['auto']
    assert 'Z00' in out['on'], out['on']

    # An unknown status is counted as Other so the categories still add up.
    odd = [soldier(1, 'SPC', 'Odd', 'quarters')]
    out = run([['odd', detailed, []]], people=odd)
    assert 'OTHER 1' in out['odd'].split('\n'), out['odd']
    print('ok')


if __name__ == '__main__':
    main()
