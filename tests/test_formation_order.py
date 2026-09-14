"""Formation mode asks the right people, in the right order.

formationQueue() in index.html is the whole rule: who still needs accounting
for at 0630, who is already on the books, and who is not this platoon's to
count. It is deliberately pure so it can be run here — the function is lifted
straight out of index.html and executed with node, so this test fails if the
rule changes or the function is renamed away.

Run with: python tests/test_formation_order.py
"""
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX = os.path.join(ROOT, 'index.html')
TODAY = '2026-03-17'


def extract(source, pattern, what):
    m = re.search(pattern, source, re.S)
    assert m, f'could not find {what} in index.html — was it renamed or removed?'
    return m.group(0)


def harness():
    """The real functions, lifted out of index.html, plus a JSON entry point."""
    src = open(INDEX, encoding='utf-8').read()
    parts = [
        extract(src, r'const RANK_ORDER = \[.*?\];', 'RANK_ORDER'),
        extract(src, r'function rankSort\(a, b\) \{.*?\n\}', 'rankSort()'),
        extract(src, r'function formationQueue\(people, todayStr\) \{.*?\n\}', 'formationQueue()'),
    ]
    parts.append(
        'const input = JSON.parse(process.argv[2]);\n'
        'const out = formationQueue(input.people, input.today);\n'
        'console.log(JSON.stringify({queue: out.queue.map(p => p.id), '
        'known: out.known.map(p => p.id)}));'
    )
    return '\n'.join(parts)


def make_runner():
    node = shutil.which('node')
    assert node, 'node is required to run the frontend rule (it ships with the CI image)'
    path = os.path.join(tempfile.mkdtemp(), 'formation.js')
    with open(path, 'w', encoding='utf-8') as fh:
        fh.write(harness())

    def run(people, today=TODAY):
        proc = subprocess.run([node, path, json.dumps({'people': people, 'today': today})],
                              capture_output=True, text=True)
        assert proc.returncode == 0, proc.stderr
        return json.loads(proc.stdout)
    return run


def soldier(pid, rank='SPC', last='Zulu', status='present', present_date=''):
    return {'id': pid, 'rank': rank, 'last': last, 'first': 'A',
            'status': status, 'present_date': present_date}


def main():
    run = make_runner()

    # 1. Everyone unaccounted gets asked, senior first, then alphabetically.
    roster = [
        soldier(1, 'SPC', 'Alvarez'),
        soldier(2, 'SFC', 'Boone'),
        soldier(3, 'SGT', 'Crane'),
        soldier(4, 'SGT', 'Ash'),
    ]
    out = run(roster)
    assert out['queue'] == [2, 4, 3, 1], out['queue']
    assert out['known'] == [], out['known']

    # 2. Someone already marked present today is not asked again.
    out = run([soldier(1), soldier(2, present_date=TODAY)])
    assert out['queue'] == [1], out['queue']
    assert out['known'] == [], 'present-today is accounted for, not "on the books away"'

    # ...but yesterday's mark is stale and they are asked again.
    out = run([soldier(1, present_date='2026-03-16')])
    assert out['queue'] == [1], out['queue']

    # 3. A soldier already away on a current absence is not asked — the app
    #    knows where they are — but is handed back so the finish screen can
    #    show them and let a wrong one be corrected.
    out = run([soldier(1), soldier(2, status='tdy'), soldier(3, status='leave')])
    assert out['queue'] == [1], out['queue']
    assert out['known'] == [2, 3], out['known']

    # 5. Every away status lands in known, never in the queue.
    away = [soldier(i + 2, status=s) for i, s in enumerate(['tdy', 'leave', 'pass', 'other', 'ftr'])]
    out = run([soldier(1)] + away)
    assert out['queue'] == [1], out['queue']
    assert out['known'] == [2, 3, 4, 5, 6], out['known']

    # 6. A fully accounted-for platoon produces an empty queue rather than
    #    walking the user through 29 pointless taps.
    out = run([soldier(1, present_date=TODAY), soldier(2, status='tdy')])
    assert out['queue'] == [], out['queue']

    # 7. An empty roster is not a crash.
    assert run([]) == {'queue': [], 'known': []}

    # 8. The absence lifecycle stays the server's: formation books an absence
    #    through POST .../schedule, never by writing personnel.status itself.
    src = open(INDEX, encoding='utf-8').read()
    mark = extract(src, r'async function formationMark\(status\) \{.*?\n\}', 'formationMark()')
    assert "/schedule`" in mark, 'formation must book absences through POST /personnel/<id>/schedule'
    assert "apiUpdate(p)" in mark, 'marking present must go through PUT /api/personnel/<id>'

    print('ok')


if __name__ == '__main__':
    main()
