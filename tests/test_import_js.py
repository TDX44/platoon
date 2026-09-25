"""The spreadsheet half of the alpha-roster import, under node.

parseDelimited() / guessImportColumn() / importTableMode() / mapImportRows()
are lifted out of index.html and run here; everything a row MEANS is the
server's (tests/test_import.py). Also checks that every profile column the
mapping offers is one POST /api/personnel/import accepts.

Run with: python tests/test_import_js.py
"""
import ast
import json
import os
import re
import shutil
import subprocess
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX = os.path.join(ROOT, 'index.html')


def extract(source, pattern, what):
    m = re.search(pattern, source, re.S)
    assert m, f'could not find {what} in index.html'
    return m.group(0)


DRIVER = r'''
const out = {};
out.csv = parseDelimited('﻿Rank,Name,Unit\r\nSGT,"Smith, John A",1st Squad\n\n PFC ,"Doe, ""JJ"" Jane",\n');
out.tsv = parseDelimited('Rank\tLast\tFirst\nSPC\tRoe\tRick');
out.guess = ['Rank', 'LAST NAME', 'First_Name', 'Name', 'Section', 'Cell Phone', 'E-mail', 'DOD ID', 'EDIPI', 'MOS', 'Shoe size', '']
  .map(guessImportColumn);
out.tableCsv = importTableMode(out.csv);
out.tableLine = importTableMode(parseDelimited('CW2 Smith, John\nWO1 Doe, Jane'));
out.tableEmpty = importTableMode([]);
out.mapped = mapImportRows(out.csv, out.csv[0].map(guessImportColumn));
out.mappedNoComma = mapImportRows([['Name', 'Rank'], ['SMITH JOHN A', 'SGT'], ['', '']], ['name', 'rank']);
out.ignored = mapImportRows([['a', 'b'], ['x', 'y']], ['', 'phone']);
out.profileKeys = IMPORT_TARGETS.map(t => t[0]).filter(k => k && !['rank', 'last', 'first', 'name', 'unit'].includes(k));
console.log(JSON.stringify(out));
'''


def main():
    node = shutil.which('node')
    assert node, 'node is required for this test'
    src = open(INDEX, encoding='utf-8').read()
    parts = [
        extract(src, r'const IMPORT_TARGETS = \[.*?\n\];', 'IMPORT_TARGETS'),
        extract(src, r'const IMPORT_HEADER_ALIASES = \{.*?\n\};', 'IMPORT_HEADER_ALIASES'),
        extract(src, r'function parseDelimited\(text\) \{.*?\n\}', 'parseDelimited()'),
        extract(src, r'function guessImportColumn\(header\) \{.*?\n\}', 'guessImportColumn()'),
        extract(src, r'function importTableMode\(table\) \{.*?\n\}', 'importTableMode()'),
        extract(src, r'function mapImportRows\(table, mapping\) \{.*?\n\}', 'mapImportRows()'),
        DRIVER,
    ]
    with tempfile.NamedTemporaryFile('w', suffix='.js', delete=False) as fh:
        fh.write('\n'.join(parts))
        path = fh.name
    try:
        out = json.loads(subprocess.run([node, path], capture_output=True, text=True, check=True).stdout)
    finally:
        os.unlink(path)

    assert out['csv'] == [['Rank', 'Name', 'Unit'], ['SGT', 'Smith, John A', '1st Squad'],
                          [' PFC ', 'Doe, "JJ" Jane', '']], out['csv']
    assert out['tsv'] == [['Rank', 'Last', 'First'], ['SPC', 'Roe', 'Rick']]
    assert out['guess'] == ['rank', 'last', 'first', 'name', 'unit', 'phone', 'email', 'dod_id', 'dod_id', 'mos', '', ''], out['guess']
    assert out['tableCsv'] is True and out['tableLine'] is False and out['tableEmpty'] is False
    assert out['mapped'] == [
        {'rank': 'SGT', 'last': 'Smith', 'first': 'John A', 'unit': '1st Squad'},
        {'rank': 'PFC', 'last': 'Doe', 'first': '"JJ" Jane'},
    ], out['mapped']
    assert out['mappedNoComma'] == [{'last': 'SMITH', 'first': 'JOHN A', 'rank': 'SGT'}], out['mappedNoComma']
    assert out['ignored'] == [{'phone': 'y'}]

    server_src = open(os.path.join(ROOT, 'server.py'), encoding='utf-8').read()
    m = re.search(r'IMPORT_PROFILE_FIELDS = (\(.*?\))\n', server_src, re.S)
    assert m, 'IMPORT_PROFILE_FIELDS not found in server.py'
    server_fields = set(ast.literal_eval(m.group(1)))
    assert set(out['profileKeys']) == server_fields, set(out['profileKeys']) ^ server_fields
    print('ok')


if __name__ == '__main__':
    main()
