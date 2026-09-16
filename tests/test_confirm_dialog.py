"""Every destructive action asks through one modal, never the browser's confirm().

confirmDialog()/settleConfirm() in index.html are lifted out and run under
node against a stub document, so this fails if the promise stops resolving,
the modal stops opening/closing, body text starts being interpreted as HTML,
or a native confirm() creeps back in.

Run with: python tests/test_confirm_dialog.py
"""
import json
import os
import re
import shutil
import subprocess
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX = os.path.join(ROOT, 'index.html')

STUB_DOM = r'''
const els = {};
function el(id) {
  if (!els[id]) els[id] = {
    id, textContent: '', innerHTML: '', children: [], focused: false,
    classes: new Set(),
    classList: { add(c) { el(id).classes.add(c); }, remove(c) { el(id).classes.delete(c); },
                 contains(c) { return el(id).classes.has(c); } },
    appendChild(n) { this.children.push(n); },
    focus() { this.focused = true; },
  };
  return els[id];
}
const document = {
  getElementById: el,
  createElement: () => ({ className: '', textContent: '' }),
};
'''

DRIVER = r'''
(async () => {
  const modal = el('confirmModal'), body = el('confirmBody'), ok = el('confirmOkBtn');
  const out = {};

  // 1. OK resolves true, modal opened then closed, labels applied.
  let p = confirmDialog('Remove this?', 'It goes away.', 'Remove');
  out.openedOnAsk = modal.classList.contains('active');
  out.title = el('confirmTitle').textContent;
  out.okLabel = ok.textContent;
  out.okFocused = ok.focused;
  out.bodyIsTextNode = body.children.length === 1 && body.children[0].textContent === 'It goes away.';
  settleConfirm(true);
  out.okResolves = await p;
  out.closedAfterOk = !modal.classList.contains('active');

  // 2. Cancel resolves false.
  p = confirmDialog('Sure?');
  settleConfirm(false);
  out.cancelResolves = await p;

  // 3. Body text is never parsed as HTML unless the caller opts in.
  body.innerHTML = ''; body.children = [];
  p = confirmDialog('x', '<img src=x onerror=alert(1)>');
  out.textBodyNotHtml = body.innerHTML === '' && body.children[0].textContent.includes('<img');
  settleConfirm(false); await p;
  p = confirmDialog('x', '<b>ok</b>', 'Yes', { html: true });
  out.htmlOptIn = body.innerHTML === '<b>ok</b>';
  settleConfirm(false); await p;

  // 4. Settling with nothing pending is harmless.
  settleConfirm(true);
  out.idleSettleOk = true;
  console.log(JSON.stringify(out));
})();
'''


def extract(source, pattern, what):
    m = re.search(pattern, source, re.S)
    assert m, f'could not find {what} in index.html — was it renamed or removed?'
    return m.group(0)


def main():
    src = open(INDEX, encoding='utf-8').read()

    # No native confirm() anywhere in executable code (comments may mention it).
    code_only = re.sub(r'//[^\n]*|<!--.*?-->', '', src, flags=re.S)
    native = re.findall(r'(?<![\w.])confirm\(', code_only)
    assert not native, f'{len(native)} native confirm() call(s) are back in index.html'

    sites = src.count('await confirmDialog(')
    assert sites >= 9, f'expected the 8 former confirm() sites plus the list page, found {sites}'

    node = shutil.which('node')
    assert node, 'node is required to run the frontend rule (it ships with the CI image)'
    js = '\n'.join([
        STUB_DOM,
        extract(src, r'let confirmResolve = null;', 'confirmResolve'),
        extract(src, r'function confirmDialog\(.*?\n\}', 'confirmDialog()'),
        extract(src, r'function settleConfirm\(.*?\n\}', 'settleConfirm()'),
        DRIVER,
    ])
    path = os.path.join(tempfile.mkdtemp(), 'confirm.js')
    with open(path, 'w', encoding='utf-8') as fh:
        fh.write(js)
    proc = subprocess.run([node, path], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    out = json.loads(proc.stdout)

    assert out['openedOnAsk'], 'modal did not open'
    assert out['title'] == 'Remove this?'
    assert out['okLabel'] == 'Remove'
    assert out['okFocused'], 'OK button not focused (Enter must confirm)'
    assert out['bodyIsTextNode'], 'body text not rendered'
    assert out['okResolves'] is True, 'OK must resolve true'
    assert out['closedAfterOk'], 'modal stayed open after OK'
    assert out['cancelResolves'] is False, 'Cancel must resolve false'
    assert out['textBodyNotHtml'], 'body string was parsed as HTML — XSS via a soldier name'
    assert out['htmlOptIn'], '{html: true} must allow pre-escaped markup'
    print('ok')


if __name__ == '__main__':
    main()
