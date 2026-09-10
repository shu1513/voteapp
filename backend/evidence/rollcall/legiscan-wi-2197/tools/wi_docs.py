"""Fetch a Wisconsin bill document through the LegiScan bulk API and verify it.

Usage:
  wi_docs.py list AB180
  wi_docs.py get AB180 Enrolled
  wi_docs.py text AB180 Enrolled      (plain pdftotext extract)

The API is used rather than docs.legis.wisconsin.gov because the campaign has
been burned twice by state sites timing out mid-run, and because the API lets the
download be checked against the dataset's own text_size and text_hash.
"""
import base64
import glob
import hashlib
import json
import os
import re
import subprocess
import sys
import urllib.request

BASE = '/Users/shu/legiscan-data/wi-2197/WI/2025-2026_Regular_Session'
CACHE = '/Users/shu/legiscan-data/wi-docs'
os.makedirs(CACHE, exist_ok=True)

_key = None


def key():
    global _key
    if _key is None:
        env = open('/Users/shu/voteApp/backend/.env').read()
        _key = re.search(r'^LEGISCAN_API_KEY=(.+)$', env, re.M).group(1).strip()
    return _key


def bill(number):
    for p in glob.glob(BASE + '/bill/*.json'):
        b = json.load(open(p))['bill']
        if b['bill_number'] == number:
            return b
    raise SystemExit(f'no bill {number}')


def fetch(doc_id, expect_size=None, expect_hash=None):
    path = os.path.join(CACHE, f'{doc_id}.pdf')
    if not os.path.exists(path):
        url = f'https://api.legiscan.com/?key={key()}&op=getBillText&id={doc_id}'
        payload = json.load(urllib.request.urlopen(url, timeout=120))
        if payload.get('status') != 'OK':
            raise SystemExit(f'getBillText {doc_id} failed: {payload.get("alert")}')
        text = payload['text']
        blob = base64.b64decode(text['doc'])
        if expect_size is not None and len(blob) != expect_size:
            raise SystemExit(f'{doc_id}: got {len(blob)} bytes, dataset says {expect_size}')
        if expect_hash is not None and hashlib.md5(blob).hexdigest() != expect_hash:
            raise SystemExit(f'{doc_id}: md5 disagrees with the dataset')
        open(path, 'wb').write(blob)
    return path


def main():
    cmd, number = sys.argv[1], sys.argv[2]
    b = bill(number)
    if cmd == 'list':
        print(f"{b['bill_number']}  status {b['status']}  {b['title'][:90]}")
        for t in b.get('texts', []):
            print(f"   {t['type']:<12} date {t['date']}  doc {t['doc_id']}  "
                  f"{t.get('mime')}  size {t.get('text_size')}  hash {t.get('text_hash')}")
        return
    want = sys.argv[3] if len(sys.argv) > 3 else 'Enrolled'
    hit = [t for t in b['texts'] if t['type'] == want]
    if not hit:
        raise SystemExit(f'{number} has no {want} text; it has '
                         f"{[t['type'] for t in b['texts']]}")
    t = hit[-1]
    path = fetch(t['doc_id'], t.get('text_size'), t.get('text_hash'))
    if cmd == 'get':
        print(path)
        return
    out = subprocess.run(['pdftotext', '-layout', path, '-'], capture_output=True, text=True)
    sys.stdout.write(out.stdout)


if __name__ == '__main__':
    main()
