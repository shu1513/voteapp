"""Audits every divided-and-enacted 2026 Oregon floor roll's LegiScan tally
against Oregon's own bill-history line for the same action and date."""
import json, glob, re, collections, sys

BD = '/Users/shu/legiscan-data/or-2252/OR/2026-2026_Regular_Session'
bills = {}
for p in glob.glob(BD + '/bill/*.json'):
    b = json.load(open(p))['bill']
    bills[b['bill_id']] = b
votes = [json.load(open(p))['roll_call'] for p in glob.glob(BD + '/vote/*.json')]

FLOOR = re.compile(r'^(House|Senate) Third Reading( in Concurrence)?$')
AYES = re.compile(r'Ayes,\s*(\d+)')
NAYS = re.compile(r'Nays,\s*(\d+)')
div = lambda y, n: (y + n) > 0 and min(y, n) >= max(y, n) / 4.0

ok = 0; mismatch = []; nomatch = []
for v in votes:
    b = bills.get(v['bill_id'])
    if b is None or b['status'] != 4 or b['bill_type'] != 'B':
        continue
    if not FLOOR.match(v['desc']) or not div(v['yea'], v['nay']):
        continue
    ch = 'H' if v['chamber'] == 'H' else 'S'
    cands = []
    for h in b.get('history', []):
        if h['date'] != v['date'] or h.get('chamber') != ch:
            continue
        a = AYES.search(h['action'])
        if not a:
            continue
        n = NAYS.search(h['action'])
        cands.append((int(a.group(1)), int(n.group(1)) if n else 0, h['action'][:80]))
    if not cands:
        nomatch.append((b['bill_number'], ch, v['date'], v['roll_call_id'], v['yea'], v['nay']))
    elif any(c[0] == v['yea'] and c[1] == v['nay'] for c in cands):
        ok += 1
    else:
        mismatch.append((b['bill_number'], ch, v['date'], v['roll_call_id'],
                         '%d-%d' % (v['yea'], v['nay']),
                         ' ; '.join('%d-%d [%s]' % c for c in cands)))

print('divided-and-enacted rolls audited: %d' % (ok + len(mismatch) + len(nomatch)))
print('  exact match to the bill history: %d' % ok)
print('  MISMATCH: %d' % len(mismatch))
for m in mismatch:
    print('     %-8s %s %s roll %s  legiscan %-7s  history %s' % m)
print('  no history line with a tally on that date: %d' % len(nomatch))
for m in nomatch:
    print('     %-8s %s %s roll %s %d-%d' % m)
