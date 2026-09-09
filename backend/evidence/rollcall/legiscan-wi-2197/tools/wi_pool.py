"""Measure the Wisconsin divided / enacted roll-call pool straight from the dataset."""
import json
import os
import sys
import re
from collections import Counter, defaultdict

BASE = sys.argv[1] if len(sys.argv) > 1 else '/Users/shu/legiscan-data/wi-2197/WI/2025-2026_Regular_Session'

bills = {}
for root, _dirs, files in os.walk(os.path.join(BASE, 'bill')):
    for name in files:
        if name.endswith('.json'):
            b = json.load(open(os.path.join(root, name)))['bill']
            bills[b['bill_id']] = b

votes = []
for root, _dirs, files in os.walk(os.path.join(BASE, 'vote')):
    for name in files:
        if name.endswith('.json'):
            votes.append(json.load(open(os.path.join(root, name)))['roll_call'])

print('bills', len(bills), 'rolls', len(votes))

PASSAGE = re.compile(
    r'^(assembly|senate):\s*(read a third time and (passed|concurred in|adopted)'
    r'(\s+as amended)?|concurred in(\s+as amended)?|adopted)$', re.I)


def divided(v):
    hi, lo = max(v['yea'], v['nay']), min(v['yea'], v['nay'])
    return hi > 0 and lo >= hi / 4.0


status_names = {1: 'introduced', 2: 'engrossed', 3: 'enrolled', 4: 'passed/enacted',
                5: 'vetoed', 6: 'failed'}

rows = []
for v in votes:
    b = bills.get(v['bill_id'])
    if b is None:
        continue
    rows.append({
        'roll': v['roll_call_id'], 'bill': b['bill_number'], 'type': b['bill_type'],
        'status': b['status'], 'date': v['date'], 'desc': v['desc'],
        'yea': v['yea'], 'nay': v['nay'], 'passed': v['passed'],
        'chamber': v['chamber'], 'title': b.get('title', ''),
        'divided': divided(v), 'passage': bool(PASSAGE.match(v['desc'].strip())),
    })

print('\n-- bill types of all rolls --', Counter(r['type'] for r in rows))
passage = [r for r in rows if r['passage']]
print('passage-caption rolls', len(passage), Counter(r['type'] for r in passage))

for label, sel in [('all rolls', rows), ('passage rolls', passage)]:
    d = [r for r in sel if r['divided']]
    de = [r for r in d if r['status'] == 4]
    print(f"\n{label}: divided {len(d)}; divided AND enacted {len(de)} "
          f"on {len(set(r['bill'] for r in de))} measures")
    print('  divided by status:', Counter(status_names.get(r['status'], r['status']) for r in d))
    print('  divided+enacted by chamber:', Counter(r['chamber'] for r in de))
    print('  divided+enacted by type:', Counter(r['type'] for r in de))

de = [r for r in passage if r['divided'] and r['status'] == 4]
de.sort(key=lambda r: (r['bill'], r['date']))
print('\n-- divided AND enacted passage rolls (bill | chamber | date | tally | passed | desc | title) --')
for r in de:
    print(f"{r['bill']:<9} {r['chamber']:<8} {r['date']} {r['yea']:>3}-{r['nay']:<3} p={r['passed']} "
          f"| {r['desc'][:58]:<58} | {r['title'][:70]}")

# Optional second argument: a path to dump every roll as JSON for later steps.
if len(sys.argv) > 2:
    json.dump(rows, open(sys.argv[2], 'w'), indent=1)
    print('\nwrote', sys.argv[2])
