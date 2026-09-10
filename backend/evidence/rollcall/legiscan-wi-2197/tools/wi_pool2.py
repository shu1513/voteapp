"""Wisconsin worklist for every scope, built from the DATABASE.

The rolls come from legislative_votes, which the fetcher already classified with
the shipped config. That is deliberate: a second copy of the patterns in Python
would drift from the config, and it did once already.

Outcome is read from the bill history, not from the status field alone, because
LegiScan marks an adopted joint resolution status 4 and a joint resolution is not
law.

Filter 4 keeps each chamber's LAST kept floor roll.
"""
import json
import os
import re
import subprocess
import sys

BASE = '/Users/shu/legiscan-data/wi-2197/WI/2025-2026_Regular_Session'
DB = 'postgresql://localhost:5432/voteapp'
OUT = os.path.dirname(os.path.abspath(__file__))

bills = {}
for root, _d, files in os.walk(os.path.join(BASE, 'bill')):
    for n in files:
        if n.endswith('.json'):
            b = json.load(open(os.path.join(root, n)))['bill']
            bills[b['bill_number']] = b

sql = """
select roll_number, chamber, measure_id, vote_date, yeas, nays, exact_question, is_floor_vote
from legislative_votes where jurisdiction='WI' and is_floor_vote is true;
"""
raw = subprocess.run(['psql', DB, '-tAF|', '-c', sql], capture_output=True, text=True).stdout

rolls = []
for line in raw.strip().split('\n'):
    if line.count('|') < 7:
        continue
    roll, chamber, measure, date, yeas, nays, q, _f = line.split('|', 7)
    rolls.append({'roll': int(roll), 'chamber': chamber, 'measure': measure.replace(' ', ''),
                  'date': date, 'yea': int(yeas), 'nay': int(nays), 'q': q})

print(f'floor rolls in the database: {len(rolls)}')


def divided(r):
    hi, lo = max(r['yea'], r['nay']), min(r['yea'], r['nay'])
    return hi > 0 and lo >= hi / 4.0


PARTIAL = re.compile(r'approved by the governor with partial veto', re.I)
APPROVED = re.compile(r'approved by the governor', re.I)
VETOED = re.compile(r'vetoed by the governor', re.I)
OVERRIDE_FAILED = re.compile(r'failed to pass notwithstanding', re.I)


def outcome(b):
    """What actually happened to the measure, read from its own history."""
    acts = [h['action'] for h in b['history']]
    joined = ' ; '.join(acts)
    if any(APPROVED.search(a) for a in acts):
        return 'partial-veto' if PARTIAL.search(joined) else 'law'
    if any(VETOED.search(a) for a in acts):
        return 'vetoed'
    if b['bill_type'] in ('JR', 'R'):
        return 'resolution'
    return 'died'


def act_number(b):
    for h in b['history']:
        m = re.search(r'(20\d\d Wisconsin Act \d+)', h['action'])
        if m:
            return m.group(1)
    return None


by_measure = {}
for r in rolls:
    by_measure.setdefault((r['measure'], r['chamber']), []).append(r)

work = []
for (measure, chamber), group in by_measure.items():
    b = bills.get(measure)
    if b is None:
        print('  !! no bill for', measure)
        continue
    group.sort(key=lambda r: (r['date'], r['roll']))
    last = group[-1]
    earlier_divided = [r for r in group[:-1] if divided(r)]
    if not divided(last):
        if earlier_divided:
            work.append({**last, 'bill': measure, 'outcome': outcome(b), 'act': act_number(b),
                         'title': b['title'], 'disposition': 'filter-4: the chamber\'s last vote is not divided'})
        continue
    work.append({**last, 'bill': measure, 'outcome': outcome(b), 'act': act_number(b),
                 'title': b['title'], 'disposition': 'candidate',
                 'supersededDivided': [r['roll'] for r in earlier_divided]})

cand = [w for w in work if w['disposition'] == 'candidate']
from collections import Counter
print('\ncandidate slots by outcome:', dict(Counter(w['outcome'] for w in cand)))
print('measures by outcome:')
for o in ('law', 'partial-veto', 'vetoed', 'died', 'resolution'):
    ms = {w['bill'] for w in cand if w['outcome'] == o}
    if ms:
        print(f"   {o:<14} {len([w for w in cand if w['outcome']==o]):>3} slots / {len(ms):>3} measures")
print('dropped by filter 4:', len([w for w in work if w['disposition'].startswith('filter-4')]))

json.dump(sorted(work, key=lambda w: (w['bill'], w['chamber'])),
          open(os.path.join(OUT, 'wi_worklist.json'), 'w'), indent=1)
print('\nwrote', os.path.join(OUT, 'wi_worklist.json'))
