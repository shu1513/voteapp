"""Audit every Wisconsin roll call against the tally Wisconsin prints in its own
bill history. Not bounded by the divided gate: a wrong tally can itself decide
whether a roll looks divided (the Oregon SB 1565 lesson).

A comparison is only made when a history line on the same date and chamber
carries the SAME question wording as the roll. Anything else is reported as
unmatched rather than counted as a defect — matching a roll against a different
question's tally manufactures false failures.

Also reports how each enacted bill became law, so a partial veto cannot pass
unnoticed. Wisconsin's governor has a partial veto on appropriation bills.
"""
import json
import os
import re
import sys
from collections import Counter

# The extracted LegiScan dataset directory. Pass a different one as the first
# argument; the default is where this campaign kept it.
BASE = sys.argv[1] if len(sys.argv) > 1 else '/Users/shu/legiscan-data/wi-2197/WI/2025-2026_Regular_Session'

bills = {}
for root, _d, files in os.walk(os.path.join(BASE, 'bill')):
    for n in files:
        if n.endswith('.json'):
            b = json.load(open(os.path.join(root, n)))['bill']
            bills[b['bill_id']] = b

votes = []
for root, _d, files in os.walk(os.path.join(BASE, 'vote')):
    for n in files:
        if n.endswith('.json'):
            votes.append(json.load(open(os.path.join(root, n)))['roll_call'])

# Wisconsin's history line can end with ', Paired N'. A pair is two members on
# opposite sides who agree to both withhold their votes, so the pair is NOT part
# of the Ayes/Noes counts and LegiScan files those members as not voting.
TALLY = re.compile(r',\s*Ayes\s+(\d+),\s*Noes\s+(\d+)(?:,\s*Paired\s+(\d+))?\s*$')


def caption(desc):
    """Strip the 'Assembly: ' / 'Senate: ' prefix; history omits it."""
    return re.sub(r'^(assembly|senate):\s*', '', desc.strip(), flags=re.I).lower()


exact, mismatch, unmatched, ambiguous = 0, 0, 0, 0
bad = []
unmatched_rows = []

for v in votes:
    b = bills.get(v['bill_id'])
    if b is None:
        continue
    cap = caption(v['desc'])
    lines = []
    for h in b['history']:
        if h['date'] != v['date'] or h['chamber'] != v['chamber']:
            continue
        m = TALLY.search(h['action'])
        if m and TALLY.sub('', h['action']).strip().lower() == cap:
            lines.append((int(m.group(1)), int(m.group(2))))
    if not lines:
        unmatched += 1
        unmatched_rows.append((b['bill_number'], v['roll_call_id'], v['date'], v['desc'],
                               f"{v['yea']}-{v['nay']}"))
        continue
    if len(set(lines)) > 1:
        # The chamber voted the same question twice that day. Only a defect if
        # the feed's tally matches neither.
        if (v['yea'], v['nay']) in lines:
            exact += 1
        else:
            ambiguous += 1
            bad.append(('REPEATED-QUESTION', b['bill_number'], v['roll_call_id'], v['date'],
                        v['desc'], f"feed {v['yea']}-{v['nay']}", f'history {lines}'))
        continue
    ayes, noes = lines[0]
    if (ayes, noes) == (v['yea'], v['nay']):
        exact += 1
    else:
        mismatch += 1
        bad.append(('TALLY-MISMATCH', b['bill_number'], v['roll_call_id'], v['date'], v['desc'],
                    f"feed {v['yea']}-{v['nay']}", f'history {ayes}-{noes}',
                    f"nv {v['nv']} absent {v['absent']} total {v['total']}",
                    f"status {b['status']}", v.get('state_link', '')))

print(f'rolls audited {len(votes)} | exact {exact} | tally mismatch {mismatch} '
      f'| repeated question unresolved {ambiguous} | no same-question history line {unmatched}')

print('\n-- failures --')
for row in bad:
    print('  ', ' | '.join(str(x) for x in row))

print(f'\n-- rolls with no same-question history line ({unmatched}) --')
for row in unmatched_rows:
    print('  ', ' | '.join(str(x) for x in row))

print('\n-- how the enacted bills became law --')
kinds = Counter()
for b in bills.values():
    if b['status'] != 4:
        continue
    joined = ' ; '.join(h['action'] for h in b['history']
                        if re.search(r'approved|vetoe?d|act \d+', h['action'], re.I))
    # Wisconsin's wording is `approved by the Governor with partial veto`; the
    # other spellings are what a check written from another state would try.
    if re.search(r'with partial veto|partial(ly)?\s+vetoe?d|vetoed in part|approved in part', joined, re.I):
        kinds['partial veto'] += 1
        print('  PARTIAL VETO', b['bill_number'], '|', joined[:220])
    elif re.search(r'approved by the governor', joined, re.I):
        kinds['approved whole'] += 1
    elif joined:
        kinds['other wording'] += 1
        print('  OTHER', b['bill_number'], '|', joined[:220])
    else:
        kinds['no approval line'] += 1
print(' ', dict(kinds))
