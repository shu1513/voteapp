"""Build the Wisconsin enacted-scope worklist and apply filter 4.

Filter 4 keeps one roll per measure per chamber: that chamber's LAST kept floor
vote, which is the one cast on the text that became law. The bill history is
printed alongside so the version question can be answered by eye.
"""
import json
import os
import re
import sys

BASE = '/Users/shu/legiscan-data/wi-2197/WI/2025-2026_Regular_Session'
HELD = {1609619, 1664169, 1597302, 1615082, 1664398}

KEPT = re.compile(
    r'^(assembly|senate): (read a third time and (passed|concurred in|adopted)( as amended)?'
    r'|adopted|concurred in( as amended)?)$', re.I)

bills = {}
for root, _d, files in os.walk(os.path.join(BASE, 'bill')):
    for n in files:
        if n.endswith('.json'):
            b = json.load(open(os.path.join(root, n)))['bill']
            bills[b['bill_id']] = b


def divided(y, n):
    hi, lo = max(y, n), min(y, n)
    return hi > 0 and lo >= hi / 4.0


def became_law(b):
    """Status 4 is not enough: a resolution is 'adopted' and is not law."""
    return b['status'] == 4 and any(
        re.search(r'approved by the governor', h['action'], re.I) for h in b['history'])


def act_line(b):
    for h in b['history']:
        m = re.search(r'(20\d\d Wisconsin Act \d+)', h['action'])
        if m:
            partial = 'with partial veto' in h['action'].lower()
            return m.group(1), partial, h['date']
    return None, False, None


rows = []
for b in bills.values():
    if not became_law(b):
        continue
    kept = [v for v in b.get('votes', []) if KEPT.match(v['desc'].strip())]
    if not kept:
        continue
    for chamber in ('A', 'S'):
        cham = [v for v in kept if v['chamber'] == chamber]
        if not cham:
            continue
        cham.sort(key=lambda v: (v['date'], v['roll_call_id']))
        last = cham[-1]
        earlier_divided = [v for v in cham[:-1] if divided(v['yea'], v['nay'])]
        if not divided(last['yea'], last['nay']):
            if earlier_divided:
                rows.append((b, chamber, last, 'FILTER-4 DROP: the chamber\'s last vote is not divided, '
                             f'but an earlier one was ({earlier_divided[-1]["yea"]}-{earlier_divided[-1]["nay"]})'))
            continue
        note = 'held roll' if last['roll_call_id'] in HELD else 'keep'
        rows.append((b, chamber, last, note))

keeps = [r for r in rows if r[3] == 'keep']
print(f'enacted measures that became law with a kept floor roll: '
      f"{len({b['bill_number'] for b, *_ in rows})}")
print(f'chamber slots after filter 4: {len(keeps)} keep, '
      f"{len([r for r in rows if r[3].startswith('FILTER-4')])} dropped by filter 4, "
      f"{len([r for r in rows if r[3] == 'held roll'])} held")
print(f"measures in the keep set: {len({b['bill_number'] for b, _c, _v, n in rows if n == 'keep'})}")

print('\n' + '=' * 100)
for b, chamber, v, note in sorted(rows, key=lambda r: (r[0]['bill_number'], r[1])):
    act, partial, adate = act_line(b)
    print(f"\n{b['bill_number']}  [{note}]  {'Assembly' if chamber == 'A' else 'Senate'} "
          f"{v['date']} {v['yea']}-{v['nay']} roll {v['roll_call_id']}")
    print(f"   act: {act}{'  ** PARTIAL VETO **' if partial else ''}  ({adate})")
    print(f"   title: {b['title'][:150]}")
    print(f"   desc:  {b.get('description', '')[:220]}")
    print(f"   texts: {[(t['type'], t['date'], t['doc_id']) for t in b.get('texts', [])]}")
    amend = [a['title'] for a in b.get('amendments', [])]
    if amend:
        print(f"   amendments filed: {amend}")
    print('   floor history:')
    for h in b['history']:
        if re.search(r'Ayes \d+|substitute amendment .* adopted|concurred|passed|vetoed|approved', h['action'], re.I):
            print(f"      {h['date']} {h['chamber']} | {h['action'][:120]}")
