"""Version check for the Wisconsin enacted worklist.

For each kept roll, list any action AFTER it that changed the bill's text. A
chamber that voted before a later change did not vote the text that became law,
and its roll can only be used if that change is immaterial — which has to be read,
not assumed.
"""
import json
import os
import re

BASE = '/Users/shu/legiscan-data/wi-2197/WI/2025-2026_Regular_Session'
SLOTS = [
    ('AB1034', 'S', '2026-03-17'), ('AB180', 'A', '2026-02-19'), ('AB180', 'S', '2026-03-17'),
    ('AB2', 'A', '2025-02-19'), ('AB223', 'S', '2026-02-11'), ('AB320', 'A', '2026-01-22'),
    ('AB35', 'S', '2025-10-14'), ('AB446', 'A', '2026-02-17'), ('AB453', 'A', '2025-10-07'),
    ('AB592', 'A', '2025-11-19'), ('AB601', 'S', '2026-03-17'), ('AB737', 'A', '2026-02-17'),
    ('AB75', 'A', '2025-03-13'), ('AB89', 'A', '2025-03-13'), ('SB106', 'S', '2025-06-18'),
    ('SB108', 'S', '2025-06-18'), ('SB11', 'A', '2025-11-19'), ('SB182', 'S', '2025-06-18'),
    ('SB279', 'S', '2025-06-18'), ('SB283', 'S', '2025-06-18'), ('SB485', 'A', '2026-02-17'),
    ('SB56', 'S', '2025-05-15'), ('SB785', 'A', '2026-02-18'), ('SB825', 'S', '2026-02-18'),
]

bills = {}
for root, _d, files in os.walk(os.path.join(BASE, 'bill')):
    for n in files:
        if n.endswith('.json'):
            b = json.load(open(os.path.join(root, n)))['bill']
            bills[b['bill_number']] = b

# Actions that change the text of the bill.
CHANGES = re.compile(
    r'(substitute amendment .*? adopted|amendment .*? adopted|amendment .*? concurred in'
    r'|concurred in as amended)', re.I)

clean, dirty = [], []
for number, chamber, date in SLOTS:
    b = bills[number]
    later = [h for h in b['history'] if h['date'] > date and CHANGES.search(h['action'])]
    if later:
        dirty.append((number, chamber, date, later))
    else:
        clean.append((number, chamber, date))

print(f'{len(clean)} of {len(SLOTS)} slots voted the final text with nothing changed afterwards\n')
print('-- slots where the text changed AFTER the vote: read before using --')
for number, chamber, date, later in dirty:
    print(f"\n{number} {'Assembly' if chamber == 'A' else 'Senate'} {date}")
    for h in later:
        print(f"   {h['date']} {h['chamber']} | {h['action'][:120]}")

print('\n-- clean slots --')
print('  ' + ', '.join(f'{n} {c}' for n, c, _d in clean))
