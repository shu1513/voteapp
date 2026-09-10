"""Find the crosswalk entries the automatic proposer cannot reach.

The proposer matches on LegiScan's `first_name`. Wisconsin's roster carries the
LEGAL first name there and the working name in `nickname`, so a member whose
`name` field is byte-identical to our candidate is still missed. This sweep
retries every unmatched member on the `name` and `nickname` fields, and only
inside the seat the member actually holds.
"""
import json
import os
import re
import subprocess

BASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '')
DB = 'postgresql://localhost:5432/voteapp'

report = json.load(open(BASE + 'crosswalk-proposal-report.json'))
people = {p['people_id']: p for p in json.load(open(BASE + 'legiscan-people-wi-2197.json'))['people']}

sql = """
select d.name, c.id, c.display_name
from elections e
join districts d on d.id = e.district_id
join offices o on o.id = e.office_id
join candidate_elections ce on ce.election_id = e.id
join candidates c on c.id = ce.candidate_id
where d.state = 'WI' and e.election_date = '2026-11-03'
  and o.canonical_name in ('State Senator', 'State Lower Chamber Legislator');
"""
raw = subprocess.run(['psql', DB, '-tAF|', '-c', sql], capture_output=True, text=True).stdout

by_seat = {}
for line in raw.strip().split('\n'):
    if line.count('|') < 2:
        continue
    district, cid, name = line.split('|', 2)
    m = re.search(r'(Assembly District|State Senate District)\s+(\d+)', district)
    if m:
        key = ('house' if m.group(1).startswith('Assembly') else 'senate', int(m.group(2)))
        by_seat.setdefault(key, []).append((cid, name))


def norm(s):
    return re.sub(r'[^a-z ]', '', s.lower()).split()


def surname(tokens):
    return tokens[-1] if tokens else ''


print('-- unmatched members retried on `name` and `nickname`, inside their own seat --')
hits = []
for person in report['unmatchedPeople']:
    raw_person = people[person['peopleId']]
    m = re.match(r'(house|senate) [HS]D-(\d+)', person['seat'])
    seat = (m.group(1), int(m.group(2)))
    tries = {raw_person['name']}
    if raw_person.get('nickname'):
        tries.add(f"{raw_person['nickname']} {raw_person['last_name']}")
    for cid, cname in by_seat.get(seat, []):
        ct = norm(cname)
        for attempt in tries:
            at = norm(attempt)
            if not at or surname(at) != surname(ct):
                continue
            # first names equal, or one a prefix of the other
            if at[0] == ct[0] or at[0].startswith(ct[0]) or ct[0].startswith(at[0]):
                hits.append((person['peopleId'], raw_person['name'], person['seat'], cid, cname, 'exact-or-prefix'))
                break
        else:
            # a near miss on the surname is worth a human look, not an entry
            continue
        break

for h in hits:
    print(f'  {h[0]} {h[1]:<22} {h[2]:<14} -> {h[4]:<24} {h[3]}')

found = {h[0] for h in hits}
print(f'\n-- still unmatched after the retry ({len(report["unmatchedPeople"]) - len(hits)}) --')
for person in report['unmatchedPeople']:
    if person['peopleId'] in found:
        continue
    m = re.match(r'(house|senate) [HS]D-(\d+)', person['seat'])
    seat = (m.group(1), int(m.group(2)))
    running = [n for _c, n in by_seat.get(seat, [])]
    label = 'seat not on the 2026 ballot' if not running else 'on the ballot there: ' + ', '.join(running)
    print(f"  {person['name']:<24} {person['seat']:<14} {label}")
