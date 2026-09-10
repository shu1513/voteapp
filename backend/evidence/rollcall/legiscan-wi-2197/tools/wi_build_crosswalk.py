"""Build the Wisconsin crosswalk file from the reviewed proposals plus the
hand-added members the automatic proposer cannot reach.

Every member in the people snapshot gets a row. A null is a decision, not a gap,
and each null carries the reason.
"""
import json
import os
import re
import subprocess

BASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '')
DB = 'postgresql://localhost:5432/voteapp'

report = json.load(open(BASE + 'crosswalk-proposal-report.json'))
people = {p['people_id']: p for p in json.load(open(BASE + 'legiscan-people-wi-2197.json'))['people']}

# Members whose `name` field is byte-identical (or near) to our candidate but
# whose LegiScan `first_name` holds the LEGAL name, so the proposer cannot
# reach them. Seat verified for each.
HAND_ADDS = {
    20179: ('e48faec2-2f58-495b-bd88-77f9bc49bb46',
            'hand-added: LegiScan first_name is the legal Jodene, nickname Jodi; name matches our candidate exactly and HD-091 agrees'),
    20183: ('9709978e-16ad-4d06-a66a-baae5e6aafee',
            'hand-added: LegiScan first_name is the legal Anthony, nickname Tony; name matches our candidate exactly and HD-041 agrees'),
    23588: ('a266d8c0-e9b4-428a-b531-4613546b85df',
            'hand-added: LegiScan first_name is the legal Robert, nickname Bob; name matches our candidate exactly and HD-061 agrees'),
    26090: ('50c911e9-fe5e-4b64-990c-1c40b51fecd0',
            'hand-added: LegiScan first_name is the legal Vincent, nickname Vinnie; name matches our candidate exactly and HD-071 agrees'),
    26093: ('736dc91f-e65f-4d0d-8008-dad17ae2c690',
            'hand-added: LegiScan spells the first name Pricilla, our roster and the state spell it Priscilla. Same surname, same seat HD-009, sitting member; the feed drops one letter'),
}

# Which Senate districts are on the 2026 ballot, read from the database rather
# than assumed.
sql = """
select d.name
from elections e join districts d on d.id=e.district_id join offices o on o.id=e.office_id
where d.state='WI' and e.election_date='2026-11-03' and o.canonical_name='State Senator';
"""
raw = subprocess.run(['psql', DB, '-tA', '-c', sql], capture_output=True, text=True, check=True).stdout
senate_up = {int(m.group(1)) for m in re.finditer(r'District\s+(\d+)', raw)}
if not senate_up:
    raise SystemExit('no Senate districts on the 2026 ballot in the database; refusing to write a crosswalk that would call every senator structural')

entries = []
counts = {'proposed': 0, 'hand': 0, 'null_structural': 0, 'null_not_running': 0}

proposed = {p['peopleId']: p for p in report['proposals']}

skipped_committee = []
disagreements = []
for people_id in sorted(people):
    person = people[people_id]
    # Committee pseudo-people are not legislators and never vote. The people
    # snapshot drops them, so the crosswalk must not name them.
    if person.get('committee_sponsor') in (1, True):
        skipped_committee.append(person.get('name'))
        continue
    # Trust the district field over the role field: LegiScan's role has been
    # wrong in several states while its district was right.
    role, seat = person.get('role', ''), person.get('district') or ''
    if (role == 'Rep' and seat.startswith('SD-')) or (role == 'Sen' and seat.startswith('HD-')):
        disagreements.append((people_id, person.get('name'), role, seat))
    if people_id in proposed:
        p = proposed[people_id]
        note = f"proposed and accepted: {p['confidence']}, seat {'agrees' if p['seatAgrees'] else 'differs'}"
        if not p['seatAgrees']:
            note += ' because the member is a sitting legislator running for a different seat in 2026'
        entries.append({'people_id': people_id, 'candidate_id': p['candidateId'], 'note': note})
        counts['proposed'] += 1
        continue
    if people_id in HAND_ADDS:
        cid, note = HAND_ADDS[people_id]
        entries.append({'people_id': people_id, 'candidate_id': cid, 'note': note})
        counts['hand'] += 1
        continue
    seat = person.get('district') or ''
    m = re.match(r'SD-0*(\d+)$', seat)
    if m and int(m.group(1)) not in senate_up:
        note = (f"reviewed, no candidate: Senate district {int(m.group(1))} is not on the November 2026 "
                "ballot. Wisconsin elects its odd-numbered Senate districts that year")
        counts['null_structural'] += 1
    else:
        note = ("reviewed, no candidate: the seat is on the November 2026 ballot but this member is not "
                "among its candidates in our roster")
        counts['null_not_running'] += 1
    entries.append({'people_id': people_id, 'candidate_id': None, 'note': note})

out = {
    'source': 'legiscan',
    'jurisdiction': 'WI',
    'sessionId': 2197,
    'entries': entries,
}
path = BASE + 'crosswalk.json'
with open(path, 'w') as fh:
    fh.write(json.dumps(out, indent=2) + '\n')

print('wrote', path)
print('entries', len(entries), counts)
print('mapped', counts['proposed'] + counts['hand'], '| null', counts['null_structural'] + counts['null_not_running'])
print('committee pseudo-people skipped:', len(skipped_committee), skipped_committee)
print('role disagrees with district (district wins):', disagreements or 'none')
