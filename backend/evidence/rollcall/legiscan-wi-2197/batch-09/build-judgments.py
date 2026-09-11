"""Build the Wisconsin batch-09 judgments file.

Batch-09 adds AB 595, the voter-list bill batch-07 dropped pending the
campaign-wide decision on Wyoming HB0318. That decision is made (Wyoming
batch-04): a list-maintenance act that adds data sources and keeps notice
before removal is election_integrity, a yes vote is for.

AB 595 passed the Assembly on a voice vote, so its only roll call is the
Senate's 18-15 concurrence. The Senate concurred without amendment, so that
vote is on the enrolled text. The governor vetoed the bill and the Assembly
failed to override on 13 May 2026. All prose is conditional and the tail names
how the bill died.
"""
import json
import os

OUT = os.path.dirname(os.path.abspath(__file__))
M = []

EI = 'election_integrity'

DIED = ('but the governor vetoed the bill and the Legislature did not override '
        'the veto, so it never became law.')


def add(bill, roll, chamber, date, body, labels):
    M.append({
        'jurisdiction': 'WI', 'chamber': chamber, 'session': '2197', 'roll': roll,
        'measure_id': bill, 'vote_date': date, 'review_status': 'approved',
        'yea_description': f'Voted for {body}',
        'nay_description': f'Voted against {body}',
        'labels': labels,
    })


def L(*triples):
    return [{'slug': s, 'yea': y, 'nay': n} for s, y, n in triples]


def agreed(chamber, yea, nay):
    return (f"{'The Assembly' if chamber == 'house' else 'The Senate'} agreed to it "
            f'{yea}-{nay}, {DIED}')


# ---------------------------------------------------------------- AB 595
ab595 = (
    'Assembly Bill 595, which would have required daily checks of the voter list '
    'against driver\'s license, death and felony records, and would have removed '
    'ineligible voters from the list instead of marking them ineligible. '
    'Every other year the Legislative Audit Bureau would have searched the list for '
    'noncitizens and confirmed each match in a federal immigration database. '
    'A voter flagged as a noncitizen would have been removed unless they showed a '
    'birth certificate, naturalization certificate or passport within 30 days. '
    'A removed voter could still register again, including at the polls. '
    'The bill would also have made the elections commission rule on complaints that '
    'it broke federal election law, and capped the fee for an electronic copy of the '
    'voter list at $1,000. {t}')
add('AB 595', 1664414, 'senate', '2026-03-17', ab595.format(t=agreed('senate', 18, 15)),
    L((EI, 'for', None)))

json.dump({'judgments': M}, open(os.path.join(OUT, 'judgments.json'), 'w'), indent=1)
print(f'{len(M)} slots, {len({m["measure_id"] for m in M})} measures')
