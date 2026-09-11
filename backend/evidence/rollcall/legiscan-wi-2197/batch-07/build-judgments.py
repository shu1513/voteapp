"""Build the Wisconsin batch-07 judgments file.

Batch-07 closes the vetoed pool: elections, environment and state government
operations. Every measure passed both chambers, was vetoed, and the chamber
where the bill started failed to override the veto on 13 May 2026. All prose is
conditional and every tail names how the bill died. See batch-02 PLAN.md.

One body per measure; the yes and no descriptions are generated from it behind
different opening clauses. Every slot cites its own roll's tally.
"""
import json
import os

OUT = os.path.dirname(os.path.abspath(__file__))
M = []

EN = 'environment_and_public_health'
EI = 'election_integrity'
GE = 'government_efficiency'
AC = 'anti_corruption'

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


def passed(chamber, yea, nay):
    return (f"{'The Assembly' if chamber == 'house' else 'The Senate'} passed it "
            f'{yea}-{nay}, {DIED}')


def agreed(chamber, yea, nay):
    return (f"{'The Assembly' if chamber == 'house' else 'The Senate'} agreed to it "
            f'{yea}-{nay}, {DIED}')


# ---------------------------------------------------------------- AB 211
ab211 = (
    'Assembly Bill 211, which would have exempted some cigar and pipe bars from '
    'Wisconsin\'s indoor smoking ban. A bar would have qualified only if it opened on '
    'or after June 4, 2009, allowed only cigars and pipes, and was not a retail food '
    'establishment. It would also have had to bar anyone under 21, post a notice, have '
    'every employee sign an acknowledgment of exposure to secondhand smoke, and show '
    'the state building plans with enough air filtration and exhaust. {t}')
add('AB 211', 1601825, 'house', '2025-09-11', ab211.format(t=passed('house', 57, 37)),
    L((EN, 'against', 'for')))
add('AB 211', 1609469, 'senate', '2025-11-18', ab211.format(t=agreed('senate', 18, 15)),
    L((EN, 'against', 'for')))

# ---------------------------------------------------------------- AB 385
add('AB 385', 1609705, 'house', '2025-11-19',
    'Assembly Bill 385, which would have required any online platform that collects '
    'credit or debit card payments to pass on to political committees to verify the '
    'card\'s security code and confirm a United States billing address at the time of '
    'the payment. A United States citizen living abroad with a foreign billing address '
    'could still have given, if the platform recorded the address the person uses to '
    'register to vote. A platform that skipped these checks would have forfeited an '
    'amount equal to all the contributions it took without them. '
    + passed('house', 55, 42),
    L((EI, 'for', None)))

# ---------------------------------------------------------------- AB 793
add('AB 793', 1636038, 'house', '2026-02-17',
    'Assembly Bill 793, which would have created an Office of Internal Audit at the '
    'Department of Employee Trust Funds, which runs the pension system for Wisconsin '
    'public employees. The internal auditor would have been appointed by, and reported '
    'directly to, the Employee Trust Funds Board rather than the department\'s '
    'managers. The office would have checked that the fund\'s assets are safeguarded, '
    'could have reviewed any record relating to the fund, and would have monitored the '
    'department\'s compliance with the law and its contracts. '
    + passed('house', 56, 43),
    L((GE, 'for', None)))

# ---------------------------------------------------------------- SB 16
add('SB 16', 1609438, 'senate', '2025-11-18',
    'Senate Bill 16, which would have barred a Wisconsin school district from belonging '
    'to a high school athletic association unless that association agreed to follow '
    'the state\'s open records and open meetings laws. The association governing high '
    'school sports in Wisconsin is a private nonprofit, so those laws do not reach it '
    'now. Records about individual referees and individual pupils would have stayed '
    'private. ' + passed('senate', 22, 11),
    L((AC, 'for', None)))

# ---------------------------------------------------------------- SB 184
add('SB 184', 1592633, 'senate', '2025-06-18',
    'Senate Bill 184, which would have barred every state agency and local government '
    'in Wisconsin from restricting the use or sale of a motor vehicle, or of any other '
    'device, because of the energy source that powers it. That would have ruled out, '
    'for example, a local ban on new gas-powered cars or gas stoves. A government '
    'could still have set its own rules for the vehicles it buys. '
    + passed('senate', 18, 14),
    L((EN, 'against', 'for')))

# ---------------------------------------------------------------- SB 420
sb420 = (
    'Senate Bill 420, which would have barred any Wisconsin city, village, town or '
    'county from passing a rights of nature ordinance. The bill defined that as an '
    'ordinance giving a natural resource a legal right to exist, to be protected from '
    'pollution, or to keep a healthy ecosystem. {t}')
add('SB 420', 1630282, 'senate', '2026-02-11', sb420.format(t=passed('senate', 19, 14)),
    L((EN, 'against', 'for')))
add('SB 420', 1639558, 'house', '2026-02-19', sb420.format(t=agreed('house', 54, 41)),
    L((EN, 'against', 'for')))

json.dump({'judgments': M}, open(os.path.join(OUT, 'judgments.json'), 'w'), indent=1)
print(f'{len(M)} slots, {len({m["measure_id"] for m in M})} measures')
