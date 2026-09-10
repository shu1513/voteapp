"""Build the Wisconsin batch-06 judgments file.

Batch-06 is the labor, unemployment insurance and taxes strand of the vetoed
pool. Every measure passed both chambers, was vetoed, and the chamber where the
bill started failed to override the veto on 13 May 2026. All prose is
conditional and every tail names how the bill died. See batch-02 PLAN.md.

One body per measure; the yes and no descriptions are generated from it behind
different opening clauses. Every slot cites its own roll's tally.
"""
import json
import os

OUT = os.path.dirname(os.path.abspath(__file__))
M = []

SP = 'social_programs_and_welfare'
GE = 'government_efficiency'
TX = 'personal_income_tax_reduction'
PI = 'public_infrastructure'
DP = 'data_privacy'

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


# ---------------------------------------------------------------- AB 162
ab162 = (
    'Assembly Bill 162, which would have required every state agency running a job '
    'training or job placement program to track and publish how well it works. That '
    'would have covered programs paid with state or federal money, including Wisconsin '
    'Works, the food stamp employment program, the Transform Milwaukee Jobs program and '
    'prisoner re-entry work. Each program would have reported the share of participants '
    'working six months and a year after leaving, their median earnings, and the share '
    'earning a credential. The results would have gone on one state website and to the '
    'Legislature every year. {t}')
add('AB 162', 1553530, 'house', '2025-04-22', ab162.format(t=passed('house', 53, 42)),
    L((GE, 'for', None)))
add('AB 162', 1603901, 'senate', '2025-10-14', ab162.format(t=agreed('senate', 18, 15)),
    L((GE, 'for', None)))

# ---------------------------------------------------------------- AB 165
ab165 = (
    'Assembly Bill 165, which would have barred any Wisconsin city, village, town or '
    'county from spending its own money on a guaranteed income program. The bill '
    'defined that as regular cash payments that people have not earned through work and '
    'may spend on anything. A program that requires work or training would not have '
    'counted. The ban covered local taxes, fees and state shared revenue. {t}')
add('AB 165', 1554274, 'house', '2025-04-22', ab165.format(t=passed('house', 53, 42)),
    L((SP, 'against', 'for')))
add('AB 165', 1609481, 'senate', '2025-11-18', ab165.format(t=agreed('senate', 18, 15)),
    L((SP, 'against', 'for')))

# ---------------------------------------------------------------- AB 167
ab167 = (
    'Assembly Bill 167, which would have made several changes to unemployment '
    'insurance. Stealing or misusing an employer\'s property, confidential information '
    'or credit card, or destroying its records, would have counted as misconduct that '
    'disqualifies a worker from benefits. A worker fired for breaking a written '
    'attendance policy could also have been denied benefits. Claimants would have had '
    'to take part in any required re-employment workshops, and the state would have had '
    'to audit at least half of all reported job search actions. Any new federal '
    'unemployment benefit would have needed approval from the Legislature\'s budget '
    'committee before the state could pay it. {t}')
add('AB 167', 1553662, 'house', '2025-04-22', ab167.format(t=passed('house', 53, 42)),
    L((SP, 'against', None)))
add('AB 167', 1615131, 'senate', '2026-01-21', ab167.format(t=agreed('senate', 18, 14)),
    L((SP, 'against', None)))

# ---------------------------------------------------------------- AB 168
ab168 = (
    'Assembly Bill 168, which would have tightened fraud controls and service standards '
    'in unemployment insurance. The state would have had to verify a claimant\'s '
    'identity to a federal digital identity standard before a claim, and check '
    'recipients every week against death records, prison records, the national new-hire '
    'directory and federal immigration databases. It would have had to tell the '
    'Legislature if it ever scaled back a fraud check. It would have had to keep a phone '
    'help line staffed longer when claims surge, and offer free training for employers. '
    'Prosecutors would have had eight years to charge fraud involving benefits paid '
    'during the pandemic. {t}')
add('AB 168', 1553728, 'house', '2025-04-22', ab168.format(t=passed('house', 53, 42)),
    L((GE, 'for', None)))
add('AB 168', 1603902, 'senate', '2025-10-14', ab168.format(t=agreed('senate', 18, 15)),
    L((GE, 'for', None)))

# ---------------------------------------------------------------- AB 169
ab169 = (
    'Assembly Bill 169, which would have required an unemployment claimant\'s weekly '
    'job search report to list every job offer, interview offer and recall to work '
    'received or answered that week. Where the law now says the state "may" recover '
    'benefits paid by mistake, including benefits paid to someone who used the '
    'claimant\'s login, it would have said the state "shall". The state would also '
    'have had to report its fraud work to the Legislature each year and let employers '
    'file reports about claimants online. {t}')
add('AB 169', 1554222, 'house', '2025-04-22', ab169.format(t=passed('house', 53, 42)),
    L((SP, 'against', None)))
add('AB 169', 1603915, 'senate', '2025-10-14', ab169.format(t=agreed('senate', 18, 15)),
    L((SP, 'against', None)))

# ---------------------------------------------------------------- AB 461
ab461 = (
    'Assembly Bill 461, which would have let Wisconsin taxpayers subtract overtime pay '
    'from their state taxable income, matching the federal overtime deduction that took '
    'effect for 2025. The amount would have been whatever the taxpayer could deduct on '
    'the federal return. The federal deduction ends after 2028; the state subtraction '
    'would have continued after that as if it had not. {t}')
add('AB 461', 1614265, 'house', '2026-01-15', ab461.format(t=passed('house', 61, 35)),
    L((TX, 'for', None)))
add('AB 461', 1664187, 'senate', '2026-03-17', ab461.format(t=agreed('senate', 21, 12)),
    L((TX, 'for', None)))

# ---------------------------------------------------------------- SB 36
sb36 = (
    'Senate Bill 36, which would have let Wisconsin taxpayers subtract tips from their '
    'state taxable income for tax years 2025 through 2028, matching the federal '
    'deduction for tips. The amount would have been whatever the taxpayer could deduct '
    'on the federal return. {t}')
add('SB 36', 1615057, 'senate', '2026-01-21', sb36.format(t=passed('senate', 21, 12)),
    L((TX, 'for', None)))
add('SB 36', 1639154, 'house', '2026-02-19', sb36.format(t=agreed('house', 60, 31)),
    L((TX, 'for', None)))

# ---------------------------------------------------------------- SB 176
# Both slots voted the Assembly's substitute, which is the enrolled text.
sb176 = (
    'Senate Bill 176, which would have given a company a state tax credit equal to 6.32 '
    'percent of the broadband expansion money it received each year from 2026 through '
    '2030. That covered grants from the state, local and tribal governments and the '
    'federal government, and federal high-cost program funding for building broadband '
    'in Wisconsin. ')
add('SB 176', 1639148, 'house', '2026-02-19', sb176 + agreed('house', 60, 34),
    L((PI, 'for', None)))
add('SB 176', 1664622, 'senate', '2026-03-17',
    sb176 + 'The Senate accepted the Assembly\'s rewrite of the bill 20-13, ' + DIED,
    L((PI, 'for', None)))

# ---------------------------------------------------------------- AB 1027
add('AB 1027', 1640348, 'house', '2026-02-19',
    'Assembly Bill 1027, which would have tightened food stamp rules. More adults would '
    'have had to meet the program\'s work requirement: the age at which it stops '
    'applying would have risen from 50 to 65, and the exemption for parents would have '
    'covered only those with a child under 14 instead of under 18. Noncitizens other '
    'than qualified aliens would have been barred, with status checked at every '
    'enrollment. The state would have had to give the U.S. Department of Agriculture '
    'the identities of everyone receiving benefits, and all records back to 2020 that '
    'the department asked for in July 2025. ' + passed('house', 54, 39),
    L((SP, 'against', None), (DP, 'against', None)))

json.dump({'judgments': M}, open(os.path.join(OUT, 'judgments.json'), 'w'), indent=1)
print(f'{len(M)} slots, {len({m["measure_id"] for m in M})} measures')
