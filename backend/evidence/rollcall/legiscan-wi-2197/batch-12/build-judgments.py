"""Build the Wisconsin batch-12 judgments file: the died pool, everything else.

Crime and courts, elections, civil rights, abortion, technology. Every bill here
passed one chamber on a closely divided roll and never passed the other before
the session's last floor period ended. Descriptions follow the text the chamber
voted: AB 617, AB 840 and AB 963 were voted as substitute amendments, and AB 66,
AB 377 and AB 380 with adopted amendments.
"""
import json
import os

OUT = os.path.dirname(os.path.abspath(__file__))
M = []

SAFE = 'public_safety_and_crime_control'
CR = 'civil_rights'
AC = 'anti_corruption'
EI = 'election_integrity'
ENV = 'environment_and_public_health'
COL = 'cost_of_living_reduction'
DP = 'data_privacy'
WRR = 'womens_reproductive_rights'


def died(chamber, yea, nay):
    first, other = ('Assembly', 'Senate') if chamber == 'house' else ('Senate', 'Assembly')
    return (f'The {first} passed it {yea}-{nay}, but the {other} did not vote on it '
            'before the session\'s last floor period ended, so it never became law.')


def add(bill, roll, chamber, date, yea, nay, body, labels):
    text = body.format(t=died(chamber, yea, nay))
    assert 'would' in text and 'the act' not in text.lower()
    M.append({
        'jurisdiction': 'WI', 'chamber': chamber, 'session': '2197', 'roll': roll,
        'measure_id': bill, 'vote_date': date, 'review_status': 'approved',
        'yea_description': f'Voted for {text}',
        'nay_description': f'Voted against {text}',
        'labels': labels,
    })


def L(*triples):
    return [{'slug': s, 'yea': y, 'nay': n} for s, y, n in triples]


add('AB 66', 1517188, 'house', '2025-03-13', 53, 44, (
    'Assembly Bill 66, which would have barred prosecutors from dropping or reducing '
    'charges for certain crimes without a judge\'s approval, and from offering '
    'deferred prosecution deals once those charges were filed. The crimes included '
    'domestic abuse, sexual assault, crimes against children, car theft, abuse of '
    'vulnerable adults, reckless driving that causes great bodily harm, and illegal '
    'gun possession by someone with a violent felony record. {t}'),
    L((SAFE, 'for', None)))

add('AB 377', 1614204, 'house', '2026-01-15', 51, 45, (
    'Assembly Bill 377, which would have made English Wisconsin\'s official language '
    'and required state and local governments to communicate in English, with '
    'exceptions such as protecting health, safety or a defendant\'s rights. Hospitals '
    'would have been exempt. Agencies and courts could have offered artificial '
    'intelligence or machine translation in place of a required interpreter, though '
    'not to a defendant accused of a violent crime. {t}'), L((CR, 'against', None)))

add('AB 380', 1613583, 'house', '2026-01-13', 57, 42, (
    'Assembly Bill 380, which would have made a judge or court commissioner go without '
    'pay while suspended by the state Supreme Court over criminal misconduct. Before a '
    'temporary suspension, a panel would have had to find probable cause at a hearing, '
    'and a judge who was later cleared would have received back pay. {t}'),
    L((AC, 'for', None)))

add('AB 58', 1601824, 'house', '2025-09-11', 50, 44, (
    'Assembly Bill 58, which would have allowed only the U.S. flag, the Wisconsin flag '
    'and state agency flags on state, local government and school buildings, with '
    'exceptions such as military, tribal and first responder flags. No excepted flag '
    'could have stood for a political or social cause, a racial identity, or a sexual '
    'orientation or gender identity. {t}'), L((CR, 'against', None)))

add('AB 617', 1609656, 'house', '2025-11-19', 53, 44, (
    'Assembly Bill 617, which would have changed absentee voting and ballot counting. '
    'Clerks would have had to return a ballot with a faulty envelope to the voter, or '
    'contact the voter, so the error could be fixed. Voters who applied by email could '
    'have gotten text alerts when their application and ballot arrived. The deadline '
    'to request a ballot by mail would have moved from five days before the election '
    'to seven. Cities could no longer have counted polling-place ballots at one central '
    'location, though central counting of absentee ballots could have continued, and '
    'early voting sites would have had to be in fixed buildings. {t}'), L((EI, 'for', None)))

add('AB 840', 1614720, 'house', '2026-01-20', 53, 44, (
    'Assembly Bill 840, which would have set rules for the largest data centers. '
    'Utility regulators would have had to keep the cost of serving them off other '
    'customers\' bills. New centers would have needed closed-loop or equally '
    'water-saving cooling, on-site placement of any renewable power plant that mainly '
    'served them, and a bond to cover cleanup. Every large center would also have '
    'reported its water use each '
    'year. {t}'), L((ENV, 'for', None), (COL, 'for', None)))

add('AB 963', 1639884, 'house', '2026-02-19', 60, 35, (
    'Assembly Bill 963, which would have required social media companies with at '
    'least $1 billion in yearly revenue to estimate users\' ages and get a parent\'s '
    'consent before a minor could hold an account. Minors\' accounts would have had '
    'the most private settings by default, no targeted ads, and no features such as '
    'infinite scroll, autoplay or like counts. {t}'), L((DP, 'for', None)))

add('SB 94', 1592439, 'senate', '2025-06-18', 18, 14, (
    'Senate Bill 94, which would have made it a felony to urge or organize a riot or '
    'to commit violence during one. People harmed by a riot or vandalism could have '
    'sued those responsible and anyone who gave them support meant for the crime. '
    'Officials could not have stopped police from arresting rioters or breaking up a '
    'riot. {t}'), L((SAFE, 'for', None)))

add('SB 384', 1629702, 'senate', '2026-02-11', 17, 16, (
    'Senate Bill 384, which would have required health care providers to give a child '
    'born alive after an attempted abortion the same care as any other newborn of that '
    'age, and to get the child to a hospital. Failing to do so would have been a felony '
    'with up to six years in prison, and intentionally killing such a child would have '
    'carried life in prison. The mother could not have been charged. {t}'),
    L((WRR, 'against', None)))

add('SB 394', 1609541, 'senate', '2025-11-18', 18, 15, (
    'Senate Bill 394, which would have made it a felony to damage or put graffiti on '
    'any statue, plaque, painting or other monument of historical or commemorative '
    'value on public property. {t}'), L((SAFE, 'for', None)))

json.dump({'judgments': M}, open(os.path.join(OUT, 'judgments.json'), 'w'), indent=1)
print(f'{len(M)} slots, {len({m["measure_id"] for m in M})} measures')
