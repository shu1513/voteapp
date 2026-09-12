"""Build the Wisconsin batch-11 judgments file: the died pool, economy and taxes.

Every bill here passed one chamber on a closely divided roll and never passed the
other; the session's last floor period ended first. Descriptions follow the text
the chamber voted, which for AB 38, AB 996 and SB 287 is a substitute amendment.
"""
import json
import os

OUT = os.path.dirname(os.path.abspath(__file__))
M = []

TAX = 'personal_income_tax_reduction'
SOC = 'social_programs_and_welfare'
IMM = 'immigration'


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


add('AB 38', 1614181, 'house', '2026-01-15', 61, 33, (
    'Assembly Bill 38, which would have let people subtract from their Wisconsin '
    'taxable income the tips they deduct on their federal return under the new '
    'federal deduction for tips, for tax years 2025 through 2028. {t}'),
    L((TAX, 'for', None)))

add('AB 164', 1553695, 'house', '2025-04-22', 53, 42, (
    'Assembly Bill 164, which would have renamed unemployment insurance "reemployment '
    'assistance" and tightened what claimants must do each week. From the third week, '
    'two of the four weekly job search actions would have had to be direct contacts '
    'with employers. Claimants living in Wisconsin would also have had to keep a '
    'current resume on the state job center website. Any claimant with three weeks or '
    'fewer of benefits left would have had to attend a counseling session. The bill '
    'would have removed the rule that a claimant '
    'need not apply for the jobs the department suggests, and made workshops mandatory '
    'for claimants likely to run out of benefits. {t}'), L((SOC, 'against', None)))

add('AB 996', 1640046, 'house', '2026-02-19', 62, 35, (
    'Assembly Bill 996, which would have had the state match the federal '
    'government\'s $1,000 deposit into the Trump account, a new federal savings account '
    'for children, of each child born in Wisconsin and still living here. The bill set '
    'aside no money, so the state would have paid only as funds allowed. {t}'),
    L((SOC, 'for', None)))

add('SB 287', 1636786, 'senate', '2026-02-18', 18, 15, (
    'Senate Bill 287, which would have required state and local government agencies, '
    'including the University of Wisconsin, to check every new hire through E-Verify, '
    'the federal system that confirms a person may legally work in the United States. '
    'The state also could not have awarded a contract of $50,000 or more to an '
    'employer, or its subcontractors, that did not check new hires the same way. {t}'),
    L((IMM, 'against', None)))

json.dump({'judgments': M}, open(os.path.join(OUT, 'judgments.json'), 'w'), indent=1)
print(f'{len(M)} slots, {len({m["measure_id"] for m in M})} measures')
