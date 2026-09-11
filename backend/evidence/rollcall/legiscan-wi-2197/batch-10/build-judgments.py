"""Build the Wisconsin batch-10 judgments file: the died pool, education strand.

Every bill here passed one chamber on a closely divided roll and never passed the
other. The history ends "Failed to concur in pursuant to Senate Joint Resolution
1": the session's last floor period ended with no vote in the second chamber.
The session is over, so the tail says the bill never became law.

Only Introduced prints exist. Where the chamber adopted an amendment before its
vote, the description is written from the introduced text plus that amendment.
"""
import json
import os

OUT = os.path.dirname(os.path.abspath(__file__))
M = []

EDU = 'public_education_quality'
SAFE = 'public_safety_and_crime_control'
ENV = 'environment_and_public_health'
EFF = 'government_efficiency'


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


add('AB 3', 1494273, 'house', '2025-02-19', 51, 46, (
    'Assembly Bill 3, which would have added cursive writing to the state\'s model '
    'English standards and required public, charter and private voucher schools to '
    'teach it, so that pupils could write legibly in cursive by the end of fifth '
    'grade. {t}'), L((EDU, 'for', None)))

add('AB 4', 1493523, 'house', '2025-02-19', 52, 46, (
    'Assembly Bill 4, which would have required public, charter and private voucher '
    'schools to teach civics starting in the 2027-28 school year. The lessons would '
    'have covered the country\'s founding principles, how to take part in government, '
    'and how communism, socialism and totalitarianism compare with representative '
    'democracy. Students graduating from the 2030-31 school year on would have needed '
    'a half credit of civics. {t}'), L((EDU, 'for', None)))

add('AB 91', 1517600, 'house', '2025-03-13', 53, 44, (
    'Assembly Bill 91, which would have split the cost of the 25 school resource '
    'officers, police officers assigned to schools, that state law already requires '
    'in Milwaukee Public Schools equally between the district and the City of '
    'Milwaukee. Both would have had 30 days to sign an agreement and certify that the '
    'officers were in place, or lose part of their state aid. {t}'),
    L((SAFE, 'for', None)))

add('AB 226', 1614194, 'house', '2026-01-15', 53, 43, (
    'Assembly Bill 226, which would have barred public schools, charter schools and '
    'private voucher schools from serving meals that contain any of five additives: '
    'brominated vegetable oil, potassium bromate, propylparaben, azodicarbonamide and '
    'red dye 3. {t}'), L((ENV, 'for', None)))

add('AB 644', 1609667, 'house', '2025-11-19', 53, 44, (
    'Assembly Bill 644, which would have raised state aid for school districts that '
    'merge in 2027, 2028 or 2029 to $1,500 per pupil in the first year and $650 in the '
    'second, up from $150 today. {t}'), L((EFF, 'for', None)))

add('AB 647', 1609704, 'house', '2025-11-19', 54, 43, (
    'Assembly Bill 647, which would have given school boards that agree to share whole '
    'grades, sending every pupil in a grade to one district, a four-year grant of $500 '
    'per pupil in those grades. A new grant would have been allowed only when enough '
    'money was set aside to pay the existing grants in full. {t}'),
    L((EFF, 'for', None)))

add('AB 648', 1609660, 'house', '2025-11-19', 54, 43, (
    'Assembly Bill 648, which would have created new state aid for school districts '
    'that merge on or after July 1, 2026, when the merged district\'s allowed tax rate '
    'is higher than the lowest rate among the districts that merged. The aid would '
    'have shrunk by a fifth each year and ended in the sixth year. It would have '
    'counted against the district\'s revenue limit, which lowers the property tax the '
    'district may levy. The bill did not fund the new aid. {t}'),
    L((EFF, 'for', None)))

add('SB 41', 1592623, 'senate', '2025-06-18', 18, 14, (
    'Senate Bill 41, which would have created competitive grants of up to $20,000 for '
    'public, private and tribal schools to make their buildings safer and train staff '
    'in security, with preference for schools that had never had a state school '
    'safety grant. Before voting, the Senate removed the bill\'s $30 million, so no '
    'money would have been set aside for the grants. No grant could have been made '
    'after June 30, 2027. {t}'), L((SAFE, 'for', None)))

json.dump({'judgments': M}, open(os.path.join(OUT, 'judgments.json'), 'w'), indent=1)
print(f'{len(M)} slots, {len({m["measure_id"] for m in M})} measures')
