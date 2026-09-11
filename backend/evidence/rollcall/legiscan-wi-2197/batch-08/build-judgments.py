"""Build the Wisconsin batch-08 judgments file.

Batch-08 adds AB 100 and AB 102, the two sports bills batch-05 dropped pending
the campaign-wide decision on Wyoming SF0044. That decision is made (Wyoming
batch-04): this class of measure is civil_rights, a yes vote is against.

Both bills passed both chambers, were vetoed, and the Assembly failed to
override on 13 May 2026. Both chambers voted the enrolled text: the Assembly
adopted its only amendment before its vote, and the Senate concurred unchanged.
All prose is conditional and every tail names how the bill died.
"""
import json
import os

OUT = os.path.dirname(os.path.abspath(__file__))
M = []

CR = 'civil_rights'

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


# ---------------------------------------------------------------- AB 100
ab100 = (
    'Assembly Bill 100, which would have made public schools, charter schools and '
    'private voucher schools label every sports team as boys, girls or coed, based on '
    'the sex on a pupil\'s original birth certificate, and bar boys from girls\' teams. '
    'A girl who lost a spot or was harmed by a violation could have sued the school. '
    'Each locker room and shower room would also have been limited to one biological '
    'sex, with a single-user room or staff locker room offered to a pupil who asked '
    'for another option. {t}')
add('AB 100', 1522672, 'house', '2025-03-20', ab100.format(t=passed('house', 51, 43)),
    L((CR, 'against', None)))
add('AB 100', 1630296, 'senate', '2026-02-11', ab100.format(t=agreed('senate', 18, 15)),
    L((CR, 'against', None)))

# ---------------------------------------------------------------- AB 102
ab102 = (
    'Assembly Bill 102, which would have made University of Wisconsin campuses and '
    'technical colleges label every college and club sports team as men\'s, women\'s '
    'or coed, based on the sex on a student\'s original birth certificate, and bar men '
    'from women\'s teams. A female student who lost a spot or was harmed by a '
    'violation could have sued the school. Each campus locker room and shower room '
    'would also have been limited to one biological sex, with a single-user room or '
    'staff locker room offered to a student who asked for another option. {t}')
add('AB 102', 1522508, 'house', '2025-03-20', ab102.format(t=passed('house', 50, 43)),
    L((CR, 'against', None)))
add('AB 102', 1630339, 'senate', '2026-02-11', ab102.format(t=agreed('senate', 18, 15)),
    L((CR, 'against', None)))

json.dump({'judgments': M}, open(os.path.join(OUT, 'judgments.json'), 'w'), indent=1)
print(f'{len(M)} slots, {len({m["measure_id"] for m in M})} measures')
