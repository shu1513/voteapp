"""Build the Wisconsin batch-04 judgments file.

Batch-04 is the immigration and foreign-adversary strand of the vetoed pool.
Every measure passed both chambers, was vetoed, and the chamber where the bill
started failed to override the veto on 13 May 2026. All prose is conditional and
every tail names how the bill died. See batch-02 PLAN.md for the scope rules.

One body per measure; the yes and no descriptions are generated from it behind
different opening clauses. Every slot cites its own roll's tally.
"""
import json
import os

OUT = os.path.dirname(os.path.abspath(__file__))
M = []

IMM = 'immigration'
ND = 'national_defense'
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


# ---------------------------------------------------------------- AB 24
add('AB 24', 1519709, 'house', '2025-03-18',
    'Assembly Bill 24, which would have required a county sheriff to ask anyone held '
    'in the county jail on a felony for proof of lawful presence in the United States. '
    'The bill listed the fifteen documents that would count, from a passport to a '
    'pending asylum application to a Wisconsin driver license. A sheriff who could not '
    'verify a person would have had to tell federal homeland security immediately, and '
    'would have had to hold a person when federal agents sent a detainer and an '
    'administrative warrant. Sheriffs would have had to certify their compliance to the '
    'state each year. A county whose sheriff did not certify would have lost 15 percent '
    'of its state shared revenue payments the following year. ' + passed('house', 51, 43),
    L((IMM, 'against', None)))

# ---------------------------------------------------------------- AB 281
add('AB 281', 1613613, 'house', '2026-01-13',
    'Assembly Bill 281, which would have required employers doing business with '
    'Wisconsin government to use E-Verify, the federal system that checks whether a '
    'new employee may legally work in the United States. State agencies could not have '
    'contracted with an employer that was not enrolled, or that knowingly employed '
    'someone the system flagged as ineligible. A contract found to violate the rule '
    'would have been terminated without liability for the unfinished part. The bill '
    'extended the same requirement to local government contracts and to state '
    'employment, and gave the administration department authority to write rules. '
    + passed('house', 54, 45),
    L((IMM, 'against', None)))

# ---------------------------------------------------------------- AB 308
ab308 = (
    'Assembly Bill 308, which would have barred public money from paying for health '
    'care for a person who is not lawfully present in the United States. The ban '
    'covered state, county, city, village, town and long-term care district funds, and '
    'federal money passing through the state treasury. Two exceptions were written in: '
    'the ban would not have applied '
    'where federal law requires the payment, and it would have applied only so far as '
    'it did not cost the state federal funds. {t}')
add('AB 308', 1601828, 'house', '2025-09-11', ab308.format(t=passed('house', 51, 44)),
    L((IMM, 'against', None)))
add('AB 308', 1609484, 'senate', '2025-11-18', ab308.format(t=agreed('senate', 21, 12)),
    L((IMM, 'against', None)))

# ---------------------------------------------------------------- AB 415
ab415 = (
    'Assembly Bill 415, which would have barred a state agency from using a state-owned '
    'device to open a social media platform, other software, or a generative artificial '
    'intelligence tool owned or controlled by a foreign adversary. The bill named China, '
    'Cuba, Iran, North Korea, Russia and the Maduro government in Venezuela, tied to the '
    'federal list. Police using such tools for law enforcement were exempt. The '
    'administration department, the Legislature\'s technology bureau and the courts '
    'would each have had to publish rules on foreign-adversary technology covering the '
    'supply chain, future purchases and phasing out equipment already bought. {t}')
add('AB 415', 1615869, 'house', '2026-01-22', ab415.format(t=passed('house', 53, 44)),
    L((ND, 'for', None)))
add('AB 415', 1637223, 'senate', '2026-02-18', ab415.format(t=agreed('senate', 18, 15)),
    L((ND, 'for', None)))

# ---------------------------------------------------------------- AB 662
add('AB 662', 1616130, 'house', '2026-01-22',
    'Assembly Bill 662, which would have barred every state agency, including the '
    'Legislature and the courts, from contracting with a business based in, '
    'headquartered in or majority-owned by a foreign adversary. The ban reached '
    'subsidiaries of such a business and anyone reselling its products to the state. '
    'The bill named China, Cuba, Iran, North Korea, Russia and the Maduro government in '
    'Venezuela, and named the Chinese government, the Chinese Communist Party and the '
    'Chinese military directly. No contract would have been valid unless the other side '
    'certified in writing that it is not such a business and had tried to keep such a '
    'business out of the goods and services supplied. ' + passed('house', 53, 44),
    L((ND, 'for', None)))

# ---------------------------------------------------------------- AB 663
ab663 = (
    'Assembly Bill 663, which would have restricted the University of Wisconsin\'s '
    'dealings with foreign adversary countries. The Board of Regents would have had to '
    'approve any research partnership, academic partnership or collaboration agreement '
    'with a university based in such a country. It could approve one only after a '
    'federal law enforcement agency assessed the national security risk, and could not '
    'approve one that let a foreign government direct the curriculum. No such '
    'arrangement would '
    'have been allowed at all for a project funded by the U.S. Department of Defense, '
    'and none with Russia during the 2025-27 budget period. The university could not '
    'have accepted gifts from those countries or their universities, and would have had '
    'to report every such arrangement to the governor and the Legislature each year. {t}')
add('AB 663', 1616128, 'house', '2026-01-22', ab663.format(t=passed('house', 53, 44)),
    L((ND, 'for', None)))
add('AB 663', 1637506, 'senate', '2026-02-18', ab663.format(t=agreed('senate', 19, 14)),
    L((ND, 'for', None)))

# ---------------------------------------------------------------- AB 673
ab673 = (
    'Assembly Bill 673, which would have barred any medical or research facility that '
    'takes state money from using a genetic sequencer, or the software that runs one, '
    'made by a company from a foreign adversary country. It would also have barred any '
    'facility, company or nonprofit from storing a Wisconsin resident\'s genome '
    'sequencing data inside such a country, and required them to keep that data out of '
    'reach of anyone located there. Data collected in a clinical trial covered by '
    'federal rules was excepted. The attorney general would have enforced the law, and '
    'each violation would have carried a $10,000 forfeiture. {t}')
add('AB 673', 1616397, 'house', '2026-01-22', ab673.format(t=passed('house', 53, 44)),
    L((ND, 'for', None), (DP, 'for', None)))
add('AB 673', 1637600, 'senate', '2026-02-18', ab673.format(t=agreed('senate', 20, 13)),
    L((ND, 'for', None), (DP, 'for', None)))

# ---------------------------------------------------------------- SB 7
# Only the Assembly slot survives. The Assembly replaced the whole bill with a
# substitute after the Senate voted, so the Senate's 18-15 is on different text.
add('SB 7', 1615923, 'house', '2026-01-22',
    'Senate Bill 7, which would have done two separate things. A foreign principal '
    'could not have acquired, owned or held any interest in Wisconsin real property. '
    'That term covered the government of China, Cuba, Iran, North Korea, Russia or '
    'Venezuela under Nicolas Maduro, citizens of those countries without a green card '
    'or valid visa, businesses organized there, investment funds they control, and any '
    'entity half-owned by those parties. Anyone who became a foreign principal would '
    'have had 180 days to sell, and property held in violation would have been '
    'forfeited to the state. Property bought before the law took effect was not '
    'covered. Separately, the bill would have barred using the power of condemnation to '
    'take property for a wind energy facility or a solar energy facility. '
    + agreed('house', 55, 42),
    L((ND, 'for', None)))

# ---------------------------------------------------------------- SB 10
sb10 = (
    'Senate Bill 10, which would have required every public high school, including '
    'Milwaukee schools and charter schools, to let military recruiters into the '
    'building\'s common areas during a school day or a school event. That access would '
    'have been required no matter how much access the school gives to colleges or to '
    'other employers. Federal law already conditions school funding on giving '
    'recruiters the same access other recruiters get; this would have set a floor in '
    'state law. Schools would not have had to admit a recruiter to a classroom during '
    'teaching time. {t}')
add('SB 10', 1592689, 'senate', '2025-06-18', sb10.format(t=passed('senate', 18, 14)),
    L((ND, 'for', None)))
add('SB 10', 1609657, 'house', '2025-11-19', sb10.format(t=agreed('house', 55, 42)),
    L((ND, 'for', None)))

json.dump({'judgments': M}, open(os.path.join(OUT, 'judgments.json'), 'w'), indent=1)
print(f'{len(M)} slots, {len({m["measure_id"] for m in M})} measures')
