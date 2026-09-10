"""Build the Wisconsin batch-03 judgments file.

Batch-03 is the crime, courts and policing strand of the vetoed pool. Every
measure here passed both chambers, was vetoed, and the chamber where the bill
started failed to override the veto on 13 May 2026. All prose is conditional and
every tail names how the bill died. See batch-02 PLAN.md for the scope rules.

One body is written per measure and the yes and no descriptions are generated
from it behind different opening clauses, so the two can never drift apart.
Every slot cites its own roll's tally.
"""
import json
import os

OUT = os.path.dirname(os.path.abspath(__file__))
M = []

PS = 'public_safety_and_crime_control'
CR = 'civil_rights'
GE = 'government_efficiency'


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


DIED = ('but the governor vetoed the bill and the Legislature did not override '
        'the veto, so it never became law.')


def passed(chamber, yea, nay):
    house = 'The Assembly' if chamber == 'house' else 'The Senate'
    return f'{house} passed it {yea}-{nay}, {DIED}'


def agreed(chamber, yea, nay):
    house = 'The Assembly' if chamber == 'house' else 'The Senate'
    return f'{house} agreed to it {yea}-{nay}, {DIED}'


# ---------------------------------------------------------------- AB 73
ab73 = (
    'Assembly Bill 73, which would have written two kinds of specialized court '
    'docket into state law. A treatment court docket handles criminal cases where '
    'addiction, mental illness or military service lies behind the offense, and puts '
    'the case before a judge trained in that subject. A commercial court docket '
    'handles business disputes and has run in Wisconsin as a pilot for more than '
    'seven years. The bill set out how cases reach each docket, which cases may not '
    'go to the commercial docket, and how a case moves between counties. The '
    'Legislature\'s stated finding was that trained judges reach better decisions '
    'faster. {t}')
add('AB 73', 1553690, 'house', '2025-04-22', ab73.format(t=passed('house', 53, 42)),
    L((PS, 'for', None), (GE, 'for', None)))
add('AB 73', 1572138, 'senate', '2025-05-15', ab73.format(t=agreed('senate', 18, 15)),
    L((PS, 'for', None), (GE, 'for', None)))

# ---------------------------------------------------------------- AB 85
ab85 = (
    'Assembly Bill 85, which would have required the state corrections department to '
    'recommend revoking a person\'s extended supervision, parole or probation whenever '
    'that person is charged with a crime. A charge is an accusation, not a conviction. '
    'The hearing process that decides whether to actually revoke would have stayed as '
    'it is, so the change is to what the department must ask for, not to who decides. '
    'It would have applied to charges filed on or after the day the law took effect. '
    '{t}')
add('AB 85', 1517055, 'house', '2025-03-13', ab85.format(t=passed('house', 53, 43)),
    L((PS, 'for', None)))
add('AB 85', 1592343, 'senate', '2025-06-18', ab85.format(t=agreed('senate', 18, 14)),
    L((PS, 'for', None)))

# ---------------------------------------------------------------- AB 87
ab87 = (
    'Assembly Bill 87, which would have done two things. A person convicted of human '
    'trafficking or of sexually exploiting a child would have had to pay restitution '
    'to the victim immediately. A court that was not paid would have had to go after '
    'the offender\'s property the way it collects an ordinary civil judgment. '
    'Separately, a person barred from voting by a felony conviction would have got '
    'their vote back only after finishing their sentence and paying every fine, cost, '
    'fee, surcharge and restitution order and finishing any court-ordered community '
    'service. Under the law as it stands, finishing the sentence is enough. {t}')
add('AB 87', 1516987, 'house', '2025-03-13', ab87.format(t=passed('house', 53, 44)),
    L((PS, 'for', None), (CR, 'against', None)))
add('AB 87', 1592450, 'senate', '2025-06-18', ab87.format(t=agreed('senate', 18, 14)),
    L((PS, 'for', None), (CR, 'against', None)))

# ---------------------------------------------------------------- AB 629
add('AB 629', 1635573, 'house', '2026-02-17',
    'Assembly Bill 629, which would have let a police officer act against a drone that '
    'poses a credible threat to people, to a large public event, to critical '
    'infrastructure or to a prison. The officer could have tracked the drone, including '
    'by intercepting the signal controlling it, warned the operator, taken control of '
    'the drone, seized it, or used reasonable force to disable or destroy it. That '
    'power would have applied only to an agency acting under the federal law that '
    'authorizes it, including its training and reporting rules. Flying a drone carrying '
    'a weapon over a prison would have become a felony. ' + passed('house', 57, 42),
    L((PS, 'for', None)))

# ---------------------------------------------------------------- AB 672
ab672 = (
    'Assembly Bill 672, which would have addressed what it called transnational '
    'repression. That means a person acting for a foreign government or a foreign '
    'terrorist group who harasses, intimidates or punishes a dissident, exile, '
    'journalist, political opponent or member of a minority group over their politics. '
    'Committing any crime '
    'that way would have raised the offense one classification. Enforcing a foreign '
    'government\'s law here without federal or state approval would have become a '
    'felony carrying at least three years in prison and a $10,000 fine. The state '
    'justice department would have had to train police, keep a list of the countries '
    'and groups that do this, run a public reporting portal, and report to the '
    'Legislature every year. {t}')
add('AB 672', 1616267, 'house', '2026-01-22', ab672.format(t=passed('house', 53, 44)),
    L((PS, 'for', None)))
add('AB 672', 1637263, 'senate', '2026-02-18', ab672.format(t=agreed('senate', 19, 14)),
    L((PS, 'for', None)))

# ---------------------------------------------------------------- SB 25
add('SB 25', 1519897, 'senate', '2025-03-18',
    'Senate Bill 25, which would have stopped a judge from allowing a criminal '
    'complaint against a police officer over a death the officer was involved in, once '
    'the district attorney has decided there is no basis to prosecute. The only way '
    'past that bar would have been new or previously unused evidence. Wisconsin law '
    'now lets a citizen ask a judge to issue a complaint when a prosecutor declines, '
    'and that route would have been closed for these cases. ' + passed('senate', 19, 13),
    L((PS, 'against', None)))

# ---------------------------------------------------------------- SB 76
sb76 = (
    'Senate Bill 76, which would have limited what a prosecutor can do with a set of '
    'charges the bill calls covered crimes. Those are domestic abuse offenses, taking '
    'a vehicle without consent, physical abuse of an elder or at-risk adult, sexual '
    'assault, crimes against children, gun possession by someone with a violent felony '
    'record, and reckless driving that causes great bodily harm. A prosecutor could '
    'not have dismissed or '
    'reduced such a charge without a judge\'s approval, and the judge could approve '
    'only after finding it serves the public interest in deterring those crimes. A '
    'court that approved any such request would have had to report every one of them '
    'to the Legislature each year. Deferred prosecution, which sets a case aside while '
    'the defendant completes conditions, would have been barred outright for these '
    'crimes. {t}')
add('SB 76', 1553496, 'senate', '2025-04-22', sb76.format(t=passed('senate', 18, 15)),
    L((PS, 'for', None)))
add('SB 76', 1603897, 'house', '2025-10-14', sb76.format(t=agreed('house', 53, 43)),
    L((PS, 'for', None)))

# ---------------------------------------------------------------- SB 146
sb146 = (
    'Senate Bill 146, which would have barred anyone convicted of a violent felony '
    'from ever changing their name, and made trying to do so a felony in itself. The '
    'ban would have reached every route to a new name: a court petition, an amendment '
    'to a birth record, a change of the name and sex on a birth record after surgery, '
    'and resuming a former surname after a divorce. It had no end date and no '
    'exceptions. {t}')
add('SB 146', 1572104, 'senate', '2025-05-15', sb146.format(t=passed('senate', 18, 15)),
    L((PS, 'for', None)))
add('SB 146', 1613595, 'house', '2026-01-13', sb146.format(t=agreed('house', 54, 45)),
    L((PS, 'for', None)))

# ---------------------------------------------------------------- SB 432
sb432 = (
    'Senate Bill 432, which would have widened who must report child abuse and what '
    'must go to the police. Any employee of an agency whose job involves working '
    'directly with children or handling child welfare cases would have joined the list '
    'of mandatory reporters. County agencies would have had to refer to the sheriff or '
    'police, within 12 hours, every reported case of suspected or threatened abuse of '
    'any kind. Under the law as it stands, that 12-hour duty covers only some kinds of '
    'abuse and each agency writes its own policy for the rest. {t}')
add('SB 432', 1609458, 'senate', '2025-11-18', sb432.format(t=passed('senate', 18, 15)),
    L((PS, 'for', None)))
add('SB 432', 1632467, 'house', '2026-02-12', sb432.format(t=agreed('house', 61, 37)),
    L((PS, 'for', None)))

# ---------------------------------------------------------------- SB 610
sb610 = (
    'Senate Bill 610, which would have added homeless shelters to the places where '
    'dealing drugs carries an extra penalty. Wisconsin already adds up to five years '
    'in prison when someone delivers a controlled substance, or holds it with intent '
    'to deliver, in or within 1,000 feet of a school, a park, a public housing project '
    'or a treatment center. That covers drugs such as cocaine, heroin, fentanyl, '
    'methamphetamine and marijuana. The bill would have '
    'covered a person inside a shelter, or within 1,000 feet of one, who knew or should '
    'have known where they were, or where the shelter is plainly recognizable as one. '
    '{t}')
add('SB 610', 1615064, 'senate', '2026-01-21', sb610.format(t=passed('senate', 18, 15)),
    L((PS, 'for', None)))
add('SB 610', 1632556, 'house', '2026-02-12', sb610.format(t=agreed('house', 56, 42)),
    L((PS, 'for', None)))

json.dump({'judgments': M}, open(os.path.join(OUT, 'judgments.json'), 'w'), indent=1)
print(f'{len(M)} slots, {len({m["measure_id"] for m in M})} measures')
