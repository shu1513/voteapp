"""Build the Wisconsin batch-05 judgments file.

Batch-05 is the health, gender and civil rights strand of the vetoed pool. Every
measure passed both chambers, was vetoed, and the chamber where the bill started
failed to override the veto on 13 May 2026. All prose is conditional and every
tail names how the bill died. See batch-02 PLAN.md for the scope rules.

One body per measure; the yes and no descriptions are generated from it behind
different opening clauses. Every slot cites its own roll's tally.
"""
import json
import os

OUT = os.path.dirname(os.path.abspath(__file__))
M = []

CR = 'civil_rights'
DP = 'data_privacy'
HC = 'healthcare_affordability'

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


# ---------------------------------------------------------------- AB 103
ab103 = (
    'Assembly Bill 103, which would have required every school board to adopt a policy '
    'on pupils\' names and pronouns by July 1, 2026. A pupil\'s school records would '
    'have stayed under the legal name and the pronouns given at enrollment unless a '
    'parent, or a pupil aged 18 or older, filed a written, notarized request with the '
    'principal. During school hours, staff could not have called a minor pupil by a name '
    'or pronouns that do not match the pupil\'s biological sex without a parent\'s '
    'written, notarized permission. A shortened form of the pupil\'s legal name would '
    'not have needed permission. {t}')
add('AB 103', 1522724, 'house', '2025-03-20', ab103.format(t=passed('house', 50, 43)),
    L((CR, 'against', None)))
add('AB 103', 1630414, 'senate', '2026-02-11', ab103.format(t=agreed('senate', 18, 15)),
    L((CR, 'against', None)))

# ---------------------------------------------------------------- AB 104
ab104 = (
    'Assembly Bill 104, which would have barred any health care provider from giving a '
    'person under 18 medical treatment meant to change the minor\'s body to match a sex '
    'different from the minor\'s biological sex, or referring a minor for it. The ban '
    'covered puberty blockers, cross-sex hormones, mastectomy and surgeries that '
    'sterilize. It did not cover care for a child born with a disorder of sex '
    'development, treatment of harm caused by an earlier procedure, or surgery needed '
    'to prevent death or serious harm. A licensing board that found a violation would '
    'have had to revoke the provider\'s license, and for a physician or nurse the board '
    'could never restore it. {t}')
add('AB 104', 1522835, 'house', '2025-03-20', ab104.format(t=passed('house', 50, 43)),
    L((CR, 'against', None)))
add('AB 104', 1630631, 'senate', '2026-02-11', ab104.format(t=agreed('senate', 18, 15)),
    L((CR, 'against', None)))

# ---------------------------------------------------------------- SB 405
add('SB 405', 1632126, 'house', '2026-02-12',
    'Senate Bill 405, which would have let a person sue a health care provider who '
    'performed a gender transition procedure on them as a minor, for any physical, '
    'psychological or emotional injury from the procedure or its aftereffects. The '
    'person could have recovered compensatory and punitive damages and attorney fees, '
    'and could have sued up to age 33. A provider would have had a defense only by '
    'meeting three conditions. It must have documented the minor\'s gender identity for '
    'at least two continuous years, two providers including a mental health '
    'professional must have certified in writing that the procedure was the only '
    'treatment, and the minor and a parent must have consented. ' + agreed('house', 53, 45),
    L((CR, 'against', None)))

# ---------------------------------------------------------------- SB 431
sb431 = (
    'Senate Bill 431, which would have widened when an employer or licensing agency in '
    'Wisconsin may turn someone away because of a pending charge. Wisconsin\'s fair '
    'employment law treats an arrest record as a protected characteristic, with an '
    'exception for a pending criminal charge that is substantially related to the job. '
    'The bill would have deleted the word "criminal" from that exception, so it would '
    'have reached any pending charge, including a non-criminal one. {t}')
add('SB 431', 1630075, 'senate', '2026-02-11', sb431.format(t=passed('senate', 18, 15)),
    L((CR, 'against', 'for')))
add('SB 431', 1639112, 'house', '2026-02-19', sb431.format(t=agreed('house', 54, 41)),
    L((CR, 'against', 'for')))

# ---------------------------------------------------------------- SB 799
# Only the Assembly slot survives: the Assembly replaced the whole bill after the
# Senate's vote. The substitute added the Milwaukee police and fire section.
add('SB 799', 1639914, 'house', '2026-02-19',
    'Senate Bill 799, which would have given parents more access to their children\'s '
    'health records. A parent could have seen, at any time, any online version of a '
    'minor\'s health care records. A developmentally disabled minor aged 14 or older '
    'could no longer have objected to a parent seeing the minor\'s treatment records, '
    'and parents of a minor aged 14 or older could have received the minor\'s HIV test '
    'results. A parent whose own conduct led a court to find the child in need of '
    'protection would have lost that access. In a separate section, Milwaukee\'s '
    'Common Council could have changed a police or fire department policy only by a '
    'unanimous vote, instead of two-thirds. ' + agreed('house', 54, 41),
    L((DP, 'against', None)))

# ---------------------------------------------------------------- SB 4
add('SB 4', 1519840, 'senate', '2025-03-18',
    'Senate Bill 4, which would have set rules in state law for direct primary care, '
    'where a patient or an employer pays a doctor\'s office a set subscription fee for '
    'primary care instead of paying per visit. The written agreement would have had to '
    'list the services covered and the fee, and state plainly that it is not health '
    'insurance and may not count toward a deductible. A provider could not have turned '
    'a patient away or ended an agreement because of the patient\'s health, and could '
    'not have billed an insurer for services the fee already covers. '
    + passed('senate', 18, 14),
    L((HC, 'for', None)))

# ---------------------------------------------------------------- SB 214
add('SB 214', 1614174, 'house', '2026-01-15',
    'Senate Bill 214, which would have let a health care provider licensed in another '
    'state treat Wisconsin patients by telehealth after registering with the state, '
    'without getting a Wisconsin license. To register, the provider would have needed '
    'an active license elsewhere with no discipline in the past five years, malpractice '
    'insurance covering Wisconsin patients, and an agent here to accept legal papers. A '
    'registered provider could not have opened an office or seen patients in person '
    'here. The state would have published each registrant\'s training, specialty and '
    'five-year discipline history. ' + agreed('house', 52, 44),
    L((HC, 'for', None)))

json.dump({'judgments': M}, open(os.path.join(OUT, 'judgments.json'), 'w'), indent=1)
print(f'{len(M)} slots, {len({m["measure_id"] for m in M})} measures')
