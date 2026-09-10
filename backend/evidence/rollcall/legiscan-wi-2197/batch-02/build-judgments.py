"""Build the Wisconsin batch-02 judgments file.

Batch-02 is the education strand of the vetoed pool. Every measure here passed
both chambers, was vetoed by the governor, and the originating chamber failed to
override the veto on 13 May 2026, so none of them became law. Every description
is therefore written in the conditional and every tail names how the bill died.

One body is written per measure-chamber slot and the yes and no descriptions are
generated from it behind different opening clauses, so the two can never drift
apart. Every body cites that roll's own tally.
"""
import json
import os

OUT = os.path.dirname(os.path.abspath(__file__))

M = []


def add(bill, roll, chamber, date, body, labels):
    M.append({
        'jurisdiction': 'WI', 'chamber': chamber, 'session': '2197', 'roll': roll,
        'measure_id': bill, 'vote_date': date, 'review_status': 'approved',
        'yea_description': f'Voted for {body}',
        'nay_description': f'Voted against {body}',
        'labels': labels,
    })


def L(slug, yea, nay=None):
    return [{'slug': slug, 'yea': yea, 'nay': nay}]


EDU = 'public_education_quality'

# The bill died the same way in every case. Only the chamber and tally change.
def tail(chamber, yea, nay):
    verb = 'The Assembly passed it' if chamber == 'A' else (
        'The Senate passed it' if chamber == 'S' else None)
    return (f'{verb} {yea}-{nay}, but the governor vetoed the bill and the '
            f'Legislature did not override the veto, so it never became law.')


def tail2(chamber, yea, nay):
    """Tail for the second chamber, which agrees to the other house's bill."""
    verb = 'The Assembly agreed to it' if chamber == 'A' else 'The Senate agreed to it'
    return (f'{verb} {yea}-{nay}, but the governor vetoed the bill and the '
            f'Legislature did not override the veto, so it never became law.')


# ---------------------------------------------------------------- AB 1
ab1 = (
    'Assembly Bill 1, which would have changed how Wisconsin grades its schools and '
    'reports test results. State school and school district report cards would have '
    'had to use the same score cutoffs, the same score ranges and the same written '
    'definitions for each of the five performance categories that the state used for '
    'the 2019-20 school year. For English and mathematics tests in grades 3 through 8, '
    'the state would have had to set cutoffs and pupil performance categories matching '
    'those of the National Assessment of Educational Progress, a test given across the '
    'country. For grades 9 through 11 it would have had to use the 2021-22 cutoffs and '
    'the labels Below Basic, Basic, Proficient and Advanced. {t}')
add('AB 1', 1493267, 'house', '2025-02-19', ab1.format(t=tail('A', 54, 44)), L(EDU, 'for'))
add('AB 1', 1519793, 'senate', '2025-03-18', ab1.format(t=tail2('S', 18, 14)), L(EDU, 'for'))

# ---------------------------------------------------------------- AB 5
ab5 = (
    'Assembly Bill 5, which would have required every school board to post its list of '
    'adopted textbooks on the board\'s website as well as file it with the school '
    'district clerk. Any resident of the district could then have asked in writing to '
    'see a copy of an adopted textbook, or of any curriculum or instructional material '
    'used in a school in the district. The board would have had to make it available '
    'within 14 days, at no cost, at the place the district uses for public records '
    'requests, and would have had to adopt procedures for doing it faster. The bill '
    'said it could not be read to require anything that would infringe copyright. {t}')
add('AB 5', 1493715, 'house', '2025-02-19', ab5.format(t=tail('A', 54, 43)), L(EDU, 'for'))
add('AB 5', 1603905, 'senate', '2025-10-14', ab5.format(t=tail2('S', 18, 15)), L(EDU, 'for'))

# ---------------------------------------------------------------- AB 166
add('AB 166', 1553838, 'house', '2025-04-22',
    'Assembly Bill 166, which would have required every public, technical and private '
    'college in Wisconsin to report on its undergraduate programs each year. The report '
    'would have covered the average salary of graduates six months out, the average '
    'debt students leave with, the graduation rate, the total and net cost of '
    'attending, the financial aid available and the ten most popular degree programs. '
    'Most of those figures would have been reported both overall and separately for '
    'each major. The Higher Educational Aids Board would have turned the reports into '
    'one document that lets families compare colleges, and would have published a list '
    'of the 50 most in-demand jobs in the state with the pay and schooling each needs. '
    'Schools would have had to give that document to pupils in grades 10 through 12 as '
    'part of career planning. ' + tail('A', 56, 39),
    L(EDU, 'for'))

# ---------------------------------------------------------------- AB 457
ab457 = (
    'Assembly Bill 457, which would have stopped a school board from asking voters for '
    'permission to borrow money or to raise more than its revenue limit. The board '
    'could only have asked if the state education department first certified that the '
    'district was up to date on the financial reports it owes the state. The '
    'certification could have been no more '
    'than 14 days old when the board adopted the resolution. A resolution adopted '
    'without one, and any referendum held on that resolution, would have been void. {t}')
add('AB 457', 1614218, 'house', '2026-01-15', ab457.format(t=tail('A', 52, 44)), L(EDU, 'for'))
add('AB 457', 1615445, 'senate', '2026-01-21', ab457.format(t=tail2('S', 18, 15)), L(EDU, 'for'))

# ---------------------------------------------------------------- AB 582
add('AB 582', 1609678, 'house', '2025-11-19',
    'Assembly Bill 582, which would have made it easier for a high school pupil to earn '
    'college credit that actually counts later. The University of Wisconsin System and '
    'the technical colleges would have had to agree on at least 72 credits of core '
    'general education courses that transfer between them. That agreement would have '
    'had to cover credits a pupil earned while still in high school. A separate '
    'agreement covering at least 36 credits would have been required with the state\'s '
    'private colleges, which could leave out a course that conflicts with a religious '
    'or professional requirement. The bill would also have created a Council on Dual '
    'Enrollment to study the programs and report on them. It would have required the '
    'state to run a public website and print a brochure explaining how the programs '
    'work. A pupil turned down by a school when asking to take a college course could '
    'have appealed to the state superintendent. ' + tail('A', 55, 43),
    L(EDU, 'for'))

# ---------------------------------------------------------------- AB 614
ab614 = (
    'Assembly Bill 614, which would have written into state law a teacher\'s authority '
    'to remove a disruptive pupil from class and send the pupil to the principal. The '
    'principal would have had to act on the removal within a set time, and a school '
    'board could not have fired, demoted or otherwise punished a teacher for making '
    'one. A district would have had to tell a pupil\'s parent when the pupil was '
    'involved in a disruptive incident, including when a pupil was removed from the '
    'class, and to adopt a code of conduct. The bill also set limits on removing a '
    'pupil whose behavior comes from a disability, and required a child\'s special '
    'education plan to say whether removal is appropriate for that behavior. {t}')
add('AB 614', 1609642, 'house', '2025-11-19', ab614.format(t=tail('A', 54, 43)), L(EDU, 'for'))
add('AB 614', 1630865, 'senate', '2026-02-11', ab614.format(t=tail2('S', 19, 14)), L(EDU, 'for'))

# ---------------------------------------------------------------- AB 1005
add('AB 1005', 1632980, 'house', '2026-02-12',
    'Assembly Bill 1005, which would have required that decisions on undergraduate '
    'admission to the University of Wisconsin-Madison be based predominantly on how an '
    'applicant scored on the ACT, the SAT or a similar test of college readiness. '
    'Pupils admitted through the state\'s guaranteed admission program would have been '
    'left out of that rule. It would have first applied to applications for the 2027-28 '
    'academic year. ' + tail('A', 52, 45),
    L(EDU, 'for'))

# ---------------------------------------------------------------- SB 389
sb389 = (
    'Senate Bill 389, which would have ended the automatic yearly increase in the money '
    'a school district may raise from state aid and property taxes. Under the law as it '
    'stands, each district\'s revenue limit rises by $325 per pupil every year with no '
    'end date. The bill would have kept that $325 increase through the 2026-27 school '
    'year and then stopped it, making no adjustment for 2027-28 or any year after. {t}')
add('SB 389', 1609464, 'senate', '2025-11-18', sb389.format(t=tail('S', 18, 15)),
    L(EDU, 'against', 'for'))
add('SB 389', 1639107, 'house', '2026-02-19', sb389.format(t=tail2('A', 54, 40)),
    L(EDU, 'against', 'for'))

# ---------------------------------------------------------------- SB 532
# The Senate voted the bill before the Assembly added the course-format section,
# so the two chambers get different bodies. See JUDGING.md.
sb532_fee = (
    'Senate Bill 532, which would have stopped University of Wisconsin System '
    'institutions from charging a student an extra fee for a course offered only '
    'online. Two exceptions were allowed: a fee that covers real costs the institution '
    'would not have if the course were taught in person, and a fee that is also charged '
    'for the same course taught in person. ')
add('SB 532', 1609480, 'senate', '2025-11-18', sb532_fee + tail('S', 18, 15), L(EDU, 'for'))
add('SB 532', 1632852, 'house', '2026-02-12',
    sb532_fee + 'The Assembly added a section requiring each institution to say whether '
    'each course is in person, online or a mix of the two when it first publishes its '
    'course list for a term, and barring a change of format once enrollment has opened. '
    + tail2('A', 53, 45),
    L(EDU, 'for'))

json.dump({'judgments': M}, open(os.path.join(OUT, 'judgments.json'), 'w'), indent=1)
print(f'{len(M)} slots, {len({m["measure_id"] for m in M})} measures')
