"""Build the Wisconsin batch-01 judgments file.

One body is written per measure-chamber slot and the yes and no descriptions are
generated from it behind different opening clauses, so the two can never drift
apart. Every body cites that roll's own tally.
"""
import json
import os

OUT = os.path.dirname(os.path.abspath(__file__))

# slot -> (roll, chamber, date, yea, nay, body, labels)
M = []


def add(bill, roll, chamber, date, yea, nay, body, labels):
    M.append({
        'jurisdiction': 'WI', 'chamber': chamber, 'session': '2197', 'roll': roll,
        'measure_id': bill, 'vote_date': date, 'review_status': 'approved',
        'yea_description': f'Voted for {body}',
        'nay_description': f'Voted against {body}',
        'labels': labels,
    })


def L(slug, yea, nay=None):
    return [{'slug': slug, 'yea': yea, 'nay': nay}]


# ---------------------------------------------------------------- AB 180
ab180 = (
    'Assembly Bill 180, which orders the state health department to ask the federal '
    'government for permission to stop people from buying candy or soft drinks with '
    'FoodShare benefits. FoodShare is Wisconsin\'s name for food stamps. The act '
    'writes out what counts as candy and what counts as a soft drink, and it exempts '
    'baked goods, milk drinks, coffee, unsweetened tea and drinks that are more than '
    'half juice. If the federal government says no, the department has to ask again '
    'every year until it says yes. The act also pays a nonprofit group up to $3 '
    'million to build a checkout system that tells stores which products can be '
    'bought with benefits. {tail}')
add('AB 180', 1640331, 'house', '2026-02-19', 71, 22,
    ab180.format(tail='The Assembly passed it 71-22 and it became law as 2025 Wisconsin Act 116.'),
    L('social_programs_and_welfare', 'against'))
add('AB 180', 1664719, 'senate', '2026-03-17', 25, 8,
    ab180.format(tail='The Senate agreed to it 25-8 and it became law as 2025 Wisconsin Act 116.'),
    L('social_programs_and_welfare', 'against'))

# ---------------------------------------------------------------- AB 2
add('AB 2', 1493536, 'house', '2025-02-19', 53, 45,
    'Assembly Bill 2, which requires every school board in Wisconsin to adopt a policy '
    'banning pupils from using their own phones and other wireless devices during '
    'class time by July 1, 2026. Boards must allow exceptions for an emergency, for a '
    'health need written into a pupil\'s care plan, for a pupil\'s special education or '
    'disability plan, and for use a teacher approves for schoolwork. A board may set '
    'consequences for breaking the policy, including taking the device for the rest of '
    'the school day. Schools must send their policy to the state and report any changes '
    'each year. The Assembly passed it 53-45 and it became law as 2025 Wisconsin Act 42.',
    L('public_education_quality', 'for'))

# ---------------------------------------------------------------- AB 223
add('AB 223', 1630502, 'senate', '2026-02-11', 19, 14,
    'Assembly Bill 223, which requires a person who collects signatures on nomination '
    'papers to be a Wisconsin voter. Before the act, someone who lived in another state '
    'could collect those signatures if they were a United States citizen aged 18 or '
    'over and would be allowed to vote if they lived here. The act keeps the old, wider '
    'rule for presidential and vice-presidential nomination papers only. It also '
    'requires that a person circulating a recall petition be a Wisconsin voter. The '
    'Senate agreed to it 19-14 and it became law as 2025 Wisconsin Act 126.',
    L('election_integrity', 'for'))

# ---------------------------------------------------------------- AB 35
add('AB 35', 1603913, 'senate', '2025-10-14', 19, 14,
    'Assembly Bill 35, which for the first time lets a candidate take their name off '
    'the ballot after filing. To withdraw, a candidate must file a sworn statement, '
    'have their identity checked in person by a county clerk, county sheriff or police '
    'chief, and pay a fee. The fee is $2,500 for statewide office, United States Senate '
    'or president, $500 for the United States House and $250 for the state Legislature. '
    'There are deadlines before the primary and before the general election. Making a '
    'false withdrawal statement is a felony. The Senate agreed to it 19-14 and it '
    'became law as 2025 Wisconsin Act 43.',
    L('election_integrity', 'for'))

# ---------------------------------------------------------------- AB 446
add('AB 446', 1635686, 'house', '2026-02-17', 66, 33,
    'Assembly Bill 446, which tells every state agency and local government in '
    'Wisconsin to consider a particular definition of antisemitism when they weigh '
    'evidence that someone acted with discriminatory intent. The definition is the one '
    'the International Holocaust Remembrance Alliance adopted in 2016, including its '
    'examples. It applies to existing laws that ban discrimination based on race, '
    'religion, color or national origin, and to the rules that increase criminal '
    'penalties when a crime is committed because of those traits. The act says it '
    'creates no new penalty and may not be read to cut back any First Amendment right. '
    'The Assembly passed it 66-33 and it became law as 2025 Wisconsin Act 143.',
    L('civil_rights', 'for'))

# ---------------------------------------------------------------- AB 592
add('AB 592', 1609669, 'house', '2025-11-19', 68, 30,
    'Assembly Bill 592, which sets up a two-year program to train science teachers. '
    'The state education department must work with Wisconsin nonprofit groups to offer '
    'professional development to people who teach science from kindergarten through '
    'grade 12, and it may give those teachers science equipment at no cost for use in '
    'their classrooms. The department has to report by October 2027 on how many teachers '
    'took part. The program only goes ahead if the Legislature\'s budget committee '
    'puts money behind it by June 2026. The Assembly passed it 68-30 and it became law '
    'as 2025 Wisconsin Act 95.',
    L('public_education_quality', 'for'))

# ---------------------------------------------------------------- AB 89
add('AB 89', 1517763, 'house', '2025-03-13', 71, 26,
    'Assembly Bill 89, which raises the penalties for repeat theft and lets prosecutors '
    'add up separate thefts. A person charged with misdemeanor theft can instead be '
    'charged with and convicted of a felony if they have an earlier theft or retail '
    'theft conviction. Someone already facing a felony theft charge can be moved up one '
    'felony class on the same basis. Prosecutors may add together the value of '
    'property from several thefts by the same person in the same prosecution area that '
    'form a single course of conduct, and charge them as one crime. The Assembly passed '
    'it 71-26 and it became law as 2025 Wisconsin Act 106.',
    L('public_safety_and_crime_control', 'for'))

# ---------------------------------------------------------------- SB 106
add('SB 106', 1592545, 'senate', '2025-06-18', 18, 14,
    'Senate Bill 106, which creates a new kind of treatment center in Wisconsin for '
    'children and young people under 21 who need inpatient psychiatric care. The state '
    'health department may certify these centers, inspect them, cap how many exist and '
    'spread them around the state, and it may give grants to help open at least one in '
    'the north and one in the south. Care at a certified center becomes a service '
    'Medicaid pays for, if the federal government approves. A center may use locked '
    'units and may record video in common areas without a child\'s consent, though it '
    'must tell the child and the parent and may not use video instead of one-to-one '
    'watching of a child at high risk of self-harm. The Senate passed it 18-14 and it '
    'became law as 2025 Wisconsin Act 9.',
    L('healthcare_affordability', 'for'))

# ---------------------------------------------------------------- SB 108
add('SB 108', 1592350, 'senate', '2025-06-18', 18, 14,
    'Senate Bill 108, which sets up a statewide online system for sharing a child\'s '
    'safety plan. A safety plan is a document a young person writes in advance, with '
    'help, saying what it looks like when they are in crisis, what calms them down and '
    'who to contact. Schools, police, fire departments, ambulance services, health '
    'providers, county agencies and 911 and 988 call centers can form a network and see '
    'the plan. A plan is only shared if the young person signs a release, the release '
    'can be withdrawn at any time except during a crisis, and the plan expires after a '
    'year. The Senate passed it 18-14 and it became law as 2025 Wisconsin Act 10.',
    L('social_programs_and_welfare', 'for'))

# ---------------------------------------------------------------- SB 182
add('SB 182', 1592319, 'senate', '2025-06-18', 18, 14,
    'Senate Bill 182, which pays for training more emergency medical workers. The state '
    'technical college board must give grants to every technical college that runs a '
    'course for emergency medical responders or emergency medical services '
    'practitioners, and a college that had a waiting list the year before must put the '
    'grant towards taking more students. The act also repays tuition and materials costs '
    'to people who finish the training and get their first license or certificate, or to '
    'the employer who paid for them. It funds a pilot letting 911 centers take live '
    'video from callers. The Senate passed it 18-14 and it became law as 2025 Wisconsin '
    'Act 35.',
    L('healthcare_affordability', 'for'))

# ---------------------------------------------------------------- SB 279
add('SB 279', 1592710, 'senate', '2025-06-18', 17, 15,
    'Senate Bill 279, which has the state justice department give grants to police '
    'agencies to buy a data-sharing platform. The act lists what the platform has to do: '
    'pull together data from common police systems in real time, strip out duplicate '
    'records, search and analyze it, control who can see what, and keep a detailed log '
    'of everything each user looks at. It must be hosted to federal criminal justice '
    'security standards and be running within 90 days. The version that became law stops '
    'any new grants after 30 June 2027. The Senate passed it 17-15 and it became law as '
    '2025 Wisconsin Act 58.',
    L('public_safety_and_crime_control', 'for'))

# ---------------------------------------------------------------- SB 485
add('SB 485', 1635598, 'house', '2026-02-17', 79, 20,
    'Senate Bill 485, which lets group homes, shelter care facilities and residential '
    'care centers for children use video cameras in common areas, entrances and exits '
    'without the child\'s consent. The facility has to tell the child and the child\'s '
    'parent or guardian that it is recording. Every such facility must adopt a policy '
    'for watching over safety. Recordings are confidential and not open to the public, '
    'though the state may review them, and video may not be used instead of one-to-one '
    'watching of a child at high risk of self-harm. The Assembly agreed to it 79-20 and '
    'it became law as 2025 Wisconsin Act 184.',
    L('social_programs_and_welfare', 'for'))

# ---------------------------------------------------------------- SB 56
add('SB 56', 1572080, 'senate', '2025-05-15', 17, 16,
    'Senate Bill 56, which lets the state forgive loans made to privately owned water '
    'utilities when the money is used to replace lead service lines. A lead service line '
    'is the pipe that carries drinking water from the main into a building. Wisconsin '
    'runs a drinking water loan program with federal money, and before this act that '
    'federal money could not be used to write off any part of a loan to a private water '
    'company. The act keeps that bar in place for everything except lead pipe '
    'replacement. The Senate passed it 17-16 and it became law as 2025 Wisconsin Act 8.',
    L('environment_and_public_health', 'for'))

# ---------------------------------------------------------------- SB 785
add('SB 785', 1638011, 'house', '2026-02-18', 66, 32,
    'Senate Bill 785, which requires the state education department to run a public '
    'website where anyone can look up a teaching license by name or number at no cost. '
    'The site must show the name of every license holder under investigation, what the '
    'investigation concluded, whether the person gave up their license while it was '
    'going on, and the name of everyone whose license was revoked. The department '
    'already had to post the name of a license holder under investigation; the act moves '
    'that into one searchable place and adds the outcomes. The Assembly agreed to it '
    '66-32 and it became law as 2025 Wisconsin Act 185.',
    L('public_education_quality', 'for'))

# ---------------------------------------------------------------- SB 825
add('SB 825', 1637587, 'senate', '2026-02-18', 24, 9,
    'Senate Bill 825, which lets a major highway project clear its environmental review '
    'step earlier and with less federal involvement. Before the act, the state '
    'Transportation Projects Commission could not recommend a major highway project '
    'until it was told that a final environmental impact statement or final environmental '
    'assessment had been approved by the Federal Highway Administration. The act lets a '
    'draft do instead of a final one, adds a third option called a categorical '
    'exclusion, which means no environmental study is required at all, and lets the state '
    'transportation department give the approval unless federal money makes federal '
    'approval necessary. The Senate passed it 24-9 and it became law as 2025 Wisconsin '
    'Act 110.',
    L('environment_and_public_health', 'against', 'for'))

json.dump({'judgments': M}, open(os.path.join(OUT, 'judgments.json'), 'w'), indent=1)
print('slots', len(M), '| measures', len({m['measure_id'] for m in M}))
for m in M:
    print(f"  {m['measure_id']:<8} {m['chamber']:<7} roll {m['roll']} {m['yea_description'].count('.')} sentences "
          f"{len(m['yea_description'].split())} words  {m['labels'][0]['slug']}/{m['labels'][0]['yea']}")
