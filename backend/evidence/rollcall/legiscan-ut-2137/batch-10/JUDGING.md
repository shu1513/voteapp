# Utah 2025 batch-10 — the direction calls, judged

The user asked on 2026-09-11 for each held measure to be investigated deeply
and judged, and for HB 267 and SB 327 to be redone now that `labor_rights`
exists. Sources: the enrolled text read through `ut_text.py` (ground truth),
Utah's bill JSON action list, Utah's current code pages, and the official
summary as an index only. The 2026 measures are in `legiscan-ut-2214/batch-10`.

## Precedents applied

- **Fluoride:** the user credits the evidence of harm, so fluoride gets no
  environment stance (FL SB 700).
- **Rules that sort people by sex at birth:** civil_rights, yes = against. This
  was settled for Wyoming SF 44 (sports) and is applied to the same class here.
- **Voter-roll upkeep and ballot checks:** election_integrity, yes = for
  (Wyoming HB 318, Wisconsin AB 595).
- **Initiatives:** election_integrity/for only when every provision is
  verification (FL HB 1205). Otherwise the Arkansas line applies: rules on who
  may make law carry no honest direction.
- **Immigration enforcement:** immigration, yes = against, because the area
  says "welcome immigration" (TX SB 8).
- **Shields for religious expression:** civil_rights/for (TX SB 965, SB 223).
- **Mixed rule:** strands that point both ways in one area get no label.

## Imported

- **HB 267 Public Sector Labor Union Amendments**, House 42-32 and Senate
  16-13: `labor_rights` / against.
  - It barred public employers from recognizing unions as bargaining agents or
    signing union contracts.
  - It barred public money or property for union activity.
  - It required yearly spending reports from unions paid by payroll deduction.
  - Signed 2/14/2025, then repealed by 2025 Second Special Session HB 2001,
    signed 12/11/2025. The description says so and uses the past tense.
  - A hand-written sponsorship record for Jordan Teuscher describes a
    sponsorship, not a vote, so it is not a duplicate.
- **SB 327 Public Sector Labor Organization Amendments**, Senate 19-5
  (concurrence): `labor_rights` / against.
  - It limits the "right to bargain collectively" clause in 34-34-16 to private
    employees and defines terms for HB 267's ban.
  - It takes effect only with HB 267. The description says it was tied to
    HB 267 and does not claim whether it ever took effect.
- **HB 233 School Curriculum**, House 52-13 and Senate 18-8:
  `womens_reproductive_rights` / against. It does one thing: it bars an entity
  that performs elective abortions, or its affiliate, from teaching health
  topics or supplying health materials in state-funded schools. There is no
  counter-strand.
- **HB 390 Religious Expression in Higher Education**, Senate 21-6:
  `civil_rights` / for. It bars colleges from denying benefits to religious,
  political or ideological student groups, including over leader-belief
  requirements.
- **HB 226 Criminal Amendments**, Senate 21-8: `immigration` / against. Sheriffs
  and the prison must notify DHS before releasing an unlawfully present person
  after a felony or listed class A sentence, and coordinate the transfer. It
  also adds a flight-risk presumption for pretrial release.
- **HB 252 State Custody Amendments**, Senate 19-6: `civil_rights` / against.
  - Jails, prisons and juvenile custody may not start cross-sex hormones or
    sex-characteristic surgery.
  - Juvenile custody also may not start puberty blockers.
  - Housing is separated by sex.
- **HB 269 Privacy Protections in Sex-designated Areas**, Senate 20-7:
  `civil_rights` / against.
  - College sex-designated dwelling units follow sex at birth, with an original
    birth certificate as the defense.
  - The medical-treatment defense is deleted from both school privacy spaces
    (63G-31-301) and public changing rooms (63G-31-302). Changing rooms keep
    the separate amended-certificate-plus-surgery defense; schools are left
    with the original birth certificate only.
- **HB 300 Amendments to Election Law**, House 56-15 (concurrence) and Senate
  19-10: `election_integrity` / for.
  - The ID digits on return envelopes and the opt-in mail ballot from 2029 are
    both confirmed in the current 20A-3a-202 and 20A-3a-204.
  - The request lapse (two missed general elections, or eight years) was
    confirmed in the enrolled text.
  - It requires SAVE registration and a voter-roll anomaly review.
  - The access objection maps to no area (the FL HB 1205 reasoning).

## Dropped

- **HB 81 Fluoride Amendments** bans adding fluoride to public water. The
  user's fluoride call applies and no other strand carries an area, so it is
  dropped as having no honest direction.
- **HB 77 Flag Display Amendments** is a facially neutral rule on what
  government may display. No area measures government speech, so it is dropped
  as having no direction.
- **HB 281 Health Curriculum** is mixed. It adds parental consent and parental
  topic control for school mental health services. It also rewrites health
  instruction around the "success sequence" and abstinence. The strands sit in
  several areas and pull different ways.
- **SB 73 Statewide Initiatives** falls under the Arkansas line. Its funding
  description and publication rules are not verification. The lieutenant
  governor may reject an initiative whose funding looks inadequate.
- **HB 209 Homeschool Amendments** has no area. It replaces the affidavit with
  a one-time notice. That also drops the bar on a parent convicted of child
  abuse, but no area covers home education.
- **HB 479 Student Athlete Revisions** is mixed for labor. Colleges may pay
  athletes directly, but athletes are declared not employees.
