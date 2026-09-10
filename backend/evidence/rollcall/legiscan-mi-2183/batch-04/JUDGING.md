# Michigan batch-04 — judging

Not-enacted measures, so there is no Public Act. Descriptions come from the
**engrossed print**, the text the House actually passed, with the nonpartisan
House Fiscal Agency analysis of that version used for orientation.

## The version check

HB 4159 adopted substitute h-1 on the floor, so its "as introduced" summary was
stale and the engrossed print was read instead. The other seven passed the text
their committee reported.

**⚠ The unmarked-new-provision trap appeared again.** HB 4159's whole substantive
addition — new subsection (2)(c), the science of reading requirement — prints
with no strike-through or underline at all. Its only marked changes are style
edits turning "shall" into "must". Reading the markup alone would have suggested
the bill does nothing. This is the third batch in which a wholly new Michigan
provision printed unmarked.

## The measures

### HB 4066 — civil_rights, a yes vote is against
Requires a district, intermediate district or charter school taking part in
school sports to label each team as for females, for males, or mixed, with sex
meaning what appears on a student's original birth certificate.

**This is not the same bill as HB 4469**, which batch-03 covered. HB 4469 provides
that the civil rights act does not stop a school from setting eligibility that
way; HB 4066 affirmatively requires the labelling. The two passed the House on
the same day, 2025-05-22, one vote apart — 58-46 and 59-45. The descriptions are
written so the difference is visible, and the duplicate sweep keyed on the tally
so records were not swapped between them.

### HB 4159, HB 5819, HB 5820, HB 5081 — public_education_quality, a yes vote is for
HB 4159 requires the State Board's model K-6 reading and writing standards to
rest on the science of reading and to use a code emphasis approach covering
decoding, phonics, vocabulary, fluency, spoken language, comprehension and word
recognition, with methods grounded in structured literacy.

HB 5819 and HB 5820 are deadline bills from the same package. HB 5819 moves the
date by which schools must use compliant literacy materials forward from the
2027-2028 school year to July 2026. HB 5820 moves the date by which teacher
preparation programs must include literacy training forward from September 2027
to July 2026, after which the Department of Education could not approve a
program that did not meet it. Each description names its own deadline so the
two records do not read as the same thing.

HB 5081 requires every district, intermediate district and charter school to
employ at least one teacher tasked with helping students with dyslexia, holding
at least 30 hours of Orton-Gillingham training, by January 2027. The description
explains what the approach is, because the name says nothing to a reader.

### HB 4222 and HB 4315 — public_safety_and_crime_control, a yes vote is for
HB 4315 requires the State Police, with the Attorney General and the Department
of Education, to build school safety and security training materials and best
practices and to provide annual training to school resource officers, safety
staff and all school staff in public and private schools.

HB 4222 requires every school to have a crisis team and private schools to have
an emergency operations plan and to carry out the periodic review.

**HB 4222 has a strand that runs the other way, and the description says so.** It
also stretches the review interval from two years to three, and lets a private
school's governing board opt out of the new requirements entirely. The direction
is still for, because the bill's core is new duties where none existed, but the
description states the longer interval and the opt-out rather than presenting the
bill as a straight tightening.

### HB 4369 — environment_and_public_health, a yes vote is for
From July 2028, schools could not give out or sell food containing brominated
vegetable oil, potassium bromate, propylparaben, or the dyes Red 40, Green 3,
Blue 1, Blue 2 and Yellow 6. The description lists the substances rather than
saying "certain additives", so a reader knows what is covered. The analysis notes
that federal approval for brominated vegetable oil was already revoked in 2024;
that background is not in the description because it is context rather than
something the bill does.

## Checks run before importing

- Builder guard for the not-enacted scope: **0 failures**.
- Plain-language lint over all 16 descriptions: **0 warnings**.
- Reading level measured: median Flesch-Kincaid grade **5.9**, maximum 9.0.
- British spelling scan clean.
- Every description cites its own roll's tally and no other.

## The run

| step | result |
| --- | --- |
| judge, dry run then live | 8 rows, 8 updated |
| import, dry run | 8 files, 747 planned inserts, 0 errors |
| import, live | 8 imported, 747 inserts, 0 errors, 0 notified |
| re-run | 747 unchanged |

Run stamp `2026-09-10T04:56:46.240Z`; the dry run's stamp matches zero rows.

Reconciled three ways at 747: import report, run-stamp query, and Michigan
roll-call records going 1,525 to 2,272. 96 candidates rather than 113 because
every roll in this batch is a House vote.

## The duplicate sweep

**7 retired**, four on HB 4066 and three on HB 5081.

The four HB 4066 records include the three that batch-03's sweep correctly
**refused** to retire as HB 4469 duplicates, because they cite "58 to 46" rather
than 59-45. They belong to HB 4066 and are retired here. The tally rule worked in
both directions: it rejected them when they were offered as the wrong bill, and
confirmed them when the right bill came along.

## Production

Production holds zero Michigan roll-call records. Nothing here touches it.
