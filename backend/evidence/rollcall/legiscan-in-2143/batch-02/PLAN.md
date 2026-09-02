# Indiana batch-02 — selection

**1 measure, 2 rolls, 95 records across 95 candidates.** Imported on the local `voteapp`
database 2026-09-02. Production untouched.

This batch is deliberately small. Eight measures were shortlisted from the 121 open rolls;
seven were dropped after reading the source, and the reasons are worth more than the
volume would have been.

## Kept

**SB 526, absentee ballot retraction** — `election_integrity`, a yes vote scores `for`.
This is Indiana's first coverage of that area. Both rolls are the conference committee
report of 2025-04-24, which is the enacted text (the act is signed `SEA 526 — CC 1`):
House 1556507 (65-26) and Senate 1556915 (40-10).

## Dropped, with reasons

**SB 281, expungement — filter 5.** The act runs in both directions inside
`public_safety_and_crime_control`. It *widens* expungement by allowing it for official
misconduct where the person is not an elected official and the prosecutor consents, and it
*narrows* it by barring expungement for unlawful firearm possession by a serious violent
felon, barring it for certain commercial driver's licence records, and extending the
elected-official restrictions to judicial officers. It also opens juvenile delinquency
records to law enforcement and prosecutors. No single direction is honest.

**SB 442, instruction on human sexuality — filter 5.** The act requires consent
instruction, which reads one way, and also mandates a presentation on human growth and
development during pregnancy, adds governing-body approval of curricular materials and
adds parental consent-form requirements, which read another. Contested direction.

**SB 249, teacher compensation — filter 5.** The change lets a school corporation pay a
supplement above the compensation plan, and that supplement is expressly *not subject to
collective bargaining*, with a portion of bargaining revenue excluded for the purpose. Pay
flexibility and narrowed bargaining point opposite ways.

**HB 1666, ownership of health care providers — VERSION TRAP, and the most interesting
drop of the batch.** The enrolled act is a clean `corporate_accountability` measure:
ownership reporting by hospitals, health care entities, insurers, third party
administrators and pharmacy benefit managers, an annual published report, and authority for
the Attorney General to investigate market concentration. But its **only divided roll is
the House third reading of 2025-02-13, and the House-passed text is not the act.** Diffing
the engrossed House version against the enrolled act gives a similarity of 0.20, and the
House version contained an entire regime the enrolled act does not: *"An Indiana health
care entity may not engage in a merger or acquisition with another health care entity
unless the health care entity has received approval from the office of the attorney
general"*. The Senate replaced pre-approval with investigation. Describing the House vote
from the enacted text would have credited 83 representatives with a much weaker bill than
they voted for. Under the California SB 707 precedent, a chamber whose only divided vote
predates the last amendment loses that roll, and for a one-chamber measure that drops the
measure.

**HB 1461 (road funding) and HB 1125 (earned wage access)** were shortlisted but their
enrolled acts are 1,474 and 1,765 lines. They are held for a later batch rather than judged
from excerpts — the rule this campaign learned from California batch-05 is that a measure is
not judged until its whole enacted text has been read.

## Left for batch-03

63 measures and 119 divided-and-enacted rolls, each dispositioned in
`../survey/divided-enacted-worklist.tsv`. Twelve of those rolls are flagged because their
LegiScan tally has no exact match in the official history, which is the signal for the
member-list problem in `../CODE-FINDINGS.md` section 2.
