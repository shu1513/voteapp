# Alabama roll-call votes — LegiScan session 1857 (2021 Second Special Session)

Registered as `AL-1857`. The dataset holds 74 bills, 54 roll calls and
138 people. The dataset and the 50 stored roll-call evidence files live outside this
repository at `/Users/shu/legiscan-data/al-1857/` and `/Users/shu/legiscan-data/al-1857-evidence/`;
this directory keeps the curated subset.

## Why this term is in scope

Alabama elects its whole legislature to four-year terms. The members here were elected in November
2018 and served through 2022, so this is the term BEFORE the one that produced the campaign's first
Alabama batches. Roughly 57 percent of them are on the November 2026 ballot, against 87 percent for the
current term, which is what makes the term worth importing and also what makes each roll reach fewer
candidates.

Every one of this session's members who maps to a November 2026 candidate was **already** in the
Alabama crosswalk before this work began. The resolver proposed no new matches for any of the six
registered sessions of the term, so the crosswalk was carried over rather than extended.

## Vocabulary

This session uses the shared 2019-2022 Alabama definitions. The term speaks a different vocabulary
from 2023 and 2024, and confusingly a more similar one to 2025 and 2026. See
`../legiscan-al-1756/CODE-FINDINGS.md` §1: chronology is no guide to which captions a session uses.

The redistricting session, held in November 2021. Every one of its 11 divided floor votes is on a
measure that became law, so it has no batch-02.

It drew four maps at once: the congressional districts, the State House, the State Senate and the
State Board of Education. **The congressional map drawn here is the one the Supreme Court held
likely unlawful in Allen v. Milligan**, and which the 2023 second special session (`AL-2060`) was
called to redraw. The session also carried two bills on COVID vaccine mandates.

## Feed health

Fetch stored 50 rows of 54 (4 on excluded instrument types, 0 identity
duplicates). No committee-sized votes, no unrecorded votes, no file errors, and
**nothing left unclassified**: every description matches a kept or excluded pattern.

No roll call id collides with any of the other fourteen Alabama sessions checked.

## Pool

23 kept floor votes, 11 divided, 11 of those on measures that became law across
6 measures. Every divided roll is dispositioned in
`survey/divided-worklist.tsv`, which carries a `roll_number_in_history` column so any misfiled roll
is visible at a glance.

## Identity

The crosswalk holds 139 entries, 79 of them mapped to a November 2026 candidate and the rest
explicit nulls for members who left before 2023. Resolution over all 50 stored rolls:
1,721 matched, no member without a crosswalk entry, none out of scope, no file errors. Fan-out is a
median of 56 mapped candidates per House roll and 19 per Senate roll.

Those fan-out figures are about half the current term's, which is the honest cost of going back a
term: the votes are real and the people are the same, but fewer of them are still on the ballot. No
name in this session's people file disagrees with the modern sessions' for any shared member.
