# Alabama roll-call votes — LegiScan session 1706 (2020 Regular Session)

Registered as `AL-1706`. The dataset holds 1,062 bills, 683 roll calls and
140 people. The dataset and the 648 stored roll-call evidence files live outside this
repository at `/Users/shu/legiscan-data/al-1706/` and `/Users/shu/legiscan-data/al-1706-evidence/`;
this directory keeps the curated subset.

## Why this term is in scope

Alabama elects its whole legislature to four-year terms. The members here were elected in November
2018 and served through 2022, so this is the term BEFORE the one that produced the campaign's first
Alabama batches. Roughly 54 percent of them are on the November 2026 ballot, against 87 percent for the
current term, which is what makes the term worth importing and also what makes each roll reach fewer
candidates.

Every one of this session's members who maps to a November 2026 candidate was **already** in the
Alabama crosswalk before this work began. The resolver proposed no new matches for any of the six
registered sessions of the term, so the crosswalk was carried over rather than extended.

## Vocabulary

This session uses the shared 2019-2022 Alabama definitions. The term speaks a different vocabulary
from 2023 and 2024, and confusingly a more similar one to 2025 and 2026. See
`../legiscan-al-1756/CODE-FINDINGS.md` §1: chronology is no guide to which captions a session uses.

The COVID-shortened session, and by a wide margin the thinnest in scope: 1,062 bills but only 683
roll calls, against 1,634 the year before. Only 5 of its 445 kept floor votes are divided, and only
2 of those are on a measure that became law.

It is registered for completeness of the term rather than for yield, and its batch is correspondingly
small. Like 2019, it records no Budget Isolation Resolution votes.

## Feed health

Fetch stored 648 rows of 683 (25 on excluded instrument types, 10 identity
duplicates). No committee-sized votes, no unrecorded votes, no file errors, and
**nothing left unclassified**: every description matches a kept or excluded pattern.

No roll call id collides with any of the other fourteen Alabama sessions checked.

## Pool

445 kept floor votes, 5 divided, 2 of those on measures that became law across
1 measures, and 3 on 3 measures that did not. Every divided roll is dispositioned in
`survey/divided-worklist.tsv`, which carries a `roll_number_in_history` column so any misfiled roll
is visible at a glance.

## Identity

The crosswalk holds 140 entries, 75 of them mapped to a November 2026 candidate and the rest
explicit nulls for members who left before 2023. Resolution over all 648 stored rolls:
18,307 matched, no member without a crosswalk entry, none out of scope, no file errors. Fan-out is a
median of 41 mapped candidates per House roll and 19 per Senate roll.

Those fan-out figures are about half the current term's, which is the honest cost of going back a
term: the votes are real and the people are the same, but fewer of them are still on the ballot. No
name in this session's people file disagrees with the modern sessions' for any shared member.
