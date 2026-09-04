# Alabama roll-call votes — LegiScan session 1756 (2021 Regular Session)

Registered as `AL-1756`. The dataset holds 1,500 bills, 2,513 roll calls and
139 people. The dataset and the 2,451 stored roll-call evidence files live outside this
repository at `/Users/shu/legiscan-data/al-1756/` and `/Users/shu/legiscan-data/al-1756-evidence/`;
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

The largest divided pool of any Alabama session in the campaign: 46 divided rolls, 23 of them on
measures that became law. It is also the first session of the term to record Budget Isolation
Resolution votes, in the `HBIR:/SBIR: <sponsor> motion to Adopt` spelling.

Three rolls are rejected by the parser for stating a yes count their member lists cannot support
(996001, 996025 and 1025051), and one roll's caption is truncated to `Motion to Roll Call 6`. Both
classes are documented in `CODE-FINDINGS.md`, which is filed in this directory because it covers the
whole term.

## Feed health

Fetch stored 2,451 rows of 2,513 (54 on excluded instrument types, 5 identity
duplicates, 3 rejected by the parser). No committee-sized votes, no unrecorded votes, no file errors, and
**nothing left unclassified**: every description matches a kept or excluded pattern.

No roll call id collides with any of the other fourteen Alabama sessions checked.

## Pool

998 kept floor votes, 46 divided, 23 of those on measures that became law across
21 measures, and 23 on 20 measures that did not. Every divided roll is dispositioned in
`survey/divided-worklist.tsv`, which carries a `roll_number_in_history` column so any misfiled roll
is visible at a glance.

## Identity

The crosswalk holds 141 entries, 76 of them mapped to a November 2026 candidate and the rest
explicit nulls for members who left before 2023. Resolution over all 2,451 stored rolls:
79,306 matched, no member without a crosswalk entry, none out of scope, no file errors. Fan-out is a
median of 51 mapped candidates per House roll and 20 per Senate roll.

Those fan-out figures are about half the current term's, which is the honest cost of going back a
term: the votes are real and the people are the same, but fewer of them are still on the ballot. No
name in this session's people file disagrees with the modern sessions' for any shared member.
