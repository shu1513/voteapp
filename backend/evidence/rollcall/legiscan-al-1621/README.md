# Alabama roll-call votes — LegiScan session 1621 (2019 Regular Session)

Registered as `AL-1621`. The dataset holds 1,492 bills, 1,634 roll calls and
140 people. The dataset and the 1,555 stored roll-call evidence files live outside this
repository at `/Users/shu/legiscan-data/al-1621/` and `/Users/shu/legiscan-data/al-1621-evidence/`;
this directory keeps the curated subset.

## Why this term is in scope

Alabama elects its whole legislature to four-year terms. The members here were elected in November
2018 and served through 2022, so this is the term BEFORE the one that produced the campaign's first
Alabama batches. Roughly 53 percent of them are on the November 2026 ballot, against 87 percent for the
current term, which is what makes the term worth importing and also what makes each roll reach fewer
candidates.

Every one of this session's members who maps to a November 2026 candidate was **already** in the
Alabama crosswalk before this work began. The resolver proposed no new matches for any of the six
registered sessions of the term, so the crosswalk was carried over rather than extended.

## Vocabulary

This session uses the shared 2019-2022 Alabama definitions. The term speaks a different vocabulary
from 2023 and 2024, and confusingly a more similar one to 2025 and 2026. See
`../legiscan-al-1756/CODE-FINDINGS.md` §1: chronology is no guide to which captions a session uses.

This is the first session of the term and the one with the largest kept-floor pool, 1,017 votes.
It records **no Budget Isolation Resolution votes at all**; those were taken by voice until 2021.

One roll is rejected by the parser: roll 826544 says 51 members voted yes while its member list holds
25. See `../legiscan-al-1756/CODE-FINDINGS.md` §3.

## Feed health

Fetch stored 1,555 rows of 1,634 (42 on excluded instrument types, 36 identity
duplicates, 1 rejected by the parser). No committee-sized votes, no unrecorded votes, no file errors, and
**nothing left unclassified**: every description matches a kept or excluded pattern.

No roll call id collides with any of the other fourteen Alabama sessions checked.

## Pool

1,017 kept floor votes, 26 divided, 18 of those on measures that became law across
15 measures, and 8 on 8 measures that did not. Every divided roll is dispositioned in
`survey/divided-worklist.tsv`, which carries a `roll_number_in_history` column so any misfiled roll
is visible at a glance.

## Identity

The crosswalk holds 140 entries, 74 of them mapped to a November 2026 candidate and the rest
explicit nulls for members who left before 2023. Resolution over all 1,555 stored rolls:
46,763 matched, no member without a crosswalk entry, none out of scope, no file errors. Fan-out is a
median of 49 mapped candidates per House roll and 19 per Senate roll.

Those fan-out figures are about half the current term's, which is the honest cost of going back a
term: the votes are real and the people are the same, but fewer of them are still on the ballot. No
name in this session's people file disagrees with the modern sessions' for any shared member.
