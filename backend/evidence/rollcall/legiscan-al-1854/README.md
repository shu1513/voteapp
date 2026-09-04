# Alabama roll-call votes — LegiScan session 1854 (2021 First Special Session)

Registered as `AL-1854`. The dataset holds 40 bills, 19 roll calls and
138 people. The dataset and the 19 stored roll-call evidence files live outside this
repository at `/Users/shu/legiscan-data/al-1854/` and `/Users/shu/legiscan-data/al-1854-evidence/`;
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

The prison construction session, held in late September and early October 2021. It is tiny and
unusually contested: **6 of its 11 kept floor votes are divided AND on measures that became law**,
the highest such share of any Alabama session in the campaign. Nothing divided died, so this session
has no batch-02.

⚠ Four of this session's roll calls also appear in the 2022 regular session's dataset, attached to
that session's own HB 2, which is a different bill. The copies here are the correct ones. See
`../legiscan-al-1756/CODE-FINDINGS.md` §2.

## Feed health

Fetch stored 19 rows of 19 (0 on excluded instrument types, 0 identity
duplicates). No committee-sized votes, no unrecorded votes, no file errors, and
**nothing left unclassified**: every description matches a kept or excluded pattern.

No roll call id collides with any of the other fourteen Alabama sessions checked.

## Pool

11 kept floor votes, 6 divided, 6 of those on measures that became law across
4 measures. Every divided roll is dispositioned in
`survey/divided-worklist.tsv`, which carries a `roll_number_in_history` column so any misfiled roll
is visible at a glance.

## Identity

The crosswalk holds 138 entries, 79 of them mapped to a November 2026 candidate and the rest
explicit nulls for members who left before 2023. Resolution over all 19 stored rolls:
800 matched, no member without a crosswalk entry, none out of scope, no file errors. Fan-out is a
median of 56 mapped candidates per House roll and 19 per Senate roll.

Those fan-out figures are about half the current term's, which is the honest cost of going back a
term: the votes are real and the people are the same, but fewer of them are still on the ballot. No
name in this session's people file disagrees with the modern sessions' for any shared member.
