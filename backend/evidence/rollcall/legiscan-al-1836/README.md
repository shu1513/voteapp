# Alabama roll-call votes — LegiScan session 1836 (2022 Regular Session)

Registered as `AL-1836`. The dataset holds 1,265 bills, 1,898 roll calls and
140 people. The dataset and the 1,890 stored roll-call evidence files live outside this
repository at `/Users/shu/legiscan-data/al-1836/` and `/Users/shu/legiscan-data/al-1836-evidence/`;
this directory keeps the curated subset.

## Why this term is in scope

Alabama elects its whole legislature to four-year terms. The members here were elected in November
2018 and served through 2022, so this is the term BEFORE the one that produced the campaign's first
Alabama batches. Roughly 56 percent of them are on the November 2026 ballot, against 87 percent for the
current term, which is what makes the term worth importing and also what makes each roll reach fewer
candidates.

Every one of this session's members who maps to a November 2026 candidate was **already** in the
Alabama crosswalk before this work began. The resolver proposed no new matches for any of the six
registered sessions of the term, so the crosswalk was carried over rather than extended.

## Vocabulary

This session uses the shared 2019-2022 Alabama definitions. The term speaks a different vocabulary
from 2023 and 2024, and confusingly a more similar one to 2025 and 2026. See
`../legiscan-al-1756/CODE-FINDINGS.md` §1: chronology is no guide to which captions a session uses.

The last session of the term, and the one with the highest crosswalk reach of the four regular
sessions: 80 of 142 members are on the November 2026 ballot.

⚠ **This dataset is the contaminated one.** It contains 34 roll calls dated outside 2022, including
four that belong to the 2021 first special session and are attached to this session's HB 2, a
different bill that happens to share the number. Two of those four are divided and would have entered
the batch as votes on the wrong measure. They are excluded, and the full account is in
`../legiscan-al-1756/CODE-FINDINGS.md` §2. Three further rolls are rejected by the parser for tally
mismatches, and one row has no description at all.

One roll is stored but surfaced rather than queued: SB 261's roll 1188976 is a county-delegation vote
with a 13-vote tally in a 105-seat chamber, below the floor-vote threshold. It is 13-0, so it could
not have entered a batch in any case.

## Feed health

Fetch stored 1,890 rows of 1,898 (3 on excluded instrument types, 2 identity
duplicates, 3 rejected by the parser). No committee-sized votes, no unrecorded votes, no file errors, and
**nothing left unclassified**: every description matches a kept or excluded pattern.
One roll is surfaced for a human rather than queued; see above.
No roll call id collides with any of the other fourteen Alabama sessions checked.

## Pool

811 kept floor votes, 41 divided, 25 of those on measures that became law across
17 measures, and 16 on 14 measures that did not. Every divided roll is dispositioned in
`survey/divided-worklist.tsv`, which carries a `roll_number_in_history` column so any misfiled roll
is visible at a glance.

## Identity

The crosswalk holds 142 entries, 80 of them mapped to a November 2026 candidate and the rest
explicit nulls for members who left before 2023. Resolution over all 1,890 stored rolls:
66,362 matched, no member without a crosswalk entry, none out of scope, no file errors. Fan-out is a
median of 56 mapped candidates per House roll and 20 per Senate roll.

Those fan-out figures are about half the current term's, which is the honest cost of going back a
term: the votes are real and the people are the same, but fewer of them are still on the ballot. No
name in this session's people file disagrees with the modern sessions' for any shared member.
