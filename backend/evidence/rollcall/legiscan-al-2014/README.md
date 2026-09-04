# Alabama roll-call votes — LegiScan session 2014 (2023 Regular Session)

Alabama elects its entire legislature to four-year terms, so the members elected in November 2022
sit through 2026 and are the people on the November 2026 ballot. That is what makes the 2023 and
2024 sessions worth importing: they are the same legislators, and 134 of this session's 140 members
were already in the Alabama crosswalk before this work began.

Registered as `AL-2014`. The dataset holds 1,255 bills, 1,485 roll calls and 140 people. The dataset
and the 1,458 stored roll-call evidence files live outside this repository at
`/Users/shu/legiscan-data/al-2014/` and `/Users/shu/legiscan-data/al-2014-evidence/`; this directory
keeps the curated subset.

## ⚠ This session does not speak the modern Alabama vocabulary

The most important fact about the older Alabama sessions is that **the feed rewrote its captions
between 2023 and 2025**, so the patterns that classify 2025 and 2026 correctly classify almost
nothing here. Two differences do the damage.

- **Passage carries no `Motion to` prefix.** In 2023 it is plainly `Read a Third Time and Pass`, in
  four casings and with or without ` as Amended`. Running the modern patterns over this session
  matches a handful of rolls and reports a false empty divided pool. That is exactly what happened on
  a first, ad-hoc pass here, and it is why the registry contract requires a survey per session.
- **There are no Budget Isolation Resolution roll calls at all.** The bill history records
  `On Third Reading in House of Origin` as a stage line with no vote attached, so those resolutions
  were taken by voice in 2023. That is why this session stores 1,458 rolls where 2024 stores 2,106
  while passing a comparable number of bills.

The one family that looks like a Budget Isolation Resolution is not one: the 26 rolls captioned
`Passed by House of Origin` are all adoptions of a SPECIAL ORDER CALENDAR resolution — the chamber
setting its own order of business — and they are excluded by name.

## Feed health

The cleanest tier, matching 2025 and 2026: no repeated roll call ids, no summary-only rolls, no
tally mismatches, no committee-sized votes, no parse errors, and **no roll call id collides with any
other Alabama session in scope**. Fetch stored 1,458 rows (1,485 minus 26 on excluded instrument
types and 1 identity duplicate), with **nothing left unclassified** — every one of the 19 surveyed
description families matches a kept or excluded pattern.

Every divided roll's printed roll call number was checked against its own bill's history, the test
the 2026 session made necessary. **This session carries no roll call numbers in its descriptions at
all**, so the check is vacuous here; the 2024 session is where it bites.

## Pool

1,003 kept floor votes, 28 divided, 21 of those on measures that became law, across 18 measures.
That is the largest divided-and-enacted pool of any Alabama session in scope. Every divided roll is
dispositioned in `survey/divided-worklist.tsv`.

## Identity

The crosswalk holds 140 entries: 134 carried over unchanged from the 2025 and 2026 crosswalks and 6
explicit nulls. Resolution over all 1,458 stored rolls: 70,415 matched, no member without a
crosswalk entry, none out of scope, no file errors. Fan-out is a median of 83 mapped candidates per
House roll and 26 per Senate roll.

The 6 nulls are all members who left the legislature mid-term and are not on the November 2026
ballot, each confirmed against the candidate pool rather than assumed: Fred Plump (House 55, resigned
2023), David Cole (House 10, resigned 2023), Kyle South (House 16, resigned 2023), John Rogers
(House 52, resigned 2024), Greg Reed (Senate 5, left in 2025) and Clay Scofield (Senate 9, resigned
2023).

No name in this session's people file disagrees with the modern sessions' for any shared member, so
the carry-over is safe: 0 disagreements over all shared ids across all five Alabama people files.
