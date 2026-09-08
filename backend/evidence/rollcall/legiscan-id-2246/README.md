# Idaho, 2026 Regular Session (LegiScan session 2246)

Dataset cut 2026-06-28, after the session adjourned: 817 bills, 852 roll calls,
129 people. Registry key `ID-2246`.

A second session for one state needs its own registry entry, because the `ID`
entry pins session 2168 and flipping it would strand the 2025 work.

**No Idaho roll call has been judged or imported yet**, in either session.
Production holds zero Idaho roll-call records, and so does the local database.

The vocabulary, the question-class caveat, the concurrence gap, the judging
source and the version-check rules are identical to the 2025 session and are
written up once in `../legiscan-id-2168/README.md`. Both entries share one
hoisted `IDAHO_KEPT_QUESTIONS` list rather than two copies that could drift.
This file records only what differs.

## Description histogram

`House Third Reading` 461, `Senate Third Reading` 391. Nothing else, nothing
unmatched, no committee votes.

## Feed health and tally audit

Cleanest tier, same as 2025. **The tally audit found zero disagreements with
Idaho's own history in this session**, over all 643 rolls carrying a tallied
history line, so no roll is held here.

## Pool

273 divided rolls; 264 on kept bill types; **196 divided and enacted across 115
measures** (99 House, 97 Senate), 77 divided in both chambers.

After filter 4 and the appropriations exclusion,
`survey/divided-enacted-worklist.tsv` reads:

| disposition | rows |
| --- | --- |
| candidate:unbatched | 92 (57 measures) |
| excluded:appropriations | 98 |
| out-of-gate:not-divided | 496 |
| out-of-gate:not-enacted | 111 |
| superseded:last-roll-not-divided | 2 |

## Crosswalk

105 entries, 90 mapped, 15 null. Validation over all 818 stored rolls: 36,308
matched, 0 no_crosswalk, 0 out_of_scope, 0 file errors, 0 zero-match rolls.
Fan-out: House median 58, Senate median 29.

**The two crosswalks differ because four seats changed hands mid-term.** Todd
Achilles left HD-016B and Anne Haws took it; Lance Clow left HD-025A and Don
Hall took it; Wendy Horman left HD-032B and Erin Bingham took it; Kevin Andrus
left HD-035A and Michael Veile took it. Achilles is mapped in 2025 only, because
he is not in this session's people file.

Hand-added here: Ted Hill, Dave Lent and Scott Grow for the same reasons as
2025, plus **Anne Haws** HD-016B (our roster has Annie, and `anne` is not a
prefix of `annie`) and **Michael Veile** HD-035A (our roster has Mike).
Deliberately not linked: Mark Sauter HD-001A.

## Status

Worked out. Three batches, 24 measures, 40 roll calls, 1,730 candidate records
in the local database, and 52 chamber rows dropped with a reason in
`survey/divided-enacted-worklist.tsv`. Zero rows are left unbatched.

Together with the 2025 session the Idaho campaign holds 3,810 records from 90
rolls across 58 measures and 91 candidates. Production holds none of it; the
promotion is a separate step.

## Version work

**99 of the 115 divided-and-enacted measures have no engrossed print.** 16 carry
one and need per-roll version work. All 115 carry a `Session Law Chapter` line.

## Sessions deliberately not registered

Idaho has 18 further sessions in `getDatasetList`, back to 2010. None is
registered. They are named here rather than dismissed, because a stale
"out of scope" note is what stops the next session from looking: 2119 (2024),
2011 (2023), 1994 (2022 special), 1954 (2022), 1800 (2021), 1766 (2020
special), 1725 (2020), 1629 (2019), 1525 (2018), 1406 (2017), 1198 (2016),
1162 (2015 special), 1126 (2015), 1080 (2014), 998 (2013), 922 (2012), 101
(2011), 51 (2010).

Before opening any of them, measure candidate reach first, the way the Alaska
1397 session was measured: an old session is worth nothing if almost none of its
voters is a current candidate.
