# Kentucky roll-call import — 2026 Regular Session (LegiScan session 2247)

The 60-day session, adjourned in April 2026. The LegiScan dataset is dated
12 July 2026 and is complete. The 2025 Regular Session is a separate LegiScan
session with its own directory, `legiscan-ky-2179/`; read its README first, as
it carries the sources and the hazards common to both.

1,737 bills, 917 roll calls, 138 people. 917 raw descriptions fold to 19
families, listed in `survey/desc-families.tsv`. As in 2025 there are no
committee votes and every tally is a whole-chamber tally.

## This session's labels are not the 2025 labels

**LegiScan's Kentucky vocabulary flips between sessions, and the difference is
not cosmetic.** In 2026 the label `House: Veto Override` is the dominant House
family at 415 rolls — LegiScan's name for every substantive House floor vote,
passage and concurrence and genuine override alike — while `House: Third
Reading` falls to 29. Applying the 2025 rules here would drop 415 House votes
and keep 29 duplicates.

Checked against Kentucky's own record, all three of these arrive under the
single label `House: Veto Override`:

| Bill | Roll | What Kentucky says |
| --- | --- | --- |
| HB 398 | RCS# 46 | Pass |
| HB 398 | RCS# 373 | Final Passage |
| HB 2 | RCS# 455 | Veto Override |

So each Kentucky session must be surveyed on its own. Never carry a Kentucky
description rule from one session to another.

The Senate, unlike the House, does name its override question in this session:
36 rolls read `Senate: Veto Override`, and 30 of them sit on a bill whose
history records an override.

## Duplicate rolls

The 2026 feed holds **31 duplicate rolls**, which the 2025 feed does not: the
same chamber and sequence number appear under two different roll call ids with
an identical bill, date and tally. Each pair names `House: Veto Override` plus
one of `House: Third Reading` (29 pairs), `House: Adopt HFA 1` (1) or `House:
Co-Sponsor` (1).

The shared identity key includes the description, so it does **not** collapse
them. Excluding the three partner spellings resolves 29 of the 31 by rule. The
co-sponsor twin sits on a simple resolution the measure-type filter drops before
the configuration is consulted, and the floor-amendment twin (HB 84, RCS# 40) is
excluded by its exact sequence number — see `CODE-FINDINGS.md`.

## Ground truth

Same document as 2025, under the `26rs` path:
`https://apps.legislature.ky.gov/record/26rs/<bill>/vote_history.pdf`

## Scope

Under the campaign's Kentucky divided gate (nay votes at least 15 percent of
votes cast — the reasoning is in the 2179 README), this session yields **155
divided-and-enacted rolls on 52 measures**, 34 of them enacted over a veto.

**36 bills in the session became law over a veto.** Thirty were whole-bill
overrides; the other six are appropriations bills where the Governor struck line
items and the legislature overrode those in whole or in part (HB 2, HB 500,
HB 501, HB 503, HB 504, HB 757). The earlier count of 30 in this file counted
only the whole-bill wording.

## The fetch and the crosswalk

The fetch stored **840 rows: 791 floor votes (473 House, 367 Senate) plus 49
excluded-question votes kept for audit**. 77 votes were dropped by measure type.
No committee votes, no duplicate identities, nothing surfaced, no file errors.
Run id `rollcall-legiscan-fetch-ky-2247-20260902T060100Z`.

**The crosswalk is the 2025 session's file**, at
`../legiscan-ky-2179/crosswalk.json`. LegiScan people_ids are stable across
sessions, and only one seat changed hands between the two: David Yates
(people_id 22180) held Senate District 37 through the 2025 session and left in
October 2025, and Gary Clemons (people_id 26549) holds it in this one. Clemons
maps to null — Kentucky staggers its Senate and District 37 is odd-numbered, so
the seat is not on the November 2026 ballot. No other member changed name, role
or district. Each session therefore reports exactly one crosswalk person absent
from its own snapshot, which is expected.

Resolving all 840 files through the crosswalk: matched 44,382, unmatched and
reviewed 11,857, **no_crosswalk 0, zero-match rolls 0, file errors 0**. Fan-out
is a House median of 83 matched members per roll (26 to 88) and a Senate median
of 18 (13 to 18).

## Layout

- `survey/` — the fetch survey report, the folded description histogram, and
  `divided-enacted-worklist.tsv`, which lists all 155 pool rolls with a
  disposition for each.
- `batch-01/` — the first batch: 10 measures, 19 rolls, 997 records.
- `legiscan-people-ky-2247.json` — this session's people snapshot.
- `CODE-FINDINGS.md` — defects recorded but deliberately not fixed.
