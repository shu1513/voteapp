# LegiScan roll calls — California, 2025-2026 Regular Session (session 2172)

Phase-4 state #3, after Texas (`legiscan-tx-2160/`) and Georgia (`legiscan-ga-2167/`).
Plan: `docs/plans/roll-call-vote-import.md` §5 phase 4. Local `voteapp` only — **prod untouched**.

## Dataset

LegiScan bulk dataset for session **2172** (CA `state_id` = 5), downloaded 2026-08-26 with
`getDatasetRaw` and the key from the main checkout's `backend/.env`. It lives outside the repo at
`/Users/shu/legiscan-data/ca-2172/` (ZIP + extracted), with the 5,328 per-roll evidence JSONs at
`/Users/shu/legiscan-data/ca-2172-evidence/`. **5,057 bills / 19,942 roll calls / 160 people.**

The session is still LIVE (sine die is in 2026): vote dates run 2025-01-23 … 2026-08-20.

## What the survey established

Written up in full in the `CA` entry of `src/pipeline/rollcall/legiscanStateConfigs.ts`. The two
findings that needed code:

1. **The lower chamber is `A`, not `H`.** California calls it the Assembly and LegiScan prints the
   state's own abbreviation, so all 9,948 Assembly rolls failed to parse until `parseLegiscanRollCall`
   learned that both letters mean `house`. NV/NJ/NY/WI will need the same.
2. **A desc ending in a bare ` Reconsider` is the vote GRANTING reconsideration**, not the question —
   excluded by rule. See `CODE-FINDINGS.md` §2.

California puts the measure and its author inside the desc, so the question is a phrase within a
near-unique string; Assembly patterns are unanchored, Senate patterns anchor at `^`.

## Feed health — California is the cleanest state yet on three of four counts

| check | CA 2172 |
| --- | --- |
| duplicate `roll_call_id` identity groups (TX had 9.4%) | **0** |
| summary-only rolls, no member positions (TX had 2,701) | **0** |
| tally mismatches / member-list mismatches | **0 / 0** |
| rolls identical except for `date` | **80** — see `CODE-FINDINGS.md` §1 |

## Fetch (local `voteapp`, 2026-08-27)

5,281 floor / 13,181 committee (rejected pre-queue) / 1,433 excluded-measure / 0 unrecorded /
0 duplicate / 0 surfaced-null. **5,328 rows stored** (5,281 floor + 47 excluded-question audit rows).
**972 divided floor votes** (`LEAST(yeas,nays) >= GREATEST(yeas,nays)/4`), 442 of them on measures
that became law, across 227 measures.

## Crosswalk — 121 entries, 80 mapped, 41 explicit null

`crosswalk.json`, built in two passes.

**First pass (2026-08-27): 33 mapped, 88 explicit null**, against the then-partial Assembly rosters.
All 33 proposals were accepted. Six needed eyes:

- **3 `seatAgrees:false`** are sitting Assembly members running for the Senate, each confirmed off
  `current_office`: Damon Connolly (AD-12 → SD-2), Esmeralda Soria (AD-27 → SD-14), Avelino Valencia
  (AD-68 → SD-34). Same pattern as Texas's Dennis Paul.
- **1 `first_prefix`**: Chris Ward = Christopher Ward.

**Neither hand-add class from Texas and Georgia applies here.** No name-variant miss survived a
token-overlap sweep of the 88 unmatched members against the 55 unmatched candidates, and **no sitting
member is running for a non-legislative California office** in our Nov-2026 data.

Resolution over all 5,328 rolls: matched 84,648 / unmatched_reviewed 218,740 / `no_crosswalk` 0 /
`out_of_scope` 0 / 0 file errors.

**Second pass (2026-08-28): +47 mapped → 80 mapped, 41 null**, after the Nov-2026 Assembly rosters
were completed (80/80 districts). All 47 new proposals were exact or approved first-prefix matches
with `seatAgrees: true`; the three `seatAgrees:false` Senate-runners above remain valid. Speaker
Robert Rivas (AD-29), a null in the first pass, is now mapped. The 41 remaining nulls are members
with no Nov-2026 candidacy on file. Review notes: `batch-01/crosswalk-review-2026-08-28.md`.

## Fan-out — was small at first import, resolved 2026-08-28 by roster completion

At the first import (2026-08-27) our database held Nov-2026 elections for only **26 of 80 Assembly
districts**, so a judged vote wrote a median of 21 records in the Assembly and 11 in the Senate
(vs Texas 114/13, Georgia 149/42) — only 33 of the session's 121 sitting members were on a ballot
we held.

The Assembly rosters were then completed (**80/80 districts**; the Senate's 20 up-in-2026 districts
were already covered), the crosswalk extended to 80 mapped, and the import re-run on 2026-08-28.
The re-run behaved exactly per the Ohio precedent: 431 new records for the 47 newly mapped members,
298 existing rows `unchanged`, batch-01 now **729 records / 80 candidates**.

## Judging source

`https://leginfo.legislature.ca.gov/faces/billTextClient.xhtml?bill_id=202520260<BILL>` serves the
**chaptered text with the Legislative Counsel's Digest** at the top — neutral, official,
section-by-section, and reachable with plain `curl` (no Cloudflare, unlike legiscan.com, and no
sponsor's statement of intent, unlike Texas's analyses). It is the CRS / LSC / HBRO analog.

**Version check:** the same page lists every version with its date. Compare each chamber's vote date
against the last `Amended` date. For batch-01 all 20 votes fall after their bill's final amendment,
so every vote in this batch was cast on the enrolled text and no description needs a version caveat.

## Batches

- `batch-01/` 20 rolls / 10 measures / **729 records** · `batch-02/` 24 / 12 / **859** ·
  `batch-03/` 18 / 9 / **645** · `batch-04/` 21 / 11 / **806** · `batch-05/` 13 / 7 / **509**
  (both-chamber seam closed) · `batch-06/` 5 / 5 / **311** (one-chamber tail begins) ·
  `batch-07/` 8 / 8 / **507** (final-text rule; Assembly-only tail) ·
  `batch-08/` 10 / 10 / **107** (Senate-only tail begins).

California total: **4,473 roll-call records across 80 candidates**, 72 measures, **17 of 27
research areas**. All descriptions plain English, American-spelled, every qualification kept.

## The rule changed on 2026-08-31: final text, not enacted

Batches 01-06 imported only bills that **became law**. That is the wrong test for a voting record.
A legislator's vote is their act; a veto is the governor's act, later. The test is now whether the
chamber voted on the **text that finished the legislative process**:

| status | usable | why |
| --- | --- | --- |
| chaptered | yes | signed into law |
| enrolled | yes | passed both chambers; enrolled text cannot change |
| vetoed | yes | the vote happened; the description says the governor vetoed it |
| engrossed | **no** | passed one chamber, still amendable |
| introduced / failed | no | never finished |

**This removed the autumn wait.** The plan had been to re-download once the governor signed. That
was only needed because the record was tied to the outcome. It no longer is.

## Dataset refreshed 2026-08-30 and re-fetched

Hash `c150cc01b198`, at `/Users/shu/legiscan-data/ca-2172-0830/`. Re-fetched 2026-08-31:
21,158 rolls, 6,395 floor votes, **6,454 stored rows** (was 5,328). No `approved_conflict` — the
store refuses to overwrite an approved row.

Signing runs into the autumn and will move measures from `enrolled` to `chaptered` or `vetoed`.
Under the new rule that changes only a description's closing sentence, not whether a vote is usable.

## The open pool, measured on the 08-30 cut

Divided floor votes on final-text bills, measures not yet worked:

| status | both chambers | Assembly-only | Senate-only |
| --- | --- | --- | --- |
| chaptered | 66 | 20 | 89 |
| enrolled | 80 | 9 | 56 |
| vetoed | 17 | 2 | 15 |

**354 open measures**, 194 of them carrying an Assembly roll. Assembly rolls are worth roughly six
times Senate rolls (80 of 80 Assembly seats on the ballot against 20 of 40 Senate seats), so
Assembly-first ordering holds for the rest of the campaign.

`batch-07` cleared the **Assembly-only** slice: of its 31 measures, 8 were judged and 23 dropped —
12 budget-package bills, 6 on the version check, 2 under filter 5, 3 already known bad.
Two measures remain permanently unavailable: AB 863 and AB 483 have only pre-amendment divided
votes.

`batch-08` opened the **Senate-only** tail (10 of 160).

## The version check runs offline

The dataset's bill JSON carries `texts[]` — one entry per version with a `date` and a `type`. So
"was this vote cast on the final text?" is answerable from local files, with **no web fetch**:

    vote_date >= max(date for t in bill["texts"] if t["type"] == "Amended")

Run across the 160 Senate-only measures it gives **118 pass / 42 fail** in a single pass. Use it
before reading anything; it is the cheapest filter in the pipeline and it removes about a quarter
of the pool.

## Where the value is

| slice | measures | records per measure | note |
| --- | --- | --- | --- |
| both-chamber, open | 163 | ~79 | **highest value, not started** |
| Senate-only, passes version check | 108 left | ~11 | batch-08 took 10 |
| Assembly-only | 0 | ~68 | cleared by batch-07 |

An Assembly roll reaches ~68 of our candidates and a Senate roll ~11, because all 80 Assembly seats
are on the November ballot against 20 of 40 Senate seats. The both-chamber pool should be worked
before the rest of the Senate-only tail.
