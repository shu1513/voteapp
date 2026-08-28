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

## Crosswalk — 121 entries, 33 mapped, 88 explicit null

`crosswalk.json`. All 33 proposals were accepted. Six needed eyes:

- **3 `seatAgrees:false`** are sitting Assembly members running for the Senate, each confirmed off
  `current_office`: Damon Connolly (AD-12 → SD-2), Esmeralda Soria (AD-27 → SD-14), Avelino Valencia
  (AD-68 → SD-34). Same pattern as Texas's Dennis Paul.
- **1 `first_prefix`**: Chris Ward = Christopher Ward.

**Neither hand-add class from Texas and Georgia applies here.** No name-variant miss survived a
token-overlap sweep of the 88 unmatched members against the 55 unmatched candidates, and **no sitting
member is running for a non-legislative California office** in our Nov-2026 data.

Resolution over all 5,328 rolls: matched 84,648 / unmatched_reviewed 218,740 / `no_crosswalk` 0 /
`out_of_scope` 0 / 0 file errors.

## ⚠ Fan-out is small, and that is OUR roster coverage, not California

**Median matched members per roll: 21 in the Assembly (max 22), 11 in the Senate (max 11)** —
against Texas 114/13 and Georgia 149/42.

Our database holds Nov-2026 state-legislative elections for only **26 of 80 Assembly districts**
(48 candidates) and for 20 Senate districts (40 candidates — the Senate is staggered, so 20 of 40
seats is the whole ballot). Only **33 of the session's 121 sitting members** are on a Nov-2026 ballot
we hold. A judged California vote therefore writes ~21 records in the Assembly and ~11 in the Senate.

Re-running the import once the Assembly rosters fill in adds the new members idempotently (the Ohio
precedent: extending the crosswalk and re-importing adds members without touching existing rows).

## Judging source

`https://leginfo.legislature.ca.gov/faces/billTextClient.xhtml?bill_id=202520260<BILL>` serves the
**chaptered text with the Legislative Counsel's Digest** at the top — neutral, official,
section-by-section, and reachable with plain `curl` (no Cloudflare, unlike legiscan.com, and no
sponsor's statement of intent, unlike Texas's analyses). It is the CRS / LSC / HBRO analog.

**Version check:** the same page lists every version with its date. Compare each chamber's vote date
against the last `Amended` date. For batch-01 all 20 votes fall after their bill's final amendment,
so every vote in this batch was cast on the enrolled text and no description needs a version caveat.

## Batches

- `batch-01/` — 20 rolls / 10 measures / **298 records**, imported 2026-08-27. See its `PLAN.md` and
  `JUDGING.md`.

**Next:** 422 divided-and-enacted rolls on ~217 measures remain.
