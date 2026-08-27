# Illinois 104th General Assembly — LegiScan roll-call import (session 2176)

Phase-4 state #3, after Texas (`legiscan-tx-2160/`) and alongside the Georgia,
Florida and California sessions. Everything here is **local `voteapp` only**.
Prod has never been touched by this campaign.

## The dataset

LegiScan session **2176** — "2025-2026 104th General Assembly", so both years
sit in one dataset. Downloaded 2026-08-26 from `getDatasetRaw` (IL `state_id`
= 13); the ZIP and the extracted tree live outside the repo at
`/Users/shu/legiscan-data/il-2176/`, and the full evidence set (8,375 JSONs,
the 37 MB `resolve-report.json` and the 4 MB fetch report) at
`/Users/shu/legiscan-data/il-2176-evidence/`. This directory keeps only the
curated subset, following the Ohio precedent.

12,073 bills / 9,077 roll calls / 181 people. Zero file errors, zero vote
parse errors, zero tally mismatches, zero summary-only rolls.

## What the survey established

Illinois prints the **plainest desc vocabulary of any state surveyed so far**:
104 distinct descs, with no per-roll id suffix (Texas' ` RV#<n>`) and no
` : House Vote #<n>` suffix (Georgia). See `survey/`.

- **Every committee desc ends in the literal word `Committee`** — including
  the clerk's doubled `House Police & Fire Committee Committee` — so the
  config excludes committees by rule rather than leaning on the tally
  heuristic.
- **Each floor family is printed in TWO spellings, split by date and never
  overlapping.** LegiScan reworded its descs mid-dataset: `Third Reading in
  House` runs 2025-04..2025-05, `House Third Reading` runs 2025-10..2026-05;
  the same split hits Concurrence and Motion. Both spellings are required.
  Before keeping both I checked all 9,077 rolls for a `(chamber, bill, date,
  yea, nay, nv, absent, member-list-hash)` group carrying more than one desc:
  there are **zero**, so the two spellings never describe the same physical
  vote and keeping both cannot double-count.
- **The `Motion` family is left SURFACED on purpose** (213 rolls). It is a
  garbage bucket: S.B. 1383's `Motion in Senate` 56-0 *is* its third reading,
  but the same desc also carries motions to reconsider, Note Act motions, and
  Illinois' amendatory-veto votes (H.B. 2568). The desc alone cannot separate
  them, so they go to the human-reviewed null bucket — the call the Texas
  config makes for bare `RV#<n>`.
- **Constitutional amendments are effectively absent.** The dataset holds
  exactly one JRCA floor roll (HJRCA 28, House 74-38, 2026-04-22, desc `House
  Amendments`); the Senate never voted it, so no amendment from this GA
  reached the ballot. Unlike Georgia, Illinois types its amendments `JRCA`,
  which the kept-types filter already admits — no code finding here.

### The Texas duplicate-id fix generalizes

Illinois has **0 repeated `roll_call_id`s**, but **434 rolls are identical in
every field except `roll_call_id`** (390 groups, largest 5) — the exact
hazard PR #853 fixed for Texas. The identity key collapses them correctly:
the fetch reports 433 `duplicateVotes` (the 434th sits on an excluded measure
type and is rejected before the duplicate check, as designed).

## Fetch (local `voteapp`)

**8,375 rows inserted / 8,375 distinct roll_numbers**, no collisions, dates
2025-01-29..2026-05-31. Reconciles exactly: 8,375 stored + 269
`excludedMeasureVotes` + 433 `duplicateVotes` = 9,077 dataset votes. Split:
floor `true` 2,063 (1,300 house / 763 senate), `false` 6,112, `null` surfaced
200. **590 divided floor votes; 427 of them divided AND enacted, across 248
measures** — the best yield of any phase-4 state, which is what a Democratic
trifecta produces: party-line bills reach the governor.

## Crosswalk — `crosswalk.json`

**181 entries = 126 proposed (all accepted) + 6 hand-added + 49 explicit
null.** Validation over all 8,375 rolls: matched **194,034**,
unmatched_reviewed 62,029, `no_crosswalk` **0**, `out_of_scope` **0**, 0 file
errors.

Only three proposals needed eyes, and `current_office` confirmed all three:

- **Paul Jacobs** is LegiScan HD-118 but matched the SD-59 candidate — he
  really is "Illinois State Senator, District 59" now, and HD-118's LegiScan
  holder is his successor Scott Doody. (`seatAgrees:false`, correct.)
- **Nabeela Syed**, HD-051, is running for SD-26. (`seatAgrees:false`,
  correct.)
- Bradley / Brad Fritts, a first-prefix match on the same seat.

Six hand-adds, all name variants the proposer cannot reach by rule:

| people_id | LegiScan | ballot name | why the proposer missed it |
|---|---|---|---|
| 23754 | William Hauter | Bill Hauter | legal first name, not a prefix |
| 24645 | Michael Coffey | Mike Coffey | Michael/Mike: neither is a prefix of the other |
| 14028 | Christopher Davidsmeyer | C.D. Davidsmeyer | ballot first token is a single letter; the prefix rule needs both ≥ 2 |
| 11932 | Susan Rezin | Sue Rezin | legal first name, not a prefix |
| 24648 | Laura Faver Dias | Laura Dias | two-part surname shortened on the ballot |
| 21041 | Anne Stava-Murray | Anne Stava | hyphenated surname shortened on the ballot |

**40 of the 55 unmatched members have no candidate rows for their seat at
all** — a roster-coverage gap, not a crosswalk miss. Of those, **all 20
senate seats are seats NOT UP in November 2026**: our pool covers 39 of the
59 SDs, and not one of the 20 appears in that up-list. That is the Illinois
senate's staggered classes, cleanly confirmed rather than assumed. The other
20 are house seats among the 19 HDs (of 118) our Nov-2026 roster does not yet
cover.

**Fan-out on floor rolls: house median 92 candidates (max 97), senate 33 (max
34).** Texas was 114/13, Georgia 149/42. Illinois' senate reach is far better
than Texas' because two-thirds of the chamber is on the ballot.

## Judging source — the best of any state so far

Illinois blocks scrapers on `www.ilga.gov` but its own block page points at a
**sanctioned public file repository**, `https://ftp.ilga.gov/`. Use it.

- `Legislation/104/BillStatus/XML/10400<BILL>.xml` carries the **Legislative
  Reference Bureau synopsis for every version** — as introduced and one per
  amendment — plus the complete action trail showing which amendments were
  **Adopted** and which were **Tabled**, **Postponed** or **Lost**. That is
  the Ohio-LSC / Georgia-HBRO analog, and it is better than Texas': there is
  **no sponsor statement of intent anywhere**, so the Texas advocacy-preamble
  hazard does not recur.
- `Legislation/104/<TYPE>/10400<BILL>{,eng,enr,sam001,ham001}.htm` serves the
  full text of every version for a line-level diff.
- **TLS note:** `ftp.ilga.gov` serves a valid Sectigo certificate but omits
  its intermediate, so `curl` fails with "unable to get local issuer
  certificate" — the same shape as Ohio's `legislature.ohio.gov`. Fetch the
  intermediate from the leaf's AIA URI and append it to the CA bundle; do not
  disable verification.

### ⚠ The Illinois hazard: gut-and-replace is structural

Every other state has had a vehicle-bill trap or two. In Illinois **the short
title is routinely unrelated to the bill**, and it is not an exception worth
listing — it is the norm. Confirmed cases in the divided-and-enacted set
alone:

| short title | what the bill actually is |
|---|---|
| SB 1950 SANITARY FOOD PREPARATION | End-of-Life Options for Terminally Ill Patients Act |
| SB 3019 FINANCE-AGRICULTURAL BORROWER | Targeted Advertising Services Tax Act |
| SB 25 SWIMMING FACILITY COLD SPA | Municipal and Cooperative Electric Utility Transparent Planning Act |
| HB 2568 TRUST CODE-UNCLAIMED PROPERTY | Equality for Every Family Act (parentage) |
| HB 1312 POW MIA RECOGNITION DAY | Illinois Bivens Act |
| HB 1836 EAVESDROP-STATEWIDE GRAND JURY | Clean Slate Act |
| HB 5090 PROCUREMENT-CONSTRUCTION | Transportation Network Driver Labor Relations Act |
| SB 3255 DHS-DSP PILOT PROGRAM | Bond Authorization Act of 2026 |
| HB 22 CROHNS AND COLITIS AWARENESS | Municipal Code planning provisions |
| HB 1863 ONE HEALTH TASK FORCE | Boards and Commissions Review Act |

**Never write a judgment from an Illinois short title.** Read the synopsis of
the version the chamber actually voted, every time. Georgia's SB 33 was one
bill; here it is a drafting convention.

Note also that LegiScan's `description` field holds only the **latest**
synopsis, which for an amended bill describes just the delta ("Reinserts the
provisions of the engrossed bill with the following changes"). It is not a
substitute for the synopsis stack in the BillStatus XML.

### Second hazard: a `Concurrence` roll can be a motion that LOST

H.B. 3564's first House concurrence motion **lost 56-36-2**; a second one
carried 64-40 two months later. Both are stored as kept floor votes, and
`passed` is the only thing separating them. A description must never say a
measure passed on the strength of a failed motion.

## Batch-01

See `batch-01/PLAN.md` and `batch-01/JUDGING.md`. 22 rolls / 11 measures /
**1,364 records across 132 candidates**, imported to local `voteapp`
2026-08-26.

## What is left

`survey/divided-enacted-worklist.tsv` is the resumable ledger: one row per
divided-and-enacted floor roll (427 of them), with a `status` column marking
the 22 judged in batch-01 and the reason H.B. 3564 was dropped. **405 rolls
on 237 measures remain for batch-02 and later.**
