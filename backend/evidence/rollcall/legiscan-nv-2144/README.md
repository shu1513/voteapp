# Nevada roll-call import — 2025 (83rd) Session, LegiScan session 2144

Nevada meets in regular session only in odd-numbered years, so the 2025 session
is the whole of this campaign apart from the 36th Special Session (LegiScan
session 2233), which is not surveyed here.

## The source

Dataset: LegiScan session 2144, dated 2025-12-07, downloaded through the
LegiScan bulk API. 1,210 bill files, 1,333 roll calls, 95 people.

**Nevada has the smallest floor vocabulary of any state in this campaign.**
Every one of the 1,333 roll calls carries one of exactly two descriptions:
`Senate Final Passage` (670) and `Assembly Final Passage` (663). There is no
third spelling. The registry entry therefore keeps both and excludes nothing.

Feed health is the cleanest tier: no repeated roll call ids, no duplicate
identities, no summary-only rolls, no tally mismatches, no roll-call parse
errors, and no committee votes at all. Every Assembly roll lists all 42
members and every Senate roll lists all 21 but one.

Chamber code `A` maps to house. That mapping was added for California and is
verified here on all 663 Nevada Assembly rolls.

## The pool

Measured before any batch was selected, using the campaign's standard divided
gate — the losing side is at least a quarter of the winning side:

| | rolls | measures |
|---|---|---|
| divided floor votes | 292 | |
| divided **and enacted** | **104** | **73** |
| divided and **vetoed** | 145 | 79 |
| divided, died in the legislature | 43 | |

Nevada's government is divided: a Democratic legislature and a Republican
governor who vetoes heavily. So the vetoed pool is larger than the enacted one.
The divided-and-enacted set is the set of divided bills the governor **signed**.
Opening the vetoed pool would be the Pennsylvania batch-02 scope and is a
separate decision.

Of the 104 enacted rolls, 64 are Senate and 40 are Assembly. Because our
candidate coverage runs the other way (see fan-out below), the Assembly rolls
carry most of the value.

## The crosswalk

`crosswalk.json` holds 75 entries: 42 mapped and 33 explicit nulls. Validation
over all 1,332 stored rolls: 26,927 matched, **zero** with no crosswalk entry,
**zero** out of scope, zero file errors, and one zero-match roll (the
two-member SB 26 roll, finding 4).

All 37 proposals were accepted. One carried `seatAgrees: false` and is real:
Danielle Gallant sits in Assembly District 23 and is running for Senate
District 20, corroborated by her candidate row's `current_office` and by both
being Republicans.

Five entries were added by hand, in two classes the proposer cannot reach:

- **A dropped second surname.** LegiScan prints `Cinthia Zermeno Moore`; the
  candidate row reads `Cinthia Moore`. Same seat, same party.
- **Four sitting legislators running for an office that is not a legislative
  seat**, so they are outside the state-legislature candidate pool the proposer
  reads: Heidi Kasama (Assembly District 2) and Tanya Flanagan (Assembly
  District 7), both running for Clark County offices, and Nicole Cannizzaro
  (Senate District 6) and Sandra Jauregui (Assembly District 41), both running
  statewide. Each is corroborated by party and by the candidate row's
  `current_office`.

The 33 nulls were each checked against the whole Nevada candidate pool, in
every office scope, by surname token. Most are structural rather than roster
gaps: Nevada limits legislators to twelve years, and the Senate is staggered,
so those members have no 2026 seat to run for.

**Fan-out: 30 candidates per Assembly roll, 11 per Senate roll.** An Assembly
vote is worth about three Senate votes.

Nevada has **no** contradiction between LegiScan's `role` and `district`
fields, unlike Texas.

## The judging source

The Nevada Legislature publishes each enrolled act at
`https://www.leg.state.nv.us/Session/83rd2025/Bills/<AB|SB>/<BILL>_EN.pdf`
(a browser user-agent header is required). Each one opens with an
`AN ACT relating to ...` title that is itself a full list of what the act does,
followed by the **Legislative Counsel's Digest** — the Legislature's own
official, neutral, section-by-section summary, with **no sponsor statement of
intent anywhere**. The advocacy hazard that Texas analyses carry does not
recur here.

The enrolled act is ground truth; the Digest is an index to it. Other printed
versions are at the same path with `_R1` and `_R2` suffixes (first and second
reprint) or no suffix (as introduced), and adopted amendments are under
`/Session/83rd2025/Bills/Amendments/`.

LegiScan's `history` array is the Legislature's own dated action trail. It is
complete and precise, and it is what settles which reprint each chamber voted
on and how the bill became law.

## Layout

- `survey/` — the survey report, the fetch report, and
  `divided-enacted-worklist.tsv`, one row per divided-and-enacted roll with its
  version and superseded status.
- `crosswalk.json` — the reviewed people-to-candidate map.
- `legiscan-people-nv-2144.json` — the people snapshot the crosswalk resolves
  against, built by hand because of finding 1.
- `crosswalk-proposals-report.json` — the resolve run's summary counts. The
  per-roll detail is left out on purpose; it is about 8 MB.
- `CODE-FINDINGS.md` — five findings recorded and not fixed.
- `batch-01/` — the first judged batch.

The full 1,332-file evidence set and the dataset itself live outside the
repository at `/Users/shu/legiscan-data/nv-2144*`, following the precedent set
by Texas. Only the curated subset is committed.

## Batch status

### The enacted pool is complete

All 104 divided rolls in `survey/divided-enacted-worklist.tsv` carry a disposition and a
reason. 50 rolls were dropped, 12 excluded as appropriations or procedural, and 3
superseded.

### The non-enacted pool is open, and includes vetoed bills

Nevada's remaining 187 divided floor rolls sit on 114 bills that did not become law.
Almost none of them fit the campaign's standing exception to filter 2 — the Pennsylvania
batch-02 scope of "one chamber passed it, the other never voted" reaches only 29 Nevada
measures. The bulk is a class no earlier state took: **79 measures the Legislature passed
through both chambers and the governor vetoed.**

On the user's direction that pool is open. Dispositions for all 187 rolls are in
`survey/divided-not-enacted-worklist.tsv`. Every description on a non-enacted measure says
the bill never became law and describes it with "would have".

| batch | pool | measures | rolls | records |
| --- | --- | --- | --- | --- |
| batch-01 | enacted | 10 | 18 | 387 |
| batch-02 | enacted | 11 | 14 | 265 |
| batch-03 | enacted | 7 | 7 | 94 |
| batch-04 | vetoed | 9 | 18 | 364 |
| **total** | | **37** | **57** | **1,110** |

One hand-written duplicate was retired during the batch-04 sweep, and a wording defect in
batches 01 to 03 — 645 records whose closing clause began with a lowercase word — was
fixed at source and rewritten in place. Both are recorded in `batch-04/JUDGING.md`.
