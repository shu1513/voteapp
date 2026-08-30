# Missouri — LegiScan session 2169 (103rd General Assembly, 2025 Regular Session)

Phase-4 roll-call campaign evidence for Missouri. Source = the LegiScan bulk dataset for session
2169, read by `rollcall:legiscan:{fetch,resolve,import}`. Local `voteapp` only; production is
untouched by everything recorded here.

The dataset ZIP and the 499 per-roll evidence JSONs live outside the repo at
`/Users/shu/legiscan-data/mo-2169{,-evidence,-crosswalk,-survey}/` (the Texas precedent: the repo
keeps the curated subset, not the whole fetch). Each batch directory carries the evidence JSONs of
the rolls that batch judged.

Missouri also has two 2025 special sessions in LegiScan (2216 and 2226). They are separate datasets
and are **not** in scope here; registering one would need its own survey and config entry.

## Dataset shape

| | |
|---|---|
| bills | 2,673 (B 2,426 · JR 166 · R 50 · CR 30 · CB 1) |
| roll calls | 557 (house 335 · senate 222) |
| people | 197 |
| repeated `roll_call_id`s | 0 — the Texas 9.4 % duplicate-id fix is a verified no-op here |
| summary-only rolls (no member list) | 0 |
| tally mismatches (`total != yea+nay+nv+absent`) | 0 |
| file/parse errors | 0 |
| identity duplicates collapsed by the shared identity key | 44 stored-eligible (49 across the raw feed; not all are reprints of one action — see CODE-FINDINGS §5) |

## Desc vocabulary (measured, `survey/rollcall-legiscan-fetch-mo-20260829T050232Z-survey.json`)

The two chambers write descs in completely different styles.

**Senate — five spellings in the whole session.** `Senate: Third Reading` (172) is *both* the first
third reading *and* the Truly Agreed To And Finally Passed vote — Missouri prints no separate TAFP
wording, so the second chamber's vote on the enacted text lands in this family.
`Senate: Conference Committee Report Adoption` (19) is the conference class.
`Senate: Adopt Substitute` (13, nine of them on SB 98 in one day) and `Senate: Emergency Clause` (8)
are excluded: neither is a vote on the measure. `Senate: Adoption` (10) is left **unmatched on
purpose** so it surfaces — nine are ceremonial resolutions (rejected as excluded measure types) but
the tenth is the Senate adopting the HB 595 conference committee report, 22-11.

**House — the calendar heading plus the bill's substitute chain**, e.g.
`House: HBs WITH SENATE AMENDMENTS SS SCS HB 225, A.A., E.C.`, so every desc is unique. 275 raw
descs fold to 16 calendar families; the config matches the heading and lets the chain trail.
Kept: `HBs/SBs/HJRs/HABs FOR THIRD READING`, `… 3rd READ - INFORMAL`, `… 3rd READING - CONSENT`
(passage), `HBs WITH SENATE AMENDMENTS` (concurrence), `BILLS IN CONFERENCE` (conference).
Excluded: `HBs/HJRs FOR PERFECTION` and `… PERFECTION - INFORMAL` (36 rolls — Missouri's
amend-and-engross stage, the second-reading analog Texas and California also exclude) and
`House: General PQ`.

Every tally in the dataset is a whole-chamber tally (house 161-163, senate 31-34): **the feed holds
no committee votes at all.**

## Fetch (local `voteapp`, 2026-08-29)

`499 inserted` = 446 floor + 1 surfaced + 52 excluded-question rows (stored as `is_floor_vote =
false` for the audit trail, the CHECK blocks approving them). 14 excluded-measure-type votes and 44
identity duplicates are counted and never stored. 499 + 14 + 44 = 557, the whole feed. 0 approved.

## Crosswalk

`crosswalk.json` — 197 entries. The original batch had **89 proposer matches (all accepted) + 5
hand-added + 103 explicit nulls = 94 mapped**. The 2026-08-30 certified-roster expansion added 23
incumbent matches, and a 2026-08-30 seat-change review added 2 more, bringing the current file to
**119 mapped / 78 explicit nulls**. Seat comes from `district`, never `role`.

Two proposals carry `seatAgrees:false` and both are real: **Betsy Fogle (HD-135)** and
**Melanie Stinnett (HD-133)** are sitting representatives running for **SD-030**, and HD-133 and
HD-135 both lie inside SD-030 (Greene County / Springfield); the incumbent, Lincoln Hough, has no
Nov-2026 candidate row.

The five hand-adds are the classes the proposer cannot reach:

| people_id | why |
|---|---|
| 24137 Dave Hinman | LegiScan `first_name` is the legal name *David* (nickname *Dave*); HD-103 agrees |
| 25296 Wick Thomas | LegiScan `first_name` is the legal name *Nicholas* (nickname *Wick*); HD-019 agrees |
| 22401 Dean Van Schoiack | surname spacing: LegiScan *Van Schoiack*, candidate *VanSchoiack*; HD-009 agrees |
| 20403 Brian Williams | sitting senator (SD-014) running for a St. Louis County office |
| 20703 Doug Clemens | sitting representative (HD-072) running for a St. Louis County office |
| 20729 Greg Sharpe | sitting representative (HD-004) running for **SD-018**; `seatAgrees:false` |
| 21704 Bill Hardwick | sitting representative (HD-121) running for **SD-016**; `seatAgrees:false` |

The last two came from the seat-change review after the roster expansion. Both are corroborated the
same way: same party (R), their own House districts carry other candidates for November so they are
not seeking re-election, and each target Senate district's sitting member has no November candidacy.
A third `seatAgrees:false` proposal was **rejected and is documented as a deliberate null** —
Rep. Jeff Coleman (HD-032) is a Republican, and the Jeff Coleman running in HD-101 is a Libertarian,
a different person who shares the name.

⚠ **Seven Senate seats are still unlinkable.** All 17 even-numbered districts are up in November
2026 and all 17 have rows in the local database, but SD-006, SD-008, SD-010, SD-014, SD-028, SD-032
and SD-034 carry only their **2026-08-04 primary** rows, with no November general row. The pipeline
default `--scope-from 2026-11-01` therefore cannot reach their members. Rostering those seven
general contests and re-running the import would add roughly 21 more records on this batch, and
would matter more for the ballot pages than for the roll call.

Original validation over all 499 stored rolls: **matched 26,979 / unmatched_reviewed 25,904 /
`no_crosswalk` 0 / `out_of_scope` 418 / 0 file errors / 0 zero-match rolls.**

⚠ The 418 `out_of_scope` resolutions are **Brian Williams and Doug Clemens only**: their sole
candidacies are the 2026-08-04 county *primary* and no November row exists yet, so the pipeline
default `--scope-from 2026-11-01` skips them. Their identity is reviewed and correct; when a
November candidacy is rostered, a re-import adds their records idempotently.

**Original fan-out: house median 81 matched candidates per roll (max 89), senate median 3.** The
original import wrote 583 records across 92 candidates. The certified-roster expansion completed
all 163 House contests and all 10 November 2026 Senate election rows present in the local database;
the rerun added 136 records. The seat-change review then added 14 more (7 rolls each for Sharpe and
Hardwick), bringing this batch to **733 records across 117 candidates**, converged.

## Selection pool

83 divided **and** enacted kept-floor rolls on 35 measures (house 44 / senate 39); only **20** of
those measures have a divided *House* roll. Ledger: `survey/divided-enacted-worklist.tsv`.

## Special sessions — surveyed 2026-08-30, NOT imported

Missouri ran two 2025 special sessions, and they are separate LegiScan datasets that the `MO` config
(pinned to session 2169) cannot fetch.

**Session 2226 (2nd Special) is the highest-value Missouri material outstanding.** 13 bills, 8 roll
calls, and both enacted measures are marquee and divided in both chambers: **HB 1**, mid-decade
congressional redistricting (House 90-65, Senate 21-11), and **HJR 3**, the "Protect Missouri Voters"
constitutional amendment that goes to the ballot (House 104-51, Senate 21-11). Its desc vocabulary is
already covered by the MO patterns — `HJRs/HBs FOR THIRD READING` kept, `FOR PERFECTION` excluded,
`Senate: Third Reading` kept — and all 194 of its people appear in the 2169 snapshot, so **the
committed `crosswalk.json` covers it unchanged** (people_ids are session-stable).

**Session 2216 (1st Special) is not worth importing and carries a trap.** Its only divided
House rolls are dated 2025-03-13 with the desc `House: SBs FOR THIRD READING SS#2 SB 4` and tallies
99-44 and 96-44 — which are the **regular session's** SB 4 (utilities) votes, attached by LegiScan to
the special session's unrelated SB 4 (Missouri Housing Trust Fund disbursement). The roll_call_ids do
not collide with 2169's, so nothing would be deduped: importing 2216 would file utility-bill votes
under a housing bill. Everything else there is either an appropriation or a senate-only roll reaching
three candidates.

Registering 2226 needs a registry entry, not a code change: the registry key and the `jurisdiction`
field are separate, so a `MO-2226` key carrying `jurisdiction: "MO"` and `sessionId: 2226` pins the
second session while every DB write stays under `MO`. That is a deliberate extension of the key
convention and should be argued in its own PR.

## Judging source

**The Missouri House publishes an official, nonpartisan bill summary for every version of every
bill, House and Senate** — `https://documents.house.mo.gov/billtracking/bills251/sumpdf/<PADDED><V>.pdf`,
V = `I` introduced, `C` committee, `P` perfected, `T` truly agreed. Judge from **T** and confirm
every operative claim against the enrolled text. There is **no sponsor statement of intent** in
these documents, so the Texas advocacy-preamble hazard does not recur.

The bill page `https://house.mo.gov/BillContent.aspx?bill=<BILL>&year=2025&code=R&style=new` lists
every version's summary, text and fiscal note **and every House roll call**. Bill text PDFs are
`.../hlrbillspdf/<LR NUMBER>.pdf`; Senate texts are at
`https://www.senate.mo.gov/25info/pdf-bill/{intro,comm,perf,tat}/<BILL>.pdf` (`tat` = truly agreed).
The LegiScan bill record additionally carries dated `texts[]` (Introduced / Comm Sub / Engrossed /
Substitute / Enrolled) and `amendments[]` with an `adopted` flag.

**Roll-call PDFs are the question-and-version ground truth** — see `CODE-FINDINGS.md` §1.
