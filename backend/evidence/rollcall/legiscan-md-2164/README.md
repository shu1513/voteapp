# Maryland — LegiScan roll-call import (session 2164, 2025 Regular Session)

Maryland is phase-4 state #7, after GA / IL / TN / TX / FL / CA. Plan:
`docs/plans/roll-call-vote-import.md` §5 phase 4.

## Why Maryland has the best fan-out shape of any state so far

The **entire** General Assembly — 141 Delegates and 47 Senators, all on
four-year terms — is on the November 2026 ballot. Every crosswalk match is
therefore a candidate a voter can actually act on, in both chambers. Measured
fan-out is **116 candidates per House roll (median) and 38 per Senate roll**;
compare Georgia 149/42, Texas 114/13, Illinois 92/33, Florida 50/9,
California 21/11, Tennessee 13/15.

## Dataset

LegiScan session **2164** (`MD/2025-2025_Regular_Session`), downloaded with the
key held in the main checkout's `backend/.env`. Maryland sits in **annual**
sessions, so this dataset is one year only — the 2026 Regular Session is its own
LegiScan session, **2240**, and is the obvious next campaign.

- 2,617 bills / 2,494 roll calls / 216 people (189 legislators + 27 pseudo-people)
- Bill types present: B (2,605) and JR (12). Maryland proposes constitutional
  amendments as ordinary **bills**, so Georgia's resolution-typed-amendment gap
  (GA CODE-FINDINGS #1) does not recur here.
- Dates 2025-01-21 .. 2025-04-07 (sine die).

**Feed health is the best of any phase-4 state, tied with Georgia:** 0 repeated
`roll_call_id`s (the Texas 9.4% duplicate-id collapse is a verified no-op), 0
identity-duplicate groups, 0 summary-only rolls (every roll carries a member
list), 0 tally mismatches, 0 file errors, 0 parse errors.

## Vocabulary (measured, `--survey`)

The smallest vocabulary of any state surveyed: 2,494 rolls collapse to **15 desc
families**, and 2,295 of them are the single literal string `Third Reading
Passed`. Every desc matches a kept or an excluded pattern — **2,310 kept, 184
excluded, 0 unmatched**. See `survey-report.json` and the entry comment in
`src/pipeline/rollcall/legiscanStateConfigs.ts`.

- Kept: `Third Reading Passed`; `Third Reading Passed with Amendments` (House)
  and `Third Readings Passed with Amendments` (Senate — the same question,
  spelled plural); the session's one conference report (SB 338).
- Excluded by rule, all floor-sized: floor amendments (154), committee
  amendments adopted on the floor (12), motions (previous question, suspend the
  rules, special order) (18).
- **The dataset carries no committee votes at all** (Georgia's shape): every
  tally is whole-chamber, House 137-141 and Senate 45-47, so nothing lands in the
  small-tally bucket and no roll is left surfaced.

## Fetch

2,494 rows stored (2,310 floor `true`, 184 excluded-question `false`, 0
surfaced), 0 committee, 0 unrecorded, 0 duplicates, 0 excluded-measure.
Non-floor rows are stored on purpose as an audit trail; the CHECK blocks
approving them. **425 divided floor votes (House 276 / Senate 149); 320 divided
AND enacted across 176 measures.** See `fetch-report.json`.

## Crosswalk

`crosswalk.json` — **189 entries: 158 mapped (136 proposed, all accepted, plus 22
hand-added) and 31 explicit null.** Validation over all 2,494 rolls: matched
**197,538**, `no_crosswalk` **0**, `out_of_scope` **0**, 0 file errors, and **0
zero-match rolls**.

Every one of the 136 proposals was checked two ways before acceptance: the
district NUMBER agrees with the candidacy in all 136, and the 5 proposals that
cross chambers are all sitting Delegates running for their own district's Senate
seat (Alston D24, C.T. Wilson D28, Chang D32, Harris D27, Ruff D41), each
corroborated against `current_office`.

The 22 hand-adds fall in three classes the proposer cannot reach:

1. **Surname-first candidate rows** — our roster stores some Maryland candidates
   as `Hershey, Steve` / `Jacobs, Jay A.` / `Augustine, Malcolm L.` /
   `Woods, Jamila J.` / `Valderrama, Kriselda` / `Turner, Veronica L.` /
   `Muse, C. Anthony` / `Ghrist, Jefferson L.` / `Arentz, Steve` /
   `Bailey, John D. Jack`.
2. **Name variants** — legal first name vs working name (Stephen/Steve,
   William/Bill Ferguson, Catherine/Cathi Forbes, Michael/Mike Griffith,
   Samuel/`S. I. "Sandy"` Rosenberg, Robert Julian Ivey where LegiScan prints the
   middle name as the first), and rows that lead with an initial
   (`J. Sandy Bartlett`, `N. Scott Phillips`).
3. **Proposer misses on a one-to-one rule** — see CODE-FINDINGS.md §1.

**⚠ IDENTITY TRAP — two different Mark Fishers.** LegiScan's `Mark Fisher`
(people_id 11554) is the District 27C Delegate; a *different* Mark Fisher is a
2026 candidate in District 13. The crosswalk pins people_id 11554 to the 27C
candidate and the note records why. A name-only match would have written every
27C vote onto the wrong person.

## Judging source — DLS FISCAL AND POLICY NOTE ⭐

`https://mgaleg.maryland.gov/2025RS/fnotes/bil_<NNNN>/<bill>.pdf` (the link is on
the bill page; the bucket directory is not derivable from the bill number).
Written by the Department of Legislative Services: official, nonpartisan,
section-by-section, and — verified across all 11 batch-01 measures — carrying
**no sponsor statement of intent**, so the Texas advocacy-preamble hazard does
not recur. `pdftotext -layout` reads it cleanly.

**The note is version-stamped in its own header** (`Enrolled - Revised`,
`Third Reader - Revised`, `Third Reader`), which states outright which text it
describes.

## ⭐ Maryland's mechanical version check: the chapter file's suffix

The bill page links the enacted chapter as either `CH_<n>_<bill>t.pdf` or
`CH_<n>_<bill>e.pdf`:

- **`…t.pdf`** — the originating chamber's **Third Reading print** was enacted
  unchanged, i.e. the second chamber did not amend it. Both chambers voted the
  same text.
- **`…e.pdf`** — the bill was **Enrolled** after the second chamber amended it.
  The originating chamber's first roll is on superseded text.

This is an official, one-glance answer to "what text did this chamber vote?",
better than Texas's amendments page or Illinois's action trail, and it agrees
with the fiscal note's own version header on every measure checked.

The whole bill toolkit is on the bill page:
`hb####f.pdf` (First Reading), `hb####t.pdf` (Third Reading), `hb####e.pdf`
(Enrolled), `/2025RS/amds/…` (each adopted amendment), and
`/2025RS/votes/{house,senate}/NNNN.pdf` (the official vote record).

**Bill-page URLs need the bill number zero-padded to four digits**
(`…/Details/HB0424?ys=2025RS`); the unpadded form 302s to Not Found. Use
LegiScan's `state_link`, which is already padded.

## ⚠ Maryland does NOT roll-call concurrence — but it re-votes third reading

No desc in the session mentions concurring. Agreement to the other chamber's
amendments is taken without a recorded vote. **Instead, the originating chamber
takes a second recorded third-reading vote right after concurring**, so an
amended Maryland bill has three rolls: the originating chamber's own version,
the second chamber's amended version, and the originating chamber again on the
enacted text. Selecting the *later* originating-chamber roll therefore lands on
the enacted text with no version-split description needed.

## ⚠ Crossfiled twins: Maryland chapters BOTH of them

Maryland's crossfiled HB/SB pairs are not "one lives, one dies" — **both twins
are commonly enacted, as consecutive chapters with identical text**: HB 424
(Ch. 611) / SB 357 (Ch. 610); HB 930 (Ch. 435) / SB 848 (Ch. 436); HB 872
(Ch. 524) / SB 606 (Ch. 525); HB 39 (Ch. 651) / SB 356 (Ch. 652); HB 1035
(Ch. 626) / SB 937 (Ch. 625); HB 1123 (Ch. 103) / SB 181 (Ch. 102). **34 twin
pairs are divided-and-enacted on both sides.** So "pick the chaptered one" does
not narrow anything here — the rule is **import exactly one twin per pair**, or
every legislator gets two records making the same claim.

## ⚠ Maryland has no date-skew risk (the opposite of Illinois)

Maryland stops the clock, so its records carry two dates. The official vote-record
PDF prints `Calendar Date: Mar 17, 2025 4:01 (PM)` and `Legislative Date: Mar 8,
2025` for the same roll: the **Calendar Date is the real wall-clock date** and the
Legislative Date is the fictional legislative day. LegiScan stamps the Calendar
Date. Audited against the official bill-page history for all 20 batch-01 rolls:
20/20 exact. Illinois's `official_vote_date` override (migration 257) is not
needed for Maryland.

## Batch-01

11 measures / 20 rolls / **1,599 records across 158 candidates**. See
`batch-01/PLAN.md` and `batch-01/JUDGING.md`.

## What is left

**300 divided-and-enacted rolls on 165 measures** remain for batch-02+ (worklist
regenerable from the dataset with the divided gate; the twin-pair rule above
removes roughly a third of it). Also unworked: the 105 divided rolls on measures
that did not become law, and the entire **2026 Regular Session (LegiScan 2240)**.

**Roster note:** our Nov-2026 Maryland pool holds 300 state-legislative
candidates and the crosswalk maps 158 of the 189 sitting members. 31 members map
to nothing — most are not seeking a General Assembly seat, but a later Maryland
roster campaign will close the remainder (Speaker Adrienne Jones, Senate
President-adjacent members and several sitting Delegates have no 2026 row yet).
Re-running the import after extending the crosswalk adds those members
**idempotently**.

**PROD IS UNTOUCHED.** All 1,599 records are on local `voteapp` only.
