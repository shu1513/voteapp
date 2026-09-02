# Maine — LegiScan roll-call import (session 2181, 132nd Legislature)

Phase-4 LegiScan campaign, state #7 (after TX, GA, FL, IL, TN, CA). Dataset =
LegiScan session **2181**, "2025-2026 Regular Session" of the 132nd Legislature
(both years in one file, sine die), downloaded 2026-08-29 from the bulk dataset
API with the key held in the main checkout's `backend/.env`. Maine `state_id`
is 19. The ZIP and the extracted dataset live OUTSIDE the repo at
`/Users/shu/legiscan-data/me-2181/`; the 1,579 per-roll evidence JSONs live at
`/Users/shu/legiscan-data/me-2181-evidence/`. This directory keeps the curated
subset.

## The feed

| | |
|---|---|
| bills | 2,454 (971 became law, 8 vetoed, 1,281 died) |
| roll calls | 1,580 |
| people | 188 (151 House + 35 Senate seats plus mid-biennium turnover) |
| committee votes | **0** — every tally is a full chamber (House 149-151, Senate 35) |
| summary-only rolls | **0** (every roll carries member positions) |
| duplicate identities | 1 (the TX `roll_call_id` fix is all but a no-op here) |
| tally mismatches / parse errors | 0 |

**Maine takes a roll call only when members demand one.** Most questions are
decided by an unrecorded division, so the recorded set is small — but it is
also almost entirely contested: **1,450 of the 1,580 rolls are divided** under
the phase-2 gate (loser ≥ 25% of winner). The state's habit filters the feed
down to exactly the votes this campaign wants.

## Vocabulary (see `survey/desc-families.json`)

Every desc, in BOTH chambers, ends with the clerk's roll number (` RC #<n>`);
1,579 raw descs fold to 227 families. The config's patterns tolerate that
suffix and never anchor on `$` without it.

**Maine passes a bill by ACCEPTING ITS COMMITTEE REPORT.** The substantive
floor question is `Accept Majority Ought To Pass As Amended Report` (Senate) /
`Acc Maj Otp As Amended Rep` (House), plus ~40 further spellings of the same
act. Enumerating them is hopeless, so the kept rule is an ought-to-pass token
test (`otp`, `otp-am`, `ought to pass`) with the ought-NOT-to-pass acceptances
excluded FIRST — a vote to accept an "Ought Not To Pass" report kills the bill,
and must never fall through as passage. Later stages have their own questions:
`Passage To Be Engrossed`, then `Enactment` (Maine's true final passage, also
`- Emer`, `- Bond Issue`, `- Mandate`, `Enact-Emer 2/3 Elect`, `Final
Enactment`, `Final Passage`). `Recede And Concur` is the concurrence analog — a BARE `Recede` is the chamber
undoing an earlier action of its OWN (LD 209's Senate `Recede`, 12-20, is
budget amendment machinery), so those 12 rolls surface instead (a review
finding, fixed in the config PR).
Veto questions are `Veto Override (2/3)` (Senate) and `Reconsideration - Veto`
(House), so the plain `Reconsider` exclusion carries a lookahead.

Classification over the dataset: **933 kept floor** (815 passage / 109
concurrence / 9 veto override), 616 excluded, **31 surfaced**. 19 of the
surfaced are `Accept Report`, `Acceptance Of Report` and `Acc Majority Report`,
which never say WHICH report — a yea might pass the bill or kill it — and the
other 12 are the bare `Recede` rolls, kept out for the reason above; all are
left for a human. That bucket earned its keep immediately: LD 2231's 2026-03-10
Senate `Accept Report` (18-13) turns out to be acceptance of a committee's
study report before referral, not a vote on the bill.

## Roster reach

`crosswalk.json` — **188 entries: 132 mapped, 56 explicit null.** Built from
128 proposer entries (all accepted; `proposals-report.json` keeps the machine
output) plus 4 hand-added:

- **Matthea Larsen Daughtry** (SD-23) — LegiScan carries the maiden name
  inline; the candidate files as Matthea Daughtry.
- **William "Billy Bob" Faulkingham** (HD-012, running for SD-06) — our
  candidate row splits the name as first `Billy` / last `Bob Faulkingham`, so
  no surname match was possible from either side.
- **Michael "Mike" Tipping** (SD-8) — nickname.
- **Richard "Rick" Bennett** (SD-018) — sitting senator running **statewide for
  Governor**, so he is outside the state-legislative pool the proposer
  searches. LegiScan lists him R (2025); the 2026 candidacy is Unaffiliated.
  The highest-value entry in the file, and the only one of its class here.

15 proposals carry `seatAgrees: false` and all were accepted: Maine's four-term
limit makes House↔Senate switching routine, and every one is a full-name match
with a matching party. The one party mismatch is **Aaron Dana**, the
Passamaquoddy tribal representative (`HD-TRIBE`, LegiScan party `N`, our row
Independent) — he is 1 of 188 people who **never appears in any roll call**,
because Maine's tribal representatives hold no vote. His entry is mapped and
simply never fans out.

Validation over all 1,579 rolls: matched 100,375 / unmatched_reviewed 40,340 /
`no_crosswalk` 0 / `out_of_scope` 0 / 0 file errors.

**Fan-out: House median 102 candidates per roll (max 106), Senate median 24**
(TX 114/13, GA 149/42, IL 92/33, CA 21/11 before its roster fix). Both chambers
are fully up in Nov-2026 — all 151 House seats and all 35 Senate seats — so a
single judged measure reaches ~126 candidates across the two chambers.

## Judging sources (all official, all free of sponsor advocacy)

- **Enacted text** = the `Chaptered` PDF in LegiScan's `texts[]`
  (`legislature.maine.gov/legis/bills/getPDF.asp?paper=<HP|SP####>&item=<n>&snum=132`),
  which carries the chapter number and the governor's action in its header.
- **Neutral summary** = the `SUMMARY` section at the END of every printed bill
  and every committee amendment PDF, drafted by the Legislature's law office.
  There is **no sponsor statement of intent anywhere** — the Texas
  advocacy-preamble hazard does not recur.
- **Version check** = LegiScan's `history[]` action trail, which names each
  amendment as it is `READ and ADOPTED` or `READ and FAILED ADOPTION`, with the
  date and (for Senate actions, and many House ones) the journal roll-call
  number and tally.

### Hazards found in Maine

1. **LegiScan's `amendments[].adopted` flag is NOT reliable here.** LD 1126
   lists all three amendments with `adopted: 1`, but the history shows Senate
   Amendment "A" (S-403) — a full substitute that would have replaced the bill
   and changed its title — **FAILED ADOPTION**. Read the history trail, never
   the flag. (Judging LD 1126 off S-403 would have described a completely
   different bill.)
2. **Chaptered PDFs interleave struck and inserted text**, and `pdftotext`
   renders both plain: "Every Except as authorized in this section, every drug
   dispensed…". Structure is the tell — "is further amended by enacting at the
   end a new paragraph to read" is a pure addition. Where a stance hinges on
   struck language, render the page and look at it (the Georgia rule).
3. **A history line can carry a stale roll-call number.** LD 2231's
   2026-04-08 Senate line repeats "Roll Call Number 752 Yeas 18 - Nays 13" from
   a March action; the actual vote record is RC #915, 20-14. The `roll_call`
   records are the reliable side; the history is the date/version witness.
4. The `display_ps.asp` bill status page renders its action table with
   JavaScript, so a plain fetch returns only the header block (final
   disposition, chapter number, committee report). It is still the quickest
   confirmation that a bill became law and how.

## Status

**The Maine campaign is complete.** Batches 01-06 are imported on the LOCAL
`voteapp` database only — **6,949 records across 131 candidates and 15 research
areas**. **Production has zero Maine roll-call
records.** See each batch's `JUDGING.md` for what was judged, what was dropped
and why.

`survey/divided-enacted-worklist.tsv` is the ledger: 463 divided rolls on
measures that reached the governor, **433 of them on measures that became
law**. **110 are judged across 55 measures** (24 in batch-01, 22 in batch-02, 24 in
batch-03, LD 613's 2 in batch-04, 20 in batch-05, 18 in batch-06). The
remaining **323 rolls on 160 measures do not clear the filters**: appropriations
and study resolves, procedural measures, technical utility and tax bills, and
contested-direction measures dispositioned with reasons in the batch PLAN files.
`batch-06/PLAN.md` sets out why the campaign stops there.

**batch-04 is a single measure, LD 613**, the Death with Dignity waiting
period — the most-voted measure in the pool, imported under `general` with **no
stance** rather than dropped. Its `JUDGING.md` carries the reasoning. The other 30 rolls sit on the 7 vetoed bills, whose overrides
all failed (`CODE-FINDINGS.md` §2).
