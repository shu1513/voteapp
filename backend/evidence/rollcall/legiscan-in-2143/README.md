# Indiana roll-call import — LegiScan session 2143

Indiana is the twelfth state in the phase-4 LegiScan rollout. The source is LegiScan
session **2143**, the 2025 Regular Session of the 124th General Assembly. That is
Indiana's long budget session; it adjourned on 2025-04-25, so the session is closed and
the dataset is final (dataset date 2025-12-07).

The dataset holds 1,489 bills, 1,010 roll calls and 151 people. It lives outside the
repository at `/Users/shu/legiscan-data/in-2143/`, with the 997 stored roll-call evidence
files at `/Users/shu/legiscan-data/in-2143-evidence/`. This directory keeps the curated
subset: the survey, the crosswalk, the people snapshot and each judged batch.

## Layout

- `crosswalk.json` — the reviewed LegiScan person to VoteApp candidate map, 151 entries.
- `legiscan-people-in-2143.json` — the people snapshot the crosswalk is checked against.
- `survey/` — the desc histogram the config was written from, the fetch ledger, and
  `divided-enacted-worklist.tsv`, which carries one row per divided-and-enacted roll with
  its disposition.
- `batch-01/` to `batch-04/` — judgments, the roll evidence files, and the import ledgers for
  each batch. Batch-04 kept nothing, so it holds only its selection notes.
- `CODE-FINDINGS.md` — the Indiana defects and source quirks recorded but deliberately not
  fixed here.

## What the feed looks like

Indiana is in the cleanest tier alongside Georgia and Maryland. The dataset holds **no
committee votes at all**: every roll's total is exactly 100 in the House or 50 in the
Senate. There are no repeated `roll_call_id` values, no summary-only rolls, no tally
inconsistencies inside a roll, and no parse errors.

Every description is prefixed with the voting chamber and a dash, for example
`House - Third reading`. The 155 distinct descriptions fold to 21 families. Final passage
is `Third reading`; the second chamber's agreement is `House concurred with Senate
amendments` and its mirror; conference reports are numbered. `Rules Suspended.` is a
scheduling prefix Indiana prints on end-of-session concurrences and conference reports and
does not change the question. Constitutional amendments ride joint resolutions, a bill type
the pipeline already keeps, so Georgia's resolution gap does not happen here.

The fetch reconciles exactly: 997 rows stored (813 floor, 174 excluded question, 10
surfaced) plus 13 rolls on excluded measure types equals the dataset's 1,010.

## Scale

160 of the 813 kept floor votes are divided under the campaign's usual test, where the
losing side is at least a quarter of the winning side (88 House, 72 Senate). **142 of those
are on measures that became law, across 68 measures** (76 House, 66 Senate).

Our roster covers all 100 House districts (172 candidates) and all 25 Senate districts on
the November 2026 ballot (49 candidates). Fan-out is a **median of 83 candidates per House
roll and 12 per Senate roll**.

## Judging sources

Indiana publishes both of the documents this campaign needs, and both are plain `curl` with
a browser User-Agent. `iga.in.gov` itself is a JavaScript application and `api.iga.in.gov`
needs a key, but the PDFs are served directly.

- **Fiscal Impact Statement**, written by the Legislative Services Agency's Office of
  Fiscal and Management Analysis. It opens with a neutral `Summary of Legislation`, states
  the effective date, and — like Maryland's fiscal notes — **stamps its own version in its
  header** (`BILL STATUS: As Passed House`, `BILL AMENDED: Jan 30, 2025`). There is no
  sponsor statement of intent anywhere in it, so the Texas advocacy hazard does not recur.
  `iga.in.gov/pdf-documents/124/2025/<chamber>/bills/<BILL>/fiscal-notes/<BILL>.<nn>.<STAGE>.FN<nnn>.pdf`
- **Enrolled act and every earlier version**, dated, at
  `iga.in.gov/pdf-documents/124/2025/<chamber>/bills/<BILL>/<BILL>.<nn>.<STAGE>.pdf`
  (stages: INTR, COMH, COMS, ENGH, ENGS, ENRH, ENRS). The dated stack is the version check.
- **The official roll-call PDF**, which gives the question, the tally and the full member
  list by side, at
  `iga.in.gov/pdf-documents/124/2025/<chamber>/bills/<BILL>/rollcalls/<BILL>.<journal>_<H|S>.pdf`

Two URL details worth writing down. The `<chamber>` directory is the bill's **chamber of
origin**, not the chamber that voted, so a House vote on a Senate bill lives under
`senate/`. And `<journal>` is Indiana's own roll-call number, which comes from the bill
history line (`Third reading: passed; Roll Call 83: yeas 88, nays 3`), not from LegiScan.

## The step this state adds to the recipe

**Every roll selected for a batch has its member list checked against the official
roll-call PDF before it is judged.** LegiScan's Indiana member lists disagree with the
official journal on 30 of the 1,010 rolls, and the disagreement is a member on the wrong
side, not just a wrong count. See `CODE-FINDINGS.md` section 2. Every roll used in batches
01 to 03 passed this check name by name. Batch-03 found four rolls that did not, and dropped
all four along with the measures that depended on them.

The tools that do these checks are in `tools/`.

## Reading a bill

Two things are worth doing the same way every batch.

**Read the annotated text, not plain `pdftotext` output.** Indiana prints additions in bold
and deletions in roman with a struck rule, and `pdftotext` flattens both into ordinary text.
`tools/annot.py` marks additions as `<<...>>` and deletions as `[[...]]`, so an amendment can
be read without rendering pages as images. See `CODE-FINDINGS.md` section 4.

**Take the version stack from the LegiScan dataset.** Each bill's JSON carries a dated link
for every printed version, so the version a roll actually voted on can be identified by date
instead of by guessing a filename. See `CODE-FINDINGS.md` section 5.

## State of the work

Batches 01 to 03, 05 and 06 are imported on the local `voteapp` database: **1,105 records across
102 candidates, over fifteen measures and twenty-three rolls.** Batch-04 read twelve measures and
kept none. **Production holds no Indiana records.**

Across both Indiana sessions the total is **2,432 live roll-call records over 104 candidates
with 1,951 area tags**.

19 measures and 35 divided-and-enacted rolls remain open in this session, each dispositioned in
`survey/divided-enacted-worklist.tsv`; one still carries the `needs member-list check` flag. What
is left is the state budget, five education omnibus bills, two elections omnibus bills and a run
of agency bills, so on the evidence of batches 04, 05 and 06 the expected yield is close to zero.

The 2026 Regular Session (LegiScan session 2234) is complete and has never been surveyed.
