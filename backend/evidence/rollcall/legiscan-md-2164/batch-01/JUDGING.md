# Maryland batch-01 — judging and import

## Source

Every description was written from the **Department of Legislative Services
FISCAL AND POLICY NOTE** for the version that became law, cross-checked against
the bill page's chapter line. No AI provider was called at any point.

The notes carry **no sponsor statement of intent** — checked on all 11 measures —
so the Texas hazard (an advocacy preamble whose numbers contradict the statute)
does not recur in Maryland. Each note states its own version in the header
(`Enrolled - Revised`, `Third Reader - Revised`), which agrees with the chapter
file's suffix on all 11 measures.

## Version check

Done per roll, mechanically, via the chapter-file suffix — see `PLAN.md`. Result:
**every one of the 20 imported rolls was cast on the text that became law**, so
no measure needed per-chamber descriptions. The superseded rolls are listed in
PLAN.md as deliberate drops.

## Date audit

All 20 rolls were checked against the official bill-page history: **20/20 exact.**
Maryland prints two dates (Calendar Date = real wall-clock, Legislative Date =
the fictional stopped-clock legislative day) and LegiScan stamps the Calendar
Date, which the official vote-record PDF confirms is the true one
(`Calendar Date: Mar 17, 2025 4:01 (PM)` on HB 1222's House roll). The Illinois
`official_vote_date` override was **not** needed and is not used here.

## Labels

Direction follows the **research area's own description**, not the bill:

- `immigration` = "Welcome immigration through a lawful, orderly, and humane
  system", so **HB 1222 limiting immigration enforcement at sensitive locations
  is FOR** — the mirror image of Texas SB 8, which scored `against` for the same
  area.
- `civil_rights` = "fair treatment under law". **HB 39** (repealing the HIV
  transfer crime) is FOR; **HB 1378** is AGAINST because it cuts what child
  sexual abuse victims can recover and caps their lawyers' fees. Both directions
  in one batch, on purpose.
- `election_integrity` = elections "trusted by the public": HB 983's translator
  access at polling places is FOR.
- `corporate_accountability` = "consumer protection": HB 1020 keeping medical
  debt out of credit reports is FOR (the Texas SB 1036 precedent).
- `healthcare_affordability`: HB 424 expanding the Prescription Drug
  Affordability Board's upper-payment-limit authority is FOR.

**HB 424 required care not to overstate.** The enacted bill lets the board set
upper payment limits by regulation for drugs *bought or paid for by state and
local government units*; its requirement to set limits on **all** purchases and
payor reimbursements is **contingent on specified actions taking place by
September 30, 2030**, and the bill separately bars the board from enforcing a
limit against Medicare Part C/D reimbursement rules, from counting a pharmacy
dispensing fee toward a limit, and from applying a new limit to a drug in
shortage. The descriptions carry all of those qualifications — the SB 2972
lesson: when a statute qualifies a power, the description must carry the
qualification.

## Plain-language lint — run BEFORE importing

`candidateRecordPlainLanguageLint` (45-word sentence line, warn-only; the
importer does not run it) was run over the judgments file before any import:
**40 records, 0 warnings.** This is the California lesson applied up front
instead of as a post-import rewrite.

The body-tail join was built **with a period**, and the judgments file was
asserted to contain no `", The "` before it was written — the comma splice that
hit Illinois batches 01 and 02.

## Import

Dry run, then the real run, then a dry convergence run:

| run | stamp | result |
|---|---|---|
| dry run | `2026-08-29T05:23:44.488Z` | 20 files, 0 errors, **1,599 planned inserts** |
| **real import** | `2026-08-29T05:24:30.345Z` | 20 files all `imported`, 0 errors, **1,599 inserts**, 0 notified |
| convergence (dry) | — | **1,599 unchanged**, 0 errors |

Reconciled three ways:

- the ledger (`import-report.json`) says 1,599 inserts across 20 rolls;
- `origin_run_id LIKE 'rollcall:MD:%:2164:%:2026-08-29T05:24:30.345Z'` returns
  **1,599 records across 158 distinct candidates**, with **1,599** matching rows
  in `candidate_record_area_tags`;
- the **dry-run** stamp `…05:23:…` matches **0** rows — positive proof
  `--dry-run` is inert.

**158 candidates = every candidate the crosswalk maps.** Unlike Texas (Speaker
Burrows) and Georgia (Speaker Burns), **the Speaker of the Maryland House votes**
and is listed by title in the official vote record, so there is no fan-out
shortfall — Illinois's shape.

`import-report.json` was **preserved before the convergence run** (the Tennessee
hazard: a real re-run overwrites it; a dry re-run writes
`import-dry-run-rerun-report.json` instead, which is what happened here).

Note on the raw row count: `candidate_records` moved 76,356 → 78,177 (+1,821)
across the real import, but only 1,599 of those are ours. Local `voteapp` is
shared with parallel state sessions and a concurrent writer added the other 222;
the run's own accounting is unaffected. Always reconcile by run stamp, never by
table delta.

## `related` flags — 7, none a duplicate

Seven rolls flagged a related pre-existing record. All are **co-sponsorship**
rows on Cheryl E. Pasteur (HB 1222, HB 1424) and Courtney Watson (HB 1424),
hand-researched from the same MGA bill pages. Co-sponsoring a bill is a
**distinct claim** from voting for it (the federal pilot's Grijalva
discharge-petition precedent), so all were kept and **nothing was retired**. A
sweep confirmed the local database holds **zero** pre-existing Maryland
vote-claim records for 2025, so there were no duplicates to find.

## Prod

**PROD IS UNTOUCHED.** All 1,599 records are on local `voteapp` only. Promotion
is a separate step.
