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

All 20 originally selected rolls were checked against the official bill-page
history: **20/20 exact.**
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
| retraction (see below) | — | HB 767's 153 records retired; batch now 18 rolls / 1,446 records |
| convergence (dry) | — | **1,599 unchanged**, 0 errors |

Reconciled three ways:

- the ledger (`import-report.json`) says 1,599 inserts across 20 rolls;
- `origin_run_id LIKE 'rollcall:MD:%:2164:%:2026-08-29T05:24:30.345Z'` returns
  **1,599 records across 158 distinct candidates**, with **1,599** matching rows
  in `candidate_record_area_tags`;
- the **dry-run** stamp `…05:23:…` matches **0** rows — positive proof
  `--dry-run` is inert.

**158 candidates = every candidate the crosswalk maps** (still true after the
HB 767 retraction). Unlike Texas (Speaker
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


## Retraction: HB 767 (post-import review response)

A PR #941 review finding, accepted after re-reading the area definition: HB 767
adds eviction notice requirements and tenant civil remedies — it does not
increase housing supply or reduce renter/homebuyer costs, which is
`housing_affordability`'s defined scope ("Increase housing supply and reduce
cost burdens for renters and homebuyers"). Every other housing call in this
campaign is supply- or cost-side (TX SB 15 lot sizes, TX SB 2835 single-stair,
MD's own dropped HB 390 PILOT). No other area fits honestly, so under selection
filter 5 the measure is **dropped, not relabeled**.

Retraction mechanics (the H.R. 1047 recipe):

1. **153 records retired** via `manual:records:retire`
   (`hb767-retirements.json`, committed; reasons name the finding). Their 153
   tags are mooted by retirement.
2. **Both rolls set back to `pending`** — allowed because the store's
   withdraw-guard counts only live (un-retired) fanned-out records.
3. The two entries removed from `judgments.json`; the two roll evidence JSONs
   removed from `batch-01/` (they remain in the out-of-repo evidence store).
4. Convergence dry run: **18 files, 1,446 unchanged, 0 errors**.

Final totals: **10 measures / 18 rolls / 1,446 records / 158 candidates**;
tags are 1,303 after the nay repair below (1,090 yea-side + 213 nay-side) — candidate coverage is unchanged, since every HB 767 voter also
appears in other batch rolls. MD queue now 18 approved.

## Shared-DB tag repair (found during the retraction verification)

Between the import (Aug 28) and the review response (Aug 29), **10 area tags on
4 un-retracted rolls vanished** while their records' `updated_at` stayed at
import time — another session on the shared local DB deleted tags out from
under roll-call records (the Florida Woodson hazard: the quality sweep and
roll-call imports fight over the same rows). Fixed the sanctioned way: a real
import re-run, which reports all rows `unchanged` but re-syncs tags to the
judgments file (`import-tag-resync-report.json`). Tag count verified back at
1,446 for 1,446 live records. The original insert ledger `import-report.json`
was preserved across the re-run (the Tennessee hazard).
## `related` flags — 7, none a duplicate

Seven rolls flagged a related pre-existing record. All are **co-sponsorship**
rows on Cheryl E. Pasteur (HB 1222, HB 1424) and Courtney Watson (HB 1424),
hand-researched from the same MGA bill pages. Co-sponsoring a bill is a
**distinct claim** from voting for it (the federal pilot's Grijalva
discharge-petition precedent), so all were kept and **nothing was retired**. A
sweep confirmed the local database holds **zero** pre-existing Maryland
vote-claim records for 2025, so there were no duplicates to find.

## Prod

**PROD IS UNTOUCHED.** All 1,446 live records are on local `voteapp` only
(plus the 153 retired HB 767 rows). Promotion
is a separate step.

## Nay repair (2026-08-30): authored nay stances, 143 mechanically flipped tags dropped

Batch-01 was judged and imported hours before the explicit-`nay` contract
landed in main (PR #950's line: a no vote is not automatically the opposite
claim; `nay` is authored, never inverted). Its 356 nay-side tags were all the
mechanical inverse. This repair authors the nay side of every label, under the
Connecticut #960 test: **is the bill's core mechanism the AREA's own
mechanism, so that a no vote is a vote against that mechanism, with no other
plausible strand to object to?**

**`nay` stated — 6 measures, 213 tags kept:**

| measure | nay | why |
|---|---|---|
| HB 1222 | against | single-subject immigration-enforcement bill; a no is the enforcement-first position, the area's own axis (the TX SB 8 mirror) |
| HB 424 | against | the whole bill is drug price caps, the area's literal cost mechanism (CT HB 5004 analog) |
| SB 901 | against | focused single-mechanism producer-responsibility bill (the CT HB 5004 pattern, not the SB 9 omnibus pattern) |
| SB 848 | against | the entire act improves abortion-care access, the area's literal mechanism |
| HB 1020 | against | single-subject consumer protection, in the area description outright (CT SB 3) |
| HB 1378 | **for** | yea is `against`; the only coherent no keeps victims' full recovery — a fiscal hawk votes yes |

**`nay: null` — 4 measures, 143 tags dropped** (nay voters keep their vote
record, lose the area tag):

| measure | dropped | why a no vote is not an area stance |
|---|---|---|
| HB 1424 | 37 | multi-strand: loan fund + expedited hiring + AG Maryland Defense Act powers + Rainy Day Fund transfers + a mandated appropriation — a no plausibly reads as fiscal (CT SB 1 pattern) |
| HB 39 | 37 | the no vote's obvious strand is public safety (keep a criminal tool against knowing HIV transmission), not opposition to equal rights (CT SB 1328 pattern) |
| HB 983 | 32 | a no most plausibly objects to the unfunded mandate on local election boards, not to elections being trusted |
| HB 197 | 37 | restorative-practices discipline is a contested pedagogy inside the area — both sides claim school quality |

Mechanics and proof, measured in `candidate_record_area_tags` (never the
import report — a labels-only change reports every record `unchanged` because
tag sync runs separately from the record compare):

- judge: **10 updated / 8 unchanged** — exactly the 6 authored measures'
  rolls; the null entries compare equal because `canonicalLabels` reads a
  missing `nay` as null, so their rows are deliberately untouched.
- import: 18 files, **1,446 unchanged**, 0 errors; nay-side tags
  **356 → 213**, the exact 143 predicted before the run, all four null
  measures fully untagged on the nay side, all six kept measures intact
  (41/39/37/37/28/31), yea side untouched at 1,090.
- Both new approval gates passed with no edits: every description already
  cites its own roll's tally, and no changed roll has a same-chamber kept
  floor peer on or after its date — the batch's only same-day pair (HB 1424's
  two Senate rolls) sits on a null measure whose entry is unchanged, so no
  `acknowledge_later_rolls` was needed.
- Repair ledger: `import-nay-repair-report.json`; `import-report.json`
  remains the original insert ledger.

## Plain-language rewrite (2026-08-30)

All 36 descriptions were rewritten in everyday English, aimed at a reader with
no legal or legislative background. The originals were accurate but written in
statute voice: they left terms of art bare (`exigent circumstances`,
`upper payment limits`, `noneconomic damages`, `previously time-barred`,
`creditworthiness`, `mandated appropriation`, `consumer reporting agency`,
`restorative practices`) and echoed statutory hedges that tell a voter nothing
(`specified individuals`, `stated criteria`, `the listed services`).

The rewrite used **only the existing evidence** — the DLS fiscal notes already
in the scratchpad for this batch — and the current descriptions. No AI
provider was called, nothing was re-researched, and no fact, date, number,
tally, chapter, stance direction, or label changed. Where a term needed a
gloss, the gloss was taken from the fiscal note rather than invented: the
Maryland Defense Act one from the note's own summary of Chapter 26 of 2017
("the Attorney General may investigate, commence, and prosecute or defend"
suits over federal action that threatens Marylanders), and restorative
practices from the note's quoted statutory definition ("a communally and
culturally responsive, relationship-focused student discipline model").

Sample, HB 39 — before and after:

> The bill struck the misdemeanor prohibition on knowingly transferring or
> attempting to transfer HIV to another person, along with its penalty.

> Knowingly giving or trying to give someone HIV had been a misdemeanor, the
> lower level of crime. The bill removed it and its penalty.

**Invariants asserted in the rewrite script, before anything was written:**
every non-description field byte-identical at rewrite time (one field then
changed separately and deliberately: the HB 1424 Senate entry gained
`acknowledge_later_rolls` when the superseded-stage gate fired — see below); each description still cites its
own roll's tally (the same tally as before); every number in the original
still present; no `", The "` comma splice; every sentence at most 45 words;
the batch total shorter than the original; and no single description grown by
more than 10%. Batch length fell **27,232 → 25,434 characters (-1,798)**.
Longest sentence **45 → 36 words**, mean 18. The plain-language lint reported
**0 warnings** over all 36 descriptions before the import.

### The superseded-stage gate fired once, correctly

HB 1424's Senate roll 1572563 was flagged against same-day peer 1566202. The
official MGA history settles the order the gate cannot see: the Senate passed
its own version **34-13** (1566202), the House refused to concur, a conference
committee met, and the Senate then adopted the conference report and re-passed
**33-13** (1572563). The judged roll is the chamber's final action, so the
entry carries `"acknowledge_later_rolls": [1566202]`. This is the same
same-day-peer situation Connecticut documented in #960; it stayed latent
through the nay repair only because that measure's entry compared unchanged,
so the gate never ran on it. No other roll in the batch has a same-chamber
peer on or after its date — verified with a direct query, not by iterating on
gate errors.

### Import

Dry run and real run agreed exactly: **18 files, 1,446 rewrites, 0 inserts,
0 errors, 0 notified**, stamp `2026-08-30T…25.560Z`; convergence re-run
**1,446 unchanged**. Records stay **1,446 across 158 candidates** — a wording
change rewrites in place (the Texas batch-02 precedent) and the freeze trigger
permits description edits on approved rows.

**Labels were guarded, not assumed.** The federal pass had to strip tags the
importer re-added on existing records; here a count-and-hash taken before the
run was compared after it and is **byte-identical** (1,303 tags, md5
`2a12def6…`), because MD's labels already matched its judgments exactly after
the nay repair. Ledger committed as `import-plain-language-report.json`;
`import-report.json` remains the original insert ledger.

**Production still holds the old wording** — it has no Maryland roll-call
records at all, so re-promotion is separate later work.

### Review fixes (2026-08-30): four descriptions corrected, 614 records rewritten

PR review found four real wording defects, each checked against the DLS
fiscal note (SB 901 against the note's producer definition) before fixing:

- **HB 1222 (157 records):** "state and local government offices" had
  broadened the law's reach. The duty covers public schools, public
  libraries, and **Executive Branch** units of state or local government
  **operating at a defined "sensitive location"** — now stated, with the
  note's list (schools, libraries, courthouses, government-run health care
  sites).
- **HB 197 (149 records):** "instead of punishing" was invented. The note's
  definition is a relationship-focused student discipline model including
  personal accountability; the description now uses those terms and makes no
  claim about eliminating punishment.
- **HB 1424 (155 records):** "It moved $5.0 million" stated a completed
  transfer. The bill **let the Governor move up to** $5.0 million — now
  worded as the authorization it is.
- **SB 901 (153 records):** "companies that produce" read as manufacturers
  only. The statutory producer generally includes brand owners,
  manufacturers, and importers — now "the companies behind packaging and
  paper products - including makers, brand owners, and importers".

Judge: 8 updated / 10 unchanged. Import dry and real agreed: **614 rewrites /
832 unchanged, 0 errors**; convergence 1,446 unchanged; records 1,446 / 158;
tag count-and-hash again byte-identical (1,303, md5 `2a12def6…`). Ledger:
`import-review-fixes-report.json` (note: a real re-run writes
`import-rerun-report.json` when `import-report.json` exists — renamed after
verifying its contents).

The review also flagged the invariant sentence above as contradicting the
diff (`acknowledge_later_rolls` was added in the same PR): correct — the
sentence now states the exception explicitly.
