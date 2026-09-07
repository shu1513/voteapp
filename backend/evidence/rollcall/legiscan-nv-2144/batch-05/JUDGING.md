# Nevada batch-05 — judging and import

**Result on local `voteapp`, 2026-09-06: 16 roll calls approved, 308 records inserted
across 41 candidates, 0 errors, 0 notified. Production untouched.**

Nevada now holds **1,418 live roll-call records**.

## Sources and version resolution

Every measure was judged from the text the chamber actually voted, downloaded through the
LegiScan `getBillText` API with both the byte length and the MD5 checksum verified against
the dataset. All documents passed both checks.

Which printed text each chamber voted was resolved from each bill's dated action trail,
using the rule established in batch-04: where a chamber amended on the floor and dispensed
with reprinting, the text it voted is the reprint printed immediately **after** the vote.

Three measures split between the chambers.

- **AB 185** — a real split, described in `PLAN.md`, with a separate description for each
  chamber. The Assembly voted the first reprint (which carried child care licensing changes
  in sections 7 and 8); the Senate voted the second reprint (which deletes them, adds
  tenant coverage, exempts condominiums and townhouses, and lets an association cap the
  number at one per 200 units).
- **AB 283** — the Assembly voted the introduced text, the Senate the first reprint. Three
  differences, all giving more time: the tenant's deadline gains a clause deferring to any
  federal notice period, and the landlord's window to prove service goes from 15 to 21
  calendar days in both the unpaid-rent and the other-breach procedures. Nothing in the
  descriptions turns on any of them.
- **AB 112** — the Assembly voted the first reprint, the Senate the second. The only
  operative difference is that a savings clause loses the words "or paid time off benefit".
  Paid time off still appears in the neighbouring paragraph, which neither version touched.

## Two facts the descriptions had to get right

**SB 391 lost with a majority in favor of it.** The Senate voted 13-8 for the bill. Nevada
requires a two-thirds majority — 14 of 21 senators — for a bill that authorizes a fee, and
the first reprint carries the header `REQUIRES TWO-THIRDS MAJORITY VOTE (§ 2)`. The action
trail records `Read third time. Lost. (Yeas: 13, Nays: 8.)`.

⚠ **LegiScan's `passed` flag says this vote passed, and the stored row's `result` reads
`Passed`.** LegiScan applies a simple-majority test and does not know about Nevada's
supermajority requirement. Anyone selecting Nevada rolls on `passed` or on `result` will
mis-describe this class of vote. The description says plainly that the vote was not enough
and the bill failed.

**SB 391's cap is 100 homes for every corporation in the state combined**, not 100 each:
"The total aggregate number of units of residential real property in this State that may be
purchased in any 1 calendar year by corporations, limited-liability companies and affiliates
of such entities must not exceed 100 units." New-build homes, apartment buildings and mobile
homes are outside the cap.

## Wording checks, all run before the import

- The real `candidateRecordPlainLanguageLint` over all 32 descriptions: **0 warnings**.
- Every description is 2 to 4 sentences; no sentence over 45 words.
- British spellings scanned for. One was found and fixed before importing: "licence" in
  both AB 185 Assembly descriptions became "license".
- Every description cites its own roll call's tally, checked against the stored row.
- Measure, date and chamber checked against the stored row for all 16 judgments.

## Reconciliation — three ways

| check | result |
| --- | --- |
| import report (seen at run time; file not kept — see review fixes) | 308 inserts, 1,110 unchanged, 0 errors |
| run-stamp predicate `rollcall:NV:%:2026-09-06T07:34:43.546Z` | 308 records, 41 candidates |
| table delta | 1,418 − 1,110 = 308 |

Per-roll fan-out: 30 candidates on every Assembly roll, 10 or 11 on every Senate roll. No
roll reached zero.

## Duplicate sweep

Swept with `origin_run_id NOT LIKE 'rollcall:%'`. Within a candidate there are **0
duplicate record identity keys and 0 duplicate source URLs** across all Nevada roll-call
records.

One hand-written row mentions a measure in this batch: a member who **introduced** AB 223.
Introducing a bill is a different act from voting on it, so it was kept, the same treatment
given to the AB 416 sponsorship row in batch-04.

## Review fixes, 2026-09-06 (PR #1193)

- **AB 185 (Assembly)** — the outdoor-play-space exception applies only to a facility in a
  multi-family dwelling or an apartment or condominium building (first reprint, sections 7
  and 8). The description said any qualifying home near a park; it now names the building
  limit. The drafting is ambiguous: "does not have an outdoor play space that is located:
  (1) in a multi-family dwelling or an apartment or condominium building; and (2) within 1
  mile of an accessible park" attaches "located" to the play space, not the facility. Read
  that way the waiver reaches every facility, because almost none has a play space inside an
  apartment building, and clause (2) would do no work. The description follows the only
  reading in which both clauses mean something: the facility is in such a building and
  within a mile of a park. The digest (lines 29 to 35) repeats the same sentence, so it
  settles nothing.
- **AB 223** — "added pests, mold, lead paint … to the list of things a rental must have"
  said the opposite of the bill. The bill requires *measures*: a unit is not habitable if it
  substantially lacks "effective measures in place to control the presence of rodents,
  insects and vermin" or measures to "prevent exposure to unsafe levels of radon, lead
  paint, asbestos, toxic mold". Lead paint present in an old building is not itself a
  breach, so the description says measures, not absence.
- **Import reports** — the two report files first committed here were batch-04's, copied
  by mistake. The importer's own report for this batch (`import-rerun-report.json` in the
  run dir) was overwritten by the batch-06 run before it was copied out, so it cannot be
  restored. The copies are removed. Evidence for the import is
  `import-db-reconciliation.json` — run-stamp counts taken from the local database, 308
  records across 41 candidates over all 16 rolls — plus batch-06's ledger, which lists all
  16 rolls as already imported.
- The rewrite ran in two passes, each with its own ledger:
  `import-review-rewrite-report.json` (07:54Z) covers AB 185, AB 223 and batch-04's SB 171 —
  **112 rewritten, 1,638 unchanged, 0 errors**. `import-rerun-report.json` (22:45Z) covers
  the second AB 223 wording pass — **41 rewritten, 1,709 unchanged, 0 errors**. The run dir
  holds only one rerun report and the second run overwrote the first, so the first was copied
  out before that run started. A database scan for either old wording returns 0; Nevada's
  live roll-call total is unchanged at 1,750.
