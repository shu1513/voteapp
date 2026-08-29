# PA batch-02 — judging notes

Judged 2026-08-29 from the House Appropriations fiscal-note ANALYSIS for the
printer's number each roll actually voted. No AI provider call anywhere.

## Wording rule for a bill that did not become law

Every body uses the conditional — "which would have required…" — the Ohio LSC
convention for provisions that never took effect. Descriptions never assert
that any of these bills changed the law, because none did.

The tail is time-stamped rather than absolute: "The Pennsylvania House passed it
143-60; as of August 2026 the Senate had not voted on it." The 2025-2026 session
runs to 30 November 2026, so a bill still sitting in the second chamber could
in principle still move. A flat "did not become law" would be a claim that could
later turn false; a statement about a past state cannot.

Failed votes take a different tail: "The measure failed in the Pennsylvania
House 101-102."

## Version check

Pennsylvania prints the printer's number in the roll-call desc, so each
description is written against the exact text that chamber voted. Because none
of these bills reached a second chamber, there is no later amendment for a
description to drift from — the version risk that produced the HB 103 split in
batch-01 does not arise here.

## Facts pinned to the analysis, with the qualifications carried

- **HB 1957 is a proposed constitutional amendment on FIRST consideration.** It
  did not create a right. A Pennsylvania amendment must pass both chambers in
  two consecutive sessions and then be approved by voters; the description says
  so, so no reader can take it as settled law. This is the Texas HJR 98 lesson.
- **HB 2103 carries its exemptions.** The description names the religious
  exercise clause for churches and other tax-exempt religious bodies, the
  carve-outs for rented rooms in a personal residence, landlord-occupied rooming
  houses and single-sex dormitory rooms, and the dress-and-grooming provision.
  Stating only the ban would flatten a qualified statute — the TX SB 2972 rule.
- **HB 1445** — insurers could still refuse an unlicensed provider, one working
  outside their scope, or a service not medically necessary. Named.
- **HB 583** — restores pre-September-2011 adult dental coverage, but the
  department may offer fewer services if funding falls short. Named.
- **HB 1593** deletes an existing exemption rather than creating a new check:
  private sales of long guns currently escape the background check that
  handguns already require. The description says that.
- **HB 1549** gives the actual county-class tiers ($15 Philadelphia, $12 larger
  counties, $10 rising to $12 elsewhere) and the 60% tipped rate, not the
  headline number.
- **HB 111** is not a health-coverage bill; it stops LIFE insurers penalising
  someone for filling a naloxone prescription, so it is labelled
  corporate_accountability, not healthcare_affordability.
- **HB 1100** is food assistance, not health insurance, so it is labelled
  social_programs_and_welfare.

## Labels

Direction follows the area description, not the bill. All 32 are `for` their
area: each expands coverage, access, protection or rights that its area
describes. There is no Senate measure in this batch, so no `against` direction
appears — that is an artefact of which chamber's bills were verified first, not
a slant. The screened queue for batch-03 contains 8 Senate measures whose
direction is `against` (RGGI repeal, emissions-inspection repeal, transgender
sports restrictions, firearm preemption).

## Dropped after reading the analysis

- **HB 1077** — creates a Commission on Children's Vision and nothing else.
  Standing up an advisory body has no honest for/against on healthcare
  affordability.
- **SB 614** — no fiscal note exists at the voted printer number; moved to
  `pending:needs-detail-read` rather than judged from its title.

## Import ledger

Dry run: 32 files, 0 errors, **5,642 planned inserts**, 0 notified.
Real run: 32 files all `imported`, 0 errors, **5,642 inserts**, 180 candidates.

Batch stamp `origin_run_id LIKE 'rollcall:PA:%:2026-08-29T06:47:30.145Z'`
returns 5,642 / 180. The dry run's own stamp `…T06:47:01.238Z` matches **zero**
rows. Batch-01's stamp still returns exactly 882, proving the per-run stamp
separates batches. PA totals: **6,524 records, 6,700 tags.** A dry re-run
reports all 5,642 `unchanged` (`import-dry-run-rerun-report.json`);
`import-report.json` is the original insert ledger, copied aside first.

`candidate_records` 81,744 → 87,386. The batch-01 baseline was 79,059, so the
local database gained rows from a concurrent session in between — the local
`voteapp` is shared. The run-stamp predicate, not the table delta, is the
authority, and it reconciles exactly.

**One `related` flag, correctly not a duplicate:** a hand-written record for Ann
Flood cites the same House Journal source but describes a different bill
(HB 1442, Morgan Rose's Law). Distinct claim, kept. 0 `ambiguous`.

**PROD UNTOUCHED.**
