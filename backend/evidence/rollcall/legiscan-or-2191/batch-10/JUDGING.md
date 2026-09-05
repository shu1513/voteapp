# Oregon batch-10 — plan and judging

Four measures, eight roll calls, **219 records across 61 candidates**. Oregon
totals: **2,570 records / 1,931 tags / 90 approved rolls.** Production has zero.

| Measure | House | Senate | Area | Yea | Nay |
| --- | --- | --- | --- | --- | --- |
| HB 3409 tagging 340B pharmacy claims | 32-18 | 18-12 | healthcare_affordability | for | null |
| SB 1168 per-visit pay in home health care | 38-12 | 18-12 | corporate_accountability | for | null |
| HB 3521 hold deposits for renters | 33-18 | 20-8 | housing_affordability | for | null |
| HB 3863 standard contracts for small generators | 40-15 | 21-8 | environment_and_public_health | for | null |

HB 3409 is the companion to batch-03's HB 2385: one stops drug makers blocking
the 340B discount, this one limits what drug plans may demand of the clinics
using it.

## Scope kept

- **HB 3409** does not ban the claim tag. It allows it in **set cases** — most
  importantly where the clinic already reports through a clearing house
  meeting the law's terms, and where the tag is needed to stop the same
  discount being taken twice.
- **SB 1168** is a widening, not a new rule: per-visit pay was already barred
  for nurses. The exclusion list is long and load-bearing, so it is named in
  full rather than summarised as "home health staff".
- **HB 3521** does three separate things — the landlord loses the right to
  keep the deposit only where the applicant walked away over **habitability**
  defects, the refund window moves from four business days to five, and a late
  refund carries a penalty of the deposit or the agreed amount, whichever is
  larger, unless an act of God caused the delay. It bites on deposits taken
  from January 1, 2026.
- **HB 3863** sets a floor on a cap. The state must set the qualifying-facility
  eligibility cap at **no less than** 10 megawatts, which is not the same as
  setting it at 10.

## Labels

All four score `for` with `nay: null`. Objections run on different axes:
program integrity and duplicate discounts (HB 3409), staffing models and
agency cost (SB 1168), landlord exposure (HB 3521), and ratepayer cost of
standard-contract power (HB 3863).

## ⚠ A process slip, recorded

This batch was **imported before the reading-level pass**, which is the wrong
order — every other batch measured and rewrote first. The first import scored
grade 10.7 to 11.1. The descriptions were then rewritten and re-imported,
which the importer handled as a rewrite in place: **165 rewrites, 54
unchanged**, with a convergence run afterwards reporting all 219 `unchanged`.
Row count never moved.

No harm done, because a rewrite is idempotent and the batch was never
promoted. But the ledger now has two files where every other batch has one,
and the rule stands: **measure the grade before the real import, not after.**

## Ledgers

- `import-report.json` — the insert run, 219 records.
- `import-rerun-report.json` — the reading-level rewrite, 165 records.

## Checks

Version check on all 8 rolls: each on the enacted text. Superseded check up
front, no acknowledgments. `related` 0, errors 0, notifications 0. Final
reading level **median 8.4, worst 9.4**.
