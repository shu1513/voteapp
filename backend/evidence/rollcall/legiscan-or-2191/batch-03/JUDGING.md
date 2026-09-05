# Oregon batch-03 — plan and judging

Nine measures, fifteen roll calls, **429 records across 61 candidates**.
Oregon totals after this batch: **972 records / 747 tags / 35 approved rolls.**
Production still has zero Oregon roll-call records.

| Measure | House | Senate | Area | Yea | Nay |
| --- | --- | --- | --- | --- | --- |
| SB 91 PFAS firefighting foam | 39-14 | — | environment_and_public_health | for | null |
| HB 2385 340B drug access | 35-20 | 17-13 | healthcare_affordability | for | null |
| HB 2563 insurance premium explanations | 33-22 | 21-8 | corporate_accountability | for | null |
| HB 2957 shortened claim deadlines | held | 16-6 | civil_rights | for | null |
| HB 3525 well water testing in rentals | 36-14 | 23-6 | environment_and_public_health | for | null |
| HB 3644 statewide shelter program | 33-11 | 19-10 | social_programs_and_welfare | for | null |
| HB 3546 data centre power rates | 37-17 | 18-12 | cost_of_living_reduction | for | null |
| HB 3792 low-income electric bill help | 38-13 | 22-7 | cost_of_living_reduction | for | null |
| SB 906 pay stub explanations | 35-17 | held | corporate_accountability | for | null |

SB 91's Senate vote was 27-3, below the divided gate, so only its House roll
is in the pool.

## ⚠⚠ The finding that shaped this batch: LegiScan puts a member on the wrong side of five Oregon rolls

Two measures here are imported for **one chamber only**, and a config change
came with them.

While version-checking HB 2957 the LegiScan tally (35-19) disagreed with the
tally Oregon's own bill history prints (36-18). Rather than treat that as a
one-off, **every divided-and-enacted roll in the session — all 393 — was
audited against the tally in the bill history**, the North Carolina batch-04
rule that a sample is not enough.

**388 match exactly. Five are off by one**, and because Oregon's journal
*names* the nay voters, the misplaced member is identifiable in each case:

| Roll | Measure | LegiScan | Oregon's journal | Wrongly listed as a nay |
| --- | --- | --- | --- | --- |
| 1543833 | SB 906 Senate | 19-8 | 20-7 | Girod |
| 1590950 | HB 2957 House | 35-19 | 36-18 | Boice |
| 1571965 | SB 817 House | 37-12 | 36-13 | — |
| 1595861 | HB 2005 House | 39-10 | 38-11 | — |
| 1594908 | HB 5015 House | 40-11 | 39-12 | — |

This is the Indiana defect recurring: a wrong side writes a **false record
about a named legislator**, and the approval check copies the stored tally
into the record text, so an import would publish the wrong number too.

All five are now listed in the Oregon config's `heldRollCallIds`, the
mechanism built for exactly this. They are stored and surfaced
(`is_floor_vote` null) and can never be queued or approved. A re-fetch moved
exactly those five rows and left the other 3,313 unchanged; floor votes fell
from 1,437 to 1,432.

The other three held rolls cost nothing: SB 817 and HB 2005 were already
dropped on their own merits, and HB 5015 is an appropriations bill excluded by
the standing rule.

**Rule for any state: audit every selected roll's tally against the state's
own record, and do it even when the member list looks complete.**

## What the enacted text changed

- **HB 2385.** The staff summary on file says the measure "requires certain
  discounts to be applied to an individual's out-of-pocket costs". That is not
  in the enrolled Act — it belongs to a superseded version. The description
  states only the two bans and the $5,000-a-day penalty.
- **HB 3525.** The summary describes the testing duty without its **key
  limit**: it applies only where the rental both draws on an exempt well
  *and* sits inside a state ground water management area. The description
  carries that limit, along with the four-year gap a clean result earns and
  the good-faith written agreement needed before a tenant can be asked to take
  the sample.
- **HB 3792.** Read as plain text the Act looks like it caps what a customer
  pays; the bracketed deletion shows it **raises** that ceiling from $500 to
  $1,000 a month while doubling the fund. Both facts are stated.
- **HB 3546.** "Large energy use facility" is a defined term — 20 megawatts or
  more, and mainly data hosting under a named industry code — so the
  description says what it means rather than repeating the label.
- **SB 906.** The penalty and the model-document duty are in a second section
  amending a different statute; both are described.

## Labels

All nine score `for`, and **all nine take `nay: null`**. In every case the
realistic objection runs on a different axis from the area: cost or compliance
burden (HB 2563, SB 906, HB 3525, HB 3546), firefighting effectiveness
(SB 91), programme design and spending (HB 3644, HB 3792), litigation exposure
(HB 2957), and drug-programme integrity (HB 2385). None is evidence that a
member opposes the area's goal.

Tag counts reconcile to that choice: 318 tags over 429 records in this batch,
the untagged records being every nay side.

## Checks

- Version check on all 15 rolls: each is on the text that became law. Four
  measures took the later concurrence roll because the other chamber amended
  after first passage.
- Superseded check run up front; no `acknowledge_later_rolls` needed.
- `related` 0, `ambiguous` 0, errors 0, notifications 0.
- Dry run matched the real run at 429 inserts; convergence reports all 429
  `unchanged`.
- Reading level measured: first drafts scored up to grade 11.2 and were
  rewritten before importing to **median 9.6, worst 10.2**. SB 91's 10.1 is
  driven by "perfluoroalkyl" and "Federal Aviation Administration", which have
  no plainer form.

## Ledger

`import-report.json` — the insert run, 429 records.
