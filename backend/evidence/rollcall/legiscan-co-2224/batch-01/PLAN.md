# Batch-01 — the whole special session

This batch is the entire 2025 First Special Session. The session is small
enough that one batch closes it: 11 measures were in the pool, 7 are imported
and 4 are dropped.

## What was imported

| measure | what it does | area | a yes vote is |
|---|---|---|---|
| HB 25B-1001 | makes permanent the rule that high earners add a federal business deduction back to state income | reduce wealth gap | for |
| HB 25B-1002 | adds five countries to the state's tax-haven list and taxes a federal break for foreign income | corporate accountability | for |
| HB 25B-1003 | ends the half-price premium tax for insurers with a Colorado home office | corporate accountability | for |
| HB 25B-1006 | puts $100 million into the health insurance affordability fund if Congress lets federal help expire | healthcare affordability | for |
| SB 25B-002 | pays clinics with state money when a new federal law blocks their Medicaid payments | women's reproductive rights | for |
| SB 25B-003 | rewrites the November 2025 ballot question so the money can also support SNAP | social programs and welfare | for |
| SB 25B-004 | delays Colorado's artificial-intelligence anti-discrimination duties by five months | civil rights | **against** |

That is 13 roll calls and 350 candidate records across 52 candidates.

## What was dropped, and why

**HB 25B-1004 — Sale of Tax Credits.** A financing device. It lets the state
treasurer sell tax credits to insurers and corporations, at 80 percent of face
value or a market price, to raise about $100 million for the general fund. No
research area describes a borrowing mechanism honestly.

**HB 25B-1005 — Eliminate State Sales Tax Vendor Fee.** This one runs both
ways inside any area that could hold it. Only retailers with under $1 million
in sales for the period keep the fee, so ending it raises costs for small
retailers; at the same time the act rewrites the share of sales tax going to
the housing fund. Neither strand is the clear point of the act.

**SB 25B-001 — Processes to Reduce Spending During Shortfall.** Internal
budget process. It moves the governor's shortfall power into the budget
statutes, makes the governor tell the Joint Budget Committee before suspending
state functions, and requires hearings when a forecast shows the reserve
dropping. It also shields the courts and the legislature from those cuts. This
is how two branches talk to each other, not a policy a voter takes a side on.

**SB 25B-005 — Reallocate Wolf Funding to Health Insurance.** A transfer that
cuts one program to pay for another: $264,268 out of gray wolf reintroduction
and into the health insurance affordability fund. It pulls against itself
across two areas.

## The three superseded chambers

A chamber is superseded when its **last** floor vote on the bill was lopsided,
even though an earlier vote was divided. The earlier vote is then not the vote
on the law.

- SB 25B-001, House: passed 56-4.
- SB 25B-004, Senate: repassed 29-3 after the House changes.
- SB 25B-005, Senate: repassed 31-3 after the House changes.

## Order of work

1. Survey the dataset and check every description against the existing
   Colorado patterns. Nothing was unmatched, so the config PR adds no pattern.
2. Fetch, then resolve members against the inherited crosswalk.
3. Verify every selected roll is actually stored, not an identity duplicate
   the fetcher folded away. Zero problems: this session has no duplicates.
4. Read each measure's final fiscal note, then its enrolled act.
5. Version-check every roll: the print in force on the vote date against the act.
6. Write the descriptions, measure the reading level, rewrite, re-read against
   the acts, then run the plain-language lint.
7. Dry run, real import, convergence run, reconcile three ways.
