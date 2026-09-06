# Oregon 2026 short session — campaign complete

Every measure in the gated pool carries a disposition. **Nothing is open and
nothing is parked.**

## Final state, local database

- **1,691 live records** across **63 candidates** — every candidate the
  crosswalk maps.
- **1,583 research-area tags.**
- **58 approved roll calls** over **36 measures**, in seven batches.
- **Production holds zero Oregon roll-call records**, for either session.

Batch counts: b01 293, b02 332, b03 163, b04 120, b05 299, b06 224, b07 260.

## The ledger, all 51 measures

| Disposition | Measures |
| --- | ---: |
| Judged and imported (batches 01-07) | 36 |
| Excluded, budget and fee ratification | 2 |
| Dropped: the divided vote was on a text that never became law | 2 |
| Dropped under filter 5 after a full read | 9 |
| **Total** | **51** |

## The two findings worth carrying forward

### 1. Do not bound the tally audit by the divided gate

Oregon's journal and LegiScan disagree on four of this session's 307 enacted
floor rolls. Three of the four would have been invisible to an audit that
checked only the rolls LegiScan itself calls divided.

One of those three matters. **SB 1565's House roll reads 45-10 in LegiScan,
which fails the divided test, and 43-12 in Oregon's journal, which passes it.**
A tally error can therefore decide whether a roll is in the pool at all, so the
gate cannot be trusted to bound the audit that checks the gate's own inputs.

Re-running the corrected audit against the 2025 session found **eleven**
mismatches where the original pass had found five. None of the six extra ones
changes the divided gate, so no 2025 record was affected — but that had to be
established, not assumed, and it was: a re-fetch moved exactly six stored rolls
and a re-import reported all 3,091 records unchanged.

The underlying cause in several cases is that Oregon lets a member change a
recorded vote by unanimous consent and prints the tally after the change. The
session has five such changes and LegiScan applies four of them. **The
mechanism does not predict which rolls are wrong**, which is the whole argument
for auditing every roll.

### 2. Oregon publishes a staff summary for the minority report too

The minority committee report gets its own staff measure summary, describing
amendments that the floor then voted down. Nothing in the file name
distinguishes it.

**SB 1594's minority summary** says the measure directs law enforcement to
cooperate with federal immigration authorities over convicted felons. The word
"felon" appears nowhere in the enrolled Act, and the Senate rejected that
report 11-18. Four measures here were first screened against a minority
summary. **No imported record was affected**, because every description is
written from the enrolled Act and the summary is only ever an index.

## Filter 4 did real work

Two measures were dropped because the divided vote was cast on a text that
never became law. HB 4145 divided the House 33-19 on the A-Engrossed text; the
Senate rewrote it, passed the new text 30-0, and the House concurred 50-3.
SB 1517 has the same shape in reverse. In both, the vote on the enacted text
was not divided.

Nine roll calls across the session are recorded from a **concurrence** rather
than a chamber's first passage, for the same reason in the other direction: the
concurrence is the vote on the text that became law.

## Directions

Most measures score `for`, which is the shape of a Democratic trifecta's
enacted agenda. **Four measures run the other way**, and in three of them the
roll call corroborates the direction rather than contradicting it:

- **HB 4060** carves school districts and million-square-foot building owners
  out of the mercury fluorescent lamp ban. Its Senate nays are mostly
  Democrats.
- **HB 4153** opens exclusive farm use land to farm stores and paid visitor
  attractions. Its nays are Democrats in both chambers.
- **HB 4018** delays the 2024 campaign disclosure law by three years. Here the
  nays came from both directions, so the yea takes a direction and the nay
  takes none.
- **HB 4179** extends the state's damages cap to doctors at OHSU nonprofits,
  and **SB 1507**'s tax strand raises what Oregonians owe.

## Areas covered

Twelve of the twenty-seven research areas: `immigration`,
`healthcare_affordability`, `environment_and_public_health`, `civil_rights`,
`housing_affordability`, `corporate_accountability`,
`public_safety_and_crime_control`, `anti_corruption`, `data_privacy`,
`womens_reproductive_rights`, `social_programs_and_welfare`,
`reduce_wealth_gap`, `personal_income_tax_reduction`.

**The labour gap bit again.** No research area covers labour standards, so
HB 4013 — a floor under the hours a child may work — went to
`social_programs_and_welfare` for want of anywhere better. That is now four
states where this gap has cost a clean label.

## Reading level

Measured before every import, never after. First drafts reached grade 13.3;
final batch medians ran **7.2 to 8.5**, with the lowest single measure at 4.6.

## Verification discipline

Every batch: a dry run matching the real run exactly, a third run reporting
every record `unchanged`, zero errors, zero notifications, and row counts
reconciled three ways — the import report's inserts, the rows in the database,
and the sum of the per-roll counts.
