# Illinois batch-04 — the first 25 measures of the unbatched pool

The 204 rolls left after batch-03 were triaged, never read. This batch reads the
first 25 measures of that pool in bill-number order and keeps 10 of them:
**14 rolls / 984 records across 132 candidates.**

Every measure was read from the Illinois General Assembly's own BillStatus XML
at `https://ftp.ilga.gov/Legislation/104/BillStatus/XML/10400<BILL>.xml` — the
Legislative Reference Bureau synopsis for each version, plus the action trail
that says which amendments were adopted. No judgment came from a short title.

## What was kept

| measure | rolls | area | what the yes vote was |
|---|---|---|---|
| HB 1430 | H 67-38, S 46-12 | `cost_of_living_reduction` | Treasurer may refinance student loans; income share agreements removed |
| HB 1616 | H 70-34 | `corporate_accountability` | part-time workers get the organ donor leave full-time workers had |
| HB 1859 | S 46-12 | `public_education_quality` | AI may not be the only instructor of a community college course |
| HB 2397 | H 78-27, S 33-19 | `civil_rights` | yearly public report on hospice care in state prisons |
| HB 2419 | H 77-39, S 37-19 | `environment_and_public_health` | local waste-siting decisions must weigh cumulative pollution |
| HB 2464 | H 84-30 | `healthcare_affordability` | no out-of-network cost for emergency newborn intensive care |
| HB 2516 | H 77-39 | `environment_and_public_health` | PFAS sales ban from 2032 on five product classes |
| HB 2726 | H 75-37, S 39-16 | `environment_and_public_health` | rewilding becomes a Department of Natural Resources power |
| HB 2987 | H 83-28 | `corporate_accountability` | warehouses must have tornado plans and shelter space |
| HB 3019 | H 84-32 | `healthcare_affordability` | insurers must report loss ratios and pay rebates |

Every kept roll is the last divided floor vote its chamber cast on the measure.
Two earlier House rolls (HB 2419 roll 1544981, HB 2516 roll 1543848) are marked
`screened:superseded-roll`: the House voted again on the enacted Senate version,
and that concurrence is the imported roll.

## Two measures were dropped because their divided roll is not a vote on the law

This is the README's fourth Illinois hazard, and it caught two of 25 measures.

- **HB 1085** (mental health parity). Senate Floor Amendment 3 replaced the
  whole bill, moving rate-setting from the Department of Insurance to a
  Department of Human Services rate floor keyed to billing codes. The House's
  vote on that enacted text was **86-19 — not divided**. The 72-33 roll in the
  pool is a vote on a version that did not become law, so importing it would
  attribute to a legislator a position on a bill the chamber later passed by a
  far wider margin.
- **HB 2771** (short title `DPH-CERTIFICATE FEES`). The divided House roll,
  76-40, was cast on a bill raising a health certificate fee from $10 to $65.
  The Senate then replaced the entire bill with hospital assessment increases
  that draw down federal Medicaid payments. Two different subjects under one
  number; the divided roll belongs to the subject that was discarded.

**Neither is visible from the title or from LegiScan's `description` field.**
Both were found only by reading the synopsis stack and the action trail.

## The other drops

Thirteen more measures were dropped, in three groups:

- **No research area fits (7)** — HB 1224 state contract retainage, HB 1332
  hospital emergency contacts, HB 1576 Court of Claims, HB 1699 water operator
  training, HB 1712 POLST training, HB 2488 federal reference updates, HB 2564
  teacher pension contributions, HB 3046 university bargaining. The last of
  those is the labor gap this campaign keeps hitting: **there is still no labor
  or union research area**, and it has now cost measures in Nevada, Kansas and
  Illinois.
- **The direction cuts both ways (4)** — HB 0742 (a one-year delay of the
  interchange fee ban), HB 1628 (forfeiture reporting both widened and
  narrowed), HB 1700 (bank reinvestment ratings *and* energy facility siting),
  HB 2394 (a weight allowance that helps clean fuels and adds road wear).
- **Two subjects of comparable weight (1)** — HB 1863 pairs a One Health Task
  Force with a Boards and Commissions Review Act repealing unrelated bodies.

## Reconciliation

Predicted independently from the crosswalk, then checked three ways:

| step | records |
|---|---|
| predicted from `crosswalk.json` | 984 |
| dry run | 984 `insert` |
| real run | 984 `insert` |
| re-run | 984 `unchanged` |
| rows in the local database | 984 |

Illinois now holds **5,824 roll-call records**, local `voteapp` only. Production
has never been touched by this campaign.

Descriptions read at a median Flesch-Kincaid grade of **7.1**, worst **8.4**,
with no sentence over 45 words and no British spellings.

## What is left

165 rolls of the original 204 still carry `candidate:unbatched` in
`../survey/divided-enacted-worklist.tsv`.
