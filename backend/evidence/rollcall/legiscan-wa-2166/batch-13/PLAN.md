# Washington batch-13 — first not-enacted batch

Eleven House bills, eleven roll calls, 957 records across 89 candidates. Local database
only. Production holds no Washington records.

## Scope

Every measure here passed the House on a divided vote, and the Senate never held a floor
vote on it. None became law. The pool and every drop reason are in
`survey/divided-not-enacted-worklist.tsv`; the README's "Not-enacted pool" section explains
the gate.

| measure | House vote | subject | area, yes = |
| --- | --- | --- | --- |
| House Bill 1002 | 70-24 | PTSD claims for coroner and medical examiner staff | labor_rights, for |
| House Bill 1080 | 58-39 | all mandatory lodging fees in the advertised rate | corporate_accountability, for |
| House Bill 1291 | 56-39 | no deductible for labor and delivery | healthcare_affordability, for |
| House Bill 1423 | 57-40 | vehicle noise camera pilot | public_safety_and_crime_control, for |
| House Bill 1622 | 58-38 | bargaining over workplace AI | labor_rights, for |
| House Bill 2041 | 56-40 | postpartum Medicaid cut from 12 to 6 months | healthcare_affordability, **against** |
| House Bill 2095 | 53-44 | negligence presumption for drivers who hit vulnerable road users | public_safety_and_crime_control, for |
| House Bill 2244 | 70-26 | public records exemptions removed | anti_corruption, for |
| House Bill 2464 | 58-38 | incident reporting at private detention facilities | civil_rights, for |
| House Bill 2515 | 51-41 | rules for large data centers | environment_and_public_health, for |
| House Bill 2637 | 52-45 | personal information exempt from public records | data_privacy, for |

## Wording

Descriptions are conditional ("It would …") and end with a dated tail:
"The Washington House passed it 58-39. As of September 2026 the Senate had not voted on it,
so it was not law." A statement about a past date stays true even if a later session moves
the bill. The builder refuses a body without "would" and any of "became law", "this law",
"the act", "signed", "took effect" or "session laws".

## Duplicates

**None retired.** The tally sweep found one hand-written record, Peter Abbarno's "Won 70-24
House passage of his prime-sponsored HB 1002". It records his sponsorship as well as the
vote, and the roll-call import carries only the vote, so retiring it would delete a fact
nothing replaces (the batch-02 co-sponsorship rule).

## Reconciliation

- Dry run planned 957, the real run inserted 957, and the database holds 957 under the run
  stamp `2026-09-11T21:15:31.575Z`. Reconciled three ways.
- The dry run's own stamp `2026-09-11T21:13:59.578Z` matches **zero** rows.
- Convergence re-run and second dry run: all 957 `unchanged`.
- 89 candidates, 0 errors, 0 notified.

## Writing

Median grade 10.3, worst 11.3 (House Bill 1002), longest sentence 32 words. Two first-draft
bodies read above grade 12 (House Bills 1002 and 2464) and were rewritten before import.
