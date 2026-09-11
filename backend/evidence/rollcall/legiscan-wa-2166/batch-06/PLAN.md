# Washington batch-06

Eight measures, fifteen roll calls, 753 records across 107 candidates. Local database
only. Production holds no Washington records.

| measure | chapter | area | direction |
| --- | --- | --- | --- |
| House Bill 1177 — child welfare housing help | 163 L 25 | social_programs_and_welfare | for |
| House Bill 1215 — pregnancy line in the living will form | 56 L 25 | womens_reproductive_rights | for |
| House Bill 1308 — personnel files | 273 L 25 | labor_rights | for |
| House Bill 1332 — ride-hailing driver receipts | 229 L 25 | labor_rights | for |
| House Bill 1524 — isolated worker safety | 47 L 25 | labor_rights | for |
| House Bill 1531 — public health measures | 105 L 25 | environment_and_public_health | for |
| House Bill 1644 — child labor enforcement | 173 L 25 | labor_rights | for |
| House Bill 1696 — Covenant Homeownership Program | 143 L 25 | housing_affordability | for |

No measure carries an authored nay. House Bill 1524 is **Senate-only**: its House vote was
86-10, not divided.

This is the first Washington batch to use `labor_rights`. That area was added after
batches 01 to 05 were judged (migration 277). Eleven labor measures dropped in the survey
for lack of a labor area are reopened in a later batch.

## Roll selection

Filter 4 takes each chamber's last kept floor vote, ordered by date, and every pick was
confirmed against the Final Bill Report's own "Votes on Final Passage" list. House Bills
1308 and 1332 end in a House vote to accept the Senate's changes. The rest took one vote
per chamber.

## Duplicates

**None.** The tally sweep found no hand-written record quoting both a bill number and a
tally from this batch. A bill-number sweep found 18 records. Nine are sponsorship records
for these same acts, which make a different claim and stay. The other nine cite older
Washington acts that reuse the number: HB 1177 of 2019 (dental laboratories) and 2023
(missing Indigenous people), HB 1215 (towing fees), HB 1308 of 2023 (graduation
pathways), HB 1524 of 2011 and 2018, HB 1531 of 2019 (medical debt), HB 1644 of 2013 and
HB 1696 of 2023 (stalking). Number reuse is now fifteen cases across four batches.

## Reconciliation

- Dry run planned 753, the real run inserted 753, and the database holds 753 under the
  run stamp `2026-09-11T05:30:10.840Z`. Reconciled three ways.
- The dry run's own stamp matches **zero** rows.
- Convergence re-run and second dry run: all 753 `unchanged`.
- 107 candidates, 0 errors, 0 notified.
- Washington now holds **4,008 records over 76 approved rolls**.

⚠ The importer does not overwrite an existing `import-report.json`. The two re-run
ledgers were therefore written from each run's own printed report, not copied from the
file. Their `startedAt` stamps differ from the first run's, which is how to tell them
apart.

## Writing

Median grade 8.9, worst 10.4 (House Bill 1524), longest sentence 35 words. The pipeline's
own plain-language lint reports **0 warnings** over all 30 descriptions.
