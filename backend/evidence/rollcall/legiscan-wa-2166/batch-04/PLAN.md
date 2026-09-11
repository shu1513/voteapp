# Washington batch-04

Six measures, eleven roll calls, 616 records across 108 candidates. Local database only.
Production holds no Washington records.

| measure | chapter | area | direction |
| --- | --- | --- | --- |
| House Bill 1183 — building code and setback reform | 139 L 25 | housing_affordability | for |
| House Bill 1795 — restraint and isolation of students | 169 L 26 | public_education_quality | for |
| House Bill 1875 — sick leave for immigration hearings | 170 L 25 | immigration | for |
| House Bill 1971 — year-long hormone therapy refills | 171 L 25 | healthcare_affordability | for |
| House Bill 2266 — shelter and supportive housing | 232 L 26 | housing_affordability | for |
| House Bill 2320 — firearm manufacturing | 203 L 26 | gun_control | for, nay against |

**`healthcare_affordability` gets its first Washington coverage.** House Bill 2320 is the
batch's only authored nay, on the same test that carried the firearms act in batch-01:
single subject, the whole operative content is the research area's own mechanism, and the
mainstream objection is to that mechanism rather than to cost or administration.

House Bill 1971 is **House-only** — the Senate passed it 40-9, which is not divided.

## Roll selection

Filter 4 takes each chamber's last kept floor vote, ordered by date, and every pick was
confirmed against the Final Bill Report's own "Votes on Final Passage" list. Three
measures (1795, 2266, 2320) end in a House concurrence in Senate amendments; the rest
took one vote per chamber with no amendment. No judgment needed
`acknowledge_later_rolls`.

## Duplicates

Ten hand-written records were retired, all on House Bill 2320. It has two House votes —
57-39 on first passage and 58-38 on concurrence — but unlike House Bill 1710 in batch-03
the two tallies differ, so bill number plus tally identifies the concurrence without
ambiguity. Each of the ten was still read, and each names the concurrence or the Senate
vote in the chamber its member actually sits in.

## ⚠ Number reuse again, three more times

The wider sweep over every live hand-written record naming these six bill numbers
returned nineteen rows, of which the ten above were the only retirements. Six are
prime-sponsorship records, which are not votes. **Three cite an older Washington act that
happens to share a number**: 2SHB 2320 of 2024 regulated high-THC cannabis, ESHB 1795 of
2022 was the Silenced No More Act on nondisclosure clauses, and EHB 2266 required
sanitary facilities for construction workers. Batch-03 found the same trap on ESSB 6002.
**In Washington a bill number identifies a measure only within its own biennium.**

## Reconciliation

- Dry run planned 616, the real run inserted 616, and the database holds 616 under the
  run stamp `2026-09-10T06:28:51.092Z`. Reconciled three ways.
- The dry run's own stamp matches **zero** rows.
- Convergence re-run: all 616 `unchanged`.
- 108 candidates, 0 errors, 0 notified.
- Washington now holds **2,708 records over 50 approved rolls**.

## Writing

Reading level was measured BEFORE the import. A first draft measured **median grade 10.2,
worst 11.1**, so three of the six bodies were rewritten to **median 9.2, worst 9.3**,
longest sentence 33 words. The pipeline's own plain-language lint reports **0 warnings**
over all 22 descriptions.
