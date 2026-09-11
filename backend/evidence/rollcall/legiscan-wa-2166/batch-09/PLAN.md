# Washington batch-09

Nine measures, fifteen roll calls, 753 records across 107 candidates. Local database only.
Production holds no Washington records.

| measure | chapter | area | direction |
| --- | --- | --- | --- |
| Senate Bill 5217 — Healthy Starts Act | 379 L 25 | labor_rights | for |
| Senate Bill 5284 — packaging recycling paid by producers | 316 L 25 | environment_and_public_health | for |
| Senate Bill 5313 — banned lease terms | 206 L 25 | corporate_accountability | for |
| Senate Bill 5463 — self-insured employers' good faith duty | 338 L 25 | corporate_accountability | for |
| Senate Bill 5486 — movie captioning | 355 L 25 | civil_rights | for |
| Senate Bill 5525 — state layoff notice law | 277 L 25 | labor_rights | for |
| Senate Bill 5557 — emergency care for pregnant patients | 182 L 25 | womens_reproductive_rights | for |
| Senate Bill 5632 — shield law expansion | 248 L 25 | womens_reproductive_rights | for |
| Senate Bill 5651 — garnishment protection | 391 L 25 | corporate_accountability | for |

No measure carries an authored nay. Senate Bill 5217 is House-only (Senate votes 43-6 and
40-8). Senate Bill 5486 is Senate-only: its only divided vote is the Senate's 34-14 vote to
accept the House's changes. Senate Bill 5557 is Senate-only (House 84-12).

## Roll selection

Filter 4 takes each chamber's last kept floor vote. Six measures end in a Senate vote to
accept the House's changes. Every pick matches the report's "Votes on Final Passage".

## Duplicates

**None.** The tally sweep found nothing. The bill-number sweep found sponsorship records
and three older acts that reuse numbers: SSB 5651 of 2022 (a capital budget), SB 5313 of
2021 (gender-affirming coverage) and SB 5217 of an earlier session (ergonomics rules).

## ⚠ One roll failed on the first real run, and how it was closed

The first real import inserted 667 records and failed on one roll, Senate Bill 5217's
House vote, because a citation link timed out while the importer was checking it. Nothing
partial was written for that roll. The convergence re-run inserted its 86 records and left
the other 667 `unchanged`. A third run then reported all 753 `unchanged`
(`import-rerun-2-report.json`).

So the batch reconciles as a total, not under one stamp:
- Dry run planned 753.
- The database holds 753 live rows for this batch's fifteen rolls: 667 under
  `2026-09-11T05:48:08.115Z` and 86 under `2026-09-11T05:48:26.327Z`.
- The dry run's own stamp matches **zero** rows.
- Second dry run and third real run: all 753 `unchanged`. 0 notified.

## Writing

Two first-draft bodies read too hard (Senate Bill 5632 at grade 12.0 and 5284 at 11.3) and
were rewritten before import. Final median grade 8.8, worst 10.7, longest sentence 29
words. The pipeline's own plain-language lint reports **0 warnings** over all 30
descriptions.
