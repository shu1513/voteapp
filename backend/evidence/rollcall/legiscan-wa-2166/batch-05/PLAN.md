# Washington batch-05

Six measures, eleven roll calls, 547 records across 108 candidates. Local database only.
Production holds no Washington records.

| measure | chapter | area | direction |
| --- | --- | --- | --- |
| House Bill 1081 — solicited home sales | 77 L 25 | corporate_accountability | for |
| House Bill 1154 — landfill and solid waste enforcement | 311 L 25 | environment_and_public_health | for |
| House Bill 1687 — social housing developers | 1 L 26 | housing_affordability | for |
| House Bill 2294 — grocery and pharmacy covenants | 24 L 26 | corporate_accountability | for |
| House Bill 2367 — coal and cap-and-invest | 37 L 26 | environment_and_public_health | for |
| House Bill 2384 — life care contracts | 140 L 26 | corporate_accountability | for |

No measure carries an authored nay. House Bill 2384 is **Senate-only**: its House votes
were 78-14 and 78-17, neither divided.

## Roll selection

Filter 4 takes each chamber's last kept floor vote, ordered by date, and every pick was
confirmed against the Final Bill Report's own "Votes on Final Passage" list. House Bill
1154 ends in a House concurrence; House Bill 2384's Senate roll is on the Senate's own
amended text, which the House then accepted; the rest took one vote per chamber.

## Duplicates

**One** hand-written record was retired, on House Bill 1687, which had only one House
vote. The office-scoped sweep and the tally sweep found it independently and found
nothing else. Of the other hits, four are prime-sponsorship records, and one describes
House Bill 1154's first House passage (58-39) rather than the 57-39 concurrence imported
here. Two more cite older Washington acts that reuse the number — ESHB 2384 of 2024 on
traffic safety cameras and SHB 2367 on a child care task force — the fifth and sixth
cases of number reuse found in three batches.

## ⚠ A failed first run, and why nothing was harmed

The first import attempt put its arguments in a shell variable. zsh does not split an
unquoted variable into words, so all four passes received one mangled argument and
exited without writing, and their output had been sent to `/dev/null`. The missing
ledgers gave it away. Before re-running, the database was checked for any row carrying
one of this batch's roll ids, and there were **none**. Every pass was then re-run with its
arguments written out.

## Reconciliation

- Dry run planned 547, the real run inserted 547, and the database holds 547 under the
  run stamp `2026-09-10T06:36:01.931Z`. Reconciled three ways.
- The dry run's own stamp matches **zero** rows.
- Convergence re-run and second dry run: all 547 `unchanged`.
- 108 candidates, 0 errors, 0 notified.
- Washington now holds **3,255 records over 61 approved rolls**.

## Writing

The first draft already measured **median grade 8.5, worst 9.6**, longest sentence 31
words, so no body needed rewriting. The pipeline's own plain-language lint reports **0
warnings** over all 22 descriptions.
