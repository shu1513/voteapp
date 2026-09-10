# Washington batch-02

Nine measures, seventeen roll calls, 925 records across 108 candidates. Local database
only. Production holds no Washington records.

| measure | chapter | area | direction |
| --- | --- | --- | --- |
| House Bill 1170 — labelling AI-generated content | 167 L 26 | ai_regulation | for |
| House Bill 1462 — hydrofluorocarbon limits | 313 L 25 | environment_and_public_health | for |
| House Bill 1491 — transit-oriented housing | 267 L 25 | housing_affordability | for |
| House Bill 1747 — Fair Chance Act | 71 L 25 | civil_rights | for |
| House Bill 2105 — immigrant worker notice | 240 L 26 | immigration | for |
| Senate Bill 5184 — parking minimums | 204 L 25 | housing_affordability | for |
| Senate Bill 5480 — medical debt and credit reports | 145 L 25 | corporate_accountability | for |
| Senate Bill 5494 — lead-based paint renovation | 180 L 25 | environment_and_public_health | for |
| Senate Bill 5917 — abortion medication supply | 52 L 26 | womens_reproductive_rights | for, nay against |

**`ai_regulation` gets its first Washington coverage**, and it is the newest area in the
taxonomy. **Senate Bill 5917 is the batch's only authored nay stance**, on the same test
used for the firearms act in batch-01: single subject, the act's whole operative content
is the area's own mechanism, and the mainstream objection is to that mechanism.

House Bill 1170 is **House-only** — the Senate passed it 46-3, which is not divided, so
only the House roll is in the pool. That is the filter working, not a gap.

## Roll selection

Filter 4 takes each chamber's last kept floor vote, ordered by date. Every pick was then
confirmed against the Final Bill Report's own "Votes on Final Passage" list:

- **Concurrences** (1170, 1462, 1491, 2105): the House passed, the Senate amended, the
  House concurred. The concurrence is the House's vote on the enacted text.
- **Senate Bill 5184** is the mirror image: the House amended it and the Senate concurred,
  which its description records as "Final Passage as Amended by the House".
- **1747, 5480, 5494, 5917** took one vote in each chamber with no amendment.

No judgment needed `acknowledge_later_rolls`.

## ⚠ A title trap, dropped: House Bill 2521

Listed as "Concerning firearms background check", which reads like a substantive firearms
measure. The act does one thing: it removes the $18 cap on the background check fee so the
State Patrol can set a fee covering its costs. It changes no eligibility and no check, so
no research area carries a direction on it. **Dropped under filter 5.** This is the
title-is-not-the-text rule, and reading the report is what caught it.

## Duplicates

The precise sweep found **10** hand-written records describing this batch's own rolls, all
retired before the import, on Senate Bill 5917 (8), House Bill 2105 and House Bill 1170.

The importer raised 113 `related` flags, and the gap between 113 and 10 was checked rather
than assumed. A wider sweep over every live hand-written record mentioning these nine bill
numbers and a vote returned five, and **all five are correctly left alone**: two are about
different bills entirely, one is a co-sponsorship, and two describe House Bill 1491's
EARLIER 58-39 House passage rather than the 57-39 concurrence this batch imports. That is
the same pattern batch-01 found on House Bill 1217.

## Reconciliation

- Dry run planned 925, the real run inserted 925, and the database holds 925 under the run
  stamp `2026-09-10T04:39:01.480Z`. Reconciled three ways.
- The dry run's own stamp matches **zero** rows.
- Convergence re-run: all 925 `unchanged`.
- 108 candidates, every member the crosswalk maps.
- Washington now holds **1,455 records over 27 approved rolls**.
- Tag arithmetic matches the label design: the only areas carrying `against` tags are
  `gun_control` (batch-01) and `womens_reproductive_rights` (this batch), which are the
  only two measures with an authored nay.

## Writing

Reading level was measured BEFORE the import, not after. A first draft measured **median
grade 11.0, worst 12.3**, so five of the nine bodies were rewritten to **median 8.8, worst
10.6**, longest sentence 42 words.

The builder's own checks caught two real defects in that first draft that no eye caught: a
48-word sentence in House Bill 1491, and a British "programme" I had written in Senate
Bill 5494. The checker is proved against a known-bad string before it is trusted, because
a checker that never fires is indistinguishable from one that passes.

Senate Bill 5480 remains the hardest to read at grade 10.6, driven by unavoidable terms
(unenforceable, collection agency, commercial). Grade 9 to 10 is the honest floor for
statutory text, as every state in this campaign has found.
