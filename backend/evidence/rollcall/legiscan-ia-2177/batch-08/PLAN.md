# Iowa batch-08 — plan (not-enacted scope, second batch)

## Selected
HF 792, HF 946, SF 180, SF 2203, SF 2274, SF 2442, SF 2444, SF 394, HF 2763, HF 2244, HF 401.
Each is one chamber's vote on a bill the other chamber never took up. Pool, screening, and the
60 written drops are described in `../batch-07/PLAN.md` and `../survey/not-enacted-worklist.json`.

## Checks particular to this batch
- HF 946 and SF 2274 change existing Code sections, so the current text of chapter 27A and
  section 724.29 was read from the Iowa Code before describing them, not recalled.
- HF 2763 has two strands (equipment repair and farm data ownership); both favor farmers over
  manufacturers and data companies, so it carries one label per strand.
- HF 401 carries a curriculum strand with no direction and a content-standards strand; only the
  second is labeled.

## Audits and import
Audit: none failed. Lint: 0 warnings over 22 descriptions; longest sentence 44 words.
Judge 11 updated. Import dry run 11 rolls / 491 inserts, real run the same, no errors.
Stamp 2026-09-11T05:35:17.874Z: 491 records, 102 candidates, 384 tags.
Review fix: SF 2442 and SF 2444 re-judged (2 updated) and the batch re-imported in place; the
re-run, stamp 2026-09-11T06:18:50.682Z, reports 33 rewrites (17 + 16) and 458 unchanged, ledger
`import-review-fix-rerun-report.json` (dry run alongside). Those 33 records carry the re-run stamp.
Same-day sweep: 3 hand-written records share a date; they concern HF 190 and HF 644, so none is
a duplicate.
