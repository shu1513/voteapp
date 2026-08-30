# TX SB 2024, senate roll 1556014 — withdrawn as corrupt source data

## What is wrong

`legislative_votes` stores this roll call as a 20-11 "Read 3rd time" with an
11-name nay list. The Texas Senate Journal for 2025-04-23
(`journals.senate.texas.gov/sjrnl/89r/html/89RSJ04-23-F.HTM`) records CSSB 2024
passing third reading **Yeas 30, Nays 1 — "Nays: Cook"**, and Texas Legislature
Online confirms the Senate third-reading record vote that day at Journal page
1421. The stored tally and nay list match CSSB 240's 20-11 vote of the same
day, not this bill. Because the member list decides which candidates receive
records, both the tally and the fanned-out membership are unreliable.

No truthful sentence can carry the "20-11" that the judge script's tally gate
requires, so this roll call cannot be re-authored — it needs a re-fetch from
the LegiScan dataset (or the Journal itself) before it can be re-judged.

## What was done (2026-08-29/30, local DB)

1. `retirements.json` (this directory) — the 13 fanned-out records retired via
   `manual:records:retire --apply`.
2. `judgments.json` — the row moved to `review_status = pending` via
   `rollcall:judge` (allowed only once no live records remain), so it cannot
   fan out again.

Note: `legiscan-tx-2160/batch-03/import-report.json` predates this withdrawal
and still shows the roll approved with records written; this directory records
the later, final state.

## Post-state verification (local DB, 2026-08-30)

```
SELECT review_status FROM legislative_votes
 WHERE jurisdiction='TX' AND chamber='senate' AND session='2160' AND roll_number=1556014;
-- pending

SELECT count(*) FILTER (WHERE retired_at IS NOT NULL) AS retired,
       count(*) FILTER (WHERE retired_at IS NULL)    AS live
FROM candidate_records
WHERE origin_run_id LIKE 'rollcall:TX:senate:2160:1556014:%';
-- retired = 13, live = 0
```

## Restore path

`rollcall:legiscan:fetch` the roll call afresh, verify the tally against the
Journal (30-1), re-judge with corrected sentences, then `rollcall:legiscan:import`.
