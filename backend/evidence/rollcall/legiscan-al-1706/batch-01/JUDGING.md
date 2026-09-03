# Alabama 2020 Regular Session batch-01 — judging notes

## Sources

Every measure was judged from the version its chamber actually voted, fetched through the LegiScan bulk
API (`getBillText`, and `getAmendment` for conference substitutes) and verified against the byte length
and MD5 hash the dataset records for that document. The state website was not used: it had been timing
out for hours at a stretch during the previous Alabama batch, and one direct download in that batch
returned HTTP 200 with a truncated, unreadable file.

Alabama prints struck and inserted text together and the conversion flattens both into one run of words.
The convention is struck text first, inserted text second, so `no less than 12 10 months` means the old
law said 12 and the new law says 10. Every changed number in these descriptions was read that way.

## Roll-attribution and date audit

Each imported roll's printed roll call number was checked against its own bill's history, and each
roll's date was checked against the bill history line recording the same action. Results for this
session are in `../survey/divided-worklist.tsv`. The term-level findings, including a case where one
session's dataset carried another session's roll calls, are in
`../../legiscan-al-1756/CODE-FINDINGS.md`.

## Label reasoning

Every stance label states `nay` explicitly, and every one is `null`: in each measure the realistic
reason for a no vote runs on a different axis than the scored area.

- **HB 147 — personal_income_tax_reduction, yes = for.** A municipal occupational tax is a tax on what a
  person earns at work. The Act bars any city from imposing a new one without a law passed for that city
  by name, while leaving every existing tax and all ordinary business licence taxes untouched. The
  objection was local control, which runs on a different axis, so nay is null.

## Duplicates

A precise sweep found the hand-written records that describe the same votes, and they were retired
before the import. The sweep is restricted to Alabama candidates, an exact vote date, a description
naming the same bill, and a description worded as a vote. It excludes only records whose origin run id
begins `rollcall:`, because hand-written records carry a `manual:candidate-records:...` run id and a
null-check misses them. Sponsorship records naming the same bill were left alone.

## Import and reconciliation

- Real run (stamp `2026-09-03T16:54:48.687Z`): **74 inserts, 0 errors, 0 notified**, across 2 rolls.
- Reconciled three ways: the report totals; the run-stamp predicate (74 rows, 74 distinct
  candidates); and the Alabama roll-call total, which moved from 4,890 to 7,527 across the six batches
  imported together.
- Convergence: a follow-up dry run reports all 74 `unchanged`.

## Writing checks run before import

`candidateRecordPlainLanguageLint`: 0 warnings. Every description is 2 to 4 sentences with no sentence
over 45 words, and a British-spelling scan is clean — it caught real slips on a first pass, including
`legalised`, `licence`, `behaviour`, `labour` and `programme`, all corrected. Reading grade was measured
per session; medians run 8.6 to 11.2.
