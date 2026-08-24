# 119-1 survey evidence

The full-session survey behind the expansion batches (plan §evidence
contract): one `rollcall:fetch` run per chamber covering every roll call
in the session — House 1-370 requested (1-362 exist, 363-370 missing;
roll 2 is the Speaker election, which the parser declines because the XML
has no `<totals-by-vote>`), Senate 1-660 requested (1-659 exist).

- `rollcall-fetch-house-119-1-20260823T214946Z-report.json` — the House
  run's full per-roll ledger: outcome, vote date, measure, question,
  result, tallies, and the classifier's `isFloorVote` /
  `questionClass` / reason for each roll.
- `rollcall-fetch-senate-119-1-20260823T214955Z-report.json` — the same
  for the Senate run.
- `dispositions.tsv` — one row per classifier-kept roll call (225 =
  179 House + 46 Senate) with its disposition: `judged` (naming the
  pilot or batch directory holding its judgment), `pending` (with the
  reason it was deliberately left unjudged), or `retracted`.

The survey's raw XMLs (1,021 files) are not committed; each is
re-fetchable by URL and pinned by the `source_sha256` stored on its
`legislative_votes` row at fetch time. The 106 judged rolls' XMLs are
committed in their pilot/batch directories, where the importer verifies
them against the stored hash before writing.

The fetch reports' `evidenceDir` fields point at the scratch directory
the survey ran in; the reports were copied here afterwards.
