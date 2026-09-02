# Maryland 2026 batch-01 — judging

## Evidence and text version

Descriptions come from the official DLS Fiscal and Policy Note for each
enrolled bill. The official bill history supplies the chapter and confirms the
final roll. Maryland records a later third-reading roll after concurrence; the
selected rolls in `PLAN.md` are the final post-concurrence rolls where that
step occurred.

Each description cites its own chamber tally. The dates in the committed
LegiScan roll evidence match the official bill-history dates. No date override
is needed.

## Labels

- HB 1076 is **for** reproductive rights because it expands on-campus access
  to nonprescription contraception. A no vote directly rejects that access.
- HB 103 is **for** corporate accountability because it protects consumer
  remedies from contract waivers. A no vote directly rejects that protection.
- SB 656 is **for** environment and public health because it adds enforcement
  and prevention for harmful cosmetic ingredients. A no vote directly rejects
  that enforcement.
- SB 791 is **for** immigration because it limits civil immigration
  enforcement without a judicial warrant. A no vote directly rejects that
  humane-enforcement limit.
- HB 115 is **for** election integrity because it restores eligible voters'
  records through a defined, reviewable process. Its `nay` stance is explicitly
  `null`: a no vote can concern implementation costs or the automatic method,
  not the area's goal of accurate and trusted elections.

All descriptions use plain language. They state neither an intent nor an
effect beyond the DLS note. No AI provider was used.

## Plain-language lint

The lint ran before judging or importing on all 20 yea and nay descriptions.
It found **zero** sentences over 45 words and zero `, The` body-tail joins.

## Import and reconciliation

The dry run planned **763 inserts** across 10 rolls, with zero errors and zero
notifications. The real local import created the same **763 records** across
**161 candidates**, with zero errors and zero notifications.

Before the required real rerun, `import-report.json` was validated as the
763-insert ledger and copied to `import-pre-rerun-report.json`; their SHA-256
hashes match. The rerun wrote `import-rerun-report.json` with **763 unchanged**
rows. The original insert ledger remains `import-report.json`.

Three-way reconciliation for real import stamp `2026-08-30T07:12:52.565Z`:

- ledger: 763 inserts across 10 rolls;
- local records: 763 live records across 161 candidates;
- local area tags: 727. The difference is expected: HB 115 has 120 yea-side
  tags and no tags for its 36 nay-side voters because its explicit `nay` is
  null.

The dry-run stamp has zero stored records. The two related-record flags were
kept: David Moon's record is a different bill's co-sponsorship and Cheryl
Kagan's is a committee vote on SB 949, not either selected roll-call claim.

Production is untouched. All changes are in local `voteapp`.

## Crosswalk link to the roster campaign (2026-08-30)

This crosswalk was built before the roster campaign (PR #978) added 2026
candidate rows for two sitting HD-031 Delegates, so people_ids 20530 (Brian
Chisholm) and 26325 (LaToya Nkongolo, ballot surname Caldwell-Nkongolo)
carried stale "no candidate row" nulls. Both voted on all five House rolls in
this batch. The two entries are now mapped, and the idempotent re-import added
exactly their records: **10 inserts / 763 unchanged, 0 errors**; convergence
**773 unchanged**. Batch totals are now **773 records / 163 candidates / 735
tags** (their two HB 115 nay records carry no tag — that label's nay side is
null by design). Ledger: `import-crosswalk-link-report.json`;
`import-report.json` and `import-rerun-report.json` remain the original insert
and convergence ledgers.

Standing rule this documents: after ANY Maryland roster addition, both session
crosswalks (2164 and 2240) are extendable — people_ids are session-stable, and
re-import picks new members up idempotently. Check both, not just the session
being worked.
