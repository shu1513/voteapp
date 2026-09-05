# British-spelling sweep of live candidate_records — 2026-09-04

Candidate-record descriptions must use American spelling. This sweep found every live
(`retired_at IS NULL`) description with a British form and fixed it locally.

## Roll-call rows (fixed through the campaign's own tools)

The words came from the committed judgments files, so the fix is in those files and the
rows were rewritten by re-judging and re-importing each batch (dry run, real run, second
run reporting everything unchanged):

| Batch | Words | Rolls | Rows rewritten |
|---|---|---|---|
| legiscan-in-2143/batch-08 | licence | 1555035, 1555540 | 92 |
| legiscan-in-2143/batch-09 | programme | 1556821, 1556820 | 90 |
| legiscan-in-2234/batch-02 (`--state IN-2234`) | licence(s), labelled, neighbour | 1631741, 1637695, 1632010, 1642595, 1645175, 1647209 | 211 |
| legiscan-ca-2172/batch-04 | counselling | 1572403, 1600943 | 70 |
| legiscan-ca-2172/batch-22 | signalled | 1601736 | 11 |

The CA batch-22 roll was imported from a one-file evidence dir copied out of
`~/legiscan-data/ca-2172-evidence/` (that batch keeps no roll files in the repo), so its
report is not committed.

## Manual rows (fixed through the plain-language backfill)

`rewrites.json` is the operator rewrites file: 69 rows, spelling-only changes
(licence→license, programme→program, authorised→authorized, defence→defense, totalled→totaled,
travelled→traveled, organisation→organization, favour→favor, neighbourhood→neighborhood,
recognised→recognized, practise→practice, honour→honor, counselling→counseling, and so on).
Applied locally with:

    npm run content:backfill-plain-language -- --rewrites-file evidence/records-repair/british-spelling-2026-09-04/rewrites.json

Dry run and real run both reported 69 processed, 69 applied, 0 flagged, 0 stale. The local
run also passed `--allow-sourced-facts`; that was unnecessary. The flag only relaxes the
upper length bound and the introduced-number check, and spelling-only rewrites trip
neither (all 69 pass the strict mechanical checks without it), so leave it off.

`originalText` pins the pre-sweep text, so re-running the file applies only to rows that
still carry the old text and skips rows that moved.

## Getting the fix to production

The backfill script refuses any non-local database, so the file is never run against prod.
The rewrite gave each row a new `record_identity_key` and logged the old→new pair in
`candidate_record_identity_transitions`; `research:promote` follows that ledger and updates
the prod row in place instead of inserting a duplicate. The roll-call rows reach prod the
same way. Nothing here is a prod step; the promotion is.

## Deliberately left alone

- Proper nouns: "Sauk Centre Township", "Immigration Defence Coalition" (name could not be
  confirmed), "Harbour" in place names, theater names ("Stone's Throw Dinner Theatre").
- "theatre" / "theatres" in arts biographies (common US usage in that field).
- "cancelled" / "cancelling" (about 1,070 rows): an accepted American variant, not a defect.
- Prose-only files (PLAN.md, JUDGING.md, survey worklists) still carry British spellings.
  They are internal working notes, not voter-facing.
