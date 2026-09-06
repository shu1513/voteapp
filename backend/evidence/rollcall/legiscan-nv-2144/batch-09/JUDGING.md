# Nevada batch-09 — judging and import

**Result on local `voteapp`, 2026-09-06: 11 roll calls approved, 230 records inserted across
41 candidates, 0 errors, 0 notified. Production untouched.**

Nevada now holds **2,571 live roll-call records** across **131 approved roll calls**.

## Sources

Each measure was judged from the text the chamber actually voted, downloaded through the
LegiScan `getBillText` API with both the byte length and the MD5 checksum verified against
the dataset. All documents passed both checks. Version comparisons used the corrected
`ne_diff.py`, which strips line numbers during normalization and filters nothing afterwards.

## An error caught by the pre-import checks

SB 414's Assembly roll was written with a vote date of 2025-05-23 recorded as 2025-05-22.
The row check against `legislative_votes` caught it before the judge ran. The judge would
also have refused it, but the check is cheaper and it is the reason the check exists.

## Superseded-stage gate

The gate did not fire on any roll in this batch.

## Labels

Six measures, six research-area labels, all with `"nay": null`. `anti_corruption` carries
SB 414, the first use of that area in the Nevada campaign; financial disclosure by officials
is what the area is for.

## Wording checks, all run before the import

- The real `candidateRecordPlainLanguageLint` over all 22 descriptions: **0 warnings**.
  It flagged eight on the first pass — AB 306 at 47 words and SB 155 at 52 — and both
  opening sentences were split before anything was imported. Splitting AB 306 pushed it to
  five sentences, so a clause was folded back in to return it to four.
- Every description is 2 to 4 sentences.
- British spellings scanned for: none found.
- Every description cites its own roll call's tally, checked against the stored row.
- Measure, date and chamber checked against the stored row for all 11 judgments.

## Reconciliation — three ways

| check | result |
| --- | --- |
| import report | 230 inserts, 2,341 unchanged, 0 errors |
| run-stamp predicate `rollcall:NV:%:2026-09-06T23:08:05.099Z` | 230 records, 41 candidates |
| table delta | 2,571 − 2,341 = 230 |

Per-roll fan-out: 27 to 30 candidates on Assembly rolls, 10 or 11 on Senate rolls, none at
zero. The ledger was copied out immediately after the run and its `startedAt` asserted equal
to the run stamp.

## Duplicate sweep

Swept with `origin_run_id NOT LIKE 'rollcall:%'`. Within a candidate there are **0 duplicate
record identity keys and 0 duplicate source URLs** across all Nevada roll-call records.

Three hand-written rows matched a measure in this batch and all three were kept: a person
testifying before a Senate committee, a member who sponsored the out-of-state police hiring
bill, and a member who introduced the gun violence special counsel bill. Testifying and
sponsoring are different acts from voting.

## Why five of Nevada's largest election bills were dropped

`PLAN.md` sets out AB 499, AB 534, SB 74, SB 422 and AB 79 in full. The short version: they
are negotiated packages that expand and restrict voting in the same text, and in AB 499's
case the two chambers voted bills that differ on the central question of whether Nevada
requires photo identification to vote at all. Filter 5 exists for exactly this, and applying
it here costs the campaign the session's most prominent election votes. That cost is
recorded deliberately, because a reader of this evidence should be able to see what the rule
excluded and disagree with it if they want to.
