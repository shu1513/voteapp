# Roll-call descriptions: live rows versus committed judgments, 2026-09-04

## Why this was run

Finishing Delaware turned up 73 records whose live description no longer matched the
`judgments.json` committed for them, and a re-import would not repair it. That raised an
obvious question for the rest of the campaign, and the honest answer at the time was that
nobody knew. This audit answers it.

## Result, in one line

**13,186 records across 11 states carry a description that differs from the committed
judgment governing them — and in every case the row is NEWER than the file, not older.**
No record anywhere is serving text that a later judgment superseded. The committed
evidence is what has fallen behind, not the database.

## What was compared

For every roll-call record still live, the row's description was compared against **both**
the `yea_description` and the `nay_description` of the committed judgment for its roll. A
record counts as drifting only if it matches neither.

Matching on side alone does not work: some descriptions open "Voted to accept the
Senate's changes…" rather than "Voted for…", so a naive `like 'Voted for %'` test
misfiles them as nay votes and invents tens of thousands of false differences. The first
pass of this audit made exactly that mistake and reported 54,615. Compare against both
sides and the figure settles at 13,186.

Where a roll appears in more than one committed file — 40 of them do, because a
`rejudge-*` or correction directory supersedes the batch that first judged it — the later
directory wins.

| | records |
| --- | --- |
| matched a committed description exactly | 97,489 |
| matched neither the yea nor the nay text | **13,186** |
| roll has no committed judgment (federal, and rolls judged before this format) | 15,830 |

## Which way the drift runs

Each drifting group was dated two ways: the last commit date of the `judgments.json` that
governs it, and the last `updated_at` of the rows themselves.

| | records | texts |
| --- | --- | --- |
| row updated **after** the file was last committed | 11,686 | 230 |
| same day, cannot be ordered | 1,500 | 31 |
| **row older than the file (would mean stale text on a live record)** | **0** | **0** |

**Zero.** Not one drifting record predates its own evidence. Delaware's 73 rows were the
only instance of that shape in the whole campaign, and they have been repaired.

## What is doing the rewriting

`plainLanguageBackfill.ts` — a separate, AI-driven pipeline that rewrites
`candidate_records.description` in place across the whole corpus, in cursor order, and
recomputes the record identity key as it goes. It is not part of the roll-call pipeline
and does not know that these descriptions were already written to a plain-language
standard by hand, lint-checked, and graded.

In Pennsylvania the match is exact: **5,963 PA records drift, and exactly 5,963 PA
records carry a `plain_language_rewrite` identity transition.** Campaign-wide the
transition log accounts for about 10,400 of the 13,186; the remainder carry either a
`rollcall_normalization` transition or none, which is consistent with edits that did not
move the identity key.

| state | drifting records | distinct texts |
| --- | --- | --- |
| PA | 5,963 | 86 |
| MD | 1,783 | 37 |
| GA | 1,732 | 36 |
| ME | 1,180 | 26 |
| TX | 1,134 | 22 |
| IL | 528 | 18 |
| KY | 520 | 14 |
| AL | 142 | 6 |
| IN | 130 | 10 |
| FL | 60 | 4 |
| TN | 14 | 2 |

## The consequence that actually matters

**A re-import reverts the sweep.** Measured, not assumed: a dry re-import of PA batch-02
against its own committed evidence plans **1,411 rewrites**, which would put the
pre-sweep text back on those records.

So the two pipelines overwrite each other, and whichever ran last wins. Any future
re-import of one of these eleven states — to pick up new roster members, or to correct a
description — silently reverts the sweep's work in that state as a side effect.

Note the asymmetry with Delaware, which looks contradictory and is not. The sweep moves
the identity key, so the importer no longer recognises the row and rewrites it.
Delaware's rows were edited by direct SQL without touching the key, so the importer
matched them, called them `unchanged`, and left the stale text in place. **The importer
repairs drift only when the identity key moved.**

## What is not at risk

- **No production record is affected.** Every one of these eleven states holds zero
  roll-call records in production; only the federal and Ohio records were ever promoted,
  on 2026-08-24. This divergence is entirely local.
- **No voter is reading superseded text**, because no drifting row is older than its
  evidence.

## The decision this leaves open

The roll-call evidence trail and the database disagree in eleven states, and the campaign
has to pick which one is authoritative. Both options are one-time work; neither is
urgent, and neither should be done by accident.

1. **Exclude `origin = 'rollcall_import'` from the plain-language backfill**, then
   re-import the eleven states to restore the reviewed text. The judgments files become
   true again and stay true. This treats the hand-written, graded, reviewed descriptions
   as the record of decision, which is what the roll-call campaign's own rules say they
   are.
2. **Fold the sweep's text into the committed judgments.** The files become true again,
   but the record of decision would then contain roughly 261 descriptions written by an
   AI pass that no human judged — against the rule that in this pipeline the session and
   the user are judge and reviewer.

Option 1 is the recommendation. Option 2 is cheaper today and worse later.

## Reproducing this

`backend/evidence/rollcall/audits/description_drift_audit.py`, run against local
`voteapp`. It re-derives every number above from the committed evidence and the database,
and prints examples per class.
