# Nevada batch-08 — judging and import

**Result on local `voteapp`, 2026-09-06: 13 roll calls approved, 255 records inserted
across 41 candidates, 0 errors, 0 notified. Production untouched.**

Nevada now holds **2,341 live roll-call records**.

## Sources

Each measure was judged from the text the chamber actually voted, downloaded through the
LegiScan `getBillText` API with both the byte length and the MD5 checksum verified against
the dataset. All documents passed both checks.

## ⚠ A second flaw in the version-diff method, found and fixed here

Batch-06 fixed one flaw: never filter diff fragments by length, because a one-word change is
usually the whole difference. **This batch found a second: never drop a fragment because it
is a bare number.**

SB 350's two versions differ only in numbers. The Assembly voted a window of 120 to 180
days; the Senate voted 180 to 270. The tool's "bare numbers are page furniture" rule threw
every one of those away and reported the versions as nearly identical apart from a must/may
in the summary. The numbers in a bill are frequently the entire policy — day counts, dollar
caps, ages, thresholds.

`ne_diff.py` was rewritten. Line numbers are now stripped during **normalization**, because
a Nevada printed line begins with its own line number, and after that nothing is filtered at
all. Fragments containing a modal verb or any digit are flagged. Re-run on SB 350, the
corrected tool surfaces all sixteen numeric changes.

A related lesson from the same bill: the must/may difference the first tool did surface was
in the **Digest**, describing existing law, not in the operative sections, which are word for
word the same on that point. A difference only counts if it is in the numbered sections.

## Superseded-stage gate

The gate did not fire on any roll in this batch.

## Labels

Eight measures, eight research-area labels, all with `"nay": null`.

`reduce_wealth_gap` carries the three collective-bargaining measures, following the campaign's
use of that area for worker bargaining power. `civil_rights` carries AB 434, because the
protection is against being compelled to hear an employer's religious or political views,
and carries AB 488 and AB 91, which are about clearing convictions and reaching a parole
hearing rather than about crime control. AB 209 sits in
`public_safety_and_crime_control` because its purpose is to get emergency calls made.

## Wording checks, all run before the import

- The real `candidateRecordPlainLanguageLint` over all 26 descriptions: **0 warnings**.
  SB 172's opening sentence ran to 52 words on the first pass; it was split, which pushed
  the description to five sentences, so a clause was then cut to bring it back inside the
  2-to-4 range. Both fixes were made before anything was imported.
- Every description is 2 to 4 sentences.
- British spellings scanned for. Two were found and fixed before importing: "counsellor" in
  AB 155 and "offences" in AB 209.
- Every description cites its own roll call's tally, checked against the stored row.
- Measure, date and chamber checked against the stored row for all 13 judgments.

## Reconciliation — three ways

| check | result |
| --- | --- |
| import report | 255 inserts, 2,086 unchanged, 0 errors |
| run-stamp predicate `rollcall:NV:%:2026-09-06T22:59:46.564Z` | 255 records, 41 candidates |
| table delta | 2,341 − 2,086 = 255 |

Per-roll fan-out: 30 candidates on every Assembly roll, 10 or 11 on Senate rolls, none at
zero.

The ledger was copied out of the run directory immediately after the run and its
`startedAt` asserted equal to the run stamp, following the rule set in batch-07.

## Duplicate sweep

Swept with `origin_run_id NOT LIKE 'rollcall:%'`. Within a candidate there are **0 duplicate
record identity keys and 0 duplicate source URLs** across all Nevada roll-call records.

Two hand-written rows matched a measure in this batch and both were kept: a member who
introduced the Agricultural Workers' Bill of Rights, and a member who introduced a consumer
data privacy bill. Introducing a bill is a different act from voting on it.

## AB 488: a version difference the descriptions deliberately do not turn on

The two chambers voted texts that differ in six places, and two of them pull against each
other. The Assembly's version gives **mandatory** relief to a petitioner who files official
documentation: the court "shall apply the presumption ..., vacate the judgment and seal all
documents." The Senate's version extends a presumption to **every** petitioner regardless of
documentation, but makes it rebuttable and lets the court weigh "any relevant factors". One
version is broader in reach; the other is stronger in effect. Their no-hearing paths are
exact opposites, one for petitioners with documentation and one for petitioners without.

The description used for both rolls states only what the two versions share and what a voter
would recognize: wider eligibility, no filing fee, one petition instead of several, and
retroactive reach. It makes no claim about the presumption or the hearing route, so it is
true of the text each chamber actually voted.
