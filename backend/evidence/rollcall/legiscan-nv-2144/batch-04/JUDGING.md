# Nevada batch-04 — judging and import

**Result on local `voteapp`, 2026-09-06: 18 roll calls approved, 364 records inserted
across 41 candidates, 0 errors, 0 notified. Production untouched.**

Nevada now holds **1,110 live roll-call records**.

## Sources

Every measure was judged from the **enrolled text**, downloaded through the LegiScan
`getBillText` API and verified two ways before it was read: the returned bytes matched the
`text_size` the dataset records, and the MD5 checksum matched the dataset's `text_hash`.
All 27 documents downloaded for this batch passed both checks. Character counts were
compared rather than byte sizes when comparing versions, because a Nevada reprint carries
strike-through markup that changes the byte size without changing the content.

A vetoed Nevada bill still has an enrolled text: it passed both houses and was enrolled
and delivered to the governor before the veto. That is the text these descriptions
describe.

Version-per-chamber was resolved from each bill's dated action trail. Nevada prints a
reprint **after** a floor amendment when the chamber "dispenses with reprinting", so the
text a chamber voted is not always the last reprint printed before the vote. Resolving on
the last reprint before the vote alone gave the wrong answer on AB 44, AB 245, AB 445 and
SB 217. The rule that works, and that was applied to all 184 divided non-enacted Nevada
rolls: if the chamber amended on the floor and dispensed with reprinting, the text it
voted is the reprint printed immediately after the vote. The results were checked against
the Nevada Legislature's own votes page, which labels each vote with its printed version
("Assembly (1st Reprint)", "Senate (2nd Reprint)").

## The superseded gate fired once, and it was right

The judge refused AB 44's Senate roll 1576925 because roll 1576924 sits on the same
measure, in the same chamber, on the same day.

Both are `Senate Final Passage` on 2025-05-22. Roll 1576924 is 13-8, roll 1576925 is 14-7,
and exactly one senator votes differently between them. Resolving it took three sources:

1. **The bill's action trail** records one Senate third reading, "Passed, as amended. Title
   approved. (Yeas: 13, Nays: 8.)" — it does not mention a second vote at all.
2. **Nevada's own votes page** for AB 44 lists three votes and shows both Senate votes, in
   the order 14-7 then 13-8.
3. **Two comparable bills settle the ordering.** AB 123 and AB 451 have the same double-roll
   shape, and their action trails spell out what happened: "Read third time. Passed…",
   then "Action of passage reconsidered", then "Read third time. Passed…" again. For both
   bills, Nevada's votes page lists the two votes in the same order the trail does — first
   vote first. So the page is chronological.

Applying that to AB 44: the page lists 14-7 first, so 14-7 was the first vote and **13-8 is
the vote taken after reconsideration**. It is also the only tally the action trail records.
Roll 1576924 was therefore judged, with `acknowledge_later_rolls: [1576925]` naming the
superseded first vote, and roll 1576925 is dispositioned as superseded in the worklist.

**An earlier reading of this was wrong and is recorded here so it is not repeated.** The
first attempt treated the action trail's 13-8 as disagreeing with the roll data and kept
the 14-7 roll because it had the higher roll id. Nevada roll ids are not chronological —
AB 123's decisive 14-7 vote carries the *lower* id of its pair — so id order proves
nothing. The action trail plus the votes page is what settles it.

## Labels

Nine measures, nine research-area labels, every one with `"nay": null`.

The two that needed the most care:

- **AB 416** has a counterweight worth naming: it creates a court petition to remove a
  library book, which did not exist in that form before. It was kept as one-directional
  anyway. Before the bill a school board could remove a book by its own vote; after it,
  only a court can, and only on a finding that the book is obscene. The petition is a
  narrowing of the power to remove, not an expansion of it. The description says both that
  removal is barred and that a court may still order an obscene book out.
- **AB 44** carries carve-outs large enough that leaving them out would mislead: resort
  hotels and rideshare companies are exempt, and the new rule gives nobody a private right
  to sue. Both are stated in the description. They narrow the ban but do not reverse it —
  no provision in the bill loosens anything that existed before — so the direction holds.

## Wording checks, all run before the import

- The real `candidateRecordPlainLanguageLint` ran over all 36 descriptions: **0 warnings**.
  No sentence exceeds 45 words.
- Every description is 2 to 4 sentences.
- British spellings scanned for and none found.
- Each description cites its own roll call's tally, checked against the stored row with the
  same boundary-anchored pattern the judge uses.
- Measure, date and chamber checked against the stored row for all 18 judgments.

## Reconciliation — three ways

| check | result |
| --- | --- |
| import report | 364 inserts, 746 unchanged, 0 errors |
| run-stamp predicate `rollcall:NV:%:2026-09-06T07:19:55.157Z` | 364 records, 41 candidates |
| table delta | 1,110 − 746 = 364 |

Per-roll fan-out matched Nevada's documented rates with no roll reaching zero: 29 or 30
candidates on every Assembly roll, 10 or 11 on every Senate roll.

A convergence dry run after all work reports all 1,110 records `unchanged`.

## Duplicate sweep

Swept with `origin_run_id NOT LIKE 'rollcall:%'`, which is what finds hand-written rows —
they carry `manual:...`, so a test for a null run id finds nothing.

Within a candidate there are **0 duplicate record identity keys and 0 duplicate source
URLs** across all Nevada roll-call records. The identity key is a hash of the description
and is shared by the different members who cast the same vote, so repeats of a key across
candidates are expected and are not duplicates.

Six hand-written rows mention a measure in this batch. Five are different acts and were
kept: sponsoring AB 416, two members of the public testifying against AB 416, the governor
vetoing AB 44, and the Attorney General championing AB 44.

**One was a true duplicate and was retired.** Record `dc14e427-d55a-4ab9-a089-b81d1fdc0890`
recorded the same member's same 2025-04-22 vote for AB 480 at the same 30-12 tally. The
roll-call row replaces it and is better: it carries the roll call, the research-area label,
and the fact that the governor vetoed the bill, which the hand-written row did not say.

## A pre-existing wording defect in batches 01 to 03, found and fixed

**645 of Nevada's 746 existing roll-call records ended with a sentence that began with a
lowercase word**: "The Nevada Senate passed it 15-6. and the bill was signed into law."
A closing clause had been joined to the tally sentence with a full stop instead of a comma.
No other state in the database had it.

Fixed at the source: the comma was restored in the `judgments.json` files of batches 01,
02 and 03 (70 description fields), those files were re-applied with `rollcall:judge`, and
`rollcall:legiscan:import` rewrote the records in place — **645 refreshed, 465 unchanged**,
which is exactly the 645 defective rows. A database scan for the pattern now returns 0 for
Nevada and 0 across every state. The insert ledger for this batch is `import-report.json`;
the rewrite is `import-rerun-report.json`.

## Review fix, 2026-09-06 (PR #1193)

**SB 171** — both chambers' descriptions said the governor could not hand a person over to
another state over gender-affirming care. Section 2 of the bill keeps extradition when the
person was physically present in the demanding state at the time of the alleged offense
and then fled. The descriptions now carry that exception. Re-applied with `rollcall:judge`
and `rollcall:legiscan:import`; the rewrite is recorded in
`batch-05/import-review-rewrite-report.json` (41 SB 171 rows inside a 112-row pass).

Section 1 carries a second limit that the description still does not spell out: the shield
does not apply if the acts charged would also be a crime under Nevada law. The first
sentence already says the care has to be lawful in Nevada, which carries the substance, and
the sentence is at 43 words against a 45-word limit. Left as is deliberately.
