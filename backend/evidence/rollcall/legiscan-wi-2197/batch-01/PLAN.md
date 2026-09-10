# Wisconsin batch-01 — the enacted scope

Prepared 2026-09-09, completed 2026-09-10. Local database only; production holds
no Wisconsin roll-call records.

**This batch is imported: 15 measures, 16 roll calls, 706 candidate records, 99
candidates, 468 area tags.** How each measure was judged, and why nine were
dropped, is in JUDGING.md next to this file. The selection and the checks that
had to happen first are recorded below, as they were written before any
description existed.

## Scope

The operator chose to work both the enacted pool and the vetoed pool. This batch
is the enacted pool. The vetoed pool is a later batch and needs the one-chamber
exception to the "it became law" filter, declared separately.

## The crosswalk

`crosswalk.json` holds all 132 voting members of the session. Every one was
reviewed, and none is left unreviewed.

- 94 proposed automatically and accepted: 90 matched on an exact first and last
  name, 4 on a first-name prefix.
- 5 added by hand.
- 33 reviewed and left with no candidate, each with the reason on the row.

**All five hand-adds are the same problem, and it has now appeared in most states
of this campaign.** LegiScan stores the member's LEGAL first name in `first_name`
and the working name in `nickname`, while the `name` field matches our candidate
exactly. The automatic proposer reads `first_name`, so it cannot reach them:
Jodene "Jodi" Emerson, Anthony "Tony" Kurtz, Robert "Bob" Donovan and Vincent
"Vinnie" Miresse. The fifth is a plain feed typo — LegiScan writes "Pricilla
Prado" where Wisconsin and our roster write Priscilla. Same surname, same seat,
sitting member.

Three accepted proposals sit in a seat that differs from the candidate's, and all
three are the same honest case: a sitting member running for a different seat in
2026. Romaine Quinn sits in Senate district 25 and is running in district 23.
Robyn Vining sits in Assembly district 13 and is running in Senate district 5.
Jenna Jacobson sits in Assembly district 50 and is running in Senate district 17.

Of the 33 reviewed nulls, 16 are structural: Wisconsin elects its odd-numbered
Senate districts in 2026, so a senator from an even district has no seat on the
ballot. The other 17 hold a seat that IS on the ballot but are not among its
candidates in our roster. That is a large number, and it is worth saying plainly
that it may be two different things — members not seeking re-election, and gaps
in our own roster. A later roster pass would settle it, and re-running the
crosswalk after one would add records to already-judged rolls at no judging cost.

Nine committee pseudo-people are excluded. They are not legislators and never
vote. No Wisconsin member has a `role` that disagrees with its `district`.

**Fan-out is unusually good.** An Assembly roll reaches a median of 86 candidates
and at most 88; a Senate roll reaches 11. All 99 Assembly seats are on the
ballot, which is why. No roll reaches zero. An Assembly roll is worth about eight
Senate rolls here.

## Filter 1 and filter 2: divided, and became law

A vote is closely divided when the smaller side is at least a quarter of the
larger. "Became law" is not the same as LegiScan status 4: an adopted joint
resolution also carries status 4 and is not law. The test used here is that the
bill history carries an "approved by the Governor" line, which 246 of the 315
status-4 measures do; the other 69 are resolutions.

That leaves **24 closely divided roll calls on 23 measures**.

## Filter 4: one roll per measure per chamber

Filter 4 keeps each chamber's last kept floor vote, which is the vote on the text
that became law. **It dropped nothing here.** In no case does a chamber follow a
closely divided vote with a wider one on the same measure. North Dakota was the
only other state where filter 4 did no work.

But the version question is not settled by that alone, and in Wisconsin it has a
particular shape:

**⚠ WISCONSIN RARELY TAKES A RECORDED VOTE WHEN THE FIRST CHAMBER ACCEPTS THE
SECOND CHAMBER'S AMENDMENT.** The history prints "Senate Amendment 1 concurred
in" with no tally. So when the second chamber amends a bill, the first chamber's
only recorded vote is on the text as it stood BEFORE that amendment.

Five of the 24 slots are in that position and **must be read before use**:

| slot | what changed after the vote |
| --- | --- |
| AB 320 Assembly, 65-32 | Senate Amendment 1, concurred in by the Assembly with no recorded vote |
| AB 453 Assembly, 55-39 | Senate Substitute Amendment 1 and an amendment to it |
| AB 75 Assembly, 54-43 | Senate Amendment 1 |
| AB 89 Assembly, 71-26 | Senate Amendments 1 and 2 |
| SB 279 Senate, 17-15 | Assembly Amendment 1 |

If the later change is material, the roll is dropped under the version rule. If it
is not, the roll may be used and the description must describe the text that
chamber voted.

Two further slots looked like the same problem and are not. On SB 485 and SB 785
the Assembly itself adopted a substitute amendment and then concurred as amended,
so the Assembly's recorded vote IS on the text that became law; the Senate's later
concurrence in that substitute is the second chamber agreeing, not a change.

The remaining 17 slots voted the final text with nothing changed afterwards.

## ⚠ One measure became law with a partial veto

**AB 1034** (2025 Wisconsin Act 203, name, image and likeness rights for
University of Wisconsin student athletes) was approved with a partial veto, and
the legislature's attempt to override that partial veto failed. The Senate's
17-16 vote — the closest in the batch — was cast on the whole bill, but what
became law is not the whole bill. The description must be written from the
published Act and must say what the partial veto removed.

The other partially vetoed measure of the session, AB 650, has no closely divided
roll and is not in this batch.

## Filters 3 and 5, as decided

The provisional screen written before the acts were read is superseded by
JUDGING.md, which records the decision on every measure against its enrolled
text. In summary:

- **15 measures kept**, carrying 16 roll calls.
- **9 measures dropped**: AB 320, AB 453 and AB 75 on the version rule; AB 1034
  and SB 45 as appropriations, AB 1034 also for the partial veto; AB 601 because
  no research area describes gambling; AB 737 because it reads both ways inside
  housing affordability; SB 11 because it imposes no duty; SB 283 because the
  nearest area misdescribes the vote.

Every one of the 24 measures in the enacted pool now carries a disposition.

## What happened to the checks listed above

1. Every kept measure was read from its enrolled act. Three drops came out of
   that reading rather than the screen.
2. AB 1034's partial veto was established by comparing the enrolled print with
   the published act. The governor struck the athletics appropriations.
3. All five version questions were settled by fetching and reading the
   amendments. Three were material and cost their measures.
4. **Wisconsin does mark deleted and added text in a way `pdftotext` throws
   away**, and the marks are filled rectangles that pdfminer cannot see.
   `tools/wi_text.py` resolves them; it was calibrated against a passage whose
   answer was known independently.
5. The plain-language lint reported 0 warnings over all 32 descriptions.
6. Judge and import were each run as a dry run and then for real, and the row
   counts reconcile.
