# Wisconsin batch-01 — the enacted scope

Prepared 2026-09-09. Local database only. Nothing is imported yet: this document
records the selection and the checks that must happen before any description is
written.

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

## Filters 3 and 5, provisional

These are screens from the title and the bill's own summary line. **A title is
never a basis for a judgment**, and every one of these has to be confirmed
against the enrolled Act before a description is written. This list exists so the
reading order is sensible, not to pre-decide anything.

Likely to carry an honest direction, subject to the read:

| measure | act | subject as titled |
| --- | --- | --- |
| AB 180 (both chambers) | 116 | seek a waiver to bar buying candy and soft drinks with FoodShare benefits |
| AB 2 | 42 | school boards must adopt a policy barring phones during instruction |
| AB 223 | 126 | residency requirement for people circulating nomination papers |
| AB 35 | 43 | withdrawal of candidacy, with a penalty |
| AB 453 | 173 | required approvals of rezoning for residential development |
| AB 592 | 95 | professional development for science teachers |
| AB 75 | 45 | Justice Department collection and reporting of criminal case data |
| AB 89 | 106 | theft crimes and a penalty |
| SB 106 | 9 | psychiatric residential treatment facilities |
| SB 108 | 10 | sharing minors' safety plans |
| SB 182 | 35 | reimbursement for emergency medical responder training |
| SB 279 | 58 | grants for law enforcement data-sharing platforms |
| SB 283 | 14 | hearing protection for public protective services |
| SB 485 | 184 | video monitoring in residential care centers for children |
| SB 56 | 8 | federal grant funds for lead service line replacement |
| SB 785 | 185 | license-holder investigations shown in the online licensing portal |
| SB 825 | 110 | environmental review before certain major highway projects |

Expected to be hard, and quite possibly filter-5 drops:

- **AB 446** (Act 143), consideration of a particular definition of antisemitism.
  The objection to it is a free-speech objection, which is the same axis as the
  protection it offers. Both readings sit inside `civil_rights`.
- **AB 601** (Act 247), excluding certain event or sports wagers from the
  definition of a bet. No research area describes gambling.
- **AB 737** (Act 120), financing infrastructure through a special charge approved
  by a neighborhood improvement district. A financing mechanism, not a policy
  direction.
- **AB 320** (Act 179), increasing court fees and indexing them to inflation.
  Also carries the version problem above.
- **SB 11** (Act 79), letting federally chartered youth organizations present to
  pupils. Salient but with no honest direction.
- **AB 1034** (Act 203), the name, image and likeness bill, on top of the partial
  veto problem.

## What must happen before anything is imported

1. Read the enrolled Act for every measure kept. The title is not the text.
2. For AB 1034, read the published Act and establish what the partial veto struck.
3. For the five slots listed above, read the later amendment and decide whether
   it is material.
4. Determine whether Wisconsin marks deleted and added text in a way `pdftotext`
   throws away. This has not been checked yet, and nearly every state does.
5. Write the descriptions, join sentences with a period, and run
   `candidateRecordPlainLanguageLint` over the judgments file before importing.
6. Judge as a dry run, then for real; import as a dry run, then for real;
   reconcile the row counts three ways.
