# Where batch-04 comes from

Batches 01 and 02 worked the divided votes on bills that became law. Batch 03
opened the other pool and took the vetoed measures. Batch 04 takes what is left:
bills that passed one chamber on a divided vote and went no further.

A member's vote is a real, recorded position whether or not the bill survived,
so these belong in the record. Pennsylvania batch-02 set the same precedent.

## The pool

49 rolls, one per measure. Every one is LegiScan status 2, meaning the bill
passed a single chamber and, as of the 2026-08-30 dataset, went no further. All 49 were marked
`candidate:batch-04-unbatched` in `../survey/divided-not-enacted-worklist.tsv`.

That worklist is now fully dispositioned. Across its 82 rolls: 19 judged,
43 dropped, 11 superseded, 9 held. No North Carolina pool remains.

## What was checked before anything was imported

**Every tally was read off North Carolina's own roll-call transcript.** All 49,
not a sample. This found one error, described below.

**Member lists.** 47 of the 49 rolls list a full chamber. Two Senate rolls list
49 of 50: S1082 on 2026-05-20 and S808 on 2026-05-05. Both are correct. Senate
District 18 was genuinely vacant — Terence Everitt resigned and Haseeb Fatmi was
appointed 2026-05-21 — so there was no 50th senator to record. This also closes
the open question in finding 3 of `../CODE-FINDINGS.md`, which had flagged the
short May 2026 Senate lists without explaining them.

**Later rolls.** Only H618, H636 and S378 have a later roll in the same chamber,
and all three are procedural motions the config excludes. Nothing here imports a
stage the chamber revisited.

**The version voted.** Six bills have a printed text newer than the vote. Five
are gut-and-replace: the bill number now carries content the chamber never saw.
Those five are dropped. The sixth, S808, is a later refinement of the same
subject and was kept.

## One roll is held, not imported

**H244, House, 2025-04-16, "Depoliticize Government Property Act."**

LegiScan stores 69-43 and codes Representative Mary Harrison of District 61 as
not voting. The official transcript, House RCS 130, prints 69-44 with 113 votes
cast and nobody not voting, and lists Harrison among the noes. LegiScan turned
one member's nay into a non-vote.

This is a new defect class. Finding 3 said a short member list is the warning
sign. Here the member list is complete — all 120 are present — and the tally is
still wrong. The only thing that catches it is reading the transcript. See
finding 4 in `../CODE-FINDINGS.md`. The roll sits in `held-rolls/`.

## In the batch

15 measures, 15 rolls, 1,133 records.

| Measure | What it would have done | Area and direction of a yes vote |
|---|---|---|
| H 38 | Bars card networks from coding gun sellers or listing gun owners | gun control against, data privacy for |
| H 123 | New crime for a provider who falsifies or destroys a medical record | public safety for |
| H 261 | Longer sentences for gang-related crime, and after a federal illegal-reentry conviction | public safety for, immigration against |
| H 519 | Cuts what a minor may consent to alone; opens records to parents | civil rights against |
| H 606 | Revives time-barred gender transition claims; bars state funds for prisoners | civil rights against |
| H 636 | School library review committees and a $5,000 right to sue | civil rights against |
| H 690 | Limits state benefits, tuition and unemployment pay by immigration status | immigration against, social programs against |
| H 781 | Bars local governments from allowing public camping | social programs against |
| H 859 | Bans local guaranteed income programs | social programs against |
| H 936 | Rewrites phone solicitation law around robocalls and spoofing | corporate accountability for |
| S 261 | Drops the 2030 carbon target; lets utilities bill for plants under construction | environment against, cost of living against |
| S 378 | Drops Planned Parenthood as a Medicaid provider | reproductive rights against |
| S 554 | Bars state-chartered lenders from cutting off farmers over emissions | environment against |
| S 1057 | Disclosure and registration duties for proxy advisory firms | corporate accountability for |
| S 1082 | Puts a right-to-work constitutional amendment to voters | reduce wealth gap against |

## Two things the descriptions get right on purpose

**S1082 has not reached the ballot.** The Senate voted 30-16 to send the
amendment to voters on 2026-11-03. The House has not acted, so the question has
not gone to voters. A description saying North Carolina will vote on
right-to-work would be wrong, and so would one saying the matter is closed:
the biennium has not adjourned, so every description in this batch describes
status as of the dataset date and claims no finality. This is also why S1082 carries `acknowledge_later_rolls`: its second and
third readings are both kept votes on the same day with the identical 30-16
tally, and only the third reading is imported.

**S378's short title does not describe the vote.** The title is "Align Medicaid
Eligibility with Federal Law." The two-section text the House voted on removes
Planned Parenthood as a Medicaid provider. The description says so plainly.

## Dropped on the fifth filter

Thirty-three measures were dropped. Each carries its reason in the worklist. The
groups:

- **Five gut-and-replace bills** whose number now carries unrelated content:
  H442, S280, S405, S599, S730. Importing these would attach a member to a
  position on a bill they never saw.
- **Five omnibus bills** with strands pulling opposite ways: H483, H415, S1047,
  S639, H832. The Regulatory Reform Acts were dropped from batch-02 for the same
  reason.
- **One budget vehicle**: S177.
- **Two study-only or definitions-only bills**: H918, H605. Neither changes what
  anyone must do.
- **Bills with no research area that fits** without editorialising: H214, H439,
  H913, H92, S370, S58.
- **Bills whose own effect cuts both ways**: H575, H618, H674, S808, S24, S493.
- **The rest**, for a hidden second policy (H414), an unrelated rider (S488), a
  title far broader than the text (H15, H870), a purely procedural subject
  (H354, H379), no stance axis (H139), or a vehicle bill with unrelated strands
  (H192).
