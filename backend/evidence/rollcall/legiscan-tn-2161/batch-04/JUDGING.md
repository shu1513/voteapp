# Judging notes, Tennessee batch-04

Eleven roll calls on ten measures, all enacted (twelve on eleven as imported;
SB 766 was retracted on PR review, see the end of this file). Every chaptered act was
downloaded through the LegiScan API with byte length and MD5 verified, extracted
to text and read. Scope `--scope-from 2026-08-01`, Tennessee's primary date.

## Version check

One roll needed the draft pulled for comparison.

**HB 1705.** The House passed it 73-21 on 16 March 2026 and later concurred in a
Senate amendment 76-14 on 6 April, which is not divided, so the only divided
House roll is on the pre-amendment text. Comparing the House-passed draft (3,803
characters) with the chaptered act (5,094), the Senate amendment added one
subsection: an employer already using a third-party vendor for Form I-9
verification may keep using it until 1 January 2027, after which E-Verify is
required. The rule the House voted — no government employer may hire on or after
1 July 2026 without verifying work authorization through E-Verify — is unchanged
and is what the description states. The roll carries
`acknowledge_later_rolls: [1676866]`.

Unlike HB 618 in batch-03, this amendment did not change the rule, so one
description serves both the House vote and the act.

Every other roll in the batch was on the text that became law. No vehicle-bill
trap.

## Supersession

Two earlier rolls were marked `superseded` in the worklist rather than left
open: HB 1237's first House passage (1509645, covered by the later concurrence
1538713) and SB 1747's first House vote (1686897, 46-29, covered by 1688346 at
63-18 the next day). The same was done for the earlier rolls on HB 754 and
SB 468 that batches 02 and 03 had already superseded, so the remaining worklist
now contains only rolls that are genuinely available.

## Label reasoning

Every label uses `nay: null`.

- **HB 1705**, **SB 392**, **SB 227** — `immigration`, against. E-Verify
  mandated of every state and local government employer including school
  districts, with the attorney general able to withhold state funds from a
  non-compliant local government; a new human smuggling offense covering
  transporting, concealing, harboring or shielding a person known to be
  unlawfully present, for commercial advantage or private financial gain; and
  civil liability for a charity that houses a person it knows is unlawfully
  present, for crimes that person commits while receiving housing, where the
  charity's own conduct was negligent or worse.
- **HB 1237**, `civil_rights`, against. State boards may not exclude on race,
  color, ethnicity or national origin, and may not operate under race-based
  composition policies — the act names affirmative action, racial preferences
  and racial quotas.
- **SB 2031**, `civil_rights`, against. It creates a civil action against a
  health care professional for injury from a procedure performed to enable a
  person to identify with a gender different from their sex, where consent was
  given in whole or in part because of the professional's coercion. The first
  description omitted the coercion condition, which is an element of the claim,
  not a defense; fixed on PR review.
- **SB 955**, `civil_rights`, against. The Medical Ethics Defense Act protects a
  provider, institution or payer refusing to participate in care that conflicts
  with its conscience, defined for an institution by its own governing
  documents. The act states that additional burden or expense falling on another
  provider is not a defense to a violation, which is the clause that makes the
  direction plain: the refusal is protected even when it shifts the cost of care
  onto someone else.
- **SB 350** and **SB 2459**, `gun_control`, against; **SB 1747**,
  `gun_control`, **for**. SB 350 removes a landlord's ability to bar a tenant's
  lawful firearm in the unit, in a vehicle in tenant parking, and in transit
  between them. SB 2459 bars the children's services department from asking a
  foster parent how many firearms or what type they own, or asking to see them,
  while expressly preserving questions about whether firearms are present and
  whether they are stored safely — the description states that limit. SB 1747
  creates a Class A misdemeanor for recklessly discharging a firearm into the
  air, ground, water or a nearby object at a public gathering of 25 or more,
  with an exception for officers in the line of duty.
- **SB 1858**, `election_integrity`, against. Section 3 changes the time a court
  must give the General Assembly to cure a defect from fifteen calendar days to
  ninety. The direction rests on that concrete fact: a plan a court has found
  defective may stay in use six times longer. The act also sets venue in any
  county and has the supreme court seat a three-judge panel with one judge from
  each grand division, both of which the description states.

## Descriptions

Each cites its own roll call's tally. Plain-language lint: 24 descriptions, 0
warnings, median Flesch-Kincaid grade 6.4, worst 9.1 — the plainest batch so far.

## Duplicates

Swept the candidates who received records for any non-roll-call record on the
same measure and date. 0 found.

## Post-import review (PR #1184)

- **SB 2031.** Coercion condition added to both descriptions; 75 records
  rewritten.
- **SB 766 retracted.** House roll 1536603 (3 April 2025) is 48-27 with 14 not
  voting, and the raw roll says `passed: 0`: 48 is short of the 50-vote
  constitutional majority. The House passed the bill 82-12 on 14 April 2025,
  and LegiScan carries no roll for that vote. So the batch's only divided roll on
  this measure is a failed vote, not the chamber's vote on the enacted text, and
  the description had called it a passage. Retraction follows the Maryland HB
  767 recipe: 65 records retired via `manual:records:retire`
  (`sb766-retirements.json`), the roll set back to pending, the entry removed
  from `judgments.json`, the evidence JSON removed from this directory (it stays
  in the out-of-repo store). The survey's divided-and-enacted worklist keys on
  the bill's enactment, so a failed roll on an enacted bill passes it; check
  `passed` before judging.

Re-import after the corrections: 11 files, 75 rewritten, 721 unchanged, 0
errors (`import-rerun-report.json`). Batch is now **11 rolls / 10 measures /
796 records**.
