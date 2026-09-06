# Nevada batch-04 — the first batch outside the enacted pool

**9 measures / 18 roll calls / 364 records / 41 candidates.** Every measure here passed
both chambers of the Nevada Legislature and was then vetoed by the governor, so none of
them became law.

## Why this batch opens a new pool

Batches 01 to 03 gated on "divided **and** became law". That pool is finished: all 104
divided-and-enacted rolls in `../survey/divided-enacted-worklist.tsv` carry a disposition
and nothing is left to judge.

What remains in session 2144 is 187 divided floor roll calls on 114 bills that did not
become law. Measured from the dataset:

| class | measures | divided rolls |
| --- | --- | --- |
| passed both chambers, vetoed by the governor | 79 | 145 |
| passed one chamber, the other never voted | 29 | 30 |
| died after both chambers had voted | 6 | 12 |

The campaign's standing exception to filter 2 is the Pennsylvania batch-02 scope:
measures one chamber passed that the other never voted on. **Nevada barely has that
pool** — 29 measures. Nearly everything left is the vetoed class, where both chambers
recorded a position and only the governor stopped the bill.

**The user was asked and directed that the vetoed pool be opened.** That decision is
recorded here because it is a scope no earlier state in this campaign has taken. The case
for it is that a bill a legislature passed through both houses is a stronger record of
where its members stand than a bill one house passed alone, which the campaign already
imports.

The obligation the scope creates sits on the wording: **every description in this batch
says the governor vetoed the bill and that it never became law, and describes the bill's
provisions with "would have".**

## The nine measures

| measure | area | rolls | what it would have done |
| --- | --- | --- | --- |
| AB 105 | gun_control / for | house 27-14, senate 13-8 | a crime to carry a gun at or within 100 feet of a polling place, counting place or ballot drop box |
| AB 204 | healthcare_affordability / for | house 28-14, senate 12-8 | limits on medical debt collection; no arrest threats, home liens, tax-refund or bank-account seizure |
| AB 245 | gun_control / for | house 27-15, senate 12-8 | under-21 ban on semiautomatic shotguns and semiautomatic centerfire rifles |
| AB 416 | civil_rights / for | house 29-13, senate 13-8 | bars content-based removal of library materials; only a court may order a book out, and only if obscene |
| AB 44 | corporate_accountability / for | house 24-18, senate 13-8 | manipulating the price of an essential good becomes an illegal restraint of trade |
| AB 445 | civil_rights / for | house 32-9, senate 14-7 | immunity for library staff for good faith acts of providing access |
| AB 480 | civil_rights / for | house 30-12, senate 13-8 | writes the disparate impact standard into the Nevada Fair Housing Law |
| SB 171 | civil_rights / for | senate 13-8, house 27-15 | shield law for medically necessary gender-affirming health care |
| SB 217 | womens_reproductive_rights / for | senate 15-5, house 27-15 | protects assisted reproduction from state and local burdens; large plans must cover infertility treatment |

Every label carries `"nay": null`. A no vote on a vetoed bill is not on its own evidence
of the opposite position, and the campaign rule is that `nay` is never inferred by
inverting `yea`.

## Dropped from this batch after reading the text

**AB 388 (paid family leave) — dropped under filter 5.** It reads two ways. It would have
raised state-worker paid family leave from 8 to 12 weeks, raised the pay rate from half
wages to full wages for lower earners, and created a new 12-week paid leave entitlement at
private and local government employers. But section 14 repeals NRS 608.0198, and that
statute — checked against the Nevada Revised Statutes, not against the bill — applies to
**every employer with no size threshold** and gives a victim of domestic violence or
sexual assault up to 160 hours of leave. The bill's replacement, "safe leave", sits inside
a section that reaches only employers with 50 or more employees. So the bill expands paid
leave for most workers while removing an existing leave right from workers at small
employers. That is the same shape as SB 277 and AB 458, both dropped in earlier Nevada
batches.

## Two corrections made while building this batch

**1. AB 44's decisive Senate roll is 1576924 (13-8), not 1576925 (14-7).** Both are stored,
both are dated 2025-05-22, and both carry the desc `Senate Final Passage`. The two differ by
one senator. The judge's superseded-stage gate caught the pair. See `JUDGING.md` for how it
was resolved and `../CODE-FINDINGS.md` §6 for the general pattern.

**2. The Assembly and the Senate voted different texts of AB 44 and AB 204.** Resolved from
each bill's own dated action trail and confirmed against the Nevada Legislature's votes page,
which names the printed version for every vote.

- **AB 44**: the Assembly voted the first reprint 24-18. The Senate then amended it on the
  floor and passed the second reprint. The second reprint added a requirement that the
  seller intend to mislead buyers, and a safe harbour for advertised time-limited sales.
  The Assembly's version was therefore broader, and the Assembly record says so.
- **AB 204**: the Assembly voted the third reprint, the Senate the fourth. The only
  difference is that the Senate capped at six months the pause on collection during a
  federally declared emergency. Too small to change either description.

Everything else in the batch: both chambers voted the same printed text, and that text is
the one that was enrolled and sent to the governor.
