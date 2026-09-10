# Wisconsin batch-04 — how each measure was judged

Judged 2026-09-10. Local database only; production holds no Wisconsin roll-call
records.

Every description was written from the enrolled print.

## Result

**9 measures, 14 roll calls, 835 candidate records, 99 candidates, 479 area
tags.** One measure was dropped and one roll call was dropped on the version
rule.

## ⭐ SB 7 is the vehicle-bill check earning its keep

LegiScan's title for SB 7 is *"Prohibiting a foreign adversary from acquiring
agricultural or forestry land in this state."* That is the title of the bill as
introduced. It is not what the Assembly voted on.

The Assembly replaced the entire bill with Assembly Substitute Amendment 1, and
the enrolled print does two things the title does not describe:

1. The ban covers **all real property**, not agricultural or forestry land.
2. A second, unrelated section would have barred using the power of
   condemnation to take property for **a wind energy facility or a solar energy
   facility**.

A description written from the title would have been wrong on both counts. The
standing rule is to read the enrolled text for every selected measure, and this
is the case in Wisconsin where skipping it would have produced a false record.

**The Senate slot is dropped.** The Senate passed SB 7 18-15 on 21 January 2026;
the Assembly substituted the whole bill the next day. A whole-bill substitute is
material by definition, which is the rule that dropped AB 453 in batch-01. Only
the Assembly's 55-42 concurrence in the substitute survives.

**Only one label, `national_defense`, for a two-strand act.** The
foreign-adversary property ban is the strand an area reaches. The condemnation
ban was considered for `environment_and_public_health`, on the reasoning that
making wind and solar projects harder to site works against the area, and
rejected: the act does not restrict wind or solar, it restricts one method of
acquiring land for them, and that is a property-rights mechanism no area's
definition describes. The description states the condemnation section plainly so
a reader sees what the vote covered.

## ⚠ Why six bills share one `national_defense` label

AB 415, AB 662, AB 663, AB 673, SB 7 and SB 10 are all labeled
`national_defense`, yes = for. The area reads "maintain military readiness and
deterrence to protect national security interests," and the reasoning is worth
stating once rather than six times.

Five of the six are state-level countermeasures against a foreign adversary: no
adversary software on state devices, no state contracts with adversary-owned
firms, no unreviewed university research partnerships, no adversary-made genetic
sequencers, no adversary ownership of land. Each is a national security interest
acted on with the tools a state has. The sixth, SB 10, is military recruiting
access, which goes to force readiness directly.

Five of them share one definition of "foreign adversary" — China, Cuba, Iran,
North Korea, Russia and the Maduro government in Venezuela — pinned to the same
federal list at 15 CFR 791.4, so the list moves when the federal list moves.
Reading the six together is what made that visible.

**AB 673 carries a second label, `data_privacy`, yes = for.** Half of that act is
not about equipment at all: it bars anyone from storing a Wisconsin resident's
genome sequencing data inside a foreign adversary country, and requires that the
data be kept out of reach of anyone located there. That is a limit on where
personal data may be held and who may see it, which is what the area names.

## The immigration three

AB 24, AB 281 and AB 308 are labeled `immigration`, yes = **against**. The
area's own definition is "welcome immigration through a lawful, orderly, and
humane system." Each of the three narrows what an unlawfully present person may
receive or makes state and local government an instrument of federal
enforcement, so a yes vote is against the area as written.

- **AB 24** requires sheriffs to check lawful presence for felony detainees,
  report failures to homeland security, and honor federal detainers, and cuts a
  county's shared revenue by 15 percent if its sheriff does not certify
  compliance. The financial penalty on a county is what makes this stronger than
  a permission.
- **AB 281** requires E-Verify of government contractors and state employment.
- **AB 308** bars public money from paying for health care for a person who is
  not lawfully present. `healthcare_affordability` was considered as a second
  label and rejected: it is one mechanism, not two strands, and the group whose
  access is removed is defined by immigration status.

All three are `nay: null`.

## The drop

- **AB 674** would have barred insurers and Medicaid from knowingly covering an
  organ transplant performed in China, or another country designated as
  participating in forced organ harvesting, or using an organ from one.
  Dropped on filter 5. No research area describes it. `national_defense` does
  not fit, because nothing here concerns security or readiness.
  `healthcare_affordability` would be actively misleading: reading a 64-33 vote
  against forced organ harvesting as a vote to remove insurance coverage
  misstates what the chamber was doing. Dropping a clean single-purpose bill
  beats filing it under the nearest slug, which is the same call made for
  Wisconsin SB 283 in batch-01.

## The labels

| measure | area | yes means |
| --- | --- | --- |
| AB 24 | immigration | against |
| AB 281 | immigration | against |
| AB 308 | immigration | against |
| AB 415 | national_defense | for |
| AB 662 | national_defense | for |
| AB 663 | national_defense | for |
| AB 673 | national_defense | for |
| AB 673 | data_privacy | for |
| SB 7 | national_defense | for |
| SB 10 | national_defense | for |

Every label is `nay: null`. None of these acts is single-subject in the way that
justifies a stated no-side position: a no vote on any of them could rest on
scope, cost or federal preemption rather than on the area itself.

## Quality, measured before importing

- Plain-language lint over all 28 descriptions: **0 warnings**. Longest sentence
  42 words. Two sentences measured 46 and 53 words on the first run and were
  split.
- Every roll number, chamber, date and tally checked against `legislative_votes`
  before judging.
- Version check on all 15 candidate slots. SB 7's Senate slot was the only one
  affected, and it was dropped.
- British-spelling scan over the descriptions and these documents: clean.

## Reconciliation

- Plan: 835 inserts over 14 rolls.
- Real run: `outcomes {imported: 14}`, `actions {insert: 835}`, 0 errors,
  0 notified. Run stamp `2026-09-10T06:30:11.733Z`.
- Run-stamp predicate: 835. All Wisconsin roll-call records: 3,258, which is
  706 plus 839 plus 878 plus this batch's 835.
- Tags predicted independently and confirmed: 429 records on the yes side, of
  which the 50 on AB 673 carry two tags each, giving 479. The database agreed.
- 99 distinct candidates.

**Duplicate sweep: nothing to retire, but one existing record is wrong.** A
hand-written record says a member "Coauthored Assembly Bill 24, which passed the
Wisconsin Assembly and **required** county sheriff assistance with certain
federal immigration functions." AB 24 never required anything: it was vetoed and
the override failed. The record is a sponsorship claim, not a vote, so it is not
a duplicate of anything imported here and nothing was retired. The wrong verb is
a separate defect and has been raised as its own task. Its record id is
`66e2434a-68dc-472f-b55a-6a058463da4e`.

This is a hazard worth naming for the rest of this pool: **a sponsorship record
written from a bill's title will describe a vetoed bill as though it took
effect.** Any state working a vetoed scope should sweep for it.
