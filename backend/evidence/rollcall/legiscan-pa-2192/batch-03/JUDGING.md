# PA batch-03 — the screened queue, worked

112 measures / 112 House and Senate rolls / **16,546 records across 194
candidates**. Judged 2026-08-30 from the House or Senate Appropriations
fiscal-note ANALYSIS for the printer's number each roll voted, and from the
bill text where no note exists at that number. No AI provider call.

All 112 are one-chamber-passage measures, so the conditional wording and the
time-stamped tail from batch-02 apply unchanged: "would have", and "As of
August 2026 the Senate had not voted on it."

## Areas

| area | direction | measures |
| --- | --- | --- |
| environment_and_public_health | for | 23 |
| environment_and_public_health | **against** | 8 |
| corporate_accountability | for | 22 |
| public_safety_and_crime_control | for | 13 |
| housing_affordability | for | 11 |
| public_education_quality | for | 8 |
| anti_corruption | for | 6 |
| government_efficiency | for | 4 |
| social_programs_and_welfare | for | 4 |
| public_infrastructure | for | 3 |
| data_privacy | for | 2 |
| civil_rights | **against** | 2 |
| others (reduce_wealth_gap, cost_of_living_reduction, election_integrity, immigration, healthcare_affordability, gun_control) | mixed | 6 |

This is the batch where the Senate finally appears: 20 of the 112 rolls are
Senate votes, and 11 of them score `against` their area — the carbon trading
repeals (SB 1068, SB 186), three emissions-inspection rollbacks (SB 35,
SB 149, SB 1298), the local gas-ban preemption (SB 311), gas plant siting
(SB 704), the vehicle power-source preemption (SB 990), the transgender
sports bills (SB 9, SB 1293) and firearm preemption (SB 822). Batch-02 was
all `for` only because House bills were verified first; that is now corrected.

## Three dropped after reading the analysis

The screen proposed 115. Reading the official analysis killed three:

- **HB 1077** — creates a Commission on Children's Vision and nothing else.
  Standing up an advisory body carries no honest for/against.
- **SB 347** — the title says controlled substance penalties; the text
  criminalizes operating an overdose prevention center. Whether that helps or
  harms public safety is exactly the contested question the fifth filter
  exists to keep out.
- **SB 755** — screened as "abolishing state authorities", which sounded like
  housekeeping. The text abolishes the Climate Change Advisory Committee and
  the Coastal Zone Advisory Committee and reshapes the Human Relations
  Commission. That is not a clean efficiency measure.

Each is a case where the one-line summary and the analysis disagreed.

## Judgment calls worth recording

- **SB 333 and SB 444** (legislative approval and three-year review of
  regulations costing over $1 million a year) are labelled
  government_efficiency/for, following the Texas SB 14 and Georgia HB 1247
  precedent for regulatory-reform bills. Reasonable people read these as
  deregulation; the campaign has already fixed its convention.
- **SB 704** is labelled environment_and_public_health/against for directing
  the state to find sites for new gas power plants. The counter-reading is
  grid reliability, which is the bill's own framing.
- **HB 1364 and HB 1788** are near-identical transit funding bills; both got
  real divided votes and both are recorded.
- **SB 1068 and SB 186** are likewise duplicate carbon-trading repeals.

## Import ledger

Dry run 16,546 planned; real run 16,546 inserts, 0 errors, 0 notified, 194
candidates, stamp `2026-08-30T01:40:29.847Z`. The dry run's own stamp matches
zero rows. A dry re-run reports all 16,546 `unchanged`. One `related` flag,
the same benign hand-written Ann Flood record on a different bill (HB 1442);
0 `ambiguous`. All 112 cleared the superseded-stage gate without needing an
acknowledgement, confirming each is its chamber's only floor vote on the
measure. Every label carries an explicit `nay: null`.

**PROD UNTOUCHED.**
