# Oklahoma batch 06

Six measures, eleven roll calls, 287 candidate records. Local database only. Production holds
no Oklahoma roll-call records.

| measure | rolls | tally | area | direction |
| --- | --- | --- | --- | --- |
| SB 137 no home monitoring after an impaired-driving injury crash | House | 72-18 | public_safety_and_crime_control | for |
| SB 713 wind farm warning lights that stay off until a plane is near | House override, Senate override | 68-17, 32-15 | environment_and_public_health | for |
| HB 3644 Blake Burgess Act on blood clots | House, Senate | 68-21, 36-11 | environment_and_public_health | for |
| HB 3834 state help for ibogaine drug trials | House, Senate | 69-21, 33-9 | environment_and_public_health | for |
| HB 4071 one-time $250 deposit into a child's Trump Account | House, Senate | 61-30, 25-19 | social_programs_and_welfare | for |
| HB 1727 Oklahoma's Promise for teachers' children | House, Senate | 60-27, 35-10 | public_education_quality | for |

SB 713 is the third Oklahoma measure imported that became law over a veto. Both chambers'
override votes are recorded, and each describes itself as an override.

## Six measures were read and dropped

- **HB 4324 did not become law.** The Governor vetoed it and the House overrode 69-18, but the
  Senate never voted to override, so the veto stands. The survey's enacted filter let it
  through because it counted the override roll without checking that both chambers held one.
  A check of every remaining measure's final status found no other case.
- **HB 1427's Senate slot is a failed vote.** The roll in the pool is the Senate's 17-25 third
  reading, which failed. A reconsideration followed, and the Senate passed a floor substitute
  45-0 in March 2026, but that vote is not in the feed. Importing the 17-25 roll would have
  recorded a failed vote as a passage vote. The act itself only lets banks claim an existing
  clean-burning fuel credit, which is too narrow to import anyway.
- **SB 1027, initiative petitions.** Some of its rules test the honesty of a petition: a plain,
  neutral summary and weekly spending reports. Others limit who may take part: a cap on
  signatures from any one county, a ban on money from outside the state, and a rule that
  circulators be Oklahoma voters. Both strands belong to `election_integrity` and run opposite
  ways, so no single direction is honest.
- **HB 2147, municipal code liens.** Cities may foreclose on rental, vacant or business
  property for unpaid code fines of $1,500 or more; owner-occupied homes are exempt. It sets
  property rights against clearing blighted buildings, and no research area carries either.
- **SB 1255, medical parole.** Only changes who may ask for a medical parole hearing.
- **SB 1509, well spacing.** Drops a required public hearing on groundwater well spacing.
  Procedural, and it reads both ways.

## Checks run before importing

- **Version check.** SB 137 came back at 0.987, SB 713 at 0.976, HB 3644 at 0.976, HB 3834 at
  0.979 and HB 4071 at 0.985. HB 1727 came back at 0.933, because the final text was settled
  after the Senate engrossed it; both of its rolls are fourth readings, which are the chambers'
  votes on that final text.
- **Reading level**, measured before the import: median Flesch-Kincaid grade 7.3, worst 8.5.
- **The repository's plain-language lint**: 22 descriptions, 0 warnings.
- **Related records**: none flagged.

## Result

Dry run planned 287 inserts; the real run inserted 287 with no errors and nobody notified,
under the stamp `2026-09-10T06:39:03.822Z`. The convergence run reports all 287 unchanged.
Oklahoma now holds 1,496 records locally across six batches.
