# West Virginia 2026 Regular Session, batch-01

Four roll calls on three measures, 169 candidate records on the local database.
Production is not touched.

## How the batch was chosen

The same five filters as the 2025 batch: closely divided, became law, a subject
a voter would recognize, one roll per measure per chamber taking that chamber's
FINAL roll, and a research area with an honest direction.

Ordering for filter 4 uses West Virginia's own printed roll number. The LegiScan
roll ids run out of chamber order 493 times in this session, so sorting by id
would pick the wrong roll.

## What is in the batch

| measure | chamber | roll | tally | area | a yes vote was |
| --- | --- | --- | --- | --- | --- |
| HB 4005 | Senate | 368 | 27-7 | social_programs_and_welfare | against |
| SB 586 | House | 430 | 68-27 | environment_and_public_health | against |
| SB 641 | House | 431 | 58-34 | environment_and_public_health | against |
| SB 641 | Senate | 600 | 25-8 | environment_and_public_health | against |

SB 641 carries both chambers because both voted the text that became law. The
Senate's earlier passage vote on 4 March, roll 334, is deliberately NOT used: it
was cast on a version that exempted brine tanks under 50,000 gallons, and the
House then cut that to 10,000 gallons or less. The Senate roll that is used is
its 13 March vote agreeing to the House version, which is the enacted text.

## What was dropped, and why

The full ledger is `survey/dispositions.tsv`. The groups:

- **Local bills.** SB 749 authorizes one county to levy an excise tax.
- **Appropriations.** SB 841 and SB 871. A vote to fund something carries no
  honest for-or-against direction.
- **Racing economics.** SB 1060, on thoroughbred eligibility and race caps.
- **Too small or purely administrative.** HB 4951 adds a nutrition requirement to
  continuing medical education. SB 694 removes a county superintendent residency
  rule. SB 1033 adjusts agriculture department land powers. HB 4464 changes
  underground utility damage-prevention mechanics. HB 4425 repeals a section on
  athletic eligibility for transfer students, which is about school sports.
- **The labor gap.** HB 4025 strips civil service protection and the state
  grievance procedure from new hires in three health departments, and HB 5441
  rewrites the state personnel system. There is no labor research area, and the
  `corporate_accountability` workaround does not reach a public employer.
- **Measures that pull both ways.** SB 890 replaces the 180 instructional day
  floor with a 900 instructional hour floor, which allows less instruction, but
  it also stops teacher preparation days being repurposed and creates three
  Educator Focus Days that classroom teachers control. A reader who cares about
  education quality could reasonably want yes on one half and no on the other.
- **Charter school expansion.** SB 63, the SOAR Act, creates rural charter
  schools sponsored by existing charter schools and lets a charter in good
  standing open a second campus on an abbreviated application. The standing
  precedent in this campaign is that school choice financing has no defensible
  direction under `public_education_quality`.

## Checks run before importing

- Every floor roll on an enacted bill in this session was compared with West
  Virginia's own vote sheet. Six rolls failed and are held in the state
  configuration, including HB 5323, whose LegiScan tally of 61-31 is one no vote
  short of the official 61-32.
- Each selected roll was checked against its own bill's history, and for SB 641
  the engrossed text was compared with the enrolled text word by word.
- The plain-language lint found no sentence over 45 words.
- A British-spelling scan came back clean.
- The import dry run planned 169 inserts and the real run inserted 169, with no
  errors and no duplicate flags.
