# West Virginia 2025 Regular Session, batch-01

Three roll calls on three measures, 97 candidate records on the local database.
Production is not touched.

## How the batch was chosen

Every kept floor roll on a bill that became law was put through five filters.

1. **Closely divided.** The smaller side is at least a quarter of the larger.
2. **Became law.** LegiScan status 4.
3. **A subject a voter would recognize.** Local and single-county bills fail here.
4. **One roll per measure per chamber, and it must be that chamber's FINAL
   roll.** This is the important one. Taking the last *divided* roll is wrong,
   because a chamber often votes again on the text that actually became law and
   that later vote is frequently lopsided. Ordering must use West Virginia's own
   printed roll number, not the LegiScan roll id: the ids run out of chamber
   order 438 times in this session.
5. **A research area with an honest for-or-against direction.** Anything that
   would land on the no-stance `general` area is dropped rather than imported.

The pool for the whole state, both sessions together, is 46 measures and 79
roll calls that are both closely divided and enacted. Filter 4 cuts that to 53
measure-chamber slots over 39 measures. This batch takes three of them from the
2025 session; the rest are dispositioned in `survey/dispositions.tsv`.

## What is in the batch

| measure | chamber | roll | tally | area | a yes vote was |
| --- | --- | --- | --- | --- | --- |
| HB 2513 | Senate | 303 | 27-7 | public_education_quality | for |
| HB 3444 | Senate | 447 | 18-15 | environment_and_public_health | against |
| SB 526 | House | 507 | 72-25 | healthcare_affordability | for |

Each measure was read in its enrolled text. `JUDGING.md` records what was
checked and what was found.

## What was dropped, and why

The full ledger is `survey/dispositions.tsv`. The groups:

- **Local bills.** HB 2695 and HB 3492 each authorize one county or city to levy
  an excise tax. HB 2960 is a snow removal pilot confined to two counties.
- **Gambling and racing economics.** HB 2444, HB 2491, HB 2511, SB 576 and
  SB 75. No research area describes a wagering authorisation or an industry
  revenue formula.
- **Administrative machinery and trivia.** HB 2267 is an omnibus authorisation of
  unrelated agency rules. SB 738 repeals a dormant board. SB 677 changes
  securities filing fees. HB 2054 changes a farmers market vendor requirement.
- **The labor gap.** HB 2013 strips civil service protection and the state
  grievance procedure from new hires in five departments. HB 2008 and HB 2009
  reorganise the executive branch. There is no labor research area, and the
  standing workaround of filing an employer mandate under
  `corporate_accountability` does not reach a public employer. This is the
  largest single loss in the West Virginia pool and it is worth raising.
- **Measures that pull both ways.** SB 627 opens state park pore space to
  leasing, which removes a park protection, but the leasing is for carbon
  sequestration, which is climate mitigation. HB 2755 subjects Board of
  Education rules to legislative approval, which reads as accountability or as a
  loss of independence depending on the reader. HB 3279 makes faculty, student
  and staff members of university boards nonvoting, which fits no research area.
- **Deferred rather than guessed at.** SB 587 is a 40,000 character rewrite of
  the competitive bidding statute. Naming one honest direction would need a
  section by section read, so it is left for a later batch.

## Checks run before importing

- Every floor roll on an enacted bill in this session was compared with West
  Virginia's own vote sheet. See the state configuration for the five rolls this
  session that failed and are now held.
- Each selected roll was checked against its own bill's history to confirm which
  text that chamber voted.
- The plain-language lint found no sentence over 45 words; the longest in the
  batch is 39.
- A British-spelling scan came back clean.
- The import dry run planned 97 inserts and the real run inserted 97, with no
  errors and no duplicate flags.
