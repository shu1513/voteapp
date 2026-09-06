# Colorado 2025 Regular Session — the session is finished

Every divided roll call on an enacted bill in LegiScan session 2173 now carries a
disposition. Nothing is left unbatched.

**Local `voteapp` totals: 5,010 records across 52 candidates, 187 approved
rolls, 3,680 area tags. Production holds no Colorado roll-call records.**

## How the session closes

| | rolls |
|---|---|
| imported across ten batches | 188 |
| dropped with a written reason | 214 |
| superseded (the chamber's last floor vote was not divided) | 5 |
| **total worklist rows** | **407** |

By measure: 240 measures were triaged after batches 01 and 02, of which **95
were judged and imported** and **145 dropped**. Adding the 12 measures in the
first two batches, the session's divided-and-enacted measures end as **107 imported and 147
dropped**, with 5 chambers superseded. (Two measures counted here as dropped
were dropped in the first two batches, before the sweep's own triage.)

"Finished" here means every gated measure has a decision on the record, not that
every measure produced records — the same standard Maryland and Georgia closed
on.

## Batches

| batch | subject | measures | rolls | records |
|---|---|---|---|---|
| 01 | marquee: firearms, voting rights, tenants, fees | 7 | 13 | 343 |
| 02 | three new areas: gas stoves, vocational rehabilitation, election intimidation | 5 | 10 | 251 |
| 03 | health care | 7 | 11 | 296 |
| 04 | civil rights and reproductive rights | 9 | 16 | 409 |
| 05 | courts, custody and policing | 10 | 17 | 438 |
| 06 | housing and firearms | 12 | 23 | 595 |
| 07 | environment, energy and infrastructure | 14 | 26 | 695 |
| 08 | schools, immigration and social supports | 7 | 14 | 316 |
| 09 | consumers, insurance and markets | 12 | 19 | 550 |
| 10 | the rest of the session | 24 | 39 | 1,117 |

## Research area coverage

Colorado went from 5 areas after batch-01 to **17 of the 24 selectable areas**:
civil rights, corporate accountability, cost of living, data privacy, election
integrity, environment and public health, government efficiency, gun control,
healthcare affordability, housing affordability, immigration, public education
quality, public infrastructure, public safety and crime control, reduce wealth
gap, social programs and welfare, and women's reproductive rights.

Seven areas drew nothing: anti-corruption keeps only two measures, and foreign
trade, integrity and ethics, national defense, peaceful foreign policy, personal
income tax reduction and government spending reduction have no matching measure
in a state session of this kind.

## Why 145 measures were dropped

Grouped by cause, the largest first:

| cause | measures |
|---|---|
| an appropriation, transfer, fee schedule or other fiscal mechanic | 34 |
| a supplemental budget bill | 13 |
| administrative or internal housekeeping | 12 |
| a sunset continuation of an existing program or board | 10 |
| an advisory body, work group, study or reporting duty | 11 |
| no research area fits the subject | 9 |
| the act runs both ways | 9 |
| conditional on future funding or federal approval | 5 |
| other, each with its own written reason | 42 |

The worklist carries the reason for every one.

**`general` is no longer available.** Roll-call records cannot carry the
non-selectable areas, so a divided vote with no honest direction is dropped
rather than imported without a stance. Marijuana regulation (HB 25-1209) and
nuclear power as clean energy (HB 25-1040) fall here.

## Judgement calls a reviewer should look at

- **Three measures were dropped only after reading the whole act**, because each
  moves in both directions inside one research area: HB 25-1171 widens the
  firearm ban for previous offenders while creating a path to restore the right,
  HB 25-1136 strengthens the police conduct database while making revocation for
  excessive force discretionary, and SB 25-142 extends the deadline for local
  wildfire codes while adding enforcement support.
- **SB 25-208 is the session's only measure scored `against`.** It delays by a
  year Colorado's promise of free prison phone calls, so a yes vote is a vote to
  postpone a benefit. That follows the Maryland HB 102 precedent for a delay.
- **Four measures carry two labels**, each because a second strand is an
  operative duty rather than a gloss: SB 25-130 (emergency care and abortion to
  stabilise a patient), HB 25-1309 (coverage for gender-affirming care as both
  anti-discrimination and cost), and HB 25-1248 (school restraint rules as both
  education and disability rights).

## A defect in my own selection, caught mid-sweep

The worklist was built offline from the raw dataset, which still holds the
identity duplicates the fetcher collapses — it stores the lowest roll id of an
identical group. So a "last roll of the chamber" chosen offline can be one the
pipeline never stored. HB 25-1013's Senate roll was exactly that.

`co_check_rolls.py` now verifies every batch's rolls against the stored set
before judging, and maps a dropped duplicate to its stored twin. Batches 03
onward were all checked; batches 01 and 02 were checked retrospectively and are
clean.

## Wording

Every batch was measured, not eyeballed: the plain-language lint over the
judgments before importing (0 warnings throughout), and Flesch-Kincaid grade
computed separately, since the lint only counts words per sentence. Batch medians
ran 6.6 to 8.5. Where a first draft measured 10 or higher, it was rewritten
before anything was imported.

Each rewrite pass was followed by re-reading the act against the new text,
because shortening is where qualifiers get lost. That check caught dropped
limits in batches 01, 02 and 04, all restored before the import.

## The not-enacted scope is now closed too (batch-11)

This file first said 249 divided rolls remained on measures that did not become
law. **That number was wrong.** It counted question types the Colorado config
excludes — second readings, floor amendments, the previous question. Counting
only the questions this pipeline keeps, the not-enacted pool is 49 divided
rolls, and **41 after the last-vote filter, on 34 measures**.

Batch-11 works all of them: 22 rolls imported over 18 measures, 19 rolls
dropped with written reasons, 16 chambers superseded. Details in
`batch-11/PLAN.md`, and every row is dispositioned in
`survey/not-enacted-worklist.tsv`.

**Both scopes of the 2025 regular session are now finished.**

## What is left for Colorado

- **Production promotion.** Colorado has no roll-call records in production.
- **One session never surveyed**: the 2026 Regular Session (LegiScan 2243).
  The 2025 First Special Session (2224) was finished on 2026-09-06 and lives in
  `backend/evidence/rollcall/legiscan-co-2224/`.
