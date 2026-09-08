# North Dakota batch-01 — plan

9 measures, 14 roll calls, 334 records across 48 candidates. Local database only.

## How the batch was selected

The five filters, applied in order:

1. **Closely divided** — the smaller side is at least a quarter of the larger.
2. **Became law** — LegiScan status 4, and each measure's own history was checked for a
   governor action. All nine read `Signed by Governor`.
3. **A subject a voter would recognize.**
4. **One roll per measure per chamber** — the chamber's last kept floor vote.
5. **A research area with a defensible for-or-against direction.**

Two North Dakota facts made filter 4 easy and filter 5 hard.

Easy: the state records **no** conference-report or concurrence vote, so a bill has at most
one floor roll per chamber. A proactive check over all 14 picked rolls for a later or
same-day peer in the same chamber returned nothing, so the superseded-stage gate never
fired and no judgment needed `acknowledge_later_rolls`.

Hard: the same fact means a measure that went to conference has only pre-conference votes
on record. 82 of the 194 divided-and-enacted rolls were cast on text a later action
changed, and they are out of the pool for that reason.

## The measures

| measure | rolls | area | direction |
|---|---|---|---|
| HB 1114 insulin out-of-pocket cap | House 59-27 | healthcare_affordability | for |
| HB 1178 students leaving campus to vote | Senate 24-23 | civil_rights | for |
| HB 1216 counting third-party drug payments | House 56-37, Senate 29-18 | healthcare_affordability | for |
| HB 1217 repeal of the HIV transfer crime | House 50-43, Senate 34-13 | civil_rights | for |
| HB 1318 pesticide labeling and the duty to warn | House 51-40, Senate 29-18 | corporate_accountability | against |
| HB 1600 immigration law clinic | House 64-29, Senate 34-13 | immigration | for |
| SB 2258 agencies must cite their authority | Senate 30-17 | government_efficiency | for |
| SB 2339 utility wildfire strict liability | House 62-28 | corporate_accountability | against |
| SB 2352 infants living with incarcerated mothers | House 48-42, Senate 28-18 | social_programs_and_welfare | for |

Both directions appear, and two of the closest votes in the session are here: HB 1178 at
24-23 in the Senate and SB 2352 at 48-42 in the House.

## Dropped under filter 5, after reading the enacted text in full

**HB 1226, wearing a mask in a public place** (House 57-32). Adds a crime of wearing a
mask to hide your identity while congregating in public with other masked people, with
Halloween and masquerades exempt. The worth of the measure is contested, which is fine, but
so is its **direction**: supporters read it as public safety, opponents as a limit on
anonymous assembly and on people who mask for health reasons. Neither area carries an
honest direction, so it is dropped rather than filed under the nearest slug.

**HB 1437, academic tenure policy** (Senate 28-19). Requires public universities to adopt
post-tenure review policies, with removal a possible outcome of an unsatisfactory review.
Read as accountability this is `public_education_quality` for; read as a limit on academic
freedom it is against. Both readings sit **inside that same area**, which is the
Connecticut HB 7042 test, so the counter-reading is not a side strand that can be described
and set aside.

**HB 1454, vaccine opt-out** (House 59-33, Senate 30-17). Bars a state agency or local
government from requiring a vaccine unless it offers an opt-out for health, religious or
philosophical reasons. Vaccine-adjacent, so it was escalated to the operator rather than
decided here, under the rule set by the Florida fluoride decision. **Operator decision,
2026-09-07: drop it.** Calling it `environment_and_public_health`/against assumes vaccine
mandates are the baseline good; calling it `civil_rights`/for assumes the opposite. Since
`general` is barred on roll-call records there is no neutral slot, so the honest outcome is
a drop with the reason written down.

## What is left

67 rolls stay `candidate:batch-02` in `survey/divided-enacted-worklist.tsv`, none of them
re-triaged. Seven agency appropriation acts are excluded by the standing rule and seven
study-only measures are dropped.
