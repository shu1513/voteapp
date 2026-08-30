# PA batch-04 — the rolls that needed a text read

29 rolls / 28 measures / **2,852 records**. These are the votes whose bill had
no fiscal note at the printer's number the chamber actually voted, so each was
judged from the enacted-style bill text at palegis.us instead of a summary.
Judged 2026-08-30. No AI provider call.

## Why this group existed

About 200 of the 256 not-enacted rolls had a fiscal note at the voted printer's
number. The other 56 did not, and most were Senate bills. Rather than judge
those from a title — Pennsylvania titles say almost nothing — they were parked
until their text could be read. Reading the text resolved 28 of them.

## The multi-vote measures live here

Four rolls are the second vote on a measure their chamber voted twice, and
each names the earlier roll in `acknowledge_later_rolls`:

- **SB 114** and **SB 1400** each got a final passage vote and then a
  reconsidered vote the same day on the same printer's number. The
  reconsidered vote is the operative one, so it is the one judged.
- **HB 1042** is the only measure in the whole pool both chambers passed. The
  House voted 149-50 in March and then 102-100 on the final version in July;
  the Senate voted 28-22 and then 30-20 on reconsideration. The decisive vote
  in each chamber is judged, and its tail says the bill had not become law as
  of August 2026 rather than claiming the other chamber never voted.

## Notable content

- **SB 1400** ends the automatic life sentence for second-degree (felony)
  murder for defendants 18 or older, letting courts set a minimum term. Its
  House companion **HB 1042** covers the same ground plus earned time for
  vocational training. Both labelled public_safety_and_crime_control/for,
  consistent with HB 150 (compassionate release) and HB 1936.
- **HB 846 and SB 908 point in opposite directions on the same statute.**
  HB 846 widens the prevailing wage law to cover duct cleaning and offsite
  fabrication; SB 908 narrows it, removing home rehabilitation and school
  safety work. corporate_accountability/for and /against respectively.
- **HB 2189** is a second minimum wage bill, on a flat statewide schedule
  ($11 / $13 / $15) rather than HB 1549's county tiers. Both are recorded.
- **SB 155 and SB 156** (death record and wage record cross-checks against
  Medicaid and food stamp rolls) are labelled government_efficiency/for
  rather than social_programs_and_welfare/against. They are eligibility
  verification, not a benefit cut; the counter-reading, that more frequent
  checks push eligible people off benefits, is recorded here rather than
  turned into a stance.
- **SB 490** (no release on recognizance for defendants found to threaten
  public safety) is labelled public_safety_and_crime_control/for. The
  counter-reading is that expanded money bail detains poor defendants.

## Eleven dropped after reading the text

HB 1532, HB 2650, SB 157, SB 403, SB 404, SB 472, SB 527, SB 682, SB 780,
SB 922 and SB 952 — reasons per roll in
`../survey/divided-not-enacted-worklist.tsv`. The recurring pattern is a
measure that reads two ways at once: SB 952 as both SEPTA accountability and
privatization, SB 403 and SB 404 as both permitting efficiency and weaker
waterway protection, SB 780 as both neighborhood relief and pressure on
unhoused people.

## Import ledger

Dry run 2,852 planned; real run 2,852 inserts, 0 errors, 0 notified, stamp
`2026-08-30T01:45:48.643Z`. Dry re-run all 2,852 `unchanged`. Every label
carries an explicit `nay: null`.

**PROD UNTOUCHED.**
