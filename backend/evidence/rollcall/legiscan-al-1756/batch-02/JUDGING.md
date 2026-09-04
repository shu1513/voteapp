# Alabama 2021 Regular Session batch-02 — judging notes

## Sources

Each measure was judged from the version its chamber voted, fetched through the LegiScan bulk API and
verified against the byte length and MD5 hash the dataset records: the engrossed print where one exists,
the introduced print otherwise. None of these measures is recorded as having become law, and no
description says one did.

## Reading the outcome of each roll, not guessing it

A tally alone does not say whether a vote carried. Alabama constitutional amendments need three fifths
of a chamber, so 46-36 in the House and 19-13 in the Senate are both defeats, and a 13-13 tie fails.
**Every roll's outcome here was read off its own bill-history line** — `adopted` or `lost` against the
matching roll call number — and for the 2019 and 2020 sessions, which print no roll call numbers, off
the history action on the same date and chamber.

That check changed what several descriptions say. Eight of the imported rolls are defeats rather than
passages, and each of those descriptions says the bill failed rather than reporting a bare tally.

## Measures that reached the Governor

Two measures in the 2021 regular session, SB 46 and SB 94, passed both chambers and were delivered to
the Governor, and the dataset records nothing after that. Their LegiScan status is `Enrolled`, not
`Passed`, which is why they fall outside batch-01. **The descriptions say the bill went to the Governor
and stop there**, because the feed does not support saying either that it became law or that it died.

## Label reasoning

Every stance label states `nay` explicitly, and every one is `null`.

- **HB 445 — public_safety_and_crime_control, yes = for.** The Anti-Aggravated Riot Act: felony assault
  on a first responder with mandatory minimums, riot redefined to require a dispersal order first, and a
  new crime of blocking a road on foot. It is scored on crime control, which is the Act's own subject;
  the objection that it chills protest runs on a different axis, so nay is null. The 2022 successor,
  HB 2, is labelled the same way.
- **HB 184 — environment_and_public_health, yes = for.** Compulsory reporting to and checking of the
  state immunization registry, with an express bar on tracking unvaccinated people.
- **HB 238, HB 72 and HB 90 — public_safety_and_crime_control, yes = for.** Boating under the influence;
  a shorter supervised-release window for the longest sentences, the same change the 2021 special
  session enacted as HB 2; and a hands-free driving law. **HB 238 and HB 90 are defeats**, 42-45 and
  47-48, and the descriptions say the bill failed.
- **SB 158 — public_safety_and_crime_control, yes = for.** A statewide police misconduct database with
  mandatory reporting deadlines and a ten-item pre-employment check. It is closed to the public, and the
  description says so.
- **SB 91 — civil_rights, yes = for.** A ban on racial profiling in traffic stops with statewide data
  collection. The description names the limit that matters: the ban reaches a stop based *solely* on race
  or ethnicity, so a stop with any other contributing reason falls outside it.
- **Nine measures carry no stance**: two gaming constitutional amendments (SB 214, which failed 19-13,
  and SB 319), the Compassion Act (SB 46), the Literacy Act delay (SB 94), the emergency-powers limits
  (SB 97), grand jury witness secrecy (HB 202), resentencing (HB 24) and the fleeing-police penalties
  (HB 239), which raises the ceiling to a Class B felony while narrowing one route to a felony.

## Duplicates

The precise sweep found the hand-written records describing the same votes, and they were retired before
the import. It excludes only records whose origin run id begins `rollcall:`, because hand-written records
carry a `manual:candidate-records:...` run id.

## Import and reconciliation

- Real run (stamp `2026-09-04T20:17:13.145Z`): **651 inserts, 0 errors, 0 notified**, across 17 rolls.
- Reconciled three ways: report totals; the run-stamp predicate (651 rows, 76 distinct candidates);
  and the Alabama roll-call total, which moved from 7,527 to 8,886 across the four batch-02 runs.
- Convergence: a follow-up dry run reports all 651 `unchanged`.

## Writing checks run before import

`candidateRecordPlainLanguageLint`: 0 warnings, every description 2 to 4 sentences with no sentence over
45 words. The British-spelling scan again earned its place, catching twelve slips across four sessions
including `enrol`, `programme`, `licence`, `immunisation`, `penalised`, `authorise` and `behaviour`, all
corrected. Reading-grade medians run 9.5 to 11.3.
