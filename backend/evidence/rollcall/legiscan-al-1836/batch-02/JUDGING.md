# Alabama 2022 Regular Session batch-02 — judging notes

## Sources

Each measure was judged from the version its chamber voted, fetched through the LegiScan bulk API and
verified against the byte length and MD5 hash the dataset records: the engrossed print where one exists,
the introduced print otherwise. The feed records none of these measures as having become law; the one
that did (SB 46, 2021) is covered below and its description says so.

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
`Passed`, which is why they fall outside batch-01. Because the feed stops at delivery, the outcome was
taken from the public record: **SB 46 was signed on 2021-05-17 and became Act 2021-450** (the act as
published by the Alabama Department of Agriculture and Industries, agi.alabama.gov), and **SB 94 was
vetoed on 2021-05-27** (the Governor's statement on SB 94, governor.alabama.gov). Both descriptions say
so. A first draft stopped at "went to the Governor"; review caught it and the records were re-imported.

## Label reasoning

Every stance label states `nay` explicitly, and every one is `null`.

- **HB 312 — civil_rights, yes = against.** A ban on teaching a listed set of ideas about race, sex and
  religion in state agencies and public schools. Most items in the list are gated by the word *solely*,
  which narrows them; three are not, which does not.
- **HB 239 and HB 63 — election_integrity, yes = for.** Barring candidates and poll watchers from helping
  a voter, and banning part-filled registration and absentee forms. HB 239 also drops the old municipal
  requirement that a voter swear to a specific disability before getting any help, and the description
  states that.
- **SB 79 — civil_rights, yes = for.** One statewide set of fair-hearing rules for school suspension and
  expulsion, matching the label given to SB 181 of 2023, which was the same idea two years later.
- **SB 301 — public_safety_and_crime_control, yes = for.** Mandatory extra prison time for carrying a gun
  during a violent crime, none of it reducible by probation or good behavior credit.
- **SB 26 — data_privacy, yes = against.** A state wiretap power for felony drug cases. It carries real
  safeguards, including a judge who may not later hear the prosecution, but it also lets the state police
  demand phone toll records and subscriber details by administrative subpoena with no judge at all.
  **The roll is a 13-13 tie**, which fails, and the description says the bill failed.
- **SB 38 — government_efficiency, yes = for.** Folding the State Auditor's office into the Treasurer's.
  **The roll is a defeat**: 46-36 is short of the three fifths a constitutional amendment needs, and the
  description says exactly that rather than merely reporting the tally.
- **SB 51 — public_education_quality, yes = for.** A mental health service coordinator in every school
  system, conditional on the Legislature funding it. **Defeated 10-16.**
- **HB 337 and SB 255 carry no stance.** The fleeing-police penalties cut both ways, as in 2021, and
  SB 255 curbs the State Health Officer's emergency authority while expanding the same officer's
  surveillance duties. **SB 255 was defeated in the House 40-53** after passing the Senate.

## Duplicates

The precise sweep found the hand-written records describing the same votes, and they were retired before
the import. It excludes only records whose origin run id begins `rollcall:`, because hand-written records
carry a `manual:candidate-records:...` run id.

## Import and reconciliation

- Real run (stamp `2026-09-04T20:17:24.596Z`): **456 inserts, 0 errors, 0 notified**, across 11 rolls.
- Reconciled three ways: report totals; the run-stamp predicate (456 rows, 79 distinct candidates);
  and the Alabama roll-call total, which moved from 7,527 to 8,886 across the four batch-02 runs.
- Convergence: a follow-up dry run reports all 456 `unchanged`.

## Writing checks run before import

`candidateRecordPlainLanguageLint`: 0 warnings, every description 2 to 4 sentences with no sentence over
45 words. The British-spelling scan again earned its place, catching twelve slips across four sessions
including `enrol`, `programme`, `licence`, `immunisation`, `penalised`, `authorise` and `behaviour`, all
corrected. Review then found twelve more the scan had missed (`maths`, `licences`, `offences`, `offence`,
`counselling`); those were corrected and the records re-imported. Reading-grade medians run 9.5 to 11.3.
