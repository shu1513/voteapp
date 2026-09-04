# Alabama 2020 Regular Session batch-02 — judging notes

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

- **HB 34 and SB 165 — general, no stance.** HB 34 widens who is pulled from a car for driving
  unlicensed while making impoundment optional, capping wheel-clamp fees and letting a driver recover
  towing costs, so it tightens and loosens at once. SB 165, the Compassion Act, is the medical cannabis
  bill, and cannabis measures carry no stance in this campaign (the Ohio HB 116 and Pennsylvania
  precedent).

## Duplicates

The precise sweep found the hand-written records describing the same votes, and they were retired before
the import. It excludes only records whose origin run id begins `rollcall:`, because hand-written records
carry a `manual:candidate-records:...` run id.

## Import and reconciliation

- Real run (stamp `2026-09-04T20:17:06.340Z`): **67 inserts, 0 errors, 0 notified**, across 2 rolls.
- Reconciled three ways: report totals; the run-stamp predicate (67 rows, 67 distinct candidates);
  and the Alabama roll-call total, which moved from 7,527 to 8,886 across the four batch-02 runs.
- Convergence: a follow-up dry run reports all 67 `unchanged`.

## Writing checks run before import

`candidateRecordPlainLanguageLint`: 0 warnings, every description 2 to 4 sentences with no sentence over
45 words. The British-spelling scan again earned its place, catching twelve slips across four sessions
including `enrol`, `programme`, `licence`, `immunisation`, `penalised`, `authorise` and `behaviour`, all
corrected. Reading-grade medians run 9.5 to 11.3.
