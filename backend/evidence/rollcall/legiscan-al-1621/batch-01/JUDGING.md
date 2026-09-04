# Alabama 2019 Regular Session batch-01 — judging notes

## Sources

Every measure was judged from the version its chamber actually voted, fetched through the LegiScan bulk
API (`getBillText`, and `getAmendment` for conference substitutes) and verified against the byte length
and MD5 hash the dataset records for that document. The state website was not used: it had been timing
out for hours at a stretch during the previous Alabama batch, and one direct download in that batch
returned HTTP 200 with a truncated, unreadable file.

Alabama prints struck and inserted text together and the conversion flattens both into one run of words.
The convention is struck text first, inserted text second, so `no less than 12 10 months` means the old
law said 12 and the new law says 10. Every changed number in these descriptions was read that way.

## Roll-attribution and date audit

Each imported roll's printed roll call number was checked against its own bill's history, and each
roll's date was checked against the bill history line recording the same action. Results for this
session are in `../survey/divided-worklist.tsv`. The term-level findings, including a case where one
session's dataset carried another session's roll calls, are in
`../../legiscan-al-1756/CODE-FINDINGS.md`.

## Label reasoning

Every stance label states `nay` explicitly, and every one is `null`: in each measure the realistic
reason for a no vote runs on a different axis than the scored area.

- **HB 380 — public_safety_and_crime_control, yes = for.** The Act rebuilt the parole board after a
  paroled prisoner killed three people, and writes first-consideration dates into statute: 85 percent of
  the sentence or 15 years for nine named serious felonies. It also lets the Governor or Attorney General
  reverse a departure from those dates. It does contain counterweights, including mandatory
  reconsideration for nonviolent prisoners, but the dominant effect is to make parole harder to reach.
- **SB 128 — election_integrity, yes = for.** A Class A misdemeanor for photographing another person's
  ballot at a polling place. Ballot selfies are expressly protected, and the description says so.
- **SB 193 — social_programs_and_welfare, yes = against.** Unemployment pay falls from a flat 26 weeks to
  14, reaching 20 only at 9.5 percent unemployment, and the cap on total benefits drops from a third of
  past wages to a quarter. The same Act raises the weekly maximum by $10 and adds five weeks for people
  in approved training, and the description states both.
- **HB 212 — public_safety_and_crime_control, yes = for.** A left-lane rule with seven exceptions and a
  60-day warning-only period, the same shape as the 2023 hands-free driving Act.

## Duplicates

A precise sweep found the hand-written records that describe the same votes, and they were retired
before the import. The sweep is restricted to Alabama candidates, an exact vote date, a description
naming the same bill, and a description worded as a vote. It excludes only records whose origin run id
begins `rollcall:`, because hand-written records carry a `manual:candidate-records:...` run id and a
null-check misses them. Sponsorship records naming the same bill were left alone.

## Import and reconciliation

- Real run (stamp `2026-09-03T16:54:39.758Z`): **452 inserts, 0 errors, 0 notified**, across 11 rolls.
- Reconciled three ways: the report totals; the run-stamp predicate (452 rows, 74 distinct
  candidates); and the Alabama roll-call total, which moved from 4,890 to 7,527 across the six batches
  imported together.
- Convergence: a follow-up dry run reports all 452 `unchanged`.

## Writing checks run before import

`candidateRecordPlainLanguageLint`: 0 warnings. Every description is 2 to 4 sentences with no sentence
over 45 words, and a British-spelling scan is clean — it caught real slips on a first pass, including
`legalised`, `licence`, `behaviour`, `labour` and `programme`, all corrected. Reading grade was measured
per session; medians run 8.6 to 11.2.
