# Alabama 2022 Regular Session batch-01 — judging notes

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

- **HB 272 and SB 2 — gun_control, yes = against.** Permitless concealed carry, and a bar on state and
  local officials enforcing presidential executive orders on firearms. HB 272 does tighten in places, with
  a new duty to tell an officer you are armed and a new crime of touching the gun during a stop, and the
  description states both.
- **HB 322 and SB 184 — civil_rights, yes = against.** School restrooms by the sex on the original birth
  certificate, plus a limit on instruction in kindergarten through fifth grade; and a Class C felony for
  providing gender-affirming care to a minor. In both, the defined terms decide the reach, and the
  descriptions carry them: "original" birth certificate, and the exception for a child born with a
  medically verified difference of sex development.
- **HB 230 — civil_rights, yes = for.** Leg and waist restraints banned for pregnant prisoners through
  the six weeks after birth.
- **HB 194 — election_integrity, yes = for.** No private money for running elections, a Class B
  misdemeanor.
- **SB 313 — anti_corruption, yes = for.** No public funds for ballot-measure advocacy, enforced by a
  demand for repayment rather than prosecution.
- **SB 158 and SB 168 — environment_and_public_health, yes = for.** A rebuilt lead abatement programme
  with daily penalties, and the legalisation of fentanyl test strips.
- **SB 171 — public_education_quality, yes = for.** The Numeracy Act: a state maths office, 60 minutes of
  maths a day, twice-yearly screening and a maths coach in every primary school subject to funding.
- **SB 224 — social_programs_and_welfare, yes = against.** Three employer contacts a week to keep drawing
  unemployment pay, with proof and random audits.
- **Three measures carry no stance.** HB 52 (probation revocation) and HB 95 (a grace period on court
  fines after release) both sit in the contested direction inside public safety. SB 200 delays the
  Literacy Act's third-grade retention rule by two years, which is contested inside education quality.
  **HB 95's description gives no number for the grace period**, because the enrolled text prints "180-day
  90-day 180-day" and the flattened struck-and-inserted text cannot settle which figure was enacted.

## Duplicates

A precise sweep found the hand-written records that describe the same votes, and they were retired
before the import. The sweep is restricted to Alabama candidates, an exact vote date, a description
naming the same bill, and a description worded as a vote. It excludes only records whose origin run id
begins `rollcall:`, because hand-written records carry a `manual:candidate-records:...` run id and a
null-check misses them. Sponsorship records naming the same bill were left alone.

## Import and reconciliation

- Real run (stamp `2026-09-03T16:55:31.235Z`): **739 inserts, 0 errors, 0 notified**, across 17 rolls.
- Reconciled three ways: the report totals; the run-stamp predicate (739 rows, 80 distinct
  candidates); and the Alabama roll-call total, which moved from 4,890 to 7,527 across the six batches
  imported together.
- Convergence: a follow-up dry run reports all 739 `unchanged`.

## Writing checks run before import

`candidateRecordPlainLanguageLint`: 0 warnings. Every description is 2 to 4 sentences with no sentence
over 45 words, and a British-spelling scan is clean — it caught real slips on a first pass, including
`legalised`, `licence`, `behaviour`, `labour` and `programme`, all corrected. Reading grade was measured
per session; medians run 8.6 to 11.2.
