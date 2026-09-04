# Alabama 2021 Second Special Session batch-01 — judging notes

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

- **The four maps — general, no stance.** HB 1 (Congress), HB 2 (State House), SB 1 (State Senate) and
  SB 2 (State Board of Education). No research area describes drawing legislative boundaries, and this is
  the fourth time the campaign has reached that conclusion, after Missouri's 2025 session and Alabama's
  2023 and 2026 special sessions. **None of the four Acts states any criterion for how the lines were
  drawn** — no mention of population equality, compactness, communities of interest, race or the Voting
  Rights Act — and the descriptions say so, because that absence is the most notable fact about them.
  The congressional map is the one the Supreme Court held likely unlawful in Allen v. Milligan, and the
  description of HB 1 says so and notes the 2023 redraw.
- **SB 9 and SB 15 — environment_and_public_health, yes = against.** SB 9 requires employers with a
  vaccine mandate to grant medical or religious exemptions, with a presumption favouring the employee and
  full pay during an appeal. SB 15 requires a parent's written consent before a minor may be vaccinated
  against COVID-19, overriding the general rule that a 14-year-old may consent to their own care, and
  bars schools from asking a minor about vaccination status. Both reduce vaccination uptake, which is the
  area's subject. The objection runs on religious liberty and parental rights, a different axis, so nay
  is null.
- **Both SB 9 and SB 15 were judged from the CONFERENCE COMMITTEE SUBSTITUTE**, not the engrossed print,
  because the imported roll in each case is the House vote on the conference report. This is the rule the
  2024 gaming package taught: a conference vote is a vote on the conference substitute, which LegiScan
  files under `amendments[]`. SB 15's enrolled PDF has no text layer at all, so the substitute was the
  only readable source for the enacted text.

## Duplicates

A precise sweep found the hand-written records that describe the same votes, and they were retired
before the import. The sweep is restricted to Alabama candidates, an exact vote date, a description
naming the same bill, and a description worded as a vote. It excludes only records whose origin run id
begins `rollcall:`, because hand-written records carry a `manual:candidate-records:...` run id and a
null-check misses them. Sponsorship records naming the same bill were left alone.

## Import and reconciliation

- Real run (stamp `2026-09-03T16:55:18.924Z`): **387 inserts, 0 errors, 0 notified**, across 9 rolls.
- Reconciled three ways: the report totals; the run-stamp predicate (387 rows, 79 distinct
  candidates); and the Alabama roll-call total, which moved from 4,890 to 7,527 across the six batches
  imported together.
- Convergence: a follow-up dry run reports all 387 `unchanged`.

## Writing checks run before import

`candidateRecordPlainLanguageLint`: 0 warnings. Every description is 2 to 4 sentences with no sentence
over 45 words, and a British-spelling scan is clean — it caught real slips on a first pass, including
`legalised`, `licence`, `behaviour`, `labour` and `programme`, all corrected. Reading grade was measured
per session; medians run 8.6 to 11.2.
