# Iowa batch-04 — plan

## Scope
Five measures on health care, corrections, teacher licensing and employment verification. 5 measures, 8 rolls, 374 candidate records. Selected from the
divided-and-enacted pool of 144 measure-chamber slots on 91 measures; every slot's
disposition is in `../survey/divided-enacted-worklist.tsv`.

## Selected
HF 571, HF 516, HF 398, HF 2724, SF 2218. The reasoning for each is in `JUDGING.md`.

## The five filters, as applied

1. **Divided**: the smaller side is at least a quarter of the larger, measured on the stored
   rolls so the dataset's nine re-issued ids are not counted twice.
2. **Became law**: LegiScan status 4, and the builder refuses any measure whose history has no
   `Signed by Governor` line.
3. **Nameable subject**: 30 slots on appropriations, school-funding-formula and state-finance
   bills are set aside; the rest were read.
4. **One roll per measure per chamber, on the text that became law.** Iowa chambers usually
   accept the other chamber's amendments by voice vote, so a recorded vote often predates the
   final text. Each roll is kept only when no amendment adoption or conference report follows
   it, ordered by journal page inside a day.
5. **Stance-defensible**: one label per policy strand, and `nay` null throughout. Iowa's
   majorities are lopsided enough that a no vote evidences no single position.

## Audits run before importing
- Every roll matches Iowa's own journal tally line.
- No roll in the batch is one of the three held rolls.
- Any roll carrying the session-end date skew has an `official_vote_date` override.
- The plain-language lint reports zero warnings, and a British-spelling scan runs over the
  descriptions and these documents.

## Notes on this batch
- **HF 1003** (child placements) was in the first draft of this batch and the judge refused it. Iowa's House adopted the Senate amendment 57-28 and then passed the bill 85-0 on the same day. The vote on the text that became law is not divided, so the measure leaves the pool. This exposed a real defect in the worklist builder, which had kept each chamber's last DIVIDED roll instead of its last roll. The rule was corrected to take the chamber's final roll and keep the slot only if that roll is divided. Re-running the corrected rule over the whole session changed exactly one slot, HF 1003, and every roll already imported stayed valid.

## Import and reconciliation
- Judge dry run 8, real run 8. Import dry run 374 inserts, real run 374 inserts, zero errors.
- Run stamp `2026-09-10T04:45:48.520Z`.
- Reconciled three ways: report total, rows matching the run stamp, and the table delta all agree.
- Duplicate sweep: hand-written records sharing a candidate and a date were checked and every
  one is about a different measure. Nothing retired.
