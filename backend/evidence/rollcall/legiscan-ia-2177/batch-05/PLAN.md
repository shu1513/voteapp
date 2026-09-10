# Iowa batch-05 — plan

## Scope
Four measures on medical licensing, emergency powers, nicotine taxes and higher education programs. 4 measures, 5 rolls, 263 candidate records. Selected from the
divided-and-enacted pool of 144 measure-chamber slots on 91 measures; every slot's
disposition is in `../survey/divided-enacted-worklist.tsv`.

## Selected
SF 469, HF 2694, SF 2480, HF 2539. The reasoning for each is in `JUDGING.md`.

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
- **HF 2694's Senate roll carries the session-end date skew.** LegiScan stamps it 2026-05-02; Iowa's journal records the 31-14 vote on 2026-05-03. The batch was first imported without the override, the audit caught it, and the roll was re-judged with `official_vote_date` set to 2026-05-03 and re-imported for real. The re-run reports 15 rewrites and 248 unchanged, and the records now carry the journal's date. The audit script is now a hard gate that stops the run instead of printing a warning.

## Import and reconciliation
- Judge dry run 5, real run 5. Import dry run 263 inserts, real run 263 inserts, zero errors.
- Run stamp `2026-09-10T04:47:41.463Z`.
- Reconciled three ways: report total, rows matching the run stamp, and the table delta all agree.
- Duplicate sweep: hand-written records sharing a candidate and a date were checked and every
  one is about a different measure. Nothing retired.
