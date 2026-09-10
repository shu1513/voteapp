# Iowa batch-03 — plan

## Scope
Eight measures, mostly higher education and schools, plus a curriculum mandate and a bail change. 8 measures, 12 rolls, 634 candidate records. Selected from the
divided-and-enacted pool of 144 measure-chamber slots on 91 measures; every slot's
disposition is in `../survey/divided-enacted-worklist.tsv`.

## Selected
HF 295, HF 437, HF 440, HF 785, HF 2230, SF 369, SF 175, SF 2399. The reasoning for each is in `JUDGING.md`.

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
- Nothing unusual. Every selected roll passed the tally, date, held-roll and version checks on the first pass.

## Import and reconciliation
- Judge dry run 12, real run 12. Import dry run 634 inserts, real run 634 inserts, zero errors.
- Run stamp `2026-09-10T04:40:03.722Z`.
- Reconciled three ways: report total, rows matching the run stamp, and the table delta all agree.
- Duplicate sweep: hand-written records sharing a candidate and a date were checked and every
  one is about a different measure. Nothing retired.

## Review fixes (2026-09-09)
- SF 2399: the "significant weight" sentence came from the introduced bill, not the act; replaced with the enacted limit on personal-recognizance release to nonviolent, nondrug simple or serious misdemeanors (sections 1, 5 and 6). HF 437: the advisory council searches and submits finalists; the Board of Regents appoints the director (section 9). Re-judged and re-imported: 176 rewrites, 458 unchanged, ledger in `import-rerun-report.json`.
