# Iowa batch-02 — plan

## Scope
Ten measures across civil rights, elections, consumer lending, public assistance and health care regulation. 10 measures, 17 rolls, 777 candidate records. Selected from the
divided-and-enacted pool of 144 measure-chamber slots on 91 measures; every slot's
disposition is in `../survey/divided-enacted-worklist.tsv`.

## Selected
SF 473, HF 2711, HF 928, HF 767, HF 2329, SF 304, SF 383, SF 2422, HF 2501, HF 2254. The reasoning for each is in `JUDGING.md`.

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
- **HF 865** (school bullying) was selected, read, and then dropped. The enacted text does not merely strike the list of protected traits, it removes the requirement that bullying be based on a trait at all, while adding that conduct must be 'repeated and targeted'. That widens coverage in one direction and narrows it in another, so no single direction is honest.

## Import and reconciliation
- Judge dry run 17, real run 17. Import dry run 777 inserts, real run 777 inserts, zero errors.
- Run stamp `2026-09-10T04:35:37.176Z`.
- Reconciled three ways: report total, rows matching the run stamp, and the table delta all agree.
- Duplicate sweep: hand-written records sharing a candidate and a date were checked and every
  one is about a different measure. Nothing retired.

## Review fixes (2026-09-09)
- HF 928: the fifteen-hundredths threshold applies only to statewide and federal offices; legislative races use one percent or fifty votes, whichever is smaller (enrolled act, section 8). SF 2422: the expenditure-neutrality rule is a default with legislative-approval and federal-compliance exceptions, not an absolute ban (section 13). Re-judged and re-imported (stamp `2026-09-10T05:44:42.259Z`): 112 rewrites, 665 unchanged, ledger in `import-rerun-report.json`.
