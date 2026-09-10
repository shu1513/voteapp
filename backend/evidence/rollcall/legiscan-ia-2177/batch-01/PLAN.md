# Iowa batch-01 — plan

## Scope
The divided-and-enacted pool: 145 measure-chamber slots on 92 measures (see
`../survey/divided-enacted-worklist.tsv`). This batch takes 10 measures and 18 rolls.

## The five filters, as applied
1. **Divided**: the smaller side is at least a quarter of the larger. Applied to the stored
   rolls, so the four re-issued duplicate ids are not counted twice.
2. **Became law**: LegiScan status 4, confirmed by a `Signed by Governor` line in every
   selected bill's history. The builder refuses a measure without one.
3. **Nameable subject**: 34 slots on appropriations, school-funding-formula and state-finance
   bills are set aside as `appropriations`. Everything else was read by title and the
   Legislative Services Agency explanation.
4. **One roll per measure per chamber, on the text that became law.** Iowa chambers agree to
   the other chamber's amendments by voice vote, so a chamber's recorded vote often predates
   the final text. Each roll was checked against the bill history, ordered by journal page
   inside a day: a roll counts only if no amendment adoption or conference report follows it.
   Two slots held two rolls on one day and the passage vote was kept:
   - SF 615 House: the concurrence vote (55-31, roll 1572629) then passage (56-30, roll
     1572630). Passage judged, concurrence acknowledged.
   - SF 579 Senate: the concurrence motion (29-16, roll 1657971) then passage (29-16, roll
     1657970). Passage judged, motion acknowledged.
5. **Stance-defensible**: one label per policy strand, `nay` null throughout (the no votes
   evidence no single position in a legislature this lopsided).

## Selected
SF 418, HF 856, SF 615, HF 2788, HF 924 (Senate only), HF 2296, SF 579, HF 2527, HF 706
(Senate only), HF 299. Reasoning per measure is in `JUDGING.md`.

## Dropped under filter 5, with reasons
- **SF 2426** (English proficiency test for commercial drivers): reads two ways on its own
  text, road safety against immigrant drivers' access to work. No single honest direction.
- **SF 140** (satellite absentee voting sites on school property during school-money votes):
  voting access and keeping electioneering away from polling sites pull opposite ways.

## Audits
- Tally: every one of the 18 rolls matches Iowa's own history line exactly.
- Date: none of the 18 is among the six rolls with the sine-die date skew.
- Held rolls: none of the three held rolls is in this batch.
- Version: for each roll, no text-changing action follows it in the bill history.
- Text: descriptions were written from the enrolled act as served by legis.iowa.gov, with
  struck and added text resolved from the drawn lines in the HTML (a tag strip keeps both the
  old and the new words). The resolver was validated on HF 924 and SF 579 before use.
- Lint: 0 warnings over 36 descriptions; longest sentence 38 words; no British spellings.

## Import and reconciliation
- Judge dry run 18, real run 18 `updated`; `legislative_votes` IA approved = 18.
- Import dry run 769 inserts, real run 769 inserts, zero errors, run stamp
  `2026-09-09T23:47:36.357Z`.
- Three ways: report 769 = run-stamp rows 769 (102 candidates, 578 tags) = table delta 0 to
  769. Per-roll counts identical between dry and real runs.
- `import-report.json` is the importer's own file and is preserved as written.
- Re-run 2026-09-10 after review: HF 2788 descriptions wrongly said anyone who dispenses
  abortion drugs outside a health care setting can be sued; section 146F.4 makes chapter 148/155A
  licensees immune, so only unlicensed dispensers face the civil action. Judge updated 2 rolls
  (16 unchanged); import rewrote 92 records, 677 unchanged, zero errors. Reports in
  `import-dry-run-rerun-report.json` and `import-rerun-report.json`.
- Duplicate sweep: nine hand-written records share a candidate and a date with a batch record,
  and every one is about a different measure (amendment offers, SF 607, HF 189, HF 2292, a
  committee roll). No duplicates; nothing retired.
