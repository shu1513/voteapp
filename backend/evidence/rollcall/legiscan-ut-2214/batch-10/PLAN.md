# Utah 2026 General Session, batch-10: the operator direction calls

15 roll calls on 10 measures, which is 421 candidate records across 72
candidates. Local database only; production holds no Utah records. Run with
`--state UT-2214`. Reasoning is in `JUDGING.md`.

| measure | rolls | area / direction |
| --- | --- | --- |
| HB 174 minors' cross-sex hormone ban made permanent | House 53-15, Senate 20-6 | civil_rights / against |
| HB 258 coverage to reverse a transition | House 53-16, Senate 17-9 | healthcare_affordability / for |
| HB 404 single-sex shared housing by sex at birth | House 50-20, Senate 19-7 | civil_rights / against |
| HB 209 citizenship checks and federal-only ballot | House 51-16, Senate 17-8 | election_integrity / for + civil_rights / against |
| SB 295 DEI ban extended to school clubs | House 48-16 | civil_rights / against |
| HB 259 parents' access to minors' medical records | House 56-14, Senate 22-6 | data_privacy / against |
| HB 204 student belief accommodation | Senate 17-5 | civil_rights / for |
| SB 174 provider conscience refusals | Senate 22-7 | womens_reproductive_rights / against |
| HB 136 impound for unlicensed drivers | Senate 20-6 | public_safety_and_crime_control / for |
| SB 153 voter records and dead-voter removal | Senate 22-7 | election_integrity / for |

Every label states `nay: null`.

**Dropped:** HB 118, SB 268, SB 170 (vitamin K) and HB 293. **Every 2026
candidate roll now has a final disposition; none is held.**

## Checks run before importing

- **Outcome.** Each roll's next action sends the bill on, and all ten measures
  were signed. Three Senate rolls are under 20 yeas: HB 258 (17-9, plain third
  reading), HB 209 (17-8) and HB 204 (17-5). Both suspension rolls were followed
  by `to House with amendments`.
- **Same-day rolls.** HB 174, SB 153, SB 295 and HB 136 each had a same-date
  roll. Utah's timestamps show ours is the later, decisive vote in every case,
  so each carries `acknowledge_later_rolls`. The table is in `JUDGING.md`.
- **Later acts.** HB 209 was signed after SB 194 and SB 153, and its split
  ballot is in the current 20A-3a-201.5. The other overlaps are recodification
  or cross-reference changes.
- **Duplicates.** None flagged.
- **Prose.** Flesch-Kincaid worst 9.0. The repository lint reports 0 warnings
  over all 30 descriptions.

## Reconciled three ways

- `import-report.json`: 15 rolls, 421 inserts, run stamp `2026-09-11T06:24:21.375Z`.
- Database on that stamp: 421 rows, 72 candidates, 348 area tags.
- Convergence dry run: 421 unchanged. The first dry-run stamp
  `2026-09-11T06:23:43.367Z` matches 0 rows.

## Review fixes (2026-09-11)

- HB 259: only the $1,000 daily electronic-access fine waits until after
  December 31, 2027 (26B-2-244(5)); the $1,000 per-record fine for missing the
  five-business-day deadline (26B-2-244(6)) applies from the May 6, 2026
  effective date. The sentence now separates the two.
- Re-judged and re-imported: `import-rerun-report.json` rewrote 68 rows in
  place (House 58, Senate 10), 353 unchanged, 0 lint warnings.
