# Utah 2025 General Session, batch-10: the operator direction calls

11 roll calls on 8 measures, which is 234 candidate records across 69
candidates. Local database only; production holds no Utah records. Run with
`--state UT`. The 2026 half is in `legiscan-ut-2214/batch-10`. Reasoning for
every call, imported or dropped, is in `JUDGING.md`.

| measure | rolls | area / direction |
| --- | --- | --- |
| HB 267 public-sector union bargaining ban (repealed Dec 2025) | House 42-32, Senate 16-13 | labor_rights / against |
| SB 327 companion to HB 267 | Senate 19-5 | labor_rights / against |
| HB 233 abortion providers barred from school health lessons | House 52-13, Senate 18-8 | womens_reproductive_rights / against |
| HB 390 college student groups | Senate 21-6 | civil_rights / for |
| HB 226 immigration notice before release | Senate 21-8 | immigration / against |
| HB 252 gender care in state custody | Senate 19-6 | civil_rights / against |
| HB 269 sex-designated college housing | Senate 20-7 | civil_rights / against |
| HB 300 mail-ballot ID digits and opt-in mail from 2029 | House 56-15, Senate 19-10 | election_integrity / for |

Every label states `nay: null`.

**Dropped, with reasons in `survey/dispositions.tsv`:** HB 81 (fluoride), HB 77
(flags), HB 281, SB 73, HB 209 (homeschool) and HB 479. **Every 2025 candidate
roll now has a final disposition; none is held.**

## Checks run before importing

- **Outcome.** Each roll's next action in Utah's action list sends the bill on,
  and every bill carries a `Governor Signed` line.
- **Version and members.** All rolls were checked earlier on the enrolled text
  and cleared name by name against Utah's vote sheets.
- **Same-day rolls.** None of the 2025 rolls has a same-day twin.
- **Later acts.**
  - HB 267 was repealed by 2025S2 HB 2001, which the description states.
  - HB 300's return-envelope ID digits and 2029 opt-in mail are still in the
    current code (20A-3a-202 and 20A-3a-204), after the 2026 election acts.
  - The other overlaps are recodifications.
- **Duplicates.** The importer flagged none. The only hand-written match is
  Jordan Teuscher's HB 267 sponsorship record, which is not a vote.
- **Prose.** Flesch-Kincaid worst 9.0 across both halves. The repository lint
  reports 0 warnings over all 22 descriptions.

## Reconciled three ways

- `import-report.json`: 11 rolls, 234 inserts, run stamp `2026-09-11T06:24:17.632Z`.
- Database on that stamp: 234 rows, 69 candidates, 164 area tags.
- Convergence dry run: 234 unchanged. The first dry-run stamp
  `2026-09-11T06:23:36.442Z` matches 0 rows.
