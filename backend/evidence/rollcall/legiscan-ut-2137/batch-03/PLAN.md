# Utah 2025 General Session, batch-03

6 roll calls on 5 measures. 292 candidate records across 69 candidates.
Local database only. Production holds no Utah records.

## How the batch was chosen

The same five filters as batch-01 and batch-02, applied to the 85 rolls those
batches left marked `candidate`. Five measures came through with a clear
subject and one honest direction each. House rolls lead the batch, because a
Utah House roll reaches a median of 56 of our candidates and a Senate roll 8.

## What is in the batch

| measure | rolls | area | direction |
| --- | --- | --- | --- |
| HB 119 Solar Panel Restrictions in Homeowners Associations | House 42-31, Senate 19-10 | environment_and_public_health | for |
| HB 360 Housing Attainability | House 48-21 | housing_affordability | for |
| SB 165 Municipal Broadband Service | House 56-17 | public_infrastructure | against |
| SB 262 Housing Affordability Modifications | House 45-22 | housing_affordability | for |
| SB 297 Congregate Care | House 53-19 | social_programs_and_welfare | for |

Every label states `nay` explicitly and every one is null. On each of these the
realistic objection is cost, local control or administrative burden, which is a
different axis from the area scored, so a no vote earns no tag.

## Checks run before importing

- **Version, per roll.** All 6 were cast on the substitute that was enrolled.
  Three of the five measures were enrolled on a later substitute than the one
  their first chamber passed (HB 360 enrolled substitute 2, SB 165 substitute 2,
  SB 262 substitute 3, SB 297 substitute 5), and in each case the roll selected
  is the one cast on the enrolled text.
- **Superseded stage.** Each selected roll is the last kept floor vote for its
  measure and chamber.
- **Governor action.** All 5 measures carry a `Governor Signed` line.
- **Tally and members.** All 6 match Utah's own record on the tally, and all 6
  were cleared name by name against Utah's per-roll vote sheet, which also
  prints the question. Every question is a form of final passage.
- **Later sessions.** Every code section these five acts touch was checked
  against the 2025 first and second special sessions and the 2026 General
  Session. Nothing repeals or reverses what is described. The touches that do
  exist are a revisor's technical corrections bill, a 2026 act that adds
  rulemaking authority for the congregate care ombudsman rather than removing
  it, a 2026 reorganization that moves the housing division into the Governor's
  Office of Economic Opportunity, and three 2025 special-session recodifications
  that renumber sections without changing them.
- **Duplicates.** The importer flagged 3 related records; all three are records
  about other bills that happen to share a vote date. The wider sweep by bill
  number over every Utah record not written by this pipeline found one row, a
  sponsorship record for HB 119 on Doug Owens, its chief sponsor. Sponsoring a
  bill is a different fact from voting on it, so nothing was retired.
- **Prose.** Flesch-Kincaid grade median 8.9, worst 9.1, longest sentence 38
  words; the repository lint reports 0 warnings over all 12 descriptions.

## Reconciled three ways

- `import-report.json`: 6 rolls, 292 inserts, run stamp
  `2026-09-10T06:20:32.472Z`.
- Database on that stamp: 292 rows, 69 distinct candidates, 196 area tags.
- Convergence dry run afterwards: 292 unchanged, 0 inserts. The earlier dry-run
  stamp `2026-09-10T06:19:41.618Z` matches 0 rows.

## What was left out of this batch, and why

- **HB 110 Combined Basic Tax Rate Reduction** — deferred, not dropped. It cuts
  a property tax rate that funds schools, so the honest reading pulls two ways
  at once and needs its own read of the appropriation clause.
- **HB 479 Student Athlete Revisions** — deferred. College athlete pay is the
  same question Wyoming's SF 44 raised, which is an open operator call.
- **HB 256 Municipal and County Zoning** and **HB 368 Local Land Use** —
  deferred. Both are large land-use omnibus acts that need their own read.
- **HB 343 Cannabis Production** — deferred. The act is about odor control and
  tenancy at production sites, which is narrow.

## Left for batch-04

79 rolls still marked `candidate` in `survey/dispositions.tsv` for the 2025
session, and the whole 2026 session (123 candidate rolls) is still untouched.
The largest single item remains **HB 300 Amendments to Election Law**, 68 code
sections and 10,303 words of change.
