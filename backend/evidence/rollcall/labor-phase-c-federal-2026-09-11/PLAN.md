# Federal labor measures reopened (labor phase C)

**9 roll calls, 9 measures, 1,218 records, 246 candidates, 731 area tags.**

These federal votes were left pending in earlier runs because no research area
covered labor or union rights. H.R. 2550 carries the note "no labor research
area exists" in `expansion-119-1/survey/dispositions.tsv`. The 117th Congress
votes sit as `unjudged` in `backfill-118-117/survey/divided-worklist.tsv`. The
`labor_rights` area now exists (migration 277, PR #1294), so each was reread
and judged under the scope rules in `labor-rights-backfill-2026-09-10/NOTES.md`.

| measure | roll | tally | a yes vote is |
| --- | --- | --- | --- |
| H.R. 842, PRO Act (union rights) | House 117-1 roll 70 | 225-206 | for |
| H.R. 1195, workplace violence standard for health care | House 117-1 roll 118 | 254-166 | for |
| S.J.Res. 29, cancel the OSHA vaccine-or-test standard | Senate 117-1 roll 489 | 52-48 | against |
| H.R. 2499, federal firefighter workers' compensation | House 117-2 roll 149 | 288-131 | for |
| H.R. 903, TSA screener bargaining rights | House 117-2 roll 172 | 220-201 | for |
| H.R. 6087, nurse practitioners in federal workers' compensation | House 117-2 roll 233 | 325-83 | for |
| H.R. 302, limits on moving jobs out of the civil service | House 117-2 roll 432 | 225-204 | for |
| H.R. 1948, VA clinician bargaining | House 117-2 roll 530 | 219-201 | for |
| H.R. 2550, restore federal worker bargaining | House 119-1 roll 332 | 231-195 | for |

Every label is `labor_rights` with `nay: null`. None became law: each passed
one chamber and the other never voted, so each roll is the only floor vote on
its text. H.R. 2550 is still pending in the Senate.

## Considered and left out

- H.R. 3992 (older job applicants), H.R. 2062 and S.J.Res. 13 (EEOC
  conciliation): employment discrimination, which the scope rules leave to
  civil rights.
- H.R. 447 (National Apprenticeship Act): apprenticeship is out of scope.
- H.J.Res. 100 (2022 rail agreement): it imposed the contract and its raises
  but barred a strike, so it moves both ways.
- H.R. 5339 and H.J.Res. 30 and S.J.Res. 60 (retirement plan investing),
  H.R. 5342 (Social Security offsets): pension rules, out of scope.
- H.R. 1156 and H.R. 1163 (unemployment fraud): general unemployment rules.
- H.R. 497, H.R. 139: vaccine mandate and telework, not labor rights as scoped.
- Omnibus bills (American Rescue Plan, INVEST, Build Back Better): many
  unrelated parts.

## Grounding

The earlier federal runs wrote judgments from congress.gov CRS summaries. These
judgments were written from the passed text itself (House engrossed or Senate
engrossed, from govinfo.gov), which is the text each roll voted on.

## Commands

`rollcall:fetch` (all nine came back `unchanged`), `rollcall:judge`,
`rollcall:resolve`, `rollcall:import --dry-run`, then `rollcall:import`, all
with `--legislators-sha 750c0608efb6ef1fc3257ba72c99af3771d35088` and
`DATABASE_URL=postgresql://localhost:5432/voteapp` inline.
