# Nevada batch-02 — the second pass over the enacted pool

## Scope

Batch-01 took the ten marquee measures. This batch works the 71 rolls the survey worklist had
left marked `candidate:unbatched` — 56 measures that became law and cleared the divided gate.

Of those 56: **11 measures and 14 rolls are imported here**, 14 measures were dropped, 4 were
excluded as appropriations, and 27 measures carrying 35 rolls survive triage and wait for
batch-03. Every one of the 71 rolls now carries a disposition and a reason in
`../survey/divided-enacted-worklist.tsv`.

## What is in this batch

| measure | area | chamber(s) |
| --- | --- | --- |
| AB 89 strip searches of children in juvenile facilities | civil_rights | Assembly |
| AB 96 heat plan in a county master plan | environment_and_public_health | Assembly + Senate |
| AB 194 balloon release ban | environment_and_public_health | Assembly + Senate |
| AB 241 apartments by right on commercial land | housing_affordability | Assembly + Senate |
| AB 527 cameras on school buses | public_safety_and_crime_control | Senate |
| SB 76 fund for victims of securities fraud | corporate_accountability | Senate |
| SB 88 prison medical debt on release | reduce_wealth_gap | Senate |
| SB 183 child welfare caseworker caseload cap | social_programs_and_welfare | Assembly |
| SB 188 language access in health care | civil_rights | Senate |
| SB 284 a foster child's own benefit money | social_programs_and_welfare | Senate |
| SB 442 public reporting of utility shutoffs | corporate_accountability | Assembly |

## Triage, before any act was read

Nine measures were set aside on the title and digest alone, because no research area describes
them: the legislative fiscal-note process (AB 249), retirement service credit for school
district employees (AB 232), county officer pay (SB 116), collective bargaining procedure
(SB 161), apprenticeship registration (SB 285), court reporter fees (SB 191), and internal
governance of the Department of Indigent Defense Services (SB 407); plus two measures that only
order a study (AB 457, SB 319). Four appropriations bills were excluded (SB 132, SB 458,
SB 488, SB 500).

Nevada still has **no labor or union research area**, which is why SB 161 and SB 285 go the
same way SB 443 went in batch-01. That gap has now cost this campaign three measures.

## Five measures dropped after reading the act

| measure | reason |
| --- | --- |
| AB 458 solar on affordable housing | Four strands. The core enables shared solar on affordable rental housing, but section 17 rewrites a different program — the expanded solar access program — to **remove** businesses, nonprofits and non-low-income residents from eligibility, while enlarging community solar. Opposite directions of comparable weight. |
| SB 177 discipline of homeless and foster pupils | Deletes the presumption that homelessness was not a factor and adds a mandatory 10-day review, but moves the extra findings so they guard only suspensions of **more than** five days, and creates a 45-day alternative placement extendable with no outer limit. |
| SB 277 school social workers | Three strands. Adds a staffing duty limited to Clark County and to "the extent that money is available", while granting districts immunity from harassment suits. |
| AB 250 coerced debt | A coerced-debt defense that helps debtors, plus a credit-instrument presentment presumption that helps creditors. The Assembly's only roll also predates both later amendments. |
| SB 330 skilled nursing terminology | Harmonizes one defined term across local ordinances. No defensible for-or-against direction. |

## Fan-out

The crosswalk maps 42 of Nevada's 63 legislators. An Assembly roll reaches roughly 30
candidates and a Senate roll roughly 11, which is why 14 rolls produce 265 records — a far
larger yield per roll than most states in this campaign.

## Ledgers in this directory

`judgments.json`, `judge-report.json`, `import-dry-run-report.json`, `import-report.json`,
`import-rerun-report.json`, and the stored roll evidence. Unlike batch-01, every report here is
the importer's own file in full form, because standard output was sent to `/dev/null` rather
than into this directory. See `../batch-01/JUDGING.md` for why that matters.
