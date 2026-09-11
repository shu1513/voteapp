# Utah 2026 General Session, batch-09

12 roll calls on 11 measures. 203 candidate records across 72 candidates.
Local database only. Production holds no Utah records. Run with `--state UT-2214`.

| measure | rolls | area | direction |
| --- | --- | --- | --- |
| HB 48 Criminal and Juvenile Justice Changes | House 52-15, Senate 20-7 | public_safety_and_crime_control | for |
| HB 54 Boating Safety Education | Senate 20-5 | public_safety_and_crime_control | for |
| HB 185 Carbon Credit Amendments | Senate 19-6 | environment_and_public_health | against |
| HB 188 Juvenile Justice Amendments | Senate 21-6 | public_safety_and_crime_control | for |
| HB 273 Classroom Technology Amendments | Senate 17-9 | ai_regulation | for |
| HB 331 Criminal Offenses Modifications | Senate 21-6 | public_safety_and_crime_control | for |
| HB 419 Environmental Legal Action Amendments | Senate 15-5 | environment_and_public_health | against |
| HB 436 Moderate Income Housing Infrastructure Amendments | Senate 20-8 | housing_affordability | for |
| HB 539 Public Safety Modifications | Senate 19-6 | public_safety_and_crime_control | for |
| HB 540 Judicial Transparency and Information Access Amendments | Senate 20-6 | anti_corruption | for |
| SB 58 Public School Attendance Amendments | House 48-22 | public_education_quality | for |

Every label states `nay: null`.

## Not imported

- **SB 26 Pest Control Amendments** is dropped because its strands conflict. It
  deletes the rule that a pesticide user is liable only if negligent. It also
  lets permit holders with a yearly video course apply weed killer under
  indirect supervision, and it drops the owner qualification for a pesticide
  business.
- **SB 251 Local Health Department and DEQ Coordination** is dropped as not
  recognizable. It sets how the environment department repays local
  governments from penalty money.
- **SB 153 Election Amendments goes to you as a direction call.** It makes most
  voter records that were private public from May 25, 2026, unless the voter
  qualifies as at risk. It also limits who may get the voter list and what they
  may use it for, and makes misuse a class A misdemeanor. Privacy moves both
  ways, so the direction is yours to call.

## Checks run before importing

- **Version, per roll.** All 12 were cast on the enrolled text.
- **Outcome.** Nine of the Senate rolls use the suspension caption, and three
  of them are under 20 yeas: HB 185 (19-6), HB 273 (17-9) and HB 539 (19-6).
  Each roll's outcome was read from Utah's action list. The next action, one
  second later, sends the bill on (`to House` or `signed by President`), and
  every bill was signed. HB 419's roll is a plain third reading.
- **Governor action.** All 11 measures carry a `Governor Signed` line.
- **Tally and members.** All 12 match Utah's own record and were cleared name
  by name against Utah's vote sheets.
- **Same-day rolls.** HB 48's two rolls were in different chambers, so the
  same-day guard does not apply.
- **Later acts in the same session.** HB 436's section `10-21-202` was also
  amended by HB 68, and HB 273's section `53E-4-202` by HB 312. Both were
  signed later. Utah's current code still holds the HB 436 text (the 2.5%
  growth test) and the HB 273 text (the AI and screen-time standards).
  HB 502 also amends `53G-6-206`, which SB 58 touches, but only its grading
  clause, which does not conflict. The other overlaps are recodification,
  cross-reference or coordination clauses.
- **Duplicates.** None flagged by the importer. The bill-number sweep found 58
  Utah records that name one of these numbers. All are 2025 House Bill 251, a
  different bill in a different session.
- **Prose.** Flesch-Kincaid grade median 8.3, worst 9.0. The repository lint
  reports 0 warnings over all 24 descriptions.

## Reconciled three ways

- `import-report.json`: 12 rolls, 203 inserts, run stamp `2026-09-10T16:43:17.988Z`.
- Database on that stamp: 203 rows, 72 distinct candidates, 145 area tags.
- Convergence dry run afterwards: 203 unchanged. The first dry-run stamp
  `2026-09-10T16:42:30.947Z` matches 0 rows.
- Review fixes to HB 331 (the house-of-worship felony needs a religious
  purpose) and HB 540 (audio recording skips justice courts), see `JUDGING.md`:
  `import-dry-run-rerun-report.json` planned 19 rewrites and 184 unchanged, and
  `import-rerun-report.json` (stamp `2026-09-11T03:55:13.395Z`) applied them.
  SB 58 keeps its wording: the excused-absence carve-out from funding
  withholding is a narrow detail, and "may hold back" already reads as
  discretionary.

## The 2026 pool is closed

Every candidate roll in 2026 now has a written disposition. What remains is
only the operator direction calls, now 14 for 2026: HB 174, HB 258, HB 404,
HB 209, SB 268, SB 295, SB 170, HB 259, HB 204, SB 174, HB 118, HB 136, HB 293
and SB 153.
