# California batch-09 — selection

**8 roll calls / 8 measures / 87 records.** Imported to local `voteapp` 2026-08-31. Prod untouched.

Second batch of the Senate-only tail. Rule and method unchanged from `../batch-07/PLAN.md`.

## What came through

| measure | status | area | yea | Senate |
| --- | --- | --- | --- | --- |
| AB 1050 covenants that block housing conversions | chaptered | housing_affordability | for | 31-9 |
| SB 294 Workplace Know Your Rights Act | chaptered | civil_rights | for | 29-8 |
| SB 1125 Water Rate Assistance Program | enrolled | social_programs_and_welfare | for | 31-9 |
| SB 655 safe maximum indoor temperature as state policy | chaptered | environment_and_public_health | for | 31-8 |
| AB 1108 independent death investigations in custody (FACTS Act) | chaptered | public_safety_and_crime_control | for | 29-9 |
| SB 464 employer pay data: 23 job categories, mandatory penalty | chaptered | corporate_accountability | for | 30-10 |
| AB 507 adaptive reuse: by-right conversion of buildings to housing | chaptered | housing_affordability | for | 30-9 |
| AB 260 medication abortion: labeling, immunity, coverage | chaptered | womens_reproductive_rights | for | 30-8 |

**No area is new in this batch**: `civil_rights` was first covered in batch-02. California covers
**14 of 27** research areas. (The first draft claimed 18; corrected on review, see `../batch-08/PLAN.md`.)

## Two measures dropped after a full read, and why they are worth naming

Both were dropped because a tag that misleads is worse than a record left out. (The first draft
said each would have opened a new area; only SB 1354's `national_defense` is actually uncovered.)

- **SB 403 (End of Life Option Act: removes the 2031 sunset).** The only area that fits is
  `womens_reproductive_rights`, whose description covers "individual bodily autonomy". Aid-in-dying
  is bodily autonomy in the abstract — but that area displays to voters as a **reproductive rights**
  record, and filing an assisted-dying vote there would misdescribe both the vote and the candidate.
  Dropped under filter 5.
- **SB 1354 (bars another state's military personnel from entering California without the
  Governor's permission).** `national_defense` reads "maintain military readiness and deterrence to
  protect national security interests", and a bill restricting out-of-state deployments is not
  clearly `for` or `against` that. The subject is contested federal-state ground, like the
  redistricting bills held out of every batch. Dropped under filter 5.

## The roll screens earned their keep three times

- **AB 1050's earlier vote was rescinded.** Rolls 1600940 (09-03) and 1601779 (09-10) are both
  31-9 with the same question. The official history shows the Senate **rescinded** the 09-03
  passage, amended the bill on 09-05, and passed it again on 09-10. A duplicate-tally pair can be a
  rescission, not just a feed artifact — the history is the only place that says so.
- **SB 294 is a duplicate-date twin** of the SB 53 shape: 09-12 and 09-13, both 29-8. The 09-12
  roll is the **Assembly's** third reading (67-8); the Senate concurred on 09-13.
- **SB 1125 and SB 464** each had an earlier divided roll that predates the Assembly amendments.

## Checks

- Version check on all 8; dates and screen notes in `rolls.json`.
- **Completeness audit before judging** — 68 substantive digest items after setting aside
  reimbursement and severability boilerplate. **Gaps found and closed in five of the eight.**
- Lint: 16 descriptions, 0 warnings, longest sentence 40 words.
