# Wisconsin batch-11: the died pool, economy and taxes

Judged 2026-09-11. Local database only; production holds no Wisconsin roll-call
records. No AI calls.

Every bill here passed one chamber on a closely divided roll and never passed
the other before the session's last floor period ended. See batch-10 JUDGING.md
for how the died pool was defined and split.

## Kept: 4 of 14

| measure | what it would have done | area | yes means |
| --- | --- | --- | --- |
| AB 38 | state income tax subtraction for tips deducted federally, 2025-2028 | personal_income_tax_reduction | for |
| AB 164 | tighter weekly work search and resume rules for unemployment claimants | social_programs_and_welfare | against |
| AB 996 | a state $1,000 match for each Wisconsin-born child's Trump account | social_programs_and_welfare | for |
| SB 287 | E-Verify for state and local hires and for state contracts of $50,000 or more | immigration | against |

Every no side is `null`. Each call follows an earlier ruling:

- **AB 38** follows AB 461 and SB 36 in batch-06, which copied the federal tips
  and overtime deductions into state tax.
- **AB 164** follows AB 167 and AB 169 in batch-06: rules that act on claimants
  and narrow who receives benefits, or how easily, are against the safety net.
- **AB 996** follows Oklahoma HB 4071, a state deposit into Trump accounts. Like
  that act, AB 996 set aside no money, and the description says so.
- **SB 287** follows AB 281 in batch-04, the vetoed E-Verify bill.

AB 38, AB 996 and SB 287 were voted as substitute amendments. Each description
is written from the substitute, read through LegiScan's `getAmendment` call.

## Dropped: 10

- **AB 652**, unemployment insurance: it raises the top weekly benefit from
  $370 to $395 and lets people get disability payments and jobless benefits
  together, but also cuts off a week of benefits when an employer reports a
  skipped interview or job offer. Inside `social_programs_and_welfare` it points
  both ways.
- **AB 870**, as voted, is three bills in one: federal tax code conformity,
  creditor rights against members of limited liability companies, and a license
  for local government heavy-equipment operators. No single label describes it.
- **AB 878**, a tax credit for local sales taxes paid on arcade and other
  amusement devices. No research area describes it.
- **AB 997** turns AB 996's funding into an open-ended appropriation.
  Appropriations are excluded by standing rule.
- **SB 179**, both rolls. The Senate voted to let fuel suppliers keep part of the
  diesel tax and give stations a refund for evaporation; no area describes fuel
  tax allowances. The Assembly then voted a substitute that added a heavy-equipment
  license taking effect only if local employees regain collective bargaining. The
  two chambers voted different bills, and neither fits one area.
- **SB 337**, grants of up to $5,000 for small towns to hire grant writers.
  `public_infrastructure` is reachable but misdescribes a capacity grant; the
  SB 283 lesson from batch-01 is to drop rather than file under the nearest slug.
- **SB 467** would have required a referendum for every local wheel tax,
  including ones already in place. It is tax relief to some readers and a cut to
  road money to others, and no area describes local taxes.
- **AB 411**, a farmland matching program. No research area describes it.
- **AB 315** and **AB 612** both renew the land stewardship program through 2028
  while cutting what it may spend on land. Renewing and cutting point opposite
  ways inside `environment_and_public_health`, and both are bonding measures.

## Reconciliation

- Plain-language lint over 8 descriptions: 0 warnings, longest sentence 40
  words.
- Judge: 4 updated. Import dry run: 264 inserts. Real run at
  `2026-09-11T21:16:21.602Z`: `outcomes {imported: 4}`, `actions {insert: 264}`,
  0 notified.
- 154 yes-side records, each with one tag. 110 no-side records, no tags.
- Duplicate sweep: no live hand-written record describes these four votes.
- All live Wisconsin roll-call records: 6,114 over 115 rolls and 99 candidates,
  with 3,758 tags. That includes batch-10, which is on its own pull request.

Review correction (2026-09-11), AB 164: the first descriptions put the
counseling trigger at "fewer than three weeks" and made the resume rule apply to
every claimant. The bill's Section 26 (new s. 108.04 (15) (ao)) fires when the
remaining entitlement is "3 or less times" the weekly benefit rate, so exactly
three weeks qualifies; Section 20 (new s. 108.04 (2) (a) 4. c.) requires the
resume only "if the claimant resides in this state". The LRB analysis says
"less than three weeks", but the statutory text governs. Descriptions now read
"three weeks or fewer" and "Claimants living in Wisconsin". The initial-week
carve-out on the resume rule is not mentioned: the paragraph is already about
weekly duties, and the one-week lag adds nothing a voter would weigh. Judge:
1 updated, 3 unchanged. Import rewrote all 84 AB 164 records in place (dry run
`import-dry-run-rerun-report.json`, real run `import-rerun-report.json`,
`actions {rewrite: 84, unchanged: 180}`). Live totals unchanged. Direction
stands: the bill tightens claimant duties, so a yes vote is against
`social_programs_and_welfare`.
