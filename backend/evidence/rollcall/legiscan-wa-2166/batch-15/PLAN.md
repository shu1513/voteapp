# Washington batch-15 — not-enacted Senate bills, part two

Twelve Senate bills, twelve roll calls, 227 records across 19 candidates. Local database
only. Production holds no Washington records.

Every measure passed the Senate on a divided vote, and the House never held a floor vote
on it. None became law. Wording follows batch-13: conditional bodies and a dated tail.

| measure | Senate vote | subject | area, yes = |
| --- | --- | --- | --- |
| Senate Bill 5360 | 27-22 | three-tier environmental crimes | environment_and_public_health, for |
| Senate Bill 5466 | 29-20 | state electric transmission authority | public_infrastructure, for |
| Senate Bill 5469 | 29-19 | ban on algorithmic rent-setting | housing_affordability, for |
| Senate Bill 5490 | 29-19 | searches of transgender and intersex people in jail | civil_rights, for |
| Senate Bill 5496 | 29-19 | limits on corporate buying of single-family homes | housing_affordability, for |
| Senate Bill 5576 | 27-21 | local short-term rental tax for affordable housing | housing_affordability, for |
| Senate Bill 5581 | 30-19 | shared-use paths in highway planning | public_infrastructure, for |
| Senate Bill 5647 | 38-11 | real estate tax exemption for affordable homeownership | housing_affordability, for |
| Senate Bill 5701 | 30-19 | certified payroll records for labor-management committees | labor_rights, for |
| Senate Bill 5708 | 36-12 | addictive feeds and minors online | data_privacy, for |
| Senate Bill 5797 | 26-21 | tax on financial assets above $50 million | reduce_wealth_gap, for |
| Senate Bill 5800 | 35-14 | $7 billion in highway bonds | public_infrastructure, for |

## Duplicates

None. The tally sweep found no hand-written record of any of these rolls.

## Reconciliation

- Dry run planned 227, the real run inserted 227, and the database holds 227 under the run
  stamp `2026-09-11T21:20:07.303Z`. Reconciled three ways.
- The dry run's own stamp `2026-09-11T21:20:04.848Z` matches **zero** rows.
- Convergence re-run and second dry run: all 227 `unchanged`.
- 19 candidates, 0 errors, 0 notified.

## Corrections (2026-09-11)

Review caught two wording errors, both fixed in `judgments.json` and re-applied with
`rollcall:judge` (2 updated) and a convergence re-run (38 `rewrite`, 189 `unchanged`):

- Senate Bill 5708: the passed text (§3) bars addictive feeds to minors outright; parental
  consent only unlocks push notifications in restricted hours (§4). The old sentence
  implied consent unlocked feeds too.
- Senate Bill 5360: negligence is "usually" a gross misdemeanor; a negligent release of
  hazardous air pollution that creates imminent danger is a class C felony (§9(1)(b)).

## Writing

Median grade 8.8, worst 12.1 (Senate Bill 5466), longest sentence 28 words. The builder
caught "the act" in Senate Bill 5360's first draft and it was reworded.
