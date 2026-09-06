# South Carolina batch-03 — the three measures the no-stance rule had closed off

The run state listed three measures as open questions: the `general` label rule
had closed the no-stance route, and that cost S 933, S 508 and H 3558, each
dropped `filter-5` for want of a defensible direction. **The user reopened that
route on 2026-09-06**: import with a neutral description and no stance
(`general`, no `yea` or `nay` key).

| measure | roll | tally | act | what it did |
|---|---|---|---|---|
| S 933 | house 1697896 | 59-48 | Act 200 | sets legislator pay at $15,000 plus a $32,500 in-district allowance, adjusted for inflation every two years, capped at 5% |
| S 508 | house 1698173 | 79-28 | Act 224 | widens which monuments may not be moved; only the General Assembly may act, by joint resolution; a registered nonprofit may sue to stop a forbidden action |
| H 3558 | house 1711985 | 74-27 | Act 247 | sets who may serve as a South Carolina commissioner at an Article V convention, and their appointment, oath and duties |

Every act number, ratification and veto date was read from
`scstatehouse.gov/sess126_2025-2026/bills/<n>.htm`, not from the dataset.

## Each imported roll is the House's vote on the act that became law

This is the rule the Illinois campaign had to state, and it decides all three:

- **S 508** had two divided House rolls. The 74-28 passage vote came before the
  conference committee; the **79-28 conference report** is the House's vote on
  the ratified text. The earlier roll is `screened:superseded-roll`.
- **H 3558** was ratified on 2026-05-14, **vetoed on 2026-05-20**, and became law
  only when the House overrode 74-27 and the Senate followed 20-5 on 2026-06-25.
  The override is the vote that made it law; the March 2025 passage roll is
  `screened:superseded-roll`.
- **S 933** has a single House roll.

The Senate rolls on all three stay `no-fan-out`: the South Carolina Senate holds
four-year terms last elected in 2024, so no senate roll matches a candidate.

## H 5683 cannot be imported, and the direction on it cannot be carried out

The user's direction covered South Carolina's congressional redistricting bill
alongside Tennessee's. **H 5683 never became law.** The House passed it 74-36 and
74-37 on 2026-05-20; the Senate read it a second time on 2026-05-23 and then
**continued it 26-18 on 2026-05-26**, which ends a bill for the session. The bill
page carries no ratification and no act number.

It therefore sits outside the divided-and-enacted scope entirely and never
entered this worklist. If it is to be recorded at all it belongs to the
**non-enacted pool** opened for Nevada, Connecticut and Missouri in #1193 — as a
South Carolina scope that does not exist yet.

## Reconciliation

| step | records |
|---|---|
| dry run | 296 `insert` |
| real run | 296 `insert` |
| re-run | 296 `unchanged` |
| rows in the local database | 296 |

South Carolina now holds **897 roll-call records**, local `voteapp` only.
Production still has none.
