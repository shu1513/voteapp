# Illinois batch-07 — measures 76 to 97 of the unbatched pool

22 measures read, **12 kept: 18 rolls / 1,314 records across 132 candidates.**
Illinois now holds **9,699 roll-call records**, local `voteapp` only.

## What was kept

| measure | rolls | area |
|---|---|---|
| HB 3097 FAFSA completion help | H 84-24 | `public_education_quality` |
| HB 3248 medically necessary laser hair removal | H 76-39, S 37-19 | `healthcare_affordability` |
| HB 3352 coerced debt | H 89-23 | `corporate_accountability` |
| HB 3363 State Public Defender Commission | H 77-36, S 36-19 | `civil_rights` |
| HB 3385 campus mental health staffing | H 91-24 | `public_education_quality` |
| HB 3409 Chemicals in Cosmetic Products Act | H 74-38 | `environment_and_public_health` |
| HB 3460 license applications from people in prison | H 75-37 | `civil_rights` |
| HB 3564 limits on up-front rental fees | H 64-40, S 39-16 | `housing_affordability` |
| HB 3566 minors named in eviction cases | H 73-38 | `housing_affordability` |
| HB 3709 campus contraception and medication abortion | H 78-39, S 37-19 | `womens_reproductive_rights` |
| HB 3751 employment social enterprises | H 76-39, S 40-19 | `reduce_wealth_gap` |
| HB 4217 public defenders in license suspension cases | H 82-28, S 43-15 | `civil_rights` |

## The rule this batch had to state out loud

**A divided roll is imported only when it is that chamber's vote on the text
that became law.** Six earlier House rolls are marked `screened:superseded-roll`
because the House voted again on the enacted Senate version, and that
concurrence is the roll in the table above.

Three more measures were lost to the same rule, and they are the expensive part:

| measure | divided roll | the House's vote on the enacted text |
|---|---|---|
| HB 3275 school cardiac emergency response | 85-27 | **115-0** |
| HB 3711 reporting sexual misconduct by health professionals | 87-26 | **116-0** |
| HB 4160 auto insurance right to appraisal | 73-38 | **115-0** |

In each, the Senate replaced the bill and the House then agreed without a real
split. Importing the earlier roll would tell a reader the chamber divided over a
law it went on to pass unanimously. The narrow exception stays what batch-04
used for HB 2987: a later vote that changes one word of a definition.

## HB 3564's first concurrence motion LOST

This is the README's third hazard, live. The House took up the Senate version of
HB 3564 twice. On 2026-02-26 the motion to concur **lost 56-36** (roll 1722461);
on 2026-04-08 a second motion carried 64-40 (roll 1722464). Only the second is a
vote on the law. The failed motion is marked `screened:failed-motion` rather than
dropped silently, because its tally looks exactly like a passing one.

## The drops

Two for a direction that cuts both ways: **HB 3638** removes the seven-year
limit on agreements barring a former employee from reapplying while keeping
other workplace transparency duties, and **HB 3800** is an insurance omnibus
spanning network adequacy, dental plans and public adjuster fees.

Five found no research area: HB 3193, HB 3200, HB 3309, HB 3662 (a
gut-and-replace whose enacted text extends tax increment financing deadlines for
eight named towns) and HB 3811. **HB 3309 is the labor gap again.**

## Reconciliation

| step | records |
|---|---|
| predicted from `crosswalk.json` | 1,314 |
| dry run | 1,314 `insert` |
| real run | 1,314 `insert` |
| re-run | 1,314 `unchanged` |
| rows in the local database | 1,314 |

Median Flesch-Kincaid grade **7.3**, worst **8.6**, no sentence over 45 words,
no British spellings.

## What is left

59 rolls of the original 204 still carry `candidate:unbatched`.
