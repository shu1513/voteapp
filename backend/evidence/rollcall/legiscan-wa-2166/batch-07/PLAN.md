# Washington batch-07

Eight measures, sixteen roll calls, 850 records across 108 candidates. Local database
only. Production holds no Washington records.

| measure | chapter | area | direction |
| --- | --- | --- | --- |
| House Bill 1974 — land banks | 233 L 26 | housing_affordability | for |
| House Bill 2039 — delays child support pass-through | 402 L 25 | social_programs_and_welfare | **against** |
| House Bill 2040 — delays end of disability aid repayment | 403 L 25 | social_programs_and_welfare | **against** |
| House Bill 2165 — posing as a peace officer | 114 L 26 | public_safety_and_crime_control | for |
| House Bill 2242 — preventive care and vaccine coverage | 13 L 26 | healthcare_affordability | for |
| House Bill 2355 — domestic workers | 15 L 26 | labor_rights | for |
| House Bill 2411 — shared leave for hate crime and immigration hardship | 241 L 26 | labor_rights | for |
| House Bill 2548 — hospital deal notices | 222 L 26 | corporate_accountability | for |

No measure carries an authored nay. House Bills 2039 and 2040 carry a **yes = against**
label: each delays a benefit for people on public aid that an earlier law had already
enacted.

## Roll selection

Filter 4 takes each chamber's last kept floor vote. House Bills 1974, 2165 and 2548 end in
a House vote to accept the Senate's changes.

⚠ **House Bill 2040 had two House votes on the same day**, 57-39 on third reading and then
56-39 on reconsideration. Same-day rolls cannot be ordered by date, and the judge refused
each one in turn because of the other. The reconsideration vote is the House's final word,
so it is the roll judged, with the earlier roll listed in `acknowledge_later_rolls` and a
note in the judgments file. The survey worklist had picked the earlier roll; the judge's
check is what caught it.

## Duplicates

**None.** The tally sweep found nothing. The bill-number sweep found 23 records: sponsorship
records for these acts, and records about **EHB 2242 of 2017**, the McCleary school funding
act, which reuses the number. Also reused: SHB 1974 of 2024, HB 2411 on suicide prevention
training and SHB 2355 on imaging technologists. All left alone.

## Dropped after reading

House Bill 2047 (ends the employee ownership tax credit early; fiscal machinery) and House
Bill 2445 (probate safeguards aimed at individual administrators; no area reaches them)
were planned keeps and were dropped. Reasons are in the worklist.

## Reconciliation

- Dry run planned 850, the real run inserted 850, and the database holds 850 under the run
  stamp `2026-09-11T05:41:34.565Z`. Reconciled three ways.
- The dry run's own stamp matches **zero** rows.
- Convergence re-run and second dry run: all 850 `unchanged`.
- 108 candidates, 0 errors, 0 notified.

A first attempt judged the wrong House roll for House Bill 2040. The judge rejected it, so
nothing was approved and every import pass reported `not_approved` with zero writes. The
ledgers here are from the corrected run only.

## Writing

Median grade 9.0, worst 9.4, longest sentence 33 words. The pipeline's own plain-language
lint reports **0 warnings** over all 32 descriptions.
