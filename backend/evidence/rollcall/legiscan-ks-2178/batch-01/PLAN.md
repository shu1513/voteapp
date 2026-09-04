# Kansas batch-01 — selection

12 measures, 12 Kansas House roll calls, **880 records across 75 candidates**.

## Why these are all House votes

Only the Kansas House is on the November 2026 ballot. Senate terms run to 2028, so
our database holds no Kansas Senate candidates, and the crosswalk resolved **zero
matched members on every one of the 665 Senate rolls**. A Senate roll would write
no records at all. House rolls reach a median of 73 candidates each.

## The filters, in order

1. **Divided** — the losing side is at least a quarter of the winning side. This
   standard gate fits Kansas without adjustment: a party-line House vote runs about
   88-37, well clear of the threshold. Measured before assuming it, because Kentucky
   needed a different gate.
2. **Enacted** — the measure became law. 280 of the 387 divided rolls qualify,
   158 of them in the House, on 96 measures.
3. **A nameable subject that fits a research area.**
4. **One roll per measure per chamber**, taking the chamber's last kept floor vote so
   the record reflects the member's final position on the enacted text.
5. **A defensible for-or-against direction.** A measure that runs both ways is dropped
   rather than filed under the nearest area.

## What the batch is about

Kansas pairs a Republican supermajority legislature with a Democratic governor, and an
override needs two thirds of each chamber. The legislature overrode Governor Laura
Kelly 69 times this biennium. Nine of these twelve measures became law that way, so the
batch is mostly a record of where each representative stood when the legislature
overruled the governor.

| measure | roll | date | question | tally | area | yea |
| --- | --- | --- | --- | --- | --- | --- |
| SB 63 Help Not Harm Act | 1478079 | 2025-01-31 | passage | 83-35 | civil_rights | against |
| SB 4 advance ballot deadline | 1526411 | 2025-03-25 | veto override | 84-41 | election_integrity | for |
| HB 2062 child support from conception | 1543283 | 2025-04-10 | veto override | 87-38 | womens_reproductive_rights | against |
| HB 2382 fetal development video | 1543322 | 2025-04-10 | veto override | 84-41 | womens_reproductive_rights | against |
| SB 244 shared restrooms | 1637133 | 2026-02-18 | veto override | 87-37 | civil_rights | against |
| HB 2437 SAVE Kansas Act | 1679846 | 2026-04-09 | veto override | 84-39 | election_integrity | for |
| SB 269 income tax rate cuts | 1544084 | 2025-04-10 | veto override | 87-37 | personal_income_tax_reduction | for |
| HB 2101 local guaranteed income ban | 1495761 | 2025-02-20 | passage | 86-37 | social_programs_and_welfare | against |
| HB 2054 campaign contribution limits | 1529499 | 2025-03-27 | conference report | 72-52 | anti_corruption | against |
| HB 2033 dyslexia reading programs | 1543277 | 2025-04-10 | veto override | 87-38 | public_education_quality | for |
| SB 30 legislative review of job licenses | 1679944 | 2026-04-09 | veto override | 88-35 | government_efficiency | for |
| HB 2109 utility liability shield | 1524944 | 2025-03-24 | concurrence | 78-44 | corporate_accountability | against |

Nine areas, none with any prior Kansas coverage. `civil_rights` and
`womens_reproductive_rights` each carry two measures.

## Where the rest of the pool went

All 158 House divided-and-enacted rolls carry a disposition in
`../survey/house-divided-enacted-worklist.tsv`:

| disposition | rolls |
| --- | --- |
| batch-01 | 12 |
| not-selected (filter 4) | 61 |
| candidate for batch-02 | 76 |
| superseded | 7 |
| held, bad tally | 2 |
