# Judging notes, South Dakota measures that did not become law

Judged from the South Dakota Legislative Research Council's own documents on
sdlegislature.gov: the bill history, the printed versions, and the amendment
documents. No sponsor material, no committee testimony, no AI provider call.

The scope is closed: 155 measures, 54 imported over five batches, 101 dropped
with the reason written out, none open. The ledger is
`dispositions-not-enacted.json`.

| batch | directory | measures | rolls | records |
| --- | --- | --- | --- | --- |
| 06 | `legiscan-sd-2170/batch-04` | 14 | 17 | 617 |
| 07 | `legiscan-sd-2170/batch-05` | 12 | 13 | 375 |
| 08 | `legiscan-sd-2231/batch-04` | 12 | 13 | 493 |
| 09 | `legiscan-sd-2231/batch-05` | 10 | 10 | 354 |
| 10 | `legiscan-sd-2231/batch-06` | 6 | 8 | 241 |
| **total** | | **54** | **61** | **2,080** |

Every batch reconciled three ways (import report inserts, records stamped with
the batch's run ids, and the table's growth), every convergence re-run came back
all unchanged, and the duplicate sweep found none.

## What each batch holds

**Batch 06, 2025.** environment_and_public_health: HB 1085, HB 1228 (for),
HB 1223 (against). civil_rights: HB 1249 (for), HB 1054, HB 1201 (against).
public_safety_and_crime_control: HB 1115, HB 1117 (for). anti_corruption:
HB 1204, HB 1242 (for). corporate_accountability: HB 1058 (for). data_privacy:
HB 1073 (for). social_programs_and_welfare: HB 1132 (for).
cost_of_living_reduction: HB 1235 (for).

**Batch 07, 2025.** civil_rights: SB 156, SB 198 (for), HB 1260, SB 51
(against). corporate_accountability: SB 157 (for), SB 177 (against).
anti_corruption: SB 201 (for). public_education_quality: SB 196 (for).
public_infrastructure: SB 202 (for). data_privacy: SB 50 (for).
election_integrity: SB 188 (against). reduce_wealth_gap: SB 67 (for).

**Batch 08, 2026.** environment_and_public_health: HB 1103, HB 1151, HB 1173
(for), HB 1068, HB 1163, HB 1171 (against). public_safety_and_crime_control:
HB 1010 (for). healthcare_affordability: HB 1105 (for). housing_affordability:
HB 1113 (for). corporate_accountability: HB 1138 (for). civil_rights: HB 1153
(against). womens_reproductive_rights: HB 1182 (against).

**Batch 09, 2026.** anti_corruption: HB 1222, HB 1246, HB 1278 (for).
immigration: HB 1209 (against). environment_and_public_health: HB 1210
(against). data_privacy: HB 1275 (for). civil_rights: HJR 5001 (for).
healthcare_affordability: HJR 5002 (against). cost_of_living_reduction: SB 118
(for). social_programs_and_welfare: SB 126 (for).

**Batch 10, 2026.** cost_of_living_reduction: SB 195, SB 196 (for).
public_education_quality: SB 198 (for), SB 85 (against). healthcare_affordability:
SB 211 (for). reduce_wealth_gap: SB 150 (for).

## How a bill's fate is written

None of these bills changed the law, so every description is in the
conditional. The closing sentences say what happened, and all of them are
derived by `sd_build_ne.py` from the dataset and the state's own history lines.
None is typed by hand. Three rules came out of reading those lines:

- **A failed vote with more yeas than nays is explained.** South Dakota passes a
  bill only with a majority of all members, 36 of 70 in the House and 18 of 35
  in the Senate, and money bills need two-thirds, 47 and 24. HB 1113 failed at
  46-20 and again at 45-21. "Rejected it, 45-21" would read as nonsense, so the
  sentence says which bar the vote missed. Six rolls carry this sentence.
- **The other chamber's word is its decisive vote, not a motion.** HB 1249's
  House description first cited a Senate tally of 16-18, which was a failed
  reconsideration; the Senate's decisive roll, the one imported, was 17-16.
  The other chamber is now read on the same kept captions as filter 4.
- **"Never voted on it" is the last resort.** The full Senate voted to table
  HB 1204 (34-0) and HB 1242 (25-8), and those descriptions now say so. Where a
  committee killed a bill, the description says a committee killed it, so the
  full chamber never voted.

These rules were found after batches 06 and 07 were first imported. The fixed
descriptions were re-judged and re-imported as rewrites of the existing
records: 296 in batch 06 and 102 in batch 07. No record was added or lost.

## Things the text said that the title did not

- **Two hoghouses.** Amendment 157C rewrote SB 157 (2025) from "an Act to
  address labor trafficking" into proof of workers' compensation insurance on
  public projects. Amendment 150D rewrote SB 150 (2026) from protecting one
  motor vehicle into raising the personal property exemption to $12,000 and
  $10,000. Each description covers the text actually voted.
- **One vehicle bill.** HB 1135 read in full "The Legislature shall provide
  opportunities for South Dakotans" when the House passed it. Dropped.
- **The House and Senate voted different versions of HB 1209.** The 50-employee
  threshold was a Senate addition. The House's version, the one imported, would
  have covered every employer.
- **HB 1085 is not a flat moratorium.** It bars a carbon dioxide pipeline permit
  only until federal safety rules are final.
- **HB 1223, SB 51 and SB 198 (2026) were changed on the floor right before the
  vote**, and each description says what the chamber actually had in front of
  it. SB 51 and SB 198 are judged once per chamber for that reason.

## Direction calls worth stating

- **Ten Commandments displays and school chaplains are civil_rights, yea
  against**, as Arkansas SB 433, North Dakota HB 1145 and HB 1456.
- **Keeping medical debt off credit reports is corporate_accountability, yea
  for**, as Washington SB 5480 and Maryland HB 1020.
- **A data center tax refund is corporate_accountability, yea against**, on the
  Delaware HB 310 precedent.
- **App store age verification is data_privacy, yea for**, as Colorado SB 51.
  The first pass had it the other way; the precedent settled it.
- **An E-Verify mandate is immigration, yea against**, as West Virginia HB 4198
  and Arkansas HB 1974. The first pass had it the other way too.
- **HB 1182 is womens_reproductive_rights, yea against.** It collects counts of
  embryos created and destroyed, not personal data, so data_privacy was wrong.
- **SB 67 went to reduce_wealth_gap.** There is still no labor research area.

## Dropped on reading the text

Ballot-access fights were dropped, not scored: HB 1169 (initiative signature
thresholds; Missouri HJR 3, the same measure, carried the barred `general`
label), HB 1220, SB 103, HB 1087 and HB 1323. SB 188 was kept because it is
different in kind, a plain narrowing of who may vote absentee.

Five more were dropped after reading: HB 1121 swaps one teen-driver exception
for another; HB 1015 is a pilot program; HB 1253 and SB 97 are formulas whose
effect runs either way by district; SB 190, a parental-rights act, sets
parents' rights against children's with no single direction.

## Version check, per roll

Every roll was judged against the version printed on or before its own date,
plus any floor amendment adopted after that print and before the vote. A failed
bill is never reprinted, so those amendments appear in no version;
`sd_lateamend.py` found 18 and each was read.

## Review response (2026-09-10) — three 2026 descriptions corrected

All three findings were checked against the LRC text and are true.

- **SB 118 (P2, true).** The Senate Taxation engrossed text, the version voted
  16-17, names the fund "as created in Senate Bill 125" and says the treasurer
  may not transfer money if SB 125 does not become effective. SB 125 passed the
  Senate 34-0 and was tabled by the House 64-3 on March 4. The description said
  "the fund exists". Now: the fund would have come from SB 125, no money could
  move unless that bill also became law, and the House later tabled it.
- **HB 1163 (P2, true).** Section 7 carves out more than federal health rules:
  facilities bound by CMS or CDC regulations, school and early-childhood entry
  vaccinations under § 13-28-7.1, clinical placements required by Board of
  Regents or Technical Education health programs, court orders, and the
  National Guard. The description now names all five.
- **HB 1151 (P2, true).** The repealed sections (§ 34-20B-115 and 115.1) bar
  sales to, purchase by, possession by and consumption by people under 21, cap
  7-hydroxymitragynine at 2% of alkaloids, ban synthetic and adulterated
  products, and require serving, content and warning labels. The description
  said the current rules "only bar sales" to under-21s. Now: bar sales to and
  use by people under 21, cap potency, ban synthetic or adulterated products
  and require warning labels.

Judge: batch 04 2 `updated` / 11 `unchanged`; batch 05 1 `updated` / 9
`unchanged`. Import rewrote 106 records in place (HB 1151 39, HB 1163 40,
SB 118 27). Dry-run re-run 493 and 354 `unchanged`, 0 errors. South Dakota
row count unchanged at 4,210.
