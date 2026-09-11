# Oklahoma batch 04

Six measures, nine roll calls, 262 candidate records. Local database only. Production holds
no Oklahoma roll-call records.

| measure | rolls | tally | area | direction |
| --- | --- | --- | --- | --- |
| SB 942 discrimination in schools and colleges | House, Senate | 56-29, 31-15 | civil_rights | for |
| SB 991 state definition of anti-Semitism | House, Senate | 62-24, 27-15 | civil_rights | for |
| SB 250 tips counted toward the minimum wage | House | 74-19 | reduce_wealth_gap | against |
| SB 2084 cap on wrongful firing settlements at state colleges | House | 70-21 | government_spending_reduction | for |
| SB 998 electric plant costs charged to customers | House, Senate | 51-39, 26-20 | corporate_accountability | against |
| SB 658 foster and adoptive parents' religious beliefs | House | 68-19 | civil_rights | against |

This is the first Oklahoma batch with more than one roll on the same subject: SB 942 and
SB 991 both adopt the same definition of anti-Semitism, by different routes, on different
days, with different tallies. They are separate acts and are kept apart.

## Three measures were read in full and dropped

- **SB 680, heated tobacco.** The act pulls products meant to be heated rather than burned
  into the cigarette stamp tax, then exempts half of that tax. A voter could read it as
  taxing a product that escaped the tax, or as writing a discount for one. Neither direction
  is safe, so it is dropped under filter five.
- **SB 641, motor vehicle repair.** The act caps what a body shop may bill and requires
  insurers to reimburse those set rates. It runs both ways: a cap protects the payer, a
  mandatory rate protects the shop. Dropped.
- **SB 1365, central purchasing.** The act exempts the Tourism Department from state
  purchasing rules for gift shop stock up to $75,000. The subject is too narrow for a voter
  to recognise, and less bidding can be read either as saving time or as weakening oversight.
  Dropped. Its House roll also carries a reconsideration the next day, which is worth a look
  before anyone puts it back in the pool.

## Checks run before importing

- **Version check.** Every measure was diffed where two texts exist. SB 942 came back at
  0.968, SB 998 at 0.976, SB 658 at 0.984, SB 250 at 0.971 and SB 2084 at 0.985. Every
  difference is a page header or the emergency section, which is described below. SB 991 has
  only one text in the feed, and neither chamber amended it.
- **The emergency sections.** SB 942 and SB 998 were engrossed with an emergency clause and a
  July 1, 2025 effective date. Both emergencies failed in the House, so the enrolled acts drop
  the clause and take effect on the ordinary date. The substance the chambers voted on did not
  change, and no description turns on an effective date.
- **Reading level**, measured before the import: median Flesch-Kincaid grade 7.5, worst 8.6.
  The first draft measured 9.9 and was rewritten into shorter sentences.
- **The repository's plain-language lint**: 18 descriptions, 0 warnings.
- **British spellings**: the builder wrote on the first attempt.
- **Related records**: four flags, all hand-written records about a different bill acted on
  the same day, May 6, 2025. Nothing retired.

## Result

Dry run planned 262 inserts; the real run inserted 262 with no errors and nobody notified,
under the stamp `2026-09-10T06:19:20.621Z`. The convergence run reports all 262 unchanged.
Oklahoma now holds 945 records locally across four batches.
