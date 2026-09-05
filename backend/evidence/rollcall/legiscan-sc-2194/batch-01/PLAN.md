# South Carolina batch-01 — selection

**6 rolls, 6 measures, 601 records, 116 candidates.** All six are House votes.

## How the batch was chosen

The five filters, in order:

1. **Divided.** The losing side is at least a quarter of the winning side. That gate leaves 70 of
   the session's 902 kept floor votes.
2. **Enacted.** The measure became law. That leaves 37 rolls on 21 measures.
3. **A nameable subject** that maps to one of the research areas.
4. **One roll per measure per chamber**, preferring the chamber's last vote, which in South
   Carolina is the vote on the text that became law.
5. **A defensible direction.** The measure is kept only if a research area carries an honest
   for-or-against reading of a yes vote. Anything that would need a no-stance label is dropped.

A sixth condition is specific to this state: **a Senate roll writes no records**, because South
Carolina senators serve four-year terms and were last elected in 2024. Fourteen of the 37 rolls
are Senate rolls, and six measures are divided only in the Senate. Those six can never reach a
voter through this campaign and are marked `no-fan-out` in the worklist rather than dropped on
their merits.

## The six measures

| Measure | Roll | Date | Tally | Question | Area | Yes vote |
| --- | --- | --- | --- | --- | --- | --- |
| H 3276 South Carolina Hands-Free and Distracted Driving Act | 1567547 | 2025-05-07 | 77-37 | concurrence | public_safety_and_crime_control | for |
| H 4216 income tax rates and deductions | 1659394 | 2026-03-10 | 71-49 | concurrence | personal_income_tax_reduction | for |
| H 4756 South Carolina Student Physical Privacy Act | 1683203 | 2026-04-15 | 77-31 | concurrence | civil_rights | against |
| S 171 waste tires and unsafe used tires | 1563446 | 2025-05-01 | 79-23 | passage | environment_and_public_health | for |
| S 214 State Commission for Community Advancement and Engagement | 1567973 | 2025-05-07 | 76-31 | passage | civil_rights | against |
| S 287 electronic nicotine delivery system regulation | 1615312 | 2026-01-21 | 66-33 | passage | environment_and_public_health | for |

`civil_rights` carries both directions in this batch only in the sense that both civil-rights
measures score `against`; there is no South Carolina measure in the pool that scores
`civil_rights` for.

## Version check, per roll

Each roll was compared with the ratified act, using the printed version's own change marking.
Every difference is a section heading the act adds above text the version already carried.

| Measure | Version voted | Similarity to the act |
| --- | --- | --- |
| H 3276 | `3276_20250430` | 0.9794 |
| H 4216 | `4216_20260224` | 0.9850 |
| H 4756 | `4756_20260401` | 0.9957 |
| S 171 | `171_20250501` | 0.9968 |
| S 214 | `214_20250506` | 0.9822 |
| S 287 | `287_20260121` | 0.9984 |

Two differences are cosmetic and worth recording. H 4756's printed version reads "must provide an
accommodation for a single-user restroom and changing facility or an accommodation", a drafting
stutter the act cleans up without changing the duty. S 287 moves one "the".

## Date and tally audit

Every roll was checked against the bill page's own history. All six match on both the date and
the tally:

- H 3276, 2025-05-07, "Concurred in Senate amendment and enrolled", Yeas-77 Nays-37
- H 4216, 2026-03-10, "Concurred in Senate amendment and enrolled", Yeas-71 Nays-49
- H 4756, 2026-04-15, "Concurred in Senate amendment and enrolled", Yeas-77 Nays-31
- S 171, 2025-05-01, "Read second time", Yeas-79 Nays-23
- S 214, 2025-05-07, "Read second time", Yeas-76 Nays-31
- S 287, 2026-01-21, "Read third time and sent to Senate", Yeas-66 Nays-33

No date override was needed.

## Superseded-stage check, run before any description was written

A single query over the stored votes asked, for each selected roll, how many kept floor votes sit
on the same measure and chamber on or after its date. The answer was zero for all six, so no
judgment needed `acknowledge_later_rolls`.

## Dropped under filter 5, after reading the whole act

- **S 62, Education Scholarship Trust Fund** — raises the household income ceiling for the state
  voucher program from 300 to 500 percent of the federal poverty guidelines, raises the
  participant cap, and adds lottery money. School-choice financing carries no honest direction
  under the standing decision made for Texas SB 2, Georgia SB 82, Pennsylvania SB 315 and North
  Carolina H87. This is the batch's largest omission and the session's most-voted measure; expect
  to be asked about it.
- **S 508, monuments** — widens the list of protected monuments, bars renaming a street or park
  named for a historic figure, requires a joint resolution of the General Assembly to move any
  monument, and lets heritage groups sue with attorney's fees. The direction is contested and no
  research area describes it.
- **H 3557, candidate filing** — shortens the filing window and lets parties charge a
  certification fee, which narrows ballot access, but also creates a procedure to reopen filing
  when a candidate dies or withdraws and tells officials to construe an error in favor of access.
  The strands run both ways. `election_integrity` is about security and accuracy, not access.
- **H 3558, Article V convention commissioners** — sets qualifications, an oath, a $200 gift cap
  and a felony for a commissioner who exceeds the state's instructions, which reads as ethics
  enforcement, but also gags commissioners from speaking to the press, which reads against
  transparency. Both strands sit inside `anti_corruption`.
- **H 4270, evictions** — removes eviction filings from the public index seven years after final
  disposition. `housing_affordability` is about supply and cost burdens; this is eviction
  procedure and has no supply or cost effect. That is the exact reading that forced the Maryland
  HB 767 retraction.
- **H 4902, athlete name, image and likeness records** — shields individual payments and
  negotiation documents from public disclosure while making aggregate revenue-sharing totals
  expressly public. Transparency moves in both directions inside one act.
- **S 832, university campus events** — bars a county or city from applying zoning to an event on
  a state college campus, while requiring 30 days' notice to nearby neighborhood associations for
  large outdoor events. No research area fits.
- **S 933, legislative compensation** — sets legislator pay and in-district allowance at a
  combined $47,500, adjusted every two years by up to 5 percent. At 59-48 this is the closest
  House vote in the pool, but the direction is contested and `government_spending_reduction` is
  about controlling spending growth generally, not a legislature's own pay. **See the note below:
  under the current label rule there is no way to record a divided vote without a stance.**
- **H 4025, the appropriations act** — the annual budget, excluded by the campaign's standing
  rule against scoring a budget vote.

## A change of practice worth flagging

Earlier campaigns imported a salient divided vote under `general` with no stance when no area
carried an honest direction: Ohio H.B. 116, Florida SB 700, Missouri SB 4 and HJR 3, Alabama HB 2
and SB 5, Maine LD 613 and Pennsylvania's cannabis batch all took that route. The label rule
recorded on 2026-09-02 closes it: `general` is a non-selectable research area whose tag is hidden
from every legislative view, so it must not go on a roll-call record.

That rule turns three of the drops above into outright omissions rather than no-stance imports:
S 933 legislative pay, S 508 monuments and H 3558 the Article V commissioners. Each is a real,
divided, enacted vote that a voter might want to see. If the rule should be revisited, these are
the measures it costs.
