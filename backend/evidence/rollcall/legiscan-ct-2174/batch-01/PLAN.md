# CT batch-01 — selection

**17 rolls / 12 measures / 10 House + 7 Senate.** Every measure became law (LegiScan status 4) and
every roll is that chamber's decisive passage vote, verified against the bill-status action trail.

## The five filters

1. **Divided** — `least(yeas,nays) >= greatest(yeas,nays)/4`. 177 of 768 stored floor rolls qualify.
2. **The roll is the chamber's decisive passage vote.** Connecticut's Senate desc does not name the
   question, so this is the filter that does the most work here: of the 105 divided rolls on enacted
   measures, **51 are Senate votes on floor amendments that failed** and 2 more are adopted
   amendments that look exactly like passage (SB 3's Senate roll 182, 25-10, beside its roll 184
   passage, 25-10). Each of the 17 kept rolls was matched to a named `… Passed …` line in the trail.
3. **Consequential** — became law.
4. **Nameable subject** mapping to a research area.
5. **A defensible for/against direction.** Anything that would land on `general` was dropped.

## Kept

| measure | chambers | rolls | Public Act | label |
|---|---|---|---|---|
| HB 7042 firearm industry responsibility | H 100-46, S 25-11 | 1562360, 1581993 | PA 25-43 | `gun_control` / for |
| HB 7066 school–immigration procedures | H 94-49, S 25-9 | 1498074, 1498834 | PA 25-1 | `immigration` / for |
| SB 9 environment, climate, planning | H 115-33, S 28-8 | 1580476, 1572213 | PA 25-33 | `environment_and_public_health` / for |
| HB 5004 environment and renewable energy | H 98-47, S 26-10 | 1564141, 1584618 | PA 25-125 | `environment_and_public_health` / for |
| SB 3 consumer protection and safety | H 112-34, S 25-10 | 1581896, 1572263 | PA 25-44 | `corporate_accountability` / for |
| SB 1 students, schools, special education | H 101-45 | 1582627 | PA 25-93 | `public_education_quality` / for |
| SB 1444 commercial-to-residential conversion | H 105-43 | 1584471 | PA 25-164 | `housing_affordability` / for |
| SB 1234 library e-book license terms | H 106-38 | 1574203 | PA 25-9 | `corporate_accountability` / for |
| SB 1328 no private state prisons | H 109-37 | 1583345 | PA 25-32 | `public_safety_and_crime_control` / for |
| SB 1542 handcuffs on children under 14 | H 99-49 | 1584485 | PA 25-163 | `civil_rights` / for |
| HB 6913 long-term care nondiscrimination | S 26-10 | 1576161 | PA 25-17 | `civil_rights` / for |
| SB 1358 nonprofit human services rates | S 28-8 | 1580725 | PA 25-151 | `social_programs_and_welfare` / for |

**All twelve directions are `for`.** That is the shape a Democratic trifecta produces — the same
mirror-image California showed, and the opposite of Texas, where `immigration` and `gun_control`
scored `against`. The direction still follows the AREA DESCRIPTION, not the party: CT's HB 7066 is
`immigration`/for because it makes the system more humane, by the same rule that made TX SB 8
`immigration`/against.

## Dropped under filter 5 (both run two ways)

- **SB 1405** campaign finance / SEEC powers (H 92-46, PA 25-26). It widens disclaimer duties to text
  messages — but it also cuts the share of committees SEEC may audit from 50% to 20% and subjects the
  commission's declaratory rulings, advisory opinions, and guidance to new restrictions. Under
  `anti_corruption` ("transparency, ethics rules, and **enforcement**") the two halves point opposite
  ways. The TX SB 11 / HB 521 precedent.
- **SB 1396** earned wage advances (H 101-46, PA 25-155). It exempts advances under $750 from the
  small-loan law's 36% APR cap — and caps their fees at $4 per advance and $30 a month, and adds
  disclosure, income-verification, free-option and collection-practice duties. Consumer protection and
  consumer exposure in one text.

## Deferred, reasons recorded

All 50 divided decisive passage rolls carry a disposition in
`../survey/divided-enacted-worklist.tsv`: 17 batch-01, **13 `candidate:unbatched`** (real batch-02
material — HB 7259 criminal justice, SB 1187, SB 1221, SB 1312, HB 6930, SB 1367, SB 1377, HB 7163,
HB 7231), 8 out-of-scope nomination confirmations, 7 appropriations, 3 no-defensible-stance,
2 local/narrow.

Also outside the gate entirely, and worth knowing before anyone asks: the divided pool holds 72 more
rolls on measures that did NOT become law, and the whole 2026 session (LegiScan 2244) is unsurveyed.

## Version and vehicle-bill checks

Every LegiScan title was compared to the enacted Act's title in the OLR Public Act Summary:
**12 of 12 match. No Connecticut vehicle-bill substitution in this batch** (contrast GA SB 33, IL's
structural gut-and-replace, TN SB 1603).

Every roll was checked for what text that chamber actually voted, against the action trail:

- **HB 5004 is the one measure whose chambers voted 34 days apart** (House 2025-05-01, Senate
  2025-06-04). The trail shows the Senate adopted House Amendment Schedule A and then passed the bill
  as amended by it — the same text the House passed. No divergence.
- HB 7042, SB 9, SB 3, SB 1, SB 1444, SB 1234, SB 1358, HB 6913: the second chamber adopted the first
  chamber's amendment schedule and passed that text. Both rolls are on the enacted text.
- HB 7066 passed both chambers on the same emergency-certified text, with every floor amendment
  rejected. The governor line-item vetoed §§ 9-11 (FY 25 appropriations) afterwards; the descriptions
  say so.
- **SB 1542 is a version split, and it resolves itself.** The Senate passed the original text 35-0 on
  05-20 (unanimous, so outside the divided gate), the House amended it with House Amendment A and
  passed 99-49 on 06-03, and the Senate re-passed as amended 36-0 on 06-04. Only the House roll is
  divided, and it is on the enacted text.
- SB 1328 was passed by both chambers with no amendments at all.
