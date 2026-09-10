# Hawaii 2025 Regular Session — batch 01

## Pool

Every floor passage vote in the 2025 dataset was stored (1,797). Keeping the last kept vote per
chamber per measure gives 1,247 chamber slots. Of those, 7 rolls on 6 measures are both closely
divided (the smaller side at least a quarter of the larger) and on a measure that became law,
read from the bill's own `Act N` history line. The worklist in `../survey/divided-enacted-worklist.tsv`
lists all 7 with a disposition.

The pool is small because the Democratic supermajority passes party-line bills far outside the
quarter gate: 347 of the 534 enacted final readings in 2025 were unanimous.

## Selection filters

1. Closely divided.
2. Became law.
3. A subject a voter would recognize.
4. One roll per measure per chamber, the chamber's last floor vote, which is the vote on the
   enacted draft. The draft named on the passage line was checked against the bill's text
   versions for every roll.
5. A research-area label with a defensible direction.

## Kept (5 rolls, 4 measures)

| measure | chamber | vote | draft voted | area | yes | no |
| --- | --- | --- | --- | --- | --- | --- |
| HB 137 | Senate | 15-10, 2025-03-31 | HD 1 (unamended by the Senate; the enacted text) | public_safety_and_crime_control | for | null |
| SB 1433 | House | 36-13, 2025-04-30 | CD 1 | environment_and_public_health | for | null |
| SB 897 | House | 39-10, 2025-04-30 | CD 1 | corporate_accountability | against | null |
| SB 897 | Senate | 20-5, 2025-04-30 | CD 1 | corporate_accountability | against | null |
| SB 97 | Senate | 18-7, 2025-05-02 | CD 2 | public_safety_and_crime_control | for | null |

HB 137 has no final reading: the Senate passed the House draft unchanged, so its third reading is
its last vote, and the House's own third reading (47-1) was not divided.

## Dropped under filter 5 (2 rolls, 2 measures)

- **SB 935** (House 39-10) — changes the pension formula for judges first hired after June 30,
  2031 (1.75 percent of average final pay per year instead of 3 percent, and class H instead of
  class A membership) and funds a $300,000 study of shorter vesting for tier 2 members. No
  research area covers public-employee pension design; the Alaska HB 78 drop is the precedent.
- **HB 1194** (House 34-16, the closest House vote in the pool) — makes midwife licensing
  permanent, sets scope-of-practice, continuing education, peer review and data rules, gives
  certified midwives prescribing authority, and narrows the licensing exemptions (the unlicensed
  birth-attendant exemption ends; Native Hawaiian traditional practices are carved out; anyone a
  patient invites may attend a birth but may not practice midwifery or use the title). It became
  law without the Governor's signature (Act 28). Tighter standards against narrower access is one
  contested axis inside health care, the same shape that dropped Montana HB 218 and Alaska HB 173.
  Expect to be asked.

## Why every "no" side is null

Each measure's realistic objection runs on an axis the scored area does not cover (mandatory
minimums, protest rights, cost to ratepayers, drug policy), so a no vote is not evidence the member
opposes the area's goal.

## Result

Real import 2026-09-10T00:03:38.012Z: 5 rolls imported, 62 records inserted, 30 candidates, 44
area tags (yes side only), 0 notifications, 0 related flags. Convergence dry run: 62 unchanged.
