# Alaska batch-01

**7 measures, 8 roll calls, 36 records across 6 candidates.** Imported on the local `voteapp`
database on 2026-09-03. Production has no Alaska roll-call records.

## How the batch was chosen

The pool is the 50 divided roll calls on 31 measures that became law. Five filters ran over it.

1. **Divided.** The losing side is at least a quarter of the winning side.
2. **Consequential.** The measure became law.
3. **A nameable subject** that maps to one of the research areas.
4. **One roll per measure per chamber**, preferring the chamber's last vote on the text that
   became law. Where a chamber passed its own version and later agreed to the other chamber's
   changes, the later concurrence is the one taken.
5. **A defensible direction.** A measure that pushes both ways inside the same area is dropped
   rather than filed under a label that misdescribes why members voted.

24 of the 50 rolls were excluded by standing campaign rules before filter 5: seven
appropriations bills, and four joint resolutions. An Alaska joint resolution never goes to the
governor and is not law, so a record about one may not say it became law — the same rule
Montana and California applied.

## What is in the batch

| measure | roll | chamber | date | tally | area | yes vote |
|---|---|---|---|---|---|---|
| HB 27 medical emergencies and CPR lessons | 1700388 | House | 2026-05-19 | 27-13 | environment_and_public_health | for |
| HB 33 ethics rule for the fish and game boards | 1570725 | House | 2025-05-13 | 28-12 | anti_corruption | against |
| HB 33 ethics rule for the fish and game boards | 1689719 | Senate | 2026-04-24 | 15-4 | anti_corruption | against |
| HB 35 prisoners' computers and release ID cards | 1574429 | House | 2025-05-20 | 28-12 | public_safety_and_crime_control | for |
| HB 48 civil legal services fund ceiling | 1645404 | House | 2026-02-25 | 27-13 | social_programs_and_welfare | for |
| HB 57 school funding, class size, charters, phones | 1562693 | House | 2025-04-30 | 31-8 | public_education_quality | for |
| HB 184 development bank workforce housing lending | 1699348 | House | 2026-05-16 | 22-18 | housing_affordability | for |
| SB 183 legislative audit committee powers | 1572959 | House | 2025-05-12 | 30-10 | anti_corruption | for |

**`anti_corruption` carries both directions on purpose.** HB 33 loosens a conflict-of-interest
rule and SB 183 strengthens legislative oversight, so a yes on one and a yes on the other point
opposite ways in the same area. Only HB 33 states a nay side, because its whole content is the
ethics rule and a no vote there is a vote to keep the standard rule with nothing else to
object to. Every other measure uses `nay: null`: the realistic objection runs on a different
axis — cost, an unfunded mandate on schools, or prison security.

## What the fan-out is

Our Alaska roster covers **6 of 40 House districts and 1 of 20 Senate districts**, so a House
roll reaches 5 candidates and a Senate roll reaches 1. That is the binding constraint, not the
feed. A roster campaign is filling the rest, and re-running this import later adds those
members without duplicating anything.

## Dropped under filter 5, after reading the whole enacted Act

- **HB 173** occupational and physical therapy — a scope expansion whose objection is patient
  safety, which sits inside `healthcare_affordability`'s own words about quality care.
- **HB 195** pharmacists and physician associates — a profession rename across 60 sections, a
  pharmacist scope expansion, and new opioid safeguards that run the other way.
- **HB 302** travel insurance and unemployment benefits — two unrelated halves, and the
  insurance half both loosens and tightens the rules on insurer marketing.
- **HB 314** registered interior designers — creates a credential but leaves the practice open
  to anyone who avoids the title, and carries unrelated pipeline and wastewater exemptions.
- **SB 15** alcohol — loosens who may serve and where minors may be, while adding a cancer
  warning to the required signs.
- **SB 50** borough comprehensive plans — adds one item to a list a plan MAY include.
- **SB 95** child care — built of paired sections that undo one another, with the largest
  question handed to a federal agency.
- **SB 164** fuel and tobacco tax collection allowances — no research area fits.
- **SB 200** service areas and farm land assessment — tightens paperwork for a tax break while
  extending it to S corporations.
- **HB 75** permanent fund dividend eligibility — no area describes the dividend.
- **HB 110** — see finding 2 in `../CODE-FINDINGS.md`.

## What is left

42 of the 50 pool rolls carry a disposition in `../survey/divided-enacted-worklist.tsv` and
none is left open. The larger untouched scope is **40 divided rolls on bills the governor
vetoed** and 17 on bills that passed one chamber only — the scope Pennsylvania opened in its
batch-02 and the one that fits Alaska's divided government best.
