# CT batch-02 — selection

**7 rolls / 4 measures / 4 House + 3 Senate.** Every measure became law. Every roll is that
chamber's decisive passage vote, checked against the bill-status action trail.

Batch-01 left 13 divided decisive rolls on 9 measures marked `candidate:unbatched`. **Five of the
nine measures are dropped here under filter 5**, which is why this batch is small. That is the
filter working, not a shortage of votes.

## Kept

| measure | chambers | Public Act | label |
|---|---|---|---|
| Senate Bill 1187 — sewer liens on owner-occupied homes | H 84-65, S 23-11 | PA 25-150 | `housing_affordability` / for |
| Senate Bill 1221 — state-run retirement savings program | H 102-38, S 25-11 | PA 25-30 | `reduce_wealth_gap` / for |
| Senate Bill 1312 — unemployment and workplace rules | H 96-42, S 25-11 | PA 25-117 | `social_programs_and_welfare` / for |
| Senate Bill 1367 — bail agents at hospitals, schools, houses of worship | H 93-55 | PA 25-25 | `public_safety_and_crime_control` / for |

`reduce_wealth_gap` is new for Connecticut. Senate Bill 1221 extends retirement accounts to home
care aides paid through state programs, which is asset building for low-wage workers — the area's
own words.

**Every nay side is null**, so no voter is tagged for a no vote in this batch. Each measure has a
plain reason to vote no that is not opposition to the area's goal, and on Senate Bill 1367 that
reason sits INSIDE the same area: a member may think bail agents need this reach to bring people
back to court, which is itself a justice-system argument. The reasoning per measure is in
`judgments.json` beside each label.

## Dropped under filter 5

- **House Bill 7259** (criminal justice, H 96-51 / S 25-11, PA 25-29). The clearest drop in the
  batch: it runs several ways at once. It expands civil immigration detainers to cover 13 more
  crimes and widens who counts as a law enforcement officer, AND creates a right to sue a town that
  violates the detainer law. It also cuts the penalty for a first failure to appear, excludes stun
  guns from "deadly force", and changes chokehold rules. No single direction is honest.
- **House Bill 6930** (Social Equity Council, H 114-30, PA 25-137). Administrative: a code of
  ethics and staff training for the council, cannabis licence renewal fee rules, and a 120-day
  deadline for loan decisions. No area is its core.
- **Senate Bill 1377** (Department of Transportation, H 110-34, PA 25-65). A 60-plus section
  grab-bag — a state coordinate system, parking rules, motorcycle and child bicycle helmets, e-bike
  rebates, ride-share drivers. The 110-34 split could be about any of them.
- **House Bill 7231** (Sunday hunting on private land, H 101-45, PA 25-138). No research area fits
  hunting access. `environment_and_public_health` is about air, water, climate and community
  health, not hunting days.
- **House Bill 7163** (special education emergency grants, S 26-7, **Special Act 25-1**). Money
  only: it moves $40 million from the General Fund to a special education account if the state
  projects a surplus. The campaign's standing rule is that a spending vote carries no honest
  direction. This differs from batch-01's Senate Bill 1, which paired its funding with an
  ombudsperson's office, a family guide, training rules and a study.

## Version and vehicle-bill checks

All four LegiScan titles match their enacted Act titles: **no vehicle-bill substitution**.

Every roll is on the enacted text. In each measure the second chamber adopted the first chamber's
amendment schedule and then passed it, so both rolls describe the same bill. Senate Bill 1187 has
the widest gap between chambers — Senate 2025-05-07, House 2025-06-04, 28 days — and the House
adopted Senate Amendment Schedule A before passing, so no text diverged.
