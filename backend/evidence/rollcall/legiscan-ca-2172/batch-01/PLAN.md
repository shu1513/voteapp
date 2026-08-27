# California batch-01 — selection

**20 roll calls / 10 measures / 298 records.** Imported to local `voteapp` 2026-08-27. Prod untouched.

## The five filters

Applied in order to the session's 5,281 stored floor votes:

1. **Divided** — `LEAST(yeas,nays) >= GREATEST(yeas,nays)/4` and `nays > 0`. 5,281 → **972**.
2. **Became law** — LegiScan `status = 4`, each one re-checked against the chaptered text on
   leginfo (chapter number, governor's approval date). 972 → **442 rolls on 227 measures**.
3. **Nameable subject** — a measure a voter can recognise, mapping to one research area.
4. **One roll per measure per chamber, preferring final action.** In California that means the
   LAST divided floor vote each chamber took: the origin chamber's concurrence in the other
   chamber's amendments, and the second chamber's third reading. Both are votes on the text that
   went to the governor.
5. **Stance-defensible** — a research-area label with an honest for/against direction, or the
   measure is dropped rather than imported as `general`.

## What came through

| measure | area | yea | Assembly | Senate |
| --- | --- | --- | --- | --- |
| SB 79 transit-oriented housing | housing_affordability | for | 43-19 | 21-8 |
| SB 627 law enforcement masks | public_safety_and_crime_control | for | 45-23 | 28-11 |
| AB 495 Family Preparedness Plan | immigration | for | 60-20 | 29-10 |
| SB 580 AG immigration model policies | immigration | for | 58-19 | 30-10 |
| SB 704 firearm barrels | gun_control | for | 57-20 | 29-10 |
| AB 325 Cartwright Act / pricing algorithms | corporate_accountability | for | 54-18 | 29-9 |
| AB 692 stay-or-pay employment terms | corporate_accountability | for | 46-20 | 25-11 |
| AB 1415 health care affordability oversight | healthcare_affordability | for | 51-19 | 26-10 |
| AB 1319 endangered species backstop | environment_and_public_health | for | 51-19 | 25-10 |
| SB 42 public campaign financing | anti_corruption | for | 59-20 | 29-8 |

Every one of the ten is a 2025 chaptered statute. **All ten carry a stance** — none fell back to
`general`, which is the point of filter 5.

Note how the picture inverts Texas: California's divided-and-enacted set produces
`gun_control`/**for** and `immigration`/**for** where Texas produced the same areas against. The
direction follows the research area's own description, not the bill's framing.

## Dropped under filter 5 after a full read

- **AB 1078 (Firearms)** — runs both ways in one text. It tightens concealed-carry disqualifications
  (out-of-state convictions, restraining orders, unlawful drug users) **and raises the one-gun-a-month
  purchase limit from 1 to 3**, exempts locked boxes on public transit, and restores rights after
  certain vacated out-of-state felonies. The Texas SB 11 / HB 521 precedent: two directions in one
  text → `general` → not imported.
- **SB 477 (FEHA enforcement procedures)** — extends tolling for complainants, but also defers a
  right-to-sue notice until a related group or class complaint is fully disposed of, which cuts the
  other way for the individual. No honest single direction.
- **AB 930 (Elections and voting procedures)** — extends the mail-ballot receipt deadline from 3 to 7
  days (access) while tightening recount procedure and voting-system access (integrity). Mixed.
- **AB 1249 (Early voting: satellite locations)** — a clean access expansion, but `election_integrity`
  is defined as "secure, accurate, auditable, and trusted", and expanded early voting is not that
  claim. Dropped rather than forced.
- **SB 825 (Consumers: financial protection)** — real but thin: one clarification that the
  commissioner may enforce the deceptive-practices rule against licensed escrow agents and finance
  lenders. Held for a later batch.

## Hazards handled in this batch

- **SB 42 sits on the duplicate-date defect** (`../CODE-FINDINGS.md` §1). Its Senate concurrence
  appears twice, 1602271 (09-12) and 1602930 (09-13), same 29-8 lineup; the bill history records one
  concurrence, on 09-13, so **1602930** is the pick.
- **SB 627's Senate concurrence happened twice for real.** 1601937 concurred 27-10, reconsideration
  was granted (1601938, now excluded by rule), and 1601939 concurred 28-11. The pick is **1601939**,
  the vote that actually sent the bill to the governor.
- **SB 42 is not fully in force.** It amends the voter-approved Political Reform Act, so its
  public-financing provisions take effect only if voters approve them on **November 3, 2026** — which
  the descriptions say outright. Voters reading a candidate's record will see this measure on their
  own ballot.

## Version check — all 20 votes were cast on the enrolled text

Every pick's date falls after its bill's last `Amended` version date, so no description needs a
"the Senate voted an earlier version" caveat. Widest margin: AB 1249 (dropped anyway). Tightest:
AB 1415, last amended 2025-08-21, Senate vote 09-04.

| measure | last amended | Assembly vote | Senate vote |
| --- | --- | --- | --- |
| SB 79 | 09-05 | 09-11 | 09-12 |
| SB 627 | 09-05 | 09-09 | 09-11 |
| AB 495 | 09-05 | 09-11 | 09-10 |
| SB 580 | 09-04 | 09-09 | 09-10 |
| SB 704 | 09-02 | 09-08 | 09-09 |
| AB 325 | 09-05 | 09-12 | 09-11 |
| AB 692 | 09-05 | 09-11 | 09-10 |
| AB 1415 | 08-21 | 09-08 | 09-04 |
| AB 1319 | 09-05 | 09-11 | 09-10 |
| SB 42 | 09-03 | 09-12 | 09-13 |

## Left for later

**422 divided-and-enacted rolls on ~217 measures**, plus 530 divided rolls on measures that did not
become law (69 of them vetoed). Rich seams not touched here: criminal procedure (AB 572, SB 281,
AB 1071), climate and energy (SB 127, SB 57), consumer and market bills (SB 763, SB 825), and the
redistricting fight (ACA 8, AB 604) — the last of which is a `general` problem, not a stance.
