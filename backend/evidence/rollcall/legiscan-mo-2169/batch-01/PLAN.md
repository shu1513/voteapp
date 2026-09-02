# Missouri batch-01 — selection

**10 roll calls / 7 measures (7 House + 3 Senate). Original import: 583 records across 92
candidates. After the certified-roster expansion: 719 records across 115 candidates.**

## How the pool narrowed

| step | left |
|---|---|
| roll calls in the LegiScan 2169 dataset | 557 |
| stored (446 floor + 1 surfaced + 52 excluded-question) | 499 |
| kept floor votes | 446 |
| divided (`min(yea,nay) >= max(yea,nay)/4`) **and** on a measure that became law | 83 rolls / 35 measures |
| measures with a divided **House** roll (the Senate reaches 3 candidates, the House ~81) | 20 |
| minus appropriations (HB 2, HB 12) and the 13 senate-only appropriations measures | 18 |
| minus measures whose only divided House roll is a **previous-question motion** (HB 199, SB 105, SB 43, SB 348) | 14 |
| minus filter 5 — no defensible for/against direction (7 measures, below) | **7** |

## The five filters

1. **Divided** — the phase-2 gate, a losing side of at least a quarter of the winning side.
2. **Consequential** — the measure became law, or (HJR 73) reached the ballot.
3. **Nameable subject** that maps to a research area.
4. **One roll per measure per chamber**, preferring the vote on the text that became law
   (Truly Agreed To And Finally Passed, or the conference substitute).
5. **A defensible for/against direction.** Anything that would land on `general` is dropped rather
   than imported. Omnibus Acts get **one label per strand** instead of a single flattened stance.

Filter 4 is doing more work in Missouri than in any previous state: see `../CODE-FINDINGS.md` §1 —
the House prints the previous question, the concurrence, the final passage and the emergency clause
under one identical desc, so every roll here was matched to its official roll-call PDF by
`(Total Yes, Total No, Total Present)` and the PDF header read before selection.

## Selected

| measure | roll | chamber | date | tally | roll-call PDF | version voted |
|---|---|---|---|---|---|---|
| HJR 73 reproductive health constitutional amendment | 1551715 | house | 2025-04-17 | 103-51 | 056.006 | 2454H.05P = enrolled |
| HJR 73 | 1578547 | senate | 2025-05-14 | 21-11 | — | truly agreed |
| HB 594 taxation | 1567074 | house | 2025-05-07 | 102-41 | 066.003 (identity-collapsed with 066.004, the Truly Agreed vote — identical tally and member list) | 1683S.04F = enrolled |
| HB 567 employee compensation | 1517204 | house | 2025-03-13 | 96-51 | 039.017 | 1616H.05P (= enrolled but for the failed emergency clause) |
| HB 567 | 1571781 | senate | 2025-05-14 | 22-11 | — | truly agreed |
| HB 145 sunshine law | 1569572 | house | 2025-05-12 | 107-50 | 069.009 | SS SCS as amended = enrolled |
| SB 152 foreign spending in ballot measure elections | 1572256 | house | 2025-05-15 | 94-47 | 072.016 | 1095S.13P = enrolled |
| HB 595 real estate transactions | 1565418 | house | 2025-05-05 | 103-37 | 064.003 | CCS 1536H.05S = enrolled |
| HB 595 | 1578662 | senate | 2025-05-07 | 23-10 | — | truly agreed |
| SB 71 public safety | 1563728 | house | 2025-05-01 | 112-33 | 063.008 | 1178H.08C + HA 1-6 = enrolled |

## Dropped under filter 5, after a full read of the enacted text

- **HB 495** public safety (the St. Louis police takeover). ~24 subjects. The marquee strand — a
  governor-appointed board taking control of the St. Louis Metropolitan Police Department, with a
  minimum city appropriation rising to 25 % of general revenue — maps to no research area, and the
  rest runs hard both ways *inside* `public_safety_and_crime_control`: a street-takeover offence,
  organized-retail-theft felonies, longer trafficking minimums, wider forfeiture and a ban on
  overdose-prevention facilities sit beside the repeal of the rioting offence and of the Missouri
  Incarceration Reimbursement Act, a 12-cent-per-minute inmate phone cap, limits on restraining
  pregnant and postpartum inmates, implicit-bias and de-escalation training, wrongful-conviction
  restitution beyond DNA cases, and a Missing and Murdered African American Women and Girls Task
  Force. A clean `immigration`/against strand exists (§ 43.505 mandatory reporting of arrestees'
  citizenship status) but labelling a 42-section Act off one reporting section flattens it.
  **A user direction call, on the Ohio batch-04 / Florida HB 1205 precedent.**
- **SB 4** utilities. 25 subjects, and `cost_of_living_reduction` — the area the measure is really
  about — runs both ways: construction work in progress for new natural-gas generating units in rate
  base, future-test-year ratemaking and PISA sunset extensions on one side; the revenue-requirement
  impact cap cut from 2.5 % to 2.25 %, CWIP recoveries refundable with interest if imprudent, a
  one-time rate cut if federal corporate income tax falls, and large-load tariffs on the other.
  **Also a user direction call.**
- **SB 22** ballot summary statements — two directions inside `election_integrity`: it compresses
  the window to challenge legislature-written ballot titles and makes courts defer to up to three
  Secretary of State rewrites, but § 116.334.2 preserves initiative signatures gathered under a title
  a court later changes.
- **SB 160** educational institutions — the divided House roll was cast on a twelve-section omnibus
  plus ten floor amendments; conference cut it to six sections, and that conference substitute passed
  the House 117-11, **not divided**. The divided Senate roll was on a two-section bill about
  belief-based student associations. Nothing here describes the law on a divided vote.
- **HB 225** first responders — omnibus accretion onto a narrow vehicle (40 enacted sections covering
  sheriffs' and firefighters' retirement financing, court surcharges, local tax authority, ambulance
  licensing and a new offence); no defensible single direction. 27 members voted *present*.
- **SB 63** nontraditional students — access to public-school athletics and activities for home,
  family-paced and virtual students; no area carries an honest direction (the Texas SB 2 and Georgia
  SB 82 school-choice precedent).
- **SB 49** school chaplains — single-subject and permissive, but the contested question is religion
  in public schools, which no area description covers (the Texas SB 11 precedent).

## Left for later

73 divided-and-enacted rolls on 28 measures, plus the 13 senate-only appropriations measures. The
ledger with every divided-and-enacted roll is `../survey/divided-enacted-worklist.tsv`. The
certified-roster expansion is complete for every November 2026 House and Senate election row in the
local database, so later batches can use the expanded crosswalk directly.
