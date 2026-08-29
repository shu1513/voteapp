# PA batch-01 — selection

5 divided floor votes / 5 enacted measures / **882 records across 179
candidates**. All five are House rolls; see below for why the Senate
contributes none.

| measure | roll | date | tally | PN voted | enacted as |
| --- | --- | --- | --- | --- | --- |
| HB 103 | 1567196 | 2025-05-07 | 148-55 | PN 1113 | Act 28 of 2025 (PN 1999) |
| HB 858 | 1583943 | 2025-06-03 | 132-71 | PN 1806 | Act 29 of 2026 (PN 1806) |
| SB 95 | 1596825 | 2025-06-30 | 126-77 | PN 1019 | Act 34 of 2025 (PN 1019) |
| HB 1425 | 1610608 | 2025-12-16 | 147-56 | PN 2675 (concurrence) | Act 57 of 2025 (PN 2675) |
| SB 375 | 1689892 | 2026-04-27 | 141-59 | PN 1539 | Act 17 of 2026 (PN 1539) |

## Filters applied, in order

1. **Divided** — `LEAST(yea,nay) >= GREATEST(yea,nay)/4`. 291 of 861 kept
   floor votes qualify.
2. **Became law** — LegiScan status 4. Cuts 291 to 35 rolls on 28 measures.
   This is where Pennsylvania's split legislature bites: 252 divided rolls
   are on bills that passed one chamber and died.
3. **Not appropriations** — 18 of the 35 rolls are budget or line-item
   appropriations bills. Federal and Georgia precedent: a fund-the-government
   vote has no honest research area. The user was shown the measured pool and
   chose the standard gate over including them.
4. **One roll per measure per chamber**, preferring final passage. No measure
   in this batch needed the tie-break; each contributes exactly one roll.
5. **A defensible for/against direction in a research area.** Anything that
   would land on `general` is dropped, not imported. This is what took the
   remaining 17 rolls down to 5.

## Dropped under filter 5, after reading the enacted text

- **SB 315** — Public School Code omnibus (Act 47 of 2025), the biggest
  education vote of the biennium. Two reasons. Its centerpiece is cyber
  charter tuition reform, which is school-choice financing — dropped in
  Texas (SB 2) and Georgia (SB 82, HB 328) on the same ground. And the
  divided House roll is on **PN 1226, two amendments before the enacted
  PN 1310**, whose fiscal note is five times longer. Expect to be asked.
- **HB 274** — unemployment compensation. Tightens requalification from 6x
  to 12x weekly benefits and adds a good-faith job-search duty, but also
  adds domestic-violence eligibility protection and delays the 250% solvency
  trigger that would otherwise cut benefits. Genuinely two-directional.
- **HB 1431** — Sunday hunting repeal (Act 36 of 2025). The only measure in
  the whole pool with a Senate roll, and the only three-roll measure. No
  research area carries a direction for hunting access, and its new
  trespass penalties pull against the access strand. Dropping it is what
  makes batch-01 house-only.
- **HB 799** — veterans' benefits workplace posting. No research area
  covers veterans services; `social_programs_and_welfare` would be a stretch.
- **HB 324** — Poultry Technician Licensure Law. Occupational-licensing
  streamlining that reads as efficiency and as weaker independent sampling.
- **HB 331** — magisterial district court fee plumbing.
- **HB 1646** — quorum of the State Board of Barber Examiners. Trivia.
- **HB 2559**, **SB 467** — named land conveyances and a Project 70
  restriction swap. Local and narrow.

Every one of the 35 divided-and-enacted rolls carries its disposition in
`../survey/divided-enacted-worklist.tsv`.

## Date audit

All five roll dates were checked against the official action history on the
bill's palegis.us page and match exactly, tally included. No Pennsylvania
roll in this batch needed the `official_vote_date` override.

## What is left

30 divided-and-enacted rolls stay pending, 18 of them appropriations. The
larger untouched pool is the **252 divided rolls on bills that passed only
one chamber** — a scope the campaign has never taken in any state, and a
user decision if it is ever wanted.
