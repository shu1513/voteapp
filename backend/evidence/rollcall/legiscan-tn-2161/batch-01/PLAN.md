# TN batch-01 — selection

**14 divided floor votes across 12 enacted measures**, chosen from the 289
divided-and-enacted rolls in `../survey/divided-enacted-worklist.tsv`.

## The five filters

1. **Divided** — `LEAST(yea,nay) >= GREATEST(yea,nay)/4`. Tennessee's Republican
   supermajority makes this the binding filter in the Senate (56 divided senate rolls
   against 260 in the House), which is why 10 of the 14 picks are House votes.
2. **Became law** — LegiScan status 4.
3. **Nameable subject** — the measure has a subject a voter can recognise, read off the
   enrolled act rather than the caption.
4. **One roll per measure per chamber**, preferring final passage over concurrence and
   conference reports.
5. **Stance-defensible** — the measure maps to a research area with an honest for /
   against direction. Anything that would land on `general` was dropped rather than
   imported.

## Selected

| measure | chamber | roll | date | tally | area / stance |
| --- | --- | --- | --- | --- | --- |
| SB 1084 Dismantling DEI Departments Act | house | 1554040 | 2025-04-22 | 72-25 | civil_rights / against |
| SB 1713 DEI compliance attestations | house | 1687321 | 2026-04-21 | 66-20 | civil_rights / against |
| HB 749 out-of-state licenses issued to illegal aliens | house | 1535748 | 2025-04-03 | 68-21 | immigration / against |
| SB 1915 local public-benefit verification | house | 1687306 | 2026-04-21 | 73-21 | immigration / against |
| SB 1915 (Senate concurrence) | senate | 1688578 | 2026-04-22 | 24-6 | immigration / against |
| SB 670 isolated wetlands | house | 1553646 | 2025-04-21 | 71-21 | environment_and_public_health / against |
| HB 2070 Tennessee Energy Freedom Act | house | 1661430 | 2026-03-12 | 69-18 | environment_and_public_health / against |
| SB 2348 high-school library specialists | house | 1681950 | 2026-04-14 | 70-24 | public_education_quality / for |
| SB 2040 FAIR Rx Act (PBM–insurer–pharmacy separation) | senate | 1686179 | 2026-04-20 | 24-9 | corporate_accountability / for |
| SB 1360 firearm-industry liability and preemption | house | 1548823 | 2025-04-15 | 72-20 | gun_control / against |
| HB 1093 machine-gun conversion devices | senate | 1556833 | 2025-04-22 | 23-8 | gun_control / for |
| SB 336 voting-rights restoration (Senate's version) | senate | 1543982 | 2025-04-10 | 24-8 | civil_rights / for |
| SB 336 voting-rights restoration (House rewrite) | house | 1657505 | 2026-03-09 | 64-24 | civil_rights / for |
| HB 2185 SAVE data in the registration portal | house | 1666206 | 2026-03-19 | 68-22 | election_integrity / for |

Both `gun_control` directions appear on purpose: SB 1360 widens industry immunity and
tightens preemption, HB 1093 restricts access. `SB 336` is the one measure whose two
chambers voted **different texts** and therefore carries two different descriptions.

## Dropped under filter 5, after a full read

- **SB 1603** — a vehicle bill, and the batch's clearest trap. Caption, LegiScan title
  and the base fiscal note all describe a TACIR **study of a medical cannabis program**;
  the adopted amendment (SA0768, filing 015699) replaced that with a bar on the
  commissioner rescheduling marijuana under state law even if federal law reschedules
  it. Judging from the title would have false-sentenced every member who voted.
- **SB 229** (campaign finance) — an omnibus that runs both ways: it adds filing duties
  and a perjury affirmation while raising the contribution threshold from $100 to $250
  and charging committees a $150 annual fee.
- **HB 548** (foreign-adversary procurement), **SB 1788** (restricting how local
  governments relocate homeless people), **HB 6004 / SB 1891** (Education Freedom Act
  vouchers — the Texas SB 2 and Georgia SB 82 precedent), **HB 1194** (E-Verify for
  one-employee firms; dropped only to keep three immigration measures from crowding a
  14-roll batch).
- The 2026-02-04 bloc of ~40 commendation resolutions, all 70-18: divided by a
  procedural pattern, trivia by content.

## Deferred

275 divided-and-enacted rolls on 227 measures remain for batch-02 and later.
