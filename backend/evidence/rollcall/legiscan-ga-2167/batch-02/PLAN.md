# GA batch-02 — the enacted tail after batch-01

**9 divided floor votes / 7 measures / 1,035 records**, selected 2026-08-27 from the 97
divided-and-enacted rolls batch-01 left behind (61 measures, of which 14 were already read and
dropped there, leaving 44 never assessed).

Six House rolls (median fan-out 149 candidates) and three Senate rolls (median 42).

## The five selection filters (unchanged from batch-01)

1. **Divided** — `LEAST(yeas,nays) >= GREATEST(yeas,nays)/4`, over the stored floor votes.
2. **Consequential** — the measure became law. All 7 are LegiScan status 4, and none appears in
   the veto sections of either House Budget & Research Office session report (checked by name:
   the 2025 report lists HB 308, HB 433, HB 532, SB 46; the 2026 report lists 12 vetoes,
   including HB 14, HB 376, HB 519, HB 1070, HB 1192, HB 1409, SB 23, SB 59, SB 478, SB 569).
3. **Nameable subject** — the enacted text is about one thing a voter can recognize.
4. **One roll per measure per chamber**, preferring passage; where the chamber's passage vote was
   on a text that is not what became law, its concurrence vote was taken instead. Every roll's
   reason is in `rolls.json` (`version_check`).
5. **Stance-defensible** — a research-area label with an honest for/against direction. Anything
   that would land on `general` was dropped rather than imported.

## What went in

| measure | area | yea | roll |
| --- | --- | --- | --- |
| SB 69 — 'Georgia Courts Access and Consumer Protection Act' (litigation financing) | corporate_accountability | for | H concurrence 120-42 |
| HB 1164 — school system fiscal audit and intervention | public_education_quality | for | H passage 103-68 |
| HB 1185 — internal entity claims into the Business Court | corporate_accountability | against | H concurrence 97-65 |
| HB 463 — income tax rate cut to 4.99%, floor to 3.99% | personal_income_tax_reduction | for | H concurrence 104-71, S concurrence 33-20 |
| SB 220 — 'Putting Georgia's Patients First Act' (medical cannabis) | healthcare_affordability | for | S concurrence 38-14 |
| SB 244 — prosecutor-disqualification fees + wrongful conviction compensation | public_safety_and_crime_control | for | H passage 103-61, S concurrence 35-18 |
| SB 40 — secondary metals recyclers, cash limits | public_safety_and_crime_control | for | H concurrence 91-63 |

## Dropped under filter 5, after reading the enacted text

- **SB 503** — a second SB 33: the only divided roll (Senate 42-11) was cast on text exempting
  *rental home marketplace guarantees* from the definition of property insurance. That text was
  replaced wholesale; the enacted Act is about orthotic and prosthetic coverage. See JUDGING.md
  — the HBRO summary of that Act is also **backwards**, which is why the enrolled text decides.
- **SB 179** — the enrolled Act pairs a computer-science graduation requirement with a mandate
  that local systems *allow* patriotic-society representatives to address students during school
  hours (HBRO's summary says "encourage"). Two directions in one text.
- **SB 13** — authorizes state loans for natural gas supply, distribution, and storage, and for
  moving electrical lines underground. Calling a yes vote "against protecting air, water, and
  climate" rests on an energy-policy premise the text does not state. Also a genuine per-chamber
  version split (the Senate voted a broader authority with no certified-area limit and no
  undergrounding).
- **HB 297** — creates the Georgia Transportation Efficiency Authority and extends the MARTA
  sales tax to 2067, but also bars counties with no transit from starting any without a
  referendum. Two directions.
- **HB 295** — property-value compensation claims against local governments *and* a sovereign
  immunity waiver for sanctuary-policy violations plus immigration mandamus. Two subjects, one of
  them immigration.
- **SB 556** — omnibus: DREAMS scholarship, fine-arts GPA, opioid antagonists on campus, a
  medical-school scholarship, 529 contribution caps, the lottery shortfall reserve.
- **HB 382** — one Schedule I revision inside a pharmacy-practice bill (supervisory ratios,
  remote technicians, interns, drug returns).
- **HB 717** — its only divided House roll **failed** 73-88. Judge only decisive votes (the
  federal 117-1 roll 160 precedent).
- **SB 218** — Prosecuting Attorneys Qualifications Commission, dropped for the same reason
  batch-01 dropped SB 605: the text reads both as prosecutor accountability and as political
  control of elected prosecutors.
- **SB 191, SB 485, HB 426, HB 369, HB 134, HB 1161, HB 1199, HB 1261, HB 1020, HB 1379, HB 90,
  HB 78, HB 58, SB 291, HB 233, HB 413, HB 516, HB 36, HB 360, HB 439, SB 125, SB 138, SB 139,
  HB 155, HB 1567, SB 170** — housekeeping, local legislation, tax-credit mechanics, or trivia
  (Brunswick stew as the official state stew; the City of Mulberry's corporate limits).

## Deferred

The enacted-and-divided pool is now worked out for anything with a defensible stance. What
remains: 81 divided rolls on measures that did **not** become law, Georgia's two enacted
Nov-2026 ballot amendments (unreachable, CODE-FINDINGS.md), and the 2026 special session
(LegiScan 2268), which has never been surveyed.
