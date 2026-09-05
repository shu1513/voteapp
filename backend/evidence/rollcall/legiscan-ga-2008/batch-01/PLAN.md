# Georgia 2023-2024, batch 01 — marquee enacted measures

**16 divided floor votes / 9 measures / 1,367 records**, chosen from the 115
divided floor votes on enacted measures the GA 2008 fetch stored, across 61
measures. Sized to match the 2025-2026 batch-01 (18 rolls / 10 measures).

## The five selection filters

1. **Divided** — `LEAST(yeas,nays) >= GREATEST(yeas,nays)/4`, applied over the
   stored floor votes on kept bill types.
2. **Consequential** — the measure became law. All nine are LegiScan status 4
   with an Act number and a signing date in their own history.
3. **Nameable subject** — the enacted text is about one thing a voter can
   recognize.
4. **One roll per measure per chamber**, keeping the last divided vote, with the
   text that chamber actually voted identified first.
5. **Stance-defensible** — the measure carries a research-area label with an
   honest for-or-against direction.

## What went in

| measure | area | yea | rolls |
| --- | --- | --- | --- |
| SB 140 — ban on gender dysphoria treatment for minors | civil_rights | against | H passage 96-75, S concurrence 31-21 |
| HB 1105 — Georgia Criminal Alien Track and Report Act | immigration | against | S passage 34-19, H concurrence 99-75 |
| SB 63 — money bail and charitable bail funds | public_safety_and_crime_control | for | S conference report 30-17, H conference report 97-69 |
| SB 44 — mandatory minimum for gang recruitment | public_safety_and_crime_control | for | H passage 99-74, S concurrence 30-20 |
| SB 222 — public-only funding of election administration | election_integrity | for | H passage 100-69, S concurrence 32-21 |
| SB 420 — foreign adversary ownership of farmland | national_defense | for | H passage 97-67, S concurrence 41-11 |
| HB 1018 — Georgia Firearms Industry Nondiscrimination Act | gun_control | against | H passage 106-60, S passage 33-19 |
| SB 351 — parental consent for minors on social media | data_privacy | for | H passage 120-45 |
| HB 1015 — income tax rate 5.49% to 5.39% | personal_income_tax_reduction | for | S passage 40-12 |

Nine House rolls (median fan-out 133 candidates) and seven Senate rolls
(median 37).

## Dropped from this batch, after reading the enacted text

- **SB 92** — Prosecuting Attorneys Qualifications Commission. Dropped under
  filter 5, following the 2025-2026 precedent that dropped SB 605 on grounds
  for prosecutor discipline. Supporters call an oversight body accountability;
  opponents call it political interference with elected prosecutors, and the
  text supports both readings. No honest direction.
- **SB 233**, the Georgia Promise Scholarship Act. Dropped under filter 5 on the
  standing precedent that school-choice financing has no defensible direction
  under `public_education_quality` (Texas SB 2, Georgia SB 82 and HB 328).
- **Local bills** — HB 422 (Ware County elections board), HB 540 (Wilkes
  County), HB 642 and HB 644 (Cherokee County), SB 231 (Richmond County and
  Augusta), SB 338 (Cobb County school districts), SB 333 (incorporating the
  City of Mulberry). Each is divided and enacted, and each fails filter 3 for a
  statewide record.

## Deferred to later batches

The remaining 50 measures, including 99 divided-and-enacted rolls, plus the 61
divided rolls on measures that did not become law.

## Steps run, in order

1. `rollcall:legiscan:fetch` over the extracted dataset. 2,201 rows stored,
   1,274 of them floor votes on kept bill types, 0 file errors.
2. Build the crosswalk from the 2025-2026 session's committed entries plus the
   resolver's proposals, accepting only proposals where the seat agrees, then
   validate it with `rollcall:legiscan:resolve` over all 2,201 stored rolls.
3. Apply the divided gate, the date-range check and the roll-attribution check.
4. Apply the selection ladder, then filters 3 and 5 by reading each enacted
   text.
5. Download the text each chamber voted through the LegiScan API, verifying byte
   length and MD5 against the dataset manifest.
6. Write the judgments, lint them, check every description cites its own tally.
7. `rollcall:judge`, then import dry run, then the live import.
8. Reconcile three ways and sweep for duplicates.
