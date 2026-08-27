# GA batch-01 — marquee enacted measures

**18 divided floor votes / 10 measures / 1,725 records**, selected 2026-08-26 from the 196
divided floor votes the GA 2167 fetch stored (115 of them on measures that became law, across
68 measures). Sized to match the Texas batch-01 pilot (25 rolls / 1,620 records); Georgia needs
fewer rolls for the same reach because every seat is on the ballot.

## The five selection filters

1. **Divided** — `LEAST(yeas,nays) >= GREATEST(yeas,nays)/4`, applied in the database over the
   stored floor votes.
2. **Consequential** — the measure became law. All 10 are LegiScan status 4, and none appears
   in the veto sections of the House Budget & Research Office session reports.
3. **Nameable subject** — the enacted text is about one thing a voter can recognize.
4. **One roll per measure per chamber**, preferring the passage vote. Where a chamber's own
   passage vote was on a text that is not what became law, or was not divided, the concurrence
   vote on the final text was taken instead — each roll's reason is in `rolls.json`
   (`version_check`).
5. **Stance-defensible** — the measure carries a research-area label with an honest for/against
   direction. Measures that would land on `general` were dropped rather than imported.

## What went in

| measure | area | yea | rolls |
| --- | --- | --- | --- |
| SB 68 — civil practice / tort revision | corporate_accountability | against | H passage 91-82, S concurrence 34-21 |
| SB 1 — 'Fair and Safe Athletic Opportunities Act' | civil_rights | against | H passage 100-64, S concurrence 34-20 |
| SB 185 — state funds for inmate transition care | civil_rights | against | S passage 37-15 |
| HB 111 — income tax rate 5.39% → 5.19% | personal_income_tax_reduction | for | H 110-60, S 30-23 |
| SB 144 — pesticide label as sufficient warning | corporate_accountability | against | S 42-12, H 101-58 |
| SB 212 — student directory information for political use | data_privacy | for | S 33-19, H 98-66 |
| HB 1247 — 'Bureaucratic Deference Elimination Act' | government_efficiency | for | H concurrence 98-70, S concurrence 34-18 |
| SB 443 — obstructing a highway | public_safety_and_crime_control | for | S 35-17, H 96-69 |
| SB 552 — 'TPUSA Act' student political groups | civil_rights | for | H passage 95-68 |
| SB 472 — local school board fiscal accountability | public_education_quality | for | H passage 96-58, S concurrence 29-17 |

Nine House rolls (median fan-out 149 candidates each) and nine Senate rolls (median 42).

## Dropped under filter 5, after reading the enacted text

- **SB 33** — LegiScan and the bill's own caption still call it the "Georgia Hemp Farming Act …
  total THC concentration"; the enacted text is a **property-tax bill** (Local Homestead Option
  Sales Tax). A vehicle bill, and the reason its divided end-of-session concurrences are not in
  this batch. See JUDGING.md.
- **SB 36** (Religious Freedom Restoration Act) — the enacted text is a legal standard plus an
  express clause disclaiming any effect on the Establishment Clause; it runs both ways, so it
  has no honest direction.
- **HB 645** — repeals a COVID-19 testing requirement *and* lowers the age for an offered flu
  vaccination from 50 to 18: two directions in one text.
- **SB 82** (local charter school authorization), **HB 328** (school scholarship tax credits) —
  the Texas SB 2 precedent: school-choice financing has no defensible direction under
  `public_education_quality`.
- **SB 447** (soil-erosion permit deadlines), **HB 987** (portable benefits), **SB 591**
  (disrupting a religious service), **SB 605** (grounds for prosecutor discipline),
  **SB 255** (legislative subpoena powers) — each reads two ways on its own text.
- **SB 154**, **HB 358**, **SB 96**, **HB 1277** — housekeeping or omnibus text with no single
  nameable subject.

## Deferred

97 divided-and-enacted rolls on 58 other measures, plus 81 divided rolls on measures that did
not become law. Georgia's constitutional amendments are unreachable for now (CODE-FINDINGS.md).
