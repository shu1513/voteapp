# California batch-02 — selection

**24 roll calls / 12 measures / 859 records.** Imported to local `voteapp` 2026-08-29. Prod untouched.

Batch-01 wrote 298 records against a crosswalk with 33 mapped members. Since then the Assembly
rosters were completed and the crosswalk extended to **80 mapped** (PR #907), which re-imported
batch-01 up to 729 records. Batch-02 therefore lands on the full fan-out from the start:
**median 68 candidates per Assembly roll and 11 per Senate roll**, about 72 records per measure.

## The five filters

Applied to the 5,281 stored floor votes, unchanged from batch-01:

1. **Divided** — `LEAST(yeas,nays) >= GREATEST(yeas,nays)/4` and `nays > 0` → 972.
2. **Became law** — LegiScan `status = 4`, each re-checked against the chaptered text on leginfo
   (chapter number and governor's approval date) → 441 rolls on 227 measures; 411 rolls on 217
   measures after batch-01.
3. **Nameable subject** mapping to one research area.
4. **One roll per measure per chamber, final action** — the last divided floor vote each chamber
   took, which is the origin chamber's concurrence and the second chamber's third reading.
5. **Stance-defensible** — a research area with an honest for/against direction, or dropped.

Selection was drawn from the 77 measures that are divided in BOTH chambers and are not budget acts,
trailer bills, or single-jurisdiction local measures.

## What came through

| measure | area | yea | Assembly | Senate |
| --- | --- | --- | --- | --- |
| AB 572 police interviews of victims' families | public_safety_and_crime_control | for | 42-22 | 22-10 |
| AB 1071 evidence disclosure in discrimination claims | civil_rights | for | 42-21 | 25-11 |
| AB 2624 address confidentiality for immigration workers | immigration | for | 59-19 | 30-10 |
| AB 1318 state tax-exempt nonprofits as grantees | immigration | for | 60-19 | 29-10 |
| SB 30 decommissioned diesel rail equipment | environment_and_public_health | for | 53-22 | 29-10 |
| AB 1037 naloxone access | environment_and_public_health | for | 53-21 | 30-10 |
| AB 858 laid-off worker recall rights | corporate_accountability | for | 49-19 | 29-10 |
| SB 763 Cartwright Act fines | corporate_accountability | for | 53-20 | 29-8 |
| SB 82 consumer contract dispute-resolution terms | corporate_accountability | for | 55-18 | 28-10 |
| SB 596 nurse-ratio penalties | healthcare_affordability | for | 48-19 | 21-10 |
| AB 1061 urban lot splits and historic districts | housing_affordability | for | 46-18 | 24-10 |
| SB 73 law enforcement and election administration | election_integrity | for | 57-19 | 29-8 |

All twelve carry a stance. **Two areas gain their first California coverage: `civil_rights`
(AB 1071) and `election_integrity` (SB 73)** — the latter matters because batch-01 deliberately
dropped two election bills for lacking a defensible direction, and SB 73 is a genuine
security-of-administration measure rather than an access expansion.

## Dropped under filter 5 after a full read

- **AB 1376 (Wards: probation)** — caps juvenile probation at 12 months, drops the $250 fine option,
  requires tailored conditions. One text, but the direction is a contested value call rather than a
  reading of the statute: rehabilitation-first juvenile justice reads as better justice-system
  performance to one voter and as less supervision to another. The Texas SB 1957 precedent.
- **SB 281 (Pleas: immigration advisement)** — requires the immigration-consequences advisement
  verbatim going forward, but states the Legislature does not intend a missing verbatim advisement
  to invalidate a plea taken before 2026-01-01. Strengthens forward, limits backward: the SB 477
  pattern.
- **SB 786 (General plan: judicial challenges)** — caps continuances at 60 days and widens temporary
  relief (pro-enforcement) while **extending the compliance window from 60 to 120 days** and
  removing the extension provision (pro-locality). Two directions on the same axis.
- **SB 551 (Corrections: state policy)** — findings, a mission statement, and staff training on
  "normalization and dynamic security". Declaratory; no operative duty a voter can weigh.
- **SB 770 (EV charging in common interest developments)** — deletes an additional-insured insurance
  requirement and fixes a cross-reference. Technical.
- **SB 127 (Climate change)** and **SB 162 (Elections)** — Committee on Budget and Fiscal Review
  bills: Energy Commission salaries, EPIC funding extension, an appropriation. Budget trailer bills,
  excluded by the appropriations precedent.
- **SB 280 (Elections)** — 2026 primary calendar mechanics (in-lieu filing-fee petition form dates,
  a feasibility study). Administrative.

## Hazards handled

- **SB 763 sits on the duplicate-date defect** (`../CODE-FINDINGS.md` §1). Its Senate concurrence
  appears twice — 1602305 (09-12) and 1602893 (09-13), same 29-8 lineup — and the bill history
  records one, on 09-13, so **1602893** is the pick. Every other pick was checked against the pair
  list and has no twin.
- **Two measures are 2026 statutes.** SB 73 is Chapter 10 and AB 2624 is Chapter 117, Statutes of
  2026, both verified chaptered on leginfo rather than trusted from LegiScan's status alone. AB
  2624's Assembly concurrence (2026-08-19) falls inside the 30-day notification window; the import
  reported **0 notified**, because no follower is attached to these candidates locally.

## Version check — all 24 votes were cast on the enrolled text

Every pick postdates its bill's last `Amended` version, so no description needs a version caveat.
Tightest margins: SB 73 (amended 2026-05-18, Assembly vote 05-22) and AB 572 / AB 1071 / AB 1037
(amended 2025-09-05, first vote 09-09). `rolls.json` records each roll's `last_amended` date.

## Left for later

**387 divided-and-enacted rolls on ~205 measures.** Beyond that, **430 divided rolls on 419
measures were still awaiting the governor** when this dataset was cut (2026-08-23), with the session
closing at the end of August — a re-download in the autumn should convert a large share of those
into batch-03 material.
