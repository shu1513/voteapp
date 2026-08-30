# California batch-03 — selection

**18 roll calls / 9 measures / 645 records.** Imported to local `voteapp` 2026-08-29. Prod untouched.

## Why now, and not a re-download

The plan after batch-02 was to wait for an autumn re-download, because 430 divided rolls on 419
measures were still awaiting the governor when this dataset was cut. That is still the right move
for those rolls — the session closes at the end of August and signing runs into the autumn — but it
does not block this batch. Checked before starting: LegiScan still serves the **same 2026-08-23
cut** for session 2172 (identical size and `dataset_hash`), so a re-download today buys nothing,
while 387 already-enacted divided rolls were sitting unused. Batch-03 works those.

## The five filters

Unchanged. Applied to the 5,281 stored floor votes: divided → became law → nameable subject → one
roll per measure per chamber (final action) → stance-defensible. After batches 01 and 02, the pool
of measures divided in **both** chambers, excluding budget acts, trailer bills, and
single-jurisdiction local measures, stood at **52**; nine came through.

## What came through

| measure | area | yea | Assembly | Senate |
| --- | --- | --- | --- | --- |
| AB 1127 machinegun-convertible pistols | gun_control | for | 54-16 | 29-9 |
| SB 524 AI-written police reports | public_safety_and_crime_control | for | 50-17 | 28-10 |
| AB 1036 postconviction discovery | public_safety_and_crime_control | for | 54-18 | 28-9 |
| SB 518 Bureau for Descendants of American Slavery | civil_rights | for | 60-17 | 30-10 |
| AB 246 Social Security Tenant Protection Act | social_programs_and_welfare | for | 50-17 | 27-11 |
| SB 634 local bans on helping homeless people | social_programs_and_welfare | for | 55-20 | 22-16 |
| AB 628 stove and refrigerator as habitability | housing_affordability | for | 49-17 | 25-11 |
| AB 1056 gill net permit transfers | environment_and_public_health | for | 51-17 | 29-10 |
| SB 825 consumer financial protection enforcement | corporate_accountability | for | 59-19 | 28-10 |

All nine carry a stance. **`social_programs_and_welfare` gets its first California coverage**
(AB 246 and SB 634). SB 825 was the measure batch-01 set aside as thin; with the statute read in
full it states cleanly in two sentences, so it is in.

## Dropped under filter 5 after a full read

- **SB 838 (Housing Accountability Act)** — bars mixed-use projects from claiming HAA protection if
  any portion is hotel, motel, or transient lodging. Preserving the act for genuine housing and
  narrowing a pro-housing statute are both fair readings of the same clause.
- **SB 177 (Medi-Cal: Fair Share from Big Corporations Act)** — a Committee on Budget and Fiscal
  Review bill that appropriates $10,000 and requires the Department of Finance to present *options*
  to a legislative committee by 2027. Budget bill, and it produces a report, not a duty.
- **SB 437 (CSU: genealogy and descendancy)** — appropriates up to $6 million for university
  research on the reparations task force's recommendations. Thin as a voter-facing record, and
  SB 518 already carries this subject substantively in this batch.
- **AB 747 (SPARE Act)** — process-server registration and service rules. Direction was not clear
  enough from a full read to defend either way.
- **SB 128, SB 168, SB 171, AB 134, AB 152, AB 179** and similar terse-titled measures
  ("Transportation.", "Public Resources.", "Labor.", "Public Safety.", "Human services.",
  "Housing.") — budget trailer bills, excluded by the appropriations precedent along with the
  budget acts themselves.

## Checks

- **Version check: all 18 votes were cast on the enrolled text.** Every pick postdates its bill's
  last `Amended` version; `rolls.json` records each date. Tightest is AB 1127 (amended 2025-09-09,
  Senate vote 09-12).
- **Duplicate-date screen (`../CODE-FINDINGS.md` §1): no pick has a twin.** Each was compared
  against every other roll on the same bill for a match on chamber, desc, all four tallies, passed
  flag, and member-list hash.
- **Exception clauses were read from the STATUTES, not the digests** — the AB 572 lesson from
  batch-02's review, where the chaptered digest paraphrased an operative exception loosely. AB
  1127's nine exemptions, SB 634's plywood carve-out and definitions, and SB 524's vendor
  troubleshooting allowance all come from the enacted sections.
- **Plain-language lint before the import:** one description flagged (SB 634's Senate concurrence,
  46 words), split; committed file reports **0 warnings across all 36 descriptions**.

## Left for later

**369 divided-and-enacted rolls on ~196 measures**, plus the 430 divided rolls on 419 measures that
were still awaiting the governor at the 2026-08-23 cut. The autumn re-download remains the natural
batch-04 trigger.
