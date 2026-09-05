# Kansas batch-04 — the last 54 measures

## Kansas is finished

**Every row in `../survey/house-divided-enacted-worklist.tsv` now carries a disposition.** Nothing
is left as a candidate. Two rows stay marked `held`, and that is permanent: HB 2240 and SB 63 have
LegiScan tallies that contradict the Kansas bill history, recorded as `../CODE-FINDINGS.md`
finding 5. They are not judgeable, not pending.

| disposition | rolls |
| --- | --- |
| batch-01 | 12 |
| batch-02 | 7 |
| batch-03 | 2 |
| batch-04 | 6 |
| dropped | 59 |
| not-selected | 61 |
| superseded | 7 |
| excluded | 2 |
| held (data defect) | 2 |

## This batch

**6 measures, 6 rolls, 442 records, 395 area tags, 0 errors.** Kansas now holds **1,977 records**.

| measure | area | direction | vote |
| --- | --- | --- | --- |
| HB 2311 religious belief in foster and adoptive placement | civil_rights | yea = against | 87-38, override |
| HB 2333 campus speech, the KIRK act | civil_rights | yea = for | 85-38, override |
| HB 2727 suing over abortion consent rules | womens_reproductive_rights | yea = against | 87-36, override |
| SB 29 local power to stop public gatherings | environment_and_public_health | yea = against | 86-38, override |
| SB 35 ending two statewide property tax levies | cost_of_living_reduction | yea = for | 96-26 |
| SB 391 preempting local rental ordinances | housing_affordability | yea = against | 85-38, override |

Five of the six passed over the governor's veto, and four record a stance on **both sides**, which
is why 442 records carry 395 tags.

## Kansas titles failed five separate times

This is the finding that should outlast the batch. Across the campaign, five Kansas worklist
titles proved wrong or badly incomplete, in three distinct ways:

1. **The title names a different bill.** HB 2183 is listed as "sexual exploitation of a child";
   the act bars courts from deferring to an agency's reading of its own rules.
2. **The title names a minor section and hides the act.** HB 2372 is listed as "unlawful approach
   of a first responder" — one section of fourteen. The rest is federal immigration enforcement.
3. **The title describes the wrong subject entirely.** HB 2464 says "graduate medical education";
   the act extends angel investor and aerospace credits. HB 2727 says "wage claims"; the act is
   the woman's-right-to-know act. SB 29 says "removing the authority of the c—"; the act removes
   *local* health officers' power, not a state officer's.

The mechanical title check caught only the first. **It is a first filter and never clearance.**
Its cached results are at `/Users/shu/legiscan-data/ks-work/titlecheck.json`.

## Why 48 of 54 were dropped

Fourteen were read in full and dropped; forty were set aside on the title and digest. The
recurring reasons, in order of frequency: **no research area covers the subject** (procurement
policy, court procedure, licensing details, guardianship jurisdiction, controlled-substance
scheduling, golf carts, a memorial designation); **the act bundles unrelated subjects** (HB 2206,
HB 2313, HB 2299, HB 2402, SB 462, HB 2444); **it moves both ways inside one area** (HB 2291,
SB 54, HB 2060); and **only the introduced text exists**, which for HB 2020 also contradicts
itself on whether reports are monthly or quarterly.

Every drop carries its own written reason in the worklist.

## Checks

12 descriptions, **0 plain-language lint warnings**, 0 checker problems, Flesch-Kincaid median
grade **7.9** and worst **9.2**, longest sentence 26 words. **All 6 stated tallies match the
stored vote row.**

## Reconciliation

Predicted independently before touching the database: **442 records and 395 area tags**. Dry run
442 insert, real run 442 insert with 0 errors, database 442 and 395, re-run all 442 unchanged.
Real stamp `2026-09-05T04:28:11.920Z`.
