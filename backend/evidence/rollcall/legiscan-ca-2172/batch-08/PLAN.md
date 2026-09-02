# California batch-08 — selection

**10 roll calls / 10 measures / 107 records.** Imported to local `voteapp` 2026-08-31. Prod untouched.

First batch of the **Senate-only tail**, under the final-text rule set out in `../batch-07/PLAN.md`.

## The economics of this tail, stated plainly

A Senate roll reaches about **11** of our candidates; an Assembly roll reaches about **68**. Only 20
of 40 Senate seats are on the November ballot against all 80 Assembly seats. So these measures cost
the same reading effort for roughly a sixth of the coverage. They are still worth doing — but they
are correctly done *after* the Assembly work, and the both-chamber pool (163 measures at ~79 records
each) outranks the rest of this tail.

Selection was meant to favor **area coverage** over raw count. It did not: the first draft of this
file claimed three new areas, and every one of them was already covered in batches 01-02. The
baseline was recalled from memory instead of measured. See "Corrected on review" below.

## The version check ran offline

The dataset's bill JSON carries `texts[]` with a date and type per version, so "was this vote cast
on the final text?" is answerable **without fetching anything**. Running it across all 160
Senate-only measures took one pass: **118 pass, 42 fail**. That is the whole triage, and it is
reproducible — see `../README.md`.

## What came through

| measure | status | area | yea | Senate |
| --- | --- | --- | --- | --- |
| AB 1263 ghost guns: aiding unlawful manufacture, code files | chaptered | gun_control | for | 30-10 |
| SB 1220 firearm ban after serial-number tampering | enrolled | gun_control | for | 30-9 |
| SB 53 Transparency in Frontier Artificial Intelligence Act | chaptered | corporate_accountability | for | 29-8 |
| AB 45 health-location data and geofencing ban | chaptered | data_privacy | for | 29-9 |
| SB 1418 preservation of election materials | enrolled | election_integrity | for | 30-10 |
| AB 1411 county voter education and outreach plans | chaptered | election_integrity | for | 30-10 |
| AB 1167 no ratepayer money for utility politics | chaptered | anti_corruption | for | 29-10 |
| SB 766 Combating Auto Retail Scams Act | chaptered | corporate_accountability | for | 30-8 |
| AB 1261 legal counsel for immigrant youth | chaptered | immigration | for | 30-8 |
| SB 771 social media liability for civil rights violations | vetoed | corporate_accountability | for | 30-8 |

**No area is new in this batch.** `gun_control` and `immigration` were first covered in batch-01,
`election_integrity` in batch-02. California covers **14 of 27** research areas, unchanged since
batch-07.

`gun_control` is still worth noting against the campaign's opening instruction. The area reads
"regulate firearm access … to reduce gun violence", so in California a yes on AB 1263 and SB 1220
is `for` — the mirror image of how Texas's firearm votes scored. The direction follows the area
description, not the bill's home state.

## Duplicate-date and version screens both caught something

- **SB 53 has a duplicate-date twin** (`../CODE-FINDINGS.md` §1): rolls 1602051 (09-12) and 1602910
  (09-13), both 29-8. The official history records the Senate concurrence on **09/13** — 09/12 was
  the *Assembly's* third reading, 59-7. Roll 1602910 is the pick.
- **SB 1220 and SB 766 each had an earlier divided roll that fails the version check** (2026-05-27
  and 2025-06-02). Both are initial Senate passage, before the Assembly amended the bill; the
  post-amendment concurrence is the pick in each case.

## Checks

- **Version check on all 10 picks**, dates recorded in `rolls.json`.
- **Completeness audit run BEFORE judging** — **75 untruncated digest items**; four gaps found and
  closed pre-import.
- Lint: 20 descriptions, 0 warnings, longest sentence 44 words.

## Corrected on review (2026-09-01)

The first draft said this batch opened `gun_control`, `election_integrity` and `immigration`, and
took California to 17 of 27 areas. **All three were already covered** — two since batch-01, one
since batch-02 — and the true count is 14. The error came from working off a remembered "13 of 27"
and an assumed list of which 13, rather than reading the slugs out of the earlier judgment files.
The measures themselves stand; the stated reason for choosing them does not. The fix that stops it
recurring is one line: count areas from the judgment files, never from memory.
