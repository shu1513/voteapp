# TN batch-02 — selection

**10 divided floor votes across 8 enacted measures**, chosen from the 275 rolls
that `../survey/divided-enacted-worklist.tsv` still had unmarked after batch-01.

Same five filters as batch-01. Two things about Tennessee made the triage
different this time, and both are worth writing down.

## Tennessee bill titles are useless for triage

Every Tennessee caption reads like "AN ACT to amend Tennessee Code Annotated,
Title 49, relative to education." The subject is not in the title. The dataset's
per-bill `description` field is, and it is written from the act as passed: it
begins "As enacted, ...". All triage here was done on that field, then confirmed
against the chaptered act before writing.

## 56 of the 227 remaining measures are ceremonial resolutions

Joint resolutions honoring a retiring judge, congratulating teachers of the
year, recognizing an anniversary. They are bill type JR, so they pass the kept
bill-type check, and they clear the divided gate because the minority votes no
on the block. They carry no stance and fail the nameable-subject filter. All 56
are excluded.

## What went in

| measure | area | yea | rolls |
| --- | --- | --- | --- |
| HB 622 Dismantle DEI in Employment Act | civil_rights | against | H 73-24 |
| SB 468 Riley Gaines Women's Safety and Protection Act | civil_rights | against | H conference report 71-18 |
| SB 2222 liability for paying demonstrators | civil_rights | against | H 71-20 |
| SB 449 Fertility Treatment and Contraceptive Protection Act | womens_reproductive_rights | for | H 54-37 |
| SB 2412 abortion-inducing drug penalties | womens_reproductive_rights | against | H 74-20 |
| HB 2219 sheriffs must join federal 287(g) | immigration | against | H 71-25, S 24-7 |
| SB 6002 centralized immigration enforcement division | immigration | against | H 72-21, S 26-7 |
| SB 694 consumer loan interest cap 30% to 36% | cost_of_living_reduction | against | H 61-26 |

Eight House rolls (median fan-out 77 candidates) and two Senate rolls (14 and 16).

SB 449 is the only `for` direction on `womens_reproductive_rights` in the
campaign so far, and at 54-37 it is the closest vote in the batch.

## Dropped, and why

- **HB 7003** (redraws Tennessee's congressional districts, H 64-25) and
  **HB 7002** (deletes the one sentence of TCA 2-16-102 that barred changing
  congressional districts between apportionments, H 66-24 / S 22-8). These are
  the highest-profile votes in the session and they are dropped under filter 5,
  not for lack of importance. The Georgia redistricting maps in
  `../legiscan-ga-2114/` could carry `civil_rights / for` because a federal
  court had ruled the prior maps unlawful and the new maps were the remedy —
  the direction came from the record, not from an opinion about the map. There
  is no equivalent fact here: this was a mid-decade redraw with no court order,
  and the chaptered act is 141,000 characters of census-block tables. Any
  direction would be an assertion about who the map favors, which is not
  something the text supports. **This one is worth a second opinion — if the
  operator wants these two in, the direction has to be decided deliberately.**
- **HB 910** (Human Rights Commission) and **HB 754** (gender clinics) — both
  substantive and both likely keepers, but deferred to batch-03 because the
  version check was not finished. See JUDGING.md.
- **HB 6004** (Education Freedom Scholarship Act), **HB 2532**, **SB 2206**
  (Federal Tax Credit Scholarship Act), **SB 1585** — school-choice financing,
  dropped on the standing precedent that it has no defensible direction under
  `public_education_quality` (Texas SB 2, Georgia SB 82 and HB 328).

## Deferred

Roughly 160 real measures remain, which is several more batches.
