# Oregon batch-07 — plan and judging

Seven measures, ten roll calls, **279 records across 61 candidates**. Oregon
totals: **1,929 records / 1,461 tags / 68 approved rolls.** Production has zero.

| Measure | House | Senate | Area | Yea | Nay |
| --- | --- | --- | --- | --- | --- |
| SB 726 landfill methane monitoring | 31-24 | 18-12 | environment_and_public_health | for | null |
| SB 827 solar and storage rebate | 36-16 | 21-7 | environment_and_public_health | for | null |
| HB 3024 unemployment benefit penalty | 35-22 | 18-10 | social_programs_and_welfare | for | null |
| SB 295 pharmacists and COVID-19 | 40-11 | — | healthcare_affordability | for | null |
| SB 1032 aerosol duster sales to minors | — | 22-6 | environment_and_public_health | for | null |
| SB 150 fees for veterans' benefit claims | 32-22 | — | corporate_accountability | for | null |
| HB 2530 school bus stop arm cameras | — | 22-7 | public_safety_and_crime_control | for | null |

## ⚠ SB 726 is a single-county law and its staff summary does not say so

The summary describes rules for "municipal solid waste landfills" generally.
The enrolled Act defines that term as a landfill unit **"located in Benton
County"**, so the whole thing reaches one county's landfill. LegiScan's own
plain-language summary got this right where the staff summary did not.

Under the standing rule from the California campaign — a single-county
**procedural** measure drops, a substantive one is kept and the county named —
this is kept, and the description says twice that it reaches only that county.

## What the enacted text settled

- **HB 3024** repeals a whole subsection, so the change is only visible as a
  bracketed deletion. It removes the rule that cut a disqualified worker's
  yearly benefit total by eight times the weekly amount even after they
  requalified. **The disqualification itself is untouched**, and the
  description says so, because "ends a penalty" alone would read as though
  misconduct no longer costs anything.
- **SB 295** is a one-line Act: it repeals a sunset. The description says
  nothing else changes, which is literally true.
- **SB 150** bans four things, not one: payment for preparing or presenting a
  claim, for advising or representing, for handling an appeal of a first
  decision, and for referring a veteran on. All four are named, along with the
  bar on guaranteeing a result and the court's test for an unreasonable fee.
- **HB 2530** carries a counter-consideration that is stated rather than
  hidden: the presumption that the warning sign was posted and the bus lights
  working shifts the burden onto the accused driver.

## Labels

All seven score `for` with `nay: null`. Objections run on different axes:
compliance cost for one landfill operator (SB 726), rebate spending (SB 827),
employer experience-rating and moral hazard (HB 3024), scope of practice
(SB 295), retail burden (SB 1032), the right to hire paid help (SB 150) and
automated enforcement and due process (HB 2530).

## Checks

- Version check on all 10 rolls: each on the enacted text. SB 726's House roll
  is the second of two, taken after the Senate amended.
- Superseded check up front; no acknowledgments.
- `related` 0, errors 0, notifications 0.
- Dry run matched the real run at 279 inserts; convergence all `unchanged`.
- **Reading level median 7.2, worst 10.2** — the best of any Oregon batch, and
  the first at the 7th-grade target. Short sentences did it. First drafts
  measured up to 11.0.
- The builder missed **"pressurised"**, a third gap in its British-spelling
  word list. Added, along with a dozen more, and every earlier batch rebuilt
  against it.

## Ledger

`import-report.json` — the insert run, 279 records.
