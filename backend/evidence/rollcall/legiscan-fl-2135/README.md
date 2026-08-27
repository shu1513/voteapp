# LegiScan roll calls — Florida 2025 Regular Session (session 2135)

Phase-4 state rollout, mirroring `legiscan-tx-2160/`. Local `voteapp` only;
production is never touched by anything in this directory.

## Files

| File | What it is |
| --- | --- |
| `survey/rollcall-legiscan-fetch-fl-*-survey.json` | The full 519-row desc histogram the config was written from. The fetch script prints only its top 40 rows to stdout, and those 40 are all committee names — read this file, not the console. |
| `survey/fetch-report.json` | The real fetch run that stored the queue rows. |
| `legiscan-people-fl-2135.json` | People snapshot from the dataset, so later runs need no dataset dir. |
| `crosswalk.json` | The reviewed identity map, 162 entries. |
| `proposals-report.json` | The proposer's output (no crosswalk applied), kept as the record of what was proposed vs. hand-added. |
| `batch-*/` | One directory per judged batch: `PLAN.md`, `rolls.json`, `judgments.json`, `JUDGING.md`, import ledgers. |

The dataset itself and the 906 evidence JSONs live **outside the repo** at
`/Users/shu/legiscan-data/fl-2135/` and `/Users/shu/legiscan-data/fl-2135-evidence/`
so they survive a session; only the curated subset above is committed.
The full-resolution resolve report is 4.5 MB and is deliberately not committed.

## The feed

3,003 roll calls over 1,960 bills. 760 are floor votes on kept instrument
types; 2,002 are committee votes rejected before the queue; 95 are on
excluded instrument types; 146 `Senate Rules` rows are stored as
non-floor audit rows. Zero surfaced-null rows, zero unrecorded (member-less)
rolls, zero duplicate floor identities.

Florida prints **failed** floor votes under the same desc as successful
ones, so a measure can carry several rolls per chamber — HB 1205 has six.
Selection picks the decisive roll; classification does not try to.

## Crosswalk

162 members with a seat, reviewed one by one:

- **63 mapped** — 58 from the proposer, 5 added by hand.
- **99 explicit nulls**, each noting whether the member's district has other
  Nov-2026 candidates on file or none at all.

Validation over all 906 evidence files: matched 23,531 / unmatched_reviewed
36,802 / **`no_crosswalk` 0 / `out_of_scope` 0**, 0 file errors, 0 zero-match
rolls. Fan-out is **median 50 matched members per HOUSE roll** and **9 per
Senate roll**.

### Why the fan-out is smaller than Texas's

Texas mapped 136 of 181 members; Florida maps 63 of 162. This is a roster
gap on our side, not a feed gap: the Nov-2026 general in our database
covers only **56 of 120 House districts and 13 of 40 Senate districts**, so
most sitting members simply have no general-election candidacy on file yet.
The pool was deliberately kept at the pipeline default (`--scope-from
2026-11-01`, 125 candidates) to match Texas and the standing Nov-2026 scope
rule; including the 2026-08-18 primary rosters would have raised the pool to
291 and the match count to 88.

When those general rosters are researched, **extend `crosswalk.json` and
re-run the import**: it adds the new members idempotently (Ohio precedent),
so nothing here has to be redone.

### The five hand-added members

The proposer cannot reach these by construction:

| people_id | Member | Why |
| --- | --- | --- |
| 21782 | Alejandro "Alex" Rizo (HD-112) | Legal first name is not a prefix of the working one (`alex` vs `alejandro`). |
| 22550 | Angela "Angie" Nixon (HD-013) | Running **statewide** (US Senate), outside the state-leg pool — and `angie` is not a prefix of `angela`. |
| 22527 | Demi Busatta Cabrera (HD-114) | Candidate drops the second surname. |
| 22207 | Jenna Persons-Mulicka (HD-078) | Former representative, now Lee County Supervisor of Elections seeking election to that office. |
| 20958 | Mike Caruso (HD-087) | Seven years in the Florida House, now Palm Beach County Clerk of the Circuit Court seeking election. |

Each was confirmed against the candidate row's own `current_office` /
summary text, and each name is unique on both sides.

Two proposals disagree on seat by design and were accepted: **Josie Tomkow**
(HD-051 → SD-014) and **Lauren Melo** (HD-082 → SD-028), both sitting
representatives with declared 2026 Senate candidacies. Same pattern as
Texas's Dennis Paul and David Cook. Trust LegiScan's `district`, never its
`role`.

## Scope note

Several marquee 2025 Florida fights fall **outside** the divided-and-enacted
gate: immigration enforcement passed in the 2025 **special** sessions
(LegiScan 2203 / 2204), not this one; the gun-age repeal (HB 759) died in
the Senate. This mirrors Texas SB 3 (vetoed hemp), which had zero divided
votes.
