# Georgia 2025-2026 Regular Session (LegiScan session 2167)

Phase-4 LegiScan roll-call campaign for Georgia, mirroring the Texas 89R run in
`../legiscan-tx-2160/`. Georgia is the second registered LegiScan state.

Why Georgia: **both chambers are entirely on the Nov-2026 ballot** (180 House +
56 Senate, two-year terms), so every crosswalk match is a candidate a voter can
actually act on — the Senate fan-out is ~4x Texas's (Texas had only 14 of 31
Senate districts on the ballot). The session ran the full biennium, sine die
2026-04-03, so 2026 votes are in the dataset too.

## Dataset

Downloaded 2026-08-26 with the operator's LegiScan key (main checkout
`backend/.env` only — never Render):

    getDatasetList&state=GA  ->  session_id 2167, 16,014,830 bytes
    getDatasetRaw&id=2167&access_key=<ak>

Extracted to `/Users/shu/legiscan-data/ga-2167/GA/2025-2026_Regular_Session/`
(outside the repo, so it survives session death). 5,480 bills / 2,520 roll
calls / 242 people.

## Survey (2026-08-26)

`survey/desc-families.json` — the run report's 1,696 raw desc rows folded over
the per-chamber vote-number suffix Georgia stamps on every desc, giving 155
families. That fold is what the config's patterns are written against.

Dataset hygiene, checked directly over all 2,520 roll calls before the config
was written:

| check | result |
| --- | --- |
| duplicate `roll_call_id` (the Texas hazard, 9.4% there) | **0** — the identity-collapse fix in `fetchLegiscanRollCallVotes.ts` is a verified no-op for Georgia |
| roll calls with an empty `votes[]` member list (the Texas summary-only hazard) | **0** |
| `total != yea+nay+nv+absent` | **0** |
| committee-sized tallies | **0** — the dataset holds floor votes only |
| vote dates | 2025-01-13 .. 2026-04-03 (1,262 in 2025, 1,258 in 2026) |

Classification the registered config produces (dry simulation over the whole
dataset): **1,338 kept floor votes** (House 702 / Senate 636) = 1,072 passage +
261 concurrence + 5 conference report; 1,016 excluded procedural; 158 excluded
as resolution-typed measures; 8 surfaced unknown; 0 small-tally.

**196 divided floor votes** (House 88 / Senate 108) under the phase-2 gate
`LEAST(yea,nay) >= GREATEST(yea,nay)/4`; **115 of them are on measures that
became law**, spanning 68 distinct measures. That is the batch worklist.

## Crosswalk (2026-08-26)

`crosswalk.json` — **242 entries: 208 mapped, 34 explicit null.** The proposer produced 196
(193 exact first+last, 3 first-prefix); all were accepted, including three `seatAgrees:false`
pairs that are sitting House members running for the Senate (Will Wade HD-009 → SD-51, Saira
Draper HD-090 → SD-44, Ruwa Romman HD-097 → SD-7), each confirmed against the candidate row's
party and profile.

**12 hand-added in three classes the proposer cannot reach:**

- 6 name variants — the snapshot's `first_name` is a legal name the candidate does not use
  (`Hugh` for Bruce Williamson, whose `Bruce` sits in the nickname field the proposer does not
  read; `Homer` DeLoach, `Butch`/`Larry` Parrish, `James`/`Matt` Hatchett), a nickname that is
  not a prefix (`Angie` vs `Angela` O'Steen), or the candidate's first token is an extra given
  name (`Muhammad` Akbar Ali). Every one was confirmed by seat **and** party.
- 5 sitting legislators running outside the state-legislative pool — Tim Fleming (HD-114 →
  Secretary of State), Tanya Miller (HD-062 → Attorney General), Brian Strickland (SD-042 →
  Attorney General), Greg Dolezal (SD-027 → Lieutenant Governor), Josh McLaurin (SD-014 →
  Lieutenant Governor). These are the highest-value entries and the proposer cannot see them
  by design.
- 1 combined name-and-seat change — Teddy Reese. His SD-15 candidacy IS in the pool
  (`state_upper`), but the snapshot's `first_name` is the legal `Tremaine` (`Teddy` is the
  nickname) and his seat changed HD-140 → SD-15, so neither the name rule nor seat
  corroboration could reach him.

Validation over all 2,362 stored rolls: matched **216,752** / unmatched_reviewed 28,363 /
`no_crosswalk` **0** / `out_of_scope` **0**, 0 file errors, 0 zero-match rolls. Fan-out size:
**median 149 matched candidates per House roll, 42 per Senate roll** (Texas was 114 / 13).

`proposals-report.json` is the proposal pass only. The full-resolution report is 8 MB and is
never committed; it lives with the dataset under `/Users/shu/legiscan-data/`.

## Batches

- `batch-01/` — 18 rolls / 10 marquee enacted measures / 1,725 records, imported to local
  `voteapp` 2026-08-26. See `batch-01/PLAN.md` and `batch-01/JUDGING.md`.
