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
