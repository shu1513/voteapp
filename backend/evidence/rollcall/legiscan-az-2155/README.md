# Arizona roll-call import — LegiScan session 2155 (2025 Regular Session)

Arizona's Fifty-seventh Legislature, First Regular Session. Config registered as the `AZ` key
in `legiscanStateConfigs.ts`. All work is on the local `voteapp` database; **production holds
no Arizona roll-call records.** No AI provider call is involved anywhere in this pipeline.

## Layout

- `crosswalk.json` — the reviewed people_id to candidate map, 91 entries.
- `legiscan-people-az-2155.json` — the people snapshot the importer reads.
- `survey/` — the measured description histogram, and the fully dispositioned worklist of
  every divided vote on a bill the Governor signed.
- `batch-01/` — 11 measures, 15 rolls, the judgments, the roll evidence, and the import ledgers.
- `batch-02/` — 21 measures, 26 rolls, which closes the divided-and-signed pool.
- `batch-03/` and `batch-04/` — the vetoed scope, 64 measures over 108 rolls, which closes the
  session.
- `CODE-FINDINGS.md` — feed and pipeline findings recorded but not fixed.

## The feed

1,854 bills, 6,727 roll calls, 91 people (60 Representatives and 30 Senators, plus one
mid-session replacement). Feed health is the cleanest tier: no file errors, no parse errors,
no tally mismatches, and nothing left surfaced for a human to classify.

The fetch stored 1,511 rows and reconciles exactly against the dataset:
247 excluded by measure type + 2,467 committee + 2,499 unrecorded + 3 duplicate identities +
1,511 stored = 6,727. Of the stored rows, 1,471 are floor votes and 40 are excluded questions
kept for audit. Dates run 2025-02-03 to 2025-06-27.

Arizona's vocabulary is the smallest of any state in the registry. Every roll call opens with
the chamber and a dash, and the only floor question either chamber prints on a measure is
`Third Reading`. The tally cut separates committee work without naming a single committee: the
largest committee roll is 21 of 60 House seats and 10 of 30 Senate seats, both under the 50%
committee ceiling, while every kept-family roll carrying a member list clears the 60% floor
threshold.

## The finding that shapes every Arizona batch

**Arizona publishes no member list for a concurrence vote.** All 82 House and 72 Senate
`Concurrence` rolls, and all 42 `House - Reconsider Third Reading` rolls, arrive with an empty
voter list and a zero tally, so the fetcher skips each one as an unrecorded vote. The vote
itself is public — the bill page prints `Senate FINAL 22-8` — but LegiScan carries no members,
so it can never be imported.

The consequence is a selection rule, not a footnote. When the second chamber amends a bill,
the originating chamber's third reading is a vote on its own earlier draft, and its vote on
the text that became law is the unrecorded final reading. Measured across the pool:

- 63 of 129 measures passed the second chamber unamended, so **both** chambers' third readings
  are on the enacted text;
- for the other 66, only the second chamber's third reading is, and the originating chamber's
  divided roll is dropped rather than described.

That rule removes 49 of 184 rows before any reading begins.

## Pool

| pool | rolls | measures |
| --- | --- | --- |
| divided third reading, bill signed by the Governor | 219 | 129 |
| of those, on the text that became law | 135 (85 House, 50 Senate) | 110 |
| divided, sent to the ballot as a concurrent resolution | 48 | 36 |
| divided, vetoed | 374 | 174 |

Arizona has a Republican legislature and a Democratic governor, so the 174 vetoed measures are
expected and the concurrent resolutions are the legislature's route around a veto. The divided
gate is the standard one: both sides non-zero, losing side at least a quarter of the winning
side.

## Crosswalk

91 entries: 54 mapped (32 House, 22 Senate) and 37 explicit nulls. Validated over all 1,511
evidence files — matched 38,389, unmatched reviewed 25,896, `no_crosswalk` 0, `out_of_scope` 0,
no file errors. **Fan-out is 31 candidates per House roll and 20 per Senate roll**, so a
measure divided in both chambers writes about 51 records.

Run at the pipeline default `--scope-from 2026-11-01`. Arizona's November rosters cover 28 of
30 Senate districts but only 21 of 30 House districts; the rest carry only July primary rows.
Widening to the primary would raise the pool from 125 to 203 candidates and proposals from 53
to 67, all of the gain in the House. A later House roster campaign plus an idempotent
re-import picks those members up without disturbing anything already written.

Two structural notes. Arizona's House has **two-member districts** — 30 districts electing 60
representatives — so two LegiScan people share one `HD-0NN`; the crosswalk maps people to
candidates rather than to seats, so this needs no special handling. Arizona also nests House
District N and Senate District N inside the same legislative district, so a member switching
chambers keeps the seat number and `seatAgrees` comes back false. Seven proposals were false
for exactly that reason and all seven were confirmed against the candidate's `current_office`.

**One identity error the name matcher would have written.** LegiScan person 24498 is the
sitting District 6 Representative: its own `name` field reads Mae Peshlakai, its district is
`HD-006`, and its `first_name` holds the legal name Jamescita with `nickname` Mae. Our database
holds a different person called Jamescita Peshlakai, who holds no legislative seat and is
running for the District 6 **Senate** seat, and the matcher proposed her. Linking that row
would have written this representative's votes onto someone else. The seat disagreement was the
only signal. Corrected by hand, with the reasoning in the crosswalk entry's note.

## Sources

Arizona's published record is the best of any state in this campaign.

- **Chaptered law**, plain HTML: `https://www.azleg.gov/legtext/57leg/1r/laws/<4-digit chapter>.htm`.
  It marks the edit in machine-readable CSS — `<span class=O>` is deleted text and
  `<span class=UP>` is new text — so what an act changed can be extracted without rendering a
  page. The struck-text hazard that cost Georgia, Maine, Montana and Kentucky so much time does
  not arise here. Helper: `az_law.py` (kept outside the repo with the dataset).
- **Two independent nonpartisan staff analyses per bill**, each version-stamped in its own
  title and **neither carrying a sponsor statement of intent**, so the Texas advocacy hazard
  does not recur: a House Bill Summary and a Senate Fact Sheet. The one headed `Signed` or
  `As Passed House` describes the enacted text. The House Summary's header also prints every
  committee and floor tally plus the chapter number, which is a free cross-check on the roll
  data — all 15 batch-01 tallies were audited against it and all 15 matched.
- **Official roll-call transcript** and full action history on the bill overview page
  (`https://apps.azleg.gov/BillStatus/BillOverview/<id>`, the LegiScan `state_link`). The
  Documents tab is script-rendered but is fed by a plain JSON endpoint,
  `https://apps.azleg.gov/api/DocType/?billStatusId=<id>`, so analyses can be fetched in bulk.

**Strike-everything amendments are a real gut-and-replace mechanism in Arizona** — 21 of the
129 pool measures carry one. Arizona announces them rather than hiding them: the chaptered
header and the bill page both print `old subject (NOW: new subject)`. Batch-01's SB 1247 is one,
and its description says so. Never judge an Arizona measure from its short title.

## Status — the 2025 session is closed

**Arizona holds 3,847 live records across 54 candidates and 2,373 tags on the local database.**
54 is every candidate the crosswalk maps — Arizona's Speaker votes, so there is no Texas or
Georgia style shortfall. **Production is untouched.**

| batch | scope | measures | rolls | records |
| --- | --- | --- | --- | --- |
| batch-01 | signed | 11 | 15 | 408 |
| batch-02 | signed | 21 | 26 | 682 |
| batch-03 | vetoed | 32 | 58 | 1,463 |
| batch-04 | vetoed | 32 | 50 | 1,294 |

**Every divided roll call on a measure that was signed or vetoed now carries a disposition, and
none is open.**

- `survey/divided-signed-worklist.tsv` — 184 rows: 41 imported, 49 not selected because that
  chamber voted an earlier draft, 94 dropped with a written reason.
- `survey/divided-vetoed-worklist.tsv` — 328 rows: 108 imported, 41 not selected, 171 dropped,
  8 deferred as direction calls.

**Fourteen research areas are covered across twenty-one area-and-direction pairs**, with both
directions present in `civil_rights`, `corporate_accountability`, `election_integrity`,
`environment_and_public_health`, `public_safety_and_crime_control`, `social_programs_and_welfare`
and — across the two scopes — the elections pair. Counted from the tag table, not from memory.

### Why the vetoed scope mattered here

Arizona has a Republican legislature and a Democratic governor. Under the divided-and-enacted
gate alone, the session reads as bipartisan housekeeping: the enacted batches produced housing,
health-coverage and criminal-law measures, and not one measure on immigration, firearms or
diversity policy. The vetoed pool is where the parties differ, and it supplied `gun_control`,
`immigration` and `anti_corruption` — three areas Arizona would otherwise have no coverage in at
all — plus eleven civil rights measures and the whole groundwater fight.

## What is left in Arizona

1. **Promotion to production**, which holds no Arizona records. Three duplicate-retirement files
   must be re-run there: `batch-02/`, `batch-03/` and `batch-04/duplicate-retirements.json`.
2. **Four vaccine-adjacent direction calls**, deferred rather than dropped, marked
   `deferred:direction-call` on the vetoed worklist: HB 2012, HB 2058, HB 2063 and HB 2257. The
   standing instruction after the Florida fluoride decision is to escalate a contested-evidence
   direction rather than assume one.
3. **48 divided votes on 36 ballot referrals**, unreachable while concurrent resolutions are
   dropped by measure type — `CODE-FINDINGS.md` finding 1. This is the largest remaining gap,
   and in a state where the legislature routes around a veto by going to the voters, it is not a
   small one.
4. **The 2026 session, LegiScan 2235**, untouched. It needs an `AZ-2235` registry key only,
   following the MO-2226 precedent, with no code change.
5. Optionally, widening scope to the July primary (`--scope-from 2026-07-01`), which would add
   about 14 sitting representatives to the House fan-out; a re-import picks them up
   idempotently.
