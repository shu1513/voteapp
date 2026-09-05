# Georgia 2023 Special Session (LegiScan session 2114)

Phase-4 LegiScan roll-call run for the redistricting special session that
Georgia held from 29 November to 7 December 2023. It sits beside the regular
sessions in `../legiscan-ga-2167/` (2025-2026) and `../legiscan-ga-2008/`
(2023-2024).

Why this session: a federal court had ruled that the state House, state Senate
and congressional maps drawn in 2021 broke federal voting rights law. The
General Assembly met for nine days and passed three replacement maps. Those are
the only three measures in the session, every one of them was close, and 179 of
the 236 members who voted (76%) are on the November 2026 ballot.

## Dataset

Downloaded with the operator's LegiScan key (main checkout `backend/.env`
only — never Render, never printed):

    getDatasetList&state=GA  ->  session_id 2114
    getDataset&id=2114&access_key=<ak>

Extracted to `/Users/shu/legiscan-data/ga-2114/`, outside the repository.

## Configuration

`GA-2114` in `backend/src/pipeline/rollcall/legiscanStateConfigs.ts`. It shares
the hoisted `GEORGIA_KEPT_QUESTIONS` and `GEORGIA_EXCLUDED_QUESTIONS` lists with
every other registered Georgia session, so one vocabulary covers all four. The
lists were widened for the older sessions and re-run against the already
imported 2167 session: exactly one roll changed classification there, and it
changed to `excluded`, so no imported 2167 record moved.

## Survey and hygiene

Checked over all 33 roll calls in the dataset before anything was judged.

| check | result |
| --- | --- |
| duplicate `roll_call_id` | 0 |
| roll calls with an empty member list | 0 |
| `total != yea + nay + nv + absent` | 0 |
| committee-sized tallies | 0 |
| roll dates | 2023-11-29 to 2023-12-07, all inside the session |
| rolls whose date is absent from their own bill's history | 0 |
| unmatched descriptions on kept bill types | 0 |

The fetch stored 20 rows, 7 of them floor votes on kept bill types.

## Selection

Seven divided roll calls survived the divided gate, all seven on enacted
measures, covering three bills. The ladder then keeps one roll per measure per
chamber, preferring the last divided vote, which leaves six.

| measure | chamber | roll | date | tally |
| --- | --- | --- | --- | --- |
| HB 1 (state House map) | house | 1358055 | 2023-12-01 | 101-77 |
| HB 1 | senate | 1361975 | 2023-12-05 | 32-21 |
| SB 1 (state Senate map) | senate | 1358054 | 2023-12-01 | 32-23 |
| SB 1 | house | 1361973 | 2023-12-05 | 98-71 |
| SB 3 (congressional map) | senate | 1361977 | 2023-12-05 | 32-22 |
| SB 3 | house | 1362081 | 2023-12-07 | 98-71 |

## Crosswalk

236 people rows, 180 mapped to a November 2026 Georgia candidate, 56 explicit
nulls with a written reason. One entry carries a name-change note: people id
24430 is printed as Lynn Gladney here and as Lynn Heffner in the 2025-2026
session, the same person in the same House district 130.

`rollcall:legiscan:resolve` over all 33 stored rolls reported 0 file errors, 0
members without a crosswalk entry and 0 crosswalk people missing from the
snapshot.

## Import

`batch-01/` holds the six roll evidence files, the judgments, and the dry-run
and live import reports. The live run wrote 518 candidate records. Report
totals, the run-stamp predicate and the table delta all agree at 518.
