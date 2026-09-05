# Georgia 2026 Special Session (LegiScan session 2268)

Phase-4 LegiScan roll-call run for the election-equipment special session
Georgia held from 17 to 23 June 2026.

Why this session: it is the most recent Georgia floor record before the
November 2026 election, and 208 of the 234 members who voted (89%) are on that
ballot. That is the highest overlap of any Georgia session.

## Dataset

Downloaded with the operator's LegiScan key (main checkout `backend/.env` only):

    getDatasetList&state=GA  ->  session_id 2268
    getDataset&id=2268&access_key=<ak>

Extracted to `/Users/shu/legiscan-data/ga-2268/`, outside the repository.

## Configuration

`GA-2268` in `backend/src/pipeline/rollcall/legiscanStateConfigs.ts`, sharing
the hoisted Georgia question vocabulary with the other three registered
sessions.

## Survey and hygiene

Checked over all 280 roll calls in the dataset.

| check | result |
| --- | --- |
| duplicate `roll_call_id` | 0 |
| roll calls with an empty member list | 0 |
| `total != yea + nay + nv + absent` | 0 |
| committee-sized tallies | 0 |
| roll dates | 2026-06-17 to 2026-06-23, all inside the session |
| rolls whose date is absent from their own bill's history | 0 |
| unmatched descriptions on kept bill types | 0 |

The fetch stored 272 rows, 9 of them floor votes on kept bill types. The other
271 roll calls are procedural: motions to table, to adjourn and to resolve
debate, amendment adoptions, and votes on resolutions the campaign does not
keep.

## Selection

Nine divided roll calls, four of them on the one enacted measure, SB 3. The
ladder leaves two.

| measure | chamber | roll | date | tally | question |
| --- | --- | --- | --- | --- | --- |
| SB 3 | house | 1711338 | 2026-06-23 | 94-79 | passage of the House substitute |
| SB 3 | senate | 1711341 | 2026-06-23 | 36-16 | agreeing to the House substitute |

## Crosswalk

234 people rows, 211 mapped to a November 2026 Georgia candidate, 23 explicit
nulls with a written reason.

`rollcall:legiscan:resolve` over all 280 stored rolls reported 0 file errors, 0
members without a crosswalk entry and 0 crosswalk people missing from the
snapshot.

## Import

`batch-01/` holds the two roll evidence files, the judgments, and the dry-run
and live import reports. The live run wrote 206 candidate records. Report
totals, the run-stamp predicate and the table delta all agree at 206.
