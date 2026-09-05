# Georgia 2023-2024 Regular Session (LegiScan session 2008)

Phase-4 LegiScan roll-call run for the two-year regular session that ran from
9 January 2023 to sine die on 29 March 2024. It sits beside the 2025-2026
regular session in `../legiscan-ga-2167/` and the two special sessions in
`../legiscan-ga-2114/` and `../legiscan-ga-2268/`.

Why this session: 181 of the 241 members who voted (75%) are on the November
2026 ballot, and the session carries the marquee fights of the term.

## Dataset

Downloaded with the operator's LegiScan key (main checkout `backend/.env`
only — never Render, never printed):

    getDatasetList&state=GA  ->  session_id 2008
    getDataset&id=2008&access_key=<ak>

Extracted to `/Users/shu/legiscan-data/ga-2008/`, outside the repository.

## Configuration

`GA-2008` in `backend/src/pipeline/rollcall/legiscanStateConfigs.ts`, sharing
the hoisted `GEORGIA_KEPT_QUESTIONS` and `GEORGIA_EXCLUDED_QUESTIONS` lists with
every other registered Georgia session.

## Survey and hygiene

Checked over all 2,348 roll calls in the dataset before anything was judged.

| check | result |
| --- | --- |
| duplicate `roll_call_id` | 0 |
| roll calls with an empty member list | 0 |
| `total != yea + nay + nv + absent` | 0 |
| committee-sized tallies | 0 |
| roll calls not attached to a bill | 0 |
| roll dates | 2023-01-09 to 2024-03-29, all inside the session |
| unmatched descriptions on kept bill types | 0 |

**Rolls whose date is absent from their own bill's history: 28**, four of them
inside the divided-and-enacted pool. All four are sine die overnight sittings:
the roll call is timestamped after midnight while the journal dates it to the
previous legislative day. Each was matched to a history line one day earlier in
the same chamber recording the same action (HB 189 conference report, SB 189
House substitute, SB 37 Senate amendment, HB 189 again). Not cross-session
contamination, which is the hazard this check exists to catch.

The fetch stored 2,201 rows, 1,274 of them floor votes on kept bill types.

## Selection

176 roll calls survived the divided gate on kept bill types. 115 of those are on
measures that became law, across 61 measures. The one-roll-per-measure-per-
chamber ladder leaves 91 rolls, nine of which would need
`acknowledge_later_rolls`.

Those 61 measures are then narrowed by the nameable-subject and
stance-defensible filters, which is where most of them fall away, and worked in
marquee batches the way the 2025-2026 session was. See each batch's PLAN.md.

## Crosswalk

241 people rows, 182 mapped to a November 2026 Georgia candidate, 59 explicit
nulls with a written reason. One entry carries a name-change note: people id
24430 is printed as Lynn Gladney here and as Lynn Heffner in the 2025-2026
session, the same person in the same House district 130.

`rollcall:legiscan:resolve` over all 2,201 stored rolls reported 0 file errors,
0 members without a crosswalk entry and 0 crosswalk people missing from the
snapshot.

## Batches

| batch | measures | rolls | records |
| --- | --- | --- | --- |
| batch-01 | 9 | 16 | 1,367 |
| batch-02 | 7 | 10 | 859 |
