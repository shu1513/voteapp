# Plan, Georgia 2026 special session, batch 01

Scope: every divided roll call on an enacted measure in LegiScan session 2268.
That is the whole session, so there is no batch 02.

Steps run, in order:

1. Register `GA-2268` in the LegiScan config registry, sharing the hoisted
   Georgia question vocabulary. Typecheck and unit tests green.
2. `rollcall:legiscan:fetch` over the extracted dataset. 272 rows stored, 9 of
   them floor votes on kept bill types, 0 file errors.
3. Build the crosswalk from the 2025-2026 session's committed entries plus the
   resolver's proposals, accepting only proposals where the seat agrees.
4. `rollcall:legiscan:resolve` over all 280 stored rolls to validate it.
5. Apply the divided gate and the selection ladder. 9 divided, 4 enacted, 1
   measure, 2 rolls after one-per-measure-per-chamber.
6. Download the enrolled text through the LegiScan API, verifying byte length
   and MD5 against the dataset manifest, and read it in full.
7. Write the judgments, lint them, check every description cites its own tally.
8. `rollcall:judge`, then import dry run, then the live import.
9. Reconcile three ways and sweep for duplicates.

Result: 206 candidate records across 206 candidates.
