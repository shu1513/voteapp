# Plan, Georgia 2023 special session, batch 01

Scope: every divided roll call on an enacted measure in LegiScan session 2114.
That is the whole session, so there is no batch 02.

Steps run, in order:

1. Register `GA-2114` in the LegiScan config registry, sharing the hoisted
   Georgia question vocabulary. Typecheck and unit tests green.
2. `rollcall:legiscan:fetch` over the extracted dataset. 20 rows stored, 7 of
   them floor votes on kept bill types, 0 file errors.
3. Build the crosswalk from the 2025-2026 session's 242 committed entries plus
   the resolver's proposals, accepting only proposals where the seat agrees.
4. `rollcall:legiscan:resolve` over all 33 stored rolls to validate it.
5. Apply the divided gate and the selection ladder. 7 divided, 7 enacted, 3
   measures, 6 rolls after one-per-measure-per-chamber.
6. Download the text each chamber voted through the LegiScan API, verifying
   byte length and MD5 against the dataset manifest.
7. Write the judgments, lint them, check every description cites its own tally.
8. `rollcall:judge`, then import dry run, then the live import.
9. Reconcile three ways and sweep for duplicates.

Result: 518 candidate records across 180 candidates.
