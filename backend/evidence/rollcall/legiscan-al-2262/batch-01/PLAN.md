# Alabama 2026 First Special Session batch-01 — selection plan

## How the batch was selected

The same filters as the two regular-session batches, plus the roll-attribution check that the 2026
regular session made necessary.

0. **The roll must be filed under the right bill.** The 2026 regular session files some physical
   votes under more than one bill (`../../legiscan-al-2218/CODE-FINDINGS.md` §1), so each candidate
   roll's printed roll call number was checked against its own bill's history. All three pass:
   HB 1 House Roll Call 2, HB 1 Senate Roll Call 10, SB 1 Senate Roll Call 7.
1. **Divided** — the losing side is at least a quarter of the winning side. All 3 of the 3 kept floor
   votes in this session are divided.
2. **Consequential** — the measure became law. Both measures did.
3. **A nameable subject** — both are statewide election law. Neither is a local act.
4. **One roll per measure per chamber, preferring the vote on the enacted text.** Each of the three
   is the only kept floor vote for its measure in its chamber, so nothing is superseded.
5. **A defensible direction, or a deliberate no-stance import.** Both measures are imported with no
   stance. The reasoning is in `JUDGING.md`.

## What is in the batch

3 rolls on 2 measures, both enacted, 150 records across 122 candidates.

| Measure | Roll | Vote | Label |
|---|---|---|---|
| HB 1 special primary after congressional redistricting | House 1694623, 2026-05-06 | 75-29 | general, no stance |
| HB 1 | Senate 1695918, 2026-05-08 | 27-8 | general, no stance |
| SB 1 special primary after State Senate redistricting | Senate 1694713, 2026-05-06 | 26-7 | general, no stance |

## Nothing was dropped

Every kept floor vote in this session is in the batch. The other six roll calls are three failed
amendment adoptions, two previous-question motions and one identity duplicate, all correctly
excluded by the shared Alabama vocabulary before selection began.

## SB 1's House vote cannot be represented

SB 1's bill history records a House passage vote on 2026-05-08, but no such roll call exists in the
dataset. What the dataset holds for that chamber and day is the previous-question motion. Missouri's
feed had the same shape. Nothing here can recover the missing vote, so SB 1 is represented by its
Senate vote alone.
