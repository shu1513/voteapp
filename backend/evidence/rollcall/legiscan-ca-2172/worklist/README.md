# California open pool — one ranked worklist

Built 2026-09-02 from the 08-30 dataset cut (`~/legiscan-data/ca-2172-0830`) and the fetch
evidence (`~/legiscan-data/ca-2172-evidence`). Regenerate with `pool.py` then `rank.py`.

Every future batch should draw from `worklist.json` rather than re-deriving the pool. It exists
so the version check and the budget-package screen are paid **once** for the whole campaign
instead of once per batch.

## How the pool narrows

| stage | measures | what it removes |
| --- | --- | --- |
| divided floor roll on a final-text bill, not yet worked | 332 | non-final-text, lopsided, committee rolls |
| **version check** (`vote_date >= last Amended text date`) | 270 | 62 — a quarter of the pool, at zero cost |
| budget / trailer / omnibus titles | 250 | 20 — no single nameable subject |

Of the 250 live measures: **133 carry an Assembly roll**, 111 are Senate-only, 13 Assembly-only.
Seven have a one-word title (`Health.`, `State government.`) and need a read before they can be
judged or dropped.

## Ordering

`value` = 6 per Assembly roll + 1 per Senate roll. All 80 Assembly seats are on the November
ballot against 20 of 40 Senate seats, so an Assembly roll is worth about six Senate rolls. The
file is sorted by that value, so **working it top-down is already the right order**.

## What the file does NOT decide

`worklist.json` is a triage artifact, not a judgment. It records the roll to use per chamber
(latest version-passing roll) and a `pkg`/`vague` hint. Filters 3 and 5 — nameable subject and
stance-defensibility — still need the digest read, and the duplicate-date and rescission screens
still need the official history. The count here (250) is an upper bound on what will be judged;
batch-07 judged 8 of 31.

## Known permanent exclusions

AB 863 and AB 483 have only pre-amendment divided votes and can never be worked.
