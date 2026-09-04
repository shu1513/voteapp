# Alabama 2022 Regular Session batch-01 — selection plan

The measures that were divided AND became law. The same five filters as every batch in this campaign,
with the roll-attribution check run first.

0. **The roll must be filed under the right bill.** Every divided roll's printed roll call number was
   checked against its own bill's history. This session's result is recorded in
   `../survey/divided-worklist.tsv` under `roll_number_in_history`.
1. **Divided** — the losing side is at least a quarter of the winning side.
2. **Consequential** — the measure became law.
3. **A nameable subject.**
4. **One roll per measure per chamber, preferring the vote on the enacted text.** Where a chamber's
   divided vote is followed by a later vote on the same measure, the divided vote is imported and the
   later one is named in `acknowledge_later_rolls`, with the description saying which version that
   chamber voted.
5. **A defensible direction, or a deliberate no-stance import.**

## What is in the batch

17 rolls on 15 measures, 739 records across 80 candidates. The full list, with
tallies and labels, is in `judgments.json`.

## Dropped under filter 5

Dropped: HB 119 and HB 176, both alcohol sales, and HB 414 (a 911 board certification scheme), all outside the taxonomy.

## Remaining

The divided rolls on measures that did NOT become law are deferred to a batch-02 that this pull request
does not carry. They are dispositioned as such in `../survey/divided-worklist.tsv`.
