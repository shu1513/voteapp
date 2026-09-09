# Nebraska batch-01

Four measures, four roll calls, 48 records across 12 candidates.

| measure | roll | date | tally | area | direction |
| --- | --- | --- | --- | --- | --- |
| LB 266, rent control ban | 1535598 | 2025-04-03 | 37-10 | housing_affordability | yes = against |
| LB 89, Stand With Women Act | 1580669 | 2025-05-28 | 33-16 | civil_rights | yes = against |
| LB 258, minimum wage | 1624711 | 2026-02-05 | 33-16 | reduce_wealth_gap | yes = against |
| LB 966, Hunger-Free Schools Act | 1680249 | 2026-04-10 | 38-11 | social_programs_and_welfare | yes = for |

## Why the batch is small

Reach, not supply, is the binding constraint in Nebraska. Only 12 of the 49
sitting senators are on the November 2026 ballot, so one roll call reaches
about ten candidates where a Maryland or Pennsylvania roll reaches well over a
hundred. Nebraska elects half its districts each cycle, and most senators whose
seat is up this year are barred from running again by the two-term limit. That
is the calendar working normally, not a gap in our roster.

The pool itself is 49 divided roll calls on 49 measures, of which five turned
out to be vetoed. Every one of the 49 carries a disposition in
`survey/divided-enacted-worklist.tsv`, and 34 are marked as candidates for a
second batch, so no measure needs to be triaged twice.

## How each measure was selected

All five filters were applied.

1. **The vote was close.** Each smaller side is at least a quarter of the
   larger: 10 against 37, 16 against 33, 16 against 33, and 11 against 38.
2. **The measure became law.** Each bill's own history records "Approved by
   Governor". This was checked bill by bill rather than read off LegiScan's
   status field, which is wrong for five measures in this pool.
3. **The subject is one a voter would recognize:** school sports, the minimum
   wage, rent control, and school meals.
4. **One roll call per measure.** Nebraska has one house, so each measure has
   one Final Reading vote to take, and each of these four is the last kept roll
   on its bill. LB 258 is the only one with an earlier Final Reading vote, and
   that vote failed and is held in the config.
5. **Each carries a research area with a direction that can be defended.** None
   of them lands on a no-stance label.

## Version check, per roll

Final Reading is the last step before a bill goes to the Governor, so a roll
there is a vote on the text that became law, provided nothing changed
afterwards. That was checked on each bill's own history: none of the four has a
text-changing action after its Final Reading vote. Each roll's tally was also
matched to the tally Nebraska's history prints for that vote.

## What was left out of this batch, and why

The four measures above were read in full. Two more were considered and not
taken:

- **LB 319, changing food-assistance eligibility.** It looked like a clean
  measure until its history was read: the Governor returned it without
  approval, the override failed 24-24, and it never became law. It belongs to
  the vetoed pool, not this one.
- **LB 965, a criminal-justice bill.** Its own title lists a dozen unrelated
  subjects, from sexual abuse offenses to attorney's fees, victim notification,
  Brady-Giglio disclosures and county conflict counsel. Deciding whether a
  single direction can honestly describe it needs a full read of a long act,
  which this batch did not do. It stays a candidate for batch-02 rather than a
  drop on the merits.
