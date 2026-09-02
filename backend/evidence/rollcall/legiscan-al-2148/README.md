# Alabama roll-call votes — LegiScan session 2148 (2025 Regular Session)

Source dataset: the LegiScan bulk dataset for Alabama's 2025 Regular Session, session id 2148,
downloaded 2026-08-31. It holds 1,449 bills, 2,851 roll calls and 139 people. The dataset and the
2,818 stored roll-call evidence files live outside this repository at
`/Users/shu/legiscan-data/al-2148/` and `/Users/shu/legiscan-data/al-2148-evidence/`; this directory
keeps only the curated subset, as every other state's evidence directory does.

## Layout

- `crosswalk.json` — the identity file: one entry per LegiScan person, mapping to one of our
  candidate rows or to an explicit null.
- `legiscan-people-al-2148.json` — the people snapshot the crosswalk was written against.
- `survey/` — the description histogram the config entry was written from, and the worklist of
  every divided floor vote.

## How Alabama's feed behaves

Feed health is the cleanest tier: no repeated roll call ids, no summary-only rolls, no tally
mismatches, no parse errors, and no committee votes at all. Every tally is a whole-chamber tally.

**The Budget Isolation Resolution is printed twice, and its second caption looks like passage.**
Alabama's constitution bars most bills from being taken up before the budget bills pass unless the
chamber first adopts a Budget Isolation Resolution by a three-fifths vote. LegiScan files that one
vote as two roll calls: `HBIR:`/`SBIR: Passed by House of Origin|Second House` and
`Third Reading in House of Origin|Second House`. All 698 pairs in this session are identical in
tally and in member list, and there is no `Third Reading` caption without its Budget Isolation
Resolution twin. Both families are excluded by the config. The vote that passes an Alabama bill is
`Motion to Read a Third Time and Pass`, with or without ` as Amended`.

**Local acts are not filtered by any rule.** Alabama passes county bills on the votes of that
county's delegation alone, but the roll still lists the whole chamber with everyone else recorded as
not voting, so the tally is chamber sized. SB 314 (Shelby County) passed the House 10 to 3 with 90
members not voting. Local bills are 175 of the 917 kept rolls but only 2 of the 34 divided ones, and
selection drops them by subject.

## Pool

917 kept floor votes, of which 773 are unanimous. 34 are divided under the campaign's gate — a
losing side at least a quarter the size of the winning side. 17 of those 34 are on measures that
became law, across 13 measures; the other 17 are on measures that passed one chamber and died.
Every divided roll is listed in `survey/divided-worklist.tsv`.

## Identity

The crosswalk holds 139 entries: 108 proposed by the resolver and all accepted, 10 added by hand,
and 21 explicit nulls for members with no November 2026 candidate row.

Resolution over all 2,818 stored rolls: 150,580 matched, 23,782 reviewed and unmatched, no member
without a crosswalk entry, and no member out of scope. Fan-out is a median of 86 candidates per
House roll (highest 89) and 26 per Senate roll (highest 29). Every Alabama seat is on the November
2026 ballot, so every mapped member is a ballot candidate; the ceiling is our own roster, which
covers 118 of the 140 seats.

### Why ten entries were added by hand

Alabama's people file puts the **legal** first name in `first_name` while the roster carries the
working name, which the resolver's prefix rule cannot bridge:

- William "Brock" Colvin, House District 26
- Jerimie "Chad" Robertson, House District 40
- James "Jay" Hovey, Senate District 27
- William "Billy" Beasley, Senate District 28
- Richard Rehm, rostered as Rick Rehm, House District 85 ("rick" is not a prefix of "richard")
- Stephen Hurst, rostered as Steve Hurst, House District 35 ("steve" is not a prefix of "stephen")

Three more are roster-side name differences: Merika Coleman-Evans is rostered as Merika Coleman;
Craig Lipscomb is rostered as B. Craig Lipscomb, whose first token is a single letter; and Phillip
Ensler is rostered as Philip Ensler.

Philip Ensler is also the one member the resolver could never reach: he sits for House District 74
but is running **statewide** in November 2026, and the resolver only reads the state-legislative
candidate pool.

The tenth is Will Barfoot, whose name matched two candidate rows, so the resolver's
one-match-each-way rule declined both. Alabama reverted to its 2021 Senate map, and Barfoot is
running again in his own Senate District 25; the Senate District 26 row is left over from the
court-drawn map. He is mapped to the District 25 row, and the District 26 row is a roster defect
this campaign does not fix.

### The one seat disagreement, and it is real

Matt Woods appears in the 2025 people file as the House District 13 member, but his only November
2026 candidacy is Senate District 5. Both are correct: he won the June 2025 Senate District 5
special election after Greg Reed left that seat, and resigned House District 13 to take it. Senate
District 5 is the one seat missing from the 2025 people file for exactly that reason. His 2025 votes
were cast in the House and belong to the same person.
