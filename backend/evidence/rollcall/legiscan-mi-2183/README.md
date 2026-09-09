# Michigan 103rd Legislature, 2025-2026 — LegiScan session 2183

Roll-call vote import, phase 4. See `docs/plans/roll-call-vote-import.md`.

Michigan runs one two-year session, so this single session covers both years
and the state needs only one registry entry.

## Source

LegiScan bulk dataset for Michigan's 103rd Legislature, downloaded 2026-09-09
with the key that lives only in the main checkout's `backend/.env`. LegiScan's
own dataset list gives the cut date as 2026-09-06 and the size as 14,396,111
bytes, and the downloaded archive matched that size exactly.

The extracted dataset lives outside the repository at
`~/legiscan-data/mi-2183/MI/2025-2026_103rd_Legislature`, because a whole
session is far larger than the curated evidence this directory keeps.

Dataset contents: 4,016 bills, 2,600 roll calls, 148 people. The 148 people are
exactly the 110 members of the House of Representatives plus the 38 senators.
Both chambers are on the November 2026 ballot.

**One download note.** `getDatasetRaw` returned the raw ZIP bytes, not a JSON
envelope with a base64 field. Save the response straight to a `.zip` file.

## Survey

`survey/` holds the report written by:

    npm run rollcall:legiscan:fetch -- --state MI \
      --dataset-dir ~/legiscan-data/mi-2183/MI/2025-2026_103rd_Legislature \
      --survey --evidence-dir evidence/rollcall/legiscan-mi-2183/survey

The state's configuration entry in
`backend/src/pipeline/rollcall/legiscanStateConfigs.ts` was written from that
report's description histogram and from nothing else.

Michigan puts a suffix on every description. Floor votes end ` Roll Call #<n>`;
committee votes end with a date such as ` 6/5/2025`. Set those two suffixes
aside and the 1,041 distinct descriptions fold into 45 families. Only sixteen
distinct floor descriptions exist in the entire session.

Classification of this session, confirmed by a dry-run fetch:

- 1,199 floor votes kept
- 1,371 roll calls excluded by rule as committee reports
- 26 roll calls on excluded measure types (simple and concurrent resolutions)
- 2 roll calls collapsed as duplicate identities
- 1 committee-sized roll call rejected on its tally
- 1 roll call surfaced and never queued, the roll held below

Those add up to the dataset's 2,600 roll calls. There were no parse errors, no
file errors, no unrecorded votes, and nothing left with an unknown question.

## What is unusual about Michigan

### `Given Immediate Effect` is the passage vote, not a separate vote

This is the opposite of West Virginia, where the vote on when an act takes
effect is a separate question that must be excluded. Michigan's journal prints
one line for both:

    Passed; Given Immediate Effect Roll Call #5 Yeas 67 Nays 38 Excused 0 Not Voting 5

LegiScan stores that description with the word `Passed;` dropped, so it reads
`House Third Reading: Given Immediate Effect Roll Call #5`. Every floor vote in
the session was matched to its own bill-history line to confirm this: 643 of
the 644 House rolls in the family sit on a history line reading
`Passed; Given Immediate Effect`.

Reading the family the West Virginia way would have discarded 644 of the
House's 717 floor votes.

### Michigan's most common concurrence vote states no question at all

81 rolls carry a description that is only a roll number:
`House Third Reading: Roll Call #12` and
`Senate Third Reading: Roll Call: Roll Call # 44`. The bill history's own line
is no better, reading `Roll Call Roll Call #12 Yeas 81 Nays 29`.

The question sits in the history line **before** it. Across the family, 37 are
`Senate Substitute (h-1) Concurred In`, 23 are `House Substitute Concurred In`,
15 are a resolution `Adopted`, four are amendment concurrences, and three are
nonconcurrences.

The family cannot be dropped. Ten of the 76 closely divided rolls on measures
that became law wear this caption, and they are the votes on the text that
became law, which is the only text a candidate record may describe. It is
classified `concurrence`, because that is what all but a handful are.

**The caption cannot tell a concurrence from a nonconcurrence.** Read the
preceding history line for every roll selected out of this family.

### Michigan letters its joint resolutions instead of numbering them

The session runs `HJR A` through `HJR AA` and `SJR A` through `SJR N`, 40
measures in all, and Michigan cites them exactly that way. A joint resolution
here is a proposed amendment to the state constitution.

The measure-id parser required letters followed by digits, so every one of
these bill files failed to parse and the fetch exited on the file errors. The
parser now also accepts an instrument prefix ending in `JR` followed by a one
or two letter designator. Only two of the 40 carry a closely divided floor
vote, and neither became law, but the fix is required to fetch Michigan at all.

None of these can ever reach LegiScan status 4. A Michigan constitutional
amendment goes to the voters, not to the Governor.

### Divided government makes the enacted pool thin

Michigan has a Democratic Governor and Senate and a Republican House. The
measured effect is large. 442 of the 1,218 floor votes are closely divided,
across 415 measures — but only 59 of those measures became law.

## The pool

Closely divided means the smaller side is at least a quarter of the larger.

Measures carrying at least one closely divided floor vote, by LegiScan status:

| status | meaning | measures |
| --- | --- | --- |
| 2 | passed a chamber, went no further | 350 |
| 4 | became law | 59 |
| 1 | introduced | 6 |

**Closely divided and became law, on kept measure types: 53 measures, 63
measure-chamber slots, 76 roll calls.** By chamber, 22 House slots and 41
Senate slots.

## Fan-out

Michigan has legislative term limits, so a sitting legislator is often not on
the next ballot. A first pass matching the 148 legislators by surname against
the 329 Michigan legislative candidates on the November 2026 ballot leaves 36
without a match, most of them senators who cannot run again. Each of the 36
needs checking by hand when the crosswalk is built.

Working estimate before that check: a House roll reaches about 90 candidates,
a Senate roll about 22.

## Held roll calls

One roll is held. LegiScan stored a single House action as two roll calls:
HB 4002's House vote of 2025-02-20, 81-29. Roll 1497863 is described
`House Third Reading: Roll Call #12` and roll 1550992
`House Third Reading: Roll Call Roll Call #12`, and both point at the one
history line. The descriptions differ, so the fetcher's identity key cannot
collapse them. Roll 1497863 is the one to use.

The doubled `Roll Call Roll Call` caption is deliberately left unmatched by the
kept patterns, so any future double filing surfaces for a person to look at
instead of being queued as a vote.

## Scope

The operator chose the enacted pool first, then the pool of measures that
passed a chamber and never became law. The second scope is large here — 350
measures — and in North Dakota and West Virginia it carried the state's real
disagreements and reached research areas the enacted pool never touches.
