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
- 1 roll call surfaced and never queued, the doubled roll held below

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

## Crosswalk

`crosswalk.json` maps LegiScan `people_id` to a VoteApp candidate id.
`legiscan-people-mi-2183.json` is the roster as the dataset held it, kept so a
later run can diff it.

148 entries: **113 mapped, 35 explicit nulls, 0 unresolved.** By chamber, 96 of
110 representatives and 17 of 38 senators are mapped.

The resolver proposed all 113 and every one was accepted after review. 108
matched on both names and 5 on a first-name prefix, each of those an ordinary
short form (Curtis to Curt, Matthew to Matt) on a candidate holding the same
district.

Eleven proposals carry a seat disagreement and all eleven are real. Ten are
sitting House members running for a Senate seat, which is ordinary here because
all 38 Senate seats are on the 2026 ballot and House members are term-limited.
The eleventh is Senator Dayna Polehanki, whose seat is SD-005 in the LegiScan
roster and State Senate District 8 in our district records. Each was checked
against the candidate's own election record.

The 35 nulls are legislators with no November 2026 candidacy in the database:
20 senators and 15 representatives. Michigan's term limits are why — a
legislator who has served twelve years cannot run again. Each of the 35 was
searched by surname across every Michigan November 2026 race, not only the
legislative ones, and none matched.

**One limit worth recording.** Michigan's Governor, United States Senator and
United States Representative elections exist in the database for November 2026
but hold no candidates at all. A legislator running for one of those offices
cannot be reached today. Senator Mallory McMorrow, who is running for the
United States Senate, is the notable case. Revisit the crosswalk if those
rosters are filled.

Validation over all 2,571 stored roll calls: 83,130 member matches, 22,197
reviewed and deliberately unmatched, **0 with no crosswalk entry and 0 out of
scope**, 0 file errors.

## Fan-out

Measured over the session's floor rolls, not estimated:

| chamber | floor rolls | median candidates reached | range |
| --- | --- | --- | --- |
| House | 701 | 93 | 78 to 96 |
| Senate | 499 | 16 | 13 to 17 |

This makes the House slots far more valuable than their count suggests. The
pool's 22 House slots reach roughly 2,000 records between them; its 41 Senate
slots reach roughly 650.

## Held roll calls

Seventeen rolls are held: one doubled filing, and sixteen last-day rolls whose
member lists disagree with the journal.

LegiScan stored a single House action as two roll calls:
HB 4002's House vote of 2025-02-20, 81-29. Roll 1497863 is described
`House Third Reading: Roll Call #12` and roll 1550992
`House Third Reading: Roll Call Roll Call #12`, and both point at the one
history line. The descriptions differ, so the fetcher's identity key cannot
collapse them. Roll 1497863 is the one to use.

The doubled `Roll Call Roll Call` caption is deliberately left unmatched by the
kept patterns, so any future double filing surfaces for a person to look at
instead of being queued as a vote.

The other sixteen are House rolls of 2026-07-03, the session's last sitting
day. Every Michigan roll in the dataset lists only the yea and nay voters, so
the member list is the tally. On 16 of that day's 55 House floor votes the
list disagrees with the journal line the bill history carries for the same
date and roll number — usually one to three members missing, twice a yea where
the journal has a nay. HB 6130 is the closely divided one, 60-45 in LegiScan
against 60-48 in the journal. Which members are missing cannot be read from
the dataset, so the counts are not corrected; each roll stays held until its
list is checked against the House Journal. The day's other 39 rolls match
their journal lines exactly. The ids are in `legiscanStateConfigs.ts` under
`MICHIGAN_LAST_DAY_TALLY_HOLDS`, and CODE-FINDINGS.md §5 has the method.

## Scope

The operator chose the enacted pool first, then the pool of measures that
passed a chamber and never became law. The second scope is large here — 350
measures — and in North Dakota and West Virginia it carried the state's real
disagreements and reached research areas the enacted pool never touches.
