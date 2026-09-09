# Wyoming roll-call vote import

This directory holds the evidence for importing Wyoming legislative roll-call
votes into candidate records, for the November 3, 2026 election.

Two LegiScan sessions are in scope, both belonging to the 68th Wyoming
Legislature elected in 2024:

- session 2157, the 2025 general session
- session 2213, the 2026 budget session

LegiScan lists nineteen Wyoming sessions in all. Every other one is 2024 or
earlier and belongs to an earlier legislature, so none of them can carry a
vote by a member on the 2026 ballot. Wyoming held no special session in
either 2025 or 2026, so nothing is hidden behind a stale note.

## What the survey found

The survey ran over both extracted datasets before a single line of
configuration was written. The two sessions were surveyed separately.

| | 2025 general (2157) | 2026 budget (2213) |
|---|---|---|
| bills | 556 | 335 |
| roll calls | 1,575 | 1,252 |
| committee tallies | 692 | 368 |
| excluded questions | 252 | 533 |
| kept floor votes | 630 | 350 |
| unknown questions | 0 | 0 |
| file errors | 0 | 0 |
| parse errors | 0 | 0 |
| committee-body (`J`) rolls | 0 | 0 |

Three findings shaped the configuration.

**Wyoming prints the tally inside the description.** Every roll reads
`3rd Reading:Passed 60-0-2-0-0`, never a bare `3rd Reading:Passed`. The raw
histogram therefore has 708 rows for what are really a handful of questions.
No pattern in the state's configuration entry may be anchored at the end of
the question text; each one requires the first digit of the tally instead.

**The two sessions do not print the same questions.** In a budget session a
non-budget bill needs a two-thirds vote just to be introduced. The 2026
session holds 281 introduction roll calls; the 2025 session holds none. An
introduction vote is not a passage vote. Wyoming spells it two ways:
`Failed Introduction` for the ones that fall short, and
`Introduced and Referred to <committee>` for the ones that carry. The second
spelling is the dangerous one, because it names a committee but the tally is
the whole chamber, so no tally test can reject it. Ten of those rolls are
both closely divided and on bills that became law, and without the exclusion
they would have entered the pool as if they were passage votes.

**LegiScan's status field is not a test of enactment.** Five 2025 bills carry
status 5, vetoed, and became law anyway when the legislature overrode the
veto: HB 36, HB 64, HB 94, SF 127 and SF 132. The only reliable test is
whether `Assigned Chapter Number` appears in the bill's own history, and that
is the test used throughout this work.

Four questions are kept, all of them full-chamber votes. Third reading is
Wyoming's passage vote in both chambers. Concurrence is the chamber of origin
taking its bill back with the other chamber's amendments. A joint conference
committee report is adopted under its own number. A whole-bill veto override
is kept as well, because for the five bills above the override is the vote
that made the act law. Line-item vetoes on the budget bill are excluded: they
are votes on single lines of an act that had already become law.

The dataset carries only bill types B and JR, so no extra bill type has to be
opted in. Wyoming proposes its constitutional amendments as joint
resolutions, which the kept set already covers.

## The pool, measured before any batch size was promised

Across both sessions there are 982 kept floor votes. 216 of them are closely
divided under the standard gate, where the smaller side is at least a quarter
of the larger. 121 of those are on measures that became law.

| | kept floor | divided | share | divided and enacted | measures | measure-chamber slots |
|---|---|---|---|---|---|---|
| 2025 general | 630 | 153 | 24.2% | 91 | 57 | 77 |
| 2026 budget | 350 | 63 | 17.9% | 30 | 27 | 29 |
| both | 982 | 216 | | 121 | 84 | 106 |

Three alternative gates were measured as well, in case the standard gate cut
through Wyoming's party-line votes the way it does in a state with a small
minority caucus. It does not. At a fifth of the larger side the pool would be
128 rolls, at fifteen percent of votes cast 141, and at ten percent 207. The
standard gate leaves a healthy pool and needs no recalibration. Wyoming's
Republican supermajority is split between its Freedom Caucus and traditional
wings, and the split shows: nearly a quarter of 2025 floor votes are closely
divided, against 10.2 percent in West Virginia, the closest one-party
comparison in this campaign.

## Audits run before any judging

Every one of the 750 enacted kept floor votes was audited, not only the
divided ones.

- The tally printed inside each description was compared with the reported
  yea and nay counts. Zero disagreements.
- Each member list was compared with the reported total and the reported yea
  count. Zero disagreements.
- Each roll's caption and tally were looked for in its own bill's history,
  with the date checked. Every passage and concurrence caption matched
  exactly. The 53 that did not match are all conference report and veto
  override rolls, where Wyoming's bill history uses different wording for the
  same action. None is a misfiled roll.
- Member lists were hashed and grouped to look for one roll's list copied
  onto another bill. Restricting the check to contested votes on the same
  day leaves seven groups. Six are outside the divided pool. The seventh is
  SF 23 and SF 50, both 20-7 in the Senate on January 20, 2025 with the same
  members. Both bills' own histories report 20-7 on that date, so the tally
  is Wyoming's, not a parsing artifact, and seven senators voting no on two
  bills on one day is ordinary. Neither bill is in batch 01; the pair is
  flagged here so that whoever selects them checks the state's own vote sheet
  first.

Two roll calls in the whole feed are exact repeats that LegiScan issued under
two identifiers. The fetcher keeps the lower identifier of each pair. Neither
changes anything: the 2025 repeat is a 60-0 vote on SF 191, and the 2026
repeat is a failed veto override on HB 178, a bill that did not become law.

## The crosswalk

Both sessions seat 93 members. The proposer matched 35 in 2025 and 37 in
2026. Seven pairs were added by hand in each session:

| people id | member | why the proposer missed it |
|---|---|---|
| 22963 | J.D. Williams, House 2 | LegiScan prints the legal first name with dots; the ballot name is `JD Williams` |
| 20193 | Mike Yin, House 16 | legal first name `Michael`, working name in the nickname field |
| 24375 | Liz Storer, House 23 | legal first name `Elizabeth`, working name in the nickname field |
| 25322 | J.R. Riggins, House 59 | legal first name `Justin`, working name in the nickname field |
| 18487 | Michael Gierau, Senate 17 | legal first name `Michael`, ballot name `Mike` |
| 14226 | Eric Barlow, Senate 23 | running for Governor, so outside the state-legislative pool the proposer draws from |
| 23879 | Robert Davis, House 47 | running for Senate District 11, so the seats disagree; working name `Bob` sits in the nickname field |

Every unmatched candidate row was checked against its own office field. The
only sitting legislators among them are the seven above plus Justin Fornstrom
and Taft Love, whom the proposer matched on its own. Nobody else was missed.

Two seats changed hands between the sessions. Taft Love replaced Darin Smith
in Senate District 6, and Justin Fornstrom replaced John Eklund in House
District 10. Both replacements are 2026 candidates.

LegiScan's 2026 feed gives eight sitting senators the role `Rep` while their
district field reads `SD-0xx`: Hutchings, Olsen, Gierau, Laursen, Biteman,
Crago, Barlow and Salazar. The district is right and the role is wrong, and
the crosswalk follows the district.

Fan-out is 32 to 34 matched candidates per House roll and 9 to 10 per Senate
roll. The Senate number is low by design, not a coverage gap: all 62 House
seats are on the 2026 ballot but only about half of the 31 Senate seats are,
so roughly half the Senate can never match a 2026 candidacy.

## Batch 01

Ten measures, one roll call each, 281 candidate records, 214 research-area
tags. See JUDGING.md for how each was judged and why the drops were dropped.

## Reproducing this

The datasets are the LegiScan bulk downloads for sessions 2157 and 2213,
extracted. They are not committed; they are large and they are LegiScan's.

    npm run rollcall:legiscan:fetch -- --state WY --dataset-dir <dir> --survey --evidence-dir <dir>
    npm run rollcall:legiscan:fetch -- --state WY --dataset-dir <dir> --evidence-dir <dir>
    npm run rollcall:legiscan:resolve -- --state WY --dataset-dir <dir> --evidence-dir <dir> --crosswalk-file crosswalk-2157.json
    npm run rollcall:judge -- --judgments-file batch-01/judgments.json
    npm run rollcall:legiscan:import -- --state WY --evidence-dir batch-01/s2157 --crosswalk-file crosswalk-2157.json --people-file legiscan-people-wy-2157.json

The 2026 session uses `--state WY-2213` and its own crosswalk and people file.

`wy_text.py` extracts an enrolled act's text with Wyoming's own markup kept.
It needs PyMuPDF. See JUDGING.md for why reading these acts without it is
unsafe.
