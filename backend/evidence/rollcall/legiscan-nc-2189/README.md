# North Carolina roll-call votes, LegiScan session 2189

North Carolina General Assembly, 2025-2026 Regular Session. The session was still
sitting when this dataset was cut, so a later download will hold votes this
campaign never saw.

## Where the data came from

LegiScan bulk dataset, session id 2189, dataset dated 2026-08-30, downloaded with
the LegiScan key that lives only in the main checkout's `backend/.env`. The
dataset holds 2,338 bills, 1,493 roll calls and 180 people. The extracted
dataset and the full evidence set live outside this repository at
`/Users/shu/legiscan-data/nc-2189/` and `/Users/shu/legiscan-data/nc-2189-evidence/`,
because they are too large to commit. This directory keeps the reviewed subset.

## What the survey found

`survey/desc-histogram.json` is the measured vocabulary the config entry was
written from. The important facts:

- North Carolina records its floor vote on second reading. A bill needs three
  readings, the roll call is called on the second, and the third reading passes
  without a roll call unless a member objects.
- The question can be a suffix. `A1 Blackwell Second Reading` is a floor
  amendment and `Second Reading M4 Previous Question` is a motion to cut off
  debate, so patterns are anchored at both ends and exclusions run first.
- Veto overrides are a real pool. The legislature and the governor are of
  different parties, so 26 kept rolls on 14 bills are override votes.
- The fetch's own checks pass: no repeated roll call ids, no duplicate rolls,
  no summary-only rolls, no committee votes, and every roll's parts add up to
  its total. That is not the same as the tallies being right. Every 2026 House
  roll is short two seated members and reports a tally North Carolina's own
  transcript contradicts, seven for seven where checked, so no 2026 House roll
  may be imported. A complete member list is not a clean bill of health either:
  a 2025 House roll with all 120 members present still had one member's nay
  stored as a non-vote, and the wrong tally with it. Read the official
  transcript for every roll before importing it. See findings 3 and 4 in
  `CODE-FINDINGS.md`.

Fetch stored 1,452 rows: 972 floor votes and 480 rolls on excluded questions.
Of the floor votes, 247 are divided and 165 are divided and enacted, across 58
measures. `survey/divided-enacted-worklist.tsv` carries one line per divided and
enacted roll with its disposition, and `survey/divided-not-enacted-worklist.tsv`
does the same for the 82 divided rolls on measures that did not become law.
Both worklists are now fully dispositioned; no North Carolina pool remains.

## Crosswalk

`crosswalk.json` maps LegiScan people to VoteApp candidates: 178 entries, 151
mapped and 27 reviewed as having no Nov-2026 candidacy. 120 came from the
proposer, all with the seat agreeing, and 31 were added by hand.

North Carolina is a nickname state. LegiScan puts the legal first name in
`first_name` and the working name in `nickname` and `name`, so the proposer
missed 30 members whose roster name matches our candidate name exactly: Larry
Dean Arp is "Dean Arp", Harold Kevin Corbin is "Kevin Corbin", Eldon Sharpe
Newton III is "Buck Newton", Maze O'Neal Jackson is "Neal Jackson". The 31st
hand entry is Zack Forde-Hawkins in House District 31, whom our roster carries
as Zack Hawkins.

Our rosters cover all 120 House districts and all 50 Senate districts, 311
candidates in total, which is the best coverage of any state in this campaign.
The fan-out is 94 candidates for a median House roll and 42 for a median Senate
roll.

## How measures are judged

The judging source is the North Carolina General Assembly's own bill summary,
written by the nonpartisan Legislative Analysis Division. Each summary names the
version it describes in its own header (`Analysis of: Ratified`), which makes the
version check part of the document. The summaries carry no sponsor statement of
intent, so the Texas advocacy problem does not arise here.

- Summary index: `ncleg.gov/Legislation/Bills/Summaries/2025/<BILL>`
- Summary PDF: `dashboard.ncleg.gov/api/Services/BillSummary/2025/<document id>`
- Enacted session law: `ncleg.gov/EnactedLegislation/SessionLaws/PDF/2025-2026/SL<year>-<n>.pdf`
- Official roll-call transcript: `ncleg.gov/Legislation/Votes/RollCallVoteTranscript/2025/<H|S>/<RCS number>`

The session law is the ground truth. The summary is an index into it.

## Layout

- `crosswalk.json`, `legiscan-people-nc-2189.json` — identity
- `survey/` — the measured vocabulary and the divided-and-enacted worklist
- `batch-01/` — seven of the twelve bills enacted over the governor's veto (the other five failed the stance filter)
- `batch-02/` — ten measures from the rest of the divided-and-enacted pool, including the two constitutional amendments on the November 2026 ballot
- `batch-03/` — three vetoed bills that never became law, the first batch from the not-enacted pool
- `batch-04/` — fifteen measures that passed one chamber and went no further, which finishes the not-enacted pool
- `CODE-FINDINGS.md` — defects found in the feed, recorded rather than fixed
