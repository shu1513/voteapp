# New Mexico roll-call evidence — 2026 Regular Session (LegiScan session 2251)

The second New Mexico session in the LegiScan roll-call campaign, and the most recent votes
available: this 30-day session ran from January to February 2026, cast by the same members who are
on the November 2026 ballot. See `../legiscan-nm-2187/` for the 2025 regular session.

## Layout

- `crosswalk.json` — every LegiScan `people_id` in the session mapped to a VoteApp candidate, or to
  null with the reason it is null.
- `legiscan-people-nm-2251.json` — the people snapshot the resolver wrote, so the importer can run
  from committed evidence alone.
- `CODE-FINDINGS.md` — defects in the feed, recorded and not fixed.
- `survey/` — the survey report and a worklist giving every gated roll a disposition.
- `batch-01/` — the judged batch: judgments, one evidence file per roll, and the import ledgers.

## What the survey found

812 bills, 256 roll calls, 112 people.

**The vocabulary is the same two sentences as 2025 and nothing else:** `House Final Passage` (148)
and `Senate Final Passage` (108). No committee votes. No parse errors. No roll missing its member
list. The feed is cleaner than 2025's: every House roll totals exactly 70 and every Senate roll
exactly 42, where the 2025 session had House rolls of 69 and seven rolls that failed the parser.

**The feed carries final passage only, exactly as in 2025.** No bill has a second roll in the same
chamber, so there is no concurrence vote anywhere. When the other chamber amends a bill, the vote
accepting the change is missing, so a chamber's only recorded vote can be on text that is not the
law. Every roll needs its own version check.

## The pool

46 rolls are divided and enacted: 21 House and 25 Senate.

**New Mexico senators serve four-year terms and were elected in 2024, so no Senate seat is on the
2026 ballot.** All 25 Senate rolls fan out to nobody. The 21 House rolls carry the session.

Of those 21: 16 are imported, 2 are excluded as appropriations, 2 are dropped for want of an honest
direction, and 1 is held because LegiScan's stored tally is wrong.

## Status

**16 measures, 938 records, 63 candidates, 711 tags, on the local database only. Production holds
no New Mexico roll-call records.**
