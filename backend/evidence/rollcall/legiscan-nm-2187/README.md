# New Mexico roll-call evidence — 2025 Regular Session (LegiScan session 2187)

New Mexico is the fourteenth state in the LegiScan roll-call campaign. This directory holds the
survey, the crosswalk that maps legislators to our candidates, and each judged batch.

## Layout

- `crosswalk.json` — every LegiScan `people_id` in the session mapped to a VoteApp candidate, or to
  null with the reason it is null.
- `legiscan-people-nm-2187.json` — the people snapshot the resolver wrote, so the importer can run
  from committed evidence alone.
- `CODE-FINDINGS.md` — defects in the feed, recorded and not fixed.
- `survey/` — the survey report, the tally audit against New Mexico's own roll call sheets, and a
  worklist giving every gated roll a disposition.
- `batch-01/` — the first judged batch: judgments, one evidence file per roll, and the import ledgers.
- `batch-02/` — the second judged batch, same layout. It clears the rest of the pool: every
  divided-and-enacted House roll in the session is now imported, held, or dropped with a reason.

## What the survey found

The dataset holds 1,328 bills, 571 roll calls and 128 people.

**New Mexico has the smallest vote vocabulary of any state in this campaign.** Every roll call in the
session carries one of exactly two descriptions, `House Final Passage` (310) and `Senate Final
Passage` (254). There is nothing procedural to exclude.

**The feed carries final passage only.** No bill has a second roll call in the same chamber anywhere
in the session. There are no amendment votes, no concurrence votes and no conference-report votes.
That is a judging hazard rather than a convenience: when the other chamber amends a bill, the vote
accepting the change is missing from the feed, so a chamber's only recorded vote can be on text that
is not the text that became law. Every roll needs its own version check.

There are no committee votes at all. Every House tally totals 69 or 70 and every Senate tally totals
exactly 42. Feed health is otherwise the cleanest tier: no repeated roll call ids, no duplicate
identities, no summary-only rolls.

The fetch stored 504 floor votes and reconciles exactly: 504 stored, plus 60 rejected because the
measure is a memorial or a resolution, plus 7 parse errors, equals the 571 in the dataset.

## The pool, and why the House carries it

125 of the stored rolls are divided, meaning the losing side is at least a quarter of the winning
side. **78 of those are on measures that became law — 40 House and 38 Senate, across 49 measures.**

**New Mexico senators serve four-year terms and were elected in 2024, so no Senate seat is on the
2026 ballot.** Our candidate pool holds 105 New Mexico candidates across all 70 House districts and
no Senate candidates at all, and the crosswalk validation confirms it: every Senate roll matched zero
candidates. **The House's 40 rolls carry the whole campaign, one roll per measure.**

Fan-out is a median of 57 candidates per House roll, with a maximum of 62.

## Sources

- **Fiscal Impact Report** — `https://www.nmlegis.gov/Sessions/25%20Regular/firs/<PADDED>.PDF`.
  Written by the Legislative Finance Committee, which is nonpartisan legislative staff. There is no
  sponsor statement of intent, so the Texas advocacy hazard does not recur. **Only the paragraph
  headed "Synopsis of ..." is neutral**; the "Fiscal Implications" and "Significant Issues" sections
  carry the committee's own evaluative wording and relay executive-agency analysis. Use the synopsis
  as an index and the enrolled act as the source.
- **Official roll call sheet** — `.../votes/<PADDED><H|S>VOTE.pdf`. Prints the date with a
  timestamp, the roll call number, every member by name, and **the exact version voted**.
- **Enrolled act** — `.../final/<PADDED>.pdf`. **Clean text with no markup.**
- **Introduced bill and each later version** — `.../bills/<house|senate>/<PADDED>[SUFFIX].pdf`;
  the bill with adopted amendments folded in is at `.../Amendments_In_Context/<PADDED>.pdf`.

## The New Mexico version tool: the underscore

New Mexico prints an amended statute in full and marks the bill's own new language with an
**underscore**; deleted language goes in square brackets. Plain text extraction keeps the brackets
and throws the underscore away, so an extracted read cannot tell what a bill changes from law it
merely reprints. **The enrolled print carries no markup at all**, so the change has to be read off a
version PDF, never off the enrolled act.

`nm_new.py` (kept outside the repo, described in CODE-FINDINGS.md) finds the underscore by geometry:
a thin horizontal rule drawn between the baseline and the descender of a run of characters. It was
proved on House Bill 6, which reprints all of Section 13-4-11 and whose only new language is the
section heading and new Subsection J.

**One warning learned the hard way: the `Amendments_In_Context` print can show a superseded
amendment alongside the one that survived.** On Senate Bill 16 it shows minor-party voters being let
into primaries; the enrolled act does not include them. When the two disagree, the enrolled act
wins.

## Helper scripts

They live at `/Users/shu/legiscan-data/` so they survive a session: `nm_docs.py` (fetch reports,
bill versions and the enrolled act), `nm_votes.py` (fetch and parse the official roll call sheets),
`nm_new.py` (print only a bill's new language), `nm_audit2.py` (audit every roll against its
official sheet), `nm_score.py` (reading level).

## Status

**Batch-01: 14 measures, 818 records, 63 candidates, 602 tags.**
**Batch-02: 15 measures, 872 records, 63 candidates, 671 tags.**
Together 29 rolls and 1,690 records, all on the local database. **Production holds none.**

The pool is now fully dispositioned: 14 in batch-01, 15 in batch-02, 8 dropped for want of an
honest direction, 2 excluded as appropriations, 1 held, and 38 Senate rolls out of scope.

Still open: Senate Bill 3, which needs an official-tally override before it can be imported; the 37
divided rolls on measures that passed one chamber and died; the 2026 regular session, LegiScan
2251, which is complete and unsurveyed; and promotion to production.
