# Oregon roll-call import — LegiScan session 2191 (2025 Regular Session)

Phase-4 LegiScan campaign for Oregon. Plan: `docs/plans/roll-call-vote-import.md`.

Oregon meets every year, so this session is only the 2025 half. The 2026
session is a separate LegiScan dataset (2252) and would need its own
`OR-2252` registry entry; nothing here covers it.

## Correction, 2026-09-06: eleven rolls disagree with the journal, not five

This directory originally said that five of Oregon's 2025 roll calls put a
member on the wrong side. **The true number is eleven.**

The original audit was bounded by the divided gate: it checked only the 393
roll calls that are both divided and enacted. Re-running it over **every**
enacted floor roll — all 1,355 — finds eleven mismatches against the tally
Oregon's own bill history prints, with 1,344 exact.

The six that were missed are:

| Roll | LegiScan | Oregon's journal |
| --- | --- | --- |
| HB 3175 House 2025-03-06 | 46-10, Harbick a nay | 47-9, Harbick not named |
| HB 3746 House 2025-06-20 | 44-5 | 42-7, Bowman and Nelson also nays |
| SB 5534 House 2025-06-24 | 49-1, Boshart Davis a nay | 50-0, no nays |
| HB 3731 House 2025-04-17 | 47-2, Cate and Diehl nays | 48-1, only Cate |
| SB 1061 Senate 2025-06-23 | 29-0 | 30-0, a member missing |
| SB 838 Senate 2025-06-16 | 28-0, no nays | 27-1, Hayden a nay |

**No imported record is affected.** None of the six is divided under either
tally, so none was ever eligible for the pool. That had to be checked rather
than assumed, and it was: a re-fetch moved exactly six stored rolls and left
3,312 unchanged, and a re-import reported all 3,091 records `unchanged`.

All eleven now sit in the config's `heldRollCallIds`, so they are surfaced and
can never be queued.

**Why the bound was unsafe.** A tally error can itself decide whether a roll
passes the divided gate. Oregon's 2026 session contains exactly that case:
SB 1565's House roll reads 45-10 in LegiScan, which fails the gate, and 43-12
in the journal, which passes it. Auditing only what the feed already calls
divided would have hidden the one roll the feed was wrong about. The audit in
`../legiscan-or-2252/tally-audit.py` takes a dataset directory and runs over
every enacted floor roll in either session.

## The dataset

LegiScan session **2191**, dataset cut 2025-12-07, hash
`cec871509ffe8cabfad7449794ca124a`, 11,734,020 bytes: 3,466 bills,
3,565 roll calls, 115 people for 60 House and 30 Senate seats (the extra names
are mid-session replacements).

Downloading it needs **two** keys, not one: the account's API `key` plus the
dataset's own `access_key`, which comes from `getDatasetList`. Omitting the
second returns a 59-byte `{"status":"ERROR", ..."Invalid access key"}` body
that is easy to mistake for a bad API key.

## What Oregon prints, and what we keep

Oregon writes the body in front of every question, so floor and committee
separate on the wording alone. A committee roll always begins
`House Committee ` or `Senate Committee `; no floor roll does. All 1,954
committee rolls carry that prefix, and the config excludes them **by rule**
rather than by tally, because Oregon's joint Ways and Means committee seats
23-24 members — up to 80 percent of the 30-seat Senate — so a tally-only cut
would have parked every large Senate committee roll in the surfaced queue.

Kept floor questions:

| Question | House | Senate | What it is |
| --- | ---: | ---: | --- |
| `Third Reading` | 672 | 670 | passage |
| `Third Reading in Concurrence` | 52 | 36 | accepting the other chamber's amendments and repassing, in one vote |
| `Repassed` | 3 | 5 | repassage after a conference report (but see below) |
| `Adopted Conference Comittee Report` | 1 | — | adopting the report; the feed's own misspelling, one `m` |

Excluded as floor-but-procedural: motions to substitute the minority
committee report, to reconsider, refer, re-refer, table and change the
calendar; the vote refusing to concur (the step that sends a bill to
conference); and withdrawing a bill from committee. 49 rolls in total.

Resolutions need no patterns. Concurrent resolutions, joint memorials and
simple resolutions (types CR, JM, R) are dropped before the config is read,
and they own the only remaining floor families — `Senate Final Reading`,
`House Read and Adopted`, `House Special Order` and `House Resolution
Adopted`.

Every one of the 3,565 rolls classifies, with **nothing surfaced**:

    1,954 committee + 1,437 kept floor + 125 dropped on measure type
        + 49 excluded questions = 3,565

## Feed health

Cleanest tier: 0 repeated `roll_call_id`s, 0 summary-only rolls (every roll
carries its member list), 0 tallies that disagree with their own
yea/nay/nv/absent fields, 0 member lists whose length disagrees with the
stated total, and 3 identity-duplicate extras.

## The pool

- 425 divided floor votes (217 House / 208 Senate) under the standard gate,
  the losing side at least a quarter of the winning side.
- **393 of them on measures that became law, across 225 measures.**
- Oregon was a Democratic trifecta in 2025, so the divided-and-enacted set is
  the majority's agenda and stance directions run opposite to the
  Republican-trifecta states.
- 7 bills were vetoed and **every veto was sustained**, so nothing in this
  session became law over a veto.

## Fetch

3,318 rows stored, 3,318 distinct roll numbers (no collisions), 1,437 floor,
0 surfaced, dates 2025-01-23 to 2025-08-29. The two rolls dated in August are
tabling motions on bills the Governor had already vetoed — Oregon's 2025 1st
Special Session is a separate LegiScan dataset (2225) and none of its votes
leak into this one.

## Crosswalk

`crosswalk.json` — 91 members, **61 mapped / 30 explicit null**. Validation
over all 3,318 evidence files: matched 50,620, unmatched_reviewed 24,320,
`no_crosswalk` 0, `out_of_scope` 0, 0 file errors.

52 pairs came from `rollcall:legiscan:resolve` and every one was checked by
hand. Nine more had to be added by hand, in three classes:

- **Nickname (six).** LegiScan keeps the legal first name in `first_name` and
  the working name in `nickname`, and the proposer reads neither `name` nor
  `nickname`: Julianne/Julie Fahey, Thomas/Tom Andersen, Julianna/Jules
  Walters, Ricardo/Ricki Ruiz, Eric/Werner Reschke, Barbara/Bobby Levy.
  Several of these have a LegiScan `name` byte-identical to the ballot name
  and were still missed. `"thomas"` is not a prefix of `"tom"`, the same
  shape as `"nick"`/`"nicholas"`.
- **Shortened multi-part surname (one).** LegiScan's `Neron Misslin` is on the
  ballot as Courtney Neron.
- **Statewide runners (two).** Christine Drazan (Governor) and David Brock
  Smith (US Senate) are sitting members, so they cast votes all session, but
  they are outside the state-legislative candidate pool the proposer draws
  from. These are the highest-value hand-adds.

One accepted seat disagreement: Jeff Helfrich is vacating House District 52
to run for Senate District 26, the seat Daniel Bonham is leaving, and House
District 52 is an open race on the 2026 ballot.

Sixteen of the 30 nulls are Senate seats that are simply not on the 2026
ballot, because Oregon staggers its Senate. That is structural, not a roster
gap.

`crosswalk-proposals-report.json` is the proposal-side report. The
full-resolution `resolve-report.json` is 10 MB and is deliberately not
committed.

## Reach and fan-out

All 60 House seats are on the Nov-2026 ballot and our rosters cover 59 of
them (109 candidates). Oregon staggers its Senate, so only 15 of 30 districts
are up (28 candidates). Measured fan-out is **51 matched candidates per House
roll and 10 per Senate roll**, so House rolls carry about five times the
value and batch selection should prefer them when it must choose.

## Judging sources

**The Staff Measure Summary is the primary source** — written by the
Legislative Policy and Research Office, Oregon's nonpartisan legislative
staff, and published for 1,886 measures this session. It carries no sponsor
statement of intent, so the Texas advocacy-preamble hazard does not arise.
Two things make it unusually good:

- It is **version-stamped in its own header** (`HB 3546 B` is the B-Engrossed
  bill), along with the committee, the action, the action date and the
  committee vote, so the version check is built into the document the way
  Connecticut's OLR analyses and Maryland's DLS notes are.
- It separates **WHAT THE MEASURE DOES** (a detailed section-by-section
  summary) from **EFFECT OF AMENDMENT** (what the amendment changed), and
  quarantines opinion in a neutral **ISSUES DISCUSSED** topic list.

It ends with an explicit disclaimer that the summary "has not been adopted or
officially endorsed by action of the committee", and it is written at the
committee stage — so it describes the version that committee reported, not
necessarily the enacted text. The enrolled Act remains ground truth.

Endpoints:

- Analysis index: `https://api.oregonlegislature.gov/odata/ODataService.svc/MeasureAnalysisDocuments?$filter=SessionKey+eq+'2025R1'` (page it with `$skip`; a page caps at 5,000 rows).
- Summary PDF: `https://olis.oregonlegislature.gov/liz/2025R1/Downloads/MeasureAnalysisDocument/<MeasureAnalysisId>`.
- Bill text, every version through Enrolled: `https://olis.oregonlegislature.gov/liz/2025R1/Downloads/MeasureDocument/<BILL>/<Version>`, and LegiScan's `texts[]` already lists each version with that link.

Both need a browser User-Agent. `pdftotext -layout` reads them cleanly.

⚠ LegiScan's `texts[]` dates for Oregon are all `0000-00-00`, so the version
stack is ordered by its version NAMES (Introduced, A-Engrossed, B-Engrossed,
Enrolled), never by date.

## Hazards recorded here

See `CODE-FINDINGS.md`.
