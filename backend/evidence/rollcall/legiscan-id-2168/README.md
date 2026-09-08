# Idaho, 2025 Regular Session (LegiScan session 2168)

Sixty-eighth Legislature, first regular session. Dataset cut 2025-12-07:
790 bills, 854 roll calls, 129 people for 70 House and 35 Senate seats.

This directory holds the survey, the reviewed crosswalk and the worklist.
**No Idaho roll call has been judged or imported yet.** Production holds zero
Idaho roll-call records, and so does the local database.

## Why the config excludes nothing

Idaho prints the smallest question vocabulary in this campaign. All 854 roll
calls carry one of two descriptions, `House Third Reading` (455) and
`Senate Third Reading` (399), and neither names the question. Passage, a vote
on an amended bill and a resolution adoption are spelled identically.

That is the Delaware shape. A config exclusion can only read the description,
so excluding either spelling would throw away every real passage vote in that
chamber. The question class on an Idaho row is LegiScan's claim, not Idaho's.

**Ground truth is the bill history line, which prints the question and the
tally together**, for example `Read Third Time in Full -- PASSED 36-34-0` and
`Read third time in full as amended -- PASSED 35-33-2`. Matching a roll to that
line on date and chamber resolves 707 of the 854 rolls; the rest are resolution
adoptions worded `ADOPTED` and rolls whose same-day line is procedural. That
match is a selection-time step, and a roll it cannot place is left unselected
rather than guessed.

There are no committee votes in the dataset at all. Every roll's total is
exactly 70 or 35.

## Feed health

Cleanest tier. No repeated roll call ids, no identity duplicates, no rolls
missing a member list, no disagreement between a stored tally and its stored
member list, no parse errors. The Texas duplicate-id fix is a verified no-op.

## Tally audit

Run over **every** roll rather than only the divided ones, because a tally
error can itself decide whether a roll passes the divided gate. 703 of the 707
rolls carrying a tallied history line match Idaho exactly.

- `H0230` roll 1498007: feed 55-10, Idaho 54-11. One member is on the wrong
  side. **Held** in the config. Outside the current pool either way.
- `H0056` 1478778, `S1148` 1525657 and `H0098` 1487890 differ only in the count
  of members not voting, which leaves the yea and nay sides identical. Left
  queueable, recorded here.

No roll in the divided-and-enacted pool has a member on the wrong side.

## The `passed` flag: one hazard refuted, a different one found

Five rolls pass with fewer than 36 yeas in a 70-seat House, which looks like the
flag defect found in Montana, Arizona and Colorado. It is not. Idaho's
constitution requires a majority of the members **present**, not of the members
elected, and Idaho's own history calls all five passages. Do not carry that
hazard into Idaho on a vote count alone.

**LegiScan's `passed` flag is wrong in Idaho in a different way.** It is
derived from the word PASSED or FAILED in Idaho's action line, and Idaho's
resolution adoptions say ADOPTED, which the flag reads as 0. That mislabels 20
rolls in this session and 23 in 2026. Almost all are CR, JM and R types the
fetcher rejects before storing anything. The exception is joint resolutions,
the type that carries a constitutional amendment, which is a kept type.

Four stored rolls are affected: **HJR004** (House 58-10, Senate 29-6) and
**HJR006** (House 59-8, Senate 30-5). Idaho's own pages say ADOPTED for all
four and both resolutions were delivered to the Secretary of State, so both go
to Idaho voters in November 2026. The fetcher writes `result` straight from
the flag, so all four were stored as "Failed". **They are held in the config**
(`heldRollCallIds`) so they cannot be queued until the stored result is
corrected. None of the four is divided, so the current pool never reaches
them, but a ballot-measure scope would, and a stored "Failed" on a measure
that voters are about to decide is the worst possible data to leave in place.

The other three stored joint-resolution rolls — HJR001 here, HJR007 and HJR009
in 2026 — also carry `passed: 0`, and there the flag is **right**: a
constitutional amendment needs two-thirds of all members (47 of 70), and 46-23
falls one short. Idaho's history calls all three FAILED. Found by the review of
the config pull request and confirmed against the dataset and Idaho's pages.

**A related feed gap:** on a day the House suspends the rules, Idaho's action
line reads `Rules Suspended: Ayes 65 Nays 0 Abs/Excd 5, read in full as
required – ADOPTED - 58-10-2`, and LegiScan's copy of that line stops at
`Rules Suspended:`. The House tally is lost from the feed on those days, which
is why HJR004's and HJR006's House rolls have no tallied history line in the
dataset and why the tally audit above could only place 707 of 854 rolls. When
the history line is missing, the tally has to come from the bill page.

## Pool

297 divided rolls; 286 on kept bill types; **212 divided and enacted across 128
measures** (103 House, 109 Senate), 79 divided in both chambers. All are bill
type B. The ratio of the smaller side to the larger runs 0.25 to 0.94, median
about 0.42. A Republican supermajority does not produce a thin pool here,
because the divisions run inside the majority caucus.

After filter 4 (one roll per measure per chamber, the chamber's last kept roll)
and the appropriations exclusion, `survey/divided-enacted-worklist.tsv` reads:

| disposition | rows |
| --- | --- |
| candidate:unbatched | 96 (65 measures) |
| excluded:appropriations | 104 |
| out-of-gate:not-divided | 477 |
| out-of-gate:not-enacted | 110 |
| superseded:last-roll-not-divided | 7 |
| held:tally-defect | 1 |

Idaho passes each agency budget as its own bill and the Freedom Caucus votes
against most of them, so appropriations are a large share of the raw pool and
the standing exclusion removes 104 rows here.

## Crosswalk

105 entries, 88 mapped, 17 null. Validation over all 830 stored rolls: 36,058
matched, 0 no_crosswalk, 0 out_of_scope, 0 file errors, 0 zero-match rolls.
**Fan-out: House median 57, Senate median 30.** The Senate reach is the best in
this campaign, because all 35 Idaho Senate districts are on the 2026 ballot, so
a both-chamber measure is worth roughly 87 records.

**Idaho splits each of its 35 districts into House seats A and B** (`HD-014A`),
so the resolver's automatic seat check cannot fire on a House row. Every House
proposal was instead checked by hand against the district number and chamber in
the candidate's own candidacy string. 83 of 84 agreed.

Hand-added, all the recurring nickname class: Ted Hill HD-014A (LegiScan stores
the legal name Edward), Dave Lent SD-033 (our roster has David), Scott Grow
SD-014 (our roster splits the name as `C.` and `Scott Grow`), and Todd Achilles
HD-016B, who runs statewide as an Independent so no seat could ever agree.

Accepted after review: Sonia Galaviz, seated at HD-016A and running for Senate
District 16 — a sitting representative running for the Senate seat covering her
district. Deliberately not linked: Mark Sauter HD-001A, because our only House
District 1 row is Jane Sauter, a different person.

## Judging source

**Idaho publishes no neutral legislative analysis.** Every bill has exactly one
supplement, `<BILL>SOP.pdf`, and it is a combined Statement of Purpose and
Fiscal Note written by the sponsor. H0001's opens "The Idaho Parental Choice
Tax Credit legislation provides for..." — advocacy, the same hazard as the Texas
author's statement of intent. Idaho belongs with Arkansas, Alaska and South
Carolina: **the act is the only source.** Use the sponsor document as an index
for triage, never as evidence in a description.

`legislature.idaho.gov` answers a plain curl with a browser user agent.

- act text `…/wp-content/uploads/sessioninfo/<year>/legislation/<BILL>.pdf`
- engrossed prints `<BILL>E1.pdf`, `<BILL>E2.pdf`
- sponsor document `<BILL>SOP.pdf`
- session laws `legislature.idaho.gov/statutesrules/sessionlaws/`

## The version check is mostly mechanical

The feed carries only `Introduced` and `Engrossed` texts and **no enrolled text
at all**, and every `texts[].date` is `0000-00-00`, so the stack cannot be
ordered by date. Engrossed prints are numbered `E1`, `E2`, so order by name.

**105 of the 128 divided-and-enacted measures have no engrossed print**, which
means both chambers voted the introduced text and that text became law. 23 carry
an engrossed print and need real per-roll version work. Every one of the 128
carries a `Session Law Chapter <n>` line in its history, which is both the
enactment fact and the citation.

That work is not optional, because **Idaho takes no recorded vote on
concurrence**: `House Concurred in Senate Amendments` appears 30 times in this
session with no tally anywhere, so a chamber's only recorded vote can sit on
text the other chamber later amended. Same shape as Nevada.

## Files

- `crosswalk.json` — 105 reviewed entries, one per person in the session file
- `legiscan-people-id-2168.json` — the people snapshot the crosswalk was built from
- `survey/rollcall-legiscan-fetch-id-*-survey.json` — the description histogram
- `survey/rollcall-legiscan-fetch-id-*-report.json` — the fetch ledger
- `survey/crosswalk-proposals-report.json` — what the proposer suggested, before review
- `survey/crosswalk-validation-report.json` — the validation summary over all 830 rolls
- `survey/divided-enacted-worklist.tsv` — one row per measure per chamber, with a disposition
- `tools/id_text.py` — the marked-text reader, see CODE-FINDINGS.md §1
