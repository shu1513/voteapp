# Montana: defects found in LegiScan's data, recorded not fixed

Five problems in the LegiScan Montana feed. None is fixed in code; each is
worked around, and each is written down here so the next campaign does not have
to rediscover it. Montana's own API (`api.legmt.gov`) is the ground truth used
to settle every one.

## 1. Forty-two committee roll calls report tallies that are multiples of their own member lists

`parseLegiscanRollCall` refuses a roll whose tallies do not match its member
list, and 42 of the 9,209 rolls in this dataset fail that check. One claims 500
votes in a 100-seat chamber; the smallest claims 16 votes from an 8-member
list. The reported total is always a whole multiple of the real one.

**All 42 are committee rolls**, so none could ever reach the review queue: every
Montana committee description joins the committee name to its question with a
double hyphen, and the largest of the 42 reports a total of 500 against a
committee whose real membership is under 25. Committee-ness is decided by the
tally check, which runs after parsing, so these rolls are reported as
`parse_error` and the fetch run exits non-zero. That exit code is expected for
Montana and does not mean the run failed.

Not fixed because the only clean fix — reading the raw `total` before parsing,
as Connecticut does for its joint-committee rolls — buys nothing here: no floor
roll is affected, and the check is doing its job by refusing data it cannot
trust.

## 2. LegiScan's `district` field is stale for most Montana members

Montana redrew its legislative map for the 2024 election. LegiScan still
carries each member's pre-2024 district. **63 of the 151 people records
disagree with Montana's own roster of the 69th Legislature** — more
disagreements than agreements among members first elected before 2024.
Examples: Zooey Zephyr is `HD-100` in LegiScan and House District 95 in the
state's roster; Braxton Mitchell is `HD-003` and House District 5; Andrea Olsen
is `SD-050` and Senate District 48.

The consequence for this pipeline is that **`seatAgrees` is not a usable signal
in Montana**: 47 of the 85 proposals came back `seatAgrees:false`, and almost
all of those are the stale-district artifact rather than a real seat change.
Every proposal was therefore checked against the official roster instead
(`https://api.legmt.gov/legislators/v1/legislators`, filtered to members serving
during the 69th Legislature).

After that check, 73 of the 85 proposals sit in the member's own 2025 seat and
12 are real moves. Ten of the twelve are explained by Montana's nesting rule —
Senate district *n* contains House districts *2n-1* and *2n* — so a
representative running for the Senate seat that contains their own House
district reads as a seat change to the proposer. The other two, Amy Regier
(House District 6, filed in Senate District 4) and George Nikolakakos (House
District 22, filed in Senate District 12), are filings in a neighbouring Senate
district; both names are unique on each side, so identity is not in doubt.

## 3. LegiScan's `party` field is wrong for six Montana members, and one identity is wrong outright

Six people records carry the wrong party against the state roster: Lyn Bennett,
Ed Byrne, Brian Close, Thedis Crowe, Scott DeMarois and Sidney Fitzpatrick. The
pipeline does not read `party`, so nothing broke, but it means party cannot be
used to corroborate an identity in Montana either.

Worse, **people_id 25400 "Sidney Fitzpatrick" is recorded with district
`SD-035` and party `R`**. Montana's roster has no Sidney Fitzpatrick in the
Senate: the member is Chip Fitzpatrick, a Democrat in House District 42, whose
official e-mail address is `Sidney.Fitzpatrick@legmt.gov`. Both the chamber and
the party are wrong in LegiScan. The crosswalk maps him from the official
record, not the LegiScan one.

One further identity is unresolved and left null: **people_id 19909 "Julie
Dooling" casts 3,047 votes in this dataset but is absent from Montana's roster
of the 69th Legislature**, whose House District 70 member is Shannon Maness.
The only serving member missing from LegiScan is Julie Darling of House
District 84. Neither name appears in our November 2026 candidate pool, so no
record could be misattributed either way, and the entry stays null with that
reason written into it.

## 4. LegiScan's stored document links for Montana are dead

Every `texts[].state_link` in the Montana bill records points at
`https://docs.legmt.gov/download-ticket?ticketId=<uuid>`. Those tickets expire:
fetching one answers `200` with an **empty body**, which is easy to mistake for
a network problem. A ticket minted in the same session works normally.

Use the state's own document service instead — `getBillVersions` for the list
and `getContent?documentId=<id>` for the file. Both are plain unauthenticated
GETs and the PDFs extract cleanly with `pdftotext -layout`.

## 5. LegiScan's `passed` flag is a bare majority check, wrong on Montana's supermajority votes

Montana proposes constitutional amendments by ordinary bill, and such a bill
needs two-thirds of the whole legislature. Five of them — HB 316, HB 821,
HB 822, HB 921 and SB 185 — won simple majorities on third reading but missed
that threshold, so Montana's own description says `3rd Reading Failed` (six of
the eight rolls add `; 2nd House Vote Required`). LegiScan computes `passed`
from the tally alone and stamps all eight rolls `passed: 1`, so the fetcher
stored `result = "Passed"` on eight pending queue rows whose question the state
says failed.

Not fixed in code, on the Florida precedent (its question fields are recorded
as LegiScan's claim, never trusted in a batch): no code path reads `result` —
the fan-out, judge and import never consult it, and descriptions are written
from the official action trail — and no description pattern can separate the
wrong rows, because two of the eight carry the plain `3rd Reading Failed`
wording shared with 36 rolls whose flag is correct. The rule for any future
Montana batch that reaches these bills: the result column mirrors LegiScan's
claim; the official action trail is the ground truth, and a judgment's
description must state the two-thirds failure in its own words. All five bills
are dead (LegiScan status 6), so none can appear in a divided-and-enacted
batch; they would surface only in a failed-votes scope like Pennsylvania's
batch 02.

## 6. Not a code defect: Montana coordination instructions can void a bill

Montana bills often end with a "coordination instruction" saying that if some
other named bill also passes and is approved, then whole blocks of this bill are
void and the other bill is amended to carry the text instead. LegiScan records
none of this. Its status field says the bill was enacted, and its stored text is
the enrolled bill, which still prints the sections that the instruction voided.

HB 231, the 2025 property tax rewrite, is the case that surfaced it. Sections
27, 29 and 31 each void sections 1 through 23, 25, 30, 33 and 34 of HB 231 if
Senate Bill 542 is also passed and approved, and transplant that text into SB
542. The governor signed both on May 13, 2025, so the instruction fired and
almost nothing substantive survives under HB 231's own number.

HB 285 in batch-04 hit a milder form of the same thing: its section 12 replaces
its own section 7 version of 75-1-201 once SB 221 passed. There the rest of the
act stands, and section 12 supplies the operative text, so the measure is still
judgeable on its own.

**Rule for future Montana batches.** Before judging any Montana measure, search
the enrolled text for "Coordination instruction". If one exists, look up whether
the named bill was signed. If it was, work out what survives before writing a
description that says "and it became law".


## 6a. Not a code defect: a stale short title

LegiScan's short title for HB 329 is "Make the Montana ammunition act
permanent". The enrolled act terminates on 31 December 2035. The title reflects
the bill as introduced, not as enacted, and a conference committee changed it.
Read the enrolled title, never the tracker's label.

## 7. A real defect: LegiScan's Montana feed can record a member's vote wrongly

Found in batch-07 while checking which text the House had voted on for SB 542.

Montana publishes its own roll calls member by member:

    https://api.legmt.gov/bills/v1/votes/findByBillId?billId=<id>

The `id` is the `id` field of the bill record already used for chapter numbers.
Each vote carries a `legislatorVotes` array of legislator ids and vote types,
which resolve against `/Users/shu/legiscan-data/mt-legmt-legislators.json`.

Three confirmed disagreements so far:

| Roll | Chamber, date | LegiScan | Montana | Members differing |
| --- | --- | --- | --- | --- |
| 1556679 (SB 542) | house 2025-04-24 | 73-26 | 72-27 | 1 (Amy Regier) |
| 1551940 (HB 231) | senate 2025-04-17 | 26-24 | 25-25 | 47 of 50 |
| 1554283 (HB 231) | senate 2025-04-22 | 24-26 | 24-26 | 8 |

The last row is the important one: **the tallies match exactly and eight
members' votes are still swapped.** A check that compares only totals passes it.
Only a member-by-member comparison catches it.

Roll 1556679 is a roll this campaign wanted to use, and Amy Regier is in the
crosswalk, so importing it as LegiScan has it would have published a record
saying a named person voted for a bill she voted against.

**Tooling.** `/Users/shu/legiscan-data/mt_verify.py` compares any bill's stored
LegiScan rolls against Montana's own record, member by member, and
`mt_prefetch.py` warms a local cache of the official records with eight threads.
Both live outside the repository, like the other Montana helpers.

**What the pipeline does right.** The importer verifies the SHA-256 of each roll
call payload against the value approved at fetch time, and separately checks the
evidence file's tally against the approved row. Those guards make a hand
correction impossible, which is correct: they exist to stop unreviewed editing of
source data.

**What is missing.** There is no supported way to record that an upstream source
is wrong about a named member and to import the corrected roll. Until there is,
an affected roll can only be held. Building that path is a code change and
belongs in its own review.

**How far it goes.** All 1,826 stored floor rolls on the 335 worklist bills were
compared member by member. The full result is in
`survey/legiscan-vote-audit.md`, with every disagreeing roll listed in
`survey/legiscan-vote-audit.tsv`. In short:

- **None of the 81 rolls this campaign has imported disagrees.** Every one was
  in scope and every one matches. No Montana record is wrong.
- 76 of 1,826 rolls (4.2%) have at least one member on the wrong side, and 63
  (3.5%) have a wrong tally.
- **13 rolls have a matching tally and still put members on the wrong side.** A
  totals-only check passes all thirteen.
- The defect concentrates on second readings: 66 of 1,007, against 10 of 812
  third readings.
- Eight of those ten third readings are a chamber's last kept floor vote, so
  they are rolls this campaign would otherwise select. Seven are marked
  `held:legiscan-vote-defect` in the worklist; the eighth, HB 636 roll 1508554,
  never entered the worklist because 84-15 is not divided.
