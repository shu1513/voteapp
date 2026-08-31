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
