# Colorado roll-call votes — LegiScan session 2243 (2026 Regular Session)

Colorado's 2026 regular session ran from 14 January to 13 May 2026 and has
adjourned. The registry entry `CO-2243` landed in a separate pull request based
on main; this directory is data only.

Everything here was produced against the local `voteapp` database. **Production
holds no Colorado roll-call records.**

## The session

714 bills, 4,571 roll calls, 101 members. The fetch stored 1,712 rows: 1,339
floor votes and 373 excluded motions, with **no duplicate rolls** and nothing
surfaced for a human to sort out.

| | |
|---|---|
| divided rolls on enacted bills, after filter 4 | 326 on 208 measures |
| divided rolls on bills that died | 57 on 43 measures |
| chambers superseded | 553 |

`survey/worklist.tsv` holds all 937 chamber rows. `batch-01/` is the first
batch; the rest of the pool is still to work.

## Four things about 2026 that are not true of 2025

**1. Bill text dates are useless.** 3,562 of this session's 3,681 texts carry
the epoch date `1969-12-31`; the 2025 session had none. The version check used
all year — diff the print in force on the vote date against the enrolled act —
cannot run here. The **action history is correctly dated**, so the check becomes:
did anything change the text after the chamber's selected vote? Concurring in
the other chamber's amendments accepts text and is not a change; refusing to
concur, adhering, passing with amendments and conference reports are.
`co_vercheck_2243.py` does this, and all 21 batch-01 rolls came back clean.

**2. LegiScan's `title` can be the gutted bill's old name.** SB 124 is titled
"Colorado Survivor Justice Act"; its enrolled act is "Concerning information
related to the automated protection order notification system". The
`description` field does match the act. **Triage on `description`, never
`title`.**

**3. Ten rolls were rejected because the feed's tally contradicts its own member
list** (for example, "yea says 36 but the member list holds 35"). Refusing them
is right. Two matter: **HB 1151's House chamber has no storable vote at all**,
and **SB 51's Senate** falls back to an earlier roll because the rejected one
was its last.

**4. No fiscal-note URLs.** The feed uses opaque
`leg.colorado.gov/bill_files/<id>/download` links, and the `2026a_<bill>_f1.pdf`
path that worked for 2025 returns 404. The enrolled act downloads fine and is
the ground truth this batch was judged from.

## The crosswalk

101 entries: 55 mapped to a November 2026 candidate, 46 null with the reason.
98 members also sat in 2025, so their reviewed decisions carry forward.

Seats come from LegiScan's `district` field, never its `role` field. **Adrienne
Benavidez is filed role `Rep` at `SD-021`** and is mapped to the Senate District
21 candidacy. Two members seated during the session — Ava Flanell (HD-14) and
Kenny Nguyen (HD-33) — are null because our roster carries no November 2026
candidate at all for those two seats, which is a roster gap worth closing.

**Fan-out: a House roll reaches 42 candidates and a Senate roll 13**, both
better than 2025.

## Two bills whose fate the feed does not record

SB 135 (school funding) was signed by both presiding officers on 20 May 2026
with no governor action recorded, and SB 048 concurred but was "not repassed" on
the session's last day. Neither is in batch-01; both need checking against the
state's own records before they are treated as law.
