# Delaware — findings recorded, not fixed

## 1. The description cannot classify the question, and no pattern can fix it

Every Delaware roll reads `House Third Reading` or `Senate Third Reading`. A vote on
final passage, a vote to adopt an amendment, the originating chamber's later vote on
the other chamber's version, and a procedural motion are all spelled the same way.

A config exclusion can only read the description, so there is nothing to exclude:
dropping either spelling would throw away every real passage vote in that chamber.
The registry entry keeps both and says in its comment that the question class it
reports is the feed's claim, the same footing Florida's is on.

The fix that works is a selection-time match against the bill history, which does name
the question. Over the whole session:

| result | rolls |
| --- | --- |
| matched a `Passed/Defeated By <chamber>` line | 1,574 |
| no history line matches on date and tally | 409 (350 of them the empty voice-vote rolls) |
| matched both an amendment line and a passage line | 59 |
| matched a line naming a procedural motion | 2 |

Among the 158 divided rolls only 8 stay unresolved, and all 8 are dispositioned as
`excluded` in `survey/divided-worklist.tsv`. The worked example is **HB 445**: on
2026-07-01 the House cast two rolls, and the 15-26 one is a motion to recess to read
an amendment, which the history says outright and the description does not.

Building this into the fetcher was considered and not done. The fetcher reads the vote
files; the question lives in the bill file's `history[]`, which is a different record,
and the match is on a free-text action line that Delaware writes by hand. A wrong
automatic classification would be worse than an honest "the description does not say".

## 2. The synopsis belongs to the introduced bill and is never updated

Delaware writes a `SYNOPSIS` into the bill itself. It is neutral and section-by-section
with no sponsor statement of intent, which makes it a good index. It is not a
description of the act.

**HB 210 is the proof.** Its title still reads "An Act To Amend Titles 3, 7, And 16",
and its synopsis still lists Chapter 22 of Title 3 (nutrient management penalties)
first. House Amendment 1 struck that entire Section 1 before the House voted. The
engrossed print both chambers passed has no Title 3 section at all, and starts at
Section 2. Judging HB 210 from its own title or synopsis would have credited every
member with raising penalties the act never touched.

The engrossed print carries no synopsis at all, so the only safe reading is: take the
synopsis from the introduced draft as an index, and write from the engrossed text.

## 3. Two candidate rows for one senator defeat the crosswalk proposer (since resolved)

`proposeLegiscanCrosswalk` requires a match to be unique in both directions. Our
roster holds two rows for **Gerald Hocker** in the same 2026 Senate District 20 race:

- `99177e7c-618c-4bc1-b633-c524ec61ce5d`, display name `Gerald Hocker`, created
  2026-07-16, carrying his summary and three existing records
- `fe8a281c-7c2a-4f05-9892-d91671201409`, display name `Gerald W. Hocker`, created
  2026-08-27, with no records

So an exact name match on both sides was declined and he needed a hand entry.
Maryland saw the same shape from a different cause (Nicholaus Kipke holding two
candidacies) and Alabama from a third (Will Barfoot with a stale court-map row).

**Resolved 2026-09-04.** The roster campaign merged the newer row into the older one
and soft-deleted it, so the resolver now proposes this mapping on its own and the
crosswalk records it as a plain proposal. Kept here because the failure mode is real
and will recur in any state where a roster run creates a second row for a sitting
member.

## 4. Delaware bill texts carry no dates

Every entry in `texts[]` reads `date: "0000-00-00"`, on all 1,296 bills. The usual
version check — compare the vote date to the date of the last amended text — cannot
run. Only the order in the file, the type (`Draft` or `Engrossed`), and the engrossed
print's own "AS AMENDED BY" header are available.

Relatedly, `amendments[]` is empty on every Delaware bill, and no House or Senate
amendment appears as its own record. Amendments exist only as `history[]` lines.

## 5. A supermajority bill can still be reported as passed on a bare majority

Two of Delaware's routes need more than a simple majority: Article VIII §10 requires
three-fifths of each house to raise a tax or license fee (HB 175 says so on its face),
and Article IX §1 requires two-thirds where an act indirectly amends a municipal
charter (SB 23 says so). LegiScan's `passed` flag is a bare-majority check, as Montana
recorded, so it cannot be trusted against either rule.

No code path reads `passed` for Delaware, and both batch-01 measures that carry a
supermajority rule cleared it on their own numbers (SB 23: Senate 14 of 21, House 29 of
41). Recorded so a later batch that reaches a failed supermajority vote knows not to
trust the flag.

## 6. A measure with no honest direction can no longer be recorded at all

`parseRollCallLabels` rejects an empty `labels_json` outright — "labels_json is not a
non-empty array" — so every judgment must carry at least one research area. The code
does support non-stance areas: `NON_STANCE_RESEARCH_AREA_SLUGS` holds `general` and
`integrity_and_ethics`, and a label of `{"slug":"general","yea":null,"nay":null}`
tags both sides topically with no stance. That is how Ohio HB 116, Maine LD 613,
Missouri SB 4 and several Alabama measures were recorded.

The label rule adopted on 2026-09-02 closes that route: `general` is a judicial,
non-selectable area whose tag is hidden from every legislative view and user ranking,
and must never go on a roll-call record.

Together those two facts mean **a divided, enacted, highly salient measure that no
research area can honestly point a direction on cannot be recorded at all** — not with
a stance, not without one. Delaware's HB 140, the End of Life Options Act, is the case
in point: it drew the closest votes of the session (House 21-17, Senate 11-8) and is
dropped for this reason and no other.

This is a policy gap rather than a defect, and it is not Delaware's to settle. Two
routes would close it: allow a roll-call record to carry no area at all, or define a
visible non-stance area for votes worth recording that no area can score. Until then
the campaign loses exactly the votes that divided a legislature most.

## 7. The dataset's bill status trails Delaware's own record, in both directions

The campaign's enacted gate reads the LegiScan bill `status`. For Delaware that field is
not merely cut-stale, it is behind the state.

Of the 20 bills parked at status 3 (passed both chambers, awaiting the Governor) in the
dataset cut on **2026-08-30**, nine had in fact been signed. Seven were signed on
2026-09-02 and 2026-09-03, which a later cut would eventually pick up. **But HB 233 and
HB 310 were signed on 2026-08-26, four days before that cut, and the dataset still
carries them as unsigned.** Re-downloading the same session would not have produced
them.

Two consequences worth carrying to any state whose session is still sitting:

- **Do not treat "re-fetch when the signed count moves" as the mechanism.** It assumes
  the feed learns of a signature before the next cut, and here it did not.
- **A re-fetch is not needed anyway.** The fetcher stores every kept-question roll as a
  pending row when the bill is first seen. The rolls behind an unsigned bill are already
  in `legislative_votes`; only the enactment fact is missing, and the state publishes it.
  Delaware's is at `legis.delaware.gov/json/BillDetail/GetRecentReportsByLegislationId`,
  keyed by the `LegislationId` inside the dataset's own `state_link`.

The same check across the other live sessions found no such gap, but did find two shapes
that are permanently parked rather than waiting: **vetoed bills** (Alaska HB 10 and
HB 93) keep status 3 in the dataset, and **concurrent or joint resolutions** (Alaska
SCR 28/201/202, California SJR 7) are never presented to a governor, so their status can
never reach enacted. Both should be dispositioned out rather than re-checked.

