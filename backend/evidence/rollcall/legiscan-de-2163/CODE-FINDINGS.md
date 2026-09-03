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

## 3. Two candidate rows for one senator defeat the crosswalk proposer

`proposeLegiscanCrosswalk` requires a match to be unique in both directions. Our
roster holds two rows for **Gerald Hocker** in the same 2026 Senate District 20 race:

- `99177e7c-618c-4bc1-b633-c524ec61ce5d`, display name `Gerald Hocker`, created
  2026-07-16, carrying his summary and three existing records
- `fe8a281c-7c2a-4f05-9892-d91671201409`, display name `Gerald W. Hocker`, created
  2026-08-27, with no records

So an exact name match on both sides was declined and he needed a hand entry. The
crosswalk maps the older row. The newer one is a roster defect, not a second
candidacy, and is left alone here because merging roster rows is outside this
campaign's scope. Maryland saw the same shape from a different cause (Nicholaus Kipke
holding two candidacies) and Alabama from a third (Will Barfoot with a stale
court-map row).

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
