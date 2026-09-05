# New Mexico feed findings (LegiScan session 2187)

Recorded, not fixed. None of these is a bug in our code.

## 1. Seven House roll calls drop the same member, while their headers stay right

Roll calls 1502166 through 1502172, all House votes of 2025-02-27, each list 69 of the chamber's 70
members. The missing member is the same one every time: Martha Garcia, people id 26456, House
District 6. In six of the seven the header's yea count is one higher than the member list; in roll
1502170 the header's absent count is one higher.

The parser refuses all seven because the tally and the member list disagree, so they never reach the
review queue. That is the right outcome. Only one of the seven, roll 1502170 at 40-26, is divided,
so the cost is a single measure.

The fetch therefore exits non-zero on a whole-session run. That is expected for New Mexico and is a
signal, not a rollback: the other 504 rolls are stored normally.

## 2. Senate Bill 3's stored tally is wrong, and this one had to be caught by hand

Roll 1496261 is the House vote on Senate Bill 3. LegiScan stores 42 yeas, 23 nays and 4 absent, with
69 of 70 members listed. **New Mexico's own official roll call sheet, RCS number 78 of 2025-02-20,
reads 44 yeas, 23 nays and 2 absent.** Two members who voted yes are missing from LegiScan's list.

Nothing in the file is internally inconsistent — LegiScan's yea count matches LegiScan's own member
list — so no parser check can see this. Only comparing against New Mexico's published sheet finds
it. That is the same shape as the North Carolina finding where a full member list still carried a
wrong tally.

The roll is **held, not imported.** The approval gate requires the description to quote the stored
tally, so importing it would tell 57 legislators' readers a number the state's own record
contradicts.

**All 40 of the House divided-and-enacted rolls were checked against their official sheets, not a
sample. Senate Bill 3 is the only failure; the other 39 match exactly on date, yeas, nays,
present-not-voting, and absent plus excused.**

## 3. One roll from the 2024 session is filed under the 2025 session

Roll 1505561 is stamped 2024-02-10 and sits on Senate Bill 236, "Look Twice For Motorcycle License
Plate". Every other roll in the dataset falls between 2025-01-22 and 2025-03-22.

This one stray roll explains an oddity in the people file: it lists 58 people with Senate districts
for a 42-seat chamber, with 16 districts holding two people each. In every one of those 16, one
person voted in this single 2024 roll and nobody else, while the other voted in all 253 of the 2025
Senate rolls. The 2024 roll carries the membership of the previous Senate.

Nothing turns on it here, because no New Mexico Senate seat is on the 2026 ballot, but a future
campaign that reaches the Senate must not treat those 16 pairs as mid-session turnover.

## 4. The bill history's date can disagree with the official vote sheet, and the sheet wins

The history line for Senate Bill 535 records the House vote on 2025-03-22. LegiScan stamps it
2025-03-21, and **the official roll call sheet, which prints a timestamp, also says 03/21**. LegiScan
is right and the history line is wrong.

This is worth stating because the opposite assumption would have produced a needless
`official_vote_date` override. **No New Mexico roll needs one.** All 40 House rolls match their
official sheets on date.

## 5. The enrolled print carries no amendment markup, so it cannot show what a bill changed

New Mexico prints an amended statute in full and marks the bill's own new language with an
underscore, with deletions in square brackets. Text extraction keeps the brackets and loses the
underscore. **The enrolled print drops the markup entirely** and reads as clean statute text, so an
enrolled-only read cannot separate the act's change from law it merely reprints — New Mexico
reprints a whole statute section when it amends any part of it.

The fix is a geometry check rather than a code change: the underscore is a thin horizontal rule
drawn between the baseline and the descender of the characters it marks, so it can be matched to
those characters. `nm_new.py` does this. Proved on House Bill 6, where the whole act reprints
Section 13-4-11 and the only new language is the section heading and Subsection J. Same family as
the Kentucky bold-font rule.

**Second warning: the `Amendments_In_Context` print can show a superseded amendment beside the one
that survived.** For Senate Bill 16 it shows minor-party voters being admitted to primaries, in
duplicated passages; the enrolled act contains no such language, because a later committee amendment
struck the earlier one. Read the delta from the version print, but take the final words from the
enrolled act.
