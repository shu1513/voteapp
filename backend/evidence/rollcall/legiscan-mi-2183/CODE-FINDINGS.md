# Michigan code findings — LegiScan session 2183

Things about the pipeline that Michigan exposed. Findings 1 and 2 are fixed in
the config pull request. Findings 3, 4 and 5 are handled by held
entries.

## 1. The measure-id parser rejected lettered joint resolutions — FIXED

`formatLegiscanMeasureId` required a bill number of letters followed by digits.
Michigan letters its joint resolutions instead of numbering them: `HJRA`
through `HJRAA` in the House and `SJRA` through `SJRN` in the Senate, 40
measures in this session. Michigan cites them as `HJR A` and `SJR N`.

Every one of those bill files failed to parse. A bill parse failure is a file
error, and the fetcher exits on file errors, so Michigan could not be fetched
at all until this was fixed.

The fix accepts an instrument prefix ending in `JR` followed by a one or two
letter designator, and only when the number contains no digits. The numbered
form is still matched first, so `SJR 10` is unchanged.

Cost before the fix: 40 measures unreachable, 5 roll calls, 2 of them closely
divided floor votes. Neither became law, because a Michigan constitutional
amendment goes to the voters rather than to the Governor and so can never reach
LegiScan status 4. Both fall inside the not-enacted scope.

Other states are unaffected. No other registered state has a bill number
without digits.

## 2. `Given Immediate Effect` had to be classified as passage — FIXED IN CONFIG

Recorded here because reading it the other way is an easy and expensive
mistake, and because West Virginia's configuration excludes a caption that
looks very similar.

In West Virginia the vote on when an act takes effect is a genuinely separate
question, needs two thirds, and is often closely divided. It is excluded.

In Michigan there is no separate vote. The journal prints one line,
`Passed; Given Immediate Effect Roll Call #5 Yeas 67 Nays 38`, and LegiScan
stores the description with the word `Passed;` dropped. Excluding the family
would have discarded 644 of the House's 717 floor votes.

Verified by matching all 1,218 floor votes to their own bill-history lines.

## 3. The bare caption cannot distinguish a concurrence from a nonconcurrence — KNOWN ROLLS HELD

81 Michigan floor votes carry a description that is only a roll number, such as
`House Third Reading: Roll Call #12`. The question sits in the bill-history
line before the roll-call line, which the fetcher does not read.

They are classified `concurrence`, because 64 of the 85 rolls in the wider
family are substitute or amendment concurrences. Three are nonconcurrences, where
the chamber **rejected** the other chamber's text. A nonconcurrence is still a
recorded vote on the measure, but it is the opposite disposition, and a record
describing it as a concurrence would be false.

Not fixed, because the fix is not a pattern. It would mean giving the
classifier the bill history and having it read the action before the roll-call
action, which is a change to what the classifier is allowed to see.

The three nonconcurrences in the dataset (HB 4706 2-107, SB 878 2-32,
HB 5630 1-105) were found by reading each bare-caption roll's preceding
history line and are held by id, so the caption rule cannot queue them as
concurrences. All three are lopsided rejections of the other chamber's
substitute and would never have been selected anyway.

**The selection-time rule still stands** for any roll fetched later in this
family: read the preceding history line and confirm the question before
writing a word of description. This is already required by filter 4, which
asks which text the chamber actually voted.

## 4. One House action stored as two roll calls — HANDLED BY A HELD ENTRY

HB 4002's House vote of 2025-02-20, 81-29, appears twice: roll 1497863 as
`House Third Reading: Roll Call #12` and roll 1550992 as
`House Third Reading: Roll Call Roll Call #12`. Both point at the single
history line `Roll Call Roll Call #12 Yeas 81 Nays 29`.

The fetcher's identity key includes the raw description, so it cannot collapse
these two. This is the Texas duplicate hazard in a new shape: there the
duplicate was a re-issued roll-call id with an identical description, here it
is one action written under two descriptions.

Roll 1550992 is held by id with that reason, and the doubled
`Roll Call Roll Call` caption is deliberately left unmatched by the kept
patterns, so a future double filing surfaces for a person rather than being
queued as a vote.

Measured across the whole session: 49 groups share a chamber, bill, date and
tally while carrying more than one roll call. All but about eight are committee
reports, which are excluded anyway, and only this one is on a kept measure type
with a floor caption.

## 5. Last-day member lists disagree with the journal — HANDLED BY HELD ENTRIES

Every Michigan roll in the dataset lists only the members who voted yea or nay;
`nv` and `absent` are always 0. So the member list is the tally, and a tally
that disagrees with the state's record means members are missing from the
list, not that a count is off.

Checked by matching every floor roll to the bill-history line carrying the
same date, chamber and roll number (`... Roll Call #315 Yeas 60 Nays 48 ...`).
1,214 of the 1,218 floor rolls have such a line; 17 disagree. Sixteen are
House rolls of 2026-07-03, the session's last sitting day, out of 55 that day:
usually one to three members short, twice a yea where the journal has a nay
(HB 4103, HB 4396). HB 6130 is the closely divided case, 60-45 against 60-48.
The seventeenth is HR 19 of 2025-02-11, a simple resolution, out of scope by
type and not held.

The sixteen are held by id. Correcting the counts alone would leave the
missing members without records and could leave a flipped member with a
wrong one, so the hold stays until each list is checked against the House
Journal for 2026-07-03. The check is a one-off script against the dataset;
it is not built into the fetch. A general fetch-time comparison of each roll
against its history line would have caught West Virginia's 91-2 / 92-2 roll
too, and is the natural follow-up if a third state shows the same thing.
