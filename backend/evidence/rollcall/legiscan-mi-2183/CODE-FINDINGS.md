# Michigan code findings — LegiScan session 2183

Things about the pipeline that Michigan exposed. Findings 1 and 2 are fixed in
the config pull request. Finding 3 is recorded and not fixed.

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

## 3. The bare caption cannot distinguish a concurrence from a nonconcurrence — NOT FIXED

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

**The selection-time rule instead:** for every roll picked out of this family,
read the preceding history line and confirm the question before writing a word
of description. This is already required by filter 4, which asks which text the
chamber actually voted.

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
