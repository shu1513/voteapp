# North Dakota code findings

Recorded, not fixed.

## 1. Constitutional amendments cannot be queued, because North Dakota rides them on concurrent resolutions

North Dakota proposes a constitutional amendment as a **concurrent resolution**, LegiScan
bill type `CR`. `LEGISCAN_KEPT_BILL_TYPES` is `["B", "JR", "JRCA", "CA"]` and the check
runs before the state config is read, so every `CR` roll is rejected as
`excluded_measure:CR` and can never reach a batch.

Measured cost: the session holds 66 concurrent resolutions with 53 floor rolls, of which
**9 are closely divided and adopted**.

The naive fix is wrong, and it is wrong the same way it was in Georgia. The `CR` type also
carries ceremonial resolutions, and North Dakota's own vocabulary does not separate them:
an amendment referral and a commendation both read `Second reading, adopted yeas N nays N`.
Keeping `CR` wholesale would queue commendations.

There is a second reason to be careful here. A concurrent resolution proposing an amendment
never goes to the governor and does not become law — it goes to the voters. So any
description written for one must never say it became law, and if it reached the November
2026 ballot it also falls under the ballot-measure expiry rule: the description is a claim
about the future and has to be revisited once that election happens.

## 2. LegiScan stores a division or floor-amendment vote under the second-reading caption

North Dakota can **divide a bill** and vote the divisions separately, and it takes recorded
votes on floor amendments. LegiScan sometimes files one of those under the plain
`Second reading` caption, so the stored roll is not the question the caption names.

Found by auditing all 2,087 second-reading rolls on kept bill types against North Dakota's
own history tallies: 2,076 exact, 11 wrong. The audit was deliberately not bounded by the
divided gate, following the Oregon SB 1565 lesson that a tally error can itself decide
whether a roll enters the pool.

Examples:

- **SB 2018** — the history reads `Division D passed / Division E passed / Division F
  passed / Second reading, passed as amended, yeas 61 nays 31`. The feed stores 51-41.
- **SB 2158** — the feed stores the 12-35 floor amendment the history records as failed.
  The reading itself passed 38-9.
- **HB 1038** — the feed stores 15-29, the lost `Division A`. The reading passed 40-4.

These are not wrong tallies. They are the wrong question, which no tally check alone would
catch — the number is real, it just belongs to a different vote.

## 3. One roll's member list copied onto other bills

Four bills record a **byte-identical 86-7 member list** on 2025-03-18. Only SB 2157 (roll
1520590) actually voted 86-7, and its history says so. The other three contradict their own
histories:

| bill | roll | feed | North Dakota's history |
|---|---|---|---|
| HB 1080 | 1520595 | 86-7 | 93-0 |
| HB 1551 | 1520370 | 86-7 | 89-4 |
| SB 2290 | 1520236 | 86-7 | 88-5 |

HB 1080 is the worst of the three: the copy turns a **unanimous** vote into a divided one,
which is exactly how a defective roll enters a pool it should never have reached.

Same class as the Kansas SB 63 and North Carolina H244 findings, but propagated across
bills rather than within one. Detect it by grouping rolls on a hash of the sorted member
list and looking for a group spanning more than one bill.

All eleven rolls from findings 2 and 3 are listed in the config's `heldRollCallIds`, so
they are stored and surfaced but can never be approved.

## 4. The enrolled act carries change markup that `pdftotext` discards, and it prints its own tally

**Correction, 2026-09-08.** The first version of this finding said the enrolled act was plain
text with no strikethrough and no underline. That was wrong, and the review of #1241 caught
it. It came from checking HB 1318, a two-page act that creates one new section, and
generalizing. Rendering HB 1114's enrolled print shows the truth: every line of a section the
act **creates** is underlined, and in a section the act **amends and reenacts** the deleted
text is struck through and the replacement is underlined — the same markup as the introduced
print. `pdftotext` throws both away, so a plain extract of an amended section shows repealed
law as if it were live. Same family as the Georgia, Maine, Montana, Kentucky, Arkansas,
Colorado, Alaska, Oregon and Missouri hazards.

Exposure in batch-01 was checked measure by measure. Seven of the nine acts only create new
sections or repeal one, so there is no struck text in them. HB 1114's description was written
from its new section 1 and did not depend on the amended section 2. HB 1216's amended section
2 changes one thing, an underlined cross-reference that extends the new rule to self-insured
plans — the description had **omitted** that reach and was corrected in the review round. No
description had treated deleted text as live.

**Rule:** render the page for every amend-and-reenact section before writing anything about
what the act changed. `/Users/shu/legiscan-data/nd_text.py` marks deletions `[[...]]` and
additions `<<...>>` by where each drawn rule sits relative to the baseline; treat it as a
first pass, because on the enrolled print it mislabels some headings, and confirm against the
rendered page.

The act also prints its own vote counts on the signature page:

```
House Vote:   Yeas 51   Nays 40   Absent 3
Senate Vote:  Yeas 29   Nays 18   Absent 0
```

That is a third independent tally source, on the enacted document itself, better than a
history line. All 14 batch-01 rolls were checked against it and all 14 match.

An `amend and reenact` section still reprints the **whole** section, so even with the
markup read correctly the act over-reports change: unmarked text is existing law being
carried along, not something the act does.

## 5. LegiScan's `passed` flag is wrong on 23 rolls, and it is not held

Auditing every stored floor roll against the outcome North Dakota's own history line
records found **23 rolls with `passed: 0` where the state says the reading passed**:
thirteen at 91-1 on 2025-03-10, nine at 75-9 on 2025-03-21, SB 2003's 42-2 Senate vote, and
SB 2261's 45-2 Senate veto override (the House then sustained the veto, which does not change
what the Senate did). Every one of the 2,053 other rolls agrees with the history.

The fetcher copies the flag into `legislative_votes.result` as `"Failed"`. No fan-out, judge
or import path reads that column, and the description a voter sees is written from the
act, so the stored string is inert metadata.

The rolls are deliberately **not** held. Their tallies and member lists match the state's
record, `heldRollCallIds` exists for rolls the survey proved wrong, and none of the 23 is
closely divided, so none can enter a batch. Holding twenty-three real passing votes to correct
a string nothing consumes would be the wrong fix, and there is no override column to correct
it with.

What the finding does change is the **selection rule**: read a roll's outcome from the bill
history's own action line (`Second reading, passed` or `failed to pass`), never from the
caption and never from `passed` alone. The nine closely divided rolls on enacted bills that
carry `passed: 0` were re-checked under that rule and every one is a genuine failure that the
chamber reconsidered and re-voted, so batch-01's pool was not affected.
