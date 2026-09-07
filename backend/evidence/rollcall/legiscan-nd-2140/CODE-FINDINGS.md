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

## 4. The enrolled act is the cleanest judging source in the campaign, and it carries its own tally

North Dakota's enrolled act is plain text with **no strikethrough and no underline** — the
struck-text hazard that GA, ME, MT, KY, AR, CO, AK, OR and MO all have simply does not
arise on the enrolled print.

It also prints its own vote counts on the signature page:

```
House Vote:   Yeas 51   Nays 40   Absent 3
Senate Vote:  Yeas 29   Nays 18   Absent 0
```

That is a third independent tally source, on the enacted document itself, better than a
history line. All 14 batch-01 rolls were checked against it and all 14 match.

The trade-off is that an `amend and reenact` section reprints the **whole** section,
including law it does not change, so the enrolled act over-reports change. To see what
actually changed, read the introduced print, which does carry the markup: North Dakota
draws deletions as a strikethrough through the glyphs and additions as an underline below
the baseline, and `pdftotext` throws both away. `/Users/shu/legiscan-data/nd_text.py`
classifies each drawn rule by where it sits relative to the baseline and prints deletions
as `[[...]]` and additions as `<<...>>`. It was verified against a known-bad input before
being relied on.
