# California code findings (LegiScan session 2172)

Two things the California dataset exposed. §2 is FIXED in the config; §1 is RECORDED, not fixed.

## 1. RECORDED, NOT FIXED — 80 roll pairs are identical except for `date`

`legiscanRollCallIdentityKey` (added for Texas, where LegiScan re-issued 640 `roll_call_id`s for one
Senate action) keys on `(chamber, bill_id, date, desc, yea, nay, nv, absent, passed, sha1(member
list))`. Because `date` is part of the key, it does **not** collapse California's variant of the same
defect: **80 groups of two rolls that agree on every one of those fields except the date**, 71 of
them floor votes on kept bill types, **15 of them divided**.

The bill history proves only one vote happened. SB 42 is the clean example: the dataset carries
concurrence rolls 1602271 (2025-09-12, 29-8) and 1602930 (2025-09-13, 29-8) with the identical
29-senator lineup, and the history records exactly one — `2025-09-13 S Assembly amendments concurred
in. (Ayes 29. Noes 8. Page 3038.)`. SB 763 repeats the pattern (09-12 / 09-13, both 29-8, one
history line on 09-13). 68 of the 80 pairs are one day apart; the other three are 6, 7 and 8 days.

**Why the naive fix is wrong:** dropping `date` from the shared identity key changes Texas, Georgia
and every future state, and a chamber CAN legitimately vote the same bill twice with the same tally
and the same lineup on two different dates — the Texas review already caught this
(SB 13's third reading and its conference report were both 23-8 with the same 23 senators, 2.5 months
apart). Those two differ in `desc`; these 80 do not. A California-specific rule — collapse only when
the descs match too and the dates are within N days — is plausible but has never been measured
against another state, so it stays a finding.

**Why it is not urgent:** the fan-out dedupes on `ls:<roll_call_id>`, so importing BOTH members of a
pair would write two near-identical records on the same legislator. Batch selection is what protects
us: filter 4 takes one roll per measure per chamber, and where a pair exists the pick must be the
roll the bill history names. Batch-01 hit this exactly once — SB 42's Senate concurrence, where
1602930 (09-13) was selected because the history names 09-13.

**Before any future California batch:** check the picked roll against the pair list, i.e. that no
other stored CA roll shares its chamber, bill, desc, tallies and member list.

## 2. FIXED — a trailing ` Reconsider` is the vote granting reconsideration

The California Senate labels the vote that GRANTS reconsideration with the original question's
wording plus a trailing ` Reconsider`, so it looked like passage or concurrence to the classifier.
SB 627 shows the whole sequence in one day of history:

```text
2025-09-11 S Assembly amendments concurred in. (Ayes 27. Noes 10.)   <- roll 1601937, concurrence
2025-09-11 S Motion to reconsider made by Senator Wiener.
2025-09-11 S Reconsideration granted. (Ayes 30. Noes 10.)            <- roll 1601938, "… Concurrence Reconsider"
2025-09-11 S Assembly amendments concurred in. (Ayes 28. Noes 11.)   <- roll 1601939, the operative vote
```

15 floor rolls in CA 2172 carry the suffix, all Senate; only SB 627's is divided (the other 14 are
39-0 / 40-0 style). `excludedQuestions` now holds `/\breconsider$/`.

The anchor matters: an unanchored `/\breconsider/` also swallows the 91 committee `Reconsideration
granted` / `Reconsideration of favorable vote granted` rolls. Those are rejected by the tally cut
today and so are never stored; matched as excluded questions instead, they WOULD be stored as
non-floor audit rows (measured: the queue grew by 80 rows before the anchor was added).
