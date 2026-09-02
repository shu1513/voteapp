# Alabama 2026 session (LegiScan 2218) — findings recorded, not fixed

## 1. One printed roll call number, filed under two different bills

The 2026 dataset attaches the same physical House vote to more than one bill. Two clear cases:

- Roll Call 1040 (71-21 on 2026-03-31) is filed under **both** HB 593 (roll_call_id 1674185) and
  HB 631 (roll_call_id 1676015), with identical tally and identical member list. HB 631's history
  records Roll Call 1040; HB 593's does not.
- Roll Call 1073 (61-35 on 2026-03-31) is filed under **both** HB 593 (1675846) and HB 169
  (1674596). HB 593's history records Roll Call 1073; HB 169's does not.

**The test that separates them is the bill's own history.** Every Alabama roll description ends
with `Roll Call <n>`, and the bill history records the same number on the action line. A roll whose
number does not appear in its own bill's history is misfiled.

Measured across both Alabama sessions:

| Session | Kept floor rolls | Number found in that bill's history | Misfiled | No number in the description |
|---|---|---|---|---|
| 2148 (2025) | 917 | 911 | **0** | 6 |
| 2218 (2026) | 1,081 | 1,048 | **20** | 13 |

So the 2025 session is clean and batch-01 of that session is unaffected. In 2026, 2 of the 20
misfiled rolls are divided, and one of those (HB 593's 1674185) is on a measure that became law, so
it would have entered a batch on subject alone.

This is a selection-time rule, not a classifier rule: the fetcher reads descriptions and has no
access to bill history, so nothing here belongs in `legiscanStateConfigs.ts`. **Every future Alabama
batch must check each candidate roll's printed number against its own bill's history before
selecting it.** The 13 descriptions with no roll number at all (for example HB 475's
`Smith motion to Concur in and Adopt Senate Amendment to HB475`) are matched on the action text and
date instead, which is how HB 475's imported roll was verified.

## 2. A failed Budget Isolation Resolution can print without the words "Budget Isolation Resolution"

HB 583's roll 1665428 (47-37) is captioned only `Lost in House of Origin`, which reads like a failed
passage vote. The bill history line for the same action says `BIR Lost in House of Origin` — it is a
Budget Isolation Resolution that failed to reach three fifths. The config excludes
`^lost in (house of origin|second house)$` for this reason. The 2025 session spells the same event
`Motion to Adopt BIR- Failed`, which the `motion to adopt bir` rule already caught.
