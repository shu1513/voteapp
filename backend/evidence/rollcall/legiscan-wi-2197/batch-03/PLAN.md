# Wisconsin batch-03 — plan

Batch-03 continues the **vetoed pool**. The scope rules were declared in
batch-02's PLAN.md and are unchanged:

- Filter 2 is read as "the measure passed both chambers", because Wisconsin's
  enacted pool is small and the vetoed pool is where the parties disagreed.
- Every description is written in the conditional.
- Every description ends by naming how the measure died, as a completed fact.
  The session has adjourned; nothing in this pool can still become law.
- The text read is the enrolled print, which a vetoed Wisconsin bill has.

Local database only. Production holds no Wisconsin roll-call records.

## What is in batch-03

The **crime, courts and policing strand**. Ten measures were read and all ten
were selected, which is unusual and worth saying: criminal law bills describe a
single mechanism, and `public_safety_and_crime_control` names both enforcement
and accountability, so direction is easier to defend here than in the education
strand.

AB 73, AB 85, AB 87, AB 629, AB 672, SB 25, SB 76, SB 146, SB 432, SB 610.

That is **18 roll calls**. Eight measures were divided in both chambers; AB 629
and SB 25 were divided in one.

## Progress through the vetoed pool

| | measures | slots |
| --- | --- | --- |
| vetoed pool, all subjects | 74 | 122 |
| read in batch-02 (education) | 13 | 21 |
| read in batch-03 (crime and courts) | 10 | 18 |
| left for later batches | 51 | 83 |

Remaining strands: immigration and measures aimed at foreign adversaries;
health, gender and civil rights; labor, unemployment insurance and taxes;
elections, environment and state government operations.

## Procedure

Identical to batch-02. Worklist from `tools/wi_pool2.py`, enrolled print read
with `tools/wi_text.py`, version check on every slot, one body per measure,
plain-language lint, then judge dry, judge, import dry, import, reconcile.
