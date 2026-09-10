# Wisconsin batch-02 — plan

Batch-02 opens the **vetoed pool**: measures that passed both chambers on a
closely divided vote and were then vetoed by the governor. The operator chose
this scope explicitly, alongside the enacted pool that batch-01 closed.

Local database only. Production holds no Wisconsin roll-call records.

## The declared exception to filter 2

The campaign's second selection filter is normally "the measure became law".
Wisconsin has divided government, so the enacted pool was small — 24 measures in
two years, all of which batch-01 dispositioned. The vetoed pool is five times
larger and is where the two parties actually disagreed.

**For this scope, filter 2 is read as "the measure passed both chambers".** A
recorded vote to pass a bill is a position taken, whether or not the governor
signed it. Everything that follows from that exception is stated as a rule:

1. Every description is written in the conditional. The bill "would have"
   required something; it never required anything.
2. Every description ends by naming how the measure died.
3. The tail states a completed fact, not a hedge. The 2025-2026 session has
   adjourned, so no bill in this pool can still become law.
4. The text read is the **enrolled print**. In Wisconsin a bill is enrolled and
   presented to the governor before it is vetoed, so a vetoed bill has exactly
   the same authoritative print an act has. All 74 measures in this pool have
   one.

## What is in batch-02

The **education strand**: every vetoed measure whose subject is schools,
colleges or what pupils are taught. Thirteen measures were read in full; nine
were selected and four were dropped. See JUDGING.md for each drop.

Selected: AB 1, AB 5, AB 166, AB 457, AB 582, AB 614, AB 1005, SB 389, SB 532.

That is **15 roll calls** — six measures were divided in both chambers.

## The vetoed pool, and what is left after this batch

| | measures | slots |
| --- | --- | --- |
| vetoed pool, all subjects | 74 | 122 |
| read in batch-02 | 13 | 21 |
| selected in batch-02 | 9 | 15 |
| left for later batches | 61 | 101 |

The remaining strands, grouped the way later batches should take them:

- crime, courts and policing
- immigration, and measures aimed at foreign adversaries
- health, gender and civil rights
- labor, unemployment insurance and taxes
- elections, environment and state government operations

## Procedure

Same as batch-01, with one change: the worklist is built from the database by
`tools/wi_pool2.py`, which reads the rolls the fetcher already classified with
the shipped config. Building it from a second copy of the config patterns in
Python is what let the two drift apart once.

1. Build the worklist and take the vetoed slots.
2. Read the enrolled print of each candidate measure with `tools/wi_text.py`,
   which resolves Wisconsin's strike-and-underline markup.
3. Apply the five filters, with filter 2 read as above.
4. Run the version check: any amendment adopted after a chamber's roll must be
   read before that roll is used.
5. Write one body per slot, generate the yes and no descriptions from it, and
   run the repository's plain-language lint.
6. Judge dry, judge, import dry, import, reconcile.
