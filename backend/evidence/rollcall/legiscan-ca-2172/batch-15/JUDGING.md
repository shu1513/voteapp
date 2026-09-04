# batch-15 — what was decided and why

## AB 2646's title promises far more than the bill delivers

The title reads "Employment: minimum wages: agricultural workers", which sounds like a farm-worker
wage floor. It is not. The $19.75 rate reaches only workers brought in from outside California
through an approved application, plus California residents doing matching work for the same
employer, in the same county, at the same time. An employer using no out-of-state workers owes
nothing extra, so most California farm workers are untouched. The description says exactly that.

The yearly increase is also tied to the Social Security cost-of-living adjustment rather than a
straight price index, which means no increase at all in a year with no adjustment. That is stated
too.

## Two measures kept despite reaching only one county

SB 1379 and SB 1414 are Riverside-only and San Bernardino-only. Both were kept because they make a
substantive policy choice, and both descriptions end by naming the county. See PLAN.md for the
test and for the four single-county measures dropped under it.

## AB 604 and ACA 8 were both read and both dropped

These are the mid-decade congressional redistricting measures, and they are among the most
consequential votes in the session. AB 604 writes a full 52-district map into statute; ACA 8 is
the constitutional amendment that would switch it on by setting aside the independent commission
for congressional maps through 2031.

They were dropped, and the reason matters. The research area list has no entry for redistricting
fairness. The nearest candidates — `anti_corruption`, `election_integrity`, `impartiality` — would
each require asserting that legislature-drawn maps are corrupt or unfair. That is a contested
political claim, not a fact, and a stance label carrying it would misrepresent the legislators on
both sides. Both are recorded in the ledger as read and dropped with that reasoning, so a later
batch does not quietly re-add them.

## SB 1425 is narrower than "high-speed rail" suggests

The bill is about trespass, encroachment and damage on rail land. It says nothing about funding,
routing or whether the system gets built. A reader who saw only the title would assume otherwise,
so the description opens with what the bill actually covers.

## A transient import error, and why nothing was lost

The run that imported batch-14 reported one error: AB 2128's Assembly roll failed validation with
"citation URL fetch timed out". That roll had already imported cleanly during batch-12, and its 66
records were present and unchanged. The importer re-validates every approved roll on each run, so a
network timeout during re-validation leaves the existing rows alone rather than corrupting them.
The reconcile still balanced. The next run re-attempts the check.

## Verification

Lint: 44 descriptions, 0 warnings, longest sentence 43 words against the 45-word limit. Two
defects were fixed before judging — a 47-word sentence in SB 1379 and British spellings in SB 1371
and SB 1425 that an earlier version of the screen did not cover.
