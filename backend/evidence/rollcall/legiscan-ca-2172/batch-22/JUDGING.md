# batch-22 — what was decided and why

## The ledger caught a real gap in itself

Two measures, SB 403 and SB 1354, appeared in the worklist as never worked. They had in fact been
read and dropped in batch-09, months earlier, with reasons written into that batch's PLAN.md.

They resurfaced because `pool.py` derives "worked" from the committed judgment files, and a
measure that is read and deliberately dropped produces no judgment. That is precisely the failure
that let AB 1078 come back after batch-01 rejected it, and it is why the ledger exists — but the
ledger was built in batch-10, after those two drops had already happened.

Both are now recorded with their original batch-09 reasoning. Any measure dropped before the
ledger existed can do this, so the check before starting a batch is to grep the earlier batch
documents, not only the ledger.

## Regional bills kept, with the geography in the description

**AB 851** bans cold-call offers to buy homes in fifteen named ZIP codes across Los Angeles and
Ventura Counties, and expires at the start of 2027. **AB 797** would have set up a state-backed
programme to buy and resell homes destroyed by the same fires in the same two counties.

Both make a substantive policy choice rather than merely scheduling a local ballot measure, so
both were kept under the rule settled in batch-15. Each description names the counties and the
expiry so no reader takes them for statewide law.

AB 797 also splits profit 90 percent to the investing banks, 5 percent to the entity managing the
property and 5 percent to the state. That is stated plainly, because a reader may weigh a bill
titled Community Stabilization differently once they see where the upside goes.

## AB 499 helps exactly one named organisation

It lowers the threshold above which the state reimburses the Robert F. Kennedy Farm Workers
Medical Plan for large claims, from $70,000 to $50,000. The $3 million annual cap is unchanged, so
the same money is spread across more claims rather than more money being added. Both facts are in
the description, along with the statement that the bill reaches that one plan and no other.

## Four measures dropped

AB 52 only permits the secretary to establish advisory committees that may never exist. AB 749
creates a study commission contingent on funding and self-repealing in 2033. AB 935 creates annual
public reports on discrimination complaints while making the underlying data confidential and
exempt from the public records act, which is two directions on one dimension. AB 632 covers four
unrelated violation types and fits no research area cleanly.

## Verification

Lint: 32 descriptions, 0 warnings, longest sentence 40 words. Five British spellings were
corrected before judging.

One process note: the import for this batch was first launched detached with its output
discarded, the pattern documented earlier as unsafe because the process dies with the shell. It
was killed, the live count confirmed unchanged at 10,525 so nothing was half-written, and the
import re-run properly.
