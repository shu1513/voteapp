# CT batch-02 — judging and import

Judged from the OLR Public Act Summary for each act. No AI calls.

## Import ledger

Local `voteapp` only. **Prod holds no Connecticut roll-call records.**

| | |
|---|---|
| files | 7, all `imported`, 0 errors |
| inserts | **583** (the dry run planned 583 — exact) |
| candidates | **158** |
| notified | 0 (every vote is 2025) |
| run stamp | `2026-08-30T02:58:23.483Z` |
| `candidate_records` | 109,708 → 110,291 (**+583**) |

Reconciled three ways: the dry-run plan, the table delta, and the run-stamp predicate
(`origin_run_id LIKE 'rollcall:CT:%:2026-08-30T02:58:23.483Z'` = 583 rows / 158 candidates). The dry
run's own stamp `…T02:57:57.182Z` matches **0** rows, which proves `--dry-run` wrote nothing. The
re-run reported all 583 `unchanged` and left the count at 2,040. `import-report.json` is the
original insert ledger, copied out before the re-run, which wrote `import-rerun-report.json`.

**Connecticut now holds 2,040 records and 1,543 tags across both batches.**

158 candidates, not 159: the batch has one fewer House roll than batch-01 and no measure that every
mapped member voted on.

## Writing

Descriptions are written for a reader with no legal background, from the first draft rather than as
a later pass. Result: **lint 0 warnings over all 14 descriptions, mean sentence 14.1 words, longest
26.**

The lint only counts words in a sentence — it is not a plain-English check — so the register was
checked separately. Every term of art is explained where it appears:

- a **lien** is introduced as "a legal claim, called a lien", then referred to as "that claim";
- **assignment** of a lien is written as "sell that claim to a private buyer, who can force the sale";
- the **Shared Work** program is explained as one "which lets a company cut hours instead of laying
  people off";
- **bail bond agents** are explained as people who "track down people who were released on bail and
  did not come back to court";
- bills are named in full — "Senate Bill 1187", not "SB 1187".

Numbers were machine-checked against the OLR summaries: `$3,000`, three years, 40 days, 60 days, 48
hours, July 1 2025 and July 1 2026 all appear in the source. Both sentences of every judgment carry
the roll's own tally, which the approval gate requires.

Two details were deliberately kept even though they cost words, because shortening them would have
narrowed the law:

- **Senate Bill 1367's list of protected places** stays complete — any state-run or state-licensed
  health care facility, the office of a licensed health care provider, any school or college, and
  any house of worship.
- **Senate Bill 1367's old rule** needed "with proof of it". A first draft said only that the person
  had to be locked up in another state and that prosecutors declined to bring them back; the act
  also requires proof of the detention.

## `related` flags: 5, all reviewed, none a duplicate

Four existing hand-written records were flagged, all because they share a vote date with one of our
rolls and none because they describe the same vote: Dave Yaccarino and Chris Aniskovich on **House
Bill 5002** (zoning, 2025-05-28), Arnold Jensen on **House Bill 5428** (mobile manufactured-home
parks) and on **House Bill 7192** (drug task force), and Carol Hall on **the state budget**. This is
the same date-scoped false positive batch-01 saw. **No records retired.**

## The superseded-stage gate: 2 acknowledgements

The gate scans same-or-later **dated** kept floor votes on the same measure and chamber. Connecticut's
Senate votes its floor amendments on the passage day under the **same desc** as passage, so those
amendment rolls are stored as kept floor votes and trip the gate on the decisive roll itself. This is
now the expected Connecticut pattern, not a surprise.

Both peers were checked against the action trail, and each is EARLIER by printed vote number and
FAILED, so neither is a later passage stage:

- **Senate Bill 1221** senate vote 114 (passage) over roll 1564448 = vote 113, rejected Senate
  Amendment A, 10-17.
- **Senate Bill 1312** senate vote 250 (passage) over roll 1580353 = vote 249, rejected Senate
  Amendment B, 11-25.

The gate cannot see that ordering because LegiScan issues Connecticut `roll_call_id`s in reverse
within a same-day Senate batch (CODE-FINDINGS §4). The other five rolls cleared it untouched.

## Note on batch-01's wording

Batch-01's 1,457 descriptions were written before this plain-English standard and still read in a
heavier register ("summary review", "lookback period", "adjudicated a delinquent", "consumer price
index"). They are accurate but harder to read than these. Rewriting them is worth a separate pass,
the way Pennsylvania (#956) and California (#951) did theirs — not folded into this batch, because a
register change is an edit and every measure has to be re-read against its source.
