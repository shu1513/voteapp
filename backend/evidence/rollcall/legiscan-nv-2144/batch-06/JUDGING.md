# Nevada batch-06 — judging and import

**Result on local `voteapp`, 2026-09-06: 17 roll calls approved, 332 records inserted
across 41 candidates, 0 errors, 0 notified. Production untouched.**

Nevada now holds **1,750 live roll-call records**.

## Sources

Each measure was judged from the text the chamber actually voted, downloaded through the
LegiScan `getBillText` API with both the byte length and the MD5 checksum verified against
the dataset. Every document passed both checks, including the SB 352 conference text that
was fetched after the fact.

## ⚠ A method error found in this batch, and what it changes

When comparing two printed versions of a bill, this campaign had been filtering short diff
fragments out of the output as page furniture. **That filter hides one-word changes, and a
one-word change is exactly what a version split usually is.**

It was caught on AB 411. A word-level diff with a "skip fragments under 10 characters" rule
reported that the Assembly's text and the Senate's text differed only in the reprint header.
They do not. The Assembly voted "must include the name of the prescribing health care
practice" and the Senate voted "may include". The filter had thrown away the only difference
that mattered.

**The rule now is: never filter a version diff by fragment length.** Filter only true page
furniture — bare numbers, dashes, and the `*BILLNAME_R1*` running stamp — and flag any
fragment containing must, may, shall or not.

Both earlier version comparisons were re-run under the corrected rule:

- **AB 204** (batch-04, already imported): re-checked, nothing was hidden. The only
  difference between the Assembly's third reprint and the Senate's fourth remains the
  six-month cap on the collection pause during a federally declared emergency, which is what
  batch-04 recorded.
- **SB 352** (this batch): re-checked with modal-verb flagging. The substantive addition is
  the limited-benefit policy exemption, which is what the Assembly record now names.

## Superseded-stage gate

The gate did not fire on any roll in this batch.

## Labels

Ten measures, ten research-area labels, all with `"nay": null`.

**SB 182 was placed in `environment_and_public_health`, not `healthcare_affordability`.**
Maximum patients per nurse is a rule about the safety and quality of care, not about what
care costs or who can get it. This follows the batch-02 precedent, where the same area
carried AB 194 and SB 157.

**SB 173 was kept despite covering five subjects.** It bans PFAS chemicals in consumer
goods, makes food delivery apps ask before sending plastic utensils, funds shade planting in
hot neighborhoods, adds a food-contact-surface inspection duty, and requires testing of hair
products. The earlier grab-bag drops in this campaign — Connecticut's sixty-section
transportation bill, Missouri's omnibus bills — were dropped because their strands pointed
in **opposite** directions, not because there were many of them. SB 173's strands all point
the same way, so a yes vote has one defensible meaning. The description names the two
largest strands and says the bill also covered other environmental measures.

## Wording checks, all run before the import

- The real `candidateRecordPlainLanguageLint` over all 34 descriptions: **0 warnings**.
  It flagged four on the first pass — AB 282's opening sentence ran to 46 and 48 words — and
  that sentence was split into two before anything was imported.
- Every description is 2 to 4 sentences.
- British spellings scanned for: none found.
- Every description cites its own roll call's tally, checked against the stored row.
- Measure, date and chamber checked against the stored row for all 17 judgments.

## Reconciliation — three ways

| check | result |
| --- | --- |
| import report | 332 inserts, 1,418 unchanged, 0 errors |
| run-stamp predicate `rollcall:NV:%:2026-09-06T07:45:55.360Z` | 332 records, 41 candidates |
| table delta | 1,750 − 1,418 = 332 |

Per-roll fan-out: 27 to 30 candidates on Assembly rolls, 10 or 11 on Senate rolls, none at
zero. SB 128's Assembly roll reaches 27 rather than 30 because three members did not vote.

## Duplicate sweep

Swept with `origin_run_id NOT LIKE 'rollcall:%'`. Within a candidate there are **0 duplicate
record identity keys and 0 duplicate source URLs** across all Nevada roll-call records.

Seven hand-written rows matched a measure number in this batch. Four are other states'
bills that share a number — Arkansas SB 352, Michigan SB 173, Missouri SB 128 — and two are
Nevada sponsorship rows for AB 414 and AB 244. All six were kept.

**The seventh is a partial overlap and was deliberately kept.** Record
`93d6f0d1-f5e0-4751-8f1f-2a121762c49f` reads "Presented his own health-billing bill on the
Nevada Assembly floor and voted for it; the Assembly passed it 26-16", and that member now
also has a roll-call record for the same AB 282 vote. It was **not** retired, unlike the
AB 480 duplicate in batch-04, because it carries a fact the roll-call pipeline cannot
produce: this member wrote the bill and presented it on the floor. Retiring it would lose
the authorship and keep only the vote. The cost is one sentence of overlap between the two
rows, which is the better trade.
