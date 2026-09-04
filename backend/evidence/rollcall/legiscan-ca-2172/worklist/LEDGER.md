# Campaign ledger

`ledger.json` tracks all 244 live measures through the campaign. Every measure ends in one of
two terminal states, and **both are progress**:

- `judged` — imported, with the batch that did it
- `dropped` — read and excluded, with the reason recorded

A measure is never left in `open` once it has been read. Batch-07 judged 8 of 31 measures it
read; the other 23 are a result, not a gap. The reason string is what stops a later batch from
re-reading the same bill and reaching a different answer.

## Why the ledger and not just the batch dirs

`pool.py` derives "worked" from the judgment files, so a measure that was *read and dropped*
looks identical to one never touched. That is how AB 1078 came back after batch-01 dropped it.
The ledger is the memory that prevents the repeat.

## Driver

`runbatch.py judge <batchdir>` runs lint, then the judge dry-run, then the real judge, and stops
on any lint warning. `runbatch.py import <batchdir>` runs the import dry-run, the real import, the
re-run, and reconciles the row delta against the prediction. Both fail closed.
