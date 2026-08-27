# Illinois LegiScan import — code findings (recorded, not fixed)

Follows the Ohio (`ohio-136/CODE-FINDINGS.md`) and Texas
(`legiscan-tx-2160/CODE-FINDINGS.md`) convention: defects or missing
mechanisms found during the data campaign that need CODE, parked here rather
than hand-patched around.

## 1. No official-date override — and every hand-fix is provably unsafe

**The gap.** S.B. 3777's House third reading is officially dated **6/1/2026**
(ILGA BillStatus XML); LegiScan stamps it 2026-05-31, because the dataset
holds **no June dates at all** — the 2026 sine-die session ran past midnight
and LegiScan kept the legislative day. The stored row, the judgment, and the
91 fanned-out records all carry the LegiScan date, so 91 candidate records
display and sort one day early. Review flagged this twice (2026-08-27), the
second time asking for "an explicit reviewed override for `event_date`".

**That override does not exist in the pipeline, and no hand-edit can
simulate it safely.** Each path was checked against the code, not assumed:

- **SQL on the records only** (`event_date` → 6/1): the importer's duplicate
  scan is date-scoped — `loadExistingRecordsForDate` filters
  `event_date = vote.voteDate` (`rollCallFanOut.ts:244`). A moved record
  becomes invisible to the scan, so the next batch-01 re-run plans `insert`
  and writes **91 duplicates**.
- **`legislative_votes.vote_date` + judgment** → 6/1: the importer
  hard-fails its own cross-check — `evidence roll_call is dated 2026-05-31,
  the approved row 2026-06-01` (`importLegiscanRollCallVotes.ts`, the
  `rollCall.date !== vote.voteDate` guard) — so batch-01 could never re-run
  or converge again.
- **Editing the evidence file's date**: the sha256 pins the `roll_call`
  element bytes → `source_mismatch`, exit 1.

These guards are doing their jobs; the missing piece is a first-class
override, not a loophole.

**Parked design** (build only after the parallel GA/FL/CA/TN campaigns land
— it needs a migration, and concurrent sessions sharing `db/migrations/` is
the known conflict zone):

- New nullable column `legislative_votes.official_vote_date`.
- `rollcall:judge` accepts an optional `official_vote_date` per judgment
  (evidence: the official source that dates it — for Illinois, the ILGA
  BillStatus XML action trail).
- The importer keeps every existing evidence-vs-row check on the raw
  LegiScan `vote_date`, but uses `official_vote_date ?? vote_date` for the
  record template's `event_date`, the `loadExistingRecordsForDate` scan, and
  the notification window. Identity keys include the event date, so setting
  an override on an already-imported roll rewrites its records in place —
  the designed rewrite path, with identity transitions logged.

**Measured exposure** (why this is worth building at all): 70 of the 401
pending divided-and-enacted rolls sit on a sine-die calendar day — 39 on
2025-05-31 and **31 on 2026-05-31**. Batch-01's audit found the checked
2025-05-31 rolls exact (that session's tail did not skew) and the one
2026-05-31 roll skewed, so the suspect pool for future batches is those 31
rolls, of which the after-midnight subset will skew. Until the override
exists, a batch judge has two honest choices for a skewed roll: import it
with the LegiScan date and document (batch-01's call for S.B. 3777 — the
cited source page shows the date it asserts, and no description states a
date), or leave it pending with a `date-skew` reason. Never hand-edit.

## 2. (none further)

The TX duplicate-roll_call_id fix, the two-spelling desc families, and the
committee rule all verified clean here — see the README.
