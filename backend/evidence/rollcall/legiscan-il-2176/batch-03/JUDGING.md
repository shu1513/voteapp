# Illinois batch-03 — the three date-skew rolls

3 rolls / 3 measures / **157 records across 126 candidates**. These are the
rolls batch-02 deliberately held at `pending:date-skew` rather than import on a
date known to be wrong.

## What made them importable

The `official_vote_date` override merged in
[#899](https://github.com/shu1513/voteapp/pull/899) (migration **257**), the
design parked in `../CODE-FINDINGS.md` §1. Each judgment now carries an
`official_vote_date` alongside the unchanged `vote_date`:

| measure | roll | LegiScan `vote_date` | `official_vote_date` |
|---|---|---|---|
| H.B. 5024 senate | 1716945 | 2026-05-31 | **2026-06-01** |
| S.B. 2437 senate | 1582772 | 2025-05-31 | **2025-06-01** |
| H.B. 5090 house | 1719024 | 2026-05-31 | **2026-06-01** |

Every date comes from the ILGA BillStatus XML action trail at
`https://ftp.ilga.gov/Legislation/104/BillStatus/XML/10400<BILL>.xml`. The
cause is the same in all three: the sine-die session ran past midnight and
LegiScan stamps the *legislative* day. `vote_date` is left exactly as the
evidence file says, so every evidence-vs-row check still pins to it; only the
records' `event_date` moves.

**Verified where it counts:** all 157 records landed on the official dates —
32 on 2025-06-01, 33 + 92 = 125 on 2026-06-01 — and together with S.B. 3777's
91 records there are now **248 Illinois records on corrected dates and none
left on a date the official record contradicts.**

## Two of the three are concurrence rolls

S.B. 2437's senate roll and H.B. 5090's house roll are concurrences, not third
readings, so their descriptions say the chamber **agreed to the other chamber's
version** rather than passed the bill. Each also carries the context a reader
needs:

- **S.B. 2437** — the Senate had passed a narrower doula-only version 56-0 in
  April; the 36-19 concurrence is the vote on the expanded Medicaid package,
  and the description says so, so nobody reads the earlier unanimity as
  agreement with the enacted law.
- **H.B. 5090** — the Senate replaced the bill's original
  construction-procurement text with the Transportation Network Driver Labor
  Relations Act on the last night of session, and the description names that.

Measure bodies are reused verbatim from batch-02 so both rolls on a measure
read identically.

## The duplicate hazard is now tested, not just reasoned about

`CODE-FINDINGS.md` §1 predicted that moving a record's `event_date` would blind
the date-scoped duplicate scan and cause a re-run to insert duplicates. #899's
follow-up commit `cd4d105d` finds prior imports by run-id prefix instead. The
re-run here exercises exactly that path: **3 files, all 157 `unchanged`**
(`import-verify-report.json`), Illinois total steady at 4,840. The cross-branch
warning in `../batch-02/JUDGING.md` is therefore historical — it described the
window before #899 merged.

## Runs

| step | result |
|---|---|
| `rollcall:judge --dry-run` | 3 rows |
| `rollcall:judge` | `{"updated": 3}` — queue to 79 approved |
| `rollcall:legiscan:import --dry-run` | 3 files, **157 planned inserts**, 0 errors |
| `rollcall:legiscan:import` | 3 files all `imported`, **157 inserts**, 0 errors, 0 notified |
| re-run | all **157 unchanged** |

`candidate_records` 71,953 → 72,110 (+157); the run stamp
`2026-08-27T20:56:11.578Z` returns exactly 157 rows over 126 candidates.
**Illinois total: 4,840 records.** Prod untouched — and note **migration 257 is
local only**, so prod needs it before any Illinois promotion.

Descriptions were built through the batch-03 guard that asserts `", The "`
appears in no description, the rule recorded after the comma splice shipped in
both batch-01 and batch-02.

## Plain-language rewrite (2026-08-30)

All 3 yea and nay descriptions were rewritten from committed evidence. No
vote date or official-date override changed. Judge dry and real runs passed;
the importer rewrote 157 local records with stamp
`2026-08-31T06:52:38.069Z`, and convergence reported all 157 unchanged.
The original `import-report.json` remains unchanged. Prod remains untouched.
