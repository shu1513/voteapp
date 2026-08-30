# Missouri special session batch-01 — judging and import

**Result on local `voteapp` 2026-08-30: 4 files all `imported`, 0 errors, 229 inserts, 0 notified,
115 candidates. PRODUCTION UNTOUCHED.** Missouri now holds **1,174 records** across three batches.

Reconciled three ways: the dry run planned 229 and the real run inserted 229; rows carrying the run
stamp `2026-08-30T06:21:17.833Z` number 229 across 115 candidates; a convergence dry run reports all
229 `unchanged`, and the dry run's own stamp matches zero rows.

## Sources

Both measures were judged from the **enrolled text** — `3344H.01T` (HB 1) and `3353H.03T` (HJR 3) —
because the House publishes no Truly Agreed summary for special-session bills (both `T` summary URLs
answer 200 with a 793-byte error page). The Perfected summaries were used only as an index, and each
claim in the descriptions was read back out of the enrolled text. For HJR 3 the perfected text the
House voted (`3353H.03P`) and the enrolled text differ only in the header stamp, verified by diff.

Roll identity came from the roll-call PDFs under `bills254/rollcalls/`, matched to LegiScan on
`(Total Yes, Total No, Total Present)` and cross-checked against the bill history's `Third Read and
Passed` lines: HJR 3 `98-58`, HB 1 `90-65`.

## The guard fired once, and was right to

The judge refused roll 1601420 as possibly not the chamber's final kept floor vote on HJR 3, naming
roll 1601419. Both rolls carry the identical desc `House: HJRs FOR THIRD READING HCS HJR 3` on the
same day — 1601419 (104-51) is PDF `006.002`, whose header carries the extra line **PREVIOUS
QUESTION**, and 1601420 (98-58) is PDF `006.003`, the passage vote that matches the bill history.
Approved with `acknowledge_later_rolls: [1601419]` and a per-judgment `note` saying so. Read the note
rather than assuming the guard was waved through.

## Labels

Both measures carry `general` with `"yea": null, "nay": null` — recorded, no stance. The reasoning,
including why HJR 3's separable-looking strands still do not earn a label, is in `PLAN.md`.

## Wording checks

Body-tail joins built with a period; the builder asserts `", The "` appears in no description. The
real `candidateRecordPlainLanguageLint` ran over all 8 descriptions before importing: 0 warnings.

## Roll dates

All four roll dates match the dates printed on the official House roll-call PDFs and the Senate
actions in the bill history (2025-09-09 House, 2025-09-12 Senate). No `official_vote_date` override
was needed.
