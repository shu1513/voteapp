# Missouri batch-02 — judging and import

**Result on local `voteapp` 2026-08-30: 3 files all `imported`, 0 errors, 212 inserts, 0 notified,
117 candidates. PRODUCTION UNTOUCHED.** Missouri now holds **945 records** across both batches.

Reconciled three ways: the dry run planned 212 and the real run inserted 212; the run stamp
`2026-08-30T05:28:33.264Z` returns 212 rows / 117 candidates; a convergence dry run reports all 212
`unchanged`. The dry run's own stamp matches zero rows.

Sources are the same as batch-01 — the Missouri House's official **Truly Agreed** bill summary
confirmed clause by clause against the enrolled text, with advocacy quarantined in the Committee
summaries (HB 495's `C` summary carries an explicit sponsor statement of intent; its `T` summary
does not).

## Two guards fired, and both were right to

The judge now refuses a roll that is not the chamber's last kept floor vote on a measure. Both House
rolls here tripped it, and in **both cases the later roll is not a later stage** — which is the
Missouri hazard in `../CODE-FINDINGS.md` §1 showing up in a new place:

- **HB 495**: roll 1516039 (111-42) is "House Adopts SS#2 SCS", 1516040 (113-39) is Truly Agreed To
  And Finally Passed, and 1516041 (113-36) is the emergency clause — all three on 2025-03-12 under
  one identical desc. 1516040 is the decisive vote on the enacted text.
- **SB 4**: roll 1517040 (99-44) is the **previous-question motion**; 1517041 (96-44, House roll-call
  PDF 039.021) is the passage vote.

Both were approved with `acknowledge_later_rolls` naming the sibling rolls, and each judgment carries
a `note` recording why the higher roll number is not the later stage. A reviewer should read those
notes rather than assume the guard was overridden casually.

## Label shape

`parseRollCallLabels` now requires an explicit `nay` on every label. Both measures here use
`"nay": null` — nay voters get no tag — which matches how batch-01's records already behave and is
the honest choice for multi-strand omnibus votes, where inverting a strand label onto the nay side is
precisely the flattening the repo-wide nay-flip removal was meant to stop.

**batch-01's `judgments.json` was updated in the same commit** to state `"nay": null` explicitly. It
previously omitted the key, which the current validator rejects — so the committed file could no
longer be re-judged. A re-judge dry run now reports all 10 `unchanged`, confirming the file matches
the database and nothing about the stored labels changed.

## Wording checks

Body-tail joins built with a period; the builder asserts `", The "` appears in no description. The
real `candidateRecordPlainLanguageLint` was run over all 6 descriptions before importing: 0 warnings.
