# Roll-call description rewrite (2026-09-13)

An audit found every approved state roll call had drifted from the plan's
"vote + one-line effect + tally, ≤ 2 sentences" rule into bill digests:
4,900 rolls averaging 6 sentences / 523 characters, fanned out to ~251k
candidate records (hand-written records average 182 characters). Nothing
checked length at approval time.

Fix, in order:

1. Hard gate (`rollCallDescriptionLength.ts`): `rollcall:judge` and
   `rollcall:rewrite` refuse a description over 3 sentences, 320
   characters, or 30 words in one sentence.
2. Per jurisdiction: `rollcall:export-rewrites` → rewrite the effect
   clause from the existing text only (no new research, no AI) →
   `rollcall:rewrite --dry-run` → apply. Each `<JUR>/` folder holds the
   applied `rewrites.json` (with the old sentence/char counts under
   `_current`) and the `apply-report.json` ledger.
3. Records are rewritten in place (same row id, tags, dates, URLs,
   origin_run_id); the identity key is recomputed with a
   `plain_language_rewrite` transition and a `plain_language_rewrites`
   audit row (`model = rollcall-rewrite`). Records whose text no longer
   matched the roll's old sentence are listed as `leftAlone`, never
   touched.

Wording rule for the effect clause: say what changes for people — who
pays, who is covered, what is now allowed or banned — not the bill's
mechanics. Keep the original opener and closing (tally, enacted/vetoed/
pending status) exactly.

Production holds only WV + ND roll-call records; rewrite local before any
promotion.

| Jurisdiction | Rolls | Records rewritten | Left alone |
|---|---|---|---|
| DE | 63 | 933 | 0 |
| PA | 179 | 26098 | 0 |
| US | 140 (of 398; the rest already fit) | 13352 | 0 |
| MD | 198 | 16695 | 0 |
| IL | 193 | 13217 | 95 |
| WA | 220 | 11002 | 0 |
| CO | 474 | 13473 | 0 |
| CA | 343 | 10949 | 0 (478 stale-revision records rewritten via `--stale-too`) |
| MT | 267 | 10719 | 0 |
| OR | 171 | 4949 | 0 |
| AZ | 149 | 3847 | 0 |
| AL | 174 | 8837 | 0 |
| MI | 138 | 7849 | 0 |
| NV | 135 | 2672 | 0 |
| WI | 125 | 6748 | 0 |
| KY | 103 | 5165 | 0 |
| SD | 125 | 4250 | 0 |
| NM | 114 | 5147 | 0 |
| ME | 110 | 6949 | 0 |
| IA | 94 | 4737 | 0 |
| UT | 93 | 2612 | 0 |
| ID | 92 | 3891 | 0 |
| US | 1 | 82 | 0 |
| TN | 81 | 5076 | 0 |
| OK | 63 | 1588 | 0 |
