# Arizona batch-01 — judging and import

## Sources used

Every description was written from the **enacted text** and cross-read against the staff
analysis for the version that became law. Arizona publishes two independent nonpartisan
analyses per bill and stamps the version in the title, so no guessing was needed: the House
Bill Summary headed `Signed`, and where it exists the Senate Fact Sheet headed `As Passed
House`. Neither carries a sponsor statement of intent, so the Texas advocacy hazard does not
arise.

Claims were checked against the chaptered law rather than taken from the summary. Arizona's
chaptered HTML marks deletions with `<span class=O>` and insertions with `<span class=UP>`, so
the change an act makes is extractable exactly, without rendering a page. Four measures turned
on that check:

- **HB 2291** — the summary says the act repeals the red cap requirement. The extracted
  deletions show precisely `a red cap and` plus the pharmacy board's waiver power, and nothing
  else. **The addiction warning label survives**, so the description says so. Describing this as
  removing the warning would have been wrong.
- **SB 1590** — the deleted runs are subsection D in full, both the $50,000 annual cap for a
  child under 9 and the $25,000 cap for ages 9 through 15, repeated across four insurance
  statutes, plus the three-condition list replaced by the psychiatric manual's criteria.
- **SB 1159** — a single change, `five thousand dollars` to `$12,000`.
- **HB 2114** — one added sentence, and it carries three conditions together: class 4 felony
  where the person is more than sixty months older than the victim **and** is older than 21,
  with a mandatory one year in jail if placed on probation. The summary's looser phrasing would
  have dropped the conjunction.

## Tally audit

Arizona's House Bill Summary header reprints every committee and floor tally plus the chapter
number. **All 15 selected rolls were checked against it and all 15 matched** the LegiScan
tally exactly. That is an independent official source, not a sample, which is what the North
Carolina finding asks for.

The header also confirmed the version rule per measure: a `Final Read` line means the second
chamber amended and the originating chamber's earlier roll is on a superseded draft, while its
absence means the second chamber passed the bill unamended and both rolls are on the enacted
text. Every selection agreed with that reading.

## Dates and superseding

All 15 roll dates match Arizona's own history, so no `official_vote_date` override was needed.
No roll had a later kept floor vote in the same chamber, so no judgment needed
`acknowledge_later_rolls`.

## Labels

Six areas. Every stance label states the nay side explicitly and every nay is `null`: on each
measure the realistic objection runs on a different axis from the area being scored, so a no
vote is not evidence of a position on the area's own goal. `environment_and_public_health`
carries both directions on purpose — SB 1247 raises the tobacco age, HB 2291 repeals an opioid
packaging safeguard.

Arizona records nothing without a stance. `general` is not a user-selectable research area, so
its tag is hidden from every legislative view; a measure with no honest direction is dropped
instead. That removes the escape hatch other states used, and it is why SB 1395, SB 1319 and
HB 2727 were dropped rather than imported without a direction.

## Writing checks, run before importing

- Plain-language lint (`listPlainLanguageWarnings`, the 45-word sentence cap): **30
  descriptions, 0 warnings.**
- Reading level measured separately, because the lint is a run-on check and not a readability
  standard: **Flesch-Kincaid median 8.2, best 7.1, worst 10.1**, mean sentence 17.5 words,
  longest 31. A first draft measured median 9.2 and worst 12.1 and was rewritten into shorter
  sentences before anything was imported. The worst remaining item is SB 1247, whose grade is
  driven by unavoidable proper nouns (Arizona teachers academy, National Guard) rather than by
  structure.
- The builder asserts, before it writes the file: bodies joined to the tail with a period, the
  string `", The "` absent so no comma splice survives, no British spelling from a list that
  includes `offence`, `licence`, `centre` and `programme`, no sentence over 45 words, and the
  roll's own tally present in both the yes and no sentence.

## Duplicate sweep

The importer reported **0 related flags**. Because that scan is weak on state measures — it
falls back to a bare "contains the word vote" test — a wider sweep was run over every live
Arizona candidate record not written by this pipeline, matching each batch measure's number in
both spellings. It found six records and **none is a duplicate**: all six are sponsorship
claims ("sponsored", "sole introducer"), which the Maryland Acevero precedent keeps as a
distinct claim from a vote. One of them describes a *different* HB 2114, from the 2026 session
— Arizona reuses bill numbers between sessions. Nothing was retired.

## Import

Dry run and real run agreed exactly.

| run | stamp | result |
| --- | --- | --- |
| dry run | `2026-09-05T03:21:02.679Z` | 408 planned inserts, 0 errors |
| real run | `2026-09-05T03:21:36.285Z` | **408 inserts**, 0 errors, 0 notified |
| convergence re-run | — | all 408 `unchanged` |

Reconciled three ways:

1. `candidate_records` with `origin='rollcall_import'` moved 126,097 to 126,505, a delta of 408.
2. The run-stamp predicate
   `origin_run_id LIKE 'rollcall:AZ:%:2026-09-05T03:21:36.285Z'` returns **408 records across
   54 candidates**.
3. The dry run's own stamp matches **zero** rows, which is positive proof that `--dry-run`
   wrote nothing.

54 candidates is every candidate the crosswalk maps — **Arizona's Speaker votes**, so there is
no Texas Burrows or Georgia Burns style shortfall.

Tags reconcile by side arithmetic: **286 tags on 286 yes-side records**, and the 122 no-side
records carry none, which is correct because every nay is `null`.

The original insert ledger is `import-report.json`; the convergence run wrote
`import-rerun-report.json` and left it alone.

**Production is untouched.** It holds no Arizona roll-call records.

## Review fixes (2026-09-05)

PR review caught two descriptions that overstated the enacted law. Both were checked against
the chaptered text on azleg.gov and corrected in `judgments.json`, re-applied with
`rollcall:judge`, and fanned out again with `rollcall:legiscan:import`:

- **SB 1353** (House roll 1567301): the 15-working-day permit deadline applies only to a
  municipality of 30,000 or more people, and the clock starts only after the city has approved
  the construction documents and cleared vertical construction (§9-470.01(A)). The old text
  stated an unconditional 15-day deadline.
- **SB 1529** (House roll 1595696): accessory-dwelling-unit plans start July 1, 2026 alongside
  single-family plans; only duplex and triplex plans start January 1, 2027 (§9-461.20, the
  codified section). The old text put ADUs in the 2027 group.

The re-import rewrote exactly the 60 records on those two rolls (31 + 29) and left the other
348 unchanged; `import-rerun-report.json` is now that run's ledger. Record count stays 3,847.
