# Nevada batch-01 — how these votes were judged and imported

`PLAN.md` recorded the selection and the reading of the enacted acts. This file records what
the judging step did with that material, and what the import produced.

## Sources

Nothing was re-read from the web. Every fact in every description comes from the reading of
the enrolled acts already recorded in `PLAN.md`, which was taken from the acts themselves with
the Legislative Counsel's Digest used only as an index. Sponsor statements were not used.

The cautions written into `PLAN.md` were carried into the descriptions rather than quietly
dropped:

- **AB 483** says plainly that it only reorders a queue — no deadline, no waived
  qualification, no fee cut, no duty to stay.
- **AB 235** separates the two mechanisms. Sealing county records still takes a sworn
  statement and a court order; only the alternate address on a driver's license is available
  on proof to the DMV alone.
- **AB 123** uses "knows, or has reason to know", never "knowingly", and states the act's own
  free-speech clause.
- **SB 260** never says the act bans outdoor work at a stated index level. It says the
  regulator picks that level.
- **SB 347** gives both halves: taking the firearm is discretionary, the receipt, the notice
  and the return duty are not.
- **AB 90** is filed under `civil_rights`, not `public_safety_and_crime_control`, because
  under the latter a yes vote would read as against — the act makes commitment harder.

## The two measures where the chambers voted different bills

Nevada records no concurrence vote, so a chamber's only roll can sit on text that never became
law. Two measures here are in that position, and each chamber gets a description of the bill
it actually voted.

**AB 121.** The Assembly description ends by saying it passed *this earlier version*, and that
the Senate later added the utility carve-out. The Senate description states the carve-out as
part of what the Senate voted.

**AB 343.** The two chambers voted materially different enforcement schemes. The Assembly
version let patients sue a hospital directly for damages, legal fees and up to $10,000 per
violation; the Assembly description says so, and says the Senate later replaced that with a
state complaint process. The Senate description carries the Bureau of Consumer Protection
route, the investigation, the freeze on collection, and the debt cancellation. The direction is
the same on both versions, which is why both rolls are kept.

## AB 123's Senate roll

Nevada lets a chamber reconsider a passage vote and retake it the same day, and LegiScan gives
the vote that **stands** the **lower** roll id — the reverse of the usual order, recorded as
`CODE-FINDINGS.md` §2. The Senate passed AB 123 13-8 as roll 1582878, reconsidered, then passed
it 14-7 as roll 1582877. The 14-7 vote is the one imported, and the superseded 13-8 roll is
named in `acknowledge_later_rolls` so the stage gate can be satisfied without hiding it.

## Checks run before importing

| check | result |
| --- | --- |
| Repository plain-language lint, 45-word sentence cap | 36 descriptions, **0 warnings** |
| `nv_check.py` — comma splices, British spellings, sentence length, reading level | **0 problems** |
| Flesch-Kincaid grade | median **7.6**, worst **8.4** |
| Longest sentence | 28 words |
| Banned areas (`general`, `impartiality`, `legal_competence`) | 0 used |
| Every stated tally against the stored vote row | **18 of 18 match** on chamber, measure, date and tally |

## Reconciliation

Predicted independently from the crosswalk and the roll evidence, before touching the
database: **387 records and 274 area tags**. `PLAN.md` had estimated "about 388".

| source | records | tags |
| --- | --- | --- |
| independent prediction | 387 | 274 |
| importer dry run | 387 insert | — |
| importer real run | 387 insert, 0 errors, 0 notified | — |
| database | 387, across 41 candidates and 18 rolls | 274 |

The pre-import dry run's stamp `2026-09-05T03:29:43.696Z` matched zero rows in the database.
The real run's stamp is `2026-09-05T03:29:56.126Z`. The convergence re-run reported all 387
unchanged, and a second dry run after that also reported 387 unchanged.

### A note on the ledger file names

The importer writes its own report into the evidence directory *and* prints a summary to
standard output, and both use the same file name. Redirecting standard output into the
evidence directory therefore overwrites the detailed report with the summary. That is why
`import-report.json` here carries the summary form, with a candidate count per roll, while
`import-rerun-report.json` and `import-dry-run-rerun-report.json` carry the full form with the
per-candidate detail. The numbers agree across all of them. **Do not redirect this script's
standard output into the evidence directory.**

## Related records

Nine candidate rows carried a related-record link. All nine point at hand-researched records
about **different bills** that happen to share a vote date — AB 480 and AB 302 on 2025-04-22,
and SB 121 on 2025-05-22. None describes any measure in this batch, so none was retired.

## What is left in Nevada

The survey worklist holds **71 rolls still marked `candidate:unbatched`**, on measures that
became law and cleared the divided gate. Those are batch-02.
