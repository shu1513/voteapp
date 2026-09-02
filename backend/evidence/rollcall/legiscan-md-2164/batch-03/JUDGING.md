# Maryland 2025 batch-03 — judging

## Evidence and text version

Every description comes from the official DLS Fiscal and Policy Note for the
version that became law, plus the official bill history for the chapter number.
Each note's version stamp was checked against the chaptered PDF file name for
all ten measures and they agree. `PLAN.md` records which roll each chamber's
final action was.

Before judging, every selected roll was checked against the database for a
later same-chamber vote on the same measure: there are none. The dates in the
committed LegiScan evidence match the official bill-history dates on all
seventeen rolls, and each description cites its own chamber's tally. No
`official_vote_date` override is needed.

## Label directions

Direction follows the research area's own description, not the sponsor.

- **HB 249 — civil_rights, for, nay null.** Ending penalties for calling 911
  protects tenants and domestic-violence victims. The `nay` is **null** because
  the bill overrides local ordinances, and a no vote can be a local-control
  position rather than a civil-rights one.
- **HB 431 — corporate_accountability, for, nay against.** The bill does one
  thing: it voids contract clauses that cut a consumer's time to sue. A no vote
  rejects that protection.
- **HB 881 — social_programs_and_welfare, for, nay null.** Passing child support
  through to families is the area's mechanism, but the change costs the state
  money and phases in through fiscal 2031, so a no vote can be fiscal.
- **HB 933 — corporate_accountability, for, nay against.** A single-subject
  reporting duty on nursing homes; a no vote rejects the scrutiny.
- **HB 974 — healthcare_affordability, for, nay null.** Same shape as batch-02's
  HB 1315: a no vote can be about the insurance mandate rather than about cost
  to patients.
- **HB 1045 — womens_reproductive_rights, for, nay against.** The contested part
  is extending the medical privacy shield to gender-affirming care, which is
  squarely a bodily-autonomy question.
- **HB 1082 — healthcare_affordability, for, nay null.** The subsidies are the
  point, but they spend state money, so a no vote can be fiscal.
- **HB 1380 — environment_and_public_health, for, nay null.** Perinatal
  standards protect health, but a no vote can be about the licensing burden on
  hospitals and birthing centers.
- **SB 154 — housing_affordability, for, nay null.** Funding tenants' lawyers
  serves the area, but the bill moves $14.0 million a year out of the unclaimed
  property fund, so a no vote can be fiscal.
- **SB 425 — environment_and_public_health, for, nay against.** The fee is the
  bill's environmental mechanism — polluters pay, and the surplus must go to
  reuse and cleanup — so a no vote is a position on that mechanism.

Every judgment carries an explicit `nay` key.

## Plain-language check

Descriptions are short, in everyday words, and each states plainly what a yes
vote and a no vote meant before giving the outcome. Terms of art are explained
in place: payment in lieu of taxes, nuisance designation, pass-through, cost
report, preventive services, perinatal care, coal combustion by-products.

`candidateRecordPlainLanguageLint` ran over all 70 yea and nay descriptions in
this batch (both sessions) **before** judging or importing: **zero warnings**.
Five sentences were over the 45-word limit on the first draft and were split
before the run. Body and tail are joined with a period, and `", The "` is
absent. Spelling was normalized to American usage to match the rest of the
corpus.

## Import and reconciliation

Real import stamp `2026-09-02T00:55:30.606Z`.

- Dry run: 17 files, **1,448 inserts**, 0 errors, 0 notifications, 0 ambiguous
  matches, 7 related flags (see below).
- Real run: identical — 17 files imported, **1,448 inserts**, 0 errors.
- Convergence re-run: **1,448 unchanged**, in `import-rerun-report.json`.
  `import-report.json` remains the original insert ledger, confirmed by its
  `dryRun: false` and `insert: 1448` read from inside the file.

Records by stamp: **1,448 across 17 rolls**. Area tags: **1,226**. The split is
exact — all 1,077 yea-side records are tagged, and of the 371 nay-side records
the 149 belonging to HB 431, HB 933, HB 1045 and SB 425 are tagged, which are
precisely the four measures with a non-null `nay`.

Reconciliation was by run stamp, never by table delta.

## Related-record flags

The ledger flags seven member rows against three pre-existing manual records.
All three were read in full **before the import**, and none is a duplicate of
anything judged here — each is about a different bill that happens to share a
date with one of these rolls:

- **Mark S. Chang** (flagged on HB 933 and HB 1380, both March 11) — his record
  is a yea vote on **HB 61**, parking-lot solar canopies.
- **Chris Tomlinson** (flagged on HB 1045 and HB 1082, both March 12) — his
  record is sponsorship of and a vote on **HB 662**, open-ended supply
  contracts.
- **Charles Sydnor** (flagged on HB 881, HB 933 and HB 1082, all April 7) — his
  record is sponsorship of **SB 342**, which the House never passed. It is
  flagged three times only because its event date is sine die.

The flag is a same-candidate, same-date heuristic, not a duplicate detector.
Counts here come from `rolls[].candidates[].relatedRecordIds`; the roll rows in
the ledger carry no `related` field, which is what produced a false "zero
flags" claim in batch-02.

Session 2164 now holds **4,362 live records across 160 candidates** from
batches 01 through 03. Production is untouched.
