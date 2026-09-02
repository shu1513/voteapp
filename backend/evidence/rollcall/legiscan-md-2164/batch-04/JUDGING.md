# Maryland 2025 batch-04 — judging

## Evidence and text version

Every description comes from the official DLS Fiscal and Policy Note for the
version that became law, with the chapter number from the official bill
history. `PLAN.md` records the validation done before any description was
written: twin collapse, superseded-roll check, same-day peers, and the
chaptered-PDF version check.

## Label directions

Direction follows the research area's own description, not the sponsor. Seven
of the 24 measures carry an `against` on the nay side; the other 17 are
explicitly `null`, using the rule established in batch-02: a no vote gets a
stance only when the bill has a single subject squarely inside the area and the
no is a recognizable position in it. Where the leading objection is fiscal, a
mandate cost, or local control, the nay is null.

**HB 102 is the one measure here whose yea side is `against`.** It delayed the
start of Maryland's paid family and medical leave program by 18 months for
contributions and at least six months for benefits. Postponing a safety-net
program is a position against the `social_programs_and_welfare` area, whatever
the implementation argument for it. Its `nay` is null because a no vote could
mean either "do not delay this program" or "do not have this program at all",
and those point opposite ways.

Every judgment carries an explicit `nay` key.

## Plain-language check

Short sentences, everyday words, and every term of art explained in place:
solar collector system, worker cooperative, content standards, permanent
absentee list, calcium score test, technical service bulletin, earned wage
access, prescription drug repository, color temperature.

`candidateRecordPlainLanguageLint` ran over all 66 descriptions **before**
judging or importing: **zero warnings**. Body and tail are joined with a
period, `", The "` is absent, spelling was normalized to American usage, and
curly apostrophes were straightened.

## Import and reconciliation

Real import stamp `2026-09-02T06:25:04.024Z`.

- Dry run: 33 files, **2,854 inserts**, 0 errors, 0 notifications, 0 ambiguous
  matches, 2 related flags.
- Real run: identical — 33 files imported, **2,854 inserts**, 0 errors.
- Convergence re-run: **2,854 unchanged**, in `import-rerun-report.json`;
  `import-report.json` remains the original insert ledger, confirmed from
  inside the file.

Records by stamp: **2,854 across 33 rolls**. All 2,135 yea-side records are
tagged. Of the 719 nay-side records, 268 are tagged — exactly the seven
measures with a non-null `nay` (HB 4, HB 161, HB 277, HB 1046, HB 1294, SB 250,
SB 842). Reconciliation was by run stamp, never by table delta.

**Related flags read before importing:** two, both pointing at pre-existing
manual records about different bills that share a date — Chris Tomlinson on
HB 662 and Charles Sydnor on SB 342. Neither is a duplicate; nothing retired.
Counts come from `rolls[].candidates[].relatedRecordIds`.

Session 2164 now holds **7,216 live records**. Production is untouched.
