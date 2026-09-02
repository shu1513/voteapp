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

## Review response (2026-09-02)

One finding on this batch, checked against the DLS note: real, fixed. Reading
the note also exposed a root cause that reached beyond the finding, so two more
descriptions were corrected in the same pass.

- **HB 634 (P2, accepted).** The description said anyone unable to file while
  incarcerated could use the program. The note defines a "justice-involved
  individual" narrowly: convicted, and still serving or released within the
  past two years after serving six months to ten years. The description now
  carries that definition, the ten-year plan limit, and that relief attaches to
  a tax bill for a year the person was incarcerated.

**Root cause.** Descriptions were authored from the note's synopsis paragraph
extracted at a fixed character cut, and 51 of the 67 finish-campaign synopses
were cut mid-paragraph. The text past the cut was read for every judged
measure in both sessions. In this batch it changed two more:

- **HB 277.** The replacement trigger is a renovation that replaces the
  fountain, not "where the code requires one"; and the rule is prospective
  only. Both now stated.
- **HB 367.** The bill also changes the nursing and pharmacy boards' English
  proficiency rules and the conditions for licensing nurses by endorsement —
  the second half of its own title, which the description had omitted.

The other 21 tails were effective dates, conforming changes, or regulation
duties that change no claim.

Fix stamp `2026-09-02T14:49:54.630Z`; ledger `import-review-fixes-report.json`
(**424 rewrites across 5 rolls, 0 inserts, 0 errors**), confirmed from inside
the file before it was renamed. `import-report.json` remains the original
insert ledger. Convergence afterwards: **2,854 unchanged**. Per-measure tag
counts and hashes were captured before and after the fix run: all 24 measures
byte-identical, as expected for description-only changes.
