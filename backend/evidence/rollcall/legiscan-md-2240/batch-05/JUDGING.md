# Maryland 2026 batch-05 — judging

## Evidence and text version

Every description comes from the official DLS Fiscal and Policy Note for the
version that became law, with the chapter number from the official bill
history. `PLAN.md` records the validation done before any description was
written.

## Label directions

Direction follows the research area's own description. Twenty-two of the 43
measures carry an `against` on the nay side; the other 21 are explicitly
`null`, on the rule set in batch-02: a no vote gets a stance only when the bill
has a single subject squarely inside the area and the no is a recognizable
position in it. Where the leading objection is fiscal, a mandate cost, or local
control, the nay is null.

Two labels are worth stating plainly:

- **HB 1578, minority business contracting — civil_rights, for.** The bill
  extends the Minority Business Enterprise program five years and rewrites the
  legislature's disparity findings. Whether the state should keep race- and
  gender-conscious contracting goals is itself the civil-rights question, so a
  no vote is a position in the area.
- **SB 187, women's prerelease services — civil_rights, for, nay null.** The
  measure exists because women in Maryland's prison system did not get the
  prerelease facility men had. That gap is the civil-rights question. The nay
  is null because a no vote can be about corrections cost and timelines rather
  than about the gap.

Every judgment carries an explicit `nay` key.

## Plain-language check

Short sentences, everyday words, and every term of art explained in place:
limited equity co-op, water submeter, parity, deactivation, temporary
protective order, vested right, letters of administration, medication-assisted
treatment, vernal pool, prerelease facility, agrivoltaics.

`candidateRecordPlainLanguageLint` ran over all 124 descriptions **before**
judging or importing: **zero warnings**. Four sentences were over 45 words on
the first draft and were split first. Body and tail are joined with a period,
`", The "` is absent, and spelling is American throughout.

## Import and reconciliation

Real import stamp `2026-09-02T06:31:54.928Z`.

- Dry run: 62 files, **5,467 inserts**, 0 errors, 0 notifications, 0 ambiguous
  matches, 7 related flags.
- Real run: identical — 62 files imported, **5,467 inserts**, 0 errors.
- Convergence re-run: **5,467 unchanged**, in `import-rerun-report.json`;
  `import-report.json` remains the original insert ledger, confirmed from
  inside the file.

Records by stamp: **5,467 across 62 rolls**. All 4,156 yea-side records are
tagged. Of the 1,311 nay-side records, 706 are tagged, and the set of measures
holding those tags matches the 22 with a non-null `nay` exactly. Reconciliation
was by run stamp, never by table delta.

**Related flags read before importing:** seven, pointing at two pre-existing
manual records, both about different bills that share a date — David Moon's
co-sponsorship of HB 634 and Cheryl Kagan's committee vote on SB 949. Kagan's
record concerns a committee vote; this batch judges SB 949's floor votes, which
are a distinct claim, and the importer correctly did not flag them against each
other. Nothing retired.

Session 2240 now holds **9,246 live records**. Production is untouched.

## Maryland is now complete

With this batch every divided-and-enacted subject in both Maryland sessions has
a disposition: judged, or deferred with a recorded reason. Totals across
batches 01 through 05: **16,462 live records across 163 candidates and 14,572
area tags**, 7,216 from the 2025 session and 9,246 from 2026.

**Production still holds zero Maryland roll-call records.** Promotion is the
only remaining step and is a separate, user-run operation.
