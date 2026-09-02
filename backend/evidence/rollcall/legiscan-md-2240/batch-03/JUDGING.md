# Maryland 2026 batch-03 — judging

## Evidence and text version

Every description comes from the official DLS Fiscal and Policy Note for the
version that became law, plus the official bill history for the chapter number.
Each note's version stamp was checked against the chaptered PDF file name for
all eleven measures and they agree. `PLAN.md` records which roll each chamber's
final action was, including SB 141's refuse-to-recede path.

Before judging, every selected roll was checked against the database for a later
same-chamber vote on the same measure: there are none. The dates in the
committed LegiScan evidence match the official bill-history dates on all
eighteen rolls, and each description cites its own chamber's tally. No
`official_vote_date` override is needed.

Two facts the descriptions state carefully rather than rounding off:

- **HB 711 became law without the Governor's signature** under Article II,
  Section 17(c), so its tally sentence says exactly that instead of "signed
  into law".
- **HB 894's ban on development impact fees is contingent** on two other bills
  failing, so the description does not present it as something the law did.

## Label directions

- **HB 191 — corporate_accountability, for, nay against.** The bill only
  regulates what merchants must accept; a no vote rejects that duty.
- **HB 284 — gun_control, for, nay against.** A State Police bill on reporting
  stolen guns. Single subject, squarely in the area.
- **HB 315 — civil_rights, for, nay against.** Whether refusing a voucher holder
  over credit counts as housing discrimination is itself the civil-rights
  question.
- **HB 624 — corporate_accountability, for, nay against.** The bill only imposes
  staffing duties on hospitals.
- **HB 711 — data_privacy, for, nay null.** The mechanism is limits on selling
  and disclosing personal data, which is the area. The `nay` is **null** because
  the bill's target is immigration enforcement, so a no vote is more naturally a
  position on law-enforcement cooperation than on data privacy.
- **HB 894 — housing_affordability, for, nay null.** More homes near transit is
  the area's goal, but the bill overrides local parking and zoning rules, so a
  no vote can be a local-control position.
- **HB 895 — corporate_accountability, for, nay against.** A single-subject ban
  on data-driven pricing by large food sellers.
- **HB 1017 — general, no stance.** Originally labeled `immigration` for/against.
  Corrected on review: the enacted definition covers detention for civil *or
  criminal* violations and only *includes* immigration facilities, and the
  immigration-specific bans predate this bill (Chapter 19 of 2021). A vote on
  private detention generally is not a position on welcoming immigration, and
  no other area fits, so it is recorded with no stance.
- **SB 141 — election_integrity, for, nay null.** Deepfake rules protect trust in
  elections, but the bill creates a speech crime, and a First Amendment
  objection is a different axis from election integrity.
- **SB 475 — civil_rights, for, nay against.** Limiting prejudicial use of a
  defendant's art is a fair-treatment mechanism; a no vote rejects it.
- **SB 937 — civil_rights, for, nay against.** Same shape as HB 315.

Every judgment carries an explicit `nay` key.

## Plain-language check

Descriptions are short, in everyday words, and each says plainly what a yes vote
and a no vote meant before giving the outcome. Terms of art are explained in
place: essential consumer good, conditional offer, deepfake, creative
expression, dynamic pricing, mixed-use development, emergency bill.

`candidateRecordPlainLanguageLint` ran over all 70 yea and nay descriptions in
this batch (both sessions) **before** judging or importing: **zero warnings**.
Five over-long sentences were split first. Body and tail are joined with a
period, `", The "` is absent, and spelling was normalized to American usage.

## Import and reconciliation

Real import stamp `2026-09-02T00:55:38.124Z`.

- Dry run: 18 files, **1,550 inserts**, 0 errors, 0 notifications, 0 ambiguous
  matches, 1 related flag (see below).
- Real run: identical — 18 files imported, **1,550 inserts**, 0 errors.
- Convergence re-run: **1,550 unchanged**, in `import-rerun-report.json`.
  `import-report.json` remains the original insert ledger, confirmed by its
  `dryRun: false` and `insert: 1550` read from inside the file.

Records by stamp: **1,550 across 18 rolls**. Area tags: **1,455**, and the
split is exact. All 1,169 yea-side records are tagged. Of the 381 nay-side
records, 286 are tagged: the 246 belonging to the seven measures with a
non-null `nay`, plus HB 1017's 40, which carry the stance-free `general` tag
its yea side also carries. HB 711, HB 894 and SB 141 contribute no nay tags.

Reconciliation was by run stamp, never by table delta.

## Related-record flags

One flag: **Cheryl Kagan** on the HB 1017 Senate roll, pointing at her existing
manual record of an April 10 committee vote on **SB 949**, a different bill.
Batch-01 kept the same record for the same reason. Not a duplicate, nothing
retired.

Counts come from `rolls[].candidates[].relatedRecordIds`; the ledger's roll rows
carry no `related` field, which is what produced a false "zero flags" claim in
batch-02.

Session 2240 now holds **3,779 live records across 163 candidates** from batches
01 through 03. Production is untouched.

## Review response (2026-09-02)

Four findings on the first version of this batch; all four were checked against
the DLS note and, where the note was silent, the enrolled bill text. All four
were real and all four are fixed. Fix stamp `2026-09-02T01:39:50.699Z`; ledger
`import-review-fixes-report.json` (**589 rewrites across 7 rolls, 0 inserts,
0 errors**), verified by its `dryRun: false` and action counts before it was
renamed. `import-report.json` remains the original insert ledger. Convergence
afterwards: **1,550 unchanged**.

- **SB 937 (P1, accepted).** The descriptions said a conviction could only be
  weighed after a conditional offer. The note lists convictions a landlord may
  ask about and reject on *before* any offer — murder, sex offenses, child
  pornography, human trafficking, meth production in federally assisted
  housing — plus sex-offender registration, and a five-year felony window after
  the offer. Rewritten to carry all of that. Same defect the batch-01 review
  caught: plain words that lost the statute's limits.
- **HB 1017 (P1, accepted as a relabel).** See the label section above.
  Relabeled `general` with no stance rather than excluded: the vote is divided
  and enacted and worth recording, the label just cannot claim a direction. The
  description now says the ban covers private detention of any kind, civil or
  criminal, and that the immigration-specific bans already existed.
- **HB 895 (P2, accepted).** The enrolled text bans using personal data to set a
  *higher* price in both of its prongs; the DLS synopsis's definition of dynamic
  pricing dropped the word and the description followed it. Now says "higher".
- **HB 284 (P2, accepted).** The body already listed attempted burglaries and
  compromised security features; only the one-line "what a yes meant" narrowed
  it to stolen guns. The line now matches the body.

Tag proof: a per-measure tag count and hash was captured before and after the
fix run. Every measure outside the four is byte-identical. HB 284, HB 895 and
SB 937 keep the same counts and hashes (descriptions changed, labels did not).
HB 1017's hash changed as intended: 157 immigration tags replaced by 157
`general` tags with no stance.
