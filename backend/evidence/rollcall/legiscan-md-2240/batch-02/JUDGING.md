# Maryland 2026 batch-02 — judging

## Evidence and text version

Every description comes from the official DLS Fiscal and Policy Note for the
version that became law, plus the official bill history for the chapter
number. Each note's version stamp was checked against the chaptered PDF file
name for all ten measures and they agree. `PLAN.md` records which roll each
chamber's final action was, including the SB 334 recede and the SB 255 House
vote that is not divided.

The dates in the committed LegiScan evidence match the official bill-history
dates on all nineteen rolls, and each description cites its own chamber's
tally. No `official_vote_date` override is needed.

## Label directions

- **HB 862 — corporate_accountability, for, nay against.** The bill does one
  thing: it holds freight railroads to a two-person crew on shared passenger
  corridors, with the company alone answerable. A no vote rejects that duty.
  Because the recorded votes are the August 3 override, both descriptions say
  the vote was on overriding the Governor's May 22 veto, and the tally
  sentence says the chamber overrode rather than passed.
- **SB 1 — public_safety_and_crime_control, for, nay null.** The area names
  accountability as part of public safety, and requiring officers to be
  identifiable serves it. The `nay` is **null** on purpose: the recorded
  objection is officer safety, which lives inside the same area on the other
  side, so a no vote is not unambiguous.
- **HB 444 and SB 810 — immigration, for, nay against.** The area asks for a
  humane system. Ending local immigration enforcement agreements and widening
  protected locations both serve it, and a no vote rejects each directly. This
  follows batch-01's SB 791 and the 2025 session's HB 1222.
- **HB 351 — civil_rights, for, nay null.** The right to sue is the point, but
  the bill also creates real damages exposure for governments, so a no vote
  can be about liability and cost rather than about rights.
- **SB 255 — civil_rights, for, nay null.** Same shape: the claim protects
  minority voting strength, but the fiscal note flags county litigation costs
  as the live objection.
- **HB 573 — civil_rights, for, nay against.** Whether housing discrimination
  can be proven by effect rather than intent is itself the civil-rights
  question, so a no vote is a position in the area.
- **SB 334 — gun_control, for, nay against.** A single-subject ban on
  convertible pistols; a no vote rejects the ban.
- **HB 372 — womens_reproductive_rights, for, nay against.** Requiring a
  hospital to end a pregnancy when medically necessary to stabilise a patient
  is squarely the area's mechanism.
- **SB 417 — corporate_accountability, for, nay against.** The bill only
  regulates what employers may compel workers to sit through.

Every judgment carries an explicit `nay` key.

## Plain-language check

Descriptions are short, in everyday words, and each says plainly what a yes
vote and a no vote meant and how the vote ended. Terms of art are explained in
place: 287(g) agreement, covered officer, polarized voting, discriminatory
effect, machine gun convertible pistol, recede.

`candidateRecordPlainLanguageLint` ran over all 38 yea and nay descriptions in
this batch **before** judging or import: **zero warnings**. Body and tail are
joined with a period, and `", The "` is absent.

As in the 2164 batch, the descriptions run five to seven short sentences
rather than two to four. Trimming further would drop the statute's own limits,
which is the defect the batch-01 review caught.

## Import and reconciliation

Real import stamp `2026-08-31T06:11:59.539Z`.

- Dry run: 19 files, **1,456 inserts**, 0 errors, 0 notifications, 0 related
  flags.
- Real run: identical — 19 files imported, **1,456 inserts**, 0 errors.
- Convergence re-run: **1,456 unchanged**, in `import-rerun-report.json`.
  `import-report.json` remains the original insert ledger, confirmed by its
  `dryRun: false` and `insert: 1456` inside the file.

Records by stamp: **1,456 records across 19 rolls**. Area tags for the stamp:
**1,363**, and the split is exact — all 1,079 yea-side records tagged, and of
the 377 nay-side records the 284 belonging to the seven measures with a
non-null `nay` are tagged while SB 1, HB 351 and SB 255 contribute none.

Reconciliation was by run stamp, not by table delta.

The crosswalk already carried the two HD-031 members the roster campaign added
(people_ids 20530 and 26325), so this batch needed no link pass. The standing
rule still holds: any Maryland roster addition makes both session crosswalks
extendable.

Session 2240 now holds **2,229 live records across 163 candidates** from
batch-01 and batch-02 together. Production is untouched.
