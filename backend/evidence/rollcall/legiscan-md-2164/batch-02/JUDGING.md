# Maryland 2025 batch-02 — judging

## Evidence and text version

Every description comes from the official DLS Fiscal and Policy Note for the
version of the bill that became law, plus the official bill history for the
chapter number. The fiscal note's own version stamp was checked against the
chaptered PDF file name for all ten measures, and they agree: the `Third
Reader` notes belong to the `…t.pdf` chapters and the `Enrolled` notes to the
`…e.pdf` chapters. `PLAN.md` lists which roll each chamber's final vote was.

The dates in the committed LegiScan evidence match the official bill-history
dates on all nineteen rolls, and each description cites its own chamber's
tally. No `official_vote_date` override is needed.

## Label directions

The direction follows the research area's own description, not the bill's
sponsor.

- **HB 390 and HB 1085 — housing_affordability, for.** Both lower the cost of
  keeping rental units affordable. Their `nay` is **null**: the recorded
  objection to each is the county revenue a property tax exemption gives up,
  which is a fiscal position, not a housing one.
- **HB 324 and HB 783 — civil_rights, for, nay against.** Each is a
  single-subject antibias training mandate. A no vote rejects that training,
  and nothing else in the bill offers a second reading.
- **HB 1473 — civil_rights, for, nay null.** The access duties are the point,
  but the bill also puts real reporting and translation costs on every covered
  state agency, so a no vote can be about the mandate rather than the access.
  This mirrors batch-01's HB 983.
- **SB 432 — civil_rights, for, nay null.** Clearing old records is fair
  treatment under law, but the natural objection is about public access to
  case records and about crime, which sits in a different area.
- **SB 608 — immigration, for, nay against.** The area asks for a humane
  system; easing U visa certification for immigrant crime victims serves it,
  and a no vote rejects that easing directly.
- **HB 765 — healthcare_affordability, for, nay null.** It cancels old
  hospital bills, but the bill also reworks how the rate-setting commission
  treats uncompensated care, so a no vote need not be about affordability.
- **HB 1315 — environment_and_public_health, for, nay null.** It protects
  existing vaccine access and coverage. A no vote can be about the insurance
  coverage mandate rather than about public health.
- **HB 861 — corporate_accountability, for, nay against.** The bill does one
  thing: it makes companies disclose earnings data to their drivers and to the
  regulator. A no vote rejects that disclosure.

Every judgment carries an explicit `nay` key, as the batch-01 repair
established.

## Plain-language check

Descriptions were written short, in everyday words, and each states plainly
what a yes vote and a no vote meant, then how the vote ended. Terms of art are
explained inside the sentence: payment in lieu of taxes, expungement, implicit
bias, U visa certification.

`candidateRecordPlainLanguageLint` ran over all 38 yea and nay descriptions in
this batch **before** any judging or import: **zero warnings**, longest
sentence 45 words or fewer. Body and tail are joined with a period, and the
`", The "` comma-splice pattern is absent.

The descriptions here run five to seven short sentences rather than the two to
four an ideal plain-English rewrite aims for. That is deliberate: trimming
further would drop the statute's own limits — the exact defect the batch-01
review caught, where simplification quietly broadened scope.

## Import and reconciliation

Real import stamp `2026-08-31T06:11:47.355Z`.

- Dry run: 19 files, **1,449 inserts**, 0 errors, 0 notifications, 7 related
  flags (see below).
- Real run: identical — 19 files imported, **1,449 inserts**, 0 errors.
- Convergence re-run: **1,449 unchanged**, written to
  `import-rerun-report.json`. `import-report.json` remains the original insert
  ledger; its `dryRun: false` and `insert: 1449` were checked inside the file
  rather than trusted from the file name.

### Related-record flags (7, reviewed post-import)

An earlier version of this file wrongly said zero related flags; all three
ledgers actually carry seven non-empty `relatedRecordIds`, pointing at four
pre-existing manual records. Reviewed each against the existing record's text:

- Chang / HB 390 → existing record is about **HB 61** (solar canopies). Not a
  duplicate; date-proximity noise.
- Tomlinson / HB 783 → existing record is about **HB 662** (supply contracts).
  Not a duplicate.
- Acevero / HB 861 → existing record is about **HB 1473**. Different bill, not
  a duplicate.
- Sydnor / HB 1315, SB 608, HB 1473 (3 flags) → existing record is about
  **SB 342** (his own sponsored bill). Not duplicates.
- **Acevero / HB 1473 → true overlap.** Existing manual record
  `24192bbd-31d5-4b32-925f-eb24585a5b6d` combined his HB 1473 sponsorship with
  the same 104-35 yea vote this batch imports as
  `9305a100-9876-4beb-a593-d33f56d0f246`. Resolution (applied to the local DB
  2026-08-31): the manual record was trimmed to sponsorship-only ("Led
  sponsorship … signed as Chapter 434"), dropping its vote clause; the
  imported vote record stays, keeping Acevero consistent with the other 133
  members on this roll. No records retired, no ledgers regenerated — the
  reports stay as the truthful record of what the importer did.

Records by stamp: **1,449 records across 19 rolls**. Area tags for the stamp:
**1,213**. That split is exact — all 1,085 yea-side records are tagged, and of
the 364 nay-side records only the 128 belonging to HB 324, HB 783, SB 608 and
HB 861 are tagged, because those are the four measures with a non-null `nay`.

Reconciliation was done by run stamp, never by table delta: other sessions
write to this shared local database concurrently.

Session 2164 now holds **2,914 live records across 160 candidates** from
batch-01 and batch-02 together. Production is untouched.
