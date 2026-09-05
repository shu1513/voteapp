# Oregon batch-01 — judging

Every description was written from the **enrolled Act**. The staff measure
summary was used as an index only, and the font-weight check described below
was run wherever a claim rested on what the Act changed.

## The bold-font check, and why it was needed

Oregon prints new statutory language in **bold** and deletes language in
[brackets]. `pdftotext` throws the weight away, so a plain text extract cannot
tell what an Act adds from the surrounding statute it merely reprints. This is
the same hazard the Kentucky campaign documented. `or_bold.py` marks each line
by the share of its characters that are bold.

**It changed a description.** SB 605's enrolled text carries subsections
capping interest on medical debt, barring interest entirely for patients who
qualify for financial assistance, and barring collection from a patient's
family. Read as plain text those look like things this Act did. The font check
shows every one of them is **pre-existing law**; only the reporting bans in
subsections (11) and (12), and the widened definition of "medical debt", are
new. The description says only what the Act did.

## Where the staff summary was wrong or incomplete

The Staff Measure Summary is nonpartisan and carries no sponsor statement of
intent, but it is written at the committee stage and describes the version
that committee reported. Two of six measures show why the enrolled Act has to
be read anyway:

- **SB 243.** The summary on file is version A. Its opening paragraph promises
  magazine-capacity limits and a 72-hour dealer waiting period, and its
  sections describe restrictions on "grounds adjacent to" public buildings and
  on a metropolitan zoo. **None of that is in the enrolled Act.** The enacted
  law reaches buildings a city, county or district uses for official meetings,
  and nothing else; there is no waiting period and no magazine cap of its own.
  Judging from the summary would have put four claims into 48 records that the
  law does not support.
- **SB 430.** The summary says the measure "provides exceptions to this
  requirement" without saying what they are — an unexpanded pointer of exactly
  the kind the California campaign learned to resolve. The enrolled Act has
  three: government taxes and fees, the actual cost of shipping or of
  providing the service, and a service fee
  that varies by distance or by the buyer's own selections, which must be shown
  prominently before the buyer agrees. It also exempts financial institutions
  and mortgage lenders where listed federal disclosure laws apply, and
  broadband providers that follow the federal label rules. The summary's own
  opening sentence names only the first two exceptions.

HB 3187's summary said the Act repeals an apprenticeship carve-out. That one
checked out: the enrolled text brackets out the sentence making it lawful to
reject an apprentice who could not finish training before turning 70.

## Labels and the nay side

Each label states both sides. The test for authoring a nay stance rather than
leaving it null is the Connecticut one: author it only where the Act is
single-subject, its whole operative content is the area's own mechanism, and
the mainstream objection runs on that same axis rather than on cost, burden or
structure.

- **`nay: "against"` on SB 243 and SB 599.** Both are single-subject. The
  objection to SB 243 is to firearm regulation itself, which is the
  `gun_control` axis. The objection to SB 599 is that landlords should be able
  to check immigration status, which is the `immigration` axis.
- **`nay: null` on SB 1098, SB 605, SB 430 and HB 3187.** In each the
  realistic objection sits on a different axis from the area: parental and
  local control over school libraries, credit-report accuracy, compliance
  burden on online sellers, and employer hiring flexibility. A no vote on one
  of these is not evidence a member opposes the area's goal.

Tag counts reconcile exactly to that choice: 262 tags over 322 records, with
the 33 untagged records being the nay side of SB 1098 and SB 605, plus the
nay sides of SB 430 and HB 3187.

## Writing

The builder refuses to write a judgments file that breaks a rule, and it
caught two problems before anything reached the database: a 48-word sentence
in SB 599 and another in SB 1098. It also asserts that every body-and-tail
join uses a period rather than a comma, that each description quotes its own
roll's tally, and that no British spelling has crept in.

Reading level was **measured, not eyeballed**. A first draft passed the
45-word lint with zero warnings while scoring Flesch-Kincaid grade 10.3 to
13.0. It was rewritten before importing: **median grade 9.2, worst 9.8,
longest sentence 42 words, lint 0 warnings.**

⚠ **The descriptions run 5 to 7 sentences, not the 2-to-4 the house style
asks for, and that is a deliberate trade.** Getting SB 243 into four sentences
means dropping either the police and machine-gun exemptions, the sign-posting
condition on the local carry rule, or the Measure 114 date. Dropping exactly
that kind of limit is what has caused most of this campaign's correction
rounds, so sentence count was traded for completeness and reading level. Grade
~9 is the honest floor for statutory text of this density.

## Checks run

- Version check on all 12 rolls against the bill history and version stack:
  all on the enacted text (see PLAN.md).
- Superseded check run up front over every selected roll rather than waiting
  for the judge gate to complain. Three measures had an earlier divided roll
  in the Senate; in each the later concurrence roll was chosen, so no
  `acknowledge_later_rolls` was needed anywhere in this batch.
- `related` flags: **0**. `ambiguous`: 0. Errors: 0. Notifications: 0 (every
  vote is from 2025, well outside the 30-day window).
- Dry run matched the real run exactly, and a convergence dry run afterwards
  reported all 322 records `unchanged`.

## Ledgers

- `import-report.json` — the original insert run, 211 records.
- `import-rerun-report.json` — the run that added SB 430 and HB 3187
  (111 inserts) and applied the plain-English rewrite to the first four
  measures (211 rewrites).
- `import-rewrite-report.json` — the review-fix run: SB 243 now states
  the two exemption conditions (agency authorization; the device is needed by
  the owner's registered machine gun) instead of a blanket exemption, and
  SB 430 names the cost of providing the service alongside shipping, per
  §1(1)(b). 100 rewrites, 222 unchanged.

Row counts after the batch: **322 live records, 61 candidates, 262 tags, 12
approved rolls.** Production has zero Oregon roll-call records.
