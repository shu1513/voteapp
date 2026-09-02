# Montana batch-02 — how each measure was judged

## Source

Same as batch-01: Montana publishes no neutral prose summary of a bill, so
**the enrolled text is the source**, read top to bottom for all seven measures
and for the two that were dropped. Fetched from the state's own document
service, `api.legmt.gov/docs/v1/documents/getContent`.

## ⚠ The struck-text hazard bit hard on HB 291

Montana's enrolled prints show statutory amendments in context: deleted text is
struck through and new text underlined. `pdftotext` renders both as ordinary
text, so **an extracted read shows deleted law as if it were still law**.

On HB 291 the extracted text appeared to keep the pathway that let the state
adopt an air rule stricter than federal, subject to a public hearing and a
written finding. The renumbering `(3)(2)` was the tell: if the old subsection
(2) had survived, the old (3) would not have been renumbered. **Rendering pages
2, 3 and 4 of the enrolled PDF and looking at them** showed three whole blocks
struck that the text extract had shown as live:

- 75-2-203(4), which had let the state set stricter standards in some localities
  once federal minimums existed;
- the whole of 75-2-207(2) — the hearing-plus-written-finding pathway and its
  petition process — leaving emergency rulemaking as the only exception;
- in 75-2-301, the words `more stringent than, or more extensive than` for local
  air programs, plus the parallel local pathway in subsection (4).

The measure is materially stronger than the extract suggested, and the
description reflects the rendered text. This is the Georgia SB 40 and Maine
LD 93 rule recurring: **render the page whenever a stance leans on what was
removed.** SB 262 showed the same signature — `(iii)(ii)` renumbering marking
two struck clauses — and was checked the same way.

## Version check, per roll

Every roll was checked against the official action trail from
`api.legmt.gov/bills/v1/bills/findBySessionIdAndDraftNumber`.

- **HB 291, HB 953, SB 170, SB 262** went from the second chamber's third
  reading straight to enrolling, with no return for amendments. Both chambers
  voted the text that became law.
- **HB 703 and HB 740** were amended by the Senate, returned to the House, and
  the House then passed them as amended. The Senate's own vote was on the text
  it had amended, and the House's later vote was on that same text, which is
  what became law. Both selected rolls are therefore on the enacted text.
- **SB 319** went to a conference committee; both selected rolls adopt the
  conference report, which is by definition a vote on the final text.

## Date audit

All fourteen roll dates match the third-reading dates in Montana's own action
trail exactly. No skew, so no `official_vote_date` override anywhere.

## Superseded-stage check

Every kept floor roll for all seven measures was listed, not only the divided
ones, and the chamber's last roll was selected in each case. That matters most
on **HB 740**, where the House's first vote was 98-1 and the decisive vote on
the Senate's version was 63-35 — the Pennsylvania HB 103 shape, caught by
listing rather than by the gate. No `acknowledge_later_rolls` entry is needed.

## Labels and direction

One area per measure, direction from the area's own description, every label
`nay: null` — the same reasoning as batch-01, and the campaign's majority
practice.

- **environment_and_public_health** is "protect air, water, climate, and
  community health through standards, enforcement, and prevention." HB 291 caps
  how strict air standards may be, HB 703 removes greenhouse gases from required
  review, and SB 262 removes environmental review from water and sewer plan
  approval. All three score *against*.
- **healthcare_affordability** is "reduce out-of-pocket costs and improve access
  to affordable, quality care." SB 170 makes a rural and tribal health worker
  program permanent, SB 319 licenses doulas and opens Medicaid coverage to them,
  and HB 953 adds direct primary care to what Medicaid may cover. All three
  score *for*.
- **HB 740** takes **corporate_accountability**, not healthcare_affordability,
  and the choice is deliberate. Its core is regulating pharmacy benefit managers
  — disclosure duties, audit limits, a ban on gagging pharmacists about cheaper
  options, and a copay cap. Read as health care *affordability* the act runs two
  ways, because its reimbursement floor for independent pharmacies raises what
  plans pay. Read as corporate accountability there is no counter-strand: every
  provision binds the intermediary. This follows the Georgia SB 69 and Texas
  SB 1036 line.

## Accuracy notes carried into the wording

- **HB 953 and SB 319** add their services to Medicaid's **optional** list —
  the statute reads "may, as provided by department rule." The descriptions say
  the coverage is optional and that the department may add it, never that
  Medicaid now covers it. This is the Maryland HB 1424 "may transfer" lesson.
- **SB 319** carries an effective date of January 1 2026 and a termination date
  of December 31 2030; both are stated.
- **SB 170** directs the department to seek federal approval; it does not itself
  create Medicaid payment.
- **HB 291's** two surviving exceptions are stated rather than glossed away.

## Writing

Written plain from the first draft, then measured — the lint counts sentence
length only and is not a readability check.

- `candidateRecordPlainLanguageLint`: **0 warnings over 28 descriptions**,
  longest sentence 23 words against the 45-word cap.
- Flesch-Kincaid across the seven measures: **median grade 7.9, worst 9.5**
  (SB 262, driven by unavoidable terms like "environmental" and "subdivision"),
  best 6.7. A first draft measured median 10.2 and worst 12.5; splitting long
  sentences brought it down without changing a single claim.
- Body and closing tally joined **with a period**; the builder asserts `", The "`
  appears in no description.
- Terms explained in place: pharmacy benefit managers as "the companies that run
  drug benefits for health plans"; a doula as "a trained helper, not a medical
  provider"; direct primary care as paying "a set fee, instead of paying visit
  by visit"; community health aides as "trained local workers who provide basic
  health services."

## Import

Dry run planned 598 inserts across 14 files, 0 errors. The real run inserted
exactly **598 across 87 candidates**, and a second dry run reported all 598
unchanged. The dry run's own stamp matches zero rows.

Reconciled three ways:

- by run stamp — `origin_run_id LIKE 'rollcall:MT:%:2026-09-02T00:54:14.878Z'` = 598 rows, 87 candidates
- batch-01 untouched — everything not on this stamp is still 764 rows
- against the plan — the dry run's 598 planned inserts

Montana now holds **1,362 records across 87 candidates, 801 tags, 32 approved
rolls**. Production has zero Montana records; nothing here touched it.

Ledgers: `import-dry-run-report.json` (the plan), `import-report.json` (the
insert ledger), `import-dry-run-rerun-report.json` (the convergence run).

## Review response, 2026-09-02

One finding on batch-02, verified against the enrolled text and real.

**SB 319 — the descriptions gave the wrong licensure rule and the wrong date.**
They said only the *title* "doula" requires a license and implied that begins
January 1 2026. Section 2(1) of the act reads: "Beginning January 1, 2027, a
person may not **practice** as a doula unless licensed." That is a practice
ban, not a title rule, and it starts a year after the act's effective date. The
title-only nuance is real but belongs to section 3's exemption: peer mentors,
advocates, coaches and tribal healers may keep doing similar work "as long as
the person does not represent by title" that they practice doula care. The
first draft collapsed that exemption onto the whole act.

Root cause: my read of section 2 started mid-sentence — the page break fell
inside subsection (1) and the extract I quoted began at "doula unless
licensed". The verb ("practice") and the date ("January 1, 2027") were in the
half I did not see. Same failure family as Pennsylvania's "owning" for
"possession": the scope lives in the verb.

Repair: both SB 319 descriptions rewritten to state the 2027 practice ban, the
title-only exemption, the 2026 effective date and the 2030 termination
separately; re-judged (2 updated, 12 unchanged), re-imported as an in-place
rewrite of the measure's records, converged. Ledger for the rewrite is
`import-rewrite-report.json`, verified by its own `actions` field.

Reading grade for SB 319 moved from 8.0 to 9.2 with the fix: stating the
practice ban, its 2027 start, the title-only exemption and both act dates
separately costs a sentence and adds a date. That is the right trade — the
first draft was easier to read and wrong.
