# Montana batch-01 — how each measure was judged

## Source

Montana publishes no neutral prose summary of a bill. There is no equivalent of
Ohio's LSC analysis, Georgia's HBRO report, Connecticut's OLR analysis or
Maryland's fiscal and policy note. Montana's fiscal notes exist but are scanned
images with no extractable text, and they are written by the executive budget
office rather than by neutral legislative staff.

So **the enrolled text is the source**. All nine were read top to bottom from
the enrolled print, fetched from Montana's own document service
(`api.legmt.gov/docs/v1/documents/getContent`). Nothing here is written from a
title, a caption, or a summary.

## Version check, per roll

Two checks were run on every one of the eighteen rolls.

First, the **official action trail** from
`api.legmt.gov/bills/v1/bills/findBySessionIdAndDraftNumber`, which records every
reading, every motion to amend, and whether a bill was returned to the other
chamber with amendments.

Second, a **text comparison** of the version in force on the vote date against
the enrolled act.

Results:

- **HB 509, HB 687, HB 809, HB 931** have only an introduced version and an
  enrolled version, and neither chamber amended them. Both chambers voted the
  text that became law.
- **HB 278** was amended by the House at second reading. The Senate passed it
  without amending. The amended print and the enrolled act carry the same
  operative sentence, so both chambers voted the text that became law.
- **HB 121**: the Senate moved to amend on February 10 and the motion did not
  carry — the bill went straight to enrolling after third reading, with no
  return to the House. The introduced and enrolled texts are word for word the
  same apart from line wrapping.
- **HB 337** was amended by the House at second reading on April 1 and passed
  the next day. The Senate's two amendment motions on April 18 did not send the
  bill back to the House. Every dollar figure and rate in the enrolled act also
  appears in the amended prints; the prints simply carry the struck figures as
  well.
- **HB 388**: the Senate returned it with amendments, but the enrolled text is
  word for word identical to both the introduced print and the last amended
  print, so the claim that both chambers voted the text that became law holds
  regardless of what the amendment did.
- **HB 818** went to a conference committee. The two selected rolls are the
  April 30 votes adopting the conference committee report, which is by
  definition a vote on the final text.

**A Montana trap worth carrying forward:** an amended bill print shows the
change in place, with the struck words left visible and the inserted words in
capitals — `shall MAY make a reasonable attempt, UPON REASONABLE SUSPICION AND
when practicable`. A plain text comparison against the enrolled act therefore
over-reports change every time. Compare the sets of figures and operative words
instead, and read the enrolled print for the text that is actually law.

## Date audit

All eighteen roll dates match the third-reading date in Montana's own action
trail exactly. No skew, so no `official_vote_date` override is set on any roll.

## Superseded-stage check

For each of the nine measures, every kept floor roll in the database was listed,
not only the divided ones. Eight measures have exactly one third-reading roll
per chamber. House Bill 818 has two per chamber, and the later pair — the
conference-report votes — are the ones selected. No roll in this batch is
followed by a later kept floor vote on the same measure in the same chamber, so
no `acknowledge_later_rolls` entry is needed anywhere.

## Labels and direction

One area per measure. Direction follows the area's own description, never the
party pattern — which matters in Montana, where a working coalition of Democrats
and moderate Republicans meant several of these votes did not split on party
lines.

- **civil_rights** is "protect equal rights, anti-discrimination enforcement,
  and fair treatment under law", so House Bill 121, which limits which shared
  restroom, changing room or sleeping area a person may use, scores *against*.
- **immigration** is "welcome immigration through a lawful, orderly, and humane
  system", so House Bill 278, an enforcement measure, scores *against*. This is
  the same rule that scored Texas Senate Bill 8 against and Illinois Senate
  Bill 2339 for.
- **womens_reproductive_rights** is "protect legal access to reproductive
  healthcare", so House Bill 388, which bars government from requiring a
  pregnancy center to offer or refer for abortion or birth control, scores
  *against*.
- **gun_control** covers regulating firearm access to reduce gun violence, so
  House Bill 809, which stops local governments enforcing red flag orders,
  scores *against*.
- **social_programs_and_welfare** is "support vulnerable populations through
  effective safety-net programs", so House Bill 687, which extends a Medicaid
  work requirement to people aged 56 through 62, scores *against*.
- **personal_income_tax_reduction**, **public_education_quality**,
  **election_integrity** and **housing_affordability** each score *for* on
  House Bills 337, 509, 818 and 931 respectively, and in each case the whole act
  points that way.

**Every label states `nay: null`.** A no vote gets no tag on the area. This
follows the majority practice of the campaign (Pennsylvania, Maine, Missouri):
the realistic objection to most of these bills runs on a different axis from the
area being scored — the size of the private lawsuit remedy in House Bill 121 and
House Bill 388, local control in House Bill 809, the revenue loss in House Bill
337 — so scoring the no side would attribute a position the vote does not
evidence.

## Writing

Descriptions were written in plain English from the first draft, not rewritten
afterwards.

- `candidateRecordPlainLanguageLint` was run over the judgments **before** the
  import: **0 warnings over 36 descriptions**, longest sentence 31 words against
  the 45-word cap.
- Reading grade was measured separately, because the lint only counts words per
  sentence and is not a readability check. Flesch-Kincaid across the nine
  measures: **median grade 7.9, worst 8.8** (House Bill 278), best 5.0 (House
  Bill 337).
- The body and the closing tally sentence are joined **with a period**, and the
  builder asserts that the string `", The "` appears in no description.
- Terms of art are explained where they appear: a red flag order is described as
  a court order that takes a person's guns away; state trust land is described
  as public land the state rents out to raise money for schools; the earned
  income tax credit is described as a refund for lower-paid workers; attainable
  workforce housing is described as homes priced for people who work in the
  area.
- Descriptions say the measure **became law**, never that it was signed. Two of
  the nine, House Bills 509 and 931, have no governor's signature in the action
  trail; they were chaptered anyway.

**One stated requirement was not met.** The instruction for this batch asked for
2 to 4 sentences per description. These run to 5 or 6 (House Bill 818 runs to
9). A first pass held to 4 sentences and measured at a median reading grade of
10.3, with sentences of 38 to 40 words — too hard for the audience. Splitting
long sentences was the only way to reach grade 8, and it costs sentence count,
not accuracy. Grade 8 is roughly the floor for these texts without dropping the
defined terms the laws are built on, which is the failure mode that produced
Connecticut's correction rounds.

## Import

Dry run planned 764 inserts across 18 files with 0 errors; the real run
inserted exactly 764 across **87 candidates**, and a second dry run reported all
764 unchanged. The dry run's own run stamp matches zero rows, which is positive
proof `--dry-run` writes nothing.

Reconciled three ways:

- by run stamp — `origin_run_id LIKE 'rollcall:MT:%:2026-08-31T06:44:43.175Z'` = 764 rows, 87 candidates
- by jurisdiction — all `rollcall:MT:%` rows = 764, 87 candidates
- against the plan — the dry run's 764 planned inserts

87 candidates is **every candidate the crosswalk maps**. Montana's Speaker casts
recorded votes, so there is no shortfall of the kind seen in Texas (Burrows) or
Georgia (Burns).

436 area tags, all on yes-side records, which is what `nay: null` produces.

Ledgers: `import-dry-run-report.json` (the plan), `import-report.json` (the
insert ledger), `import-dry-run-rerun-report.json` (the convergence run).

Production has zero Montana records. Nothing here touched it.

## Review response, 2026-08-31

Four findings on the first import, every one verified against the enrolled text
and every one real. All four were errors of the same family: a plain-language
gloss that dropped or changed a statutory limit.

- **House Bill 931** — the descriptions said the master-lease housing "must be
  attainable workforce housing." The operative definition in section 77-1-902
  permits single-family or multifamily residential development under a master
  lease generally; the nonprofit workforce-housing lease is an example the
  statute introduces with "including," not a requirement. The bill's own title
  reads as if workforce housing were the whole point, which is where the first
  draft picked it up — the title-is-not-the-text rule applies to our own state
  as much as to Georgia's. Rewritten to state the general permission first and
  the nonprofit example as an example.
- **House Bill 818** — the foreign-owned-firm exemption's first route requires
  BOTH that the firm employ people who pay Montana income tax AND that it pay
  Montana property tax. The descriptions carried only the employment half.
- **House Bill 388** — the enrolled act says the court "may award treble
  damages" where the defendant "acted with malice." The descriptions had made
  the tripling automatic and softened the standard to "bad faith." Both the
  discretion and the standard are now stated.
- **House Bill 337** — the $47,500 and $65,000 brackets apply to Montana
  taxable income, not to the first dollars earned. The descriptions now say
  taxable income and add a one-line gloss ("what is left after deductions").

Repair: judgments.json edited (8 entries updated, 10 unchanged on re-judge), a
real import re-run rewrote **338 records in place** (the four measures' full
fan-out) at stamp `2026-08-31T18:08:28.321Z`, and a convergence dry run reports
all 764 unchanged. Rows stay 764 across 87 candidates; tags stay 436; labels
and tails untouched. Ledgers: the original insert ledger is unchanged at
`import-report.json`; the rewrite run is `import-rewrite-report.json` (verified
by its own `actions` field, per the ledger-naming rule); the convergence run is
`import-dry-run-rerun-report.json`. Lint stays at 0 warnings, longest sentence
32 words; reading grades stay at median 7.9, worst 8.9.

The batch now spans two run stamps: 426 records at
`2026-08-31T06:44:43.175Z` and 338 at `2026-08-31T18:08:28.321Z`.
