# Arkansas batch-01 — how each measure was judged

## Source and method

Every judgment was written from the enrolled Act, read top to bottom. No summary, analysis or
news report was used, and no AI provider was called. Acts were fetched through LegiScan's bulk
`getBillText` using the dataset's own `doc_id`, and each download was checked against the
`text_size` the API reported. The citation is Arkansas's own page for the act.

**Neutrality was checked first, before anything was judged.** An Arkansas act contains its
title, a Subtitle stating plainly what it does, the enacting clause, and the sections. There is
no sponsor statement of intent anywhere in the document, so the advocacy hazard that makes
Texas's bill analyses dangerous does not arise. Arkansas publishes no neutral prose analysis
comparable to Ohio's LSC, Georgia's HBRO, Maryland's DLS or Connecticut's OLR, so the act itself
is the source.

**Every act was read through the strikethrough reader.** Arkansas prints amendments in place:
deleted words are struck through, new words underlined, and a plain text extraction shows both
as ordinary text. `/Users/shu/legiscan-data/ar-work/ar_text.py` marks deletions `[[...]]` and
additions `<<...>>`. This mattered directly: Act 116's alcohol-permit section reads, in a plain
extract, as though the Division must weigh both the old rule and the new one, when the old rule
is struck. It also settled HB 1017, where the whole label question turns on which eligibility
words are new.

**The readings were checked, not trusted.** Four subagents each read three acts and reported
back with section-numbered quotations. Every claim a description rests on was then re-checked
against the act. The claim that decided a label — that HB 1017's female-only eligibility is
newly added rather than pre-existing — was verified directly, by reading the marked text of
§ 6-17-122(a)(1) and confirming that the female-only and leave-abuse conditions are underlined
additions and that the prior definition of maternity leave carried no sex restriction. The
one-year service condition is not new: the prior definition of "education personnel" already
required full-time employment for more than one year, and that clause is unmarked in the act.
The first draft called all three limits new; PR review caught it and the description was
corrected. Kentucky's campaign had a subagent write a section it had not read, so a report is
evidence, not testimony.

## Writing

Descriptions were written in plain English from the first draft, not rewritten afterwards.
A first draft measured Flesch-Kincaid grade 11.1 median and 12.5 at worst; it was rewritten
into shorter sentences before anything was judged. The committed text measures:

- **Flesch-Kincaid grade: median 7.1, worst 8.9.**
- Longest sentence 29 words. The repository's `candidateRecordPlainLanguageLint` reports
  **0 warnings** over all 30 descriptions.
- Descriptions run five to nine short sentences rather than the two-to-four the standard
  suggests. That is a deliberate trade: cutting further would drop the statutory limits these
  acts turn on, and dropping exactly those limits has caused most of this campaign's correction
  rounds. Reading level was treated as the binding constraint.

Mechanical checks run before judging: the body is joined to the closing tally sentence with a
**period**, and the builder asserts the string `", The "` appears in no description; a British
spelling scan over a 22-word list ran clean, after it caught "offences" in a first draft; each
description was asserted to cite its own roll's tally.

The yes and no descriptions are generated from one body behind different opening clauses, so
the pair cannot drift apart — the failure that has produced rewrite rounds elsewhere.

## Labels

Every stance label carries an explicit `nay: null`. The test the campaign settled on is whether
the act is single-subject, whether its whole operative content is the area's own mechanism, and
whether the mainstream objection is to that mechanism rather than to cost, local control or a
rider. None of these eleven passes all three cleanly:

- SB 3 repeals programs across alcohol permits, scholarships and procurement, so a no vote
  could be about scope.
- SB 520 and SB 486 are about local control and litigation exposure as much as about the area.
- SB 426, HB 1974 and HB 1017 each carry more than one strand.

Tags therefore sit on the yes side only. **661 tags, and the arithmetic matches exactly**: the
sum over all 15 rolls of (yes voters matched) times (labels on that roll) is 661, and the
database holds 661, with 0 tags on any no-side record.

Directions follow the research area's own description, never the bill's framing. `immigration`
reads "Welcome immigration through a lawful, orderly, and humane system", so enforcement
measures score against — the same rule that made Texas SB 8 immigration/against and Connecticut
HB 7066 immigration/for.

### The election-integrity cluster

SB 207, SB 211 and HB 1713 all tighten Arkansas's citizen initiative process. They score
`election_integrity`/for, following Florida HB 1205, which was re-investigated from enacted text
and reached the same answer: each provision is a warning, a sworn statement, or a readability
standard, and the objection — that these burden citizen lawmaking — maps to no research area.
`election_integrity` reads "secure, accurate, auditable, and trusted by the public", which is
what these provisions address; California's rule that voter-access measures belong in
`civil_rights` does not reach them, because none of the three is about access to voting.

Each description carries the provisions that cut the other way, rather than hiding them:
SB 211's death-or-disability exception and the rule that the affidavit is not evidence either
way; HB 1713's duty on the Attorney General to give reasons, and its carve-out for titles
already approved.

### HB 1017 carries two labels, one per strand

The act pushes in opposite directions on different subjects, so it takes one label per strand,
the pattern set by Florida SB 700. The state now pays the entire cost of school maternity leave
instead of half, schools can no longer opt out, and foster placement of an infant is newly
covered — `social_programs_and_welfare`/for. In the same act, eligibility is newly narrowed to
female employees with no leave-abuse discipline (the year of service was already required), which
writes adoptive and foster fathers out of a benefit the prior text did not limit by sex —
`civil_rights`/against.
Both strands are stated in the description.

### SB 591 is a standby law, and the description says so

Act 973 would ban abortions sought because of the race of the fetus, but Section 2 makes the
whole act effective only once the Attorney General certifies that a court has blocked Arkansas's
existing near-total abortion ban or that the state has repealed or loosened it. The description
keeps the substance (blocked, repealed or loosened) and leaves out the certification step, which
is procedure rather than a condition a voter weighs. Describing it as a live restriction would be
false. The description opens by
saying it does not apply yet and writes the substance in the conditional. The label stays
`womens_reproductive_rights`/against, because the vote is on a measure that would restrict.

### Where the title misleads

Three acts do materially more than their titles say, and the descriptions follow the text:

- **SB 3** is titled as a ban on preferential treatment. Most of it is repeals of existing
  minority recruitment, retention and reporting programs.
- **HB 1974** is titled as prohibiting state entities from employing unauthorized workers. Its
  definition of employer reaches counties and cities too, and the act attaches no fine and no
  criminal penalty — the only enforcement is a notice and 30 days to fix. The description says
  both.
- **HB 1150** is titled as barring a pharmacy benefits manager from "obtaining" permits. The act
  also forces existing permits to be revoked or not renewed.

## Import

- Judge: 15 rolls, all `updated`, no gate errors. No roll needed `acknowledge_later_rolls`,
  because selecting each chamber's last kept floor vote already satisfies the superseded-stage
  gate and Arkansas's emergency-clause votes are excluded by the config rather than competing
  with passage.
- Dry run planned 812 inserts across 96 candidates, 0 errors, 0 related flags, 0 ambiguous.
- Real run: **812 inserts, 96 candidates, 0 errors, 0 notified**, stamp
  `2026-09-03T01:56:16.672Z`, ledger `import-report.json`.
- **Reconciled three ways.** Rows carrying that stamp: 812. All live Arkansas roll-call rows:
  812 across 96 candidates. Table total moved 158,040 to 158,852, a difference of exactly 812.
  The dry run's own stamp `2026-09-03T01:55:36.429Z` matches **zero** rows, which is positive
  proof `--dry-run` wrote nothing.
- Convergence: a second dry run reports all 812 `unchanged`
  (`import-dry-run-rerun-report.json`). The original insert ledger was not overwritten.
- **Review fixes.** PR review found four descriptions that misstated their act: HB 1017 called
  the pre-existing one-year service rule new; SB 486 dropped the conditions on the right to sue
  (the operator let the person in or failed to take reasonable steps; sleeping-quarter claims
  need compelled sharing); SB 211 dropped the exception that lets a canvasser keep collecting
  once the Secretary of State finds the petition eligible for a cure period; SB 591 said
  "repeals" where the act also triggers on a partial amendment. `judgments.json` was corrected,
  re-judged (15 `updated`) and re-imported, stamp `2026-09-04T05:39:54.643Z`: **306 `rewrite`,
  506 `unchanged`, 0 errors**, ledger `import-rerun-report.json`. The 306 are exactly the five
  affected rolls (HB 1017 74, SB 486 72, SB 591 70, SB 211 House 79 + Senate 11), and a query
  for the old wording afterwards matched 0 live rows.
- 96 candidates is every member the crosswalk maps. **Arkansas's Speaker votes**, so there is no
  shortfall of the kind Texas (Burrows) and Georgia (Burns) show.

## Duplicate check

The importer flagged 0 related records. Because that scan misses a hand-written record whose
event date differs from the roll date, a wider sweep also ran over every Arkansas candidate
record not written by this pipeline, searching for each batch measure's bill number and act
number. It found two records, both pure sponsorship claims (Kendon Underwood on SB 207, Ryan
Rose on HB 1713). Neither states a vote or a tally, so both are distinct claims from a vote
record and nothing was retired.

## Production

Production was not touched. Arkansas has zero roll-call records there.
