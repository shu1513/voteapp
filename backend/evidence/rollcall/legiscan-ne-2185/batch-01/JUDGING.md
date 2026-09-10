# Nebraska batch-01, judging notes

Every description was written from the enacted act, read with the markup the
plain text dump loses. The Committee Statement was used as an index to the
sections, never as the source, and the introducer's statement of intent was
never opened.

## The run

- judge: 4 approved, 0 errors
- dry run: 48 planned inserts, stamp `2026-09-09T23:14:23.569Z`
- real run: **48 inserts, 0 errors, 0 notified, 0 related flags, 0 ambiguous**,
  stamp `2026-09-09T23:14:41.778Z`
- convergence run: all 48 unchanged

Reconciled three ways: the dry run planned 48, the real run inserted 48, and
the database holds 48 live records under `origin_run_id LIKE 'rollcall:NE:%'`
across 12 candidates. The dry run's own stamp matches zero rows, which is the
proof it wrote nothing.

**37 tags.** Every label states `nay: null`, so only the yes side is tagged.
The yes-side counts in the ledger are 9, 9, 9 and 10, which is 37. That number
was worked out from the ledger before the database was asked, and the database
agreed.

## The measures

### LB 266, rent control — housing_affordability, yes = against

The whole act is one rule: a city, village or county may not pass or enforce a
local law that has the effect of rent control on private property, and any such
law is void, even under a home rule charter. The description carries both
exceptions the act states, because a ban qualified by the statute has to be
described with its qualification: land-use or inclusionary housing rules
adopted to add affordable housing are untouched, and so is a rent-limiting
program a property owner joins by contract.

The direction follows the area's own words, "reduce cost burdens for renters".
Removing the tool that caps rents pushes against that. The reply — that rent
caps hold back new building — is a real argument, but it sits inside the same
area, so the no side takes no tag. That is the Oregon HB 3054 shape, where a
rent cap scored for the area and the investment argument left the no side null.

### LB 89, Stand With Women Act — civil_rights, yes = against

A wholly new act, so there is no struck text to recover. Follows the line
already set by Ohio SB 1, Texas SB 12, Indiana HB 1041 and South Carolina
H 4756.

**The Committee Statement describes a version that did not pass.** It walks
through eleven sections, including one requiring state agencies to define sex
in their rules and decisions. The enacted act has eight sections and no such
requirement. The description is written from the act.

The doctor's-note requirement is stated because it is what the act asks of
every student who wants to play on a team labeled by sex. The no side takes no
tag: a no vote could rest on the exclusion or on that paperwork, and the second
of those is a different argument inside the same area.

### LB 258, minimum wage — reduce_wealth_gap, yes = against

**This is the measure that proves the markup reader earns its keep.** Flattened,
the escalator clause reads "shall be increased on January 1, 2027 ... by one
and three-quarters percent the increase in the cost of living", which is the
new rule and the old rule run together with nothing to tell them apart. With
the marks recovered, the act plainly strikes the cost-of-living link that
Nebraska's voters put in the law in 2022 and replaces it with a flat 1.75 per
cent a year.

The act also adds a youth minimum wage of $13.50 for workers aged 14 and 15,
and rewrites the training wage for new workers aged 16 to 19, setting it at
$13.50 where it had been 75 percent of the federal minimum wage. **That last
change is an increase, and it is stated in the description rather than left
out**, under the rule that a narrow counter-provision inside an otherwise
one-directional act is described, not hidden. It does not change the direction:
against a $15 minimum wage, both new rates are sub-minimum, and the escalator
change reaches every minimum-wage worker permanently.

The no side takes no tag. A senator could have voted no on the wage cut, or
because the bill rewrites a law the voters passed, and the second is a
different question.

### LB 966, Hunger-Free Schools Act — social_programs_and_welfare, yes = for

**Here too the Committee Statement describes the introduced bill, not the act.**
It says qualified schools "must" serve the meals. The enacted act makes it a
pilot: a school "may apply", and the Department of Education picks which
applicants take part, for the school years 2026-27 through 2031-32. The
description says so.

The act repeals an older state payment of five cents for each school breakfast.
That is stated, and it is not a counter-direction: the committee's own
section-by-section note records that those sections are replaced by the new
program.

The act carries an appropriation, but it is not an appropriations measure: it
creates a program and the money rides with it, which is the line Connecticut
drew between SB 1, kept, and HB 7163, dropped. The no side takes no tag,
because a no vote here reads as a vote about spending.

## Checks run before the import

- The builder refuses to write a file that breaks any of them: a sentence over
  45 words, a comma splice before the closing sentence, a British spelling, or
  a description that does not carry its own roll's tally. The
  British-spelling check is proved to fire on a known-bad string first, since a
  check that never fires cannot be told from one that passes.
- The repository's own plain-language lint: 8 descriptions, 0 warnings.
- Reading level measured separately, because the lint only counts sentence
  length. A first draft measured grade 9.4 to 10.2 and was rewritten before
  anything was imported. The final text measures **grade median 7.5, worst
  7.8**, longest sentence 24 words. The review round below added one clause
  each to two descriptions; the longest sentence is now 30 words, and the lint
  still reports 0 warnings.
- Descriptions run 8 to 15 short sentences rather than the usual two to four.
  That is a deliberate trade: cutting further means dropping the limits and
  exceptions the acts actually contain, and dropping exactly those is what has
  caused most correction rounds in this campaign.

## Review round

The pull request review caught two places where a description was broader
than its act.

- LB 89. The text said no government body, licensing group or sports
  association may look into or punish a school that keeps separate female
  teams, which reads as blanket immunity. Section 5 shields a school only
  "for maintaining" those teams. The sentence now ends "for keeping those
  teams".
- LB 966. The text said a school may apply if it does not already feed every
  student for free. Section 3(5) excludes only a school that serves free meals
  to all students under the federal community eligibility provision; a school
  that funds universal free meals another way still qualifies. The sentence
  now carries "under the federal community eligibility option".

Re-judged and re-imported in place: 24 records rewritten (12 per measure), 24
unchanged, 0 notifications. Ledger: `import-review-fixes-report.json`.
