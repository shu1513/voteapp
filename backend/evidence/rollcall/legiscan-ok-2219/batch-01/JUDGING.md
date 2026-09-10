# How batch 01 was judged

Every description was written from the enrolled act, read with the strikethrough and
underline marks made visible. Oklahoma prints amendments in place, so a plain text
extraction runs the old words and the new words together and reads as though repealed law
were still in force. On SB 364 the plain extraction reads "using corporal punishment on
students any student identified with the most significant cognitive disabilities", which is
two different rules run together. The marks are recovered from the rules drawn in the PDF: a
rule at about 0.42 of the glyph height struck the text, one at about −0.06 underlined it.

Nothing was judged from the bill title or from LegiScan's description. Both were used only
to decide what to read.

## The seven measures

**SB 364, corporal punishment.** The old law protected only students with the most severe
cognitive disabilities, and even for them the protection was the default: it gave way only
where the student's yearly education plan addressed the punishment ("unless addressed in an
annual IEP"). The first draft read that clause backwards, as if the plan had to opt the
student in; the review caught it. The act extends the protection to any student identified
with a disability under federal special education law, strikes the education-plan exception,
and deletes the subsection that let a parent or guardian waive the protection in writing. Scored `civil_rights`, yes = for. The no side is null: an objection here is
usually about school discipline authority, which is a different question from the rights of
the student.

**SB 504, minimum marriage age.** The act deletes the whole of the subsection that allowed
marriage below eighteen — parental consent for under-eighteens, and court approval for
under-sixteens where a paternity or seduction suit was being settled or where the girl was
pregnant or had given birth. Only the bar on marrying a close relative survives. Section 2
sets the effective date at 1 November 2026, six months after the House vote, so the
description states that date instead of presenting the new rule as current law.
`civil_rights`, yes = for.

**HB 2263, phones in school and work zones.** This one is a veto override, and the
description says so rather than saying the House passed the bill. The Governor vetoed it on
13 May 2025; the House overrode 68-21 on 29 May and the Senate overrode 38-7 the same day,
so the act became law. The Senate's vote is not in the pool because 38-7 is not closely
divided. The ban is narrower than "no phones while driving" and the description says so: it
reaches only a marked school zone with a reduced limit in force, or a road work zone, and
only while the vehicle is moving. The description also carries the limits that cut the other
way — no licence points, a hundred-dollar ceiling, and the bar on police taking or reading
the phone without consent, a warrant, or probable cause.
`public_safety_and_crime_control`, yes = for.

**SB 1344, insulin.** This is a programme with money attached, not a study, which is why it
survives the rule that dropped nineteen study bills from this pool. The department may pay
one or more manufacturers of a fast-acting biosimilar insulin, on conditions: made in the
United States, matching non-state money, a signed agreement committing the maker to a low net
price, and a clawback if the maker does not deliver. `healthcare_affordability`, yes = for.

**SB 109, cancer testing.** A coverage mandate with the patient's cost share set to zero, and
one carve-out that the description keeps: on a high-deductible plan tied to a health savings
account the rule starts only after the federal minimum deductible is met, except for care
federal law counts as preventive. Dropping that carve-out would overstate the act, which is
the failure that has forced correction rounds in several states.
`healthcare_affordability`, yes = for.

**SB 139, school phones.** The requirement is for one school year only. For 2025-2026 every
district board **had to** adopt a bell-to-bell ban; for later years the act says a board
**may** adopt one. The description states both halves, because describing this as a standing
statewide ban would be wrong. `public_education_quality`, yes = for.

**SB 889, hospital prices.** Publication duties plus real consequences: a hospital that was
materially out of compliance on the day it treated a patient may not pursue that patient for
the bill, the patient may sue, collection stops while the case runs, and a hospital found out
of compliance must refund whatever was paid, pay the patient a penalty equal to the total
debt (Section 7(C)(1) — the whole debt, not the amount paid, so a patient who paid nothing
still collects), cover legal costs and clear the debt from the patient's credit record.
`healthcare_affordability`, yes = for.

## Things worth recording

- **Three of the seven became law without the Governor's signature** (SB 364, SB 504,
  SB 109). Their descriptions say so. Writing "the Governor signed it" would have been false
  for all three.
- **The builder refuses to write bad prose** rather than reporting on it afterwards. It
  asserts the sentence join, the absence of a comma splice, the roll's own tally in both
  sentences, a 45-word ceiling and an explicit British-spelling word list, and it prints a
  Flesch-Kincaid grade for every description. It stopped the file being written three times:
  a 51-word sentence in SB 504, `licence` in HB 2263 and `recognised` in SB 109.
- **Reading level was measured before the import, not after.** The first draft measured a
  median grade of 11.7. Rewriting to short sentences brought it to 6.1. That rewrite was
  then re-read against each act, because a plain rewrite is where scope quietly broadens —
  the defect that has cost several states a correction round.
- **No judgment needed `acknowledge_later_rolls`.** Filter 4 takes each chamber's last kept
  roll, and Oklahoma's fourth reading is that chamber's final action, so the superseded gate
  fired on nothing.

## Ledgers

`import-report.json` is the original insert ledger, 303 inserts at stamp
`2026-09-10T02:38:35.564Z`. `import-dry-run-report.json` is the plan that preceded it, and
its own stamp matches no row in the database, which is what proves a dry run writes nothing.
`import-dry-run-rerun-report.json` is the convergence run taken after the import, reporting
all 303 rows unchanged.
