# Nebraska batch-03, judging notes

## The run

- judge: 3 approved, 0 errors
- dry run: 36 planned inserts, stamp `2026-09-10T04:41:01.911Z`
- real run: **36 inserts, 0 errors, 0 notified, 0 related flags, 0 ambiguous**,
  stamp `2026-09-10T04:41:28.948Z`
- convergence run: all 36 unchanged

Reconciled three ways, and the dry run's stamp matches zero rows. **22 tags**,
the yes-side count, since every label states `nay: null`.

Review of the pull request found two reading errors (LB 319 and LB 929, noted
under each measure). The builder was corrected, `rollcall:judge` re-applied the
three judgments, and a second import rewrote the 24 records on those two rolls
in place (`import-dry-run-rerun-report.json` holds the plan,
`import-rerun-report.json` the run). LB 1029's 12 were unchanged; no record ids
moved and nobody was notified.

## The measures

### LB 319, food assistance after a drug conviction — social_programs_and_welfare, for

Federal law bars anyone convicted of a drug felony from food assistance for
life, and lets a state opt out. Nebraska's current opt-out is partial: one or
two possession-or-use felonies keep eligibility with treatment, three or more
or any sale conviction lose it outright. The bill would have opened the program
to anyone with a drug felony who had finished their sentence or was on parole,
probation or post-release supervision. One section, one subject.

Review of the pull request caught a condition the first draft dropped. The
Final Reading text (pages 6-7, read with `ne_text.py` so the struck old rule
and the underlined new one could be told apart) keeps a treatment requirement
for people with three or more possession-or-use felonies: in, or finished, a
licensed treatment program since the latest conviction or while incarcerated or
under supervision, unless a provider licensed under the Uniform Credentialing
Act finds treatment unnecessary. The draft said sentence completion alone was
enough for everyone. Both descriptions now carry the condition.

This measure was on the shortlist for batch-01 on the strength of its title and
was pulled when its history was read. It passed 32-17, the Governor vetoed it,
and the override failed on a 24-24 tie.

### LB 929, managed care paying Medicaid copays — healthcare_affordability, for

The first draft described the bill as introduced: a bar on charging Medicaid
recipients a deductible, copay or coinsurance unless federal law required it,
a delay to October 2028, the federal minimum, and no refusal of care for
non-payment. Review of the pull request caught that none of that survived.
The Final Reading text — the version this roll voted on — keeps the state's
power to set premiums, copays and deductibles untouched and adds one
subsection: the department must let a managed care organization pay those
charges on an enrollee's behalf where federal law allows. The title changed to
match ("to allow managed care organizations to pay deductibles, cost sharing,
or similar charges"). Both descriptions were rewritten around that narrower
change, with one sentence saying the ban was in an earlier draft and removed
before the vote, since that is how the bill was reported.

The label stays `healthcare_affordability`, yes = for: letting the plan pick
up the charge lowers what the patient pays out of pocket, even if the bill no
longer removes the charge itself.

The Intro text was the trap here. The rule that applies to enacted measures —
read the act, never the committee statement — applies to vetoed ones too: read
the Final Reading text, never the introduced bill.

### LB 1029, reporting foreign money at public colleges — anti_corruption, against

Nebraska law makes a public college report funding and contracts from a foreign
adversarial source. The bill would have counted a contract only where it
benefits that source, and would have taken salary, wages and other pay out of
what is reportable at all. Less would have been disclosed.

The area is anti_corruption because it turns on transparency: the area's own
words are transparency, ethics rules and enforcement, and West Virginia already
established that records access belongs there. The no side takes no tag, since
a no vote could rest on disclosure or on the burden the reporting puts on
universities.

## Wording, and the check behind it

Every body is conditional and every tail states what actually happened. The
builder requires "would have" in each description and refuses text that calls
the bill an act or says it became law. That check was proved to fire on a
deliberately wrong string before the real file was written, because a check
that never fires cannot be told from one that passes.

Reading level: **grade median 8.9, worst 10.0**. LB 1029 is the worst and stays
there — adversarial, disclosed and contract are the words the statute uses.
Lint: 0 warnings.
