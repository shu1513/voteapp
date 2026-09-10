# Nebraska batch-03, judging notes

## The run

- judge: 3 approved, 0 errors
- dry run: 36 planned inserts, stamp `2026-09-10T04:41:01.911Z`
- real run: **36 inserts, 0 errors, 0 notified, 0 related flags, 0 ambiguous**,
  stamp `2026-09-10T04:41:28.948Z`
- convergence run: all 36 unchanged

Reconciled three ways, and the dry run's stamp matches zero rows. **22 tags**,
the yes-side count, since every label states `nay: null`.

## The measures

### LB 319, food assistance after a drug conviction — social_programs_and_welfare, for

Federal law bars anyone convicted of a drug felony from food assistance for
life, and lets a state opt out. The bill would have had Nebraska opt out for
people who had finished their sentence or were on parole, probation or
post-release supervision. One section, one subject.

This measure was on the shortlist for batch-01 on the strength of its title and
was pulled when its history was read. It passed 32-17, the Governor vetoed it,
and the override failed on a 24-24 tie.

### LB 929, Medicaid copays — healthcare_affordability, for

The bill would have barred the state from charging Medicaid recipients a
deductible, copay or coinsurance unless federal law required it, and where
federal law does require one, would have delayed it to October 2028 and
required the lightest version. The description carries all three parts of that
lighter version, because they are what the bill would have done rather than
background.

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
