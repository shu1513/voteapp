# Nebraska batch-02, judging notes

Every description was written from the enacted act, read with the markup a
plain text dump loses. The Committee Statement was an index to the sections and
never the source, and the introducer's statement of intent was never opened.

## The run

- judge: 9 approved, 0 errors
- dry run: 107 planned inserts, stamp `2026-09-10T04:40:58.807Z`
- real run: **107 inserts, 0 errors, 0 notified, 0 related flags, 0 ambiguous**,
  stamp `2026-09-10T04:41:18.071Z`
- convergence run: all 107 unchanged

Reconciled three ways, and the dry run's stamp matches zero rows. **78 tags**,
which is the yes-side count in the ledger, because every label states
`nay: null`.

## ⚠ The Committee Statement described a different bill five times

This is the finding of the batch. Nebraska's Committee Statement is neutral and
section by section, and it is written when the committee sends the bill out.
The Legislature then amends it on General File and Select File. In five of the
measures read for this batch the statement no longer matched the act:

- **LB 921** — the statement says the notice duty falls on employers with 25 or
  more workers. The act says 100, the same as federal law. Describing it from
  the statement would have stated a threshold four times too low.
- **LB 1067** — the statement says the transfer tax rises by $1.50 with 75 cents
  to each housing fund. The act raises it by $1.00, split 50 and 50.
- **LB 397** — the statement says the safety committee requirement is removed.
  The act removes it for private employers and keeps it for public ones.
- **LB 653** — the statement describes a cap on how many applications from
  students with disabilities a district may deny. No such cap is in the act.
- **LB 1022** — the statement says human relations training is dropped for
  teacher and administrator certification. The act keeps it and carves out
  applicants for a substitute teacher's certificate.

Two of these five became drops. **Read the act, every time.**

## The measures

### LB 229, gig work and unemployment insurance — social_programs_and_welfare, against

The act adds app-based workers to the list of people whose work is not
employment under the Employment Security Law, which is the law that funds and
pays unemployment benefits. The description carries all four conditions the
exclusion depends on, and the federal carve-out, because a rule the statute
qualifies has to be described with its qualification.

The area is the safety net rather than corporate accountability: what changes
is who is covered by an unemployment insurance program. That follows West
Virginia HB 4005, where removing a worker protection with no labor area
available went to the same place. **Nebraska has no research area for labor or
union questions, the gap now recorded in eight states.**

### LB 241, data breach class actions — corporate_accountability, against

Single subject. The act's whole content is the standard a company is held to,
which is the area's own mechanism. The description states the standard that
replaces ordinary negligence, and lists what counts as private data, because
the shield's reach depends on that definition.

### LB 9, nicotine analogues — environment_and_public_health, for

Follows Texas SB 2024, Alabama HB 8, South Carolina S 287 and Pennsylvania
HB 1425. The act closes a gap: the old rules named nicotine, and lab-made
stand-ins that act like nicotine fell outside them.

### LB 48, juvenile assessment center — public_safety_and_crime_control, for

The area's own description names prevention. The act is a five-year pilot in
one city, and the description says so and names Omaha, following the rule that
a substantive single-city measure is kept with the place named.

**This is the measure behind the emergency clause trap.** LB 48 first failed
26-22 with an emergency clause, which takes 33 votes, and then passed 27-21 the
same day with the clause stricken. The roll imported is the one that passed.

### LB 275, foster children's Social Security money — social_programs_and_welfare, for

The strongest measure in the batch and the closest vote at 29-19. The
description carries the protected shares by age, because those percentages are
the whole point: they are the money the state may not take back for the cost of
care.

### LB 203, community-wide health orders — environment_and_public_health, against

Follows Kansas SB 29 and Alabama SB 71. The description carries the limit that
makes the act narrower than it first sounds: the approval requirement reaches
only orders covering everyone with no known chain of infection, so an order
aimed at people linked to a known outbreak is untouched.

Review of the pull request caught that the first draft generalized one
arrangement to every department. The act has two. Section 3 (71-1632) is the
general rule: written approval by a majority of the county board, or for a
city-county department a majority of the city council. Section 2 (71-1630(4))
is the one large city-county department, a county over 200,000 people, where
approval comes from the elected county board and city council members who sit
on its board of health. The draft described only the second. Section 4, which
the draft left out, makes every such order expire after seven days unless
approved again. The builder was corrected, `rollcall:judge` re-applied, and a
second import rewrote the 12 records on this roll in place
(`import-dry-run-rerun-report.json` holds the plan, `import-rerun-report.json`
the run); the other 95 were unchanged and nobody was notified.

The no side takes no tag. A senator could have voted no on the health authority
question or on making elected members sign off, and the second is an argument
about who decides rather than about public health.

### LB 397, workplace safety committees — corporate_accountability, against

The act ends a duty private employers have had since 1994 and keeps it for
public ones. **A first reading of the flattened text looked worse than the act
is**: large blocks about cancelling a workers' compensation policy appear
struck. Reading further shows those subsections had already terminated on
January 1, 2012, so what looks like a live protection being removed is dead law
being cleaned up. That is what the markup reader is for.

### LB 795, fentanyl and bromazolam — public_safety_and_crime_control, for

The description gives the three quantity thresholds and their felony classes
rather than saying penalties went up.

### LB 1067, transfer tax for workforce housing — housing_affordability, for

The act raises the documentary stamp tax by a dollar per thousand until 2032
and sends it to two workforce housing funds, and stops money being moved out of
the Affordable Housing Trust Fund from July 2027. The no side takes no tag: a
no vote reads as a vote about raising a tax, which is a different question from
housing supply.

## Checks run before the import

- The builder refuses to write a file with an over-long sentence, a comma
  splice, a British spelling, or a description missing its own roll's tally.
- The repository's plain-language lint over both batches: 24 descriptions, 0
  warnings.
- Reading level measured separately. A first draft measured grade median 10.9
  and worst 13.6 and was rewritten before anything was imported. The final text
  measures **grade median 7.9, worst 10.1**. LB 241 is the worst and stays
  there: cybersecurity, biometric and negligence are the words the act is built
  from, and replacing them would change what it says.
