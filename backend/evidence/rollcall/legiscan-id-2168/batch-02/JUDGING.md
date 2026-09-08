# Idaho 2025 batch-02 — how each measure was judged

Same method as batch-01. Idaho publishes no neutral analysis, so the act is the
only source, read with `tools/id_text.py` so struck and underlined text stay
apart. One body per measure, with the yes and no descriptions generated from it.

## House Bill 79 — chapter 21

Raises the ceiling on community college tuition for in-district students from
two thousand five hundred dollars to three thousand two hundred fifty dollars a
year. The ten percent yearly step limit and the three hundred fifty dollar floor
are unchanged. Cost of living reduction, a yes vote is against, because the act
lets the price a student pays rise by up to thirty percent.

## House Bill 109 — chapter 340

Orders the department to seek a federal waiver removing candy and soda from the
foods food stamps cover, to enforce the ban if the waiver is granted, and to ask
again every year if it is refused. The act defines both terms carefully, which
is why the description quotes the flour and juice thresholds.

Two strands pull opposite ways and both are recorded. Social programs and
welfare, a yes vote is against, because it narrows what a benefit can buy.
Environment and public health, a yes vote is for, because that is the act's
stated aim. A reader who cares about one strand should not be shown the other's
direction.

## House Bill 180 — chapter 325

Puts a deadline on local decisions for broadband and cell equipment siting,
sixty to one hundred fifty days depending on the work, after which the
application is deemed approved. Adds notice and judicial review steps and
rewrites pole attachment rules. Public infrastructure, a yes vote is for. The
act does take discretion away from cities and counties, and the description says
what the deemed approval means.

## House Bill 187 — chapter 140

The advisement half is uncontroversial: a judge must explain probation
conditions, penalties and rewards. The second half writes into statute that a
parolee waives Fourth Amendment protection and consents to warrantless search of
their person, home, vehicle and phone at any hour with or without cause, must
sign that acceptance, and cannot be paroled if they refuse. Officers may not
search solely to harass.

Two strands, both honest. Public safety and crime control, a yes vote is for.
Civil rights, a yes vote is against.

## Senate Bill 1069 — chapter 91

Replaces a general duty to offer literacy professional development with a duty
to train every kindergarten through grade three teacher and elementary
administrator, over several years, subject to appropriation, on the science of
reading with coaching on the job. Broadens reporting to cover charter schools.
Public education quality, a yes vote is for.

## Senate Bill 1198 — chapter 317

Bans diversity, equity and inclusion offices, officers, required trainings,
required courses and bias reporting systems at Idaho's public colleges. The
statute names the concepts it reaches. Work an attorney certifies as needed to
follow a court order or a state or federal law is exempt. The attorney general
may enforce it and an affected person may sue. Civil rights, a yes vote is
against.

## Checks run

- Repository plain-language lint over all 24 descriptions: 0 warnings.
- Flesch-Kincaid grade measured separately: median 10.3, worst 10.9.
- British spellings checked against an explicit word list: none.
- `", The "` appears in no description.

## Reconciliation

| source | records |
| --- | --- |
| `import-report.json` inserts | 516 |
| Idaho roll-call rows added to `candidate_records` | 516 |
| crosswalk-matched members summed over the 12 rolls | 516 |

12 rolls imported, 0 errors. Idaho now holds 1,388 roll-call records across 88
candidates, from 32 rolls in two batches.
