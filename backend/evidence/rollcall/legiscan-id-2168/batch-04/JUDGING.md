# Idaho 2025 batch-04 — how each measure was judged

Same method as the earlier batches. The act is the only source, read with
`tools/id_text.py`, and the engrossed print is read wherever a measure was
amended.

## House Bill 35 — chapter 6, state cybersecurity

Requires multifactor identification across the legislature, the courts, the
elected constitutional officers and their staffs, for local and remote access to
email, cloud storage, web applications, networks, databases and servers. Also
changes the Office of Information Technology Services from recommending to
directing how agencies buy telecommunications equipment. Data privacy, a yes
vote is for.

Worth noting for anyone reading the code later: this act and House Bill 32, the
mask mandate ban in batch-03, both create a section numbered 67-2362. Both
became law in the same session.

## House Bill 136 — chapter 246, 340B reporting

Providers in the federal 340B discount drug program must report yearly what they
paid for the drugs, what they were paid, what went to contract pharmacies, how
many claims were involved, and how the savings were used. Reports are broken out
by commercial, Medicaid and Medicare payers. Individual reports stay
confidential and only a combined total is published. Corporate accountability, a
yes vote is for.

## House Bill 146 — chapter 153, wind turbine lights

Requires the owner of a wind farm, new or existing, to apply to the Federal
Aviation Administration for a system that keeps aviation warning lights dark
until an aircraft approaches, and to install it if the federal agency approves.
New farms have twenty-four months after approval, existing ones sixty.
Environment and public health, a yes vote is for.

The direction was argued, because the requirement is a cost on wind developers.
The obligation is to apply and then install if the federal agency agrees, so a
refusal does not block the wind farm. What the act does is remove a nightly
light nuisance for people who live near turbines. That is the effect the
description states.

## Senate Bill 1099 — chapter 171, vehicular manslaughter

Splits the penalty for vehicular manslaughter, keeping the existing maximum for
a first offender and creating a higher tier for a driver with an earlier
conviction for driving under the influence here or a comparable offense
elsewhere. Requires courts to warn a defendant at sentencing that a later
conviction can carry the higher penalty. Public safety and crime control, a yes
vote is for.

## Senate Bill 1139 — chapter 182, police certification

Replaces a blanket bar on certifying anyone convicted of a felony with a bar
limited to felonies listed in section 18-310(2) of Idaho Code and comparable
out-of-state offenses, leaving the council discretion elsewhere, including where
a felony was set aside, expunged, pardoned, dismissed or reduced. Public safety
and crime control, a yes vote is for. The description says the automatic bar
narrows, because that is the part a reader may weigh differently.

## Senate Bill 1211 — chapter 334, ivermectin

Lets ivermectin made for human use be sold and bought over the counter in Idaho
with no prescription and no consultation with any health care professional.
Environment and public health, a yes vote is against, because the act removes
the professional check that stands between a patient and a prescription drug.

## Checks run

- Repository plain-language lint over all 12 descriptions: 0 warnings.
- Flesch-Kincaid grade measured separately: median 11.0, worst 13.5.
- British spellings checked against an explicit word list: none.
- `", The "` appears in no description.

## Reconciliation

| source | records |
| --- | --- |
| `import-report.json` inserts | 257 |
| Idaho roll-call rows added to `candidate_records` | 257 |
| crosswalk-matched members summed over the 6 rolls | 257 |

6 rolls imported, 0 errors. The 2025 session now holds 2,080 records from 50
rolls across 34 measures.
