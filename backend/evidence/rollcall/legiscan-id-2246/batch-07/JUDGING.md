# Idaho 2026 batch-07 — how each measure was judged

Same method throughout the campaign. The act is the only source, read with
`tools/id_text.py` so struck and underlined text stay apart, and the engrossed
print is read wherever a measure was amended.

## House Bill 542 — chapter 268, social media and children

Requires a platform to collect a birth date, to estimate the age of existing
account holders and treat anyone it cannot place above sixteen as a child, and
to get verifiable parental consent before opening or changing a child's account.
A child's account starts at the most private settings. The parent must be
offered a password giving them time reporting and the power to set daily,
weekly and time-of-day limits. Addictive interface features and profile-targeted
advertising may not be shown to a child. Data privacy, a yes vote is for.

## House Bill 561 — chapter 260, flags again

Rewrites the 2025 flag law, which is in batch-03 as House Bill 96. City and
county flags qualify only if official before 2023, a college may fly one flag if
its policy names exactly one, and foreign flags may mark an occasion or a
historic tie but not those of countries the United States is fighting. Property
now includes parks, roads and boulevards. Non-political banners on poles and
streetlights are allowed, and temporary parades are exempt. The new part is the
enforcement: two thousand dollars per flag per day, enforced by the attorney
general after a ten-day chance to take a flag down. Civil rights, a yes vote is
against.

## House Bill 667 — chapter 167, non-domiciled commercial licenses

Repeals the statute allowing Idaho to issue a commercial learner's permit or
commercial driver's license to someone domiciled in a foreign country or in
another state, and strikes the definition and references. Immigration, a yes
vote is for.

## House Bill 706 — chapter 320, single stairway apartments

Lets a local government adopt an exception permitting one stairway in qualifying
apartment buildings, which building codes normally forbid, and adjusts the local
code adoption rule to fit. The exception is permissive, not mandatory, and the
description says so. Housing affordability, a yes vote is for.

## House Bill 723 — chapter 139, children's residential care

Gives the department a quality of care oversight and inspection duty over
licensed children's residential facilities, requires an individual service plan
for each child, writes a youth bill of rights into law, and requires critical
incident reporting. Social programs and welfare, a yes vote is for.

## House Bill 822 — chapter 340, parental notice on transitions

Bars a school, child care provider, or medical, behavioral or mental health
provider from helping a child under eighteen with a sex transition procedure or
a social transition without informing the parents and obtaining consent. Social
transition is defined to include a change of name, pronouns, appearance or
dress. A parent may sue and the attorney general may enforce. Civil rights, a
yes vote is against.

## House Bill 929 — chapter 275, cash price for care

Bars an insurer from stopping a provider offering a patient the discounted cash
price, and requires the insurer to count what the patient paid out of pocket
toward the deductible and the yearly maximum where the service is covered,
medically necessary, priced below the plan's allowed amount, and documented.
Healthcare affordability, a yes vote is for.

## Senate Bill 1297 — chapter 249, conversational AI

Requires a chatbot that could be mistaken for a person to say it is artificial
intelligence, requires a protocol referring a user who raises suicidal thoughts
to crisis help, and bars programming the service to claim it gives professional
mental health care. For minors it adds a standing or repeating disclosure, bars
unpredictable rewards designed to increase engagement, and requires reasonable
measures against sexual material and against claims of being human or sentient.
Data privacy, a yes vote is for.

## Checks run

- Repository plain-language lint over all 16 descriptions: 0 warnings.
- Flesch-Kincaid grade measured separately: median 10.5, worst 11.5.
- British spellings checked against an explicit word list. The guard fired on a
  first draft of the House Bill 667 description, which used two British
  spellings; both were corrected before the file was written.
- `", The "` appears in no description.

## Reconciliation

| source | records |
| --- | --- |
| `import-report.json` inserts | 324 |
| Idaho roll-call rows added to `candidate_records` | 324 |
| crosswalk-matched members summed over the 8 rolls | 324 |

8 rolls imported, 0 errors. The Idaho campaign closes at 3,810 records from 90
rolls across 58 measures and 91 candidates.
