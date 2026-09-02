# Montana batch-06 — how each measure was judged

Montana publishes no neutral prose summary of a bill, so the enrolled text is
both the ground truth and the only source. Every judgment rests on the enrolled
bill from `https://api.legmt.gov/docs/v1/documents`, plus the official action
trail and the session law chapter number from the same site.

Every measure was searched for the phrase "Coordination instruction", which is
the standing rule added in batch-04 after HB 231 turned out to have been largely
voided by its own. **None of the four has one**, and neither does any of the
measures read and set aside.

## HB 20 — voted levies stated in dollars, not mills

Chapter 95. 5 substantive pages, **all five rendered and read**. Amends
15-10-420 and 15-10-425 and repeals 7-6-4431.

A mill is a property tax rate: one mill raises one dollar for every thousand
dollars of taxable value. A levy fixed in mills therefore raises more money as
property values rise. A levy fixed in dollars raises the same amount every year,
and the mill rate floats to hit it.

Before the act, a local government putting a levy to voters could write the
question either way: a dollar amount with an approximate mill figure, or a mill
figure with an approximate dollar amount. The act deletes the second option. The
binding number is now always the dollar amount, and the mill figure is labelled
approximate and is informational only. Because 15-10-425(3) already required the
ballot to reflect the resolution, the ballot follows.

What the page images showed, and this one is severe:

- Page 5 carried the single operative sentence, 15-10-425(4). The extracted text
  reads "impose the levy in either the amount or the number of mills specified".
  The image shows "either" and "or the number of mills" both **struck through**,
  so the real text is "impose the levy in the amount specified". The extracted
  file states the opposite of the enacted law. These were mid-sentence phrase
  deletions with **no numbering artefact at all** — the third time in this
  campaign that the tell failed, after HB 685 and HB 719.
- The same page shows "or number of mills" struck from 15-10-425(5), which is
  the rule on carrying unused levy capacity forward.
- Page 4 shows the whole second option in the resolution contents struck, and the
  connector "or" replaced by "and", which the extracted text rendered as the
  meaningless string "or and". The same page confirms that all of new
  15-10-425(2)(b) is underlined, so it is new law.
- Pages 1, 2 and 3 carry only insertions and trivial wording fixes. Every figure
  in the growth formula and the whole list of exempted levies survive unchanged.

New 15-10-425(2)(b) is the one thing that lets a voted dollar levy grow: a
county, city, town or special district may write into the resolution that the
levy is subject to 15-10-420(1)(a), which permits the prior year's dollars plus
half of the average inflation rate for the prior three years. **School district
levies are expressly excluded from that option.** The act is silent on growth
from new construction, and silent on levies voters approved in mills before it
passed; there is no transition, conversion or grandfather clause anywhere.

No effective date section, so Montana's default rule at 1-2-201 applies and the
act took effect October 1, 2025.

Area `cost_of_living_reduction`, yes vote **for**. A dollar-stated levy cannot
grow with assessed values, so a household's bill from that levy is capped at what
voters approved. The counter-fact is real and the description carries it: local
governments lose the automatic growth, and school districts are the only class
barred from asking voters for the inflation option.

## HB 300 — sex-based rules for school sports and facilities

Chapter 36. Amends 49-2-307, the education section of Montana's Human Rights
Act. Page 2 rendered, and the render changed the answer.

**The hazard ran the other way here.** A first draft of the description said the
act adds three rules, the third being that a student does not discriminate by
calling another student by their legal name or referring to them by their sex.
The page image shows subsection (2), which carries that rule, printed in **plain
type with no underline**, so it is pre-existing law that the act merely reprints.
Only (3) and (4) are underlined. The act's own title confirms it: the title
names the athletics rule and the facilities rule and says nothing about names.
The description was corrected before the batch was finalised, and 85 records were
rewritten. The lesson is that unmarked text can be mistaken for new law just as
easily as struck text can be mistaken for live law.

Two additions, both fully underlined with nothing struck:

- New (3): it is an unlawful discriminatory practice for an educational
  institution that operates, sponsors or facilitates athletic programs to permit a
  person whose sex is male to participate in an athletic program or activity
  designated for females.
- New (4): it is an unlawful discriminatory practice for an educational
  institution to fail to provide access to a restroom, locker room, shower area
  or sleeping quarter that is not accessible by a person of the opposite sex
  while in use.

New (4) carries real carve-outs, also underlined. It does not apply to a person
entering a space designated for the opposite sex for custodial or maintenance
purposes, to render medical assistance, or during a natural disaster, emergency
or serious threat to order or safety. And nothing in it stops a school from
adopting policies needed to accommodate people protected under the Americans with
Disabilities Act, young children needing assistance or elderly people needing
aid, from establishing single-occupancy or family spaces, or from redesignating a
multi-occupancy space from one sex to the other.

The four existing prohibitions on discrimination in admissions, application
forms, advertising and quotas are untouched.

Area `civil_rights`, yes vote **against**. The corpus scores measures that
narrow gender-identity protections in this direction — Pennsylvania SB 9 and SB
1293 on school sports, Texas SB 12, North Carolina H 805, and the federal
H.J.Res. 165. The mechanism here is unusual and worth noting: the act does not
add a prohibition outside the Human Rights Act, it rewrites the Human Rights Act
itself so that including a transgender student becomes the discriminatory
practice.

## HB 638 — ban on diversity statements in government

Chapter 331. New sections only, codified as a new chapter in Title 49. No
amended law, so no strike-through hazard.

A state or local government agency may not require, request, solicit or compel a
person to provide a diversity statement, and may not grant any preferential
consideration or treatment to a person who provides one, solicited or not. A
"diversity statement" is a submission, statement or document that promotes or is
intended to promote differential treatment based on race, color, ethnicity, sex,
sexual orientation, national origin, religion, or gender identity.

Two limits survive. If federal law requires an agency to accept or require such a
statement, the agency may accept it only to the extent federal law requires and
must limit its consideration accordingly. And nothing in the section stops an
agency from having policies needed to comply with state or federal law, including
laws on prohibited discrimination and harassment.

The reach is wide: "state or local government agency" covers any branch,
department, office, board, bureau, commission, agency, university unit or college
of state government, and any county, city, town, school district or other unit of
local government.

Area `civil_rights`, yes vote **against**, on the same precedent as Texas SB 12,
which barred schools from assigning diversity, equity and inclusion duties.

## HB 723 — reporting on infants born alive after an abortion

Chapter 517. New section codified into Title 50, chapter 20, part 8. No amended
law, so no strike-through hazard.

A medical facility in which an infant is born alive after an abortion or
attempted abortion must file an annual report with the Department of Public
Health and Human Services by February 28. A report must be filed even when the
number is zero. It must give the approximate gestational age in nine set bands
from under 9 weeks to 37 weeks to term, the medical actions taken to preserve
life, the outcome including survival, death and place of death, and the infant's
medical conditions before and after the attempted abortion.

Penalties escalate. Up to $500 for missing the deadline by 30 days, up to another
$500 for each further 30-day period, and after a year of non-filing or
uncorrected deficiencies the department may sue for an injunction compelling the
report.

The department must publish an aggregate annual report by June 30 and must take
care that nothing in it could reasonably identify anyone who supplied
information.

Area `womens_reproductive_rights`, yes vote **against**. The reporting duty and
its escalating fines fall only on facilities that provide abortions, and the
statute is codified in Montana's abortion title rather than in general vital
statistics law. The description states the mechanics without characterising them,
so a reader can weigh the anonymity protection alongside the fines.

## Measures read and set aside

Written up in `../survey/filter-5-drops.md`. In short: **HB 343** and **HB 495**
are genuinely two-sided; **HB 401** has no research area that maps to it with a
defensible direction; **HB 329** and **HB 801** are deferred for roll-selection
and text reasons rather than dropped.

## Reading level

Measured after writing with the Flesch-Kincaid grade formula. Median **7.5**,
worst **7.8**. The longest sentence anywhere is 24 words, well inside the 45-word
ceiling `candidateRecordPlainLanguageLint` enforces, and the lint was run before
import.

Median **7.2** after the HB 300 correction. The first drafts of HB 300, HB 638
and HB 723 measured 9.2, 10.5 and 9.0. They
were rewritten by splitting long sentences and moving each act's list of
protected characteristics into a sentence of its own. Nothing was dropped from
the substance; the lists are still complete.

## Which roll, and which text

Each measure contributes one roll per chamber, and the superseded-stage gate
accepted all eight with no `acknowledge_later_rolls` entry, which independently
confirms each is its chamber's last kept floor vote. HB 20 and HB 723 were
amended by the Senate, so the House rolls used are the later ones backing the
Senate version, not the original passage votes.

## What was checked and found clean

- All four became law and carry a session law chapter number, confirmed against
  `api.legmt.gov`.
- Every roll is divided: the losing side is at least a quarter of the winning
  side. The widest margin is HB 20 in the Senate at 34-16, which is 47 percent.
- None of the four is a joint resolution.
- No measure contains a coordination instruction.
- The import reconciles three ways: the insert ledger records 342 rows across 8
  rolls, the database holds 342 rows at run stamp `2026-09-02T06:36:22.021Z`, and
  the dry run beforehand wrote 0 rows.
- After the HB 300 correction, a second import run rewrote 85 rows and left 257
  unchanged, which matches HB 300's two rolls exactly. That run is preserved as
  `import-rewrite-report.json`; the original insert ledger is untouched in
  `import-report.json`.
- Montana's jurisdiction total is now 3,155 records across 87 candidates and
  1,827 area tags in 14 research areas.
