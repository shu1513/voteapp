# Montana batch-09 — how each measure was judged

Montana publishes no neutral prose summary of a bill, so the enrolled text is
both the ground truth and the only source. Every judgment rests on the enrolled
bill, read from rendered page images, plus the official action trail and the
session law chapter number from `api.legmt.gov`.

**None of the eight bills read for this batch contains a coordination
instruction.**

Every roll was compared member by member against Montana's own vote record. All
agree exactly.

## A better way to find the strike-throughs

Until now every amended page has been rendered as an image and read by eye. On
HB 690, a 24-page bill, a faster and more complete method worked: extract the
PDF's actual **underline and strike line objects** with `pdfplumber` and match
each to the characters it crosses. That produces a mechanical map of every mark
in the document, and it says which pages carry no marks at all, so those can be
skipped honestly rather than skimmed. A sample was confirmed against rendered
images. This is now recorded in the campaign checkpoint as the preferred method
for long bills, with image rendering used to confirm.

## HB 168 — state school money follows preschoolers with disabilities

Chapter 672. Amends 20-5-101 and 20-9-311. All nine pages checked; only three
carry marks.

Montana schools must already provide special education from age 3. But ANB —
"average number belonging", the student count that drives state school funding —
excluded children under 5, so a district received no state money for them.

- New 20-9-311(7)(a)(ii), fully underlined: "Preschool children with
  disabilities receiving special education services as required under
  20-7-411(3) may be included in ANB calculations based on the aggregate hours
  of pupil instruction."
- The existing ban in (7)(a) is narrowed rather than removed: "in preschool
  programs created at the discretion of trustees" is inserted, so it now clearly
  covers only discretionary preschool.
- New 20-5-101(3)(b)(iv), underlined, makes being a child with a disability
  entering a special education program an "exceptional circumstance" that lets
  trustees admit a child under 5.
- **The page image mattered.** In 20-9-311(4)(e)(i), "in kindergarten through
  grade 12" is **struck**. The extracted text prints it as live law. Dropping
  that grade band is what lets the one-full-time-pupil cap apply to
  preschoolers.

There is **no dollar figure anywhere in the act** — no appropriation, no
per-pupil amount, no fiscal cap. Money follows through the existing formula, and
the only limits are the hour bands already in subsection (4). Effective 1 July
2025, applying to counts in fiscal years beginning on or after that date.

Area `public_education_quality`, yes vote **for**. The area is defined as
strengthening student outcomes through effective teaching, standards, funding
and accountability, and this is a funding change that reaches a group the
formula had excluded.

## HB 396 — bail

Chapter 651. Amends 46-9-105, 46-9-109 and 46-9-301. Five pages, three with
marks.

Three changes:

- New in 46-9-105: a person arrested on a judicial warrant after being returned
  to Montana from another state under 46-30-411 "may not be admitted to bail
  without first appearing before the judge who issued the warrant, unless that
  judge is unavailable". The act sets no time limit on how quickly that
  appearance must happen.
- Also new there: where an interstate compact or agreement applies, its terms
  and federal law govern.
- New mandatory factors in both 46-9-109(2)(b) and 46-9-301: "the defendant's
  prior history of abscondence and fugitivity, including costs incurred by a
  government entity to transport the defendant to this state". These sit inside
  the existing "shall take into account" list, so they are mandatory, not
  optional.

**No dollar amount, deadline, hour count or list of offences changes.** The
existing offence list in 46-9-109(2)(e) is reprinted unmarked, as is the
"financial ability of the accused" factor in 46-9-301(6). The only other strikes
are the word "and" and renumbered subsection labels.

Area `public_safety_and_crime_control`, yes vote **for**, on the corpus
precedent for pretrial-detention measures, including the federal District of
Columbia Cash Bail Reform Act. The counter-fact is in the description: the
practical effect falls on people brought back from another state, who cannot post
bail until they see the issuing judge.

## HB 492 — caps on parking requirements

Chapter 682. Amends 76-2-304 and 76-25-303. Five substantive pages.

The new subsections are wholly underlined, with nothing struck except a stray
"or" and one renumbering from (5) to (7). A city may no longer require:

- more than **one parking space per residential dwelling unit**;
- **any** minimum for four cases — an existing or vacant building changing use, a
  licensed or registered child-care facility, deed-restricted affordable
  housing, and an assisted living facility;
- more than **half a space** for a residential unit under 1,200 square feet
  under general municipal zoning — and under the Montana Land Use Planning Act,
  **no** minimum at all for such units.

Accessible spaces required by the Americans with Disabilities Act are carved
out. A builder may still provide more parking voluntarily. The general zoning
section carries **no population threshold and no opt-out**; the Land Use
Planning Act section binds municipalities of 5,000 or more in counties of
70,000 or more, plus those that opt in. Effective 1 October 2026.

Area `housing_affordability`, yes vote **for**. The area is defined as
increasing housing supply and reducing cost burdens, and parking minimums are a
direct per-unit construction cost.

One correction worth recording: the read reported chapter 620. The authoritative
source, `api.legmt.gov` for LC0886, gives **chapter 682**. Chapter numbers are
always taken from that endpoint, never from a summary.

## HB 690 — child abuse and neglect definitions

Chapter 714. Amends 41-3-102 and 41-3-205. Twenty-four pages; marks on seven.

41-3-102 prints in two versions, a temporary one and one effective 1 July 2025.
**Every edit falls in the July 2025 version**; the temporary version is
reprinted unchanged.

The same sentence is added in five places — to "abused or neglected", to the
exclusions list in "child abuse or neglect", to "physical neglect", to "physical
or psychological harm to a child", and to "psychological abuse or neglect":

> "The term does not include referring to and raising the child in a manner
> consistent with the child's biological sex, including in the making of related
> mental health or medical decisions."

Nothing is struck anywhere in Section 1 except the word "or" and five subsection
labels replaced by roman numerals. **No standard of proof, deadline or day count
changes.** The effect runs entirely through the definitions: that conduct alone
can no longer support a finding of abuse or neglect.

Section 2 adds two subparagraphs to 41-3-205(4)(b). A member of Congress or the
Legislature inspecting a case file may not pass a parent material the department
has designated **in writing** as attorney-client privileged, and the member's
right to discuss the rest "may not be limited unless the department has provided
the member with a listing of documents" so designated. A member may challenge a
designation in district court, under seal.

There is no effective date, applicability or termination section.

Area `civil_rights`, yes vote **against**, on Montana's own precedent in this
corpus: HB 121 on shared facilities, HB 300 on school sports and facilities,
HB 400 on names and pronouns, HB 471 on lessons about sexual orientation and
gender identity, and HB 682 on gender transition treatment are all scored the
same way.

## HB 810 — no extra fee for how rent is paid

Chapter 768. Amends 70-24-103 and 70-24-201 (the Residential Landlord and Tenant
Act) and 70-33-103 and 70-33-201 (the Mobile Home Lot Rental Act). Seven
substantive pages; marks on three.

The same new subsection is added to both acts, fully underlined:

> "A landlord may not charge an additional fee based on rent payment type except
> to recoup an electronic bank fee incurred for electronic payment."

A new definition lists what "rent payment type" covers: cash, check, electronic,
or other forms agreed in the rental agreement. Everything else is renumbering
and one punctuation fix. **No notice period, day count, deposit amount, deadline
or ground for eviction changes anywhere in the bill.**

Worth noting from the page image: 70-24-201(2)(b), which already allowed rent by
electronic funds transfer, is **not** underlined — that was existing law, not
something this act added.

Area `housing_affordability`, yes vote **for**, on the precedent of measures
limiting what a landlord may charge, such as the rental application fee bill in
the corpus. The act removes a charge landlords could previously set above cost,
leaving a pass-through of an actual bank fee.

## Measures read and set aside

### HB 480 — jury trial in constitutional challenges

Chapter 680. Creates a new section in Title 3, chapter 15, and amends 27-8-302.
Dropped under filter 5.

Where a proceeding challenges the constitutionality or other illegality of a
legislative act or a ballot issue and turns on a disputed fact, "any party to
the proceeding is entitled to a trial by jury on a determination of an issue of
fact that is in dispute". The right runs to either side, including the State.
Before, 27-8-302 was permissive; these cases are decided by a judge in practice.
Nothing is struck except the word "such", replaced by "the". It reaches pending
cases, because the trigger is the date of the jury demand.

The House split 50-49, so the measure is plainly contested. But contested is not
the same as directionally defensible. No area in the catalogue maps to court
procedure: `anti_corruption` is about abuse of public office, `government_efficiency`
about service delivery, and `impartiality` and `legal_competence` are
judicial-candidate areas that may never be used on a legislative record. Rather
than stretch one, it is dropped.

### HB 591 — Celebrate Freedom Week

Chapter 320. Amends 20-1-306. Dropped under filter 5.

It adds Freedom Week, the last full week of September, to the existing list of
commemorative days, with the stated purpose of educating students "about the
sacrifices made for freedom in the founding of the United States". Districts
"shall conduct appropriate exercises", and trustees must adopt a policy covering
all the days on the list.

What it does **not** do is the point. It names no document that must be taught
or read aloud — the Declaration of Independence and the two constitutions appear
only in the WHEREAS preamble, which is not part of the code. It sets no hours,
no grades, no opt-out, no penalty and no money.

And the page image shows the one real change cuts the other way: **"during the
school day" is struck** from 20-1-306(1). The extracted text prints those words
as live law. So the act loosens when the exercises for every day on that list
must happen. There is no defensible direction.

### HB 636 — marijuana edibles

Chapter 500. Amends 16-12-224. Dropped under filter 5.

One line changes: "10" struck, "5" underlined, and "a package" inserted. The THC
in a single serving of an edible falls from 10 mg to 5 mg, and the 100 mg cap is
now explicitly per package — so a package holds up to 20 servings instead of 10.
Nothing else moves: not the 35% flower potency cap, not license fees, not taxes,
not local opt-outs, and not the higher-potency allowance for registered
cardholders. Effective 1 July 2026.

A serving-size cap on one product type does not evidence a stance on any area in
the catalogue.

## Reading level

Measured after writing with the Flesch-Kincaid grade formula. Median **6.9**,
worst **8.2**. The longest sentence anywhere is 29 words, inside the 45-word
ceiling `candidateRecordPlainLanguageLint` enforces, and the lint was clean over
all sixteen descriptions before import. HB 690's and HB 396's first drafts
measured 9.4 and 8.4 and were rewritten by splitting long sentences.

## Which roll, and which text

Each measure contributes its chamber's last kept floor vote, and the
superseded-stage gate accepted all eight with no `acknowledge_later_rolls`
entry, which independently confirms the choice.

## What was checked and found clean

- All five measures became law and carry a session law chapter number, confirmed
  against `api.legmt.gov`: HB 168 chapter 672, HB 396 chapter 651, HB 492
  chapter 682, HB 690 chapter 714, HB 810 chapter 768.
- Every imported roll is divided. The widest margin is HB 492 in the House at
  73-26, which is 36 percent.
- None of the five is a joint resolution.
- Every roll on all eight measures read was compared member by member against
  Montana's own vote record and agrees exactly.
- The import reconciles three ways: the dry run planned 343 rows across 8 rolls
  and wrote 0, the run inserted 343 with no errors, and the database holds 343
  rows at run stamp `2026-09-03T17:15:18.319Z`.
- Montana's jurisdiction total is now 4,238 records across 87 candidates and
  2,479 area tags in 15 research areas, on 97 approved rolls.
