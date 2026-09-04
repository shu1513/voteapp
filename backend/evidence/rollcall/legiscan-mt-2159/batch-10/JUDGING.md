# Montana batch-10 — how each measure was judged

Montana publishes no neutral prose summary of a bill, so the enrolled text is
both the ground truth and the only source. Every judgment rests on the enrolled
bill, plus the official action trail and the session law chapter number from
`api.legmt.gov`.

Every roll was compared member by member against Montana's own vote record. All
agree exactly.

## Reading the marks mechanically

Batch-09 introduced extracting the PDF's underline and strike **line objects**
with `pdfplumber` and matching each to the characters it crosses. This batch used
it on all thirty-three measures, and it changed what is affordable: a bill with
**zero line objects touched no existing law at all**, which can be established in
seconds and stated with confidence rather than hedged.

That single fact cleared several bills. HB 9, HB 10, HB 427, HB 595 and HB 866
have no marks anywhere, so each either only appropriates money or only creates
new sections.

Two further shapes of the strike-through hazard turned up, both recorded in the
campaign checkpoint:

- **A struck token can be identical to its replacement.** HB 714 prints "within
  20 20 working days". The first 20 is struck and the second inserted. The
  deadline did not change. Reading the text file, one would assume a number moved.
- **Twin sections need not change alike.** HB 614 amends 76-2-118 and 76-2-229,
  which are near-identical. Their subsection (1)(c) looks the same in the
  extracted text. In 76-2-118 it is old text merely renumbered; in 76-2-229 the
  whole paragraph is underlined and is new law.

## HB 15 — school funding figures

Chapter 38. Amends 20-9-306 only, the definitions section of the school funding
law. Twenty-four marked spots, all the same shape: the old pair of figures
(fiscal 2024 and 2025) struck, the new pair (2026 and each succeeding year)
underlined.

Twenty-one dollar figures rise, including the high school basic entitlement
($353,787 to $364,401), the elementary basic entitlement ($58,963 to $60,732),
the per-student entitlement ($8,075 to $8,317 for high school), the quality
educator payment ($3,673 to $3,783), the Indian education for all payment and the
American Indian achievement gap payment. Each new figure is 3% above the one
before it, though **the act states no percentage** — that is arithmetic on the
printed numbers, not a figure in the text, and the description says "about 3
percent" rather than asserting a rate the statute does not contain.

No structural element changes: not the ANB thresholds, not the per-ANB
decrements, not the 44.7%, 35.3%, 80%, 140% or 175% percentages. There is no
appropriation. The figures carry "for each succeeding fiscal year" with no
sunset, so they are permanent.

Area `public_education_quality`, yes vote **for**. The area names funding
explicitly.

**Roll selection.** The House third reading of 27 January is one of the seven
rolls held on the LegiScan vote defect: LegiScan reports 78-20 with Randyn Gregg
voting yes, Montana reports 77-21 with him voting no. HB 15 is therefore carried
on its Senate roll alone.

## HB 59 — water well contractors

Chapter 633. Amends 37-43-301, 37-43-302, 37-43-306 and 37-43-309.

The individual licensee bond rises from **$4,000 to $25,000** — a sixfold
increase, appearing twice for the two licence categories — and the firm blanket
bond from **$10,000 to $25,000**. Abandoning or decommissioning a well is added
to the activities requiring a licence. The rule that the licensed contractor must
personally be the individual who contracts on behalf of a firm is struck.

The complaint rule changes materially. Old text let licensees "respond to
complaints and demonstrate or achieve legal compliance prior to disciplinary
action". New text lets them "respond to a complaint, after which the board may
begin disciplinary action under 37-43-310". The chance to cure before discipline
is gone.

Area `corporate_accountability`, yes vote **for**. A bond exists to pay a
customer when the work fails, and removing a cure-first step makes enforcement
quicker. Both go to holding a business to account for its work. The description
states the bond rise and the lost cure step so a reader can weigh the cost to
small rural contractors.

## HB 311 — rental application fees

Chapter 254. Entirely new law, codified into Title 37, chapter 56, part 1. Zero
line objects across all four pages, so nothing existing was touched.

A property manager of four or more dwelling units who charges an application fee
"shall refund the application fee within a reasonable period of time if the
applicant does not become a party to a signed rental agreement". The manager may
keep only "the out-of-pocket expense ... for a specific service ... including but
not limited to a credit check", and only where the applicant "was given written
notice of the portions of the total application fee allocated to each cost at the
time the application fee is collected". Two limits close the obvious gaps: the
manager "may not retain the cost of a service that was not performed, even if the
cost was specified in the written notice", and the definition of cost "does not
include a fee for the property manager's time or effort".

Enforcement is a private action for the amount withheld, with attorney fees
available to either side at the court's discretion, and the burden of proving
services rendered on the manager.

Worth noting what the act does **not** do: it sets no deadline in days, only "a
reasonable period of time"; it caps no fee; and it reaches only managers of four
or more units.

Area `housing_affordability`, yes vote **for**, on near-exact precedent — the
corpus already scores a bill limiting rental application charges to the actual
cost of processing the same way.

## HB 338 — early help widened to maths

Chapter 439. Amends eight sections; only about 101 marks across 26 pages, so
most of the text is reprinted unchanged.

"Literacy" is struck repeatedly and "numeracy", "or math" and "and math"
inserted, extending the targeted early intervention programme from reading to
reading and maths. Singular "an evaluation methodology" becomes plural, so the
board of public education may approve more than one screening tool. Two
restrictions are struck: **"in April, May, or June"**, the window in which the
evaluation could be given, and **"for the subsequent school year"**, which had
limited an eligible child's help to the next year alone.

**No money changes.** The $1,000 per child annual cap appears in two places and
carries no mark, as do the age and grade tests. So the programme covers more
ground from the same pot, and the description says so.

Area `public_education_quality`, yes vote **for**.

## HB 427 — building departments must cite the code

Chapter 483. Entirely new law. Zero line objects, so nothing existing was
touched.

Where the Department of Labor and Industry or a local building department delays
a permit past the standard review period, or stops work for building-code
noncompliance, the applicant or builder may ask in writing or electronically for
"the text or citation of the specific sections of the building code relied on".
The department must supply it **within 7 business days**, and owes **$50 for each
day** it is late. The requester may also sue in district court for relief
including compensatory damages, with reasonable costs and attorney fees to the
prevailing party. Where a project also needs zoning or subdivision approval, the
agency may still require those first.

Area `government_efficiency`, yes vote **for**. The area is defined as improving
service delivery and modernising administrative operations, and this puts a
deadline, a price and a remedy on a government body explaining a decision it has
already made.

## HB 515 — school buildings and technology

Chapter 687. The heaviest bill in the batch: amends ten sections, repeals
20-9-534, adds two free-standing sections, and carries 1,301 marks.

The money:

- The school major maintenance aid formula multiplier rises from **187% to
  355%**.
- The school major maintenance amount rises from **$15,000 to $40,000**, plus
  **$115** per budgeted student instead of **$110**.
- A new **$1 million a year** statutory appropriation for school technology
  grants, allocated by each district's share of the statewide BASE budget.
- A one-time transfer bringing the school facilities fund to **$275 million** by
  15 August 2025.

Structurally, the separate school major maintenance aid account is abolished and
folded into the school facility and technology account, with a spending priority
written in: technology, then major maintenance, then state debt service
assistance. The coal severance trust earnings distribution moves from monthly to
annually on 15 May.

Unchanged and unmarked: the 10-mill local effort limit, the 18%-of-mill-value
floor, the 97%-of-maximum-budget test and the 80% state aid cap.

Area `public_education_quality`, yes vote **for**.

## HB 599 — student data

Chapter 696. Amends 40-6-701 only.

The existing parental right in (2)(k) — to opt a child out of collection that
would feed the statewide data system — is **struck** and replaced by a split
rule. Under new (k)(i) a parent may **opt out** of any analysis, evaluation,
survey or data collection that does **not** require personally identifiable
information. Under new (k)(ii) a parent must **opt in** to any that **does**. The
exception covers the student's own education record and a demographic survey
validating a college admission test.

A new subsection requires parents be told of their right to opt out of physical
and mental health screenings and surveys, and be told of any issues or concerns
arising from one.

Area `data_privacy`, yes vote **for**. Moving identifiable collection from
opt-out to affirmative consent is squarely that area's subject.

## HB 908 — job growth tax credit

Chapter 751. Amends 15-30-2357, 15-30-2361, 15-31-175 and 39-11-404, and repeals
two session-law sections.

Three changes:

- The definition of "qualifying new employee" gains a wholly new alternative:
  "an apprentice as defined in 39-6-101 who is in the construction industry". An
  apprentice qualifying that way need not meet the $50,000 minimum wage, the
  6-month employment test, or the headcount-increase thresholds.
- The whole of 15-30-2361(8) is struck — the bar on claiming this credit and the
  apprenticeship credit in the same tax year. Stacking is now allowed.
- The credit's **termination date of 31 December 2028 is repealed**, so it no
  longer expires.

No rate or dollar figure moves: the credit stays at 50% of estimated FICA taxes,
and the wage floor, employment test, headcount thresholds, 7-year claim limit and
10-year carryover all stand.

Area `government_spending_reduction`, yes vote **against**, on the corpus
precedent for extending a tax credit — Kentucky SB 324, which let unused film tax
credit capacity carry forward, is scored the same way. Deleting a sunset removes
the mechanism by which a legislature re-justifies a subsidy.

## The twenty-five drops

Set out in `../survey/filter-5-drops.md`. The ones worth knowing:

**HB 846** was the hardest call. It creates a new payment from property-rich
school districts to the district actually teaching an "isolated pupil" — one more
than 60 minutes' travel from their home district's school — and sets a $100
petition fee, deadlines, and an advisory council where such pupils are 5% or more
of budgeted ANB. But the same act **repeals the rule that a K-12 district must
agree in writing before territory is transferred**, and cuts the wait before
re-petitioning the same territory from 4 years to 1 fiscal year. A funding-equity
mechanism bundled with a loosening of boundary protections. No single defensible
direction.

**HB 24** was dropped for a different reason: the direction could not be
established. Behind a "clarify" title it deletes the half-time/full-time
kindergarten designation and two rules that gave districts extra funded student
count for opening or expanding a kindergarten programme. Whether that reduces
kindergarten funding or merely removes provisions another 2025 act made redundant
is not determinable from the enrolled text, and guessing is exactly the failure
this campaign guards against.

**HB 13**, the state pay plan, points three ways at once: the raise is **cut**
from $1.50 an hour or 4% to $1.00 or 2.5% against the prior biennium, $138.2
million is appropriated, and legislator session pay is reindexed from $10.33 an
hour to 80% and then 100% of Montana's average hourly wage.

**HB 72** doubles Guard pay for a whole state call-up rather than the first 15
days, which is real, but `national_defense` is defined around military readiness
and deterrence and this is state disaster response.

**SB 33** moves permitting and inspection of state-agency buildings from local
governments to the state, leaving locals a comment right. Who holds the authority
is the whole of it, and that is not a direction in any catalogue area.

## Reading level

Measured after writing with the Flesch-Kincaid grade formula. Median **6.6**,
worst **7.7**. The longest sentence anywhere is 29 words, inside the 45-word
ceiling `candidateRecordPlainLanguageLint` enforces, and the lint was clean over
all twenty-six descriptions before import. First drafts of HB 15, HB 515 and
HB 908 measured 11.0, 8.8 and 9.0 and were rewritten by splitting long sentences
and breaking up lists.

## What was checked and found clean

- All eight measures became law and carry a session law chapter number, confirmed
  against `api.legmt.gov`. HB 492's read in batch-09 reported a chapter number
  that the API contradicted, so every chapter here was taken from the endpoint,
  never from a summary.
- Every imported roll is divided. The widest margin is HB 515 in the House at
  80-20, which is 25 percent — just inside the threshold.
- None of the eight is a joint resolution.
- Every roll on all thirty-three measures read was compared member by member
  against Montana's own vote record and agrees exactly.
- Two coordination instructions were found and chased: HB 846's fired, HB 266's
  did not because SB 258 was vetoed.
- The import reconciles three ways: the dry run planned 462 rows across 13 rolls
  and wrote 0, the run inserted 462 with no errors, and the database holds 462
  rows at run stamp `2026-09-04T20:29:50.249Z`.
- Montana's jurisdiction total is now 4,700 records across 87 candidates and
  2,790 area tags in 18 research areas, on 110 approved rolls.
