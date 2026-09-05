# Montana batch-11 — how each measure was judged

Montana publishes no neutral prose summary of a bill, so the enrolled text is
both the ground truth and the only source. Every judgment rests on the enrolled
bill, plus the official action trail and the session law chapter number from
`api.legmt.gov`.

Every roll was compared member by member against Montana's own vote record. All
nineteen agree exactly.

## A process note that belongs at the top

Two of the reading briefs written for this batch paired bill numbers with the
titles of **other** bills in the same group. The worklist held the right pairs;
they were mis-typed when the brief was written by hand. Both errors were caught
because the reading was keyed to the bill number and the enrolled text was
checked against its own header, so the wrong titles never reached a description.

The fix is now a standing rule in the campaign checkpoint: **bill and title lists
are generated from the worklist file, never retyped.** The batch was then read
from the enrolled PDFs directly rather than from any summary of them.

## Reading the marks mechanically

As in batches 09 and 10, underline and strike **line objects** were extracted
with `pdfplumber` and matched to the characters they cross. A bill with zero line
objects touched no existing law.

That fact decided two drops on its own: SB 55 and SB 61 both have **zero** marks,
so neither amends anything. Both are statements of legislative intent about
measuring streamflow.

## HB 74 — private fish ponds

Chapter 39. The change that matters is one word: 87-4-603(1) read that an owner
"may apply" for a fish pond license, and "may" is struck with "shall" underlined.
A license is now required rather than optional.

The fees follow: the application fee is struck at $10 and set at $600, the
renewal fee struck at $10 and set at $250. A license runs 10 years (87-4-606(1));
only a licensee who sells fish or eggs renews annually (87-4-606(3)(b)), so the
renewal fee is not a yearly cost for an ordinary owner. The surety bond required before
selling fish, eggs or fry is struck at $500 and set at $2,500. Recordkeeping,
site inspections and disease testing are added, and paddlefish caviar sales are
repealed. Direction **for** `environment_and_public_health`: mandatory licensing,
inspection and disease testing of stocked waters.

## HB 197 — workers' compensation

Chapter 112. Amends 39-71-609 and adds a new subsection (3). The existing rule
gives a claimant 14 days' written notice before benefits stop. The new subsection
says that when a claimant is released to full duty at or before maximum medical
improvement, temporary total disability benefits may end "as of the time the
claimant returned to work or after 14 days' written notice, whichever is
earlier."

"Whichever is earlier" is the whole of it: the insurer may stop paying from the
return-to-work date instead of after the notice period. Direction **against**
`social_programs_and_welfare`, because it shortens the period a hurt worker is
paid.

## HB 239 — construction contractors

Chapter 644. Section 1 states the purpose as protecting "the public health,
safety, and welfare of the public through the regulation of construction
contractors", and sections 1 to 8 build a licensing program with licensing
standards, unprofessional-conduct grounds, fines and penalties.

The fee change was checked against the marks rather than assumed. In 39-9-206 the
words "construction contractors and h" are struck from "shall charge fees to
construction contractors and home inspectors", and the whole $70 cap — both the
initial and the renewal figure — is struck. What remains of that section covers
home inspectors only. Contractor license fees are set by the department under
section 1 and must cover the full cost of the program.

So the accurate statement is that the $70 cap is deleted and replaced by
department-set fees, not that fees were merely raised. Direction **for**
`corporate_accountability`.

## HB 254 — professional employer organizations

Chapter 125. Amends 39-8-202. The marks show the fingerprint requirement struck
in full: the duty on an applicant and its controlling persons to submit
fingerprints for checks by the Montana Department of Justice and the FBI, the
rule limiting who may receive the report, and the duty to destroy the fingerprint
card afterwards. Direction **against** `corporate_accountability`: vetting is
removed from a licensing regime.

## HB 270 — remedies under the environmental policy act

Chapter 246. Three changes, all confirmed in the marked text.

A remand power is added at 75-1-201(5)(c)(ii): if the court finds noncompliance,
it "may remand the matter to the agency to correct the noncompliance."

The bar on relief is widened. "vacatur" is inserted twice, and "may not enjoin"
becomes "may not enjoin, void, nullify, revoke, modify, or suspend". So cancelling
a permit now faces the same test as an injunction: the challenger must be more
likely than not to prevail, must show irreparable harm, and the relief must be in
the public interest, with the court directed to weigh "the implications of the
relief on the local and state economy" and to ensure "that the project or as much
of the project as possible can go forward".

Section 9 states: "[This act] applies to all decisions pending but not decided by
a court and cases filed on or after [the effective date of this act]." The act
reaches lawsuits already under way, and the description says so.

**The coordination instruction is load-bearing.** Section 8 provides that if
HB 285, SB 221 and HB 270 all pass and all amend 75-1-201, then every bill's own
amending section is void and 75-1-201 reads as printed inside section 8. All
three were signed. The marked text confirms the merged version carries the same
remedy language quoted above, which is why it can be described as the law.

Direction **against** `environment_and_public_health`, matching HB 285 in
batch-04 and HB 664 in batch-03.

## HB 342 — medical malpractice

Chapter 263. A two-sentence act. New law: "In medical malpractice actions, the
foreseeability of risks or of a specific risk does not change or heighten the duty
owed beyond the reasonable standard of care applicable to the medical provider."

Section 4 applies it to actions filed on or after the effective date and says it
"is intended to clarify any court ruling to the contrary" — so it is aimed at
existing case law. Direction **against** `corporate_accountability`: it narrows
what a patient may argue.

## HB 344 — drugged driving

Chapter 264. Adds 61-8-1002(1)(f). A driver commits the offense if, "without a
valid prescription", a blood test shows an amount at or above a listed figure.

The table was read off the enrolled text rather than summarized: amphetamine 20,
cocaine 20, cocaine metabolite 20, heroin 1, morphine 20, 6-monoacetyl morphine
1, lysergic acid diethylamide 0.1, methamphetamine 20, phencyclidine 5, fentanyl
0.5 — nine listed substances carrying ten limits, all in nanograms per milliliter,
because the heroin metabolite entry splits in two. The description says "nine
substances with ten blood limits" for that reason.

Reaching the limit is the offense, so impairment need not be separately proved.
Direction **for** `public_safety_and_crime_control` as a **detection** standard;
see the note below on why that direction is available here and not for a
sentencing bill.

## HB 466 — categorical exclusions

Chapter 297. New section 1 lets each agency identify actions meeting the new
definition of a "categorical exclusion" in 75-1-220(3), and those actions are
"exempt from the provisions of Title 75, chapter 1, parts 1 and 2." Anything
qualifying under the federal equivalent is exempt too. Subsection (2) creates a
rebuttable presumption that no extraordinary circumstances exist. Section 3
exempts the Department of Administration for state building work, and section 2
requires agencies to count and report their use of exclusions.

**Section 4 is void.** The coordination instruction at section 8 voids it if
HB 346 also passed; HB 346 was signed on 5 May 2025. Section 4 was the Department
of Commerce historic-preservation exemption, so it never took effect and the
records do not mention it. Direction **against**
`environment_and_public_health`.

## HB 467 — oral fluid testing

Chapter 298. Amends 61-8-806, 61-8-1001, 61-8-1002 and 61-8-1016. "oral fluid" is
inserted throughout the implied-consent scheme, for commercial operators in
61-8-806 and for drivers generally in 61-8-1016. The officer may designate which
test to administer.

The description says "oral fluid is saliva, taken with a mouth swab" because the
statute's own term would not tell a reader what is being collected. Direction
**for** `public_safety_and_crime_control` as a detection standard.

## HB 575 — public defender for a parent

Chapter 690. Amends 47-1-104 and adds subsection (4)(a)(xi): "for a parent in a
proceeding to involuntarily terminate the parent's parental rights pursuant to
42-2-607".

Two details decided the wording. It sits under subsection **(4)(a)**, the branch
conditioned on a finding of indigence under 47-1-111, so the description says the
parent must first be found unable to afford a lawyer. And 42-2-607 is in the
adoption title, so this is a termination brought against the parent's wishes.
The description does not claim parents previously had no route to counsel:
In re Adoption of A.W.S. (2014 MT 198) already required appointed counsel for
indigent parents in Adoption Act terminations on equal-protection grounds, and
courts were ordering the public defender office to take those cases. The act
writes that duty into 47-1-104. Direction **for** `civil_rights`.

The act's title also mentions assigning a public defender to a treatment court
team. No operative text for that was located in the enrolled bill, so it is **not
described**.

## SB 48 — publicising a complaint about a judge

Chapter 544. Amends 3-1-1106 and 3-1-1123. One sentence is added: "A citizen has
the right to make public the citizen's complaints concerning a judicial officer
at any time." 3-1-1123 is amended so the commission may respond once the matter
becomes public "through the citizen complainant".

The two WHEREAS clauses state the reason: a citizen "should not be forced to
choose between filing a complaint against a judicial officer and the citizen's
right of free speech". Direction **for** `anti_corruption`.

## SB 168 — lakeshore protection

Chapter 362. **Zero line objects in the whole PDF**, so it amends nothing; it is
entirely new law codified into Title 75, chapter 7, part 2.

It sets a 3-year limitation period for an action against a lakefront owner or
their contractor over a boat ramp, boat house, dock, pier, wharf, retaining wall,
road or similar structure, running from completion of construction or issuance of
the permit, whichever is later. The limit does not apply where the structure
causes documented material harm to lakeshore stability, water quality or aquatic
life, materially interferes with navigation or recreation, or is a documented
public nuisance. Minor work is exempt from review, "minor" meaning under $10,000
over a 5-year period excluding unpaid labor — but not when it involves
significant excavation, dredging, in-fill, diminishment of aquatic life, or
interference with navigation or recreation (75-7-218(2)(a)).

Direction **against** `environment_and_public_health`: it shortens the window for
enforcement and removes a class of work from review.

## Why seven crime bills were dropped

Stated in full in `../survey/filter-5-drops.md`, and summarized here because it
governs the two bills that were kept.

The `public_safety_and_crime_control` area is not an axis where harsher is always
"for". Records already imported by this campaign tag both a violence-intervention
bill and a bill **ending** automatic life sentences as "for" that area. A bill
whose only content is a longer sentence, a wider detention power, or a new way of
counting a prior conviction therefore has no defensible direction and fails filter
5. HB 415, HB 535, HB 578, HB 582, HB 612, SB 19 and HB 626 were dropped on that
basis.

HB 344 and HB 467 were kept because they change how impairment is detected and
proved, which does carry a defensible direction.
