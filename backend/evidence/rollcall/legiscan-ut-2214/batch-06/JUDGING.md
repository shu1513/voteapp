# Utah 2026 batch-06 — how each measure was judged

The first batch from Utah's 2026 General Session (LegiScan session 2214).
Sources: the Office of Legislative Research and General Counsel's summary as an
index, and the enrolled text, read through `ut_text.py` with added and deleted
text marked, as ground truth.

The 2026 session is the latest in the data, so there is no later session to
check for a repeal. Instead each act was checked against the other 2026 acts
that amend the same code sections, because two bills can amend one section in
the same session.

## HB 337 Nicotine Product Tax Amendments

Effective July 1, 2026. The cigarette tax becomes a flat 11 cents per
cigarette, replacing 8.5 cents (9.963 cents for heavier cigarettes), so a pack
of 20 goes from $1.70 to $2.20, with an inventory tax on stamped stock. The tax
on electronic cigarette products and nontherapeutic nicotine devices rises from
0.56 to 0.71 times the manufacturer's sales price. Nicotine pouches are taxed at
$1 per package plus 5 cents for each pouch beyond 20; other alternative nicotine
products move from $1.83 per ounce to 0.73 times the manufacturer's price.
Revenue above $15.9 million a year from the nicotine taxes goes to the General
Fund. `environment_and_public_health` / for.

HB 265 (Non-nicotine Inhalation Product Amendments) also amends 59-14-804 and
reprints the old 0.56 rate as unchanged text. It changes only the list of taxed
products and who pays, not the rate, and has no coordination clause with
HB 337, so the two merge and HB 337's rate stands.

## SB 69 School Device Revisions

Moves Utah's student device ban from "classroom hours" (scheduled instruction
only) to "school hours", from the first bell to the last and including lunch,
recess and passing time. A school may still set exceptions, and must adopt a
policy letting a parent request brief use in a designated area during
non-instructional time. Effective July 1, 2026. `public_education_quality` /
for.

## SB 252 Water Usage at State-owned Facilities Amendments

A state agency must select low-water turf grass when replacing or installing
lawn, unless the Division of Water Resources exempts it for slope, erosion or
stoniness; a landscape irrigation system installed on or after May 6, 2026 must
reach at least 75 percent distribution uniformity; and the Division of
Facilities Construction and Management must audit state facilities and document
the repairs needed to reach 75 percent. `environment_and_public_health` / for.

SB 46 (Water Wise Landscaping Amendments) amends the same section, 63A-5b-1108,
in the same session, with no coordination clause. Its changes are to the turf
definitions, the limit on non-functional turf at new facilities, and the
daytime spray ban; SB 252's changes are new items in the list of required
practices and a new audit subsection. The edits do not collide, so both stand.

## HB 296 Water Commitment Amendments

One change: "the commitment of available water to uses on the Great Salt Lake"
is added to the list of measures a water conservation plan under 73-10-32 may
include. A one-line act, but the subject is one of the most recognized in the
state. `environment_and_public_health` / for.

## HB 330 Liability Limitations Amendments

Creates an affirmative defense in any civil action when the conduct, omission or
condition that caused the harm was authorized or required, at the time, by a
statute, ordinance, administrative rule, permit, license, order, or other
written instrument with the force of law. The defense fails only if the plaintiff
proves the issuing government entity itself determined that the defendant
materially failed to comply, exceeded the authorization, or procured it by
fraud. It does not apply to product liability actions, creates no cause of
action, and states that it does not reduce the duty of reasonable care.

The record carries the act's own statement about reasonable care, because a
reader should see the limit the Legislature wrote in. The net effect is still
that a permit holder sued for harm has a new defense that turns on the
permit, not on the harm. `corporate_accountability` / against.

## SB 68 Disability Litigation Amendments

Creates a cause of action for a Utah resident or business sued over website
accessibility under the Americans with Disabilities Act, against the filing
party, if the suit was abusive: its primary purpose was to obtain money rather
than to remedy the violation. Abuse is presumed if the defendant tried in good
faith to cure within 30 days of a sufficiently detailed notice, or cured within
90 days. Remedies are fees for both suits, punitive damages, and sanctions, the
last two capped at three times the fees. The attorney general may sue on a
defendant's behalf. The part is repealed July 1, 2031.

Supporters describe serial, form-letter suits by out-of-state filers, and the
act's factors (many similar suits, a distant venue, an out-of-state filer) aim
there. But the mechanism is a new financial risk for anyone who sues to enforce
website access, triggered by a cure that can come after the suit is filed.
`civil_rights` / against.

## SB 292 Autonomous Systems Amendments

In an action relating to an automated-driving-system vehicle, noneconomic
damages are capped at $1 million except for wrongful death, and punitive damages
are barred for level four and five systems. A new no-fault claim lets a person
hurt by an engaged level four or five system sue the registered owner or the
dispatcher without proving negligence or defect, but total recovery is capped at
$100,000 and that recovery is the exclusive remedy. Manufacturers and developers
of those systems cannot be sued in negligence; a design defect exists only if a
feasible safer design existed and the system, at scale, causes more injuries
than human drivers doing the same task; and a state-of-the-art defense bars
liability if the developer met the knowledge of the time or the system reduces
injuries in the aggregate. These liability provisions are repealed July 1, 2030.
`corporate_accountability` / against.

## SB 287 Targeted Advertising Tax — dropped

An annual tax, from 2027, on large companies whose business is mostly targeted
advertising, at the state sales tax rate on their apportioned Utah receipts,
with the money restricted to children's programs. It is a revenue measure aimed
at one industry, not a rule about company conduct, consumer protection or data
collection, so no research area scores it either way.

## Why every no side is untagged

On each measure the no side mixes objections that are not the opposite of the
area: tax policy on HB 337, local control on SB 69 and SB 252, drafting on
HB 296, and tort-reform arguments on HB 330, SB 68 and SB 292.
