# Washington batch-01 — how each measure was judged

Source for every measure: the **Final Bill Report** on leg.wa.gov, headed "Synopsis as
Enacted" and carrying the chapter number, plus the **session law** text. The report is
written by non-partisan legislative staff and says so on its face, and it carries **no
sponsor statement of intent**, so the Texas advocacy hazard does not arise here.

The report is the delta index — authoritative on WHAT CHANGED. Qualifications were taken
from the session law itself.

## ⭐ Washington marks deletions in ((double parentheses)) and they survive extraction

    (a) The purchaser ((provides proof of completion of a recognized
    ((purchaser)) applicant shall ((sign and deliver to the dealer an
    (i) ((His or her)) The applicant's full name, residential

So a plain `pdftotext` dump shows exactly what the act REMOVED, with no page rendering.
This is unlike Georgia, Maine, Montana, Kentucky and North Dakota, where the same
information needs a render or a font check.

**⚠ The other half is NOT free.** New language is marked by UNDERLINING, and that mark is
lost. So the flattened text cannot tell new law from existing law being reprinted. Any
claim about what an act ADDS must come from the bill report's summary or from a check
against the prior statute, never from the flattened text alone. No description in this
batch rests on unmarked text being new.

## ⭐ A third tally oracle

Every Final Bill Report ends with "Votes on Final Passage", listing the whole sequence of
votes with tallies and naming which chamber amended and which concurred. That confirmed
the roll selection for all five measures independently of the feed and of the bill
history. House Bill 1217's entry is the one that earns its keep: it shows the House
refusing to concur and a conference committee producing the enacted version.

## Per measure

### House Bill 1052 — hate crime, `civil_rights` yes = for, no = null

The enacted section reads that a person commits the offence when they "maliciously and
intentionally commit[] one of the following acts **in whole or in part** because of their
perception of another person's race, color, religion, ancestry, national origin, gender,
sexual orientation, gender expression or identity, or **mental, physical, or sensory
disability**". The report confirms "in whole or in part" is the change.

Two details were taken from the statute rather than the summary. The listed acts are
assault, property damage and threats, so the description names them instead of saying
"acts". And the protected trait is "mental, physical, or sensory disability", not plain
"disability" — a first draft had shortened it, which is the shortened-legal-list failure
this campaign has hit repeatedly.

Nay is null: the realistic objection is to the reach of a criminal statute, which runs on
a different axis from civil rights.

### House Bill 1155 — noncompete agreements, `corporate_accountability` yes = for, no = null

Every noncompete is void, including agreements signed before the act, and an employer may
not enforce one, threaten to enforce one, or tell a worker they are still bound. Employers
must make reasonable efforts to notify current and former workers by 1 October 2027, and a
worker may sue for damages and fees.

The carve-out is carried because it changes who is covered: an agreement to repay
out-of-pocket education costs is not a noncompete only if it meets all three of the
act's conditions (§3): it ends within 18 months of the worker's start date, repayment is
prorated to the time left in those 18 months, and the debt is waived when the worker
leaves for good cause under RCW 50.20.050. The description names all three.

`corporate_accountability` rather than a labour area, because **the campaign has no labour
research area** and the standing workaround is that a mandate running against employers
belongs here. That fits unusually well in this case: every operative provision binds the
employer.

**⚠ The act takes effect on 30 June 2027**, so the description says "takes effect", not
"now bars".

### House Bill 1163 — permit to buy a firearm, `gun_control` yes = for, **no = against**

A dealer may not transfer a firearm until the buyer shows a valid permit. The applicant
gives the State Patrol a full set of fingerprints and proof of a certified firearms safety
course within five years, or proof of exemption. Permits last five years, and must be
refused to someone barred from possession, someone under a firearms court order, or
someone with an outstanding warrant for a disqualifying offence.

**This is the one measure in the batch with an authored nay stance.** It meets the test
used in Connecticut and the federal set: the act is single-subject, its whole operative
content IS the research area's own mechanism, which the area description names, and the
mainstream objection is to that mechanism rather than to cost or administration. The other
four take `nay: null`.

**⚠ The act takes effect on 1 May 2027**, so the description says "will require".

### House Bill 1217 — rent increase limit, `housing_affordability` yes = for, no = null

Rent increases are capped in any 12-month period at the lesser of 7 percent plus inflation
or 10 percent under the residential landlord-tenant act, and at 5 percent for manufactured
home lots. No increase is allowed in the first year of a tenancy, and a landlord may set
any rent once a tenant has moved out.

The exemptions are carried because they decide who the law reaches: buildings first
occupied within the past 12 years, public housing and regulated affordable housing, and
certain owner-occupied homes where the owner rents no more than two units. Only the
residential cap (§101) expires on 1 July 2040; the 5 percent manufactured-home-lot cap
(§201) has no sunset, and the description keeps the two apart.

Nay is null on the Connecticut test: the strongest argument against a rent cap is that it
discourages investment and so reduces supply, and that argument sits INSIDE
`housing_affordability` rather than on another axis. The same reasoning was applied to
Oregon's housing measures.

### House Bill 1604 — jail searches, `civil_rights` yes = for, no = null

Local jails must adopt search policies for transgender and intersex people that meet the
federal Prison Rape Elimination Act, by 1 September 2026. A search may not be done "for
the sole purpose" of determining genital status. Cross-gender searches are allowed only in
exigent circumstances, and the act says explicitly that being short of trained female
staff is NOT an exigent circumstance — a qualification the description keeps, because
without it the rule reads as absolute.

Nay is null: the objection is operational, about staffing and jail security.

## Duplicates

56 hand-written records describing these exact votes were retired before the import; the
manifest is `retirements.json` and the reasoning is in PLAN.md. **Re-run that manifest at
promotion time**, because production has not seen these retirements.

## What was checked and what was not

- Every roll's tally matches Washington's own bill history and the Final Bill Report.
- Every roll is its chamber's vote on the text that became law.
- No measure in this batch is partially vetoed.
- **Not checked:** member-level sides. Washington's history gives tallies, not names, so a
  swapped pair of members with a correct total would be invisible to the audit. Spot-check
  against Washington's own roll call pages if a future batch has reason to doubt one.
