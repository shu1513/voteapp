# Batch-08 judging notes

## Two version-check flags that are not text changes

The 2026 version check asks whether any text-changing action follows the roll a
chamber is judged on. It flagged two:

- HB 1038, Senate roll 13 April 2026 → "2026-04-21 H House Consideration of
  First Conference Committee Report result was to Adopt Committee Report".
- HB 1084, Senate roll 30 April 2026 → "2026-05-07 H House Consideration of
  First Conference Committee Report result was to Adopt Committee Report".

In both cases the Senate adopted the conference report and repassed the bill
under it on its own date, and the House then adopted **the same report**. Both
chambers voted on identical text; the later action changes nothing. The check is
right to surface the pattern and the flags are cleared here on the record.

## HB 1113 dropped — a 69-section election omnibus

The act rewrites 69 sections of title 1, from the definition of a coordinated
election through voter registration by high school liaisons, cancellation of
duplicate registrations, presidential electors, vacancy procedures in every
party form, ballot arrangement, multilingual ballot access, voting machine
adoption and more. Some of it widens access, some tightens administration, and
most is housekeeping. There is no single direction a voter could be told they
agree or disagree with, so it is dropped rather than given one.

## SB 116 dropped — it raises tax on one group and cuts spending for another

Two things happen in the act:

- The **qualified-senior primary residence** subclass, which lowers the taxable
  value of an older owner-occupier's home, ends after the 2026 property tax year.
  Applications stop after 15 July 2026 and the death-matching machinery runs one
  last time in April 2027.
- The state's **reimbursement to counties** for the business personal property
  exemption is wound down: calculations end before the 2027 property tax year,
  reports run through March 2027, warrants through April 2027, and the
  alternative-exemption mechanism repeals on 1 January 2028.

Ending a senior tax break is a tax rise for seniors. Ending the reimbursement is
a general fund saving. Those pull in different directions and the House vote of
39-26 is close. The measure is dropped rather than given a stance it does not
have.

## SB 150 removes ten elected seats

RTD's board is fifteen elected directors. The act apportions **five** director
districts by September 2027 for the November 2028 election, and creates four
board members appointed by the governor and confirmed by the Senate, whose terms
begin 1 January 2029. Directors are limited to two terms and eight total years,
counting service before 2029. Two of the five elected in 2028 draw two-year
terms by lot. From the 2030 census onward the state's independent legislative
redistricting commission draws the director districts.

The description says both halves — fewer elected seats and new appointed ones —
because a reader told only that the act "increases accountability" would be
misled by the title.

## HB 1084 softens a warning; HB 1320 rewrites where it goes

These two acts touch the same statute, 1-40-106 (3)(e). HB 1084 changes "which
will reduce funding" to "which will **likely** reduce funding" in the mandatory
tax-cut ballot title, and widens the fiscal impact statement in the ballot
information booklet. HB 1320 separately allows that same required language to
appear **anywhere** in the ballot title instead of having to begin it, and adds
the accessible-language requirement. Both are recorded as they stand.

## HB 1318's numbers

A school zone is every roadway within 1,000 feet of a school property boundary.
State highways are excluded unless the transportation department designates
them, or a local government does with the department's written approval. An
existing zone extending more than 200 feet is preserved as it is; one under 200
feet must be enlarged to at least 200 feet; a local government may reduce a zone
under subsection (4). Signs must indicate the zone and that penalties and
surcharges double. The act's short title is the "Liam Stewart School Zone Act".

## SB 59's exemptions matter

The bar on holding another elected office starts with the 76th General Assembly
and does not apply to a special district office, to a member with less than a
year left in the other office when sworn into the legislature, to a member with
less than a year left in the legislature when sworn into the other office, or to
a senator mid-term at the start of that first regular session.

## Reconciliation

517 ledger inserts = 517 rows across 55 candidates. 348 rows are yea-side and
there are exactly 348 tags. The convergence run reports all 517 `unchanged`. The
dry run wrote nothing: 3,973 records before it and 3,973 after.

No existing record was flagged as related, and nothing was retired.

## A slug that refuses a stance

The first judge run refused SB 59 with "research_area_slug
'integrity_and_ethics' must not include stance". That area is reserved for
official adverse findings and carries no direction. SB 59 was relabeled
**anti-corruption**, which does take a stance and fits a rule against holding two
elected offices at once.
