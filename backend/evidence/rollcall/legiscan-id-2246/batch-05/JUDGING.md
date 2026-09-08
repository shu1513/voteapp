# Idaho 2026 batch-05 — how each measure was judged

Same method as the 2025 batches. The act is the only source, read with
`tools/id_text.py`, and the engrossed print is read wherever a measure was
amended. One body per measure, with the yes and no descriptions generated from
it.

## House Bill 508 — chapter 299, bike lanes and sidewalks

Corrected in review; the first reading had the act backwards. The rule that a
bicycle or pedestrian facility may be improved only as a secondary benefit of a
highway project, or for safety near a school or park, already existed. It came
from Senate Bill 1144 of 2025, which this campaign dropped as unrecognizable
without reading it closely, so the marked text here was read as if the whole
rule were new. The act adds a third case, work paid for by federal funds or a
grant, and keeps the two existing ones. It widens what may be built. Public
infrastructure, a yes vote is for. Re-judged and re-imported in place.

## House Bill 583 — chapter 22, short-term rentals

Extends the existing ban on local prohibition to any type of short-term rental,
keeps public health and safety as the grounds for local rules while dropping
general welfare and neighborhood character, and adds a new limit that a local
rule may not treat a short-term rental differently from an ordinary house. Also
limits the tax duties that may be placed on a rental marketplace. Corrected in
review: the first description said only public health survived, missing the
reinserted "and safety". Housing affordability, a
yes vote is against, because it removes local tools for protecting housing stock
in residential neighborhoods.

## House Bill 752 — chapter 263, restrooms and changing rooms

Makes knowingly entering a restroom or changing room designated for the opposite
biological sex a misdemeanor punishable by up to a year in jail, and a felony
punishable by up to five years on a second conviction within five years. The
statute lists exceptions for maintenance, medical and law enforcement help,
disasters, coaching, accompanying a person who needs help, single-user rooms,
and dire need. Civil rights, a yes vote is against.

## House Bill 776 — chapter 276, newborn safety review

Requires the department to verify a listed risk factor within twelve hours of a
mandatory report about a child under one, and, if confirmed, to open its highest
priority response and complete a written safety assessment. The risk factors are
a central registry entry within ten years, a conviction for injury to a child, an
earlier termination of parental rights, or a baby born with neonatal abstinence
syndrome. Social programs and welfare, a yes vote is for.

## House Bill 930 — chapter 337, campaign treasurers

Read carefully, because the caption suggested the opposite of what the act does.
The old paragraph requiring detailed accounts within seven days is struck, but
the same duty is re-enacted in a new paragraph alongside real additions: a
dedicated checking account, all receipts deposited into it, no commingling with
other money, investment limited to cash-equivalent accounts rather than stocks,
and candidate loans over one thousand dollars moved into the campaign account.
The act strengthens the rules. Anti-corruption, a yes vote is for.

## Senate Bill 1288 — chapter 234, high-needs student fund

Creates a state fund reimbursing districts and independent charter schools for
special education costs above thirty thousand dollars a year per student, after
Medicaid and other funding. The statute lists what counts and excludes ordinary
classroom costs, supplies and standard transport. Public education quality, a
yes vote is for.

## Senate Bill 1352 — chapter 264, starter home subdivisions

Bars cities with more than ten thousand residents from banning compact
single-family subdivisions from residential zones and requires each of them to
rewrite its plan and zoning by February 2027,
with caps on the minimum lot size, setbacks, lot widths, lot depths and fees a
city may impose. Historic districts are exempt. Housing affordability, a yes vote
is for.

## Senate Bill 1354 — chapter 265, accessory dwelling units

Bars cities with more than ten thousand residents from banning accessory
dwelling units from residential zones and requires each of them to allow one
internal or one detached unit per single-family lot by February 2027, with limits on parking requirements and fees. Historic
districts are exempt. Housing affordability, a yes vote is for.

## Checks run

- Repository plain-language lint over all 32 descriptions: 0 warnings.
- Flesch-Kincaid grade measured separately: median 11.8, worst 12.5.
- British spellings checked against an explicit word list: none.
- `", The "` appears in no description.
- Four `Rules Suspended:` tallies confirmed against Idaho's bill pages.

## Reconciliation

| source | records |
| --- | --- |
| `import-report.json` inserts | 712 |
| Idaho roll-call rows added to `candidate_records` | 712 |
| crosswalk-matched members summed over the 16 rolls | 712 |

16 rolls imported, 0 errors. Idaho now holds 2,792 records across 91 candidates.

## Review round

Four of the eight measures were corrected after the pull request review and
re-imported in place: House Bill 508's direction and reading, House Bill 583's
wording on the surviving grounds for local rules, and the population threshold
that both housing acts carry in their final subsection and the first descriptions
omitted. Senate Bill 1352 and Senate Bill 1354 apply only to cities above ten
thousand residents; the descriptions had said every city. 358 records rewritten,
354 unchanged. Ledger: `import-review-fixes-report.json`.