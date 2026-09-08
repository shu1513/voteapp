# Idaho 2025 batch-01 — how each measure was judged

Idaho publishes no neutral legislative analysis. Every bill carries one
supplement, a combined Statement of Purpose and Fiscal Note written by the
sponsor, which is advocacy. The act itself is the only source, read with
`tools/id_text.py` so that struck and underlined text stay apart. Plain
`pdftotext` flattens them and can invert an act; House Bill 294 in this batch is
the proof (CODE-FINDINGS §1).

Each measure has one body of text. The yes and no descriptions are generated
from that body by a script, so the two cannot drift apart.

## House Bill 7 — chapter 7

Possession of three ounces of marijuana or less was already a misdemeanor. The
act adds a minimum fine of three hundred dollars for a person eighteen or older,
on top of the penalties the law already allows. More than three ounces stays a
felony. Public safety and crime control, a yes vote is for.

## House Bill 135 — chapter 275

Removes exemptions from Idaho's lawful-presence check. Immunizations, testing
and treatment of communicable diseases, prenatal care, postnatal care in the
first year, and food assistance for a child under eighteen all lose their
exemption. The community-services exemption narrows to short-term shelters.
Emergency medical treatment stays exempt and a federal-law exemption is added.
Two strands, so two labels: immigration, a yes vote is for; social programs and
welfare, a yes vote is against.

## House Bill 245 — chapter 142

Raises extended foster care, continued care and transitional living from age
twenty-one to twenty-three. Lets the department set separate licensing standards
for relatives. Ends the health board's rulemaking duty for relative foster care
and voids the current licensing rules. Social programs and welfare, a yes vote
is for.

## House Bill 253 — chapter 298

Splits public records rules by residency. Three working days to answer a
resident, thirty days for anyone else. The free first two hours and first
hundred pages, and the public-interest fee waiver, now reach residents only. An
agency may set its own fee schedule for people who are not residents. Requesters
must declare residency under oath. Anti-corruption, a yes vote is against,
because the act reduces public access to government records.

## House Bill 271 — chapter 252

New misdemeanor for willfully publishing a commercial advertisement in Idaho for
a product or service that is illegal where it is offered. Five hundred dollars
per violation, each day a separate offense, fines split ninety-ten between the
county sheriff and the district court fund. Public safety and crime control, a
yes vote is for.

## House Bill 290 — chapter 174

Moves the school and daycare vaccine lists out of health board rules and into
statute, voids the department's immunization rules, and ends the childhood
immunization policy commission. Existing medical and religious or other-grounds
exemptions are preserved. A new right lets an adult student exempt themselves at
any school, including a college. The act also adds daycare exclusion, department
inspections and yearly school reporting.

This one cuts both ways and the direction was argued rather than assumed. The
new enforcement provisions tighten daycare compliance. The dominant effect is
still deregulation: the health board loses the power to set the required vaccine
list, the existing rules are voided, and the opt-out right reaches adult students
at every institution in the state, private ones included. Environment and public
health, a yes vote is against. The description names both sides so a reader can
weigh them.

## House Bill 294 — chapter 144

Read flat, this act appears to cap pipeline safety penalties at two thousand
dollars a day with a two hundred thousand dollar ceiling. Both of those figures
are struck. The act deletes the caps and substitutes the federal maximums under
49 CFR 190.223, which are far higher. A description written from flattened text
would have said the opposite of what the legislature did. Corporate
accountability, a yes vote is for.

## House Bill 398 — chapter 280

Repeals the lobbying sections of the campaign finance law and creates a lobbying
disclosure chapter in Title 74, the open-government title. Widens lobbying to
cover indirect lobbying, meaning paid campaigns urging the public to contact
lawmakers, and to cover executive-branch contacts about rulemaking, rate setting,
buying, contracts, bids and bond issues. Anti-corruption, a yes vote is for. The
description names the indirect-lobbying expansion, which is the part critics
object to, so the reader sees it.

## Senate Bill 1180 — chapter 316

Limits automated license plate reader data to felony, misdemeanor and accident
investigations and to missing or endangered person searches, and bars its use for
ordinary traffic tickets. Requires access controls, logging, twice-yearly audits
and user training, with discipline or prosecution for misuse. Also requires a
front plate only where the vehicle has a bracket. Data privacy, a yes vote is
for. The act does authorize the readers, and the description says so.

## Senate Bill 1183 — chapter 249

Requires wildfire mitigation plans, then gives a utility that reasonably followed
an approved plan a rebuttable presumption that it was not negligent, makes those
suits the only civil remedy, and lets a utility enter land after thirty days of
silence with immunity short of willful or reckless substantial damage. The
operative core is liability protection. Corporate accountability, a yes vote is
against.

## Checks run

- Repository plain-language lint over all 40 descriptions: 0 warnings.
- Flesch-Kincaid grade measured separately: median 10.4, worst 11.9.
- British spellings checked against an explicit word list, not a suffix rule: none.
- `", The "` appears in no description.
- Tallies on the four `Rules Suspended:` rolls confirmed against
  legislature.idaho.gov before selection.

## Reconciliation

Three ways, all 872.

| source | records |
| --- | --- |
| `import-report.json` inserts | 872 |
| `candidate_records` rows with an Idaho roll-call run id | 872 |
| crosswalk-matched members summed over the 20 rolls | 872 |

20 rolls imported, 0 errors, 88 distinct candidates. Fan-out ran 57 in the House
and 30 in the Senate, as the crosswalk validation predicted.
