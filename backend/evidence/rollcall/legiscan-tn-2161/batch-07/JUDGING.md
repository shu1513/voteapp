# Judging notes, Tennessee batch-07

Seven roll calls on seven measures, all enacted. Every chaptered act was
downloaded through the LegiScan API with byte length and MD5 verified, extracted
and read. Scope `--scope-from 2026-08-01`.

## SB 229, and what Tennessee's roll numbering does not tell you

SB 229 moved through both chambers and a conference committee in a single day,
22 April 2025, the last day of the session. The order from the bill's own
history is: the Senate passed it as amended 28-3; the House adopted its own
amendment and passed 73-21; the Senate refused to concur and refused to recede;
a conference committee was appointed; the Senate adopted the conference report
23-6; and the House adopted the conference report 78-13.

Two consequences.

First, **the House's divided roll is not on the enacted text.** It is on the
House's own amended version, which the Senate then rejected. The House vote on
the conference report that became law was 78-13, which is not divided. So SB 229
carries a Senate record and no House record, the same outcome as SB 159 in
batch-02.

Second, **Tennessee roll ids are not chronological, even inside one chamber.**
The Senate's conference report vote is roll 1556906 and its earlier passage vote
is roll 1556907 — the later action has the lower id. The supersession gate,
which orders by date and then id, therefore flagged the passage vote as though
it came after. It did not. The bill history settles the order, and roll 1556907
is listed under `acknowledge_later_rolls` on that basis. Anyone reading a gate
warning on a Tennessee sine-die bill should check the history rather than the id.

## The other six

**HB 1886** is imported on its House concurrence roll, a vote on the final text.
The other five each have a single divided roll on the text that became law. No
vehicle-bill trap in any of the seven.

## Label reasoning

Every label uses `nay: null`.

- **SB 690**, **SB 2087**, **SB 2441**, **HB 1886** — `public_education_quality`,
  for. Access to school sports for virtual school students where the athletic
  association already allows it; a duty on the principal to notify the parent of
  every student in a classroom that had to be cleared for violent or severely
  disruptive behavior; mandatory state intervention, up to closure, for a
  virtual school on the priority list or three consecutive years of growth far
  below expectations; and a required internet acceptable use policy with safety
  instruction built into the curriculum.
- **HB 1784**, `womens_reproductive_rights`, against. The act is symbolic — it
  designates a day — and it is imported anyway because the text states its own
  position: 22 January is observed "to renew its commitment each year to protect
  the sanctity of life, and to honor the lives of those unborn children lost to
  legalized abortion." That is a stance a member took on the record, and 22
  January is the anniversary of Roe v. Wade. The description quotes the purpose
  rather than characterizing it.
- **SB 799**, `election_integrity`, for. Statewide parties must nominate by
  primary election for every partisan office on the August or November ballot,
  with a grandfather for counties that used another method in 2022 or 2024. More
  offices are settled by voters in a primary rather than by party committee.
- **SB 229**, `anti_corruption`, for. Political campaign committees pay a $150
  annual registration fee to help fund the registry of election finance that
  regulates them, with candidates expressly exempt so as not to deter people
  from running, and a candidate in a multi-county local election must file
  treasurer certification with each county's election commission.

## Descriptions

Each cites its own roll call's tally. Plain-language lint: 16 descriptions, 0
warnings, median Flesch-Kincaid grade 8.2, worst 10.2.

## Duplicates

0 found.
