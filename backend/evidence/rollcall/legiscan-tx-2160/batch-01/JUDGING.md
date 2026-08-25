# Batch 01 — judging notes

25 judgments, one per selected roll call, in `judgments.json`. Applied to
the local review queue; the fan-out has **not** been run for real yet, only
`--dry-run` (report in `import-dry-run-report.json`).

## Grounding

Every judgment is written from the official Texas Legislature bill analysis
for the version that passed, fetched from `capitol.texas.gov/tlodocs/89R/analysis/`
via the `state_link` in the LegiScan bill record. No judgment was written
without one, matching the Ohio rule (LSC Final Analysis) and the federal
rule (CRS summary).

### The Texas-specific hazard: the sponsor statement is not the analysis

This is the difference from Ohio and worth carrying to every LegiScan state.
A Texas analysis **opens with "AUTHOR'S / SPONSOR'S STATEMENT OF INTENT"**,
which is advocacy written by the bill's sponsor. The neutral content is the
"SECTION BY SECTION ANALYSIS" below it. Reading the top of the document and
stopping would import the sponsor's framing — and, worse, the sponsor's
numbers, which repeatedly **do not match the enacted text**:

| measure | sponsor statement says | enacted section-by-section says |
|---|---|---|
| SB 15 | applies to cities over **90,000**; lots as small as **1,400 sq ft**; **31.1** units/acre | cities over **150,000**; **3,000 sq ft**; no density figure |
| SB 2 | **$10,000** per student, **$11,500** with a disability | **85 percent** of average state and local per-student funding; no such dollar figures |
| SB 8 | mandate applies to counties over **100,000** | no population threshold at all — every jail-operating county |
| SB 12 | "Class A misdemeanor" penalties; a funding-loss mechanism | employee discipline only; **no** funding loss anywhere in the enrolled bill |
| SB 37 | a **60**-member senate cap; a **25**-day resolution period | neither figure appears |
| SB 2972 | flat bans on masks and on protests in the last two weeks | intent and effect qualifiers; the two-week rule covers specified conduct |

SB 15 is the clearest trap: the widely-repeated 90,000/1,400 figures are the
sponsor's, and the law says 150,000/3,000. A description built from them
would have been wrong on ~118 candidates.

`judgments.json` is scanned for every one of these figures; none appear.

### Stale captions

SB 12's older caption still says the bill concerns "the loss of funding for
public schools that fail to comply." The enrolled bill contains no funding
penalty, and adds two subjects the caption omits entirely — social
transitioning and student clubs. Judged from the analysis, not the caption,
per the Ohio vehicle-bill rule.

## Labels

Only `general` and `integrity_and_ethics` may carry no stance; every other
research area requires `for` or `against`. The gate is Ohio's: a research
area must fit **without inventing a direction the analysis does not take**.

| measure | label | why |
|---|---|---|
| SJR 18 | `personal_income_tax_reduction` / for | permanently forecloses an individual capital gains tax |
| SB 8 | `immigration` / for | mandates and funds local participation in federal immigration enforcement |
| SB 33 | `womens_reproductive_rights` / against | bars public money and logistical support for obtaining an abortion |
| SB 15 | `housing_affordability` / for | preempts municipal minimum-lot-size and density floors |
| SB 2972 | `civil_rights` / against | narrows protected expressive activity to students and employees, ends traditional-public-forum status of common outdoor areas, mandates new prohibitions |
| the other 9 measures | `general`, no stance | direction genuinely contested — see below |

**15 of 25 votes carry no stance.** That is high, and deliberate. The
contested ones:

- **SB 2** (vouchers) — the analysis funds the program separately, caps it
  at $1 billion, and gives districts an extra allotment when a student
  returns. Calling a yes vote "against public education quality" asserts a
  harm the document does not.
- **SB 12** — genuinely mixed: it expands parental access and consent rights
  while restricting DEI duties, instruction on sexual orientation or gender
  identity, and certain student clubs. Ohio's "mixed-direction" category.
- **SB 13** — strengthened parental control of library collections, or
  restricted student access; the document does not settle which.
- **SB 17** — a national-security reading and a civil-rights reading are
  both available, and the exemption for citizens and lawful permanent
  residents cuts against the second.
- **SB 37** — shifts authority from faculty to appointed boards; accountability
  or loss of academic independence depending on the reader.
- **HJR 2, HJR 4, HJR 34** — narrow tax prohibitions and one property-tax
  exemption authorization; no area's direction follows from the text.
- **HJR 98** — bundles fiscal restraint, limiting federal power, and term
  limits into a procedural request.

SB 2972 is the one call where the direction is in the operative text even
though the policy merit is contested: the bill's net effect is a restriction
on who may speak on campus and when. That is the same shape as Ohio's S.B. 1
(`civil_rights` / against).

## HJR 98

Judged as what it is: an application asking the U.S. Congress to call an
Article V convention. Both descriptions say "It is a request to Congress and
was not placed before Texas voters," and neither uses the phrase
"constitutional amendment." Describing it as a ballot amendment would have
put a false sentence on roughly 103 candidates.

## Dry-run result

```text
files 25 | outcomes {"dry_run": 25} | errors 0
inserts 1,620 | notified 0
```

Per measure: SB 2 134, SB 33 131, HJR 98 130, SB 17 129, SB 12 125, SB 8 123,
SB 2972 123, SB 37 122, SB 13 121, SJR 18 120, SB 15 118, HJR 4 116, HJR 2
115, HJR 34 13. (HJR 34 is a senate-only vote, and only 13 senators are on
the Nov-2026 ballot.)

`notified` is 0 because every vote is from 2025, well outside the 30-day
notification window.

## Next

Run the import for real once the descriptions have been reviewed. The
sentences are the thing that replicates ~120 times, so they get read before
they are written, not after.
