# GA batch-02 — judging notes

## Source

Same two sources as batch-01, no AI call: the House Budget & Research Office (HBRO) end-of-session
reports (2025 and 2026, "with Vetoes") for the section-by-section Final Bill Summary, and the
dated bill-text versions on legis.ga.gov reached from the LegiScan bill record.

**New this batch: the HBRO summary is a table of contents, not the Act.** Two of its summaries
would have produced false records:

- **SB 503** — HBRO says the Act "requires insurance companies to cover medically necessary
  orthotic and prosthetic devices." The enrolled text does the opposite where it acts: it revises
  the definition of "health benefit policy" in the *existing* coverage requirement so that the
  exclusion of state employee, teacher, and Board of Regents plans — which had been set to expire
  on January 1, 2027 — **never expires**, and adds Medicaid contracts to the exclusion. (Dropped
  for other reasons too; see PLAN.md.)
- **SB 179** — HBRO says the Act "encourages" schools to facilitate patriotic-society access. The
  enrolled caption says "to require local school systems to allow the representatives of certain
  organizations to speak with students during school hours."

The Ohio rule therefore holds in Georgia: **write from the enrolled text; treat the summary as an
index.**

## Reading Georgia's enrolled PDFs

Georgia prints amendments in place — struck text is crossed out, new text underlined — and
`pdftotext` renders both as plain characters, so an extracted line like

```text
(9) Remit to the Georgia Sheriffs' Association $100.00 of each $200.00 registration fee and remit
to the general fund of the county treasury the remaining $100.00 of each registration fee Enter
into contracts with the governing authority of a county, municipality, or consolidated government
```

is ambiguous: either half could be the deleted one. The resolution used here is to **render that
page of the PDF and look at it** — page 6 of SB 40's enrolled text shows the remittance sentence
underlined and "Enter into contracts…" struck, so the remittance is what the Act does. Anywhere a
description depended on which half survived, the page was read, not inferred.

## Georgia's vehicle-bill problem is systemic

Batch-01 found SB 33 (a hemp caption over a property-tax Act). It is not an outlier. Among this
batch's 44 candidate measures, the LegiScan title and the enacted text disagree in **at least ten**:
SB 503 (rental-marketplace insurance → orthotics), SB 170 (homeless council → rural hospital
generators), HB 297 (off-highway vehicle ad valorem → a new state transit authority), HB 369 (food
truck equipment → nonpartisan election of county officers), HB 413 (mobile sawmills →
transfer-on-death deeds), HB 439 (dealer deductions → local property-tax credit funds), HB 134
(manufactured homes → forestry manufacturing tax credits), SB 179 (transferring-student records →
computer science graduation requirement), HB 1567 and SB 139 (local annexations).

**HB 463 is the reverse case and the reason it is in this batch**: its title is a senior homestead
exemption, and the Act is the state income tax cut. A title-driven selection would have missed the
biggest tax vote of the biennium.

## Version check, roll by roll

Every roll was checked by diffing the text in force on the vote date against the enrolled Act,
normalized for line numbers and page furniture:

- **Identical to the enacted text:** HB 1164 House (committee substitute of 2026-02-26), SB 244
  House (committee substitute of 2025-03-28).
- **Concurrence taken instead of a divided passage vote**, because the passage text is not what
  became law: SB 69 House (the passed substitute lacked the seat-belt evidence section, lacked the
  bank/institutional-investor exclusion, and made financier liability mandatory rather than
  permissive), HB 1185 House (the Senate narrowed the disclosure claim to shareholders and members
  and made a records-inspection order discretionary), HB 463 Senate (the Senate substitute of
  2026-02-09 carried only the rate cut and standard deduction; overtime, tips, retirement income,
  and the credit repeals came later), SB 220 Senate (the 2025 passage was on the 2025 engrossed
  text; the 2026 concurrence is the enacted one).
- **Concurrence taken because the chamber's own passage vote was not divided:** SB 244 Senate
  (55-0), SB 40 House (159-3, and cast before the Title 12 solid-waste section existed), HB 463
  House.

## Stance directions

Direction follows the **research area's description**, never the bill's framing.

- `corporate_accountability` = "Hold companies accountable for legal compliance, consumer
  protection, and public impact." **SB 69 is for**: it puts a class of companies — litigation
  financiers — under registration, disclosure, contract, and liability rules. **HB 1185 is
  against**: it lets a company's own bylaws confine shareholder claims, including records
  inspection, to a single court.
  - *Recorded counter-reading for SB 69*: it is the companion to SB 68, which batch-01 labelled
    `corporate_accountability`/**against**, and its final text carries a seat-belt evidence rule
    that helps defendants. The two labels are not in tension under the area description: SB 68
    limits what injured people can recover from businesses; SB 69 regulates the finance companies.
    The seat-belt provision is named in the descriptions so a reader can weigh it.
- `public_education_quality` names accountability explicitly, so HB 1164's audit committee, risk
  designations, and intervention machinery are **for** — the same reading batch-01 gave SB 472.
- `personal_income_tax_reduction` is literally the personal income tax → HB 463 **for**. The
  descriptions also name the repealed credits and sales-tax exemptions, which are not personal
  income tax, so a reader sees the whole Act.
- `healthcare_affordability` = "improve access to affordable, quality care" → SB 220 **for**
  (more forms, more qualifying conditions, longer registration).
- `public_safety_and_crime_control` names "accountability, and justice system performance" → SB 244
  **for** (compensation for wrongful conviction; fees when a prosecutor is disqualified for
  misconduct) and SB 40 **for** (cash limits aimed at metal and catalytic-converter theft).

Descriptions end **"and became law"**, matching batch-01: LegiScan status 4 records enactment, not
whether the governor signed.

## Import result

Real run on local `voteapp` 2026-08-27: **9 files all `imported`, 0 errors, 1,035 inserts, 0
rewrites, 0 notified, 207 distinct candidates**, reconciled three ways against the dry run:

| check | value |
| --- | --- |
| report `insert` actions | 1,035 |
| `candidate_records` where `origin='rollcall_import'` | 27,897 → 28,932 (+1,035) |
| `origin_run_id LIKE 'rollcall:GA:%:2026-08-28T00:48:23.837Z'` | 1,035 rows / 207 candidates |
| the dry run's own stamp `…T00:48:02.049Z` | **0 rows** — positive proof `--dry-run` is inert |
| batch-01's stamp `2026-08-27T01:03:53.893Z` | still 1,725 — the batches stay separable |

A re-run dry run reports all 1,035 `unchanged`.

**207 of 208 crosswalk-mapped candidates**, the same as batch-01: Speaker Jon Burns casts no
recorded vote.

The dry run flagged **no** related existing records, so nothing was retired by hand this batch.

PROD UNTOUCHED — promotion is a separate step.
