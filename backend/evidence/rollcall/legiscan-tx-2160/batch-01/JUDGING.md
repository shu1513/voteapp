# Batch 01 — judging notes

25 judgments, one per selected roll call, in `judgments.json`. Applied to
the local review queue, then fanned out for real on 2026-08-25
(`import-report.json`; the pre-import plan is `import-dry-run-report.json`).

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

Stance direction follows the AREA DESCRIPTION in `research_areas`, not the
bill's framing. The one that bites: `immigration` reads "Welcome immigration
through a lawful, orderly, and humane system", so an enforcement bill is
**against** — the convention every federal batch and Ohio's S.B. 172 already
follow (Laken Riley, H.R. 2, the sanctuary-city bills: all against). The
first revision of this file had SB 8 as `immigration / for`; that was
direction-inverted and is corrected below. Likewise `national_defense` has
only ever labeled NDAAs — strictly military — so SB 17 does not get it, and
`personal_income_tax_reduction` is literally "Lower personal income tax",
which fits a capital gains foreclosure (SJR 18) but not an estate-tax ban
(HJR 2 stays general).

| measure | label | why |
|---|---|---|
| SJR 18 | `personal_income_tax_reduction` / for | permanently forecloses an individual capital gains tax |
| SB 8 | `immigration` / against | mandates and funds local participation in federal immigration enforcement (Ohio S.B. 172, Laken Riley convention) |
| HJR 34 | `immigration` / against | authorizes a tax break for hosting border security infrastructure (H.R. 2 border-wall convention) |
| SB 33 | `womens_reproductive_rights` / against | bars public money and logistical support for obtaining an abortion |
| SB 15 | `housing_affordability` / for | preempts municipal minimum-lot-size and density floors |
| SB 2972 | `civil_rights` / against | narrows protected expressive activity to students and employees, ends traditional-public-forum status of common outdoor areas, mandates new prohibitions |
| SB 12 | `civil_rights` / against | bars student clubs based on sexual orientation or gender identity, bars instruction on those subjects, bans DEI duties — the Ohio S.B. 1 precedent (`civil_rights` / against for a DEI ban), and the federal convention (H.R. 28, H.R. 3492: against) |
| SB 13 | `civil_rights` / against | mandatory content prohibitions ("indecent", "profane") broader than the prior harmful-material standard, plus an automatic block on student access to any challenged title until the district rules — same restriction-on-access shape as SB 2972 |
| the other 6 measures | `general`, no stance | direction genuinely contested — see below |

**10 of 25 votes carry no stance** (a second pass tightened this from 15:
SB 12, SB 13, and HJR 34 gained stances under the conventions above, and
SB 8's direction was corrected). The ones still `general`:

- **SB 2** (vouchers) — the analysis funds the program separately, caps it
  at $1 billion, and gives districts an extra allotment when a student
  returns. Calling a yes vote "against public education quality" asserts a
  harm the document does not.
- **SB 17** — a national-security reading and a civil-rights reading are
  both available, and the exemption for citizens and lawful permanent
  residents cuts against the second.
- **SB 37** — shifts authority from faculty to appointed boards; accountability
  or loss of academic independence depending on the reader.
- **HJR 2, HJR 4** — narrow tax prohibitions; `personal_income_tax_reduction`
  is literally "Lower personal income tax", and neither an estate-tax ban nor
  a securities-transaction-tax ban is an income tax.
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

## Result

The real run reconciled exactly to the dry run — same file count, same
insert count, no errors either time. The dry run's 1,620 is a *plan*
(`"dryRun": true`, nothing written); only the real run's 1,620 is rows in
the database:

```text
dry run   files 25 | outcomes {"dry_run": 25}  | errors 0 | planned inserts 1,620 | notified 0
real run  files 25 | outcomes {"imported": 25} | errors 0 | inserts         1,620 | notified 0
```

In the local database that is `candidate_records` 60,152 → 61,772, with
1,620 rows across 136 distinct Texas candidates:

```sql
select count(*), count(distinct candidate_id)
  from candidate_records
 where origin_run_id like 'rollcall:TX:%:2160:%:2026-08-25T05:30:09.633Z';
-- 1620 | 136
```

The trailing timestamp is the run's `startedAt`, which the importer stamps
once and shares across every roll in the run
(`importLegiscanRollCallVotes.ts`, `originRunId`). It is what pins this
query to batch-01. **Do not shorten it to `'rollcall:TX:%'` or to the
session** — batch-02 will be session 2160 as well, so both of those will
silently grow to include it and stop reconciling to 1,620. (They match
exactly 1,620 today only because batch-01 is the sole Texas import so far.)

The dry run's own stamp, `2026-08-25T05:18:18.287Z`, matches **zero** rows —
positive proof the dry run wrote nothing.

The review queue still reads 25 `approved` / 6,159 `pending`: the importer
reads the queue, it does not consume it.

Prod is untouched. Promotion is a separate `research:promote` run.

Per measure: SB 2 134, SB 33 131, HJR 98 130, SB 17 129, SB 12 125, SB 8 123,
SB 2972 123, SB 37 122, SB 13 121, SJR 18 120, SB 15 118, HJR 4 116, HJR 2
115, HJR 34 13. (HJR 34 is a senate-only vote, and only 13 senators are on
the Nov-2026 ballot.)

`notified` is 0 because every vote is from 2025, well outside the 30-day
notification window.

## Next

Batch-02, drawn from the 743 divided actions batch-01 left on the table.

The discipline that made this batch safe, for whoever picks up the next one:
the sentences replicate ~120 times each, so they get read before they are
written, not after. Every figure in a description was checked against the
section-by-section analysis, never the sponsor statement above it.

## Plain-language rewrite (2026-08-30)

All 25 yea and nay descriptions were rewritten from this committed evidence.
The judge dry run and real run both passed. The importer then rewrote 1,620
local candidate records with stamp `2026-08-31T06:32:32.572Z`; a final dry run
reported all 1,620 unchanged. The original `import-report.json` remains
unchanged. Prod remains untouched.

The current judge requires explicit acknowledgment when a later same-chamber
floor roll is intentionally left outside this batch. Acknowledged roll pairs:
1483210→1557913, 1578072→1582920, 1550482→1579565, 1580073→1582991,
1571446→1587503, 1578076→1582754, 1523046→1585648, 1522908→1588929,
1579978→1582956, 1523169→1583924, and 1550655→1585606.
