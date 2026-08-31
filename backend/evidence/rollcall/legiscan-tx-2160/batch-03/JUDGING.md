# Batch 03 — judging notes

12 judgments, one per selected roll call, in `judgments.json`. Applied to the
local review queue, then fanned out (`import-report.json`; the pre-import plan
is `import-dry-run-report.json`).

## Grounding

Four of the eight measures are Senate bills, judged from the Senate Research
Center analysis of the **enrolled** version
(`capitol.texas.gov/tlodocs/89R/analysis/html/SB…F.htm`).

The four House bills have **no enrolled analysis** — Texas Legislature Online
publishes House-bill analyses only through the engrossed version — so each was
judged from the **enrolled bill text**
(`…/billtext/html/HB…F.htm`). Batch 02 established this; nothing here
contradicted it.

Two enrolled-text readings changed a sentence that the caption alone would
have gotten wrong:

| measure | what the caption implies | what the statute says |
|---|---|---|
| HB 5033 | Texas ends its vehicle emissions inspection program | the agencies "are not required to implement or enforce" it **only if** Congress repeals or amends the federal Clean Air Act mandate, or a constitutional amendment lets states bar such programs. Until then the program stands, and the description says so |
| HB 1586 | a new immunization exemption | eligibility is unchanged; the bill changes only how the affidavit form is obtained — downloadable, mailed on request, no information may be demanded, and the anti-copying seal is gone |

## Version checks

Batch 02's SB 379 defect — describing the enacted text to members who voted on
a different version — was checked for on every roll here, via the TLO
Amendments page (`BillLookup/Amendments.aspx?LegSess=89R&Bill=…`).

| measure | finding | effect on the descriptions |
|---|---|---|
| SB 14 | all three House floor amendments **tabled** | House voted the enacted text |
| HB 5033, SB 1036, SB 2835 | "Bill does not have any amendments" | one text throughout |
| HB 1586 | all five House floor amendments **failed** | House voted the enacted text |
| HB 121 | the selected roll is the House **concurrence** in Senate amendments | that vote is on the final text |
| HB 223 | one adopted Senate floor amendment (Middleton, 5/26) | engrossed and enrolled texts compared directly: the enrolled version adds only "directly or indirectly" before "influence." Same substance, so one body for both chambers |
| SB 2024 | Senate passed 4/23 with one floor amendment; House added five and a conference report followed | **the two chambers get different sentences.** See below |

### SB 2024, the one split

The Senate voted the engrossed text on 4/23. Its senate description says what
that text barred — cartoon and celebrity containers, food-lookalike images,
disguised shapes, products made **in China**, and products containing
cannabinoids, alcohol, kratom, kava, or mushrooms — and then names what the
enacted law added: other countries designated foreign adversaries, tianeptine,
and a Class A misdemeanor penalty. The House voted the conference report on
6/01, so its description is the enacted text.

## The Texas hazard, again

Every figure and category in these descriptions came from the SECTION BY
SECTION ANALYSIS or the enrolled bill text, never from the AUTHOR'S /
SPONSOR'S STATEMENT OF INTENT above it. One example of the gap this batch
found: SB 1957's statement of intent describes disqualification for crimes of
moral turpitude; the statute disqualifies only for a felony conviction or
felony deferred adjudication. (SB 1957 was dropped for other reasons.)

## Labels

Only `general` and `integrity_and_ethics` may carry no stance; every other
research area requires `for` or `against`, and the direction follows the AREA
DESCRIPTION in `research_areas`, not the bill's framing.

**All eight measures carry a stance. None is `general`** — this batch's
selection filter required it.

| measure | label | why this direction |
|---|---|---|
| HB 223 | `anti_corruption` / for | area is "prevent abuse of public office through transparency…"; forcing city lobbying contracts through competitive procurement is transparency over how public money buys influence |
| SB 14 | `government_efficiency` / for | area is "improve service delivery, reduce waste, and modernize administrative operations"; the bill creates the Texas Regulatory Efficiency Office, mandates plain language and a searchable rule database |
| HB 121 | `public_safety_and_crime_control` / for | armed officer coverage, threat assessment procedure, and dedicated safety funding |
| HB 1586 | `environment_and_public_health` / against | area is "protect… community health through standards, enforcement, and prevention"; easing access to the exemption affidavit cuts against prevention, even though eligibility is unchanged |
| SB 2024 | `environment_and_public_health` / for | restricts youth-targeted and adulterated e-cigarette products |
| HB 5033 | `environment_and_public_health` / against | the trigger is contingent, but the vote is a vote to end emissions inspections when the federal mandate lifts |
| SB 2835 | `housing_affordability` / for | area is "increase housing supply"; single-stair authorization makes small apartment buildings buildable on lots that two-stair rules rule out |
| SB 1036 | `corporate_accountability` / for | area is "hold companies accountable for legal compliance, consumer protection"; registration, a five-business-day cancellation right, a deception ban, and refund orders |

## The run

```text
judge dry run  12 judgments | outcomes {"dry_run": 12}
judge real run 12 judgments | all 12 rows now review_status = approved
import dry run files 12 | outcomes {"dry_run": 12}  | errors 0 | planned inserts 967 | notified 0
import real run files 12 | outcomes {"imported": 12} | errors 0 | inserts         967 | notified 0
```

`candidate_records` went 63,513 → 64,480, a delta of 967 — the report's insert
count exactly, with no updates or rewrites.

## Provenance

The batch is identified by its `origin_run_id` timestamp. Batch 02 carries two
stamps because its review fixes re-ran the importer, which re-stamps
`origin_run_id` with the rewriting run's `startedAt`; batch 03 carries one so
far. Session alone cannot separate batches — every Texas batch is session
2160.

```sql
select right(origin_run_id, 24) as stamp, count(*), count(distinct candidate_id)
  from candidate_records
 where origin_run_id like 'rollcall:TX:%:2160:%'
 group by 1 order by 1;
-- 2026-08-25T05:30:09.633Z | 1620 | 136   batch 01
-- 2026-08-25T18:06:23.818Z | 1380 | 135   batch 02, initial import
-- 2026-08-25T18:38:27.616Z |  361 | 135   batch 02, review rewrites
-- 2026-08-25T19:13:13.818Z |  967 | 135   batch 03
```

135 candidates, not 136: Dustin Burrows, Speaker of the Texas House, casts no
recorded vote on ordinary House rolls, and batch 03 has no roll he voted on.
This is the same explanation batch 02 recorded.

Texas total after this batch: **4,328 candidate records, local only.** Prod is
untouched.

## Plain-language rewrite (2026-08-30)

All 12 yea and nay descriptions were rewritten from committed evidence. Roll
1556014 remains pending because its stored SB 2024 tally and voter list conflict
with the already-committed Texas Senate Journal evidence; its 13 retired records
were not revived. The other 11 judgments passed dry and real judging.

The importer rewrote 954 local candidate records with stamp
`2026-08-31T06:35:02.565Z`; a final dry run reported all 954 unchanged and one
not-approved roll. The original `import-report.json` remains unchanged. Prod
remains untouched.

Later-roll acknowledgments required by the current judge are:
1567009→1582159, 1568356→1580234, and 1539641→1538969.

A final jargon-definition pass rewrote 113 SB 2835 records at
`2026-08-31T07:26:46.791Z`; the next dry run reported all 954 unchanged.
