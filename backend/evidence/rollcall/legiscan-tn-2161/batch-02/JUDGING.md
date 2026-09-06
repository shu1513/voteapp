# Judging notes, Tennessee batch-02

Ten roll calls on eight measures, every one enacted with a public chapter
number.

## What each chamber voted

Tennessee's LegiScan record carries only two text types, `Draft` and
`Chaptered`. There is no engrossed or committee-substitute print, so the version
check here is simply whether the roll came before or after the act took its
final shape, read against the chaptered act. All eight chaptered acts were
downloaded through the LegiScan API with byte length and MD5 verified against
the dataset manifest, extracted to text, and read.

No vehicle-bill trap in these eight: each chaptered act is on the subject its
`description` and short title name. Batch-01 caught one (SB 1603), so the check
was run on every measure.

Byte size was not used as a proxy for content anywhere. The chaptered PDFs run
from 44 KB to 3.4 MB, and the character counts run from 824 to 141,618 — the
two do not track each other.

## The scope-from mistake, and the fix

The first import of this batch was run with `--scope-from 2026-11-01`, the
pipeline default. That is wrong for Tennessee. This state's crosswalk is built
with `--scope-from 2026-08-01` because the Tennessee primary is 6 August 2026,
which is the operator's explicit call recorded in `../README.md`.

The error was visible in the report: both Senate rolls fanned out to **zero**
candidates, against 14 and 16 on the correct scope, and every House roll was
short by roughly eight. 557 records were written instead of 646.

Fixed by deleting all 557 records and their area tags by run stamp, deleting the
reports for runs that no longer existed, and re-running the dry run and the live
import once each on the correct scope. The batch's reports now describe exactly
what is in the database. Final reconciliation: report 646, run-stamp predicate
646, table delta 646.

## Supersession

No roll in this batch tripped the gate. SB 468 has two divided House rolls, the
2025 passage at 72-22 and the 2026 conference report at 71-18; the ladder keeps
the conference report, which is the final action and the text that became law.
HB 754 has the same shape and is deferred, below.

## Deferred for an unfinished version check

- **HB 910** — the LegiScan `description` says it schedules the Tennessee Human
  Rights Commission for termination on 1 July 2025 and moves its discrimination
  duties to the attorney general. The chaptered act rewrites Title 4, Chapter 21,
  Part 1 and still contains commission appointment terms and grand-division
  rules, which does not read like an abolition. One of the two is describing a
  different version. A separate remaining measure, SB 2364, refers to "the
  defunct human rights commission", so something did abolish it, but that does
  not establish what HB 910 itself did. Not imported until the act is read
  through.
- **HB 754** — gender clinics, three divided rolls. Deferred with HB 910 rather
  than rushed into this batch.

Both are substantive and both are likely keepers for batch-03.

## Label reasoning

Every label uses `nay: null`.

- **HB 622**, **SB 468**, **SB 2222** — `civil_rights`, yea against. HB 622 bars
  local governments from considering race, ethnicity, sex or age in employment
  decisions. SB 468 fixes the statutory meaning of sex and separates single-sex
  spaces on that basis in prisons, juvenile detention, public colleges and
  domestic violence shelters. SB 2222 makes whoever paid a demonstrator
  vicariously liable for damage if the demonstrator's conduct meets the elements
  of rioting, disorderly conduct, obstructing a highway, harassment and similar
  offenses, which reaches organized protest.
- **SB 449**, `womens_reproductive_rights`, yea **for**. The act states that
  Tennessee law does not prohibit contraception or fertility treatment and
  acknowledges the right to provide and to use them. Its own subsection (c)
  says it creates no entitlement to the services or to funding or coverage, and
  the description says so too rather than overstating the act.
- **SB 2412**, `womens_reproductive_rights`, yea against. It lets the attorney
  general sue to enforce the abortion-inducing drug protocol, at $10,000 per
  knowing violation, $1,000,000 where a court finds serious bodily injury or
  death, and each person supplied counted separately.
- **HB 2219** and **SB 6002**, `immigration`, yea against. The area's direction
  is pro-immigration, as the existing federal records show. HB 2219 compels
  every sheriff into a federal 287(g) agreement without county approval and lets
  the state withhold funds otherwise; SB 6002 creates a state immigration
  enforcement division under a governor-appointed chief officer with its own
  grant fund.
- **SB 694**, `cost_of_living_reduction`, yea against. It raises the maximum
  effective interest rate on an industrial loan and thrift company loan of $100
  or more from 30 to 36 percent a year, and a separate cap from 10 to 12.5
  percent.

## Descriptions

Each cites its own roll call's tally. The first drafts came in at a median
Flesch-Kincaid grade of 10.0 and were rewritten in plainer words; the imported
text is 20 descriptions, 0 warnings, median grade 7.6, worst 9.3.

## Duplicates

Swept the 99 candidates who received records for any non-roll-call record on the
same measure and date. 0 found.
