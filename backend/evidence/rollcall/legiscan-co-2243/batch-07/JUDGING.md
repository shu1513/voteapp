# Batch-07 judging notes

## HB 1272 does not create the protections its own declaration calls for

The legislative declaration says "establishing statewide enforceable protections
for workers exposed to extreme temperatures is necessary to safeguard the
state's workforce". Reading the operative sections, the act does none of that.
What it requires is:

- by 15 January 2027, the division of labor standards must build a public
  reporting page, pull heat-illness data from the health department's syndromic
  surveillance program, and start collecting workers' compensation claim data
  and health-care data twice a year; and
- by 1 July 2028, publish a **model** temperature-related injury and illness
  prevention plan covering drinking water, cool-down and warm-up areas,
  temperature monitoring, a 14-day acclimatization period, training and
  emergency response, and review it at least every five years.

No employer is required to adopt the model plan. The appropriation is $76,651
and 0.6 of a full-time post. The description says "The act does not set a heat
rule employers must follow" for exactly this reason. This is the same class of
error as HB 1004 in the 2025 session's batch-11, where a description promised a
campaign that existed only in the introduced print.

## SB 131's enrolled text is missing from the feed

Every other measure this session has an `Enrolled` text in the LegiScan bill
record. SB 131 has Introduced, two Engrossed, two Amended and **Chaptered** — no
Enrolled. The chaptered print is the signed act, so it was downloaded directly
from `leg.colorado.gov/bill_files/116926/download` and read. It is kept in the
docs directory as `SB131_chaptered.pdf`.

## SB 121 passed the House by exactly one vote over the constitutional majority

33-32. Colorado's constitution requires 33 of 65 in the House, so this is the
narrowest possible passage. LegiScan's `passed` flag is right here, but the
tally is worth stating because the same flag has been wrong elsewhere this
session — HB 1187 was marked passed at 32-28 when the House needs 33.

**Two Senate repassage rolls on the same day.** On 17 April the Senate recorded
1685197 concurrence 30-5, then 1685198 repassage 20-15, then 1685199 repassage
19-16. All three account for the full 35 seats. The history records a single
"Senate Considered House Amendments - Result was to Concur - Repass". The later
roll is judged as the chamber's final word; the other two are acknowledged as
peers. Same pattern as HB 1312 and SB 190 in batch-05.

## SB 47 changes when a vote may be held, not who wins it

The whole act is two definition edits. "General election" in the firefighter
bargaining statute now also means a **coordinated election** as defined in
1-1-104 (6.5), and the petition threshold is measured against the last regular
municipal or district election rather than a "general" one. The ballot question,
the five percent petition threshold and the consequences of a yes or no vote are
untouched.

## SB 52's preference is bounded

It applies from 1 January 2027, only to a covered business — one constructing or
operating railroads, utilities, energy generation or advanced manufacturing —
operating in a coal transition community, and only to a **qualified** coal
transition worker, meaning one who meets the qualifications for the post. State
and local government are expressly not covered, and the business may hire
someone else where no qualified coal worker applies.

## SB 160 has two unrelated halves

Sections 1 and 2 define personal protective equipment and bar deducting its cost
from wages. The exclusions matter: non-specialty safety-toe footwear and
prescription safety eyewear the employer lets workers take home, employee-
requested metatarsal guards, logging boots, everyday clothing, and weather items
like coats, gloves and sunscreen are all outside the definition. Section 3 is
separate: a meat processing employer with 500 or more Colorado employees may not
unreasonably deny a restroom break during compensable time, on a $100-per-
employee fine capped at $200 per employee per week.

## HB 1207 survives a federal repeal

The act requires EEO-1 data — the federal count of employees by race, ethnicity,
gender and job category as the form stood on 1 March 2026 — in the periodic
report to the secretary of state from 1 July 2027, and says in terms that the
duty stands "even if the federal government repeals or discontinues" the federal
requirement.

## Reconciliation

629 ledger inserts = 629 rows across 55 candidates. 439 rows are yea-side and
there are exactly 439 tags. The convergence run reports all 629 `unchanged`. The
dry run wrote nothing: 3,344 records before it and 3,344 after.

No existing record was flagged as related, and nothing was retired.
