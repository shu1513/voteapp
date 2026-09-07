# Batch-14 judging notes

## A mistake made and corrected: Colorado's print sequence follows the chamber of origin

The first version of the 2026 not-enacted text picker assumed a House vote
always wants an "Engrossed" print and a Senate vote always wants an "Amended"
one. That is only true for **House** bills.

Colorado engrosses a bill in the chamber it starts in and revises it in the
second chamber. LegiScan labels the first pair Engrossed and Reengrossed both as
`Engrossed`, and the second pair Revised and Rerevised both as `Amended`. So for
a Senate bill it is the Senate votes that want the Engrossed prints and the
House votes that want the Amended ones.

The first run therefore fetched, for SB 43, SB 48 and SB 87, the print produced
by the **other** chamber. Those files were deleted and refetched under the
corrected rule before anything was read. Nothing was written from a wrong print,
but the error is recorded because it would have been invisible in the output.

## SB 166's Senate day carries two third readings

Rolls 1690323 (22-12) and 1691056 (21-13), both on 28 April 2026, both with the
same description and both accounting for all 35 senators. One senator moved
between them. The judge refused the batch and named the other roll. The later
roll is judged and 1690323 is acknowledged as a peer, matching how HB 1312,
SB 190 and SB 121 were handled earlier this session. That is the fourth
occurrence, so it is a Colorado 2026 pattern rather than a one-off.

## Never read the bill summary block

Every one of these prints opens with a bill summary that says, in its own words,
that it "applies to this bill as introduced and does not reflect any amendments
that may be subsequently adopted". Two of them — SB 66 and SB 166 — have summary
blocks that describe the bill more broadly than the operative text does. Every
description in this batch was written from the sections after "Be it enacted",
not from the summary. This is the rule that HB 1004 broke in the 2025 session.

## HB 1114 and HB 1308 are the same idea from opposite ends

HB 1114 would have capped how large a minimum lot size a jurisdiction may
demand: from 1 October 2031, no more than 2,000 square feet where residential
use is limited to a single family home, and no frontage, setback, open space or
coverage rule with the practical effect of preventing such a home on a
2,000-square-foot lot. HB 1308 would have required administrative approval of a
split of one lot into two from 31 December 2027, subject to conditions: neither
new lot under 1,200 square feet, the smaller at least 30 percent of the
original, the original never split before, residential use allowed, and both new
lots able to be accessed and served by utilities. Both apply only to "subject
jurisdictions" and both exclude exempt lots.

## HB 1327's threshold is 500 supported workers, not 500 employees

A "large employer" is one with 500 or more **supported workers** — workers
receiving state medical assistance — in the preceding calendar year, excluding
workers under 18 and seasonal workers. But the annual reporting duty falls on
every employer that had 500 or more employees in the state at any time in the
preceding year, which is a wider group. The description says the fee falls on
employers with 500 or more such workers.

## SB 62 names four chemicals

Brodifacoum, bromadiolone, difenacoum and difethialone. From 1 July 2027 they
would have been restricted-use pesticides, and distributing or using one against
that restriction would have become an unlawful act and a deceptive trade
practice under the pesticide statute.

## SB 140 exempts, it does not cap

The bill would have removed two categories from the prescription drug
affordability board's reach: drugs designated for a rare disease or condition
under 21 U.S.C. 360bb, and licensed biological products derived from human whole
blood or plasma. Cannabis-derived drugs were already exempt. A yes vote narrows
what the board may review.

## Reconciliation

398 ledger inserts = 398 rows across 55 candidates. 263 rows are yea-side and
there are exactly 263 tags. The convergence run reports all 398 `unchanged`. The
dry run wrote nothing: 6,528 records before it and 6,528 after.

No existing record was flagged as related, and nothing was retired.
