# Maine batch-03 — judging notes

Judged 2026-08-30 from the enacted "Chaptered" text of each Act, with the
committee-amendment SUMMARY as the index and LegiScan's `history[]` trail as
the version check. Descriptions were written in plain English from the start,
at roughly a 7th-grade level, so no rewrite pass is owed on this batch. No AI
provider call.

## Import ledger

- Dry run: 24 files, 0 errors, **1,512 planned inserts**, 0 notified.
- Real run: 24 files, 0 errors, **1,512 inserts**, 0 notified, **131
  candidates** (every candidate the crosswalk maps except Aaron Dana, the
  tribal representative, who casts no recorded vote).
- Stamp `rollcall:ME:<chamber>:2181:<roll>:2026-08-30T06:46:17.231Z`.
- Maine total: 2,908 → **4,420 records**. Convergence dry run: all 1,512
  `unchanged`. The dry run's own stamp (`2026-08-30T06:45:58.660Z`) matches **zero** rows.
- **890 area tags** for 1,512 records — yea voters only, plus LD 2051's second
  label.

## Version checks

Every roll was checked for an amendment adopted by the SAME chamber after the
roll, the hazard the LD 1016 review surfaced. Three measures had one, and none
of the three changes what the descriptions say:

- **LD 54 and LD 493** sat on the Special Appropriations Table for almost a
  year. On 2026-04-13 the House receded and adopted a House amendment to the
  committee amendment in each — but both amendments only replace the
  appropriations and allocations section, except that LD 493's also moves the
  landlord well-testing start date from 2026 to 2027. **The LD 493 description
  therefore does not state that date**, so it is true of the version each roll
  was cast on and of the Act. LD 493's Senate roll is the 2026 enactment
  (RC #956), which is on the final text.
- **LD 166** took a Senate amendment on the last day (S-448, removing a
  cigarette stamp discount) after both chambers' report votes, so the rolls
  taken here are the House's recede-and-concur (RC #590) and the Senate's
  enactment (RC #650), both on the enacted text.

**LD 913 is the batch's near-miss.** Senate Amendment "A" (S-351) would have
struck the resale price cap from the committee amendment — and it **FAILED**
11-20 (RC #465). The cap survived, so the description states it: a resale
marketplace may not charge more than 10 percent of the original ticket price.
Reading the amendment list without the history would have inverted this.

**One same-day pair needed `acknowledge_later_rolls`:** LD 589 house 1584033
acknowledges 1583726. The House accepted the report (RC #290, 74-72) and
enacted the bill (RC #292, 74-73) on the same day; RC #292 is the later and
decisive one.

## Reading the enacted text, not the title

- **LD 1871's title undersells it.** "Permit Sealing Criminal History Record
  Information of Victims of Sex Trafficking" describes Sec. 10; Sec. 1 enacts
  a whole new chapter regulating **business screening services** — the
  background-check industry — requiring complete, updated records and
  correction or deletion of wrong or sealed ones. Both strands point the same
  way, so the measure keeps one label, but the description names both.
- **LD 1900's title in the worklist is truncated.** The Act covers **two**
  parities: tribal power districts for all four Wabanaki nations, and child
  support licence-suspension enforcement for the Penobscot Nation and Maliseet
  tribal courts. Both are described.
- **LD 166 is a civil violation, not a crime** — up to $2,000 a day, each day a
  separate offence. Checked the penalty section before writing, per the LD 1126
  lesson from the plain-language review.
- **LD 163's enacted text came from the committee's MINORITY report.** It drops
  the pharmacist standing-order billing route and requires the buyer to
  purchase at the pharmacy counter, which the description states.
- **LD 2051** is written against a moving federal baseline: it covers people who
  were receiving federal SNAP on July 3, 2025 and lost it under Public Law
  119-21. The description says "a federal law passed that year" rather than
  naming a nickname the statute does not use.

## Labels

`nay: null` on all 26 labels, consistent with batches 01-02 and the PA
precedent: for each of these the realistic objection runs on a different axis
from the area being scored (cost for LD 2051 and LD 589, employer burden for
LD 54, business burden for LD 913, local enforcement priorities for LD 670).

**LD 2051 is the campaign's first Maine measure with two labels.** Its single
provision — state-funded food assistance for households that cannot get federal
SNAP because of immigration status — is simultaneously a safety-net expansion
(`social_programs_and_welfare`) and an immigrant-inclusion measure
(`immigration`, whose area description is about a humane system). Flattening it
to one would lose half of what the vote says; the FL SB 700 multi-label pattern
applies.

## Flags

Two `related` flags, both on Barbara Bagshaw and both false positives from the
same-date scan: a hand-researched record about **LD 702** (January 6 day of
remembrance) that shares 2025-05-13 with the LD 54 and LD 163 House rolls. No
duplicates, no retirements, 0 ambiguous.

PROD UNTOUCHED — Maine's 4,420 records are still local-only.

## Review fixes, 2026-08-31

Two findings on the descriptions, both real, both accepted:

- **LD 589 (P1)** — the descriptions said the farm-worker minimum wage
  "is $14.65 an hour" starting January 1, 2026. Wrong on the enacted text
  alone: §652 sets $14.65 as a BASE "increased by the increase, if any, in the
  cost of living" (CPI-W, August 2024 to August 2025) — so the wage that took
  effect was never $14.65. Maine DOL announced the adjusted figure: **$15.10**
  (CPI-W +3.1%). The descriptions now state the base, the mechanism, and the
  $15.10 result.
- **LD 2051 (P2)** — the descriptions presented lawful presence as sufficient
  and the July-2025 federal-cutoff group as a separate coverage class. The
  enacted §3104-A(1-A)(B) requires BOTH a qualifying status under subparagraph
  (1) AND a hardship condition under subparagraph (2) — elderly or disabled, a
  victim of domestic violence or trafficking, or unable to work/unemployed
  while obtaining work documentation. The July-2025 group is item (h) INSIDE
  the status list, so the hardship requirement applies to them too. The
  descriptions now carry the two-part test.

253 records rewritten in place (127 LD 589 + 126 LD 2051), rows unchanged at
1,512, convergence all unchanged. Rewrite ledger stamp
`2026-08-31T05:18:30.338Z`.

A third review claim was **refuted with evidence**: an automated reviewer said
`ls-me-house-2181-roll1594847.json` holds 150 vote entries against a 151
header. The file holds exactly 151 (71 Yea + 68 Nay + 12 Absent), and a sweep
of all 24 batch files shows len(votes) == total, per-position counts matching
the header, and no duplicate people_ids in every one. The evidence is
sha-pinned to the LegiScan element, so it could not be hand-edited even if the
claim were true.
