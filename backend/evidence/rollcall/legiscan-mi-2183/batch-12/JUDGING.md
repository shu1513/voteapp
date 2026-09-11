# Michigan batch-12: judging

Not-enacted measures, all Senate votes. Each description comes from the
**engrossed print**, read with the fixed markup reader (batch-06/JUDGING.md),
and the Senate Fiscal Agency analysis used as an index.

## The measures

### SB 757 — addictive feeds
New act. Section 7: no addictive feed unless commercially reasonable methods
show the user is not a minor, or a parent gives verifiable consent. Section 9:
no feed notifications to a covered minor from 10 p.m. to 6 a.m., or from 8 a.m.
to 4 p.m. on weekdays from September 1 to May 31. Section 11: age and consent
data used only for that purpose and deleted immediately. The attorney general
enforces and keeps a complaint website. An "addictive feed" is one that selects
media from information about the user; feeds based only on a user's own choices
are excluded.

### SB 758 — the Kids Code
New act. Default privacy settings at the highest level for a covered minor;
data minimization; no targeted advertising, profiling (with narrow exceptions),
dark patterns, or ads for narcotics, tobacco, gambling and alcohol; the same
notification hours as SB 757; limits on using a minor's data to recommend media.
Civil fines of up to $50,000 per violation begin January 1, 2027. Tie-barred to
SB 759, which is dropped as its enforcement companion.

### SB 760 — AI companion chatbots
New act. Section 5 bars, unless the operator actually knows the user is not a
minor, features that simulate companionship or a relationship, claim to be
human or to feel emotion, or pursue engagement over safety, and outputs that
promote suicide, self-harm, eating disorders or drug use or that encourage
secrecy. Self-identification as an adult is not enough for actual knowledge.
The attorney general may seek $25,000 per violation, with each day a separate
violation.

### SB 134 — consumer protection
Section 4(1)(a) narrows the exemption to a "specific method, act, or practice
that is expressly authorized", and adds that a practice is not exempt merely
because the general transaction is authorized or regulated. The Senate analysis
explains this answers Smith v Globe Life (1999) and Liss v Lewiston Richards
(2007), which read the old exemption to cover any regulated business. Per-
violation fines of up to $25,000, attorney general class actions (the greater of
actual damages or $250 per member), and coverage of nonprofits and small
businesses (under 250 employees or under $6 million in sales).

### SB 360 — data security
New section 11a requires reasonable security procedures with named elements.
The definition of personal information adds government ID numbers, medical and
health insurance information, account credentials and biometric data. Breach
notice "not later than 45 days after the determination of the breach", with
limited delays. New section 20c: attorney general civil actions, including up
to $2,000 for knowingly failing to keep safeguards or to investigate a breach,
and $250 per notice failure up to $750,000.

### SB 451 — medical debt
New act. A consumer reporting agency may not report adverse medical debt
information, except for a credit transaction above the national conforming
loan limit for a one-unit property; medical creditors and collectors may not
report medical debt; private action for actual damages, injunction and fees.

### SB 1041, SB 1042, SB 1043 — price gouging
Three new acts with the same structure. An "excessively increased price" is a
disparity of more than 20% over the pre-emergency or pre-disruption price,
unless explained by the listed exceptions (costs, a prior discount, for SB 1042
a retail markup at or below the retailer's cost, and for lodging a prior
contract or regular seasonal rates). SB 1043's trigger is a
"market disruption" such as weather, supply failure, strike or civil disorder,
and it excludes utility-regulated energy. Attorney general fines of up to
$10,000 per violation for an individual and $500,000 for others.

## Import

Judge: 9 updated. Dry run: 141 planned inserts, 0 errors, stamp
`2026-09-11T07:04:10.214Z`, which matches zero rows. Real run: 9 files imported,
**141 inserts**, 0 notified, stamp `2026-09-11T07:04:12.549Z`, 17 candidates,
83 area tags. Michigan roll-call records went 7,165 to 7,306.

## Duplicates

None. The sweep's tally matches for SB 757, SB 758 and SB 760 were all on other
dates and other bills.

## Review fixes

Three sentences were corrected after PR review, each checked against the
engrossed print:

- SB 451, section 5(2): the exception is any credit transaction above the
  conforming loan limit, not only home loans. "very large home loans" became
  "a loan of any kind larger than the federal conforming mortgage limit".
- SB 1042, section 2(e): a third exception, "a markup at or less than the cost
  to the retailer", was missing. Added. SB 1041 and SB 1043 do not have it.
- SB 757, section 7(2): the two routes are alternatives. The two sentences
  became one: "unless reasonable steps show the user is not a minor or a
  parent gives verified consent".

Judge: 3 updated. `import-dry-run-rerun-report.json`: 47 rewrite, 94
unchanged. `import-rerun-report.json`: **47 rewritten in place, 94
unchanged**, 0 notified, stamp `2026-09-11T19:08:33.601Z`. Michigan roll-call
records stay at 7,306 (no inserts, no retirements).
