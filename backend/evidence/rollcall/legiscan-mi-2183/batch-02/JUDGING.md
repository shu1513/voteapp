# Michigan batch-02 — judging

Sources were the Michigan Public Act text and the nonpartisan House or Senate
Fiscal Agency analysis of the enrolled version. No description was written from
a bill title, a sponsor statement, or a LegiScan summary.

## The version check

Six of the thirteen rolls are concurrence votes, where the Senate accepted the
House substitute. Michigan gives a concurrence vote no question at all in the
vote description, so the question was read from the bill-history line
immediately before the roll-call line: SB 577, SB 578, SB 579 and SB 723 on
`House Substitute (h-1) Concurred In`, SB 579 on `(h-3)` and SB 903 on `(h-2)`,
and SB 966 on `(h-1)`. Each description says "agreed to the House version" so a
reader knows which text was voted.

The other seven are passage votes that no later version replaced.

## The measures

### The 2025 road funding package — HB 4183 and SB 578
HB 4183 raises the motor fuel tax from 31 cents a gallon to at least 51 cents
from January 2026. SB 578 creates the Neighborhood Roads Fund and the
Infrastructure Projects Authority Fund, which are where the package's money
goes. Both are `public_infrastructure` with a yes vote for, on the same reasoning
used for HB 4951 in batch-01: a fuel tax could be filed under a tax area where a
yes would read as against taxpayers, but on the topic of paying for roads the
direction is clean.

The package's other members were dropped: HB 4180, HB 4181 and HB 4182 lost their
House slots to filter 4, and HB 4961 and HB 4968 are tax and reporting machinery.

### HB 4187 — environment_and_public_health, a yes vote is against
This is the one measure in the batch where the direction runs against the vote's
own package. The act cuts the Renew Michigan Fund earmark from $69.0 million a
year to $52.0 million for 2026-27 and $56.0 million for the two years after,
restoring $69.0 million from 2029-30. It also moves the shares between the fund's
three uses: environmental cleanup falls from 65% to 64%, waste management rises
from 13% to 17%, and recycling falls from 22% to 19%.

Less money for cleanup and recycling for three years is a clear direction on
environment and public health, so a yes vote is against. The description states
the reduction and the restoration together, because leaving out the restoration
would overstate it.

### HB 5055 — public_safety_and_crime_control, a yes vote is for
The act lets a House or Senate sergeant at arms holding a police commission
enforce state law anywhere in Michigan, but only to guard a lawmaker, their
family or staff, to guard an event a lawmaker attends, or to investigate threats
or crimes against a lawmaker, and only when the Senate majority leader or the
House speaker asks. Investigations must coordinate with other agencies as far as
practicable.

**The other reading was considered and rejected.** A police force answerable to
legislative leaders rather than to the executive is a real accountability
concern, and it is very likely why 16 senators voted no. It does not make the
direction dishonest, because the act's own text confines the power to protecting
people from threats and crimes and gates every use behind a request from a
presiding officer. On the topic of public safety the direction is clean. The
description carries every limit rather than stating the power flatly.

### The housing opportunity tax credit — HB 5806, HB 5807 and SB 966
Three acts, Public Acts 23, 30 and 31 of 2026, that together create a state tax
credit alongside the federal Low Income Housing Tax Credit. SB 966 puts the
program at the Michigan State Housing Development Authority, HB 5806 creates the
credit against the income tax, and HB 5807 lets a qualifying out-of-state insurer
use it against Michigan's retaliatory tax on insurers. Total credits are capped
at $42.0 million for the 2027 award cycle, rising with inflation.

All three are `housing_affordability` with a yes vote for. **Each is described in
its own terms**, naming what that act does, so a reader is not given three
records that read as the same thing.

The Fiscal Agency puts the eventual revenue loss at roughly $252.0 million a year
by year six if every credit is awarded. That figure is not in the descriptions,
because it is a projection rather than something the acts say.

### HB 6043 — social_programs_and_welfare, a yes vote is for
Writes the Tri-Share child care program into the child care licensing act. The
program had run since 2020 on appropriations alone. An employer, the worker and
the state each pay about a third of that worker's child care costs, and an
employer qualifies by covering at least a third. The act also writes the related
CareShare arrangement into law.

### SB 577 and SB 579 — environment_and_public_health, a yes vote is for
Both extend expiry dates on environmental fees to October 2029. SB 577 covers the
fees for training and certifying public water supply operators; SB 579 covers
floodplain permits, wastewater and stormwater operator training and
certification, stormwater and surface water discharge, the hazardous waste
handler charge, and land and water permits.

Letting these fees lapse would remove the money that pays for the permitting and
oversight they fund, so keeping them is for environment and public health. The
descriptions name the fees rather than calling them "environmental fees", so the
reader can see what is covered.

### SB 723 — corporate_accountability, a yes vote is against
The act removes the December 2027 expiry on approving new transformational
brownfield plans, permits a further $80.0 million a year of tax capture on top of
the $80.0 million already allowed, and raises the lifetime cap on captured
withholding, income and sales and use tax from $1.6 billion to $3.2 billion.

A transformational brownfield plan lets the developer of a large project keep
state taxes generated on the site. Doubling both the annual and lifetime caps and
removing the expiry enlarges that transfer, so on corporate accountability a yes
vote is against. This is the Illinois practice of using
`corporate_accountability` where a measure changes what is required of, or given
to, private business.

Both chambers are recorded: the House passed it 82-26 and the Senate agreed to
the House version 24-12. This is the only House slot in the batch.

### SB 903 — public_education_quality, a yes vote is for
No Fiscal Agency analysis exists for this bill, so it was judged from Public Act
22 of 2026 directly. New section 1280h requires a district receiving a weighted
foundation payment to work on literacy and mathematics achievement, give direct
English language development instruction to English learners, teach at-risk
pupils under the existing at-risk section, administer the WIDA ACCESS assessment,
meet a minimum weekly number of instruction minutes, and run a multi-tiered
system of supports across every grade it operates. No more than 2% of the
weighted money may go to administrative costs.

Attaching conditions so extra money reaches the pupils it is meant for is for
education quality.

## Checks run before importing

- Plain-language lint over all 26 descriptions: **0 warnings**, no sentence over
  45 words.
- Reading level measured: median Flesch-Kincaid grade **7.7**, maximum 9.6. Two
  rows sat above 10 in the first draft and were rewritten by splitting sentences.
  They remain the highest in the batch because "Michigan State Housing
  Development Authority" and the fee names cannot be shortened without losing
  accuracy.
- British spelling scan clean. The scan list was widened after batch-01 with two
  words it had missed, and one more was added for this batch. Note that
  `analysis` is a false positive: that spelling is American too.
  A caution learned here: this very paragraph tripped the scan on its first
  draft, because it quoted the flagged words. Re-read any sentence that talks
  about spelling after a scan, rather than assuming the hit is real.
- Every description cites its own roll's tally and no other.

## The run

| step | result |
| --- | --- |
| judge, dry run | 13 rows, supersession gate clear |
| judge, live | 13 updated |
| import, dry run | 13 files, 294 planned inserts, 0 errors |
| import, live | 13 imported, 294 inserts, 0 errors, 0 notified |
| re-run | 294 unchanged |

Run stamp `2026-09-10T04:35:55.517Z`. The dry run's stamp,
`2026-09-10T04:35:50.219Z`, matches zero rows.

### Reconciled three ways, on a Michigan-scoped count
- import report: 294 inserts
- run-stamp query: 294 rows, 112 candidates
- Michigan roll-call records: 516 before, 810 after, a change of 294

**The raw `candidate_records` table delta was 991 and is not usable here.**
Another session was writing to the same local database at the same time. The
Michigan-scoped count is the correct third check, and it agrees. This is worth
remembering: on a shared local database the whole-table delta is only valid when
nothing else is running.

112 candidates rather than 113 because only one roll in this batch is a House
vote, so one mapped legislator received nothing.

### Duplicate sweep
Six hand-written duplicates retired, listed in `duplicate-retirements.json`.

Five are on HB 4183, the motor fuel tax: Kevin Hertel, Roger Hauck, Sarah
Anthony, Sue Shink and Thomas Albert.

**The sixth is a batch-01 straggler, and it shows a real gap in that sweep.** A
record for Sue Shink describes the HB 4951 Senate vote as "the marijuana tax that
anchored the 2025 road funding package" and never names the bill number.
Batch-01's sweep matched on bill numbers, so it found nothing. This is the known
limit written into the campaign notes — a row naming only the policy and not the
bill is not detected — and it surfaced here only because the record shares a date
with a batch-02 measure.

**The lesson for later batches: sweep on the date and the subject words, not only
on the bill number.**

## Production

Production holds zero Michigan roll-call records. Nothing here touches it.
