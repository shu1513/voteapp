# Michigan batch-06: judging

Not-enacted measures. Each description comes from the **engrossed print**, the
text the House passed, with the House Fiscal Agency analysis used as an index.

## ⚠ Tool fix made during this batch

The text tool (`mi_text.py to_marked_text`) turned Michigan's strike-through
and underline spans into `[[deleted]]` and `<<added>>` markers with a regular
expression that skipped **nested** spans. Some deleted text therefore printed as
plain, current-looking text. It surfaced on HB 5710, where the struck duties to
"identify environmental justice communities" and to forecast greenhouse gas
emissions read as if they stayed in the law. The raw HTML showed both inside
`FormattedStrike` spans.

The converter is now a stack-based HTML parser, and all 322 cached prints were
converted again. Batch-05's descriptions were checked against the new output:
HB 4492's deleted keep-all-tips paragraph now shows as struck, which matches
what batch-05 said.

## The measures

### HB 4283 and HB 4007
HB 4007's engrossed text only adds a definition of "coal replacement engine"
(a reciprocating internal combustion engine placed in service before
February 27, 2024, to replace Upper Peninsula coal generation) and takes effect only if HB 4283 does, so it is dropped. HB 4283's
new section 34 lets a provider with such an engine and a renewable energy credit
portfolio of at least 15% file a certification instead of compliance filings. A
rate-regulated provider must still file an integrated resource plan showing how
it will comply by the end of the planning year ending in 2050, when the option
ends.

### HB 4028 and HB 4027
HB 4028's enacting section repeals Part 8 of the clean and renewable energy act,
the 2023 state siting certificate for large wind, solar and storage projects.
HB 4027 strikes Part 8 from the list of acts a zoning ordinance is subject to,
and strikes subsection (7), which treated renewable projects approved on or
after January 1, 2021 as prior nonconforming uses whose approval could not be
revoked once substantial construction or the lesser of $10,000 or 10% of costs
had been spent. Both passed 58-48 on the same day.

### HB 5711 and HB 5710
HB 5711's enacting section repeals subpart A of part 2 (the clean energy and
renewable energy standards), sections 71 to 99 (energy waste reduction), part 3
(state government energy conservation), section 179, and section 17 of the
property assessed clean energy act. In section 173, the distributed generation
program, it lowers the per-customer generation cap from 110% to 100% of
past-year use (subsection 2) and lowers the program size a utility must allow
from 10% to 1% of its average in-state peak load (subsection 3). Both caps
apply inside that program, not to all customer-owned generation; the first
description said "limit customer-owned generation such as rooftop solar" and
skipped the 10% to 1% change, and review caught it. It also adds municipal
solid waste, landfill gas from it, and waste-derived fuel to the renewable
resource list. The standards'
targets quoted in the description (80% clean by 2035 and 100% by 2040; 50%
renewable by 2030 and 60% by 2035) are the current-law numbers being repealed.

HB 5710 rewrites the integrated resource plan test to "prioritizes reliability
and affordability while minimizing the net cost to ratepayers", allowing other
factors only where they do not materially compromise reliability or raise total
costs. It strikes the greenhouse gas forecast, the environmental justice
community identification, and the environmental justice impact analysis for new
gas plants, and ends the Utility Consumer Representation Fund's grant program.

### HB 4486
New act, three sections: no local ordinance, resolution or policy banning the
use of natural gas or propane, or the installation of the infrastructure that
carries them. Rules adopted on or after the effective date are void.

### HB 4160
New subsections 32(8) to (10) and matching items in the regulatory impact
statement. Where the federal government mandates a state rule, the rule may not
be more stringent unless the director finds a clear and convincing need. Where
it does not, a specific Michigan statute also suffices. Emergency rules and the
amendment of the current special education rules are excepted.

## Import

Judge: 7 updated. Dry run: 648 planned inserts, 0 errors, stamp
`2026-09-11T06:41:51.547Z`, which matches zero rows. Real run: 7 files imported,
**648 inserts**, 0 notified, stamp `2026-09-11T06:42:23.750Z`, 96 candidates,
400 area tags. Michigan roll-call records went 2,416 to 3,064, which reconciles
with the import report and the stamp query.

**HB 5711 review fix.** Judge: 1 updated (HB 5711), 6 unchanged. Re-import:
93 rewrites on roll 1698058, 555 unchanged elsewhere, 0 notified. Reports are
`import-hb5711-fix-dry-run-report.json` and `import-hb5711-fix-rerun-report.json`;
the originals are kept. Local database only.

## Duplicates

**19 hand-written records retired**: 1 on HB 4283, 3 on HB 4028, 13 on HB 5711,
and 2 on HB 4486. Each cites the same tally on the same day and describes the
same subject, and each names its replacement record in
`duplicate-retirements.json`.

HB 4027 and HB 4028 passed 58-48 on the same day. The hand-written records
describe the state permit route that HB 4028 repeals, so they were matched to
HB 4028 and none to HB 4027.

**Left alone on purpose:** four records that are not simply votes. One HB 4028
record also records a co-sponsorship; one HB 4486 record and one HB 5710 record
describe sponsoring the bill; and one HB 5710 record describes a proposed
amendment. Retiring a sponsorship record would delete a fact the roll-call
import does not replace (the Washington SB 5917 lesson).
