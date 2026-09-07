# Connecticut batch-04 — the rest of the divided votes that did not become law

33 measures read, **14 kept: 16 rolls / 1,144 records.** With batch-03's seven
rolls, every decisive roll in `../survey/divided-not-enacted-worklist.tsv` now
carries a disposition. Connecticut holds **4,062 roll-call records**, local
`voteapp` only.

Each measure was read from the text the chamber actually voted: the file copy
plus every Amendment Schedule that chamber adopted before the vote, taken from
the bill history (LegiScan's `adopted` flag is unreliable for Connecticut).
`www.cga.ct.gov` serves both to plain `curl` once the GoDaddy intermediate it
omits is added to the CA bundle — the browser workaround is no longer needed.

## What was kept

| measure | rolls | area | how it died |
|---|---|---|---|
| HB 5002 housing and homelessness | H 84-67, S 20-15 | `housing_affordability` | vetoed |
| HB 6907 warehouse quotas | H 97-48 | `corporate_accountability` | Senate never voted |
| HB 7035 no bans on multifamily housing | H 94-54 | `housing_affordability` | Senate never voted |
| HB 7158 minor patient safety plans to schools | H 105-42 | `environment_and_public_health` | Senate never voted |
| HB 7248 well water quality | H 101-46 | `environment_and_public_health` | Senate never voted |
| HB 8002 housing growth | H 90-56 | `housing_affordability` | Senate never voted |
| SB 6 infants, toddlers, disconnected youth | S 22-12 | `social_programs_and_welfare` | House never voted |
| SB 7 access to health care | S 25-10, H 92-55 | `healthcare_affordability` | disagreeing action |
| SB 8 strikers' unemployment benefits | S 24-11 | `reduce_wealth_gap` | vetoed |
| SB 1355 compounding contraception and abortion drugs | S 24-11 | `womens_reproductive_rights` | House never voted |
| SB 1356 data privacy | S 26-9 | `data_privacy` | House never voted |
| SB 1427 paid leave for school staff | S 24-11 | `social_programs_and_welfare` | disagreeing action |
| SB 1481 no discrimination in Medicaid services | S 24-11 | `civil_rights` | House never voted |
| SB 1488 withholding pay for prevailing-wage violations | S 27-8 | `corporate_accountability` | House never voted |

SB 8 and SB 1427 extend measures batch-03 already imported for the other
chamber, and carry the same area and wording.

## The judge gate orders by roll id; Connecticut orders by vote number

Twelve of the sixteen rolls tripped the superseded-stage gate. LegiScan assigns
Connecticut's roll ids in *descending* vote order — on HB 5002's Senate day,
roll 1582313 is Vote 323 (20-15, passage) and rolls 1582314 through 1582321 are
Votes 322 down to 315 (eight amendment rejections, 11-24 each). The gate reads
the higher ids as later votes. Every kept roll was checked against the printed
vote number and its tally pattern — the rejections run 11-24 or 47-101, the
passage 20-15 or 94-54 — and the 45 earlier rolls are listed in
`acknowledge_later_rolls`. **The README's rule stands: order Connecticut rolls
by the printed vote number, never by roll id.**

## Two endings that are not "the other chamber never voted"

**SB 7** and **SB 1427** passed the Senate, then passed the House *as amended by
the House*, and the Senate never took up the House's changes — the history reads
"Disagreeing Action, Tabled for Calendar, Senate." Their descriptions say so.
The House roll on SB 7 is a vote on the House-amended text and is worded as one.

## The Public Act number trap, twice

HB 5002 is "Public Act 25-49" and SB 8 is "Public Act 25-64". Both were vetoed.
A Connecticut bill is numbered on passage, before the Governor acts; the number
proves nothing about enactment.

## The drops

Seven are studies or working groups as voted (HB 5607, HB 6841, HB 6908,
HB 6999, HB 7242, SB 797, SB 1493). Two are omnibuses (HB 7004, a 500-section
"Municipal Issues" vehicle; HB 8004, the special-session package). Two cut both
ways (SB 1227 ethics thresholds, and HB 7004's referendum clause). Five have no
research area, and **three of those are the labor gap** — HB 6844, SB 1222 and
SB 1371, each a worker protection against a public employer, where the
`corporate_accountability` workaround does not reach.

## Reconciliation

| step | records |
|---|---|
| dry run | 1,144 `insert` |
| real run | 1,144 `insert` |
| re-run | 1,144 `unchanged` |
| rows in the local database, stamp `2026-09-07T03:22:22.510Z` | 1,144 |

House rolls reach 124-130 candidates, Senate rolls 27-29. Median Flesch-Kincaid
grade **7.2**, worst **8.3**, no sentence over 45 words, no British spellings.
