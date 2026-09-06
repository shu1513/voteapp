# Tennessee batch-10 — the two congressional redistricting acts, imported with no stance

The run summary left these two "open for the operator": both are divided, both
became law, and both were dropped under filter 5 because no direction could be
read out of the text. **The user directed on 2026-09-06 that they be imported
with a neutral description and no stance** — `general`, with no `yea` or `nay`
key — matching Missouri HB 1 and New York A 10710 / A 10711 already in the
database.

| measure | roll | tally | what it did |
|---|---|---|---|
| HB 7002 | house 1695420 | 66-24 | removes the bar on redrawing congressional districts between the ten-year federal counts |
| HB 7002 | senate 1695421 | 22-8 | same act |
| HB 7003 | house 1695538 | 64-25 | redraws the congressional district lines set earlier in 2026 |

HB 7002 is **Public Chapter 1**, signed 2026-07-27. HB 7003 is **Public Chapter
3**, signed 2026-06-03. Both were confirmed on the General Assembly's own bill
pages, not from the dataset.

## Why no stance, in the state's own terms

The run summary already set this out and the user's direction follows it: Georgia's
maps in `../legiscan-ga-2114/` could carry `civil_rights / for` because a federal
court had ruled the prior maps unlawful and the new maps were the remedy.
Tennessee's was a mid-decade redraw with no court order behind it, and the act is
141,000 characters of census-block tables. Any direction would be an assertion
about whom the map favors rather than something the text settles.

## The Senate roll writes no records, and that is a finding

HB 7002's Senate roll resolves **30 members to zero candidates** — 16 unmatched,
14 out of scope. **Tennessee's Senate is in the same position as South
Carolina's**: those seats are not on the November 2026 ballot in numbers our
roster covers, so a Senate roll matches nobody. The judgment is kept and the
evidence file committed, because the roll is correctly judged and the report
records the zero; only the records are absent.

## Reconciliation

| step | records |
|---|---|
| dry run | 137 `insert` |
| real run | 137 `insert` |
| re-run | 137 `unchanged` |
| rows in the local database | 137 |

House rolls carry the whole batch: 69 records for HB 7002 and 68 for HB 7003.
