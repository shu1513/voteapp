# New Mexico 2025 first special session, batch-01 — judging

## Sources

The enrolled acts at `nmlegis.gov/Sessions/25%20Special/final/<PADDED>.pdf` are ground truth. The
Legislative Finance Committee's fiscal impact reports were used as an index. The official roll call
sheets at `/Sessions/25%20Special/votes/<PADDED>HVOTE.pdf` fix the tally and the date.

## Tally audit

Both rolls were checked against their official sheets and **both are exact** on date, yea, nay, and
not-voting versus absent. House Bill 2 is RCS# 6 and Senate Bill 3 is RCS# 12.

## Version check

Neither measure needs one. House Bill 2 passed the House on October 1 and the Senate the next day
with no amendment and no concurrence step. Senate Bill 3 passed the Senate first and the House last,
so the House vote is on the text that was signed. Both were signed within a week.

## Reading Senate Bill 3 correctly

The act removes the federal Advisory Committee on Immunization Practices from New Mexico statute and
lets the Department of Health write its own childhood immunization rules or follow the American
Academy of Pediatrics. It also gives the department authority over adult recommendations, guided by
national physician colleges, and extends immunization requirements to children in licensed child
care as well as school.

**Sections 8 through 13 of this act are a delayed reversion, not policy.** They would have restored
the old federal-committee language on July 1, 2026. The 2026 regular session repealed them in House
Bill 156, so they never took effect. The record for this measure describes what the act did; the
record for House Bill 156 describes the repeal.

## Writing the record text

Same rules as every New Mexico batch. Reading level measured at **median Flesch-Kincaid grade 8.9,
worst 9.5**, longest sentence 29 words. Both descriptions passed
`candidateRecordPlainLanguageLint.listPlainLanguageWarnings` with 0 warnings before importing, as
part of the same 36-description run as the 2026 session.

## Import reconciliation

| Check | Records |
|---|---|
| Importer dry run | 120 |
| Independent recount from the evidence files against the crosswalk, counting only Yea and Nay | 120 |
| Rows in the local database under this run stamp | 120 |

Area tags were predicted before checking: **85 expected, 85 written**.

- Run stamp: `rollcall:NM:house:2227:<roll>:2026-09-05T03:26:04.614Z`.
- 2 files, 2 imported, 0 errors, 62 candidates, 0 notifications.
