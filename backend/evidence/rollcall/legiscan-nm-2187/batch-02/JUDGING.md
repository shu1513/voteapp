# New Mexico batch-02 — judging

## Sources

Same two-source rule as batch-01.

- **The enrolled act is ground truth.** `nmlegis.gov/Sessions/25%20Regular/final/<PADDED>.pdf`. Every
  sentence below was written from the enrolled text.
- **The Legislative Finance Committee's fiscal impact report is an index only.**
  `/firs/<PADDED>.PDF`. It is nonpartisan and carries no sponsor statement of intent, but only its
  "Synopsis of ..." paragraph is neutral; the analysis sections relay agency views. It is used to
  find the operative sections quickly, never quoted.
- **The official roll call sheet fixes the tally, the date and the version.**
  `/Sessions/25%20Regular/votes/<PADDED>HVOTE.pdf`. Its header names the exact version voted.

**The fiscal impact report described a version that did not become law on two of these 15 measures.**
Batch-01 hit the same problem on three of 14. It is a recurring hazard, not an accident.

- **House Bill 78.** The report says the bill stops *insurance companies and pharmacy benefit
  managers* from interfering with 340B entities. The enrolled act does no such thing: Section 1
  defines "manufacturer" and prohibits **a manufacturer, its agent or its affiliate**. The report
  also describes "applicable entity" more broadly than the act, which limits it to federally
  qualified health centers and lookalikes. The record text follows the act.
- **Senate Bill 376.** The report says the substitute eliminates the tiered salary thresholds for the
  employer premium share. The enrolled act still prints those tiers in Subsections B, C and D,
  because New Mexico leaves superseded layers in statute. Subsection E is the operative one: from
  July 1, 2025 the state's contribution "shall be eighty percent of the cost of insurance". The
  report's conclusion is right, but only Subsection E supports it.

## Tally audit

All 40 divided-and-enacted House rolls were re-checked against the official sheets, not a sample.
**All 15 in this batch are exact** on date, yea, nay, and not-voting versus absent. The one failure
in the session is Senate Bill 3, which is not in this batch and stays held.

## Version check

The feed carries final passage only, so no chamber has a second roll and there is no concurrence
vote to read. Each bill's history settles which text the House vote endorsed.

Twelve of the 15 need no further work:

- **Ten Senate bills** — Senate Bills 1, 7, 23, 37, 45, 48, 59, 83, 376 and the Senate's own earlier
  passage of others — reached the House last and were signed with no agreement step afterwards, so
  the House vote is on the enacted text.
- **Senate Bills 5 and 88** were amended by the House and the Senate then concurred, which means the
  Senate accepted the House's version. The House vote is again on the enacted text.

**Three are House bills the Senate amended and the House later agreed to, so the House's only
recorded vote predates the law.** Each was diffed by pulling the adopted amendments out of the
`Amendments_In_Context` print, which tags every folded-in change with the committee or floor
amendment that made it. In all three the change is immaterial to what the record says.

- **House Bill 8** (voted as `HJC/HB 8`, enacted as `HJC/HB 8/a`). The Senate Judiciary Committee and
  five floor amendments changed deadlines ("a reasonable time" to ninety days, thirty days to seven
  or fifteen or fourteen), renumbered subsections, added a line making each weapon conversion device
  a separate offense, and reworded the machine-gun definition from "shoot more than one shot" to
  "fire each cartridge or shell". None of it changes what the bill does.
- **House Bill 78** (voted as `HJC/HJC/HB 78`). The only change after the House vote is Senate Floor
  Amendment 2, which sets the effective date at January 1, 2026.
- **House Bill 493** (voted as `HAFC/HB 493`, enacted as `HAFC/HB 493/a`). The Senate Tax, Business
  and Transportation Committee tightened the audit conditions: the most recent audit must be a public
  record under the Audit Act, and the state agency must name a fiscal agent when the audit is stale.
  It sharpens the same requirement the House voted for.

Descriptions for those three stay at the level both versions share.

## Writing the record text

- Plain English, one paragraph, four to six sentences, no sentence over 45 words.
- Every description says what the measure did and what a yes or a no vote meant, and closes with the
  House tally and the fact that it became law.
- Reading level was measured, not assumed: **median Flesch-Kincaid grade 8.2, worst 9.6**, mean
  sentence 17.4 words, longest 34. Nine of the 15 were rewritten plainer after a first pass came back
  at median 10.0.
- `candidateRecordPlainLanguageLint.listPlainLanguageWarnings` was run over all 30 descriptions
  before importing: **0 warnings**.
- A generator assertion blocks comma splices, any sentence over 45 words, and British spellings.

## Import reconciliation

The count was checked three ways and all three agree.

| Check | Records |
|---|---|
| Importer dry run, `import-dry-run-report.json` | 872 |
| Independent recount from the evidence files against the crosswalk, counting only Yea and Nay | 872 |
| Rows in the local database under this run stamp | 872 |

Area tags were predicted from the judgments before checking: **671 expected, 671 written**. The
prediction is the sum over rolls of matched yea voters times the number of labels, because every
label carries `nay: null`.

- Run stamp: `rollcall:NM:house:2187:<roll>:2026-09-04T20:17:53.555Z`, one stamp for the whole batch.
- 15 files, 15 imported, 0 errors, 63 candidates, 0 notifications.
- New Mexico now holds **1,690 records across 29 rolls** locally. Production still holds none.

## Known feed defects, unchanged from batch-01

Seven House rolls dated 2025-02-27 drop the same member while their headers stay correct, so a
whole-session fetch exits non-zero for New Mexico. That is expected and none of those rolls is in
this batch. One roll stamped 2024-02-10 is a 2024-session vote misfiled into this dataset. No roll in
this batch needs an `official_vote_date` override.
