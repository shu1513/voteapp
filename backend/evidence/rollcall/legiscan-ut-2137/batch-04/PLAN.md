# Utah 2025 General Session, batch-04

6 roll calls on 4 measures. 135 candidate records across 67 candidates.
Local database only. Production holds no Utah records.

## What is in the batch

| measure | rolls | area | direction |
| --- | --- | --- | --- |
| HB 355 Mining and Critical Infrastructure Materials | House 41-29, Senate 17-7 | environment_and_public_health | against |
| SB 277 Government Records Management | House 44-23, Senate 18-6 | anti_corruption | against |
| HB 294 Infectious Disease Procedures | Senate 18-5 | environment_and_public_health | against |
| HB 195 Firearm Retention | Senate 21-6 | gun_control | against |

Every label states `nay` explicitly and every one is null.

This batch reaches fewer candidates than batch-03 because four of its six rolls
are Senate rolls, and only 15 of Utah's 29 Senate seats are on the 2026 ballot.

## Checks run before importing

- **Version, per roll.** All 6 were cast on the substitute that was enrolled.
- **HB 195 needed the same-day guard.** The Senate passed it 20-6 at 3:33 PM,
  moved to reconsider, adopted floor amendment 3, and passed the amended text
  21-6 at 3:39 PM. Utah's own action list carries those times; LegiScan does
  not, so the store refused to approve the later roll until it was told. The
  judgment names the 20-6 roll in `acknowledge_later_rolls`. The 21-6 roll is
  the decisive one and the one the House then concurred in.
- **Governor action.** All 4 measures carry a `Governor Signed` line.
- **Tally and members.** All 6 match Utah's own record on the tally and were
  cleared name by name against Utah's per-roll vote sheet. Every question is a
  form of final passage.
- **Later sessions.** Nothing in the 2025 special sessions or the 2026 General
  Session restores the State Records Committee, reverses the quarry rights, or
  repeals the other two acts. The touches that exist are recodifications, a
  revisor's corrections bill, and unrelated amendments to shared sections.
- **Duplicates.** The importer flagged 2 related records, both about other bills
  that share a vote date (the same two batch-03 cleared). The bill-number sweep
  over Utah records not written by this pipeline found only a 2023 record about
  a different HB 294. The 69 "HB 195" rows it also matched are Missouri records.
- **Prose.** Flesch-Kincaid grade median 7.5, worst 8.5, longest sentence 34
  words; the repository lint reports 0 warnings over all 12 descriptions.

## Reconciled three ways

- `import-report.json`: 6 rolls, 135 inserts, run stamp
  `2026-09-10T06:31:36.630Z`.
- Database on that stamp: 135 rows, 67 distinct candidates, 84 area tags.
- Convergence dry run afterwards: 135 unchanged, 0 inserts. The earlier dry-run
  stamp `2026-09-10T06:31:28.541Z` matches 0 rows.

## Dispositioned without importing

- **HB 300 Amendments to Election Law — needs your direction.** This is Utah's
  vote-by-mail overhaul. Voters must write the last four digits of a driver
  license, state ID or Social Security number on the return envelope; from 2029
  a ballot is mailed only to a voter who asks for one, and a request lasts eight
  years; the lieutenant governor must review voter rolls for address anomalies
  and join the federal SAVE citizenship check. The only area that fits is
  `election_integrity`, and the direction splits along the usual line: one
  reading scores the verification strands as for, the other scores the end of
  automatic mail ballots as a loss of access that the area also covers. I have
  not picked a side.
- **HB 312 Criminal Justice Amendments — dropped.** An omnibus whose strands
  point different ways: it tightens release for overcrowding and pretrial
  release for habitual offenders, bans state money for syringe exchange
  programs, and bars private prisons except for immigration detention. No
  single honest label covers it.
- **SB 268 Rules Review and General Oversight Committee — dropped.** Its closed
  meetings are narrow (individual child welfare cases and information under a
  confidentiality agreement). Its main strand lets a legislative committee delay
  State Board of Education rules, a dispute between two branches that no
  research area covers.
- **HB 256 Municipal and County Zoning — dropped.** It gives cities and counties
  tools to enforce their own short-term rental rules. Whether that changes
  housing supply depends entirely on what each city chooses, so no direction
  can be stated.

## Left for batch-05

67 rolls still marked `candidate` in `survey/dispositions.tsv` for the 2025
session, and the whole 2026 session (123 candidate rolls).
