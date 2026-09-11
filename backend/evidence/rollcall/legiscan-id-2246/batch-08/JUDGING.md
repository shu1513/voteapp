# Idaho batch-08: judging

## H 516
Read from the text "As Amended in the Senate" (`ls_text.py`), which the House
concurred in. New section 33-1277 bars school districts from using taxpayer
funds to support teachers unions by deducting dues, raising pay to cover dues,
giving unions more personal or contact data than public records law requires,
making teachers meet with a union, distributing union messages, giving money to
a union, or paying for union activities. A contract may still allow unpaid union
leave, personal leave the teacher chooses to use, and paid representational
work if the union repays its pro rata cost twice a year. Civil penalties run
from $250 to $2,500, enforced by the attorney general or county prosecutor.
Section 33-1275 voids any contract term that breaks these rules, and section
33-513 drops paid time for state teachers association meetings. Every change
narrows union rights, so a yes vote is `against`.

Review correction (2026-09-11): the first descriptions dropped two exceptions
the bill keeps. Section 33-1277(1)(c) lets a district share extra contact
information with the teacher's written authorization, and (2)(b) lets a
teacher spend their own compensated personal leave on union work without any
union repayment; repayment applies only to on-duty representational work
under (2)(c). All four sentences now carry both exceptions.

## H 645
Read from the introduced text, which passed unamended. See PLAN.md for the drop.

## Import

First run: judge 2 updated; dry run 86 planned inserts, 0 errors, stamp
`2026-09-11T07:35:37.819Z`; real run 2 files imported, **86 inserts**, 0
notified, stamp `2026-09-11T07:36:14.229Z`, 86 candidates, 53 area tags.

Correction (2026-09-11), in this order: judge 2 updated with the fixed
sentences; import rewrote all 86 records in place (dry run committed as
`import-dry-run-rerun-report.json`, 86 planned rewrites; the retire step had
not run yet, it needs `DATABASE_URL` in the environment); the five substitute-cast
records below were then retired; a final import planned **81 unchanged, 5
retired**, 0 inserts, and is committed as `import-rerun-report.json` (the
importer keeps the first run's `import-report.json`). Live totals: 81 records,
81 candidates, 49 area tags.

## Substitute-cast votes retired

Idaho lets an absent member appoint a substitute, and the journals print the
vote as `Member(Substitute)`. LegiScan carries only the regular member's
`people_id`, so the importer attributed five votes to candidates who did not
cast them:

| candidate | journal entry | who voted | roll |
| --- | --- | --- | --- |
| John Shirts | Shirts(Batt), aye | Gayle Batt | House 1676245 |
| Megan Egbert | Egbert(Beazer), nay | Jennifer Beazer | House 1676245 |
| David Cannon | Cannon(Cannon), aye | Lisa Cannon | House 1676245 |
| Ted Hill | Hill(Hill), aye | Randolph J. Hill | House 1676245 |
| Carl Bjerke | Bjerke (Bjerke), aye | Lesli Bjerke | Senate 1675366 |

Sources: [House journal](https://legislature.idaho.gov/wp-content/uploads/sessioninfo/2026/journals/hfinal.pdf)
page 396, [Senate journal](https://legislature.idaho.gov/wp-content/uploads/sessioninfo/2026/journals/sfinal.pdf)
page 300. The House roll also shows Hall(Stone), but Hall is not a November
2026 candidate in the crosswalk, so no record existed.

All five were soft-retired with `npm run manual:records:retire --
--retirements-file evidence/rollcall/legiscan-id-2246/batch-08/substitute-vote-retirements.json --apply`.
The manifest is committed beside this file. A retired row blocks the fan-out
for that candidate and roll (the importer plans `retired`, never `insert`), so
re-running the import cannot bring them back. Their area tags stay attached to
the retired rows, which read paths already hide.

Every Idaho batch should check the journal roll for `Name(Name)` entries before
import; this batch is the first to hit one.

## Duplicates

None. The sweep found no hand-written record on either vote.
