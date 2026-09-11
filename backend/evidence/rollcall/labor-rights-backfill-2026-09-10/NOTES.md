# labor_rights label backfill — 2026-09-10

Adds the `labor_rights` research area (migration 277) to roll calls and
hand-written records that already covered a labor vote or act under another
area. Same recipe as `ai-regulation-backfill-2026-09-09`: one judgments file
across states, copied from `legislative_votes` with the new label added,
applied with `rollcall:judge`, then each roll re-imported from a scoped folder
so `syncRollCallRecordTags` adds the tag. Sentences, other labels and vote
dates were not changed.

## What counts as labor_rights

A yes vote is `for` when the measure widens worker pay, leave, workplace safety,
workers' compensation, or the right to organize and bargain. It is `against`
when it narrows them. In scope: minimum, prevailing and tipped wages, wage
theft, overtime, pay transparency and equal pay, paid sick and family leave,
workplace safety and heat rules, workers' compensation, unions, bargaining and
right-to-work, noncompete and stay-or-pay contracts, child labor, worker
misclassification, and unemployment pay during a strike.

Left out on purpose: general unemployment insurance rules (they stay under
social programs), E-Verify, workplace diversity programs, public pension and
health plan costs, job-training grants, apprenticeship offices, and taxes on
tips or overtime.

## Roll calls (199 rolls, 38 state sessions plus federal)

- `nay` is null on every new label, except where the roll's other labels
  already give nay voters a stance. There the labor label mirrors them (15
  rolls: 9 in Maryland, NC S 1082, OR HB 3550, and H.R. 7, H.J.Res. 98 in both
  chambers, and H.R. 2262).
- Alabama SB 231 carried only a `general` label. It now carries
  `labor_rights` (yes = against), and the `general` tags are gone.
- `acknowledge_later_rolls` and `note` fields were copied from the original
  batch judgments (20 rolls) so the final-vote check passes.
- 29 evidence files were missing from the repo (22 California, 2 Oklahoma,
  1 South Dakota, 2 Washington, 2 federal). The state files came from the
  local LegiScan evidence folders. The two 2026 House rolls were re-fetched
  with `rollcall:fetch`, and both came back `unchanged`.
- Every state import came back `unchanged` apart from the tag change. The two
  Alabama `retired` actions are older duplicate copies that block a re-insert.
  They are not new retirements.
- The federal re-imports also inserted 509 new records: H.R. 7, H.R. 1065 and
  H.R. 3110 (165), H.J.Res. 98 in the House (73), and H.R. 2262 and H.R. 5408
  (271). Each belongs to a House candidate added on 2026-09-05 to 09-08,
  after those rolls were first imported. There is one record per candidate per
  roll, and none matches a hand-written record of the same vote.

## Rolls left alone

- Michigan HB 4002 (earned sick time) and SB 8 (minimum wage): each moves in two
  directions at once (see `legiscan-mi-2183/batch-01/PLAN.md`), so there is no
  honest single stance. Hand-written records about these votes were not tagged
  either.
- Arkansas HB 1017 widens paid maternity leave but narrows who qualifies.
  Connecticut SB 1312 mixes unemployment administration with one reporting
  duty. Illinois SB 2339 both adds and removes worker protections.
- Topic mentions without a labor position, such as E-Verify, diversity
  programs, public pensions and credit checks in hiring.

## Hand-written records

See `records-repair/labor-rights-tags-2026-09-10/`: 207 records reviewed,
194 tagged. 13 were skipped by the office gate because the candidate is running
for an office without the labor area, such as a city council, county executive,
sheriff or state auditor.

Union endorsements (about 110 records) were not tagged. An endorsement is
another group's act, not the candidate's own stance. One Tennessee AFL-CIO
endorsement record was tagged `labor_rights/for` by a separate session the same
day, and it was left as it is.

## Result (local database)

7,940 live `labor_rights` tags: 6,600 for and 1,340 against.
