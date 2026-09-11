# Wisconsin batch-08: AB 100 and AB 102

Judged 2026-09-10. Local database only; production holds no Wisconsin roll-call
records.

## Why this batch exists

Batch-05 dropped AB 100 and AB 102 "on SF0044's reasoning, pending the same
decision". Wyoming batch-04 made that decision: a bill that sorts school sports
by biological sex is `civil_rights`, and a yes vote is **against**. That matches
Michigan HB 4469 and the Arkansas, Arizona and Idaho imports. These two bills
are judged the same way.

## Do they qualify?

Yes. Both passed both chambers, the governor vetoed both on 31 March 2026, and
the Assembly failed to override on 13 May 2026. Wisconsin's pool covers vetoed
measures (see batch-02 PLAN.md), so both are in scope. The prose is conditional
("would have") and names how each bill died.

Both chambers voted the enrolled text. The Assembly adopted its only amendment
before its vote, and the Senate concurred without changes. AB 100's later "LRB
correction" is a drafting fix, not a vote. So all four rolls are kept.

## What the bills would have done

Read from the enrolled prints.

- **AB 100** — public schools, charter schools and private voucher schools must
  label each team boys, girls or coed by the sex on a pupil's original birth
  certificate, and bar boys from girls' teams. A girl harmed by a violation may
  sue the school. Each locker room and shower room is limited to one biological
  sex; a pupil who asks gets a single-user or staff locker room.
- **AB 102** — the same rules for University of Wisconsin campuses and technical
  colleges, for college and club teams and campus locker rooms.

Both bills also carve these rules out of the state's anti-discrimination
statutes for schools and colleges.

## The labels

| measure | area | yes means | no means |
| --- | --- | --- | --- |
| AB 100 | civil_rights | against | — |
| AB 102 | civil_rights | against | — |

The no side is `null`, as with SF 44 and AB 103 and AB 104: a no vote could rest
on other grounds.

## Rolls

| measure | chamber | roll | date | tally | records |
| --- | --- | --- | --- | --- | --- |
| AB 100 | Assembly | 1522672 | 2025-03-20 | 51-43 | 84 |
| AB 100 | Senate | 1630296 | 2026-02-11 | 18-15 | 11 |
| AB 102 | Assembly | 1522508 | 2025-03-20 | 50-43 | 83 |
| AB 102 | Senate | 1630339 | 2026-02-11 | 18-15 | 11 |

## How it was run

Same commands as the other Wisconsin batches, from `backend/`:

1. `npm run rollcall:legiscan:fetch -- --state WI --dataset-dir <wi-2197 dataset> --bills ab100,ab102 --dry-run --evidence-dir <scratch>` to write the four roll files, copied here. The vote rows already existed from the first fetch.
2. `python3 build-judgments.py`, then `npm run rollcall:judge -- --judgments-file evidence/rollcall/legiscan-wi-2197/batch-08/judgments.json`, dry run first.
3. `npm run rollcall:legiscan:import -- --state WI --evidence-dir evidence/rollcall/legiscan-wi-2197/batch-08 --crosswalk-file evidence/rollcall/legiscan-wi-2197/crosswalk.json --people-file evidence/rollcall/legiscan-wi-2197/legiscan-people-wi-2197.json`, dry run first.

No AI calls.

## Reconciliation

- Dry run: 189 inserts over 4 rolls. Real run: `outcomes {imported: 4}`,
  `actions {insert: 189}`, 0 errors, 0 notified.
- 97 yes-side records and 92 no-side records.
- Tags: 97, all `civil_rights` against, one per yes-side record. No no-side tags.
- All Wisconsin roll-call records: 5,225, which is 5,036 before this batch plus
  189, over 102 roll calls and 99 candidates.

## Left open

Batch-07 dropped AB 595 (voter list maintenance) as an analog of Wyoming HB 318,
which Wyoming batch-04 has since settled as `election_integrity`, yes = for.
AB 595 is not touched here.
