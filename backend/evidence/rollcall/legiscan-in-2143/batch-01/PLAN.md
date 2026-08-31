# Indiana batch-01 — selection

**4 measures, 6 rolls, 339 records across 101 candidates.** Imported on the local
`voteapp` database 2026-08-31. Production untouched.

## How the batch was chosen

The pool is the 142 divided roll calls on measures that became law, across 68 measures.
The five campaign filters were applied in order.

1. **Divided.** The losing side is at least a quarter of the winning side.
2. **Became law.** LegiScan status 4. This removes the 18 divided rolls on measures that
   died.
3. **A nameable subject that maps to a research area.** This removes the state budget,
   the study commissions, the charity gaming bills, pensions and the aviation board.
4. **One roll per measure per chamber, preferring the chamber's final action.** In Indiana
   that means the conference committee report where there was one, then the concurrence,
   then the third reading. Choosing the last action is what lands the roll on the text that
   became law.
5. **A defensible direction.** A measure is only taken if a research area carries an honest
   for-or-against reading of it.

On top of those, Indiana adds a step no other state needs: **each selected roll's member
list was compared name by name against the official Indiana roll-call PDF.** All six
passed. See `../CODE-FINDINGS.md` section 2 for why.

## The measures

| Measure | Rolls | Area | Yes vote means |
| --- | --- | --- | --- |
| SB 289, unlawful discrimination | House 1556465 (64-26), Senate 1556839 (34-16) | civil_rights | against |
| HB 1393, immigration notice | House 1549534 (58-19), Senate 1524754 (37-10) | immigration | against |
| SB 475, physician non-compete agreements | House 1556843 (65-21) | corporate_accountability | for |
| HB 1041, college sports eligibility | House 1491965 (71-25) | civil_rights | against |

SB 475 and HB 1041 have no divided vote in the other chamber, so filter 4's one-roll-per-
chamber cap simply has one chamber to fill.

## Version check, per roll

Indiana publishes a dated stack of every bill version ending in an enrolled act, so the
check is exact rather than inferred.

- **SB 289** — both rolls are the conference committee report of 2025-04-24, which is the
  enrolled text.
- **HB 1393** — the Senate passed its engrossed version on 2025-03-24 and the House agreed
  to that same version on 2025-04-16 (the act is signed `HEA 1393 — Concur`), so both rolls
  sit on the text that became law.
- **SB 475** — the conference committee report of 2025-04-24, the enrolled text.
- **HB 1041** — the only divided roll on the measure, and it predates the Senate's committee
  substitute. The operative chapter of the version the House passed was diffed against the
  enrolled act and they are the same, so the roll stands with no version split. Had the
  Senate changed it, the California SB 707 precedent would have dropped the roll.

## Dropped under filter 5 after reading the enrolled act

- **SB 405, labor organization membership.** It bars a government that contracts out the
  management or lease of a public asset from requiring or considering whether the
  contractor's employees are union members. The text is **symmetric** — it bars favouring
  union labour and bars disfavouring it in the same clause — so neither direction is honest.

## Deferred to batch-02

64 measures remain, with 136 divided-and-enacted rolls. Each carries a disposition in
`../survey/divided-enacted-worklist.tsv`. The largest named items still open are SB 1
(local government finance, the session's marquee property tax act), SB 2 (Medicaid),
SB 10 (voter registration), SB 249 (teacher compensation), SB 442 (instruction on human
sexuality), SB 423 and SB 424 (small modular nuclear reactors), SB 457 (carbon dioxide
sequestration) and HB 1461 (road funding).

Twelve of the remaining rolls are flagged in the worklist because their LegiScan tally has
no exact match in the official history, which is the signal for the member-list problem in
`../CODE-FINDINGS.md` section 2. They need the official roll-call PDF read before use.
