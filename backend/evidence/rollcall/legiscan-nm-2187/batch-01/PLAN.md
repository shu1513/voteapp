# New Mexico batch-01 — selection

**14 measures, 14 House roll calls, 818 records across 63 candidates.**

## How the batch was picked

The five standing filters, applied in order.

1. **Divided.** The losing side is at least a quarter of the winning side. 125 rolls on kept bill
   types clear this.
2. **Became law.** 78 rolls, on 49 measures. The rest either passed one chamber and died (37), were
   vetoed (9), or never left introduction (1).
3. **A nameable subject that maps to a research area.**
4. **One roll per measure per chamber.** New Mexico satisfies this for free: no bill has a second
   roll call in the same chamber anywhere in the session.
5. **A defensible direction.** A measure is taken only if it carries a research area with an honest
   for-or-against reading. Anything that would land on `general` is dropped rather than imported.

**Only House rolls are eligible at all.** New Mexico senators serve four-year terms and were elected
in 2024, so no Senate seat is on the 2026 ballot; all 38 divided-and-enacted Senate rolls fan out to
nobody. That leaves 40 House rolls, one per measure, and this batch takes 14 of them.

## The 14

| Measure | Roll | Vote | Area, and which way a yes vote points |
|---|---|---|---|
| House Bill 12 | 1499308 | 41-27 | gun control, for |
| House Bill 6 | 1492677 | 41-26 | corporate accountability, for |
| House Bill 89 | 1503003 | 40-25 | public education quality, for; immigration, for |
| House Bill 128 | 1503058 | 43-22 | environment and public health, for |
| House Bill 586 | 1519227 | 41-26 | corporate accountability, for |
| Senate Bill 9 | 1524388 | 45-15 | environment and public health, for |
| Senate Bill 16 | 1524320 | 36-33 | civil rights, for |
| Senate Bill 21 | 1521928 | 43-25 | environment and public health, for |
| Senate Bill 36 | 1520184 | 42-23 | data privacy, for; immigration, for |
| Senate Bill 57 | 1520145 | 42-25 | women's reproductive rights, for |
| Senate Bill 120 | 1524430 | 46-17 | healthcare affordability, for |
| Senate Bill 124 | 1522178 | 40-23 | corporate accountability, for |
| Senate Bill 267 | 1523871 | 42-21 | housing affordability, for |
| Senate Bill 364 | 1524440 | 38-20 | immigration, for |

Ten research areas, none of which had any New Mexico coverage before. Senate Bill 16 at 36-33 is the
closest vote of the session.

Every label states the nay side explicitly, and every one is null. New Mexico had a Democratic
trifecta in 2025, so the divided-and-enacted set is the majority's agenda and every direction here
reads `for`; on each measure the realistic objection runs on a different axis from the area being
scored, which is what a null nay records.

## Version check

The feed has no concurrence rolls, so this was settled from each bill's own history.

- Four measures in the pool are House bills the Senate amended and the House then agreed to, which
  means the House's only recorded vote predates the text that became law: House Bills 2, 8, 78 and
  493. **None of them is in this batch.**
- Four are Senate bills the House amended and the Senate then agreed to, so the House vote is on the
  enacted text: Senate Bills 3, 5, 88 and 535. Only Senate Bill 3 was a batch candidate, and it is
  held for a different reason.
- The remaining measures show no agreement step at all, so the second chamber passed what it
  received and the House vote is on the enacted text.

The official roll call sheet also prints the exact version voted in its header, for example
`SFC/SB 3/a/ec` and `HB 128/aaa`, which was checked against each selected roll.

## Dropped under filter 5, after reading the acts

- **Senate Bill 113** extends the sunset date of five boards and commissions. Housekeeping.
- **Senate Bill 127** exempts film and television make-up artists and hairstylists from barber and
  cosmetology licensing. No research area fits.
- **Senate Bill 290** raises the marriage license fee and splits it between county funds.
- **Senate Bill 481** creates a governing district for the Albuquerque state fairgrounds. Single
  jurisdiction.
- **Senate Bill 535** carries three unrelated subjects at once: a supported decision-making act, a
  higher fee on the utilities the state regulates, and a doubled 911 surcharge. No single direction.

## Excluded by rule

**House Bill 2**, the general appropriation act, and **House Bill 450**, the capital outlay bill. No
research area carries an honest direction on a vote to fund the government.

## Held, not dropped

**Senate Bill 3, the Behavioral Health Reform and Investment Act.** It is the session's most
consequential divided health measure, and its LegiScan tally is wrong: the state's own roll call
sheet reads 44-23 where LegiScan stores 42-23. The approval gate writes the stored tally into the
record text, so importing it would publish a number New Mexico's own record contradicts. See
CODE-FINDINGS.md, finding 2.

## What is left

The worklist at `survey/divided-enacted-worklist.tsv` gives all 78 divided-and-enacted rolls a
disposition: 14 in this batch, 18 House rolls marked as candidates for a second batch, 5 dropped, 2
excluded as appropriations, 1 held, and the 38 Senate rolls out of scope.
