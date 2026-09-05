# New Mexico 2026 session, batch-01 — selection

**16 measures, 16 House roll calls, 938 records across 63 candidates.**

This batch clears the session's whole pool in one pass. Every divided-and-enacted House roll is
imported, excluded, dropped, or held with a reason recorded in the committed worklist.

## How the batch was picked

The five standing filters.

1. **Divided.** The losing side is at least a quarter of the winning side.
2. **Became law.** LegiScan status 4.
3. **A nameable subject that maps to a research area.**
4. **One roll per measure per chamber.** New Mexico satisfies this for free: no bill has a second
   roll call in the same chamber anywhere in the session.
5. **A defensible direction.** A measure is taken only if a research area carries an honest
   for-or-against reading of a yes vote.

**Only House rolls are eligible.** New Mexico senators serve four-year terms and were elected in
2024, so no Senate seat is on the 2026 ballot and all 25 divided-and-enacted Senate rolls fan out to
nobody. That leaves 21 House rolls.

## The 16

| Measure | Roll | Vote | Area, and which way a yes vote points |
|---|---|---|---|
| House Bill 4 | 1632642 | 48-19 | healthcare affordability, for |
| House Bill 9 | 1620250 | 40-29 | immigration, for |
| House Bill 124 | 1628713 | 40-21 | immigration, for |
| House Bill 156 | 1627425 | 51-14 | environment and public health, for |
| House Bill 200 | 1632501 | 52-15 | housing affordability, for |
| House Bill 247 | 1627420 | 48-20 | government efficiency, for |
| House Bill 270 | 1633948 | 41-25 | reduce wealth gap, for |
| Senate Bill 2 | 1620240 | 44-23 | public infrastructure, for |
| Senate Bill 21 | 1636452 | 41-22 | healthcare affordability, for |
| Senate Bill 30 | 1634862 | 39-27 | women's reproductive rights, for |
| Senate Bill 40 | 1636280 | 42-22 | data privacy, for; immigration, for |
| Senate Bill 96 | 1636446 | 41-23 | cost of living reduction, for |
| Senate Bill 111 | 1638035 | 37-24 | data privacy, for; immigration, for |
| Senate Bill 152 | 1637987 | 48-14 | cost of living reduction, for |
| Senate Bill 241 | 1636395 | 37-19 | social programs and welfare, for |
| Senate Bill 264 | 1636283 | 41-26 | election integrity, for |

Eighteen labels across twelve research areas. Two are new to New Mexico: government efficiency and
election integrity. House Bill 9, the Immigrant Safety Act, at 40-29 is the narrowest vote in the
batch and the closest of the session.

Every label states the nay side explicitly, and every one is null. New Mexico had a Democratic
trifecta, so the divided-and-enacted set is the majority's agenda and every direction reads `for`.

## Held

**Senate Bill 151** (corporate income tax) passed both gates and carries a defensible direction, but
LegiScan's stored tally disagrees with New Mexico's official roll call sheet: 42-19 stored against
43-19 official, with LegiScan's own bill history siding with the sheet. It is held, not imported.
`../CODE-FINDINGS.md` has the detail. This is the second such defect in two New Mexico sessions.

## Excluded and dropped

- **House Bill 2** (55-15), the general appropriation act. No research area carries an honest
  direction on a vote to fund the government.
- **House Bill 332** (42-20), 380 capital outlay reauthorizations. Appropriations housekeeping.
- **House Bill 8** (45-23), the higher education major projects fund. This is a funding vehicle for
  university construction, not a policy change, and its named first priorities are led by a stadium.
  A yes vote does not honestly read as "for public education quality". House Bill 247 is kept by
  contrast because it changes the *rules* for capital outlay rather than handing money out.
- **House Joint Resolution 5** (41-26), a constitutional amendment on legislative pay, which goes to
  the voters rather than the governor. A salaried legislature reads either as reducing lawmakers'
  reliance on outside income or as lawmakers voting themselves a salary. No research area settles
  it.
