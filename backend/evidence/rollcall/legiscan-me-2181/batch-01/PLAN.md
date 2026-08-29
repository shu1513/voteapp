# Maine batch-01 — selection

**24 rolls / 12 measures / 1,516 records**, drawn from the 433 divided rolls on
measures that became law (215 measures; the count after the bare-`Recede`
review fix). Every measure appears once per chamber.

## The five filters

1. **Divided** — loser ≥ 25% of winner (the phase-2 gate). Maine gives this for
   free: 1,450 of 1,580 recorded rolls qualify, because the state only takes a
   roll call when members demand one.
2. **Became law** — LegiScan status 4. The 7 vetoed bills are excluded: all 8
   veto-override rolls FAILED, so none of them became law (see
   `../CODE-FINDINGS.md` §2).
3. **A nameable subject** that maps to a research area.
4. **One roll per measure per chamber.** Maine's rule, which differs from every
   earlier state: **prefer the `Enactment` roll** — Maine's true final passage,
   taken on the enacted text — and fall back to the report-acceptance roll when
   enactment drew no roll call. Never take a roll that FAILED, and where the
   other chamber amended afterwards, the **recede-and-concur** roll is the one
   cast on the final text (LD 1126 House, LD 598 House, LD 1016 Senate).
5. **A defensible for/against direction.** Anything that would land on
   `general` is dropped rather than imported.

## Selected

| Measure | Subject | Area / direction | Rolls |
|---|---|---|---|
| LD 1126 | serial numbers on firearms; undetectable firearms | gun_control / for | H 1594835, S 1595032 |
| LD 1971 | state and local police vs. federal immigration enforcement | immigration / for | H 1592017, S 1592547 |
| LD 538 | mifepristone labels may name the facility, not the prescriber | womens_reproductive_rights / for | H 1574213, S 1574972 |
| LD 556 | bars municipal bans on a heating or energy system | environment_and_public_health / **against** | H 1592313, S 1594826 |
| LD 1868 | new Class III clean-energy portfolio standard | environment_and_public_health / for | H 1590978, S 1591936 |
| LD 61 | employer electronic-surveillance notice and limits | data_privacy / for | H 1579051, S 1579868 |
| LD 517 | synthetic-media disclosure in campaign ads | election_integrity / for | H 1658072, S 1661402 |
| LD 1937 | hospital charity care and financial assistance | healthcare_affordability / for | S 1589984, H 1591741 |
| LD 598 | minimum pay when a shift is cancelled | corporate_accountability / for | H 1587268, S 1588827 |
| LD 2231 | manufactured-housing lot rent and mediation | housing_affordability / for | H 1678714, S 1679427 |
| LD 1016 | transfer fee funding park preservation | housing_affordability / for | H 1587988, S 1588941 |
| LD 2176 | landlord disclosure of tenant personal information | data_privacy / for | S 1678354, H 1678710 |

Nine areas, one of them scored in the **opposite** direction from the rest of
the batch (LD 556), and both chambers on every measure.

## Deferred

- **LD 427** (municipal parking minimums, housing_affordability) — the House
  rejected the report 63-83, receded and concurred 72-70, then FAILED enactment
  71-74 before receding again 82-65. Which roll is the chamber's decision needs
  the journal, not the feed. Batch-02.
- **LD 713** (data centers out of the business equipment tax exemption and the
  Dirigo incentives program) — dropped under filter 5. Which industries keep a
  tax exemption maps to no area with an honest direction; the same reasoning
  that keeps appropriations out.
- **LD 93** (expanding the universal immunization program to adults) — a real
  divided-and-enacted measure, held for a user direction call rather than
  assumed, per the standing rule on vaccine- and fluoride-adjacent directions.
- 409 divided-and-enacted status-4 rolls remain; the ledger is
  `../survey/divided-enacted-worklist.tsv`.
