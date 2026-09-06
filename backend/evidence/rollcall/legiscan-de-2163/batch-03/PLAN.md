# Delaware batch-03 — the measures the Governor signed after the dataset was cut

## Why this batch exists at all

The campaign imports a roll call only when its measure became law. Forty Delaware
divided rolls were parked because the LegiScan dataset, cut on 2026-08-30, shows their
bills at status 3: passed both chambers, waiting on the Governor.

Checking those same 20 bills against Delaware's own action log at legis.delaware.gov
shows **9 of them have been signed**. Seven were signed on 2026-09-02 and 2026-09-03,
after the cut. **Two — HB 233 and HB 310 — were signed on 2026-08-26, four days BEFORE
the cut, and the dataset still shows them unsigned.** So this is not only a timing lag.
LegiScan's Delaware status field is behind the state's own record, and re-downloading
the dataset would not have produced these nine.

The roll calls themselves were already fetched and already sitting in `legislative_votes`
as pending rows. Nothing needed re-fetching. What was missing was the enactment fact,
and Delaware publishes that itself.

## What was imported

Nine measures, 17 rolls. Twenty rolls belong to the nine bills; three are earlier votes
by the originating chamber, superseded when that chamber voted again on the amended text.

| Bill | Signed | Rolls | Subject |
|---|---|---|---|
| HB 150 | 2026-09-03 | 1 | bars civil arrest at courthouses and the Industrial Accident Board |
| HB 233 | 2026-08-26 | 2 | large electricity users must fund their own grid connection |
| HB 310 | 2026-08-26 | 2 | removes a business tax break from large electricity users |
| HB 368 | 2026-09-03 | 2 | limits police cooperation with federal immigration enforcement |
| HB 369 | 2026-09-03 | 2 | puts the Office of Gun Violence Prevention into statute |
| HB 380 | 2026-09-02 | 2 | widens the Delaware Personal Data Privacy Act |
| HB 418 | 2026-09-03 | 2 | compliance path for unserialized firearms |
| HB 94  | 2026-09-03 | 2 | bars police help with immigration actions at sensitive locations |
| SB 300 | 2026-09-03 | 2 | state licensing for firearms dealers |

Superseded and left out: HB 233 roll 1709702, HB 310 roll 1694284, SB 300 roll 1701387.

## What is still parked

Eleven bills / 20 divided rolls are genuinely still with the Governor and stay
`out-of-gate`: HB 133, HB 145, HB 180, HB 222, HB 35, HB 355, HB 430, SB 232, SB 249,
SB 26, SB 3. Some have waited a long time — HB 35 passed the Senate on 2025-06-30 and
has no further action on the official log.

The other 19 out-of-gate rolls are not waiting on anything: 10 passed only one chamber,
5 died, 4 were vetoed.

## The same check on the other live sessions

Alaska and California are the only other campaign sessions still sitting that have
parked divided rolls. Neither yields any work:

- **Alaska HB 10 was vetoed on 2026-08-31 and HB 93 on 2026-08-10.** Both show status 3
  in the dataset. Vetoed measures never enter the gate.
- **Alaska SCR 201, SCR 202 and SCR 28 are concurrent resolutions.** They became
  Legislative Resolves. A governor is never asked to sign one, so their status can
  never move to enacted. These are structurally parked, not waiting.
- **California SB 1196 was presented to the Governor on 2026-08-21 and is still there.
  SB 934 has not been presented. SJR 7 is a joint resolution** and is never signed.

## Counts

17 rolls, 243 records, 0 errors. Dry run planned 243 inserts, the run made 243, and the
re-run reported 243 unchanged. Delaware now holds 888 roll-call records across 60 rolls
and 29 candidates. Production still has zero.
