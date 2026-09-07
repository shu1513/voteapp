# Colorado 2026 regular session (LegiScan 2243) — complete

Every divided floor vote in the session has been dispositioned. This file is the
audit trail for what was judged, what was not, and why.

## The numbers

| | count |
|---|---|
| chamber rows in the worklist | 937 |
| divided | 383 |
| superseded (that chamber's last vote was not divided) | 553 |
| unusable (feed tally contradicts its own member list) | 1 |
| **divided rolls judged and imported** | **250** |
| divided rolls deliberately not judged | 133 |
| distinct measures judged | 159 |
| **candidate records written** | **7,442** across **55 candidates** |
| research area tags | 5,173 |

Of the 326 divided rolls on enacted measures, 201 were judged. Of the 57 divided
rolls on measures that did not become law, 49 were judged.

## Reconciliation

Every batch reconciles three ways and every convergence re-run reports all rows
unchanged:

| batch | subject | measures | rolls | records |
|---|---|---|---|---|
| 01 | marquee measures | 11 | 21 | 579 |
| 02 | energy and environment | 12 | 17 | 523 |
| 03 | health care | 11 | 14 | 492 |
| 04 | housing and the cost of living | 12 | 20 | 598 |
| 05 | courts, jails and policing | 12 | 22 | 620 |
| 06 | schools and children | 11 | 17 | 532 |
| 07 | work, business and consumers | 12 | 22 | 629 |
| 08 | elections, government, getting around | 10 | 18 | 517 |
| 09 | homes, care and consumer protection | 8 | 15 | 418 |
| 10 | crime, courts and the road | 12 | 19 | 556 |
| 11 | the last enacted measures with a stance | 11 | 16 | 519 |
| 12 | SB 80, filed under the wrong status | 1 | 1 | 42 |
| 13 | the vetoed measures | 10 | 19 | 503 |
| 14 | bills that died, part one | 12 | 13 | 398 |
| 15 | bills that died, part two | 14 | 16 | 516 |

Session totals: **7,442 ledger inserts = 7,442 rows across 55 candidates;
5,173 yea-side rows = 5,173 tags; 250 approved roll calls.**

## Why 133 divided rolls were not judged

Every one falls into a group with no defensible for-or-against stance:

- **eleven supplemental appropriations** (HB 1152 to HB 1171) and the
  controller's over-expenditure allowance (HB 1178);
- **twelve sunset continuations** (HB 1181, 1183, 1184, 1188, 1194, 1208, 1214,
  1280, 1287, 1324, 1326, 1344);
- **the long bill**, HB 1410;
- **about thirty cash fund transfers, TABOR mechanics and appropriation
  adjustments**;
- **twenty administrative or study-only measures**;
- **nine measures dropped after their text was read**: SB 124 (vehicle bill),
  HB 1411 and SB 181 (budget trims of a benefit program, no single direction),
  HB 1288 (a working group), HB 1353 (narrows and broadens testing at once),
  HB 1113 (a 69-section election omnibus), SB 116 (a tax rise for one group and
  a spending cut for another), HB 1313, HB 1360, HB 1355 (fund mechanics),
  HB 1221, HB 1222, HB 1330;
- **one measure whose chamber is unusable**: HB 1374, where the House voted on
  text the Senate then amended and the feed records no House concurrence;
- **one measure whose fate the feed does not record**: SB 135.

## The two open items

1. **SB 135** — the K-12 funding bill. Both presiding officers signed it on
   20 May 2026; the feed records no "Sent to the Governor" and no governor
   action, and it has an Enrolled but no Chaptered text. It needs checking
   against the Colorado Secretary of State's session laws. Its two divided rolls
   are House 42-21 on 9 May and Senate 23-12 on 12 May.
2. **Production promotion** — none of these 7,442 records is in production.

## Session-specific traps, all confirmed in the run

1. **Every bill text is dated 1969-12-31** — 3,562 of 3,681. The version check
   was rebuilt around the correctly dated action history. It cleared 248 rolls,
   surfaced two conference-committee false alarms in batch-08, and cost one
   measure (HB 1374) in batch-09.
2. **LegiScan's `title` can be a gutted bill's old name.** SB 124 and HB 1107
   were both caught this way. Triage on `description`, then still read the act.
3. **LegiScan's `status` field can be wrong.** SB 80 is marked failed and was
   signed into law on 4 June 2026. Check each bill's own history for a governor
   action before believing the status.
4. **Ten rolls are rejected because the feed's tally contradicts its own member
   list.** HB 1151's House chamber is unusable as a result.
5. **No `_f1` fiscal note URLs exist for 2026.** SB 131 has no Enrolled text at
   all; its Chaptered print was downloaded directly from the legislature.
6. **Same-day twin rolls are a Colorado 2026 pattern, not a one-off.** Four
   measures — HB 1312, SB 190, SB 121 and SB 166 — record two floor votes of the
   same kind on one day with different tallies. The judge caught every one. The
   roll accounting for the full chamber, or failing that the later roll, is
   judged and the twin acknowledged.
7. **Print position depends on the chamber of origin.** For a Senate bill the
   Senate's votes want the Engrossed prints and the House's want the Amended
   ones. Getting this backwards fetched the wrong chamber's text for three bills
   in batch-14 before it was caught.
