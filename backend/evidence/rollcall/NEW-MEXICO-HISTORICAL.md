# New Mexico roll-call backfill — the nine historical sessions

The three modern New Mexico sessions were imported first: 2025 regular (`legiscan-nm-2187`),
2026 regular (`legiscan-nm-2251`) and the October 2025 special (`legiscan-nm-2227`). This document
covers the nine older sessions worked afterwards, from 2020 through 2024.

Every New Mexico House seat is on the November 2026 ballot, so a member who served in an older
session and is running again carries that vote forward. The Senate is not on the 2026 ballot,
because senators serve four-year terms and were last elected in 2024. Senate rolls therefore fan
out to nobody and are fetched but never judged.

## Sessions

| Session | LegiScan id | Divided and enacted House rolls | Imported | Dropped | Held |
|---|---|---|---|---|---|
| 2024 regular | 2126 | 19 | 11 | 3 | 5 |
| 2023 regular | 2030 | 36 | 20 | 7 | 9 |
| 2022 regular | 1961 | 11 | 6 | 0 | 5 |
| 2022 third special | 1977 | 1 | 0 | 0 | 1 |
| 2021 second special | 1967 | 2 | 0 | 2 | 0 |
| 2021 regular | 1812 | 28 | 15 | 7 | 6 |
| 2021 first special | 1830 | 3 | 1 | 1 | 1 |
| 2020 first special | 1750 | 5 | 3 | 1 | 1 |
| 2020 regular | 1731 | 18 | 13 | 5 | 0 |
| **Total** | | **123** | **69** | **26** | **28** |

## The source problem, and what replaced it

nmlegis.gov, the state legislature's own website, stopped answering during this work. It sits behind
a Check Point web firewall: the connection opens in about 25 milliseconds and then no HTTP response
ever comes back, on every address including the site's front page. The block did not lift.

That site holds the three things this campaign normally relies on: the enrolled act, the Legislative
Finance Committee's fiscal impact report, and the official roll-call sheet. Two of the three were
replaced from LegiScan, which stayed reachable throughout.

**Enrolled acts.** LegiScan's `getBillText` serves its own copy of each bill's enrolled text, the
version the governor signed. All 95 texts were pulled that way and read directly, which is a better
source than the fiscal impact report anyway: in the modern sessions, five of 29 fiscal impact
reports described a version of the bill that never became law.

**The tally audit.** Every New Mexico tally has to be checked, because LegiScan's stored roll-call
numbers have twice been wrong (Senate Bill 3 in 2187 and Senate Bill 151 in 2251, both found by hand
against the state's official sheet and both held out of the import). Without the official sheet a
different check was needed.

LegiScan parses a bill's *history* separately from its roll-call member list, and the history line
carries its own numbers: `Passed in the House of Representatives - Y:44 N:23`. Comparing the two is
an independent check inside the same feed. It reproduces both known defects exactly — for Senate
Bill 3 the history reads 44-23, which is what New Mexico's official sheet says, where the stored
roll call says 42-23. **A roll is imported only when the history line and the roll call agree.**

Of the 123 pool rolls, 95 agree and 28 do not. The 28 are held, not imported. They are not lost:
when nmlegis.gov answers again, each can be checked against the official sheet and imported with
the correct tally.

This check is weaker than the official sheet. Agreement between the two does not prove the number is
right, only that two separately parsed copies say the same thing. It caught every defect found by
hand, which is the strongest claim available for it.

## What is not available

LegiScan carries **no enrolled text for New Mexico special sessions** — 1750, 1830 and 1967 have the
introduced bill only. Four measures were judged from introduced text: House Bill 5, Senate Bill 8 and
Senate Bill 4 in 1750, and House Bill 2 in 1830. Each of those bills was introduced and passed within
two days, so the text is close to the law, and each description was kept to the core of the measure,
which does not change between versions. This limit is recorded here rather than hidden.

## Selection

The same five filters as every other session in this campaign: enacted (LegiScan status 4), a kept
bill type (`B`, `JR`, `JRCA`, `CA`), a House final passage roll, a divided vote (the losing side at
least 25 percent of the winning side), and a defensible for-or-against direction. The 26 drops are
listed in each session's `batch-01/PLAN.md`.

Appropriation bills are dropped by rule, as in the modern sessions: no research area carries an
honest direction on a vote to fund the government. That covers the general appropriation acts, the
feed bills and the capital outlay bills.
