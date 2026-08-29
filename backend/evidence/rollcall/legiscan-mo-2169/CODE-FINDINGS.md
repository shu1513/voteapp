# Missouri (LegiScan 2169) — findings recorded during the campaign

## 1. The Missouri House prints up to four DIFFERENT questions under one desc

**This is the state's binding hazard and it is not visible anywhere in the LegiScan feed.**

A Missouri House desc is the *calendar heading* plus the bill's substitute chain — e.g.
`House: HBs WITH SENATE AMENDMENTS SS SCS HB 225, A.A., E.C.`. On one calendar day, that identical
string can cover:

1. the **motion for the previous question** (the debate cutoff),
2. **House Adopts** — concurrence in the Senate substitute,
3. **Truly Agreed To And Finally Passed** — the vote on the enacted text,
4. the **emergency clause** — whether the act takes effect at once.

Only 3 (or, on a bill's first trip, `Third Read and Passed`) is a vote on the measure. Measured
examples, all confirmed against the official roll-call PDFs and the bill history:

| bill | date | rolls under one desc |
|---|---|---|
| HB 225 | 2025-05-08 | 92-45 previous question · 89-32 House Adopts · **88-26 Truly Agreed** · 88-34 emergency clause **defeated** |
| HB 495 | 2025-03-12 | 111-42 House Adopts · **113-39 Truly Agreed** · 113-36 emergency clause adopted |
| HB 567 | 2025-03-13 | **96-51 Third Read and Passed** · 97-49 previous question · 84-62 emergency clause **defeated** |
| HJR 73 | 2025-04-17 | 104-49 previous question · **103-51 Third Read and Passed** |

The Senate has its own `Senate: Emergency Clause` desc (excluded by the config), but the House does
not: its emergency-clause votes wear the passage desc. Likewise `House: General PQ` exists as a desc
but covers exactly **one** roll all session — every other previous-question motion is printed with
the pending question's calendar heading, exactly as Tennessee's House does.

### The recipe that resolves it

The House publishes a PDF for **every** roll call:
`https://documents.house.mo.gov/billtracking/bills251/rollcalls/<session day>.<n>.pdf`, linked from
`https://house.mo.gov/BillContent.aspx?bill=<BILL>&year=2025&code=R&style=new`. Its header is

```
Session Day : 56        HJRs FOR THIRD READING        Date : 4/17/2025
Roll Call   : 6         HCS HJR 73
                        SEITZ (156)
                        REPRODUCTIVE HEALTH CARE
                        2454H.05P
                        PREVIOUS QUESTION          <- present ONLY on a non-passage question
```

* A header that stops after the **LR number** is the calendar's own question (passage / adoption).
* **Any further line** names a different question (`PREVIOUS QUESTION`, an `HA n` amendment, …).
* The **LR number is the per-roll version check**: `2454H.05P` is the perfected text, `1683S.04F` the
  Senate substitute, `1382S.06S` the conference substitute.
* Match a PDF to its LegiScan roll on `(Total Yes, Total No, Total Present)` = `(yea, nay, nv)`.

**Four measures were dropped from batch-01 because their only divided House roll is a previous-
question motion: HB 199, SB 105, SB 43, SB 348.**

## 2. LegiScan's Missouri tallies are exact, but the feed is INCOMPLETE

Across the 60 House roll-call PDFs pulled for the 18 candidate measures, 55 match a LegiScan roll
exactly on `(yea, nay, nv)`. The five with no LegiScan row at all are the **conference-committee
*report* adoption** rolls (HB 595 064.002, SB 160 069.010, SB 63 067.013) and two **floor amendment**
rolls (SB 105 069.014, SB 43 067.015). So LegiScan never mis-states a Missouri tally; it simply does
not carry every roll the House took. Anything that depends on a *complete* roll inventory must read
the House bill page, not the dataset.

## 3. `Senate: Adoption` is one desc over two questions

Nine of its ten rolls are ceremonial concurrent/simple resolutions (rejected earlier as excluded
measure types); the tenth is the Senate adopting the **HB 595 conference committee report**, 22-11
(`Senate Adopts Conference Committee Report (S) - AYES: 22 NOES: 11`). The config deliberately
leaves the desc unmatched so the roll **surfaces** for review instead of being classified by guess.
The Senate's own `Senate: Third Reading` for HB 595 the same day (23-10) is the Truly Agreed To And
Finally Passed vote, so nothing of value is lost by surfacing rather than keeping it.

## 4. Roster coverage, not votes, is Missouri's binding constraint on the Senate side

The Nov-2026 pool holds 111 of 163 House districts (204 candidates) but only **5 of 34 Senate
districts** (11 candidates). Fan-out is therefore house median 81 / senate median 3. Missouri's
Senate is staggered (~17 seats up in 2026), so most of that gap is a roster gap, not the staggering
— a Missouri **Senate roster campaign** would be worth more than any further roll-call batch.
