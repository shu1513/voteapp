# Texas 89R — batch 01 (proposed)

The narrowing decision for the Texas roll-call import. 813 divided votes is
months of review at Ohio's pace, so this batch takes 25 of them. **Nothing
here is judged yet** — this is the slice to judge, for sign-off before any
writing starts.

`rolls.json` is the same selection, machine-readable.

## The numbers

| | |
|---|---|
| divided floor votes in the session | 813 |
| …distinct actions (duplicate ids collapsed — see CODE-FINDINGS §1) | 768 |
| selected for batch 01 | **25** (13 house, 12 senate) |
| measures covered | 14 |
| duplicate roll ids collapsed inside the selection | 8 |
| estimated records written | **~1,640** (house 13 × ~114, senate 12 × 13) |

For scale: the Ohio pilot judged 24 roll calls and wrote 1,330 records. This
batch is deliberately the same size. Texas house votes are the value here —
each reaches ~114 candidates against the senate's 13, because only 14 senate
districts are on the Nov-2026 ballot.

## How these 25 were chosen

Four filters, applied in order:

1. **Divided** — the phase-2 gate: the losing side is at least a quarter of
   the winning side. Already true of all 813.
2. **Consequential** — the measure became law, was vetoed, or is a joint
   resolution the legislature adopted. A divided vote on a bill that died in
   the other chamber is a real vote, but it is a weaker thing to tell a
   voter, so it waits.
3. **Nameable subject** — the bill has a clear policy subject that maps onto
   one of the 27 research areas. This is what makes an honest two-sentence
   description possible; it is the same reason the federal expansion left
   appropriations votes unjudged.
4. **One roll per measure per chamber** — preferring the passage vote
   ("Read 3rd time"), falling back to the conference report or concurrence
   when only that step was divided. This is what keeps the same senator from
   collecting three copies of one vote.

## The batch

### Proposed constitutional amendments (5 votes, 4 measures)

These four propose amendments to the **Texas** constitution. Each needs
two-thirds of each chamber, and once adopted it goes to the voters — so a
divided amendment vote is worth reading twice. All four carry titles
beginning "Proposing a constitutional amendment", which is the test used
here; `rolls.json` marks them `measure_class:
proposed_constitutional_amendment`.

| measure | chamber | roll | date | tally | question |
|---|---|---|---|---|---|
| HJR 2 | house | 1559942 | 2025-04-29 | 112-29 | Adopted |
| HJR 4 | house | 1532010 | 2025-04-01 | 111-31 | Adopted |
| HJR 34 | senate | 1575590 | 2025-05-21 | 23-8 | Adopted |
| SJR 18 | house | 1559068 | 2025-04-28 | 104-26 | Adopted as amended |
| SJR 18 | senate | 1520079 | 2025-03-18 | 24-6 | Read 3rd time |

Subjects: prohibiting death taxes (HJR 2), barring an occupation tax on
certain securities transactions (HJR 4), an ad valorem exemption for border
security infrastructure (HJR 34), and barring a tax on capital gains
(SJR 18).

Where only one chamber appears, the other chamber adopted it without
division.

### Article V convention application (2 votes, 1 measure)

**HJR 98 is not a proposed Texas constitutional amendment and never went on
a ballot.** Its title begins "Applying to the Congress of the United States
to call a convention under Article V" — it is an application asking Congress
to convene a convention to amend the **federal** constitution, for fiscal
restraints, limits on federal power, and federal term limits. It is a joint
resolution like the four above, and LegiScan gives it the same bill type,
which is exactly why it is easy to file in the wrong drawer.

| measure | chamber | roll | date | tally | question |
|---|---|---|---|---|---|
| HJR 98 | house | 1558765 | 2025-04-28 | 86-57 | Adopted |
| HJR 98 | senate | 1568736 | 2025-05-08 | 17-14 | Adopted |

Kept in the batch — a convention application is a real and contested
position, and the 17-14 senate vote is the closest in this batch. But
**its judgment must describe it as an application to Congress, never as a
constitutional amendment placed before voters.** Getting that wrong would
put a false sentence on roughly 103 candidates at once. `rolls.json` marks
it `measure_class: article_v_convention_application` so the distinction
survives into the judging step.

### Marquee enacted bills (18 votes, 9 measures)

| measure | subject | house | senate |
|---|---|---|---|
| SB 2 | education savings accounts (school vouchers) | 86-61 | 19-12 |
| SB 8 | sheriff agreements with ICE | 86-47 | 20-11 |
| SB 12 | parental rights; DEI in public schools | 88-47 | 20-11 |
| SB 13 | school library materials and advisory councils | 81-48 | 23-8 |
| SB 15 | residential lot size and density rules | 86-43 | 24-7 |
| SB 17 | foreign ownership of real property | 85-57 | 24-7 |
| SB 33 | public funding of abortion assistance | 87-58 | 22-9 |
| SB 37 | governance of public universities | 83-47 | 20-11 |
| SB 2972 | expressive activity on university campuses | 97-39 | 21-10 |

All nine became law. Exact roll numbers, dates, and the specific question
for each are in `rolls.json`.

## What this batch deliberately leaves out

- **The other 743 divided actions.** Most are single divided votes on
  narrow or local bills — manufactured housing rules, an alcohol sales
  boundary, a utility district's bond authority. Real votes, weak reading.
- **350 measures that passed only one chamber.** Worth revisiting, but a
  vote on a bill that never became law needs its description to carry that,
  and that is a harder sentence to write well.
- **Vetoed bills.** Nine measures with divided votes were vetoed. Only SB 974
  and HB 2243 (both teacher-related) are notable, and a vetoed bill needs
  the veto in the description to avoid implying a law that does not exist.
  Deferred rather than rushed.
- **SB 3**, the hemp/THC ban that was vetoed and got the most attention of
  any bill this session. It has **no divided votes** — it passed both
  chambers by wide margins, so the divided gate excludes it. Worth knowing
  before someone asks why it is missing.

## After sign-off

Judging follows the Ohio procedure: read the bill text and analysis for each
of the 25, write the yea and nay sentences and issue labels into a committed
`judgments.json`, then `rollcall:judge` and `rollcall:legiscan:import`. No
AI calls anywhere in that path — the session and the reviewer are the judge.
