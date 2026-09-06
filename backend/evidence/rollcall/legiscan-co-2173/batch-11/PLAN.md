# Batch-11 — the bills that did not become law

The first ten batches covered divided votes on bills Colorado enacted. This
batch covers the other side of the 2025 regular session: divided votes on bills
that **failed**. A member's vote on a bill that died says as much about them as
a vote on one that passed, and in a state where one party holds everything, it
is often where the real disagreement shows.

## The pool

Applying the same filters — kept floor questions, bills only, one roll per
measure per chamber, the chamber's last floor vote, the divided gate — the
not-enacted pool is **41 divided rolls on 34 measures**, out of 57 chamber
rows. The other 16 chambers are superseded, because their last floor vote was
lopsided.

**This corrects a number in SWEEP.md.** That file said 249 divided rolls
remained on measures that did not become law. That count included question
types the Colorado config excludes — second readings, floor amendments, the
previous question. Counting only the questions this pipeline keeps, the pool is
49 divided rolls, and 41 after the last-vote filter.

| | rolls |
|---|---|
| imported | 22 |
| dropped, each with a written reason | 19 |
| superseded | 16 |
| **total chamber rows** | **57** |

`survey/not-enacted-worklist.tsv` carries every row with its disposition.

## How these bills ended

Four endings, and each roll's description says which one applies:

- **Vetoed by the governor** — 7 measures. Colorado had one-party control, so
  these are vetoes of the majority's own bills.
- **Rejected on the floor** — 3 rolls lost outright.
- **Killed in a committee of the second chamber** — 5 measures.
- **Left on the calendar when the session ended** on 7 May 2025 — 3 measures.

## What was imported

| measure | what it would have done | area | a yes vote is |
|---|---|---|---|
| HB 1004 | ban rent-setting software shared between landlords | housing affordability | for |
| HB 1011 | rules for child care centers owned by investment firms | corporate accountability | for |
| HB 1026 | end prison health-care copayments | healthcare affordability | for |
| HB 1079 | put school and special districts under the state ethics board | anti-corruption | for |
| HB 1122 | require a licensed driver aboard a self-driving truck | public safety | for |
| HB 1147 | cap city-court jail terms at the state equivalent | civil rights | for |
| HB 1158 | ban ads in school digital research collections | public education | for |
| HB 1169 | allow housing on faith and school land | housing affordability | for |
| HB 1235 | jury trials in eviction cases | civil rights | for |
| HB 1277 | warning labels on fuel | environment and public health | for |
| HB 1282 | bar card fees on sales tax and tips | corporate accountability | for |
| HB 1291 | ride-hailing driver checks and hour limits | corporate accountability | for |
| SB 77 | slow open-records answers for everyone but the press | anti-corruption | **against** |
| SB 86 | duties on social media companies | corporate accountability | for |
| SB 124 | make nonprofit hospitals spend drug discounts on patients | healthcare affordability | for |
| SB 141 | excuse small towns from the home energy code | environment and public health | **against** |
| SB 157 | ease the bar for targeted people to sue over scams | corporate accountability | for |
| SB 185 | let homeowners sue builders over defects | corporate accountability | for |

18 measures, 22 rolls, 620 candidate records across 52 candidates.

## What was dropped

Sixteen measures, grouped by cause:

- **A fee, loan or enterprise mechanic** — HB 1268, HB 1302, HB 1303, SB 011.
- **Runs both ways inside its area** — HB 1123, HB 1187, HB 1220, SB 160.
- **No research area fits** — HB 1065, HB 1078, HB 1151, SB 064, SB 132, SB 284.
- **Housekeeping** — SB 251.
- **No labor area exists** — SB 005, the collective bargaining bill. Colorado's
  24 selectable areas include none for labor or workers' rights, so the
  session's most-watched veto has no honest home. Four Nevada measures were
  dropped for the same reason.

One more roll was dropped on its own: **SB 124's Senate roll**, because the
Senate voted a materially different bill from the one the House passed. The
House roll is imported.
