# Texas 89R — batch 02

The second slice of the Texas roll-call import. Batch 01 took the 25 marquee
votes; this batch takes 27 more from the 743 divided actions it left on the
table.

`rolls.json` is the same selection, machine-readable. `JUDGING.md` records
how each measure was judged.

## The numbers

| | |
|---|---|
| divided floor votes still `pending` after batch 01 | 743 |
| …distinct measures among them | 654 |
| …that became law or were vetoed (not just passed one chamber) | 295 |
| …with at least one divided **house** vote | 264 |
| selected for batch 02 | **27** (14 house, 13 senate) |
| measures covered | 14 |
| estimated records written | **~1,765** (house 14 × ~114, senate 13 × 13) |

Batch 01 wrote 1,620 records from 25 votes. This is the same order of size.
House votes carry the batch: each reaches ~114 candidates against the
senate's 13, because only 14 senate districts are on the Nov-2026 ballot.

## How these 27 were chosen

The same four filters as batch 01, applied in order:

1. **Divided** — the losing side is at least a quarter of the winning side.
   True of all 743.
2. **Consequential** — the measure became law. All 14 here did; nothing
   vetoed and nothing that died in the second chamber. (350 of the 654
   remaining measures passed only one chamber. A vote on a bill that never
   became law needs its description to carry that, which is a harder
   sentence to write well — still deferred.)
3. **Nameable subject** — the bill has a policy subject a voter can
   recognize, and one that maps onto a research area or is honestly
   `general`. This is what rules out the long tail: municipal management
   districts, specialty license plates, one utility's rate case, a single
   county's hospital district.
4. **One roll per measure per chamber**, preferring the third-reading
   passage vote and falling back to the conference committee report when
   that was the divided step. This keeps one senator from collecting three
   copies of one vote.

Where a measure shows only one chamber below, the other chamber passed it
without division.

## The batch

### Elections (4 measures, 8 votes)

| measure | subject | house | senate |
|---|---|---|---|
| HB 493 | ineligibility to serve as a poll watcher | 86-52 | 23-8 (conf.) |
| HB 521 | accommodating voters with a disability | 84-54 | 20-11 |
| HB 5115 | penalty for the crime of election fraud | 88-54 | 21-10 |
| SB 510 | failure of a voter registrar to comply | 109-33 | 20-11 |

`election_integrity` has no Texas coverage at all before this batch.

### Firearms (3 measures, 5 votes)

| measure | subject | house | senate |
|---|---|---|---|
| HB 3053 | municipal and county firearm buyback programs | 85-56 | 20-11 |
| SB 1362 | recognition of extreme risk protective orders | 86-53 | 19-11 |
| SB 2284 | local authority to regulate firearms and weapons | 96-43 | — |

### Religion in public schools (2 measures, 4 votes)

| measure | subject | house | senate |
|---|---|---|---|
| SB 11 | period of prayer and reading of the Bible | 88-48 | 23-7 |
| SB 965 | employee religious speech or prayer while on duty | 86-48 | 20-11 |

### Health coverage and liability (2 measures, 4 votes)

| measure | subject | house | senate |
|---|---|---|---|
| SB 1257 | coverage for gender transition adverse effects | 87-58 | 20-11 |
| HB 3441 | liability of vaccine manufacturers that advertise | 88-31 | 21-10 |

### Benefits, markets, and public funds (3 measures, 6 votes)

| measure | subject | house | senate |
|---|---|---|---|
| SB 379 | SNAP purchases of sweetened drinks and candy | 90-37 | 22-8 |
| SB 2337 | regulation of proxy advisory services | 92-50 | 20-11 |
| SB 21 | Texas Strategic Bitcoin Reserve | 101-42 | 23-8 (conf.) |

Exact roll numbers, dates, and the specific question for each are in
`rolls.json`.

## What this batch still leaves out

- **716 divided actions.** Mostly single divided votes on narrow or local
  bills — a management district's bond authority, an alcohol sales
  boundary, one county's composting rules. Real votes, weak reading.
- **350 measures that passed only one chamber**, per filter 2.
- **The 9 vetoed measures with divided votes**, including SB 974 and
  HB 2243 (both teacher-related). A vetoed bill needs the veto in the
  description to avoid implying a law that does not exist.
- **SB 3**, the vetoed hemp/THC ban that got the most attention of the
  session. It still has no divided votes, so the gate still excludes it.
