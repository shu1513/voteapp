# Alabama 2019-2022 (the previous term) — findings that outlive these batches

Filed under the 2021 regular session because that is the largest dataset of the term, but every
finding here applies to all six registered sessions of it.

## 1. The caption history is not a straight line

Alabama's floor-question captions now have four eras in the registry, and they do not evolve in one
direction:

| Term | Passage caption | Roll call number in the desc |
|---|---|---|
| 2019-2022 | `Motion to Read a Third Time and Pass` | ` Roll Call 215`, no hyphen |
| 2023 | `Read a Third Time and Pass`, no `Motion to` | none at all |
| 2024 | both of the above, in one session | ` - Roll Call 215` on the modern half |
| 2025-2026 | `Motion to Read a Third Time and Pass` | ` - Roll Call 215` |

So this term reads more like 2025 than 2023 does, even though 2023 sits between them. **Chronology
is no guide to which vocabulary a session speaks.** Survey each one.

The missing hyphen is the trap that costs time rather than correctness. A desc histogram folded with
a hyphen-only stripper (`\s*-\s*Roll Call \d+`) leaves every roll in its own family: 2,305 "families"
for the 2021 regular session instead of 292. The patterns themselves are unanchored at the end and
match either way.

## 2. A roll call filed under a same-numbered bill in a DIFFERENT session

The 2022 regular session dataset contains four roll calls that belong to the **2021 first special
session**: rc1109543, rc1109544, rc1109545 and rc1109546, dated 2021-09-29 and 2021-10-01. They are
attached to the 2022 session's HB 2, which is a bill about assault against a first responder. The
votes they actually record are on the 2021 special session's HB 2, which is about mandatory
supervised release from prison. Two different bills, same number, different sessions.

The same four votes also appear correctly in the 2021 first special session's own dataset, under
different roll call ids (rc1109392, rc1109393, rc1109394, rc1109471) with identical dates and
tallies. There is **no roll call id collision** anywhere across the fifteen Alabama sessions checked,
so an id-based check would not have caught this.

**Two of the four are divided**, 77-23 and 24-6, and both would have entered the 2022 batch as votes
on the wrong bill, with descriptions about first-responder assault attached to members who were
actually voting on parole.

What caught it is the roll-attribution check the 2026 session made necessary: a roll's printed
`Roll Call <n>` must appear in its own bill's history. The 2022 HB 2's history uses roll calls 278
to 282, not 11 and 19.

### The cheap generalisation of that check

Comparing each roll's date against the session's own sitting dates finds the same contamination
faster, and finds more besides. The 2022 dataset holds **34 rolls dated outside 2022**:

| Date | Rolls | What they are |
|---|---|---|
| 1998-02-20 | 29 | a corrupted date; none is divided |
| 2021-09-29 and 2021-10-01 | 4 | the foreign special-session rolls above |
| 1969-12-31 | 1 | a null date printed as an epoch boundary |

Only the four foreign rolls matter for selection, and only two of those are divided. The other five
registered sessions have **zero** rolls dated outside their own sitting period. Run this check on
any new session: it is one line and it catches a class of error that tallies and member lists cannot.

## 3. Rolls whose stated tally does not match their own member list

Six rolls across three sessions state a `yea` count that the attached member list cannot support,
and the parser rejects each one rather than storing it:

| Session | Roll | Claimed yeas | Members listed |
|---|---|---|---|
| 2019 regular | 826544 | 51 | 25 |
| 2021 regular | 996001 | 106 | 52 |
| 2021 regular | 996025 | 96 | 46 |
| 2021 regular | 1025051 | 88 | 54 |
| 2022 regular | 1124583 | 141 | 71 |
| 2022 regular | 1185411 | 87 | 29 |

Five of the six claim close to exactly twice the members listed, which points at a double-count in
the feed rather than a missing member list. A seventh row, SB 124 of 2022, is rejected for a
different reason: it has no desc at all, and its date is 1969-12-31.

This matters because the modern Alabama sessions have **zero** tally mismatches. Do not carry the
"cleanest tier" description backwards to this term.

## 4. Two captions truncated to nothing, and one that lost a letter

- HB 192 of 2021 carries `Motion to Roll Call 6`. The bill history shows the same stub. That
  measure's real passage is Roll Call 8, so the stub is an amendment vote whose caption was cut off.
  Excluded by pattern.
- HB 520 of 2022 carries `otion to Read a Third Time and Pass Roll Call 881`, with the leading M
  missing from **both** the desc and the matching history line. This one is a real passage vote,
  24-0 on an Etowah County local act, and it is kept as passage by an explicit pattern rather than
  dropped as an unknown question.

The general lesson: a desc that fails to match is not always noise. Look at each one against the
bill history before deciding whether to exclude it or widen a pattern.

## 5. The Budget Isolation Resolution, in a fifth spelling

Alabama's Budget Isolation Resolution is the procedural vote taken before most bills are considered
ahead of the budget. It is not a vote on the measure. Across the registry it now appears as:

| Session | Spelling |
|---|---|
| 2021 and 2022 regular | `HBIR:/SBIR: <sponsor> motion to Adopt` |
| 2023 | not recorded at all; taken by voice |
| 2024 | `Third Reading in House of Origin` and `Third Reading House of Origin` |
| 2025 and 2026 | `HBIR:/SBIR: Passed by House of Origin` and `Third Reading in House of Origin` |

This term's spelling is caught by the existing `^[hs]bir:` rule, so it needed no new pattern. The
2019 and 2020 sessions and all four special sessions of the term record none of these votes at all.
