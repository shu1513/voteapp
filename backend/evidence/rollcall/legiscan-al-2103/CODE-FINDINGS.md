# Alabama 2024 (LegiScan 2103) — findings that outlive this batch

## 1. One session, two caption systems, and a string that reverses meaning

The 2024 regular session is where the Alabama feed changed how it names floor questions, and it did
not change all at once. Both naming systems appear in the same session, on bills finished weeks
apart and sometimes on the same day.

The dangerous part is not that there are two systems. It is that **`Passed House Of Origin` names the
passage vote in 2024 and a Budget Isolation Resolution in 2025**. A Budget Isolation Resolution is
the procedural vote Alabama takes before considering most bills ahead of the budget; it is not a vote
on the measure and must never become a candidate record.

### How the two were told apart

Not by the tally, and not by whether two rolls share a member list. Both tests are misleading here,
because a Budget Isolation Resolution and the passage that follows it minutes later usually draw the
same members voting the same way. Measured: of 351 rolls captioned `Third Reading in House of
Origin`, only 128 share an identical member list with another roll on the same bill. In 2025 the
equivalent figure was 698 of 698 — near-certain double printing. In 2024 it is a coin flip, so
pairing proves nothing.

What settles it is the **bill history**, which names each action and, for the real floor votes, gives
a roll call number. SB 47:

```
history:  2024-02-27 S  Third Reading in House of Origin
          2024-02-27 S  Motion to Read a Third Time and Pass - Adopted Roll Call 108
rolls:    2024-02-27 S  rc1396906  34-0  "Third Reading House of Origin"
          2024-02-27 S  rc1396907  34-0  "Passed House Of Origin"
```

The history records two actions: the resolution, then passage. The dataset stores two rolls under
System A captions. The mapping is forced — `Third Reading House of Origin` is the resolution and
`Passed House Of Origin` is passage — and it is the opposite of the 2025 reading.

### What it costs to get wrong

Treating `Passed House Of Origin` as a Budget Isolation Resolution drops 176 real passage votes and
takes the divided-and-enacted pool from 10 rolls down to 4, losing the CHOOSE Act, the absentee
voting bill and the diversity-programme ban — three of the session's most contested measures.

## 2. The check that failed in 2026 passes here

The 2026 regular session files some physical votes under more than one bill, so a roll's printed roll
call number can belong to a different bill's history (`../legiscan-al-2218/CODE-FINDINGS.md` §1). The
same test was run over every divided roll in this session: **29 of 29 pass**. System A rolls print no
roll call number, and every System B roll's number appears in its own bill's history. The 2026 defect
did not exist yet in 2024.

## 3. What this means for any future Alabama session

Three sessions, three vocabularies, and one string that means opposite things in two of them. The
registry rule that a session's patterns come only from that session's own measured survey is not
bureaucracy here — it is the only thing standing between a correct import and a silently halved one.
Before adding any Alabama session, survey it, fold its description families, and check a sample of
bills against their own histories.
