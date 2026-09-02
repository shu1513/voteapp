# Alabama roll-call votes — LegiScan session 2262 (2026 First Special Session)

Alabama's May 2026 special session, called to redraw districts. Registered as `AL-2262`, sharing the
one Alabama vocabulary definition with the two regular sessions.

**Surveyed and fetched, but nothing is judged or imported yet.** See "What is left" below.

## The session

9 bills, 9 roll calls, 140 people, May 4 to May 12 2026. It is entirely conventional: every
description matches a kept or excluded pattern already defined for Alabama, with **nothing left
unmatched**, which is why the registry entry adds no rules of its own. No roll call id collides with
either regular session, and the people file is **identical** to the 2026 regular session's, so that
session's crosswalk is reused here unchanged (140 entries, 122 mapped, 421 member votes matched, no
member without an entry and none out of scope).

Fetch stored 8 rows (9 minus 1 identity duplicate): 3 floor votes and 5 excluded questions.

## The pool

Only 3 of the 9 rolls are floor votes on a measure, and **all three are divided and on measures that
became law**:

| Measure | Roll | Vote |
|---|---|---|
| HB 1 special primary elections after congressional redistricting | House 1694623, 2026-05-06 | 75-29 |
| HB 1 | Senate 1695918, 2026-05-08 | 27-8 |
| SB 1 special primary elections after State Senate redistricting | Senate 1694713, 2026-05-06 | 26-7 |

The other six rolls are three failed amendment adoptions and two previous-question motions, all
correctly excluded, plus one identity duplicate.

## ⚠ Feed gap: SB 1's House passage vote is missing

SB 1's bill history records `Motion to Read a Third Time and Pass - Adopted Roll Call 4` in the
House on 2026-05-08, but **no such roll call exists in the vote files**. What the dataset holds for
that chamber and day is the previous-question motion (Roll Call 3, 75-29). Missouri's feed had the
same shape — exact tallies, incomplete coverage. Nothing here can recover the missing vote, so
SB 1 can only ever be represented by its Senate vote.

## What is left

Judging and importing. The two measures were selected but **not judged**, because judging requires
reading the enacted Act and `alison.legislature.state.al.us` was not serving documents when this
work was done (repeated attempts timed out, including on URLs that had served fine earlier the same
day). A title is not a basis for a judgment, so nothing was written.

To finish: fetch `https://alison.legislature.state.al.us/files/pdf/SearchableInstruments/2026SS1/{HB1,SB1}-enr.pdf`,
read both Acts top to bottom, decide the label question below, then judge and import against this
directory's crosswalk.

**The label question to settle first.** These acts authorise special primary elections following
redistricting. Missouri's HB 1 of its 2025 special session — also a redistricting measure — was
imported under `general` with no stance on the ground that no research area describes redistricting.
These are one step removed: they are election-administration machinery rather than the map itself,
so `election_integrity` is at least arguable. Read the Acts before deciding, and note that both
passed on near party-line margins in a session called for redistricting, which is exactly the
context in which a wrong direction would be worst.
