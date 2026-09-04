# Alabama roll-call votes — LegiScan session 2103 (2024 Regular Session)

Registered as `AL-2103`. The dataset holds 1,229 bills, 2,147 roll calls and 139 people. The dataset
and the 2,106 stored roll-call evidence files live outside this repository at
`/Users/shu/legiscan-data/al-2103/` and `/Users/shu/legiscan-data/al-2103-evidence/`; this directory
keeps the curated subset.

Like the 2023 session, this one covers the legislators elected in November 2022, who serve through
2026 and are on the November 2026 ballot. 137 of its 139 members were already in the Alabama
crosswalk.

## ⚠ The one thing to know: this session prints TWO caption systems at once

2024 is the transition year between the old Alabama feed captions and the modern ones, and it uses
both **inside the same session**, sometimes on bills that passed on the same day.

| | Budget Isolation Resolution | Passage |
|---|---|---|
| **System A** (no roll call number in the description) | `Third Reading House of Origin` | `Passed House Of Origin` |
| **System B** (description ends ` - Roll Call <n>`) | `Third Reading in House of Origin` | `Motion to Read a Third Time and Pass` |

Two traps sit in that table.

1. **The two Budget Isolation Resolution captions differ by one word.** `Third Reading in House of
   Origin` and `Third Reading House of Origin` are different strings, and a pattern anchored on the
   modern spelling silently keeps 173 resolution votes that should be excluded.
2. **`Passed House Of Origin` is a PASSAGE vote here, and a Budget Isolation Resolution in 2025.**
   The same words mean opposite things two sessions apart. Reading them the 2025 way hides 176 real
   passage votes and understates the divided pool by more than half — which is what a first, ad-hoc
   pass over this session did, reporting 4 divided-and-enacted rolls where there are 10.

The proof is SB 47. Its history records `Third Reading in House of Origin` and then `Motion to Read a
Third Time and Pass - Adopted Roll Call 108`, while the two stored rolls are captioned `Third Reading
House of Origin` (34-0) and `Passed House Of Origin` (34-0). The passage vote is there; only its
caption changed. The same shape repeats on HB 60 and HB 151.

Both Budget Isolation Resolution captions are excluded and both passage captions are kept. See
`CODE-FINDINGS.md` for how the two systems were told apart.

## Feed health

No repeated roll call ids, no summary-only rolls, no tally mismatches, no committee-sized votes, no
parse errors, and no roll call id collides with any other Alabama session in scope. Fetch stored
2,106 rows (2,147 minus 27 on excluded instrument types and 14 identity duplicates), with **nothing
left unclassified** across 111 description families.

Every divided roll's printed roll call number was checked against its own bill's history — the test
the 2026 session made necessary. **All 29 pass**; the System A rolls carry no roll call number to
check, and every System B roll's number is in its own bill's history. This session does not have the
2026 misfiling defect.

## Pool

824 kept floor votes, 29 divided, 10 of those on measures that became law, across 6 measures. Every
divided roll is dispositioned in `survey/divided-worklist.tsv`.

## Identity

The crosswalk holds 139 entries: 137 carried over unchanged from the 2025 and 2026 crosswalks and 2
explicit nulls — John Rogers (House 52, resigned 2024) and Greg Reed (Senate 5, left in 2025),
neither on the November 2026 ballot. Resolution over all 2,106 stored rolls: 109,274 matched, no
member without a crosswalk entry, none out of scope, no file errors. Fan-out is a median of 85 mapped
candidates per House roll and 26 per Senate roll.
