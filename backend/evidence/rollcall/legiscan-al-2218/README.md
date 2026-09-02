# Alabama roll-call votes — LegiScan session 2218 (2026 Regular Session)

Alabama sits in annual regular sessions, so 2026 is a separate LegiScan session from 2025 and gets
its own registry key, `AL-2218`, sharing one vocabulary definition with the 2025 entry and with the
2026 First Special Session (`AL-2262`, see `../legiscan-al-2262/`). The dataset
holds 1,531 bills, 3,541 roll calls and 140 people. The dataset and the 3,503 stored roll-call
evidence files live outside this repository at `/Users/shu/legiscan-data/al-2218/` and
`/Users/shu/legiscan-data/al-2218-evidence/`; this directory keeps the curated subset.

## Feed health

The same clean tier as 2025: no repeated roll call ids, no summary-only rolls, no tally mismatches,
no committee votes, and no roll call id collides with the 2025 session. Fetch stored 3,503 rows
(3,541 minus 30 on excluded instrument types and 8 identity duplicates), with nothing left
unclassified once two 2026-only spellings were added to the shared Alabama vocabulary:

- `<sponsor> Concur-In and Adopt Executive Amendment` — the vote to accept a change the **Governor**
  sent back with a bill. Kept as a concurrence; the hyphen is why a space-only pattern missed it.
- `Lost in House of Origin` — a **failed Budget Isolation Resolution**, excluded. See
  `CODE-FINDINGS.md` §2.

**`CODE-FINDINGS.md` §1 is the one that matters for future batches**: this session files some
physical votes under more than one bill, and the check that catches it is whether a roll's printed
number appears in its own bill's history.

## Pool

1,081 kept floor votes, 27 divided, 18 of those on measures that became law. Every divided roll is
dispositioned in `survey/divided-worklist.tsv`, which carries a `roll_number_in_history` column so
the misfiled rolls are visible at a glance.

## Identity

The crosswalk holds 140 entries: 112 proposed by the resolver and all accepted, 10 carried over from
the 2025 crosswalk, and 18 explicit nulls. Resolution over all 3,503 stored rolls: 189,323 matched,
no member without a crosswalk entry, none out of scope. Fan-out is a median of 87 candidates per
House roll and 28 per Senate roll.

The 2025 and 2026 people files overlap by 135 of 140, so the earlier crosswalk carried nearly whole.
Four members left and four arrived, and each new member matched a candidate exactly on name and
seat: Heath Allbright (House 11), Cindy Myrex (House 12), Norman Crow (House 63) and Kristin Nelson
(House 38). The fifth new member, Greg Barnes (House 13), has no November 2026 candidate row and is
an explicit null.

**One 2025 anomaly resolved itself here.** Matt Woods appears in the 2025 file as the House District
13 member and in this one at Senate District 5, the seat he won in a June 2025 special election. The
2025 crosswalk records a seat disagreement for him; this one does not, and the two entries point at
the same candidate.

The ten hand-added entries carried over are the same classes documented in the 2025 README:
Alabama's people file stores the legal first name while the roster carries the working name (Brock
Colvin, Chad Robertson, Jay Hovey, Billy Beasley, Rick Rehm, Steve Hurst), plus roster-side
differences (Merika Coleman, B. Craig Lipscomb, Philip Ensler) and Will Barfoot, whose name matches
two candidate rows because Alabama reverted to its 2021 Senate map.
