# Kentucky 2026 batch-01 — what was selected and why

Ten measures, nineteen roll calls, imported into the local database only.

## The pool this came from

The 2026 Regular Session fetch stored **840 rows: 791 floor votes (473 House,
367 Senate) plus 49 excluded-question votes kept for audit**. Nothing surfaced
for a human to sort out, there were no committee votes, no duplicate identities
and no file errors.

Under the campaign's Kentucky divided gate — nay votes at least 15 percent of
the votes cast, the reasoning is in the 2179 README — the session yields **155
divided-and-enacted roll calls on 52 measures**. Taking one roll per measure per
chamber leaves 87 candidate rolls. Every roll in the pool is listed with its
disposition in `../survey/divided-enacted-worklist.tsv`.

## The five filters

1. **Divided.** The 15 percent gate, applied to every kept floor vote.
2. **Consequential.** The bill became law. All ten measures here did.
3. **A nameable subject.** Each measure maps to one research area a reader
   would recognize as a subject, not a procedure.
4. **One roll per measure per chamber.** The chamber's last divided roll. For
   nine of the ten measures that is the veto-override vote, which Kentucky takes
   on the enrolled Act with no amendment possible, so the version check is free.
   Twenty-one earlier divided rolls on these same measures are marked
   `batch-01:not-selected` in the worklist.
5. **A defensible for-or-against stance.** Nine measures carry a stance. One,
   Senate Bill 100, carries none: it is tagged `general` because its strands run
   in different directions.

None of the nineteen rolls needed `acknowledge_later_rolls`. Filter 4 already
lands on each chamber's last kept floor vote.

## The ten measures

| Measure | Area | A yes vote is | Chambers |
| --- | --- | --- | --- |
| SB 199 pesticide labeling | corporate_accountability | against | House, Senate |
| HB 78 firearms liability | gun_control | against | House, Senate |
| HB 312 concealed carry at 18 | gun_control | against | House, Senate |
| SB 183 proxy advisers | corporate_accountability | against | House, Senate |
| HB 58 license plate readers | data_privacy | for | House |
| SB 59 public money and ballot questions | election_integrity | for | House, Senate |
| SB 173 Medicaid plan review | government_efficiency | for | House, Senate |
| SB 251 execution protocols | anti_corruption | against | House, Senate |
| SB 77 ibogaine trials | environment_and_public_health | for | House, Senate |
| SB 100 Energy Planning Commission | general | no stance | House, Senate |

Nine of the ten became law over the Governor's veto. House Bill 58 is the
exception: the House concurred in the Senate's changes on 31 March 2026 and the
Governor signed it on 10 April 2026.

## What was left out

The 2026 session's largest measures were not taken. The budget and revenue bills
(HB 500, HB 501, HB 503, HB 504, HB 757, HB 869) are appropriations, which carry
no single subject. HB 139 on elections and SB 4 on education are omnibus Acts
that run several directions at once — the same reason HB 684 was dropped from
the 2025 batch. HB 2 on Medicaid is an appropriation whose vetoes were line-item
vetoes overridden in part.

Four measures were read and set aside for a later batch rather than dropped:
SB 195 (road-contractor liability shields bundled with address confidentiality
for prosecutors and public defenders — two unrelated strands), SB 104 (a
25-foot buffer around first responders bundled with rescue-squad benefits),
SB 29 (out-of-county waste facilities, direction unclear) and HB 652 (a
program moved from one agency to another, thin substance).

## The result

- **997 candidate records across 106 candidates**, 833 area tags, 0 errors,
  0 notified.
- Import stamp `2026-09-02T06:17:37.254Z`.
- The dry run's stamp `2026-09-02T06:16:55.222Z` matches zero rows.
- A convergence dry run afterwards reports all 997 `unchanged`.

The 106 candidates are one fewer than the 2025 session's 107. David Yates held
Senate District 37 through 2025 and left the Senate in October 2025; his
successor, Gary Clemons, is not on the November 2026 ballot because Kentucky
staggers its Senate and District 37 is odd-numbered.

## What is left in this session

**115 divided-and-enacted rolls on 42 measures** remain marked
`candidate:batch-02` in the worklist. They are already dispositioned, so no
re-triage is needed.
