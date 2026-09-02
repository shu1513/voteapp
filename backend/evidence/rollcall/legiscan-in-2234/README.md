# Indiana roll-call import — LegiScan session 2234

Indiana's second session in the phase-4 LegiScan rollout, and the second entry in the
registry for one state. The source is LegiScan session **2234**, the 2026 Regular Session of
the 124th General Assembly. It is the short session: it convened 2025-11-18 and adjourned
sine die 2026-03-12, so the session is closed and the dataset is final.

This directory began as a survey and is now the campaign's working directory. The survey
findings below are kept as written, because they are what the config was built from.

The dataset is LegiScan session **2234**, dated 2026-06-28, unpacked at
`/Users/shu/legiscan-data/in-2234/`. It holds **935 bills, 689 roll calls and 152 people**.
The session ran from 2025-11-18 to 2026-03-12, so it is Indiana's short session and it is
closed and final.

## Scale, next to the 2025 session

| | 2025 (session 2143) | 2026 (session 2234) |
| --- | --- | --- |
| Bills | 1,489 | 935 |
| Roll calls | 1,010 | 689 |
| Kept floor votes | 813 | 536 |
| Divided floor votes | 160 | 120 |
| **Divided and became law** | **142, over 68 measures** | **95, over 47 measures** |
| Measures with a House roll | 49 of 61 then open | **42 of 47** |
| Committee votes in the feed | 0 | 0 |
| Parse errors | 0 | 0 |

The 2026 count is 95 rather than the 96 the first draft of this survey reported, because the
defeated concurrence on HB 1368 is now excluded. Taking one roll per measure per chamber,
preferring the final action, gives **about 70 rolls to judge**. At the fan-out this campaign measures in Indiana — a median of 83 candidates per
House roll and 12 per Senate roll — the pool is worth roughly 3,800 records if every measure
survived selection. It will not; but even at the keep rate of the harder 2025 batches this is
several times what the 2025 tail still has to offer.

## The subject mix is better than what is left of 2025

This matters more than the raw count. Batch-04 kept nothing from 2025 because what remains
there is omnibus work — the budget, and a dozen bills titled "various education matters" —
which the fifth selection filter is built to reject. The 2026 pool still has those, but it
also has a run of single-subject measures whose titles name one thing:

- HB 1368, "Carbon" — three House rolls and a Senate roll
- SB 258, "Nuclear facility permits"
- SB 91, "Syringe exchange program"
- HB 1042, "Regulation and investment of cryptocurrency"
- HB 1193, "Civil rights commission"
- SB 285, "Housing matters"
- HB 1150, "Local regulation"
- SB 277, "Indiana department of environmental management"

These are the shape that survives filter 5. No promise is made here that they will — each
still has to be read in full — but the odds are plainly better than the 2025 remainder.

## The crosswalk carries over almost whole

Of the 152 entries in the 2026 people snapshot, **149 are already in the reviewed 2143
crosswalk**, one is not a person, and two are new members. Two members present in 2025 do not
appear in 2026. What was decided about the three is under "The crosswalk, as committed"
below.

## What the survey found the config needed

The 2025 patterns nearly carry over. Applied to the 677 rolls that sit on a measure type the
pipeline keeps, they leave **8 rolls in 7 descriptions unmatched**. The first draft of this
survey said nine in eight, counting across all measure types; the ninth was a `Senate - First
reading` roll on SCR 1, a concurrent resolution the shared kept-types list drops before the
config is ever consulted. The 677 figure is the one that matters, because it is the set the
config actually sees.

Of the 8: two are the blank-question defect and must stay unmatched, one is a defeated
concurrence that has to be excluded rather than kept, and five are procedural. What was done
about each is in the next section.

## The registry entry, and what it decided

`LEGISCAN_STATE_CONFIGS` is keyed by state, and Indiana's first entry pins
`sessionId: 2143`. Flipping that number would leave the 2025 batches unable to re-run, so
2026 gets its own entry, `IN-2234`, following the `ST-session` key form the later Missouri,
Maryland and Kentucky sessions already use. Each entry carries a `jurisdiction` separate
from its key and `LEGISCAN_RECORD_JURISDICTIONS` de-duplicates through a `Set`, so records
still land under Indiana and Indiana is still named once. Nothing collides with 2025:
evidence filenames carry the session, and a `legislative_votes` row is keyed by
jurisdiction, chamber, session and roll.

Unlike Kentucky, Indiana's **kept** vocabulary does carry across sessions unchanged. What
changed is the procedural vocabulary, and one change is a trap.

**`House - Concurrence defeated` is excluded, not kept.** 2025 spelt a failed concurrence
`Concurrence failed for lack of constitutional majority`, and that spelling is kept, because
it is a recorded vote on the measure. 2026's one occurrence looks like the same question
under a shorter name and is not safe to treat that way. On HB 1368 roll 399 the House
defeated the motion 48 to 42. An Indiana concurrence needs a constitutional majority of 51,
but LegiScan reports `passed: 1`, because its flag is a bare-majority check, and the fetcher
writes `result` straight from that flag. Keeping the desc would store a defeated vote as
"Passed". It is the only roll in the session whose flag disagrees with the
constitutional-majority rule. Nothing is lost by excluding it: a defeated concurrence can
never be the final action on a bill that became law, and this one was superseded the next day
by roll 420, which concurred 57 to 40 and is kept. Same defect class as Montana's eight
two-thirds rolls.

The four other new families are procedural and excluded: the full chamber adopting a
committee report, a motion to postpone indefinitely, `Recommitted to Committee on ...` (2026's
verb for the 2025 `Referred to committee on ` exclusion), and first reading.

**The classification reconciles exactly.** 689 dataset rolls = 536 floor + 139 excluded
question + 12 on excluded measure types (11 concurrent resolutions, 1 simple resolution) + 2
surfaced. The two surfaced rolls are the blank-question defect, on HB 1002 and SB 0076, and
they stay unmatched on purpose. The `First reading` exclusion is written down but never
fires today, because its one roll is on a concurrent resolution; it is kept as a guard,
since for a bill Indiana's first reading is a referral with no vote.

## The crosswalk, as committed

`crosswalk.json` holds **151 entries, 104 mapped and 47 reviewed and left unmapped**. 149
carry over unchanged from the 2143 crosswalk. The two new members were decided by hand:

- **Randy Novak**, Democrat, HD-009 — **mapped**. He is on the November 3 2026 ballot for
  State House District 9 as a Democrat, so name, party and seat all agree.
- **Nick McKinley**, Republican, SD-017 — **left unmapped**. SD-017 is on the 2026 ballot,
  but McKinley is not among the candidates our roster holds for that seat, which are Cynthia
  Wehr and Chris Parker. There is no candidate to attach his votes to. Same call as the 2143
  entries for Edward Clere and Bruce Borders.

The snapshot's 152nd entry, `Rules`, gets no crosswalk entry at all. It is not a person:
LegiScan flags it `committee_sponsor: 1`, it holds no district or party, and it casts no vote
in any of the 689 rolls. `parseLegiscanPerson` drops committee sponsors by design.

Resolution over all 677 stored evidence files reports **35,414 matched member votes and
13,422 reviewed-unmapped**, with no member missing from the crosswalk and no file errors.

## The member-list check still applies

Five of the divided-and-enacted rolls have a LegiScan tally with no exact match in the
bill history, which is the signal for the defect in
`../legiscan-in-2143/CODE-FINDINGS.md` section 2. That is 5.2%, against about 8% in 2025.
Every roll selected for a batch must still be checked name by name against the official
roll-call PDF, and batch-04 established that a flagged roll is unusable whichever direction
the error runs.

One more instance turned up while the config was written and is worth recording: HB 1032's
`House - Committee report` is reported by LegiScan as 63-23 where the journal says 63-24.
That roll is excluded as procedural, so it costs nothing, but it is the same defect.

## Layout

- `crosswalk.json` — the reviewed person to candidate map, 151 entries.
- `legiscan-people-in-2234.json` — the people snapshot the crosswalk is checked against.
- `survey/` — the measured desc histogram the config was written from, and
  `divided-enacted-worklist.tsv`, one row per divided-and-enacted roll with its disposition.
- `tools/worklist.py` — rebuilds that worklist from the dataset.
- The reading and verification tools are shared with 2025 and live in
  `../legiscan-in-2143/tools/`. Their Indiana URL recipe needs its year segment changed from
  `124/2025/` to `124/2026/`.

## Layout addition

- `batch-01/` and later — the judgments, the roll evidence files, the import ledgers and the
  selection notes for each batch.

## State of the work

Batch-01 is imported on the local `voteapp` database: **548 records across 104 candidates
with 378 area tags, over six measures and eight rolls.** Indiana now holds **1,368 live
roll-call records across 104 candidates with 1,044 area tags** across both sessions.
**Production still holds no Indiana records.**

`survey/divided-enacted-worklist.tsv` started at **95 rolls over 47 measures**. After
batch-01 it stands at 8 in batch-01, 9 dropped, 1 superseded, 5 still flagged for the
member-list check, and 72 unbatched.

## Files

- `survey/rollcall-legiscan-fetch-in-20260902T065350Z-survey.json` — the measured run: 935
  bills, 689 votes, 152 people, 131 distinct descriptions, 0 committee votes, 0 parse errors.
