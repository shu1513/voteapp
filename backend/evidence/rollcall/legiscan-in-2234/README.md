# Indiana 2026 session survey — LegiScan session 2234

This is a survey only. **Nothing was fetched, judged or imported, and the database is
untouched.** The purpose is to measure Indiana's 2026 Regular Session and say what a campaign
on it would need.

The dataset is LegiScan session **2234**, dated 2026-06-28, unpacked at
`/Users/shu/legiscan-data/in-2234/`. It holds **935 bills, 689 roll calls and 152 people**.
The session ran from 2025-11-18 to 2026-03-12, so it is Indiana's short session and it is
closed and final.

## Scale, next to the 2025 session

| | 2025 (session 2143) | 2026 (session 2234) |
| --- | --- | --- |
| Bills | 1,489 | 935 |
| Roll calls | 1,010 | 689 |
| Kept floor votes | 813 | 537 |
| Divided floor votes | 160 | 120 |
| **Divided and became law** | **142, over 68 measures** | **96, over 47 measures** |
| Measures with a House roll | 49 of 61 then open | **42 of 47** |
| Committee votes in the feed | 0 | 0 |
| Parse errors | 0 | 0 |

Taking one roll per measure per chamber, preferring the final action, gives **70 rolls to
judge**. At the fan-out this campaign measures in Indiana — a median of 83 candidates per
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

Of the 152 people in the 2026 dataset, **149 are already in the reviewed 2143 crosswalk**.
Only three are not:

- **Randy Novak**, Representative, HD-009 — a real member who needs a crosswalk decision.
- **Nick McKinley**, Senator, SD-017 — likewise.
- **"Rules"** — not a person. It carries no district and is a placeholder the feed emits; it
  should be reviewed and left unmapped, the way the 2143 crosswalk records its 48 unmapped
  entries.

Two members present in 2025 do not appear in 2026. Of the 151 committed crosswalk entries,
103 map to a candidate on the November 2026 ballot, and that is what sets the fan-out.

## What the config needs

The 2143 question patterns already classify **680 of the 689 rolls**. Nine rolls in eight
descriptions are not matched, and they fall into four groups.

**One is a real vote that must still stay out of the kept patterns.** 2025 printed a
failed concurrence as `Senate - Concurrence failed for lack of constitutional majority`.
2026 prints `House - Concurrence defeated`, once, on HB 1368 (roll 399, 2026-02-26). The
natural move is to widen the kept pattern to cover both spellings. Do not. LegiScan marks
this roll `passed: 1` even though the House defeated it 48 to 42: a concurrence in Indiana
needs a constitutional majority of 51, and LegiScan set its flag by simple majority. The
fetcher takes `result` straight from that flag, so matching the roll would store it as
"Passed". It is the only roll in the session whose flag disagrees with the constitutional
majority rule; the 2025 counterpart carries `passed: 0` and was stored correctly. Add the
spelling to the exclusions with a comment saying why. Nothing is lost: a defeated
concurrence can never be the final action on a bill that became law, and this one was
superseded the next day by roll 420, which concurred 57 to 40.

**Four are procedural and belong in the exclusions.** `First reading`,
`Motion to postpone indefinitely, failed`, `Committee report`, and
`Rules Suspended. Committee report, adopted`. All are full-chamber votes — every total is
100 or 50 — but none is a vote on the measure.

**One is the existing referral exclusion under a new spelling.** 2025 printed
`referred to committee on ...`; 2026 prints
`House - Recommitted to Committee on Veterans Affairs and Public Safety pursuant to House Rule 126.4`.
The exclusion should cover both verbs and the trailing rule citation.

**Two are the blank-question defect and must stay unmatched.** Two House rolls carry the
description `House -` with nothing after it, on HB 1002 and SB 0076. This is the same defect
as `../legiscan-in-2143/CODE-FINDINGS.md` section 1. Leaving them unmatched is correct: they
surface as `unknown_question` rather than being guessed at.

## The registry needs a second entry for the same state

`LEGISCAN_STATE_CONFIGS` is keyed by state, and Indiana's single entry pins
`sessionId: 2143`. Flipping that number to 2234 would leave the 2025 batches unable to
re-run.

The registry already anticipates this. Each entry carries its own `jurisdiction` field
separate from its key, `LEGISCAN_RECORD_JURISDICTIONS` de-duplicates those through a `Set`,
and `--state` is trimmed and upper-cased with no format check. Later sessions already use
the key form `ST-session` (`MO-2226`, `MD-2240`, `KY-2247`). So a second entry keyed
`IN-2234` with `jurisdiction: "IN"` and `sessionId: 2234` works without touching any shared
logic. Records still land under Indiana, and nothing collides with 2025: the stored evidence
filenames carry the session, and a `legislative_votes` row is keyed by jurisdiction, chamber,
session and roll.

That entry, its tests, and the crosswalk decisions for the two new members are the next step.
This survey does not make them.

## The member-list check still applies

Five of the 96 divided-and-enacted rolls have a LegiScan tally with no exact match in the
bill history, which is the signal for the defect in
`../legiscan-in-2143/CODE-FINDINGS.md` section 2. That is 5.2%, against about 8% in 2025.
Every roll selected for a batch must still be checked name by name against the official
roll-call PDF, and batch-04 established that a flagged roll is unusable whichever direction
the error runs.

## Files

- `survey/rollcall-legiscan-fetch-in-20260902T065350Z-survey.json` — the measured run: 935
  bills, 689 votes, 152 people, 131 distinct descriptions, 0 committee votes, 0 parse errors.
