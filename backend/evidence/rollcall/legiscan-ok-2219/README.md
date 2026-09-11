# Oklahoma roll-call votes, LegiScan session 2219

Session 2219 is Oklahoma's 2026 Regular Session dataset, and it holds the whole 60th
Legislature: 8,604 recorded votes dated 2025-01-07 to 2026-05-14.

## Why Oklahoma is registered once, not twice

LegiScan also publishes a 2025 dataset, session 2165. It is **not** a separate year of
votes. Session 2219 re-files the entire 2025 session under fresh roll-call ids and fresh
bill ids. Measured on 2026-09-09 across both extracted datasets:

| | 2165 | 2219 |
| --- | --- | --- |
| bills | 3,253 | 6,008 |
| recorded votes | 4,517 | 8,604 |
| vote dates | 2025 only | 2025 and 2026 |
| votes dated 2025 | 4,517 | 4,523 |
| bills at status 4 (became law) | 562 | 1,076 |

The two datasets share **all 3,253 bill numbers and zero roll-call ids**.

Registering both would import every 2025 vote twice. The fan-out removes duplicates by the
roll's own web address key, `ls:<roll call id>`, and the two datasets give the same vote
different ids, so nothing would recognise the second copy. Every Oklahoma legislator would
end up with two records making the same claim about the same vote.

Session 2219 is also the only place where a 2025 bill that became law in 2026 shows as law,
which is why it is the one to keep.

The 2025 dataset stays on disk as a read-only cross-check. The two do not agree: 57 roll
identities appear only in 2165, and 64 of 2219's 2025-dated rolls have no match in 2165.
Most are committee rolls, but floor votes differ too — HB 1095's third reading reads 70-15
on 2025-03-17 in one dataset and 66-13 on 2025-03-18 in the other. **Audit an Oklahoma roll
against Oklahoma's own record, never against the other dataset.**

## What the feed looks like

Cleanest tier, in both sessions: no file errors, no parse errors, no committee-chamber
rolls, and no roll missing its member list. Every description opens `House: ` or `Senate: `,
and every committee roll names its committee, so floor votes are separated by rule rather
than by tally size.

Constitutional amendments ride joint resolutions, a bill type the pipeline already keeps, so
the gap that hides Georgia's and North Dakota's amendments does not arise here.

Floor questions in session 2219:

| rolls | question | what it is |
| --- | --- | --- |
| 2,760 | `THIRD READING` | passage, in the chamber where the measure starts |
| 371 | `FOURTH READING` | the originating chamber's final vote after the other chamber changed the measure |
| 111 | `VETO OVERRIDE …` | overriding the Governor, in three spellings |
| 41 | `EMERGENCY` | a separate vote on the emergency clause, not on the measure |

`FOURTH READING` is one of two questions, and the bill history says which: taking the other
chamber's amendments (`SA's read, adopted`, then `Fourth Reading, Measure passed`) or
adopting a conference committee report (`CCR adopted`). Both sit on the text that became
law, so taking each chamber's last kept roll lands on the enacted text with no per-chamber
version split.

## Traps found before anything was written

1. **The emergency-clause exclusion must be anchored at the start of the description.**
   Every veto-override caption contains the word "emergency" (`VETO OVERRIDE WITHOUT
   EMERGENCY`), so an unanchored rule silently deletes all 111 override rolls, 26 of them
   closely divided. There is a regression test on this.
2. **An overridden bill can still be marked vetoed.** Eight override rolls sit on bills
   LegiScan leaves at status 5. Oklahoma's history records the outcome (`Veto overridden`
   or `Veto override failed`, and three of these failed), so read the history, never the
   status or the `passed` flag.
3. **Four rolls wear a committee caption with an empty committee name and a whole-chamber
   tally** (`Senate:  Committee: DO PASS`). Three are genuine Senate passage votes the feed
   mis-captioned; the fourth, HB 4440, is not a passage vote at all — its 30-9 matches the
   history's `Special Election failed: Ayes: 30 Nays: 9`. No pattern can tell them apart, so
   they are left unmatched and surface for a human.
4. **Ninety Senate committee rolls would otherwise clutter the surfaced queue.** Oklahoma's
   Senate appropriations committee seats 21 to 26 of 48, which is neither clearly floor nor
   clearly committee by tally, so committee work is excluded by name instead.
5. **A tally match is not a passage check.** HB 3127's House roll reads 47-46 and the history
   line reads `Third Reading, Measure failed: Ayes: 47 Nays: 46`: the counts agree, the verb
   does not. Oklahoma needs a majority of all members (51 of 101, 25 of 48), and LegiScan's
   `passed` flag only compares yes to no. The roll was imported and had to be retracted. Read
   the verb of the matching history line, and treat any divided roll short of the
   constitutional majority as failed.

## Oklahoma ships its own tally oracle

Bill history action lines print the count: `Third Reading, Measure passed: Ayes: 83 Nays: 0`.
Every roll can therefore be checked with no network calls.

**Audited all 3,242 kept floor rolls, not only the closely divided ones: 3,209 exact, 20
mismatched, 13 with no history line for that day.** ⚠ That first audit matched on the SAME
DAY, which is one day too narrow: Oklahoma can date a journal line a day after the vote, and
HB 1576's House override (roll-dated 2025-05-29, journal line 2025-05-30) was a false
positive. Re-running over a one-day window cleared it and left the other nineteen standing,
so **nineteen** rolls are held. Use a one-day window. Bounding a tally audit by the divided
gate hides the roll the feed got wrong about being divided in the first place, which is the
lesson Oregon's SB 1565 paid for.

All 20 mismatches are held in the config by roll-call id, with the reason on each. Six are
closely divided and would otherwise have reached a batch. The usual shape is one missing no
vote (HB 4073 reads 47-0 where Oklahoma prints 47-1) — the same defect already recorded in
North Carolina, Indiana, Kansas and Oregon. HB 3383 is worse than a wrong count: Oklahoma
records the measure as failed.

## The pool, measured before any batch size was promised

3,242 kept floor rolls → 2,761 measure-chamber slots → 1,938 slots on measures that became
law → **325 closely divided final rolls on 253 measures**.

    by year      2025 172, 2026 153
    by chamber   Senate 199, House 126
    by question  third reading 243, fourth reading 57, veto override 25
    by type      bills 317, joint resolutions 8

That count is before the requirement that a measure carry a research area with a defensible
direction, before the vehicle-bill check and before the per-roll version check, each of which
has historically cut a pool by a third or more.

It is a large pool for a state where one party holds both chambers, and the Senate is why:
199 of the 325 are Senate rolls. Only 24 of Oklahoma's 48 Senate seats are on the November
2026 ballot, against all 101 House seats, so a Senate roll will reach far fewer candidates
than a House roll. Check the fan-out per chamber before choosing what a first batch holds.

## Crosswalk and reach

154 people, 53 mapped, 101 null, no unresolved member. Fifty-one mappings were proposed by
name and seat and all were accepted. Two were added by hand, both the nickname class that has
appeared in eleven states: Trey Caldwell, whose legal first name in the feed is `Hurchel`,
and T. J. Marti, filed as `Thomas`. Every unmatched member was checked by surname and seat
against every unmatched candidate, and those two were the only real misses.

Seats come from LegiScan's `district` field, never `role`: four sitting senators are filed as
`Rep` (Shane Jett SD-017, Casey Murdock SD-027, Avery Frix SD-009, Regina Goodwin SD-011).

**Reach is the binding constraint, and it favours the House.** A full-chamber House roll
reaches about 41 of our candidates, a Senate roll about 9. All 101 House seats are on the
November 2026 ballot but only 24 of 48 Senate seats are, and our roster covers 55 of those
125 seats. Prefer House-side measures when choosing a batch, and re-import after any roster
work — that adds members without any new judging.

## Layout

    survey/     the measured description histogram this config was written from,
                the divided-and-enacted worklist, and the triage decisions
    batch-01/   seven measures, eleven rolls, 303 records
    batch-02/   five measures, six rolls, 173 records (HB 3127 retracted, see its PLAN.md)
    batch-03/   four measures, five rolls, 164 records
    batch-04/   six measures, nine rolls, 262 records
    batch-05/   six measures, ten rolls, 264 records
    batch-06/   six measures, eleven rolls, 287 records
    batch-07/   six measures, six rolls, 58 records
    batch-08/   four measures, four rolls, 39 records
    batch-09/   one measure, one roll, 38 records, plus label fixes after review
