# Oregon 2026 regular session — LegiScan 2252

Oregon meets every year. 2025 was the long session and lives in
`legiscan-or-2191`; 2026 is the 35-day short session, which ran from
2026-02-02 to sine die on 2026-03-06. It is a separate LegiScan dataset and a
separate config entry, `OR-2252`, because nothing about the 2025 session
carries over automatically.

## What the dataset holds

304 measures, 724 roll calls, 93 people. 142 measures were enacted and one was
vetoed.

## The question vocabulary is not the 2025 vocabulary

This is the reason the session needs its own config rather than a session-id
change.

Two families that carried 2025 measures are simply absent. There is no
`Repassed` roll and no conference committee report, because no 2026 measure
went to conference.

Two families appear that 2025 never printed, and both belong to resolutions
rather than bills. `Senate Final Reading` (10 rolls) and `House Read and
Adopted` (9 rolls) are how Oregon adopts concurrent resolutions and joint
memorials. Every one of those 19 rolls sits on a CR or JM measure, so the
measure-type filter drops them before any question pattern is consulted.

Two smaller differences: `Withdraw from Committee` now appears in both
chambers where 2025 printed it in the House only, and `House Special Order`
is new.

## The pool

**85 roll calls across 51 measures are both divided and enacted.** Divided
means the smaller side is at least a quarter of the larger one.

## One roll call is held back, and the reason is new

Oregon lets a senator change a recorded vote by unanimous consent, and the
journal prints the tally **after** the change. The 2026 session has five such
changes. LegiScan applies four of them. On the fifth it kept the pre-change
tally:

> **HB 4116, Senate, 2026-03-05.** LegiScan reports 17-12 and lists McLane as
> a yea. Oregon's journal reports 16-13 and names McLane among the thirteen
> nays.

A wrong side writes a false record about a named legislator, so roll 1655091
sits in the config's `heldRollCallIds`. The importer stores it with no floor
flag, which means it is visible but can never be queued or approved. HB 4116
can still be used from its House roll.

The point worth keeping is that the mechanism does not predict which rolls are
wrong — four of the five vote changes came through correctly. Every roll in
the pool was audited against the tally Oregon's own bill history prints, and
84 of 85 match exactly. `tally-audit.py` in this directory is that audit.

## The crosswalk

LegiScan people ids are stable across sessions, so `crosswalk.json` starts
from the 2025 file, every entry of which was checked by hand, and only the
differences were reviewed again.

The 2026 proposer agreed with the 2025 file everywhere it could reach. It
contradicted no mapped entry, and it proposed no candidate for an entry 2025
had deliberately left null.

Three entries needed fresh work:

- **Matt Bunch**, Republican, HD-051 — new this session, appointed to fill a
  vacancy. Confirmed on the November 2026 ballot for State Representative,
  House District 51.
- **Lamar Wise**, Democratic, HD-048 — likewise new by appointment, and
  confirmed for House District 48.
- **Christine Drazan** moved from HD-051 to SD-026 on appointment to the
  Senate. She stays mapped to her Governor candidacy, which is what the
  November 2026 ballot holds for her.

**90 entries, 63 mapped and 27 null.** Validation across all 683 evidence
files matched 11,693 member votes with 4,945 reviewed as unmatched, no file
errors, and no crosswalk person missing from the people snapshot.

Seats come from LegiScan's `district` field and never from its `role` field,
which contradicts it in Oregon by printing senators as "house".
