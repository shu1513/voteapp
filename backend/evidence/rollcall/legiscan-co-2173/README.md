# Colorado roll-call votes — LegiScan session 2173 (2025 Regular Session)

Phase-4 evidence for Colorado. The pipeline is the shared LegiScan one
(`rollcall:legiscan:fetch` / `:resolve` / `:import` plus `rollcall:judge`); the
Colorado registry entry landed in a separate pull request based on main, and
this directory is data only.

Everything here was produced against the local `voteapp` database. **Production
holds no Colorado roll-call records.**

## Layout

| path | what it holds |
|---|---|
| `crosswalk.json` | 101 LegiScan `people_id` entries: 52 mapped to a VoteApp candidate, 49 explicitly null with the reason |
| `legiscan-people-co-2173.json` | the session's people snapshot, so the importer can run off committed evidence |
| `crosswalk-proposals-report.json` | the proposer's output and the validation counts (per-roll detail removed; the full report is 20+ MB) |
| `survey/` | the description histogram the config was written from, and the divided-and-enacted worklist |
| `batch-01/` | the first batch: plan, judging notes, judgments, the 13 roll evidence files and the import ledgers |
| `batch-02/` | the second batch: five measures widening area coverage, with the same layout |

The dataset itself and the 1,930 fetched roll evidence files live outside the
repository at `/Users/shu/legiscan-data/co-2173{,-evidence}/`, following the
practice set in Texas.

## The session

733 bills, 4,839 roll calls, 101 people, votes from 2025-01-08 to 2025-05-07.

Feed health is in the cleanest tier: no repeated `roll_call_id`s, no
summary-only rolls, no tally mismatches, and every member list agrees with its
own tally. Ten rolls are identity duplicates, which the existing fold handles,
and 29 carry the committee chamber code `J`.

The fetch stored 1,930 rows: 1,471 floor votes and 459 excluded questions, with
nothing surfaced for a human to sort out.

## How Colorado prints a vote

Colorado names the body in front of every question, so floor and committee
votes separate on the caption alone. A floor roll reads `House: Third Reading
Bill`; a committee roll spells the committee out, as in `House Appropriations:
Adopt amendment J.001`.

**Colorado votes twice to take the other chamber's changes.** The chamber
concurs, then repasses the bill the same day, and the two tallies differ — on
HB 25-1133 the House concurred 43-20 and then repassed 38-25. Both are stored;
the repassage is the chamber's final action on the text that became law, so
batch selection prefers it and acknowledges the same-day concurrence roll.
Conference reports follow the same shape.

Second reading happens in Committee of the Whole, which is where Colorado takes
its floor amendments, so those rolls are excluded.

## The pool

611 divided roll calls on bills that became law, across 257 measures. Applying
filter 4 — one roll per measure per chamber, the chamber's last floor vote —
leaves **401 rolls on 253 measures, 148 of them with both chambers**. Six
chambers drop out because their last floor vote was not divided, which means
their divided roll is superseded.

`survey/divided-enacted-worklist.tsv` carries every one of those rolls.

## Fan-out

A House roll reaches **40 candidates** (at most 41) and a Senate roll **11**.
All 65 House seats are on the November 2026 ballot; the Senate is staggered, and
few sitting senators are candidates, so House rolls carry the value.

## Judging source

**The Legislative Council Staff final fiscal note**, headed "Nonpartisan
Services for Colorado's Legislature". It carries a `Summary of Legislation`
section, states its own version (`Version: Final Fiscal Note`, `The final fiscal
note reflects the enacted bill`), and contains no sponsor statement of intent.

- note: `https://leg.colorado.gov/sites/default/files/documents/2025A/bills/fn/2025a_<bill>_f1.pdf`
- enrolled act: `https://leg.colorado.gov/sites/default/files/documents/2025A/bills/2025a_<number>_enr.pdf`

Both redirect to `content.leg.colorado.gov`, so a fetch has to follow
redirects. The enrolled act is the ground truth and the note is the index to it.

**Two traps found while judging, both recorded in `batch-01/JUDGING.md`:** the
dataset's `supplements[]` list can omit the final note, and the earlier note it
does list describes the *introduced* bill; and a note's own header is the only
way to tell which version it describes.

## Version check

The dataset gives a dated version stack (Introduced, Engrossed, Amended,
Enrolled), so the check is mechanical: compare the last print in force on the
vote date against the enrolled act. All seven batch-01 measures came back
identical apart from typography and one section renumbering.
