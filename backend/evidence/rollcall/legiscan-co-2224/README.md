# Colorado roll-call votes — LegiScan session 2224 (2025 First Special Session)

Colorado's legislature met again in August 2025, in a special session called
after a federal budget law cut state revenue and ended extra federal help with
health insurance premiums. This directory holds the evidence for importing that
session's roll-call votes.

The pipeline is the shared LegiScan one (`rollcall:legiscan:fetch` / `:resolve`
/ `:import` plus `rollcall:judge`). The registry entry for the session, keyed
`CO-2224`, landed in a separate pull request based on main; this directory is
data only.

Everything here was produced against the local `voteapp` database. **Production
holds no Colorado roll-call records.**

## The session in one table

| | |
|---|---|
| bills | 35 |
| roll calls in the dataset | 173 |
| committee votes the pipeline sets aside | 105 |
| votes on resolutions and other non-bills | 8 |
| floor votes stored | 60 (30 kept, 30 excluded motions) |
| members | 99 |
| dates | 2025-08-21 to 2025-08-26 |

Feed health is clean: no repeated roll ids, no summary-only rolls, no tally
mismatches, and **no identity duplicates at all**, so the trap that cost the
regular session one roll does not arise here.

## The pool, and how it closes

Of the 30 kept floor votes, **23 are divided votes on bills that became law**.
Applying filter 4 — one roll per measure per chamber, the chamber's last floor
vote — leaves **19 rolls on 11 measures**, and marks 3 chambers superseded
because their last floor vote was lopsided.

| | rolls |
|---|---|
| imported | 13 |
| dropped, each with a written reason | 6 |
| superseded | 3 |
| **total** | **22 chamber rows** |

`survey/divided-enacted-worklist.tsv` carries all 22 rows with the disposition
of each. **Nothing is left unworked: this session is finished.**

## Layout

| path | what it holds |
|---|---|
| `crosswalk.json` | 99 LegiScan `people_id` entries: 52 mapped to a VoteApp candidate, 47 null with the reason |
| `legiscan-people-co-2224.json` | the session's people snapshot, so the importer can run off committed evidence |
| `survey/` | the measured description histogram the config was checked against, and the worklist |
| `batch-01/` | the whole session: plan, judging notes, judgments, 13 roll evidence files and the three import reports |

The dataset and the 60 fetched roll evidence files live outside the repository
at `/Users/shu/legiscan-data/co-2224{,-evidence}/`, following the practice set
in Texas.

## The crosswalk is inherited, and checked

The special session was held by the same legislature as the regular session, so
the crosswalk is the reviewed 2173 one carried over. 98 of the 99 members
appear in both sessions, and a fresh proposer run over the special-session
people agreed with all 51 mapped proposals — no difference at all. Lori Garcia
Sander stays hand-mapped, because our roster spells her Lori Sander.

One member is new: **Lynda Zamora Wilson**, appointed to Senate District 9
after Paul Lundeen left. She is not among our November 2026 candidates, so her
entry is null. The two candidates our roster carries for that seat are Terri
Carver and William Delano Moses III.

Seats come from LegiScan's `district` field, never its `role` field.

## Fan-out

A House roll reaches **41 candidates** and a Senate roll **11**, the same
reach as the regular session.

## Judging source

**The Legislative Council Staff final fiscal note**, headed "Nonpartisan
Services for Colorado's Legislature", with the enrolled act as ground truth.

- note: `https://leg.colorado.gov/sites/default/files/documents/2025B/bills/fn/2025b_<hb|sb>25b-<number>_f1.pdf`
- enrolled act: `https://leg.colorado.gov/sites/default/files/documents/2025B/bills/2025b_<number>_enr.pdf`

Note the special-session URL shape: the note filename carries the full bill
number with its `25b-` prefix, which the regular session's filenames do not.

**One note lies about its own version.** The file at HB 25B-1001's `_f1`
address — the address that holds the final note for every other bill in the
session — is headed `Version: Initial Fiscal Note` and says it "reflects the
introduced bill". That measure was judged from its act alone. Every other note
was read only after its own header confirmed it describes the enacted bill.

## Version check

Every selected roll was checked: the last print in force on the vote date,
diffed against the enrolled act. All 13 came back identical apart from
typography and one enrollment habit — Colorado spells numbers out in
pre-enrollment prints and converts them to numerals at enrollment, so HB
25B-1003's "fifteen of the eighteen" becomes "15 of the 18".
