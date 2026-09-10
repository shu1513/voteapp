# Hawaii roll-call votes (sessions 2175 and 2245)

Hawaii gets its own pipeline, `rollcall:hi:fetch` / `rollcall:hi:resolve` / `rollcall:hi:import`,
because the LegiScan vote feed for Hawaii holds no floor votes at all. This directory holds the
2025 Regular Session (LegiScan session 2175). The 2026 Regular Session (2245) sits in
`../hawaii-2245/`. The crosswalk in this directory serves both sessions.

## Why Hawaii cannot use the LegiScan pipeline

Every roll call in LegiScan's Hawaii datasets is a committee vote. Measured over every roll call
on 2026-09-09: the 2025 dataset has 2,826 roll calls in 41 description families, the 2026 dataset
3,909 in 46, and the largest tally in either is a 17-member conference committee. The House has
51 seats and the Senate 25. Every description reads `<Chamber> <Committee>: Passed, With Amendments`
or `Passed, Unamended`, plus `House Conference` and `Senate Conference`. The bills' own `votes[]`
arrays point only at those same committee roll calls.

The floor votes exist, but only as text in each bill's `history[]`. The Senate prints an aye
count and names the members who voted no, voted aye with reservations, or were excused. The House
prints no aye count at all; it names only the dissenters, the reservations, and the excused:

    Senate: Passed Final Reading, as amended (CD 1). Ayes, 23; Aye(s) with reservations:
            Senator(s) Rhoads. Noes, 1 (Senator(s) DeCorte). Excused, 1 (Senator(s) McKelvey).
    House:  Passed Final Reading as amended in CD 1 with Representative(s) Souza voting aye with
            reservations; Representative(s) Garcia, Pierick voting no (2) and Representative(s)
            Cochran excused (1).

So Hawaii publishes no per-member floor roll. The fetcher reads those lines, resolves each named
member against the dataset's people file, and reconstructs the aye list as every sitting member of
the chamber who is not named as a no or as excused. Two checks run before anything is stored: the
three lists must fill the chamber's seats exactly, and where the journal prints an aye count (the
Senate) the reconstruction must equal it. The yes-side record text says once that the yes vote is
inferred from the list of no votes.

## The seat file is mandatory

Because the House prints no aye count, a vacant seat is invisible in its text. `seats.json` says
who was not sitting on a date, from official sources only. In 2025 Rep. Gene Ward (HD-018) retired
on March 31 and died on April 4, so every House floor vote from April 1 to the end of the session
was taken by 50 members. Without the seat file the fetcher would have counted Ward as a yes on all
of them. The 2026 file records three appointments and the vacancies between them.

## Selection

Kept votes are each chamber's floor passage lines: `Passed Third Reading` (with or without a
`Report adopted;` prefix) and `Passed Final Reading`. A bill that passes both chambers unamended
has no final reading; the second chamber's third reading is its last vote. The draft named on the
line (`HD 2`, `SD 3`, `CD 1`) is the text that chamber voted, so the version check is exact. The
last kept vote per chamber is the vote on the enacted text; earlier readings on earlier drafts
are never selected. Enactment is read from the bill's own history line (`Act 016, on 04/10/2025`
or `Became law without the Governor's signature, Act 028`), never from LegiScan's status flag.

The 2026 dataset also carries the 2025 votes on bills that carried over within the biennium. The
fetcher files a vote under the session it was cast in and skips the 603 earlier-session rows.

## Sessions and counts

| session | floor votes stored | closely divided and enacted | judged | dropped |
| --- | --- | --- | --- | --- |
| 2175 (2025 Regular) | 1,797 (870 House / 927 Senate) | 7 rolls / 6 measures | 5 rolls / 4 measures | 2 |
| 2245 (2026 Regular) | 1,778 (936 House / 842 Senate); 603 prior-session rows skipped | 6 rolls / 6 measures | 6 rolls / 6 measures | 0 |

Two thirds of Hawaii's enacted final readings are unanimous. A supermajority passes party-line
bills far outside the quarter gate, the West Virginia shape.

## Roster and fan-out

`crosswalk.json` maps 79 LegiScan people to 32 candidates (29 House, 3 Senate) and marks 47 as
having no November 2026 state-legislative candidacy in the local roster. Three entries were added
by hand: Mike Lee (legal name Michael), Sue Keohokapu-Lee Loy (the ballot spells the surname
without a hyphen), and Rachele Fernandez Lamosao (the ballot uses her middle initial; she moved
from HD-036 to SD-019 for 2026). Fan-out is about 28 records per House roll and 2 to 3 per Senate
roll. The Senate reach is structural: few Senate seats on the 2026 ballot carry a sitting member
in the roster.

## Layout

- `seats.json` — who was not sitting on which dates (official sources cited inside).
- `crosswalk.json` — people_id to candidate id, both sessions.
- `hawaii-people-2175.json` — the people snapshot the resolver wrote from the dataset.
- `resolve-report.json` — the proposal run over the dataset (no crosswalk yet).
- `survey/` — the pipeline survey of the LegiScan vote feed, the whole-session fetch ledger (every
  stored roll with its reconstructed tally), and `divided-enacted-worklist.tsv` (every roll in the
  pool with its disposition). The whole-session resolve report is not committed (5 MB, regenerable
  with the resolve command below).
- `batch-01/` — evidence JSONs, `judgments.json`, `PLAN.md`, `JUDGING.md`, and the import ledgers.
- `tools/` — the scripts that produced the batch: `hi_text.py` (bill HTML to marked-up text),
  `hi_build.py` + `hi_measures.py` (the judgments builder that refuses bad prose).
- `CODE-FINDINGS.md` — data defects and tooling notes recorded, not fixed.

## Commands

    npm run rollcall:hi:fetch -- --session 2175 --dataset-dir <extracted dataset> \
      --seat-file evidence/rollcall/hawaii-2175/seats.json --evidence-dir <run dir>
    npm run rollcall:hi:resolve -- --session 2175 --people-file evidence/rollcall/hawaii-2175/hawaii-people-2175.json \
      --crosswalk-file evidence/rollcall/hawaii-2175/crosswalk.json --evidence-dir <run dir>
    npm run rollcall:judge -- --judgments-file evidence/rollcall/hawaii-2175/batch-01/judgments.json
    npm run rollcall:hi:import -- --session 2175 --evidence-dir evidence/rollcall/hawaii-2175/batch-01 \
      --crosswalk-file evidence/rollcall/hawaii-2175/crosswalk.json \
      --people-file evidence/rollcall/hawaii-2175/hawaii-people-2175.json \
      --seat-file evidence/rollcall/hawaii-2175/seats.json --dry-run

The importer re-derives the member lists from the pinned history line, the people snapshot and the
seat file, and refuses the file if they differ from what the fetcher wrote.

## Production

Production holds zero Hawaii roll-call records. Promotion is a separate step.
