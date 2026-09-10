# South Dakota roll-call import — survey and state configuration

This directory holds the South Dakota survey evidence and, with its two sister
directories, the record of how the state's configuration entry was written.

    legiscan-sd-2170/   2025 Regular Session        (config key `SD`)
    legiscan-sd-2231/   2026 Regular Session        (config key `SD-2231`)
    legiscan-sd-2222/   2025 First Special Session  (config key `SD-2222`)

Every number below was measured from the three LegiScan datasets. Nothing was
copied from another state. No AI provider was called at any point.

## What the datasets contain

| Session | LegiScan id | Bills | Roll calls | People | Parse errors |
|---|---|---|---|---|---|
| 2025 Regular | 2170 | 571 | 1,604 | 124 | 0 |
| 2026 Regular | 2231 | 666 | 1,652 | 125 | 0 |
| 2025 First Special | 2222 | 2 | 4 | 105 | 0 |

South Dakota meets every year for a short session of about 38 working days, so
each year is a separate dataset and gets its own configuration entry. The
Legislature seats 70 representatives and 35 senators. All 105 seats are elected
every two years, so every member the crosswalk maps is on the November 2026
ballot.

## The special session is registered, because it is not empty

The 2025 First Special Session ran for one day, 23 September 2025, on one
subject: whether the Department of Corrections could buy and exchange land for
a new prison. Senate Bill 2 became law and both chambers divided on it — the
House 51 to 18 and the Senate 24 to 11. That is one measure and two votes a
voter would recognize, so the session is registered rather than dismissed.

An earlier state in this campaign was declared finished while an unsurveyed
special session sat in its dataset list, and a whole batch was missed. Every
South Dakota session LegiScan publishes for the two years in scope has now been
enumerated and accounted for.

## The caption vocabulary, as measured

South Dakota prints the smallest and plainest set of vote captions in the
campaign: 39 distinct families in 2025 and 41 in 2026. Four families carry
almost every vote.

Kept as votes on the measure itself:

| Caption | Meaning | Question class |
|---|---|---|
| `Do Pass` | passage of the bill as it stands | passage |
| `Do Pass Amended` | passage of the bill as amended earlier in the sitting | passage |
| `Concurred in amendments` | the first chamber accepting the second chamber's changes | concurrence |
| `Conference Committee Report adopted` | adoption of a conference committee's compromise | conference report |
| `Veto override` | passing a bill over the Governor's veto | veto override |

Excluded as procedural: `Deferred to the 41st legislative day`, `Deferred to
another day`, `Tabled`, `Removed from table`, `Referred to`, `Referred as
Amended to`, `Reconsidered`, `Motion to amend`, `Placed on calendar pursuant to
JR 6F-6`, `Report out of committee without recommendation` (and its amended
spelling), `Do Not Pass as amended`, `Fiscal Note Requested`, `Conference
Committee report not adopted, no committee appointed`, `Failed to concur`
(both spellings) and `Recalled`.

Two further exclusion rules were drafted for South Dakota's resolution
captions, `Adopt Resolution` and `Concurred in Resolution`, and then removed:
measurement showed they never fire, because those captions appear only on
concurrent and simple resolutions, which the shared bill-type filter drops
before any caption is read.

Deferring a bill to the 41st legislative day is how South Dakota kills a
measure. The session never reaches a 41st day, so the deferral is the end of
the bill. It is a real decision but it is not a vote on the measure's merits,
and the campaign's second filter requires the measure to have become law.

After these rules, **not one roll call in any of the three sessions carries a
caption the configuration does not recognize.**

## The finding that shapes the state: one caption, two kinds of body

South Dakota is the first surveyed state where committee votes and floor votes
print the identical caption. A committee reporting a bill out and the whole
House passing it both read `Do Pass`. Nothing in the roll call distinguishes
them except how many members voted.

That is safe here, and it was measured rather than assumed. The two groups do
not overlap or come close:

| Body | Members listed | Chamber size |
|---|---|---|
| House committees | 5 to 15 | 70 |
| House floor | 68 to 70 | 70 |
| Senate committees | 5 to 9 | 35 |
| Senate floor | 35 | 35 |

No roll call in either regular session falls between those groups. The
pipeline's floor line — 60 percent of the chamber — therefore separates them
with a wide margin on both sides.

The consequence is that South Dakota's committee votes land in the surfaced
bucket rather than being dropped: 557 rolls in 2025 and 579 in 2026. Surfaced
rows are stored, never queued and never approved, so this is the expected shape
of the state rather than a defect. It is written down here so that a later
reader does not go looking for a problem.

The Joint Committee on Appropriations is handled separately and needs no new
rule. LegiScan files its 51 (2025) and 59 (2026) votes under chamber code `J`,
which the shared pipeline already recognizes as a committee body and drops
before parsing.

## The finding that governs selection: read the outcome, not the flag

**LegiScan's `passed` flag is wrong on 31 of the 1,255 floor roll calls on kept
bill types, and it is wrong in both directions.** South Dakota prints both the
tally and the outcome word in its own bill history, for example:

    House of Representatives Do Pass Amended, Passed, YEAS 68, NAYS 2. H.J. 309

Comparing the two sources:

* **29 roll calls read `passed: 1` where South Dakota says Failed.** These are
  measures that needed a two-thirds majority — 47 of 70 in the House, 24 of 35
  in the Senate — which appropriations bills, bills carrying an emergency
  clause and veto overrides all do. A 46 to 22 House vote is a defeat in South
  Dakota, and LegiScan calls it a pass because it counts a bare majority of
  those voting.
* **2 roll calls read `passed: 0` where South Dakota says Passed.** Both are
  Senate votes that divided 17 to 17. The Lieutenant Governor breaks a tie in
  the South Dakota Senate, and that tie-breaking vote is not in the member
  list, so the feed sees a deadlock where the state records a passage.

Selection must therefore read the outcome word in the bill history, never the
feed's flag. The flag is stored in `result` and no part of the pipeline reads
it, so nothing downstream is affected.

## Tally audit

The audit was run over **every** floor roll call on a kept bill type in both
regular sessions, not only the closely divided ones, because a wrong tally can
itself decide whether a vote looks closely divided.

    1,255 rolls audited against South Dakota's own bill history
    1,254 match exactly on date, yeas and nays
        1 has no matching history line at all

The single exception is held in the configuration. Roll call 1494248 is filed
under House Bill 1096 and dated 19 February 2025, but House Bill 1096's history
records no vote that day — the House passed it 68 to 0 on 11 February. The
roll's member list is byte-identical to the 69 to 1 votes filed the same day
under House Bill 1099 and House Bill 1196, both of which the history does
record. It is another bill's vote copied onto this one. It is not closely
divided, so it could never have reached a batch, but it is held so it cannot be
approved by mistake.

Roll call identifiers also run in the chambers' own order. Sorting each
chamber's floor roll calls by identifier produces zero date inversions in
either session, so the fourth filter can order a measure's votes by identifier.

## The pool, measured before any batch was promised

The campaign keeps a vote when it was closely divided (the smaller side is at
least a quarter of the larger), the measure became law, one vote per measure
per chamber, and that vote is the chamber's word on the text that became law.

| Step | 2025 (2170) | 2026 (2231) |
|---|---|---|
| Floor roll calls on kept bill types | 605 | 650 |
| Closely divided among them | 181 (29.9%) | 179 (27.5%) |
| Measure-and-chamber slots on enacted bills | 434 | 480 |
| Slots whose final passing vote is closely divided | 72 | 62 |
| Distinct measures behind those slots | 53 | 45 |

**The working pool is 134 measure-and-chamber slots over 98 measures**, plus 2
slots on 1 measure in the special session. Nineteen measures in 2025 and
seventeen in 2026 divided in both chambers.

South Dakota divides on close to 30 percent of its floor votes. That is the
opposite of West Virginia, where a supermajority made votes lopsided and only
about 5 to 10 percent were close. Republicans hold supermajorities here too,
but the caucus splits, so the close votes are real disagreements inside the
majority rather than party-line contests.

Three of the 2025 slots sit on joint resolutions. In South Dakota a joint
resolution proposes an amendment to the state constitution, which goes to the
voters rather than to the Governor, so it never becomes law by being passed.
Those slots will be checked against the second filter before any of them is
selected.

## What this pull request does and does not do

This pull request adds the three configuration entries, the three survey
reports and this README. It writes nothing to any database, imports no records
and changes no shared behavior. The batch work follows in a separate pull
request, also based on `main`.

## Reproducing the survey

    npm run rollcall:legiscan:fetch -- --state SD \
      --dataset-dir <extracted session directory> --survey \
      --evidence-dir backend/evidence/rollcall/legiscan-sd-<session>/survey

The survey reads the dataset only. It needs no configuration entry, no
database and no network.
