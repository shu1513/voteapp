# Washington roll-call import — LegiScan session 2166

Washington's 2025-2026 Regular Session. The state runs a two-year biennium in one
dataset, and its LegiScan bulk list carries no special session at all, so this single
session is the whole current term.

The config entry for this session was added in a separate pull request and is in main.

## Layout

| path | what it is |
| --- | --- |
| `crosswalk.json` | the reviewed member-to-candidate map, 151 entries |
| `legiscan-people-wa-2166.json` | the people snapshot the importer runs against |
| `survey/…-survey.json` | the description histogram the config was written from |
| `survey/divided-enacted-worklist.tsv` | one row per measure-chamber slot, with a disposition |
| `survey/divided-not-enacted-worklist.tsv` | the same for measures that did not become law |
| `survey/tally-audit.json` | every floor roll checked against Washington's own tally |

## Feed health — the cleanest tier

0 repeated roll call ids, 0 identity duplicates, 0 summary-only rolls, and no roll whose
parts fail to add up to its total. Four parse errors, **all of them committee rolls**, so
none can ever reach the queue; three report exactly double their own member list. A
non-zero fetch exit is expected for Washington and does not mean the run failed.

The fetch reconciles exactly:

    5,038 rolls = 2,015 floor
                + 1 excluded motion
                + 2,962 committee
                + 56 excluded measure type
                + 4 parse errors

with **nothing surfaced**.

## Why the vocabulary needed only one kept pattern

Every floor vote says **final passage** and no committee vote does. The survey folds
5,038 rolls into 212 description families: 196 are spelled
`<Chamber> Committee on <Name>: <question>`, and the other 16 all contain those words.
The overlap between the two sets is zero rolls across the whole dataset.

**⚠ The one trap.** Washington also prints
`House Motion to Place Measure on Final Passage as Amended by the House`, which is a
motion to bring a measure up, not a vote on the bill. Excluded questions are checked
before kept ones, which is the only reason a bare pattern is safe. One roll, on SB 5181,
and it never became law.

## ⭐ Washington ships its own tally oracle

2,038 bill-history action lines carry the state's own tally, one for every floor roll:

    Third reading, passed; yeas, 41; nays, 8; absent, 0; excused, 0.

So the audit needs no network. It was run over **every** floor roll rather than only the
ones the divided gate selects, because a wrong tally can itself decide whether a roll
passes that gate. **2,037 of 2,038 match exactly.** The single exception is the
procedural motion above. No roll needs holding, so this state has no held roll call ids.

⚠ What this does NOT cover: the audit compares tallies. A swapped pair of members with a
correct total would be invisible. Spot-check a selected roll against Washington's own
roll call page when a batch is judged.

## ⚠ Roll call ids do not ascend with date

63 inversions in the House and 43 in the Senate. Anything picking a chamber's last vote
must order by DATE and settle same-day ties from the bill history, never by id.

## ⚠ Sixteen partial vetoes

The session carries 16 partial vetoes and 1 full veto, and **no override roll exists at
all**. A partial veto means part of an enacted act never took effect, so a measure has to
be checked against the veto message before it is described. Eleven measures in the
worklist sit on a partially vetoed act and are flagged in its `partial_veto` column.

## Crosswalk

151 entries: **108 mapped, 43 explicit nulls**, `no_crosswalk` 0, 0 people missing from
the snapshot, 103,652 member matches over all 2,016 stored rolls.

**⚠ `seatAgrees` is useless in Washington, and it is the Maryland shape inverted.**
LegiScan is MORE specific than our roster: it gives House seats as `HD-009B`, carrying
the position letter, while our roster stores `Legislative (House) District 9 (2024)` with
no position. The strings cannot be compared, so 89 of 107 proposals came back null.
**Corroborate on the district NUMBER instead**, which 100 of 107 proposals agree on.

The seven that did not, all reviewed by hand:

- **Five are the routine chamber switch** — a sitting House member running for the
  same-numbered Senate district, because Washington nests both House positions inside the
  same-numbered Senate district (Cindy Ryu, Jeremie Dufault, Alex Ybarra, Sharlett Mena,
  Chipalo Street). Each was confirmed against `current_office`, and the LegiScan `A`/`B`
  suffix matches the stated Position 1/2 in every case.
- **Chris Corry** — LegiScan says `HD-014A`, our roster says House District 15. The
  LegiScan district is **stale**: District 14's two seats are held by Gloria Mendoza
  (Position 1) and Deb Manjarrez (Position 2), both of whom appear separately in the same
  people file, and our own record has Corry as the sitting District 15 Position 1 member.
  This is also why `HD-014A` carries two people. Same person, stale seat — the Montana
  finding recurring.
- **Nikki Torres** — LegiScan's `SD-015` agrees with her `current_office`; she is a
  sitting District 15 senator running in District 8 in 2026, and Jeremie Dufault is
  running for the District 15 Senate seat she is leaving.

**One hand-add, and it is the familiar class:** Mari Leavitt. LegiScan's `last_name` is
`Kruger-Leavitt` while its `name` field is byte-identical to our candidate. The proposer
matches on `first_name` plus `last_name` and reads neither `name` nor `nickname`, so it
declined an exact match. Every other unmatched person was checked against the WHOLE
candidate pool, not just their own district, and none has a match.

**The 43 nulls are almost all structural, confirmed rather than assumed.** 25 of the 31
unmatched senators sit in seats that are not on the 2026 ballot, which is exactly
49 − 24. Six more sit in seats that ARE up but are not running for them, and those six
seats are precisely the ones the House members above are moving into. The 12 unmatched
House members are sitting members not on the 2026 ballot.

## Fan-out

**House median 87 per roll (max 89), Senate 19 (max 19).** A House roll is worth about
four and a half Senate rolls, so batches should be House-heavy.

## Pool

| stage | count |
| --- | --- |
| kept floor rolls | 2,015 |
| divided (smaller side at least a quarter of the larger) | 701 |
| divided AND enacted | 600 rolls over 275 measures |
| **after filter 4 (each chamber's last kept floor roll)** | **456 slots over 269 measures** |
| by chamber | 234 House / 222 Senate |

144 divided-and-enacted rolls are not their chamber's last word. Only 4 slots have a
same-day peer needing `acknowledge_later_rolls`, which is unusually few.

Washington is a Democratic trifecta, so party-line bills become law and the
divided-and-enacted set is large. If every slot were eventually imported the yield would
be roughly 24,500 records.

## Not-enacted pool (batches 13 to 16)

A second pass took measures that did **not** become law. What passes a trifecta is often
bipartisan, so contested votes on stalled bills are where much of the disagreement sits.
The worklist is `survey/divided-not-enacted-worklist.tsv`, one row per slot, each with a
disposition and reason.

| stage | count |
| --- | --- |
| chamber's last kept floor roll divided, measure not enacted | 92 slots over 92 measures |
| by chamber | 29 House / 63 Senate |
| LegiScan status | all 92 at status 2 (passed one chamber) |
| other chamber held any floor vote | 1 (House Bill 2675, dropped) |
| **imported** | **42 measures, 42 rolls, 1,544 records** |
| dropped with a written reason | 50 |

All 92 final votes passed; no divided final-passage vote failed. The session's one full
veto (House Bill 1108) was not a divided vote.

- **Gate, per measure:** not enacted, the chamber's last floor vote divided and passed,
  and the other chamber held no floor vote at all.
- **Source:** the bill report written for the exact version the chamber passed, `HBR APH`
  (as passed House) or `SBR APS` (as passed Senate). Eight carry a suffix (`1574.E`,
  `5017-S … APS2`) and were found by listing the report folders, not by guessing names.
- **Wording:** conditional ("It would …") with a dated tail, "As of September 2026 the
  Senate had not voted on it, so it was not law." The builder refuses enactment language.
- **Most drops** are fee, fund or bond machinery, administrative changes, cannabis,
  criminal-justice leniency that reads two ways, and voter-access expansion.

| batch | measures | records | candidates | retired |
| --- | --- | --- | --- | --- |
| 13 (House) | 11 | 957 | 89 | 0 |
| 14 (Senate) | 10 | 190 | 19 | 2 |
| 15 (Senate) | 12 | 227 | 19 | 0 |
| 16 (Senate) | 9 | 170 | 19 | 0 |

Washington now holds **11,002 live roll-call records across 108 candidates** locally. Both
pools are closed. Production holds none.

## Judging notes for this state

- **Stance direction follows the research-area description, not the bill's framing.**
  Washington mirrors Texas: a gun bill is `gun_control`/for.
- **Version prefixes matter.** ESHB, 2SSB and ESSB name the version, and a striking
  amendment replaces the whole text, so check per roll which version that chamber voted.
- **Sources:** the nonpartisan House and Senate bill reports on leg.wa.gov, which are
  written for every version, plus the enrolled session-law text.
- Constitutional amendments ride joint resolutions, an already-kept bill type, so the
  Georgia resolution gap does not arise. It is moot here in any case: all 27 joint
  resolutions in the session are still at introduced status and none reached a floor vote.
