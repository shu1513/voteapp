# Alaska data findings

Recorded, not fixed. Each one is handled at selection time or by a config rule.

## 1. Alaska overrides a veto in JOINT SESSION, and the feed files that vote under one chamber

Alaska's constitution sends a vetoed bill to a joint session of both chambers, not to each
chamber in turn. The dataset holds five rolls whose desc says veto override, and none of them
can be stored honestly:

| roll | date | desc | tally | members |
|---|---|---|---|---|
| 1556378 | 2025-04-22 | `House: Veto Override` | 33-27 | 60 (44 Representatives + 16 Senators) |
| 1593391 | 2025-05-20 | `House: Veto Override SENATE` | 46-14 | 60 (44 + 16) |
| 1710463 | 2026-06-19 | `House: Veto Override` | 29-11 | 40 |
| 1692904 | 2026-05-04 | `Senate: Veto Override` | 15-5 | 20 |
| 1599277 | 2025-08-02 | `Senate: <member names> SENATE SB 183 Veto Override` | 16-3 | 20 |

The first two are the joint session itself, filed under `chamber: "H"`. They are the only
rolls in the whole feed whose `total` exceeds the chamber size, and storing one would record
a vote of the entire legislature as a House floor vote.

The other three do not match the joint-session result the bill history prints. HB 314's
`House: Veto Override` says 29-11 where the history says `GOVERNOR VETO OVERRIDDEN Y45 N15`.

So the config excludes the whole family. This costs real content — HB 57's 46-14 override is
the marquee Alaska vote of the biennium — but the passage and concurrence rolls on those same
bills are unaffected and are in the pool. Hand-researched records about the joint-session
overrides already exist in the database for several Alaska legislators, which is the right
place for that vote until the data model can hold a joint session.

Note also that LegiScan's `passed` flag is a bare majority check, so the 33-27 joint roll
reads `passed: 1` although 33 of 60 is short of the two thirds an override needs. Alaska's own
history calls it `GOVERNOR VETO SUSTAINED`. Never trust `passed` against a two-thirds rule;
this is the same defect Montana and Indiana show.

## 2. The roll that STANDS can be filed under a procedural desc — HB 110

On 2026-05-20 the Senate voted HB 110 down 9-11, rescinded that action 13-7, and then passed
the bill 13-7. LegiScan files it like this:

| roll | desc | tally |
|---|---|---|
| 1701651 | `Senate: Third Reading - Final Passage` | 9-11 |
| 1701652 | `Senate: Rescind Previous Action` | 13-7 |
| 1701653 | `Senate: Rescind Previous Action` | 13-7 |

So the only roll wearing the passage desc is the vote that FAILED, and the vote that carried
is stored under the rescind wording the config excludes. Importing the passage-desc roll would
have recorded a failed vote as a senator's position on an Act that became law. HB 110 was
dropped. This is a selection-time check, not a config rule: no pattern can tell a rescind
motion from the passage vote that follows it in the same minute.

## 3. LegiScan's `role` contradicts its `district` for six Alaska senators

Mike Cronk (SD-R), Matt Claman (SD-H), James Kaufman (SD-F), Scott Kawasaki (SD-P), Shelley
Hughes (SD-M) and Kelly Merrick (SD-L) are all filed with `role: "Rep"`. The `district` field
is right for all 62 serving members. Seat logic must read `district`, never `role` — the same
rule Texas established.

## 4. The bill page prints the date and journal page BEFORE the action

Alaska's history table is `DATE | JOURNAL PAGE | (CHAMBER) ACTION`. A scrape that pairs each
action with the date that FOLLOWS it shifts every row by one, which made HB 35's concurrence
appear to have happened on 7/30/2025 rather than 5/20/2025. Tennessee's bill page causes the
same off-by-one in the opposite direction.

## 5. Two desc families are left surfaced on purpose

`House: Special Order of Business` on its own maps to `PASSED` on some days and to a
scheduling motion on others, and `House: Third Reading Constitutional Budget Reserve
Appropriations` maps to both `PASSED` and `CBRF SECTION(S) FAILED`. Ten rolls in total,
stored and flagged for a person rather than guessed at.

## 6. LegiScan's bill status lags the state's own page by weeks

Our dataset was cut on 2026-08-30. In it, HB 10 and HB 93 both carry status 3, "enrolled",
with HB 10 showing a transmittal to the governor on 2026-08-14 and HB 93 still showing
`AWAITING TRANSMITTAL TO GOV` from 2026-05-17.

Both were wrong on the day we read them. The state's own bill pages, read on 2026-09-04, show:

- HB 10: `9/1/2026 (H) VETOED BY GOVERNOR 8/31/26`
- HB 93: `8/11/2026 (H) VETOED BY GOVERNOR 8/10/26`, transmitted to the governor 2026-07-17

HB 93's snapshot was stale by nearly three months, not by days. The bill had been transmitted
and vetoed while the dataset still showed it waiting to be sent.

**Rule this produced.** A bill's status in the dataset is a starting point, never the answer.
Any measure whose description depends on what finally happened to it — became law, vetoed,
died — must have that fact confirmed against `www.akleg.gov` before the description is
written. A record that says a bill is "waiting on the governor" goes stale on its own; a
record that says it was vetoed does not.

## 7. A selected roll can have no stored evidence, because the fetch collapsed its twin

LegiScan filed HB 133's House concurrence vote twice, as roll `1700368` and roll `1700369`.
The two are identical in every field the identity key uses: same chamber, bill, date,
description, 22-18 tally, zero not-voting, zero absent, same passed flag, and a byte-identical
member list.

The fetch collapsed the pair and stored `1700368`. Selection for this batch ran over the raw
dataset with its own de-duplication and kept `1700369`. Both choices are defensible in
isolation, and together they produce a judgment that points at a roll with no evidence file
and no database row.

**Rule this produced.** Select rolls from the stored evidence set, not from the raw dataset.
If selection must run over the dataset, reconcile the chosen roll ids against
`ls-ak-*-roll*.json` before judging. The failure is loud — the importer simply finds nothing —
but it is easy to mistake for a fetch gap when it is really a duplicate-pair disagreement.
