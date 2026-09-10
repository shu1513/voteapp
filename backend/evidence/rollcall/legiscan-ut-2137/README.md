# Utah 2025 General Session — LegiScan session 2137

Roll-call vote import, phase 4. See `docs/plans/roll-call-vote-import.md`.

## Source

LegiScan bulk dataset for Utah's 2025 General Session, downloaded 2026-09-09
(`dataset_date` 2025-12-07, `dataset_hash` 920b7a4e513e1423a48d096268243109).
The extracted dataset lives outside the repository at `~/legiscan-data/ut-2137/`,
because a whole session is far larger than the curated evidence kept here.

Contents: 959 bills, 4,123 roll calls, 104 people. The 104 people are exactly
Utah's 75 representatives and 29 senators, one per seat.

Utah meets in an annual General Session, so 2026 is a separate dataset with its
own registry key and its own directory, `legiscan-ut-2214/`. One crosswalk
covers both and lives here.

## Survey

`survey/` holds the report written by:

    npm run rollcall:legiscan:fetch -- --state UT --dataset-dir ~/legiscan-data/ut-2137 \
      --survey --evidence-dir backend/evidence/rollcall/legiscan-ut-2137/survey

Utah's configuration entry in
`backend/src/pipeline/rollcall/legiscanStateConfigs.ts` was written from that
report's description histogram and from nothing else.

The feed is in the cleanest tier: no file errors, no vote parse errors, no
joint-committee rolls, and no summary-only rolls. One pair of roll calls is an
identity duplicate, described in `CODE-FINDINGS.md`.

## What the survey settled

**Third reading is passage in both chambers, and the Senate votes twice.** The
Senate records a second-reading vote, a third-reading vote and a combined
suspension vote; the House records only third reading, and no House caption in
either session contains "2nd". Which of the Senate's two votes is passage was
read off Utah's own action trail rather than copied from another state: each
Utah action names its destination, and 350 of the 359 bills that print
"passed 2nd reading" follow it with a third reading, while "passed 3rd reading"
sends the bill to the other chamber.

**Committee rolls are rejected by tally alone.** Every committee caption
contains " Comm - ", and no committee roll's total exceeds 16 of 75 in the
House or 9 of 29 in the Senate, both under the shared committee cut. So no
pattern in the config names a committee.

## Pool

After the divided gate, the enacted gate and one roll per chamber per measure:
**120 rolls on 95 measures**. Utah divides on 15.0 percent of its floor rolls,
close to Ohio's 14 percent. The Republican supermajority does not shrink the
pool, because the majority divides among itself: of the 155 divided-and-enacted
rolls, the no side is purely Democratic on only 3 of 65 House rolls and 21 of
90 Senate rolls.

All 1,557 stored floor rolls carry a disposition in `survey/dispositions.tsv`.

## Reach

All 75 House seats are on the November 2026 ballot and 15 of the 29 Senate
seats, so the default `--scope-from` of 2026-11-01 is correct. A House roll
reaches a median of 56 of our candidates and a Senate roll 8, which is why
batches lead with House rolls.

## Audits, run before any judging and not bounded by the divided gate

A tally error can itself decide whether a roll passes the divided gate, so all
three audits cover every floor roll on a kept bill type, not only the divided
ones.

- **Tally**: 1,914 of 1,916 match Utah's own record exactly. The two
  exceptions are the identity-duplicate pair in `CODE-FINDINGS.md`.
- **Version**: all 120 candidate rolls were cast on the text that was enrolled.
  Across the whole session 321 of 1,916 floor votes were cast on some other
  substitute, so this was checked per roll, not assumed.
- **Members**: all 120 candidate rolls were compared name by name against
  Utah's own per-roll vote sheet. Zero members on the wrong side, zero names
  missing on either side, every sheet tally equal to LegiScan's.

## Layout

- `crosswalk.json` — people_id to candidate id, covering BOTH sessions
- `legiscan-people-ut-2137.json` — the people snapshot
- `survey/` — the survey report and the disposition worklist
- `CODE-FINDINGS.md` — defects found and not fixed
- `batch-01/` — the first batch: plan, judging notes, judgments, ledgers,
  and the roll evidence files
