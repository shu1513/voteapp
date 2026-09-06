# Nevada session 2144 — complete run summary

**Nevada is finished.** Every divided floor roll call in LegiScan session 2144 carries a
disposition, in both pools. Local `voteapp` only; production holds no Nevada roll-call
records.

## Totals

| | |
|---|---|
| divided floor roll calls in the session | **291** |
| approved and imported | **131** |
| live candidate records | **2,571** |
| candidates reached | **41** |
| batches | 9 |

The 291 is the 104 divided-and-enacted rolls plus the 187 divided non-enacted rolls. One
further divided roll, AB 44's superseded first Senate vote, appears in the non-enacted
worklist and is counted there.

## Pool one — divided and enacted (104 rolls)

Worked in batches 01 to 03, before this session. `survey/divided-enacted-worklist.tsv`.

| disposition | rolls |
| --- | --- |
| imported, batch-01 | 18 |
| imported, batch-02 | 14 |
| imported, batch-03 | 7 |
| dropped with a reason | 50 |
| excluded as appropriations or procedural | 12 |
| superseded by a later roll | 3 |
| **total** | **104** |

## Pool two — divided and NOT enacted (187 rolls)

Opened in this session on the user's direction, because Nevada has almost none of the
campaign's usual exception class. `survey/divided-not-enacted-worklist.tsv`.

| disposition | rolls |
| --- | --- |
| imported, batch-04 | 18 |
| imported, batch-05 | 16 |
| imported, batch-06 | 17 |
| imported, batch-07 | 17 |
| imported, batch-08 | 13 |
| imported, batch-09 | 11 |
| dropped with a reason | 94 |
| superseded by a later roll | 1 |
| **total** | **187** |

Of the 94 drops, **56 fail filter 3** (no subject a voter would judge a legislator on, or no
research area fits) and **38 fail filter 5** (the measure reads two ways).

### Why the pool was opened

Nevada's divided non-enacted pool splits three ways: 79 measures the Legislature passed
through **both** chambers and the governor vetoed, 29 measures one chamber passed that the
other never voted on, and 6 that died after both chambers had voted. Only the middle group
fits the campaign's standing exception to filter 2, the Pennsylvania batch-02 scope. Taking
only that group would have finished Nevada at six or seven measures. The user was asked and
directed that the vetoed pool be opened. Every description on a measure that did not become
law says so and uses "would have".

## Import batches

| batch | pool | measures | rolls | records | run stamp |
| --- | --- | --- | --- | --- | --- |
| batch-01 | enacted | 10 | 18 | 387 | 2026-09-05T03:29:56.126Z |
| batch-02 | enacted | 11 | 14 | 265 | 2026-09-05T03:42:57.034Z |
| batch-03 | enacted | 7 | 7 | 94 | 2026-09-05T03:52:18.296Z |
| batch-04 | vetoed | 9 | 18 | 364 | 2026-09-06T07:19:55.157Z |
| batch-05 | mixed | 9 | 16 | 308 | 2026-09-06T07:34:43.546Z |
| batch-06 | mixed | 10 | 17 | 332 | 2026-09-06T07:45:55.360Z |
| batch-07 | mixed | 9 | 17 | 336 | 2026-09-06T22:50:12.012Z |
| batch-08 | mixed | 8 | 13 | 255 | 2026-09-06T22:59:46.564Z |
| batch-09 | mixed | 6 | 11 | 230 | 2026-09-06T23:08:05.099Z |
| **total** | | **79** | **131** | **2,571** | |

Every batch reconciles three ways: the import report's insert count, the count of rows
carrying that run stamp, and the change in the Nevada table total. Fan-out held at 27 to 30
candidates per Assembly roll and 10 or 11 per Senate roll throughout, with no roll ever
reaching zero. The duplicate sweep, run on `origin_run_id NOT LIKE 'rollcall:%'`, finds 0
duplicate record identity keys and 0 duplicate source URLs within any candidate.

## Data corrections made during the run

1. **645 of Nevada's 746 pre-existing records** ended with a sentence beginning in lowercase
   — "The Nevada Senate passed it 15-6. and the bill was signed into law." Fixed at source in
   the batch-01, 02 and 03 judgment files and rewritten in place. The pattern now returns
   zero rows in every state.
2. **One hand-written record retired**: it duplicated a member's own AB 480 vote at the same
   tally on the same day and said less than the roll-call row does.
3. **Two description errors found in review of PR #1193** and fixed: AB 223 said the bill
   "added pests, mold, lead paint to the list of things a rental must have", the opposite of
   what it does, and AB 185's outdoor play space exception was stated more broadly than the
   bill allows.

## Method problems found and fixed, in order

- **Roll ids are not chronological.** AB 44 has two Senate rolls on one day; the decisive one
  is the re-vote after reconsideration and it carries the **lower** id. `CODE-FINDINGS.md` §2.
- **Nevada prints a reprint AFTER a floor amendment** when a chamber dispenses with
  reprinting, so the last reprint before a vote is often not the text voted.
- **Version diffs must not filter by fragment length.** AB 411's whole difference was
  "must" against "may", and a ten-character filter hid it.
- **Version diffs must not drop bare numbers.** SB 350's whole difference was 120/180 days
  against 180/270.
- **The importer keeps the first run's report and overwrites one rerun file** for every later
  run, so copying `import-report.json` after a later batch copies batch-04's numbers. Ledgers
  are now copied immediately after each run and their `startedAt` asserted equal to the run
  stamp.
- **LegiScan's `passed` flag ignores Nevada's two-thirds requirement.** SB 391 got 13-8 and
  lost, yet the stored `result` reads `Passed`.

## The largest thing this run did not import

Five of Nevada's most prominent 2025 election bills were dropped under filter 5 because they
expand and restrict voting in the same text: **AB 499** (photo identification), **AB 534**
(an 82-section omnibus), **SB 74** (an elections omnibus), **SB 422** (registration
identification), and **AB 79** (campaign finance). AB 499 is the sharpest case: the Assembly
version contains no photo identification requirement at all, and the Senate version does. The
reasoning is set out in `batch-09/PLAN.md` so a reader can disagree with it.
